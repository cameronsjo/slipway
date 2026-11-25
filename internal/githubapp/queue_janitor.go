package githubapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The queue janitor cancels queued workflow runs that can never matter: runs
// superseded by a newer commit (R1) and runs for a pull request that closed
// after the run was created (R2). It runs under its OWN GitHub App, never the
// pool App: actions:write lives only on that second, narrow App, so the pool
// App's key and every holder of it stay exactly as they were.
//
// Every path that cannot prove a run dead keeps it. Any error, empty result
// or 404 is a KEEP, never a cancel.

const (
	// janitorRunsPerPage is the queued-run listing's page size.
	janitorRunsPerPage = 100
	// janitorQueuedPageCap bounds the queued-run pages read per repository:
	// the newest page, which is also what R1 looks for newer queued runs in,
	// plus the oldest pages up to this total.
	//
	// debt: 300 queued runs per repository per pass, beyond which the middle
	// of a larger backlog is not examined this pass; raise it, or walk the
	// listing by created= windows, if a pass's log shows a repository whose
	// queue stays above 300 across passes.
	janitorQueuedPageCap = 3
	// janitorNewerRunsPerPage is how many of a workflow's newest runs on one
	// branch and event R1 reads. A superseding run older than these is not
	// seen, which keeps the run: the safe direction.
	janitorNewerRunsPerPage = 50
	// The per-pass and per-repository cancel caps. Dry-run applies the same.
	janitorMaxCancelsPerPass = 50
	janitorMaxCancelsPerRepo = 10
)

// janitorReadPermissions is the one permission set the janitor's read token
// is minted for and the one set its mint response must echo. It is a separate
// value from actionsReadPermissions (the REST demand reader's) on purpose:
// the two credentials belong to different Apps and never share a flag.
var janitorReadPermissions = map[string]string{"actions": "read", "pull_requests": "read"}

// janitorWritePermissions is the one permission set the janitor's cancel
// token is minted for, in enforce mode only, and the one set its mint
// response must echo.
var janitorWritePermissions = map[string]string{"actions": "write"}

// ErrJanitorPermissionsMissing means the janitor installation lacks a
// permission the requested mode needs.
var ErrJanitorPermissionsMissing = errors.New("the janitor GitHub App installation lacks a required permission")

// JanitorMissingPermissions names every permission the installation lacks
// for the janitor: actions:read and pull_requests:read always, and
// actions:write when enforce is true. A write grant satisfies a read.
func (i Installation) JanitorMissingPermissions(enforce bool) []string {
	var missing []string
	if level := i.Permissions["actions"]; level != "read" && level != "write" {
		missing = append(missing, "actions:read")
	}
	if level := i.Permissions["pull_requests"]; level != "read" && level != "write" {
		missing = append(missing, "pull_requests:read")
	}
	if enforce && i.Permissions["actions"] != "write" {
		missing = append(missing, "actions:write")
	}
	return missing
}

// JanitorInstallation reads the janitor App's installation by ID and checks
// it belongs to `owner` (the pool installation's account) with a selected or
// all-repositories grant. The permissions it returns are what the controller
// checks at startup.
func (a *App) JanitorInstallation(ctx context.Context, id int64, owner string) (Installation, error) {
	var installation Installation
	if id <= 0 || owner == "" {
		return installation, errors.New("janitor installation requires a positive installation ID and an owner")
	}
	token, err := a.jwt()
	if err != nil {
		return installation, errors.New("JWT signing failed")
	}
	if err := a.request(ctx, http.MethodGet, fmt.Sprintf("/app/installations/%d", id), token, nil, &installation); err != nil {
		return Installation{}, err
	}
	if installation.ID != id || !strings.EqualFold(installation.Account.Login, owner) ||
		(installation.Selection != "selected" && installation.Selection != "all") {
		return Installation{}, errors.New("janitor installation is not a selected or all-repositories installation on the pool's account")
	}
	return installation, nil
}

// JanitorAction is one cancel, or in dry-run one would-cancel. WorkflowID
// and Jobs (the job count read immediately before the cancel) make a
// multi-job workflow visible in the audit line; RefRunID and RefJobs name the
// successful run whose job count Jobs had to equal.
type JanitorAction struct {
	Repository string
	RunID      int64
	Rule       string
	WorkflowID int64
	Jobs       int
	RefRunID   int64
	RefJobs    int
	Age        time.Duration
	URL        string
	Cancelled  bool
}

// JanitorKept is a candidate the pre-cancel re-check kept, and why.
type JanitorKept struct {
	Repository string
	RunID      int64
	Rule       string
	Reason     string
}

// QueueJanitorPass is the outcome of one janitor pass.
type QueueJanitorPass struct {
	// Examined counts the queued runs listed across every repository read.
	Examined    int
	Actions     []JanitorAction
	Cancelled   int
	WouldCancel int
	// Kept lists every candidate the pre-cancel re-check kept.
	Kept []JanitorKept
	// KeptUnprovenJobs counts runs a rule applied to that were kept only
	// because their listed jobs could not be shown to be all of the
	// workflow's jobs, at evaluation or at the pre-cancel re-check.
	KeptUnprovenJobs int
	// Capped is true when a per-repository or per-pass cap dropped a
	// candidate, or a repository's queue was longer than the page cap.
	Capped bool
	// SkippedErrors counts reads and cancels that failed; each failure kept
	// its run. FirstError names the first, without any response body.
	SkippedErrors int
	FirstError    string
	// Refused names every supplied repository the mint rule refused.
	Refused        []string
	APICalls       int
	RateRemaining  int
	RateLimitKnown bool
	// RateLimited is true when the pass stopped early on a rate-limit
	// response or on rateLimitFloor; nothing is cancelled after that point.
	RateLimited bool
	RetryAfter  time.Time

	rateReset time.Time
}

func (p *QueueJanitorPass) skip(repo string, err error) {
	p.SkippedErrors++
	if p.FirstError == "" {
		p.FirstError = repo + ": " + err.Error()
	}
}

func (p *QueueJanitorPass) keep(candidate janitorCandidate, reason string) {
	p.Kept = append(p.Kept, JanitorKept{Repository: candidate.Repository.FullName, RunID: candidate.Run.ID,
		Rule: candidate.Rule, Reason: reason})
	if janitorJobCountReason(reason) {
		p.KeptUnprovenJobs++
	}
}

// errJanitorRateFloor stops a pass once GitHub reports fewer remaining
// requests than rateLimitFloor.
var errJanitorRateFloor = errors.New("GitHub API rate limit floor reached")

// QueueJanitorPass runs one janitor pass over `repos`, which are the pool's
// served repositories. label is the pool label every job of a cancelled run
// must carry and carry alone. enforce false is dry-run: the same reads, caps
// and re-checks, no write token and no cancel.
func (a *App) QueueJanitorPass(ctx context.Context, installation Installation, repos []Repository, label string, enforce bool) (QueueJanitorPass, error) {
	return a.queueJanitorPass(ctx, installation, repos, label, enforce, time.Now())
}

func (a *App) queueJanitorPass(ctx context.Context, installation Installation, repos []Repository, label string, enforce bool, now time.Time) (QueueJanitorPass, error) {
	var pass QueueJanitorPass
	if label == "" {
		return pass, errors.New("queue janitor requires a non-empty pool label")
	}
	if installation.ID <= 0 || (installation.Selection != "selected" && installation.Selection != "all") {
		return pass, errors.New("queue janitor requires a selected or all-repositories installation")
	}
	if missing := installation.JanitorMissingPermissions(enforce); len(missing) > 0 {
		return pass, fmt.Errorf("%w: %s", ErrJanitorPermissionsMissing, strings.Join(missing, ","))
	}
	// A fresh discovery each pass: visibility is read from what GitHub says
	// now about the janitor's own installation, never from the caller's copy.
	if _, err := a.janitorDiscover(ctx, installation); err != nil {
		return pass, fmt.Errorf("janitor repository discovery: %w", err)
	}
	accepted := make([]Repository, 0, len(repos))
	for _, repo := range repos {
		if a.actionsReadRefusal(installation, repo) != nil {
			pass.Refused = append(pass.Refused, repo.FullName)
			continue
		}
		accepted = append(accepted, repo)
	}
	if len(accepted) == 0 {
		return pass, nil
	}
	readToken, err := a.janitorToken(ctx, installation, accepted, janitorReadPermissions, "read")
	if err != nil {
		return pass, fmt.Errorf("minting the janitor read token: %w", err)
	}
	defer a.revokeJanitorToken(ctx, installation, readToken, "read")

	byRepo := make([][]janitorCandidate, 0, len(accepted))
	for _, repo := range accepted {
		candidates, err := a.janitorRepository(ctx, repo, readToken, label, &pass)
		if stop := janitorStop(ctx, err, &pass); stop != nil {
			return pass, stop
		}
		if pass.RateLimited {
			return pass, nil
		}
		if err != nil {
			pass.skip(repo.FullName, err)
			continue
		}
		byRepo = append(byRepo, candidates)
	}
	selected, capped := applyJanitorCaps(byRepo, janitorMaxCancelsPerRepo, janitorMaxCancelsPerPass)
	pass.Capped = pass.Capped || capped
	if len(selected) == 0 {
		return pass, nil
	}
	var writeToken string
	if enforce {
		var writeRepos []Repository
		for _, candidate := range selected {
			if !slices.ContainsFunc(writeRepos, func(repo Repository) bool { return repo.ID == candidate.Repository.ID }) {
				writeRepos = append(writeRepos, candidate.Repository)
			}
		}
		writeToken, err = a.janitorWriteToken(ctx, installation, writeRepos)
		if err != nil {
			return pass, fmt.Errorf("minting the janitor write token: %w", err)
		}
		defer a.revokeJanitorToken(ctx, installation, writeToken, "write")
	}
	for _, candidate := range selected {
		fresh, err := a.janitorRun(ctx, candidate.Repository.FullName, candidate.Run.ID, readToken, &pass)
		if stop := janitorStop(ctx, err, &pass); stop != nil {
			return pass, stop
		}
		if pass.RateLimited {
			return pass, nil
		}
		if err != nil {
			pass.skip(candidate.Repository.FullName, err)
			continue
		}
		if !janitorStillCancellable(fresh, candidate.Run) {
			pass.keep(candidate, "run no longer queued, first-attempt and on the same commit")
			continue
		}
		// The jobs are read again immediately before the cancel: a runner
		// may have taken one since the evaluation, and jobs created since
		// then are checked too. A failed read keeps the run.
		jobs, err := a.janitorJobs(ctx, candidate.Repository.FullName, candidate.Run.ID, readToken, &pass)
		if stop := janitorStop(ctx, err, &pass); stop != nil {
			return pass, stop
		}
		if pass.RateLimited {
			return pass, nil
		}
		if err != nil {
			pass.skip(candidate.Repository.FullName, fmt.Errorf("re-reading jobs of run %d: %w", candidate.Run.ID, err))
			pass.keep(candidate, "jobs re-read failed")
			continue
		}
		if reason := janitorPreCancelRefusal(fresh, candidate.Run, jobs, label, &candidate.Reference); reason != "" {
			pass.keep(candidate, reason)
			continue
		}
		if candidate.Rule == JanitorRuleClosedPR {
			// R2's pull requests are re-read too: one reopened since the
			// evaluation keeps the run.
			stillClosed, err := a.janitorPullRequestsStillClosed(ctx, candidate, readToken, &pass)
			if stop := janitorStop(ctx, err, &pass); stop != nil {
				return pass, stop
			}
			if pass.RateLimited {
				return pass, nil
			}
			if err != nil {
				pass.skip(candidate.Repository.FullName, err)
				continue
			}
			if !stillClosed {
				pass.keep(candidate, "pull request no longer closed after the run was created")
				continue
			}
		}
		action := JanitorAction{Repository: candidate.Repository.FullName, RunID: candidate.Run.ID, Rule: candidate.Rule,
			WorkflowID: candidate.Run.WorkflowID, Jobs: len(jobs.Jobs), RefRunID: candidate.Reference.RunID, RefJobs: candidate.Reference.JobCount,
			Age: now.Sub(candidate.Run.CreatedAt), URL: fmt.Sprintf("https://github.com/%s/actions/runs/%d", candidate.Repository.FullName, candidate.Run.ID)}
		if !enforce {
			pass.WouldCancel++
			pass.Actions = append(pass.Actions, action)
			continue
		}
		err = a.janitorCancel(ctx, candidate.Repository.FullName, candidate.Run.ID, writeToken, &pass)
		if stop := janitorStop(ctx, err, &pass); stop != nil {
			return pass, stop
		}
		if pass.RateLimited {
			return pass, nil
		}
		if err != nil {
			pass.skip(candidate.Repository.FullName, err)
			continue
		}
		action.Cancelled = true
		pass.Cancelled++
		pass.Actions = append(pass.Actions, action)
	}
	return pass, nil
}

// janitorStop turns a rate-limit error into the pass's stop state, and a
// cancelled context into the error the pass returns. Any other error is left
// for the caller to record as a skip.
func janitorStop(ctx context.Context, err error, pass *QueueJanitorPass) error {
	if err == nil {
		return nil
	}
	var limited *rateLimitError
	switch {
	case errors.As(err, &limited):
		pass.RateLimited, pass.RetryAfter = true, limited.until
	case errors.Is(err, errJanitorRateFloor):
		pass.RateLimited, pass.RetryAfter = true, boundedRetry(pass.rateReset, time.Now())
	case ctx.Err() != nil:
		return ctx.Err()
	}
	return nil
}

// janitorRepository lists one repository's queued runs and returns every
// run it can prove dead, stopping once it holds one more than the
// per-repository cap so applyJanitorCaps can tell a full repository from an
// exactly-full one.
func (a *App) janitorRepository(ctx context.Context, repo Repository, token, label string, pass *QueueJanitorPass) ([]janitorCandidate, error) {
	queued, truncated, err := a.janitorQueuedRuns(ctx, repo.FullName, token, pass)
	if err != nil {
		return nil, fmt.Errorf("listing queued runs: %w", err)
	}
	pass.Examined += len(queued)
	pass.Capped = pass.Capped || truncated
	caches := janitorRepoCaches{newer: map[string][]janitorRun{}, prs: map[int64]janitorPR{}, references: map[string]*janitorReference{}}
	var candidates []janitorCandidate
	for _, run := range queued {
		if len(candidates) > janitorMaxCancelsPerRepo {
			break
		}
		rule, reference, err := a.janitorEvaluate(ctx, repo, run, queued, label, token, &caches, pass)
		if err != nil {
			var limited *rateLimitError
			if errors.As(err, &limited) || errors.Is(err, errJanitorRateFloor) || ctx.Err() != nil {
				return nil, err
			}
			pass.skip(repo.FullName, fmt.Errorf("run %d: %w", run.ID, err))
			continue
		}
		if rule != "" && reference != nil {
			candidates = append(candidates, janitorCandidate{Repository: repo, Run: run, Rule: rule, Reference: *reference})
		}
	}
	return candidates, nil
}

// janitorRepoCaches holds one repository's reads for one pass, so runs that
// share a workflow, branch, pull request or tag listing cost one read.
type janitorRepoCaches struct {
	newer map[string][]janitorRun
	prs   map[int64]janitorPR
	tags  janitorTagCache
	// references is keyed by workflow ID, event and head branch. A nil value
	// is a read that found no usable reference; it keeps every such run.
	references map[string]*janitorReference
}

// janitorEvaluate gathers the facts for one queued run, cheapest first, and
// returns the rule decideJanitorRun authorizes a cancel under, or "", with
// the job-count reference the run matched.
func (a *App) janitorEvaluate(ctx context.Context, repo Repository, run janitorRun, queued []janitorRun, label, token string,
	caches *janitorRepoCaches, pass *QueueJanitorPass) (string, *janitorReference, error) {
	if janitorPrefilter(run) != "" {
		return "", nil, nil
	}
	newerCache, prCache, tags := caches.newer, caches.prs, &caches.tags
	facts := janitorFacts{Run: run, RepoID: repo.ID, Label: label, Newer: queued, PullRequests: prCache}
	// R1 first against the queued listing already in hand, then against the
	// workflow's own listing for this branch and event.
	if _, ok := janitorSupersededBy(run, queued); !ok && (!isPullRequestEvent(run.Event) || len(run.PullRequests) > 0) {
		key := fmt.Sprintf("%d\x00%s\x00%s", run.WorkflowID, run.HeadBranch, run.Event)
		listed, cached := newerCache[key]
		if !cached {
			var err error
			if listed, err = a.janitorWorkflowRuns(ctx, repo.FullName, run, token, pass); err != nil {
				return "", nil, fmt.Errorf("listing newer runs: %w", err)
			}
			newerCache[key] = listed
		}
		facts.Newer = append(slices.Clone(queued), listed...)
	}
	if janitorRule(facts) == "" && janitorClosedPRApplies(run, repo.ID) {
		for _, listed := range run.PullRequests {
			if _, cached := prCache[listed.Number]; cached {
				continue
			}
			pr, err := a.janitorPullRequest(ctx, repo.FullName, listed.Number, token, pass)
			if err != nil {
				return "", nil, fmt.Errorf("reading pull request %d: %w", listed.Number, err)
			}
			prCache[listed.Number] = pr
		}
	}
	if janitorRule(facts) == "" {
		return "", nil, nil
	}
	jobs, err := a.janitorJobs(ctx, repo.FullName, run.ID, token, pass)
	if err != nil {
		return "", nil, fmt.Errorf("listing jobs: %w", err)
	}
	facts.Jobs = &jobs
	if run.Event == "push" {
		names, err := tags.names(func() (map[string]bool, error) { return a.janitorTags(ctx, repo.FullName, token, pass) })
		if err != nil {
			return "", nil, fmt.Errorf("reading tags: %w", err)
		}
		facts.NoTagNamedHeadBranch = !names[run.HeadBranch]
	}
	rule, kept := decideJanitorRun(facts)
	if kept == janitorKeptJobCountUnknown {
		// Every other guard passed: only now is the workflow's job count
		// worth two reads, once per workflow, event and branch per pass.
		key := fmt.Sprintf("%d\x00%s\x00%s", run.WorkflowID, run.Event, run.HeadBranch)
		reference, cached := caches.references[key]
		if !cached {
			if reference, err = a.janitorReferenceRun(ctx, repo.FullName, run, token, pass); err != nil {
				return "", nil, fmt.Errorf("reading the workflow's job count: %w", err)
			}
			caches.references[key] = reference
		}
		facts.Reference = reference
		rule, kept = decideJanitorRun(facts)
	}
	if janitorJobCountReason(kept) {
		pass.KeptUnprovenJobs++
	}
	return rule, facts.Reference, nil
}

// janitorRunJSON is the part of a workflow run object the janitor decodes.
type janitorRunJSON struct {
	ID             int64  `json:"id"`
	WorkflowID     int64  `json:"workflow_id"`
	HeadBranch     string `json:"head_branch"`
	HeadSHA        string `json:"head_sha"`
	Event          string `json:"event"`
	Status         string `json:"status"`
	Conclusion     string `json:"conclusion"`
	RunAttempt     int    `json:"run_attempt"`
	CreatedAt      string `json:"created_at"`
	HeadRepository *struct {
		ID int64 `json:"id"`
	} `json:"head_repository"`
	PullRequests []struct {
		Number int64 `json:"number"`
		Base   struct {
			Repo struct {
				ID int64 `json:"id"`
			} `json:"repo"`
		} `json:"base"`
	} `json:"pull_requests"`
}

// run converts the decoded object. An unparseable created_at stays zero,
// which janitorPrefilter keeps.
func (raw janitorRunJSON) run() janitorRun {
	run := janitorRun{ID: raw.ID, WorkflowID: raw.WorkflowID, HeadBranch: raw.HeadBranch, HeadSHA: raw.HeadSHA,
		Event: raw.Event, Status: raw.Status, Conclusion: raw.Conclusion, RunAttempt: raw.RunAttempt}
	if created, err := time.Parse(time.RFC3339, raw.CreatedAt); err == nil {
		run.CreatedAt = created
	}
	if raw.HeadRepository != nil {
		run.HeadRepositoryID = raw.HeadRepository.ID
	}
	for _, pr := range raw.PullRequests {
		run.PullRequests = append(run.PullRequests, janitorRunPR{Number: pr.Number, BaseRepositoryID: pr.Base.Repo.ID})
	}
	return run
}

type janitorRunsResponse struct {
	TotalCount   *int             `json:"total_count"`
	WorkflowRuns []janitorRunJSON `json:"workflow_runs"`
}

// janitorQueuedRuns lists a repository's queued runs, oldest first. GitHub
// lists newest first and offers no other order, so the oldest runs are on
// the LAST pages: it reads page 1 for the total, then the last pages up to
// janitorQueuedPageCap. truncated reports that pages in between were not
// read.
func (a *App) janitorQueuedRuns(ctx context.Context, fullName, token string, pass *QueueJanitorPass) (runs []janitorRun, truncated bool, err error) {
	page := func(number int) (janitorRunsResponse, error) {
		query := url.Values{"status": {"queued"}, "per_page": {strconv.Itoa(janitorRunsPerPage)}, "page": {strconv.Itoa(number)}}
		var response janitorRunsResponse
		err := a.janitorGet(ctx, "/repos/"+fullName+"/actions/runs?"+query.Encode(), token, &response, pass)
		if err == nil && response.TotalCount == nil {
			err = errors.New("run listing carries no total_count")
		}
		return response, err
	}
	first, err := page(1)
	if err != nil {
		return nil, false, err
	}
	pages := (*first.TotalCount + janitorRunsPerPage - 1) / janitorRunsPerPage
	seen := map[int64]bool{}
	add := func(response janitorRunsResponse) {
		for _, raw := range response.WorkflowRuns {
			if raw.ID > 0 && !seen[raw.ID] {
				seen[raw.ID] = true
				runs = append(runs, raw.run())
			}
		}
	}
	add(first)
	start := 2
	if pages > janitorQueuedPageCap {
		start, truncated = pages-janitorQueuedPageCap+2, true
	}
	for number := start; number <= pages; number++ {
		response, err := page(number)
		if err != nil {
			return nil, false, err
		}
		add(response)
	}
	slices.SortStableFunc(runs, func(a, b janitorRun) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}
		switch {
		case a.ID < b.ID:
			return -1
		case a.ID > b.ID:
			return 1
		}
		return 0
	})
	return runs, truncated, nil
}

// janitorWorkflowRuns reads the newest runs of the candidate's workflow on
// the same branch and event, for R1.
func (a *App) janitorWorkflowRuns(ctx context.Context, fullName string, run janitorRun, token string, pass *QueueJanitorPass) ([]janitorRun, error) {
	query := url.Values{"branch": {run.HeadBranch}, "event": {run.Event}, "per_page": {strconv.Itoa(janitorNewerRunsPerPage)}}
	var response janitorRunsResponse
	path := fmt.Sprintf("/repos/%s/actions/workflows/%d/runs?%s", fullName, run.WorkflowID, query.Encode())
	if err := a.janitorGet(ctx, path, token, &response, pass); err != nil {
		return nil, err
	}
	runs := make([]janitorRun, 0, len(response.WorkflowRuns))
	for _, raw := range response.WorkflowRuns {
		runs = append(runs, raw.run())
	}
	return runs, nil
}

// janitorRun is the pre-cancel status re-check's fresh GET of one run.
func (a *App) janitorRun(ctx context.Context, fullName string, runID int64, token string, pass *QueueJanitorPass) (janitorRun, error) {
	var raw janitorRunJSON
	if err := a.janitorGet(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d", fullName, runID), token, &raw, pass); err != nil {
		return janitorRun{}, err
	}
	return raw.run(), nil
}

// janitorReferenceRun reads the workflow's newest successful run for the
// candidate's event and head branch, then that run's jobs, for the job-count
// rule. No successful run, a run that is not the same workflow, event and
// branch, or a job listing that is empty or incomplete returns nil, which
// keeps the candidate. Any read error is returned, which also keeps it.
// Whether the run is newer than the candidate is janitorJobCountRefusal's
// check, since one read serves every candidate on the branch.
func (a *App) janitorReferenceRun(ctx context.Context, fullName string, run janitorRun, token string, pass *QueueJanitorPass) (*janitorReference, error) {
	query := url.Values{"branch": {run.HeadBranch}, "event": {run.Event}, "status": {"success"}, "per_page": {"1"}}
	var response janitorRunsResponse
	path := fmt.Sprintf("/repos/%s/actions/workflows/%d/runs?%s", fullName, run.WorkflowID, query.Encode())
	if err := a.janitorGet(ctx, path, token, &response, pass); err != nil {
		return nil, err
	}
	if len(response.WorkflowRuns) == 0 {
		return nil, nil
	}
	reference := response.WorkflowRuns[0].run()
	if reference.ID <= 0 || reference.WorkflowID != run.WorkflowID || reference.Event != run.Event ||
		reference.HeadBranch != run.HeadBranch || reference.CreatedAt.IsZero() ||
		reference.Status != "completed" || reference.Conclusion != "success" {
		return nil, nil
	}
	jobs, err := a.janitorJobs(ctx, fullName, reference.ID, token, pass)
	if err != nil {
		return nil, err
	}
	if len(jobs.Jobs) == 0 || jobs.TotalCount != len(jobs.Jobs) {
		return nil, nil
	}
	return &janitorReference{RunID: reference.ID, CreatedAt: reference.CreatedAt, JobCount: len(jobs.Jobs)}, nil
}

// janitorJobs reads one run's jobs: labels, status and runner_name each.
func (a *App) janitorJobs(ctx context.Context, fullName string, runID int64, token string, pass *QueueJanitorPass) (janitorJobs, error) {
	var response struct {
		TotalCount *int `json:"total_count"`
		Jobs       []struct {
			Labels     []string `json:"labels"`
			Status     string   `json:"status"`
			RunnerName *string  `json:"runner_name"`
		} `json:"jobs"`
	}
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?per_page=%d", fullName, runID, jobsPerPage)
	if err := a.janitorGet(ctx, path, token, &response, pass); err != nil {
		return janitorJobs{}, err
	}
	if response.TotalCount == nil {
		return janitorJobs{}, errors.New("job listing carries no total_count")
	}
	jobs := janitorJobs{TotalCount: *response.TotalCount}
	for _, job := range response.Jobs {
		decoded := janitorJob{Labels: job.Labels, Status: job.Status}
		if job.RunnerName != nil {
			decoded.RunnerName = *job.RunnerName
		}
		jobs.Jobs = append(jobs.Jobs, decoded)
	}
	return jobs, nil
}

func (a *App) janitorPullRequest(ctx context.Context, fullName string, number int64, token string, pass *QueueJanitorPass) (janitorPR, error) {
	var response struct {
		Number   int64  `json:"number"`
		State    string `json:"state"`
		ClosedAt string `json:"closed_at"`
		Base     struct {
			Repo struct {
				ID int64 `json:"id"`
			} `json:"repo"`
		} `json:"base"`
	}
	if err := a.janitorGet(ctx, fmt.Sprintf("/repos/%s/pulls/%d", fullName, number), token, &response, pass); err != nil {
		return janitorPR{}, err
	}
	pr := janitorPR{Number: response.Number, State: response.State, BaseRepositoryID: response.Base.Repo.ID}
	if closed, err := time.Parse(time.RFC3339, response.ClosedAt); err == nil {
		pr.ClosedAt = closed
	}
	return pr, nil
}

// janitorPullRequestsStillClosed is R2's pre-cancel re-check: every pull
// request the run lists is read again, and R2 is asked again against those
// fresh reads. false keeps the run; so does any error.
func (a *App) janitorPullRequestsStillClosed(ctx context.Context, candidate janitorCandidate, token string, pass *QueueJanitorPass) (bool, error) {
	fresh := make(map[int64]janitorPR, len(candidate.Run.PullRequests))
	for _, listed := range candidate.Run.PullRequests {
		pr, err := a.janitorPullRequest(ctx, candidate.Repository.FullName, listed.Number, token, pass)
		if err != nil {
			return false, fmt.Errorf("re-reading pull request %d: %w", listed.Number, err)
		}
		fresh[listed.Number] = pr
	}
	return janitorClosedPR(candidate.Run, candidate.Repository.ID, fresh), nil
}

const (
	// janitorTagsPerPage is the tag listing's page size.
	janitorTagsPerPage = 100
	// janitorTagPageCap bounds the tag pages read per repository per pass. A
	// repository with more tags than this is never proven tag-free, so every
	// push run in it is kept.
	janitorTagPageCap = 10
)

// errJanitorTagCap means a repository's tag listing was still full at the
// page cap, so a tag named after a push run's head_branch cannot be ruled out.
var errJanitorTagCap = errors.New("tag listing exceeds the page cap")

// janitorTags reads every tag name in a repository with GET
// /repos/{r}/tags, which needs only metadata:read. It reads until a page
// shorter than janitorTagsPerPage; any error, or a full page at
// janitorTagPageCap, returns an error, which keeps every push run.
func (a *App) janitorTags(ctx context.Context, fullName, token string, pass *QueueJanitorPass) (map[string]bool, error) {
	names := map[string]bool{}
	for number := 1; number <= janitorTagPageCap; number++ {
		query := url.Values{"per_page": {strconv.Itoa(janitorTagsPerPage)}, "page": {strconv.Itoa(number)}}
		var page []struct {
			Name *string `json:"name"`
		}
		if err := a.janitorGet(ctx, "/repos/"+fullName+"/tags?"+query.Encode(), token, &page, pass); err != nil {
			return nil, err
		}
		for _, tag := range page {
			if tag.Name == nil {
				return nil, errors.New("tag listing entry carries no name")
			}
			names[*tag.Name] = true
		}
		if len(page) < janitorTagsPerPage {
			return names, nil
		}
	}
	return nil, errJanitorTagCap
}

// janitorTagCache holds one repository's tag read for one pass, so several
// push runs in the same repository cost one listing. A failed read is kept
// too: every later push run in that repository is kept for the same reason.
type janitorTagCache struct {
	read bool
	tags map[string]bool
	err  error
}

func (c *janitorTagCache) names(load func() (map[string]bool, error)) (map[string]bool, error) {
	if !c.read {
		c.tags, c.err = load()
		c.read = true
	}
	return c.tags, c.err
}

// janitorDiscover is the janitor's own discovery. It mints the same
// metadata:read token Discover does, then refuses it, and fails the pass,
// unless GitHub granted exactly metadata:read. The token is revoked when
// discovery returns, on every path, since the janitor App holds actions:write
// and none of its tokens may outlive their use. The pool App's Discover is
// left as it is.
func (a *App) janitorDiscover(ctx context.Context, installation Installation) ([]Repository, error) {
	access, err := a.discoveryToken(ctx, installation)
	if err != nil {
		return nil, err
	}
	defer a.revokeJanitorToken(ctx, installation, access.Token, "discovery")
	if err := verifyDiscoveryGrant(access.Permissions); err != nil {
		return nil, err
	}
	return a.discoverWith(ctx, installation, access.Token)
}

// verifyDiscoveryGrant reports whether a discovery mint granted exactly
// metadata:read and nothing else. A response with no permissions, or
// permissions that are not an object of strings, is refused.
func verifyDiscoveryGrant(raw json.RawMessage) error {
	var permissions map[string]string
	if err := json.Unmarshal(raw, &permissions); err != nil {
		return errors.New("janitor discovery token response carries no readable permissions")
	}
	if len(permissions) != len(discoveryPermissions) {
		return fmt.Errorf("janitor discovery token granted permissions %v, expected exactly %v", permissions, discoveryPermissions)
	}
	for name, level := range discoveryPermissions {
		if permissions[name] != level {
			return fmt.Errorf("janitor discovery token granted permissions %v, expected exactly %v", permissions, discoveryPermissions)
		}
	}
	return nil
}

// janitorGet is one GET for a janitor pass. It refuses to spend the last of
// the installation's budget, and records the rate-limit headers on pass
// whether or not the call succeeded.
func (a *App) janitorGet(ctx context.Context, path, token string, target any, pass *QueueJanitorPass) error {
	if pass.RateLimitKnown && pass.RateRemaining < rateLimitFloor {
		return errJanitorRateFloor
	}
	pass.APICalls++
	meta, err := a.requestWithRateLimit(ctx, http.MethodGet, path, token, nil, target)
	pass.recordRate(meta)
	return err
}

func (p *QueueJanitorPass) recordRate(meta rateMeta) {
	if meta.remainingKnown {
		p.RateRemaining, p.RateLimitKnown = meta.remaining, true
	}
	if !meta.reset.IsZero() {
		p.rateReset = meta.reset
	}
}

// janitorCancel is POST /repos/{r}/actions/runs/{id}/cancel. Only 202 is a
// cancel; a 409 (the run already finished) or anything else keeps the run.
func (a *App) janitorCancel(ctx context.Context, fullName string, runID int64, token string, pass *QueueJanitorPass) error {
	if pass.RateLimitKnown && pass.RateRemaining < rateLimitFloor {
		return errJanitorRateFloor
	}
	pass.APICalls++
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/repos/%s/actions/runs/%d/cancel", a.baseURL, fullName, runID), bytes.NewReader(nil))
	if err != nil {
		return errors.New("invalid API request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := a.http.Do(req)
	if err != nil {
		return errors.New("GitHub API transport failed; inspect HTTP receipt")
	}
	defer resp.Body.Close()
	now := time.Now()
	var meta rateMeta
	if raw := resp.Header.Get("X-RateLimit-Remaining"); raw != "" {
		if parsed, parseErr := strconv.Atoi(raw); parseErr == nil {
			meta.remaining, meta.remainingKnown = parsed, true
		}
	}
	if raw := resp.Header.Get("X-RateLimit-Reset"); raw != "" {
		if epoch, parseErr := strconv.ParseInt(raw, 10, 64); parseErr == nil && epoch > 0 {
			meta.reset = time.Unix(epoch, 0)
		}
	}
	pass.recordRate(meta)
	// The same rate-limit reading requestWithRateLimit applies to a GET.
	retryAfter := resp.Header.Get("Retry-After")
	if resp.StatusCode == http.StatusTooManyRequests ||
		(resp.StatusCode == http.StatusForbidden && (retryAfter != "" || (meta.remainingKnown && meta.remaining == 0))) {
		until := meta.reset
		if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil && seconds >= 0 {
			until = now.Add(time.Duration(seconds) * time.Second)
		}
		return &rateLimitError{status: resp.StatusCode, until: boundedRetry(until, now)}
	}
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("cancel returned GitHub API HTTP %d, expected 202", resp.StatusCode)
	}
	return nil
}

// janitorTokenRequest is the janitor's installation-token request body.
// Field order is fixed so the body is byte-for-byte predictable.
type janitorTokenRequest struct {
	RepositoryIDs []int64           `json:"repository_ids"`
	Permissions   map[string]string `json:"permissions"`
}

type janitorTokenResponse struct {
	Token        string            `json:"token"`
	ExpiresAt    string            `json:"expires_at"`
	Permissions  map[string]string `json:"permissions"`
	Repositories []struct {
		ID int64 `json:"id"`
	} `json:"repositories"`
}

// janitorWriteToken is the enforce-mode cancel token. The mint rule is
// re-applied here, from the discovery record, for every repository the token
// would cover, even though each already passed it for the read token.
func (a *App) janitorWriteToken(ctx context.Context, installation Installation, repos []Repository) (string, error) {
	for _, repo := range repos {
		if err := a.actionsReadRefusal(installation, repo); err != nil {
			return "", fmt.Errorf("janitor write token refuses %s: %w", repo.FullName, err)
		}
	}
	return a.janitorToken(ctx, installation, repos, janitorWritePermissions, "write")
}

// janitorToken mints an installation token scoped to exactly `permissions`
// and exactly the supplied repositories, by ID, and checks GitHub's response
// against the request. A token granting anything else is revoked unused.
// An empty repository list is refused before any call: GitHub reads it as
// every repository the installation reaches.
func (a *App) janitorToken(ctx context.Context, installation Installation, repos []Repository, permissions map[string]string, purpose string) (string, error) {
	if len(repos) == 0 {
		return "", errors.New("janitor token requires at least one repository")
	}
	jwtToken, err := a.jwt()
	if err != nil {
		return "", errors.New("JWT signing failed")
	}
	ids := make([]int64, 0, len(repos))
	names := make([]string, 0, len(repos))
	for _, repo := range repos {
		if repo.ID <= 0 {
			return "", fmt.Errorf("invalid repository ID %d for %q", repo.ID, repo.FullName)
		}
		ids = append(ids, repo.ID)
		names = append(names, repo.FullName)
	}
	var access janitorTokenResponse
	err = a.request(ctx, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", installation.ID), jwtToken,
		janitorTokenRequest{RepositoryIDs: ids, Permissions: permissions}, &access)
	if err != nil {
		return "", err
	}
	if access.Token == "" {
		return "", errors.New("installation access token missing")
	}
	granted := make([]int64, 0, len(access.Repositories))
	for _, repo := range access.Repositories {
		granted = append(granted, repo.ID)
	}
	a.logger().Info("Minted a scoped queue janitor installation token.",
		"installation", installation.ID, "purpose", purpose, "permissions", formatPermissions(permissions),
		"repositories", strings.Join(names, ","), "repository_ids", ids, "expires_at", access.ExpiresAt)
	if err := verifyJanitorGrant(permissions, ids, access.Permissions, granted); err != nil {
		a.revokeJanitorToken(ctx, installation, access.Token, purpose)
		return "", err
	}
	return access.Token, nil
}

func formatPermissions(permissions map[string]string) string {
	names := make([]string, 0, len(permissions))
	for name, level := range permissions {
		names = append(names, name+":"+level)
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

// verifyJanitorGrant reports whether a mint response granted exactly the
// requested permissions, plus at most GitHub's implicit metadata:read, and
// exactly the requested repository IDs. A nil grant list is a mismatch, not
// "every repository".
func verifyJanitorGrant(want map[string]string, requested []int64, permissions map[string]string, granted []int64) error {
	for name, level := range want {
		if permissions[name] != level {
			return fmt.Errorf("janitor token granted permissions %v, expected %v", permissions, want)
		}
	}
	for name, level := range permissions {
		wanted, isWanted := want[name]
		implicit, isImplicit := implicitGrantPermissions[name]
		if !(isWanted && level == wanted) && !(isImplicit && level == implicit) {
			return fmt.Errorf("janitor token granted permissions %v, expected %v plus at most %v", permissions, want, implicitGrantPermissions)
		}
	}
	wantIDs, gotIDs := slices.Clone(requested), slices.Clone(granted)
	slices.Sort(wantIDs)
	slices.Sort(gotIDs)
	if len(wantIDs) == 0 || !slices.Equal(wantIDs, gotIDs) {
		return fmt.Errorf("janitor token granted repository IDs %v, expected exactly %v", gotIDs, wantIDs)
	}
	return nil
}

// revokeJanitorToken deletes a janitor token GitHub would otherwise keep
// valid for an hour. Best effort, and run even on a cancelled context, since
// that is exactly the path a token must not be left alive on.
func (a *App) revokeJanitorToken(ctx context.Context, installation Installation, token, purpose string) {
	revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), revokeTimeout)
	defer cancel()
	if err := a.deleteInstallationToken(revokeCtx, token); err != nil {
		a.logger().Warn("Encountered an error while revoking a queue janitor installation token; it stays valid until it expires.",
			"installation", installation.ID, "purpose", purpose, "error", err)
	}
}
