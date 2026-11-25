package githubapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxRunsExaminedPerRepo is the one bound on how many active workflow runs a
// single read examines per repository, queued and in-progress runs COMBINED.
// It is both the per_page of each run listing and the budget the two listings
// share: queued runs are listed first, and the in-progress listing asks only
// for what the queued one left over, so a repository costs at most two listing
// calls plus this many job-listing calls.
//
// debt: ten active runs per repository per read, beyond which that
// repository's demand undercounts for this read only; paginate, or raise it,
// if the read's log shows a served repository routinely holding more than ten
// active runs while its queued pool jobs wait.
const maxRunsExaminedPerRepo = 10

// jobsPerPage is the job listing's page size. GitHub's default is 30; a
// matrix run can hold more jobs than that, and a queued pool job on page two
// would otherwise be invisible.
const jobsPerPage = 100

// rateLimitFloor stops a read early once GitHub reports fewer remaining
// requests than this on the installation token, so the reader never spends
// the last of a budget the scale-set sessions and registry share.
const rateLimitFloor = 100

// secondaryRateLimitWait is GitHub's documented wait when a rate-limited
// response carries neither Retry-After nor X-RateLimit-Reset ("wait for at
// least one minute before retrying").
const secondaryRateLimitWait = time.Minute

// maxRateLimitWait bounds how far a reported reset can push the next read, so
// a malformed or hostile header can never disable the reader indefinitely. The
// primary rate-limit window is one hour.
const maxRateLimitWait = time.Hour

// ErrActionsReadNotGranted means the installation does not grant the
// `actions` permission, so no actions:read token can be minted for it.
var ErrActionsReadNotGranted = errors.New("the GitHub App installation lacks actions:read")

// GrantsActionsRead reports whether the installation's granted permissions
// include `actions` at read or write, the permission QueuedPoolJobs mints.
func (i Installation) GrantsActionsRead() bool {
	permission := i.Permissions["actions"]
	return permission == "read" || permission == "write"
}

// QueuedJobsRead is one QueuedPoolJobs pass across the supplied repositories.
type QueuedJobsRead struct {
	// Counts holds a queued-pool-job count for every repository read
	// successfully, zeros included. A repository that was refused, failed, or
	// never reached because the read stopped early has no entry, so its
	// previous reading is kept rather than overwritten.
	Counts map[int64]int
	// Refused names every supplied repository the mint rule refused. None of
	// them was included in the token request.
	Refused []string
	// DiscoveryPending is true when no complete discovery has been recorded
	// yet, so every repository was refused for that reason alone.
	DiscoveryPending bool
	// Failed counts repositories whose read failed; FailedFirst names the first
	// one and its safe error (no response body) for the log line.
	Failed      int
	FailedFirst string
	APICalls    int
	// RateRemaining is the last X-RateLimit-Remaining GitHub sent.
	RateRemaining  int
	RateLimitKnown bool
	// RateLimited is true when the read stopped early on a rate-limit response
	// or because RateRemaining fell below rateLimitFloor. RetryAfter is then the
	// earliest instant the next read should start.
	RateLimited bool
	RetryAfter  time.Time

	rateReset time.Time
}

// rateLimitError is a 429, or a 403 carrying rate-limit headers: the read
// must stop, not skip one repository and hammer the next.
type rateLimitError struct {
	status int
	until  time.Time
}

func (e *rateLimitError) Error() string {
	return fmt.Sprintf("GitHub API rate limited (HTTP %d)", e.status)
}

// QueuedPoolJobs asks the REST API, for each supplied repository, how many
// workflow JOBS are queued against `label` -- the scale-set name this pool
// advertises under (`slipway-linux` / `slipway-macos`). It is the demand source
// homelab#913 adds: independent of the scale-set queue sessions, so a job
// queued while those sessions were deaf, or during a restart before any
// session re-observes it, still counts.
//
// The token is minted for actions:read and exactly the repositories that pass
// the same rule ScaleSetClient applies -- owner-matched, present in the last
// complete discovery, and private and active AS DISCOVERY REPORTED THEM. The
// rule lives here, at the credential, rather than trusting the caller's
// eligibility filter: the App is installed on every repository in the
// account, so an unscoped or over-scoped token would read them all.
//
// Per repository it lists queued runs, then in-progress runs (a dependent job
// after a hosted one, or a later matrix leg, queues inside a run that is
// already in progress), sharing maxRunsExaminedPerRepo between them, and
// counts each examined run's jobs whose status is "queued" and whose labels
// include `label`. One repository's failure is recorded and skipped; a
// rate-limit response, or a remaining budget below rateLimitFloor, stops the
// read early with what it has.
func (a *App) QueuedPoolJobs(ctx context.Context, installation Installation, repos []Repository, label string) (QueuedJobsRead, error) {
	read := QueuedJobsRead{Counts: map[int64]int{}}
	if label == "" {
		return read, errors.New("queued pool job read requires a non-empty label")
	}
	if installation.ID <= 0 || (installation.Selection != "selected" && installation.Selection != "all") {
		return read, errors.New("queued pool job read requires a selected or all-repositories installation")
	}
	if !installation.GrantsActionsRead() {
		return read, ErrActionsReadNotGranted
	}
	read.DiscoveryPending = !a.hasDiscovery()
	accepted := make([]Repository, 0, len(repos))
	for _, repo := range repos {
		if a.actionsReadRefusal(installation, repo) != nil {
			read.Refused = append(read.Refused, repo.FullName)
			continue
		}
		accepted = append(accepted, repo)
	}
	if len(accepted) == 0 {
		return read, nil
	}
	token, err := a.actionsReadToken(ctx, installation, accepted)
	if err != nil {
		return read, fmt.Errorf("minting a scoped actions:read token: %w", err)
	}
	// The token lives for this one read. Revoking it on every return path --
	// a rate-limit stop and a cancelled context included -- keeps the window a
	// leaked copy could be used in to the read itself, not the hour GitHub
	// would otherwise leave it valid.
	defer a.revokeInstallationToken(ctx, installation, token)
	for _, repo := range accepted {
		count, err := a.countQueuedPoolJobs(ctx, repo.FullName, token, label, &read)
		var limited *rateLimitError
		if errors.As(err, &limited) {
			read.RateLimited, read.RetryAfter = true, limited.until
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				return read, ctx.Err()
			}
			read.Failed++
			if read.FailedFirst == "" {
				read.FailedFirst = repo.FullName + ": " + err.Error()
			}
			continue
		}
		read.Counts[repo.ID] = count
		if read.RateLimitKnown && read.RateRemaining < rateLimitFloor {
			read.RateLimited, read.RetryAfter = true, boundedRetry(read.rateReset, time.Now())
			break
		}
	}
	return read, nil
}

// actionsReadRefusal is ScaleSetClient's repository rule, re-applied where the
// actions:read credential is minted. Visibility is read from the discovery
// record, never from the caller's copy.
func (a *App) actionsReadRefusal(installation Installation, repo Repository) error {
	if repo.ID <= 0 || !repositoryName.MatchString(repo.FullName) || !strings.EqualFold(strings.Split(repo.FullName, "/")[0], installation.Account.Login) {
		return errors.New("repository outside installation owner")
	}
	switch err := a.discoveredEligibility(repo); {
	case errors.Is(err, errNotDiscovered):
		return errors.New("repository absent from the installation's last complete discovery")
	case err != nil:
		return errors.New("actions:read token refuses a public, archived or disabled repository")
	}
	return nil
}

func (a *App) countQueuedPoolJobs(ctx context.Context, fullName, token, label string, read *QueuedJobsRead) (int, error) {
	seen := map[int64]bool{}
	var runIDs []int64
	for _, status := range []string{"queued", "in_progress"} {
		budget := maxRunsExaminedPerRepo - len(runIDs)
		if budget <= 0 {
			break
		}
		ids, err := a.runIDs(ctx, fullName, status, budget, token, read)
		if err != nil {
			return 0, fmt.Errorf("listing %s runs: %w", status, err)
		}
		for _, id := range ids {
			// A run can move from queued to in_progress between the two
			// listings; count it once.
			if !seen[id] {
				seen[id] = true
				runIDs = append(runIDs, id)
			}
		}
	}
	total := 0
	for _, runID := range runIDs {
		jobs, err := a.queuedPoolJobsInRun(ctx, fullName, runID, token, label, read)
		if err != nil {
			return 0, fmt.Errorf("listing jobs for run %d: %w", runID, err)
		}
		total += jobs
	}
	return total, nil
}

type runsResponse struct {
	WorkflowRuns []struct {
		ID int64 `json:"id"`
	} `json:"workflow_runs"`
}

// runIDs lists up to `limit` run IDs in one status. The slice is truncated to
// `limit` even if GitHub returns more than per_page asked for, so the shared
// per-repository budget holds regardless of the server.
func (a *App) runIDs(ctx context.Context, fullName, status string, limit int, token string, read *QueuedJobsRead) ([]int64, error) {
	path := fmt.Sprintf("/repos/%s/actions/runs?status=%s&per_page=%d", fullName, status, limit)
	var response runsResponse
	if err := a.readGet(ctx, path, token, &response, read); err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(response.WorkflowRuns))
	for _, run := range response.WorkflowRuns {
		if run.ID > 0 && len(ids) < limit {
			ids = append(ids, run.ID)
		}
	}
	return ids, nil
}

type jobsResponse struct {
	Jobs []struct {
		Status string   `json:"status"`
		Labels []string `json:"labels"`
	} `json:"jobs"`
}

func (a *App) queuedPoolJobsInRun(ctx context.Context, fullName string, runID int64, token, label string, read *QueuedJobsRead) (int, error) {
	path := fmt.Sprintf("/repos/%s/actions/runs/%s/jobs?per_page=%d", fullName, strconv.FormatInt(runID, 10), jobsPerPage)
	var response jobsResponse
	if err := a.readGet(ctx, path, token, &response, read); err != nil {
		return 0, err
	}
	count := 0
	for _, job := range response.Jobs {
		if job.Status != "queued" {
			continue
		}
		if servesExactly(job.Labels, label) {
			count++
		}
	}
	return count, nil
}

// actionsReadPermissions is the one permission set the REST demand token is
// minted for, and the one set the mint response must echo back.
var actionsReadPermissions = map[string]string{"actions": "read"}

// actionsReadTokenRequest is the installation-token request body. Field order
// is fixed so the body is byte-for-byte predictable.
type actionsReadTokenRequest struct {
	RepositoryIDs []int64           `json:"repository_ids"`
	Permissions   map[string]string `json:"permissions"`
}

// actionsReadTokenResponse is the part of GitHub's mint response this reader
// checks before using the token.
type actionsReadTokenResponse struct {
	Token        string            `json:"token"`
	ExpiresAt    string            `json:"expires_at"`
	Permissions  map[string]string `json:"permissions"`
	Repositories []struct {
		ID int64 `json:"id"`
	} `json:"repositories"`
}

// actionsReadToken mints an installation access token scoped to `actions:read`
// and exactly the supplied repositories -- never the broader Administration
// write credential ScaleSetClient uses, and never an unscoped installation
// token. Callers pass only repositories actionsReadRefusal accepted; an empty
// list is refused here too, because GitHub reads an omitted or empty
// repository list as every repository the installation can reach.
//
// The scope is sent as repository IDs from the discovery record, not names: a
// repository renamed away and a new one created under the old name between
// discoveries would otherwise receive the token. The response is then checked
// against the request, and a token granting anything else is revoked unused.
func (a *App) actionsReadToken(ctx context.Context, installation Installation, repos []Repository) (string, error) {
	if len(repos) == 0 {
		return "", errors.New("actions:read token requires at least one repository")
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
	var access actionsReadTokenResponse
	err = a.request(ctx, "POST", fmt.Sprintf("/app/installations/%d/access_tokens", installation.ID), jwtToken,
		actionsReadTokenRequest{RepositoryIDs: ids, Permissions: actionsReadPermissions}, &access)
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
	a.logger().Info("Minted a scoped actions:read installation token for the REST demand read.",
		"installation", installation.ID, "permissions", "actions:read",
		"repositories", strings.Join(names, ","), "repository_ids", ids, "expires_at", access.ExpiresAt)
	if err := verifyActionsReadGrant(ids, access.Permissions, granted); err != nil {
		a.revokeInstallationToken(ctx, installation, access.Token)
		return "", err
	}
	return access.Token, nil
}

// implicitGrantPermissions are the permissions GitHub may add to any
// repository-scoped token without being asked: metadata:read comes with every
// repository permission, so the mint response can echo it beside actions:read.
var implicitGrantPermissions = map[string]string{"metadata": "read"}

// verifyActionsReadGrant reports whether a mint response granted exactly what
// was requested: actions:read, plus at most GitHub's implicit metadata:read and
// nothing else, and the requested repository IDs and no others. A nil grant
// list is a mismatch, not "every repository": GitHub omits the list only for
// an unscoped token.
func verifyActionsReadGrant(requested []int64, permissions map[string]string, granted []int64) error {
	if permissions["actions"] != actionsReadPermissions["actions"] {
		return fmt.Errorf("installation token granted permissions %v, expected %v", permissions, actionsReadPermissions)
	}
	for name, level := range permissions {
		requested, isRequested := actionsReadPermissions[name]
		implicit, isImplicit := implicitGrantPermissions[name]
		if !(isRequested && level == requested) && !(isImplicit && level == implicit) {
			return fmt.Errorf("installation token granted permissions %v, expected %v plus at most %v",
				permissions, actionsReadPermissions, implicitGrantPermissions)
		}
	}
	want, got := slices.Clone(requested), slices.Clone(granted)
	slices.Sort(want)
	slices.Sort(got)
	if len(want) == 0 || !slices.Equal(want, got) {
		return fmt.Errorf("installation token granted repository IDs %v, expected exactly %v", got, want)
	}
	return nil
}

// revokeTimeout bounds the best-effort revoke so a slow GitHub cannot hold a
// finished read open.
const revokeTimeout = 10 * time.Second

// revokeInstallationToken deletes an installation token GitHub would otherwise
// keep valid for an hour. It is best effort: a failure is logged and never
// fails the read. It runs even when the read's context is already cancelled,
// since that is exactly the path a token must not be left alive on.
func (a *App) revokeInstallationToken(ctx context.Context, installation Installation, token string) {
	revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), revokeTimeout)
	defer cancel()
	if err := a.deleteInstallationToken(revokeCtx, token); err != nil {
		a.logger().Warn("Encountered an error while revoking the actions:read installation token; it stays valid until it expires.",
			"installation", installation.ID, "error", err)
	}
}

func (a *App) deleteInstallationToken(ctx context.Context, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, a.baseURL+"/installation/token", nil)
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
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("GitHub API HTTP %d, expected 204", resp.StatusCode)
	}
	return nil
}

// readGet is one GET for QueuedPoolJobs: it counts the call and records the
// rate-limit headers on `read` whether or not the call succeeded.
func (a *App) readGet(ctx context.Context, path, token string, target any, read *QueuedJobsRead) error {
	read.APICalls++
	meta, err := a.requestWithRateLimit(ctx, http.MethodGet, path, token, nil, target)
	if meta.remainingKnown {
		read.RateRemaining, read.RateLimitKnown = meta.remaining, true
	}
	if !meta.reset.IsZero() {
		read.rateReset = meta.reset
	}
	return err
}

type rateMeta struct {
	remaining      int
	remainingKnown bool
	reset          time.Time
}

// requestWithRateLimit is App.request plus the rate-limit headers this reader
// acts on. Kept separate from request rather than widening its signature,
// since every other caller has no use for the headers.
func (a *App) requestWithRateLimit(ctx context.Context, method, path, token string, body, target any) (rateMeta, error) {
	var meta rateMeta
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return meta, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return meta, errors.New("invalid API request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return meta, errors.New("GitHub API transport failed; inspect HTTP receipt")
	}
	defer resp.Body.Close()
	now := time.Now()
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
	retryAfter := resp.Header.Get("Retry-After")
	if resp.StatusCode == http.StatusTooManyRequests ||
		(resp.StatusCode == http.StatusForbidden && (retryAfter != "" || (meta.remainingKnown && meta.remaining == 0))) {
		until := meta.reset
		if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil && seconds >= 0 {
			until = now.Add(time.Duration(seconds) * time.Second)
		}
		return meta, &rateLimitError{status: resp.StatusCode, until: boundedRetry(until, now)}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return meta, fmt.Errorf("GitHub API HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(target); err != nil {
		return meta, errors.New("invalid GitHub API JSON")
	}
	return meta, nil
}

// boundedRetry turns a reported reset into the next permitted read: GitHub's
// documented one-minute wait when no future reset is known, and never more
// than maxRateLimitWait away.
func boundedRetry(until, now time.Time) time.Time {
	if !until.After(now) {
		return now.Add(secondaryRateLimitWait)
	}
	if limit := now.Add(maxRateLimitWait); until.After(limit) {
		return limit
	}
	return until
}

// servesExactly reports whether a job's labels are exactly the pool label, the
// same rule janitorJobsRefusal applies. A job that asks for the pool label
// plus anything else ([slipway-linux, gpu]) is one no worker of this pool can run,
// so counting it would claim demand forever: an unseen-demand probe every
// retry window, and a deaf watchdog that never sees the queue drain
// (homelab#1216).
func servesExactly(labels []string, label string) bool {
	return len(labels) == 1 && strings.EqualFold(labels[0], label)
}
