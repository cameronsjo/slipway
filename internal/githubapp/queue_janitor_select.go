package githubapp

import (
	"slices"
	"time"
)

// This file is the queue janitor's decision: pure functions over runs, jobs
// and pull requests already decoded from GitHub. Nothing here performs I/O,
// so every rule is table-testable, and the I/O in queue_janitor.go only
// gathers facts and asks these functions what they allow.
//
// The rules are written so that a missing fact always means KEEP. A run the
// janitor cannot prove dead is a run someone may still be waiting on.

// The two rules a run can be cancelled under. They appear verbatim in the
// audit line for each cancel.
const (
	JanitorRuleSuperseded = "superseded"
	JanitorRuleClosedPR   = "closed-pr"
)

// janitorEvents is the complete event allowlist. A push is further required
// to have a head_branch that no tag in the repository is named
// (janitorFacts.NoTagNamedHeadBranch); a tag push carries its tag name in
// head_branch and is indistinguishable from a branch push on the run object
// alone.
var janitorEvents = map[string]bool{"push": true, "pull_request": true, "pull_request_target": true}

// janitorRun is the part of a workflow run the janitor decides on.
type janitorRun struct {
	ID               int64
	WorkflowID       int64
	HeadBranch       string
	HeadSHA          string
	HeadRepositoryID int64
	Event            string
	Status           string
	Conclusion       string
	RunAttempt       int
	CreatedAt        time.Time
	PullRequests     []janitorRunPR
}

// janitorRunPR is one entry of a run's pull_requests[]. BaseRepositoryID is
// kept because GitHub can list a pull request from ANOTHER repository whose
// head happens to be this branch.
type janitorRunPR struct {
	Number           int64
	BaseRepositoryID int64
}

// janitorPR is a pull request as GET /repos/{r}/pulls/{n} reported it.
type janitorPR struct {
	Number           int64
	State            string
	ClosedAt         time.Time
	BaseRepositoryID int64
}

// janitorJob is one job of a run, as the jobs listing reported it. A job
// that has a runner, or any status but queued, has started or finished: its
// run is not dead work.
type janitorJob struct {
	Labels     []string
	Status     string
	RunnerName string
}

// janitorJobs is a run's job listing. TotalCount is GitHub's own count, so a
// listing that did not return every job can be told apart from one that did.
type janitorJobs struct {
	TotalCount int
	Jobs       []janitorJob
}

// janitorReference is the newest successful run of a workflow for one event
// on one branch, when it was created, and how many jobs it had. A successful
// run has had every job created, including jobs that wait on another through
// needs:, so its count is the workflow's job count as of its commit. A
// queued run lists only the jobs GitHub has created so far.
type janitorReference struct {
	RunID     int64
	CreatedAt time.Time
	JobCount  int
}

// Reasons decideJanitorRun keeps a run for when only its job count is
// unproven. They are the last refusal it checks, so a run kept for one of
// them passed every other guard.
const (
	janitorKeptJobCountUnknown = "workflow job count unknown"
	janitorKeptJobCountStale   = "no successful run of the workflow on this branch since the run was created"
	janitorKeptJobCountDiffers = "listed jobs differ from the workflow's last successful run"
)

// janitorJobCountReason reports whether a kept reason is one of the three
// job-count reasons.
func janitorJobCountReason(reason string) bool {
	return reason == janitorKeptJobCountUnknown || reason == janitorKeptJobCountStale || reason == janitorKeptJobCountDiffers
}

// janitorFacts is everything decideJanitorRun needs about one queued run.
// A nil Jobs, a nil Reference, an unchecked or colliding tag name, an empty
// Newer list or a missing pull request each leave the run kept.
type janitorFacts struct {
	Run    janitorRun
	RepoID int64
	Label  string
	Jobs   *janitorJobs
	// Reference is the workflow's job count, from its newest successful run
	// for the same event. Required: without it, jobs GitHub has not created
	// yet cannot be ruled out.
	Reference *janitorReference
	// NoTagNamedHeadBranch is true only when a complete read of the
	// repository's tags found none named exactly the run's head_branch. It is
	// required for a push.
	NoTagNamedHeadBranch bool
	// Newer holds runs that may supersede Run: the repository's own queued
	// listing and the workflow's run listing for the same branch and event.
	Newer []janitorRun
	// PullRequests holds the looked-up state of each pull request Run lists.
	PullRequests map[int64]janitorPR
}

func isPullRequestEvent(event string) bool {
	return event == "pull_request" || event == "pull_request_target"
}

// janitorPrefilter returns why a run is kept before any further read, or ""
// when it may be considered at all. It is the event allowlist, the re-run
// refusal and the queued requirement, applied to the listing's copy.
func janitorPrefilter(run janitorRun) string {
	switch {
	case run.ID <= 0 || run.WorkflowID <= 0 || run.HeadSHA == "" || run.HeadBranch == "" || run.CreatedAt.IsZero():
		return "incomplete run record"
	case run.Status != "queued":
		return "not queued"
	case run.RunAttempt != 1:
		return "re-run"
	case !janitorEvents[run.Event]:
		return "event outside the allowlist"
	}
	return ""
}

// janitorJobsRefusal returns why a run's jobs keep it, or "" when every job
// is a pool job that has not started. A job with any label other than
// exactly the pool label may be waiting on another runner, a listing shorter
// than GitHub's own total may hide one, and a job with a runner or a status
// other than queued has started work that a cancel would stop.
func janitorJobsRefusal(jobs janitorJobs, label string) string {
	switch {
	case label == "":
		return "no pool label"
	case jobs.TotalCount > len(jobs.Jobs):
		return "partial job list"
	case len(jobs.Jobs) == 0:
		return "no jobs listed"
	}
	for _, job := range jobs.Jobs {
		switch {
		case job.RunnerName != "":
			return "a job has a runner"
		case job.Status != "queued":
			return "a job is not queued"
		case !servesExactly(job.Labels, label):
			return "a job's labels are not exactly the pool label"
		}
	}
	return ""
}

// janitorJobCountRefusal returns why a run's listed jobs may not be all of
// its jobs, or "". A queued run lists only the jobs GitHub has created; a job
// that waits on another through needs: is created later, so its labels are
// never seen. The rule: the run must list exactly as many jobs as the
// workflow's newest successful run for the same event and branch had, and
// that run must be created AFTER this one. A later run on the same branch ran
// a workflow file at least as new as this run's, so a needs: job this run's
// file has, and a successful run before an edit lacked, cannot be matched by
// an older count. Any job the workflow creates later makes the count larger
// than the listing, and the run is kept.
func janitorJobCountRefusal(run janitorRun, jobs janitorJobs, reference *janitorReference) string {
	switch {
	case reference == nil || reference.RunID <= 0 || reference.JobCount <= 0:
		return janitorKeptJobCountUnknown
	case run.CreatedAt.IsZero() || !reference.CreatedAt.After(run.CreatedAt):
		return janitorKeptJobCountStale
	case len(jobs.Jobs) != reference.JobCount:
		return janitorKeptJobCountDiffers
	}
	return ""
}

// samePullRequests reports whether two runs list the same, non-empty set of
// pull request numbers.
func samePullRequests(a, b janitorRun) bool {
	if len(a.PullRequests) == 0 || len(b.PullRequests) == 0 {
		return false
	}
	numbers := func(run janitorRun) []int64 {
		out := make([]int64, 0, len(run.PullRequests))
		for _, pr := range run.PullRequests {
			out = append(out, pr.Number)
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	return slices.Equal(numbers(a), numbers(b))
}

// supersedingState reports whether a newer run's state lets it supersede an
// older one: still running, or finished with a verdict. A newer run that was
// cancelled, skipped or hit startup_failure proves nothing about the code.
func supersedingState(run janitorRun) bool {
	switch run.Status {
	case "queued", "in_progress":
		return true
	case "completed":
		return run.Conclusion == "success" || run.Conclusion == "failure"
	}
	return false
}

// janitorSupersededBy is rule R1. It returns the first run in newer that
// supersedes candidate: the same workflow, head repository, branch and
// event, the same pull request for pull_request* events, a DIFFERENT head
// SHA, a strictly later created_at, and a superseding state.
func janitorSupersededBy(candidate janitorRun, newer []janitorRun) (janitorRun, bool) {
	for _, run := range newer {
		switch {
		case run.ID == candidate.ID:
		case run.WorkflowID != candidate.WorkflowID:
		case run.HeadRepositoryID == 0 || run.HeadRepositoryID != candidate.HeadRepositoryID:
		case run.HeadBranch != candidate.HeadBranch || run.Event != candidate.Event:
		case isPullRequestEvent(candidate.Event) && !samePullRequests(candidate, run):
		case run.HeadSHA == "" || run.HeadSHA == candidate.HeadSHA:
		case !run.CreatedAt.After(candidate.CreatedAt):
		case !supersedingState(run):
		default:
			return run, true
		}
	}
	return janitorRun{}, false
}

// janitorClosedPRApplies reports whether rule R2 can be asked about a run at
// all: a pull_request* run from this repository itself, listing at least one
// pull request. Fork runs list none and are skipped here.
func janitorClosedPRApplies(run janitorRun, repoID int64) bool {
	return isPullRequestEvent(run.Event) && len(run.PullRequests) > 0 && run.HeadRepositoryID == repoID
}

// janitorClosedPR is rule R2: every pull request the run lists is this
// repository's own, is closed, and closed AFTER the run was created. The last
// condition spares the cleanup run a `types: [closed]` trigger creates.
func janitorClosedPR(run janitorRun, repoID int64, prs map[int64]janitorPR) bool {
	if !janitorClosedPRApplies(run, repoID) {
		return false
	}
	for _, listed := range run.PullRequests {
		pr, ok := prs[listed.Number]
		if !ok || listed.BaseRepositoryID != repoID || pr.BaseRepositoryID != repoID || pr.Number != listed.Number ||
			pr.State != "closed" || pr.ClosedAt.IsZero() || !run.CreatedAt.Before(pr.ClosedAt) {
			return false
		}
	}
	return true
}

// janitorRule returns the rule the gathered facts satisfy, ignoring jobs and
// the branch check. The I/O uses it to decide whether a run is worth the job
// read at all; decideJanitorRun is what authorizes a cancel.
func janitorRule(facts janitorFacts) string {
	if _, ok := janitorSupersededBy(facts.Run, facts.Newer); ok {
		return JanitorRuleSuperseded
	}
	if janitorClosedPR(facts.Run, facts.RepoID, facts.PullRequests) {
		return JanitorRuleClosedPR
	}
	return ""
}

// decideJanitorRun is the janitor's whole decision for one run. It returns
// the rule a cancel is made under, or "" and the reason the run is kept.
func decideJanitorRun(facts janitorFacts) (rule, kept string) {
	if reason := janitorPrefilter(facts.Run); reason != "" {
		return "", reason
	}
	if facts.Jobs == nil {
		return "", "jobs not read"
	}
	if reason := janitorJobsRefusal(*facts.Jobs, facts.Label); reason != "" {
		return "", reason
	}
	if facts.Run.Event == "push" && !facts.NoTagNamedHeadBranch {
		return "", "push head_branch not proven to be no tag's name"
	}
	rule = janitorRule(facts)
	if rule == "" {
		return "", "no rule applies"
	}
	// Checked last, so the I/O reads the reference only for a run that
	// passed everything else.
	if reason := janitorJobCountRefusal(facts.Run, *facts.Jobs, facts.Reference); reason != "" {
		return "", reason
	}
	return rule, ""
}

// janitorStillCancellable is the pre-cancel re-check: the run as a fresh GET
// reports it, immediately before the cancel, is the same run, still queued,
// still its first attempt and still the same commit.
func janitorStillCancellable(fresh, listed janitorRun) bool {
	return fresh.ID == listed.ID && fresh.Status == "queued" && fresh.RunAttempt == 1 && fresh.HeadSHA == listed.HeadSHA
}

// janitorPreCancelRefusal is the whole pre-cancel re-check on fresh reads:
// the run itself, then its jobs read again immediately before the cancel.
// It returns why the run is kept, or "" when the cancel may go ahead. Jobs
// created since the evaluation are checked too, and the job count must still
// equal the workflow's.
func janitorPreCancelRefusal(fresh, listed janitorRun, jobs janitorJobs, label string, reference *janitorReference) string {
	if !janitorStillCancellable(fresh, listed) {
		return "run no longer queued, first-attempt and on the same commit"
	}
	if reason := janitorJobsRefusal(jobs, label); reason != "" {
		return reason
	}
	return janitorJobCountRefusal(listed, jobs, reference)
}

// janitorCandidate is one run the janitor would cancel, and why.
type janitorCandidate struct {
	Repository Repository
	Run        janitorRun
	Rule       string
	// Reference is the job count the run matched at evaluation; the
	// pre-cancel re-check holds the re-read jobs to it.
	Reference janitorReference
}

// applyJanitorCaps bounds one pass: at most perRepo candidates from each
// repository, oldest first, then at most perPass in total, taken one round
// at a time across repositories (every repository's oldest, then every
// repository's second oldest, ...) so no one repository's backlog uses the
// whole pass. capped reports whether either cap dropped a candidate.
func applyJanitorCaps(byRepo [][]janitorCandidate, perRepo, perPass int) (selected []janitorCandidate, capped bool) {
	oldestFirst := func(a, b janitorCandidate) int {
		if c := a.Run.CreatedAt.Compare(b.Run.CreatedAt); c != 0 {
			return c
		}
		switch {
		case a.Run.ID < b.Run.ID:
			return -1
		case a.Run.ID > b.Run.ID:
			return 1
		}
		return 0
	}
	queues := make([][]janitorCandidate, 0, len(byRepo))
	for _, candidates := range byRepo {
		sorted := slices.Clone(candidates)
		slices.SortStableFunc(sorted, oldestFirst)
		if len(sorted) > perRepo {
			sorted, capped = sorted[:perRepo], true
		}
		queues = append(queues, sorted)
	}
	for round := 0; ; round++ {
		var batch []janitorCandidate
		for _, queue := range queues {
			if round < len(queue) {
				batch = append(batch, queue[round])
			}
		}
		if len(batch) == 0 {
			return selected, capped
		}
		slices.SortStableFunc(batch, oldestFirst)
		for _, candidate := range batch {
			if len(selected) == perPass {
				return selected, true
			}
			selected = append(selected, candidate)
		}
	}
}
