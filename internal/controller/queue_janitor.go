package controller

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"time"
)

// QueueJanitorMode is the queue janitor's operating mode. Only enforce ever
// mints a write credential or cancels a run.
type QueueJanitorMode string

const (
	QueueJanitorOff     QueueJanitorMode = "off"
	QueueJanitorDryRun  QueueJanitorMode = "dry-run"
	QueueJanitorEnforce QueueJanitorMode = "enforce"
)

// ParseQueueJanitorMode reads the -queue-janitor flag value. Anything that
// is not exactly one of the three modes, in lowercase with no surrounding
// space, fails closed to off, and ok is false so the caller can say so. The
// match is exact so the controller accepts the same set verify-manifests.py
// pins in CI.
func ParseQueueJanitorMode(raw string) (mode QueueJanitorMode, ok bool) {
	switch QueueJanitorMode(raw) {
	case QueueJanitorOff:
		return QueueJanitorOff, true
	case QueueJanitorDryRun:
		return QueueJanitorDryRun, true
	case QueueJanitorEnforce:
		return QueueJanitorEnforce, true
	}
	return QueueJanitorOff, false
}

// QueueJanitorAction is one cancel, or in dry-run one would-cancel.
type QueueJanitorAction struct {
	Repository string
	RunID      int64
	Rule       string
	WorkflowID int64
	// Jobs is the run's job count read immediately before the cancel;
	// RefRunID and RefJobs are the successful run of the same workflow whose
	// job count it had to equal.
	Jobs      int
	RefRunID  int64
	RefJobs   int
	Age       time.Duration
	URL       string
	Cancelled bool
}

// QueueJanitorKept is a candidate the pre-cancel re-check kept, and why.
type QueueJanitorKept struct {
	Repository string
	RunID      int64
	Rule       string
	Reason     string
}

// QueueJanitorResult is one pass across the repositories supplied.
type QueueJanitorResult struct {
	Examined    int
	Actions     []QueueJanitorAction
	Kept        []QueueJanitorKept
	Cancelled   int
	WouldCancel int
	// KeptUnprovenJobs counts runs a rule applied to that were kept only
	// because their listed jobs could not be shown to be all of their
	// workflow's jobs.
	KeptUnprovenJobs int
	Capped           bool
	SkippedErrors    int
	FirstError       string
	Refused          []string
	APICalls         int
	RateLimited      bool
	RetryAfter       time.Time
}

// QueueJanitorSource is the seam the janitor calls through. The production
// implementation wraps the janitor's own githubapp.App; tests use a fake.
type QueueJanitorSource interface {
	Pass(ctx context.Context, repos []RepoRef, label string, enforce bool) (QueueJanitorResult, error)
}

// QueueJanitor runs a janitor pass every interval, independent of
// Daemon.Cycle and of the REST demand tracker. It never blocks or fails a
// cycle: a failed pass is logged, and every run it did not act on is kept.
type QueueJanitor struct {
	store    *Store
	source   QueueJanitorSource
	mode     QueueJanitorMode
	label    string
	interval time.Duration
	log      *slog.Logger
	// print writes the audit and summary lines. They are fixed-format text
	// read by an operator and by log queries, like the cycle line, so they
	// bypass slog's key quoting.
	print     func(string)
	now       func() time.Time
	skipUntil time.Time
}

// NewQueueJanitor builds a janitor for one platform's pool label. A nil
// source or the off mode builds a disabled janitor whose Run returns at once.
func NewQueueJanitor(store *Store, source QueueJanitorSource, platform string, mode QueueJanitorMode, interval time.Duration) *QueueJanitor {
	return &QueueJanitor{store: store, source: source, mode: mode, label: "slipway-" + platform, interval: interval,
		log: slog.Default(), print: func(line string) { log.Print(line) }, now: time.Now}
}

// Enabled reports whether Run will do anything.
func (j *QueueJanitor) Enabled() bool {
	return j != nil && j.source != nil && (j.mode == QueueJanitorDryRun || j.mode == QueueJanitorEnforce)
}

// formatQueueJanitorSummary is the one line every pass prints, success or
// failure: the counts an operator reads dry-run by, in a fixed order.
func formatQueueJanitorSummary(mode QueueJanitorMode, result QueueJanitorResult) string {
	return fmt.Sprintf("janitor mode=%s examined=%d cancelled=%d would_cancel=%d capped=%t skipped_errors=%d kept_unproven_jobs=%d",
		mode, result.Examined, result.Cancelled, result.WouldCancel, result.Capped, result.SkippedErrors, result.KeptUnprovenJobs)
}

// formatQueueJanitorAction is the audit line for one cancel or would-cancel.
func formatQueueJanitorAction(action QueueJanitorAction) string {
	verb := "would_cancel"
	if action.Cancelled {
		verb = "cancelled"
	}
	return fmt.Sprintf("janitor %s repo=%s run=%d rule=%s workflow_id=%d jobs=%d ref_run=%d ref_jobs=%d age=%s url=%s",
		verb, action.Repository, action.RunID, action.Rule, action.WorkflowID, action.Jobs, action.RefRunID, action.RefJobs,
		action.Age.Round(time.Second), action.URL)
}

// formatQueueJanitorKept is the audit line for a candidate the pre-cancel
// re-check kept.
func formatQueueJanitorKept(kept QueueJanitorKept) string {
	return fmt.Sprintf("janitor kept repo=%s run=%d rule=%s reason=%q", kept.Repository, kept.RunID, kept.Rule, kept.Reason)
}

// Pass runs one janitor pass. A failure is logged, never returned: the
// janitor is optional, and a failed pass cancels nothing.
func (j *QueueJanitor) Pass(ctx context.Context) {
	if !j.Enabled() {
		return
	}
	if now := j.now(); now.Before(j.skipUntil) {
		j.log.Info("Queue janitor pass skipped: backing off after GitHub rate limiting.",
			"retry_after", j.skipUntil.UTC().Format(time.RFC3339))
		return
	}
	var result QueueJanitorResult
	defer func() { j.print(formatQueueJanitorSummary(j.mode, result)) }()
	repos, err := j.store.Repositories(ctx)
	if err != nil {
		result.SkippedErrors++
		j.log.Warn("Queue janitor pass skipped: served-repository list unavailable. Nothing was cancelled.", "error", err)
		return
	}
	refs := make([]RepoRef, 0, len(repos))
	for _, repo := range repos {
		if repo.Eligible {
			refs = append(refs, RepoRef{ID: repo.Repository.ID, FullName: repo.Repository.FullName})
		}
	}
	if len(refs) == 0 {
		return
	}
	result, err = j.source.Pass(ctx, refs, j.label, j.mode == QueueJanitorEnforce)
	for _, kept := range result.Kept {
		j.print(formatQueueJanitorKept(kept))
	}
	for _, action := range result.Actions {
		j.print(formatQueueJanitorAction(action))
	}
	if result.RateLimited {
		j.skipUntil = result.RetryAfter
	}
	if err != nil {
		result.SkippedErrors++
		j.log.Warn("Queue janitor pass failed. Every run it had not already acted on was kept.", "error", err, "repos_queried", len(refs))
	}
	if len(result.Refused) > 0 {
		j.log.Warn("Queue janitor refused repositories its credential rule does not allow; they were left out of its tokens.",
			"repos_refused", len(result.Refused), "refused", strings.Join(result.Refused, ","))
	}
	if result.SkippedErrors > 0 && result.FirstError != "" {
		j.log.Warn("Queue janitor kept runs it could not read or cancel.", "skipped_errors", result.SkippedErrors, "first_error", result.FirstError)
	}
	if result.RateLimited {
		j.log.Info("Queue janitor pass stopped early on GitHub rate limiting; nothing was cancelled after that point.",
			"rate_limited_until", result.RetryAfter.UTC().Format(time.RFC3339))
	}
}

// nextDelay is the interval, or the rest of a rate-limit backoff when that
// is longer.
func (j *QueueJanitor) nextDelay() time.Duration {
	if backoff := j.skipUntil.Sub(j.now()); backoff > j.interval {
		return backoff
	}
	return j.interval
}

// Run passes after one interval, then after each nextDelay, until ctx is
// cancelled. The first pass waits an interval rather than running at start:
// the first cycles are busy discovering and onboarding, and a queue that has
// waited hours can wait ten more minutes. A disabled janitor returns at once.
func (j *QueueJanitor) Run(ctx context.Context) {
	if !j.Enabled() {
		return
	}
	timer := time.NewTimer(j.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			j.Pass(ctx)
			timer.Reset(j.nextDelay())
		}
	}
}
