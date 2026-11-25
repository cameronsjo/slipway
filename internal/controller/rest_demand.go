package controller

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// restDemandReading is one repository's last REST-observed queued-pool-job
// count. freshUntil is computed by the reader at read time (twice the read
// interval after observedAt, see RESTDemandTracker.Refresh) rather than
// recomputed from a stored interval, so classifyRESTDemand never needs to
// know the interval at all -- only the instant it stops trusting the reading.
type restDemandReading struct {
	count      int
	observedAt time.Time
	freshUntil time.Time
}

// classifyRESTDemand decides how much demand one repository's last REST
// reading contributes to a sweep planning at `now`. It is pure so every case
// advertisement.go depends on is table-testable with no HTTP.
//
// A fresh reading contributes its WHOLE COUNT, and the caller adds it ON TOP
// of the repository's journaled work (see the comment at restDemandWant's call
// in advertisement.go). Demand in planCapacity is a repository's desired
// TOTAL, busy workers included: a current statistic contributes
// TotalAssignedJobs + TotalAvailableJobs. This used to return an absolute 1,
// which bought a slot only for a repository with nothing running: once one
// job ran, occupied 1 already met demand 1, and every further queued job
// waited with slots free. Measured 2026-09-25 23:07-23:19Z: homelab advertised
// 1 in all 13 cycles with 1-3 slots free and 4 runs queued, and its 01Z burst
// waited a median 2383 s with 2.17 of 4 slots in use. No monopoly follows:
// planCapacity still hands out one slot per repository per pass, and the
// admission headroom still bounds the total.
//
// FRESH is bounded, exactly like a statistics observation (classifyStatistics
// beside this). A reading that stops being re-confirmed must stop contributing,
// or a repository whose queue drained keeps claiming a slot forever -- the
// same starvation shape scale_sets.statistics caused before #904 bounded it.
func classifyRESTDemand(reading restDemandReading, now time.Time) int {
	if reading.count <= 0 || reading.freshUntil.IsZero() || now.After(reading.freshUntil) {
		return 0
	}
	return reading.count
}

// restDemandWant returns this repository's REST-demand contribution at
// planningAt. The caller must already hold s.mu -- exactly like every other
// process-local field this store keeps beside restDemand (started,
// lastEnvelope, observation), it is read without a second lock from inside
// PlanAdvertisements, which already holds one for the whole sweep.
func (s *Store) restDemandWant(repoID int64, planningAt time.Time) int {
	return classifyRESTDemand(s.restDemand[repoID], planningAt)
}

// RecordRESTDemand stores one successful REST read's queued-pool-job counts,
// replacing any previous reading for each repository in the result -- INCLUDING
// a zero count. A zero is stored deliberately: a repository whose queue
// drained must stop claiming a slot the moment this read confirms it, not
// only once the previous positive reading ages out up to two intervals later.
//
// A FAILED READ MUST NEVER CALL THIS. Keeping the previous reading, rather
// than clearing it, is the fail-open direction the design requires: a
// transient REST failure costs at most the remainder of the freshness window,
// never an immediate loss of the one signal a deaf-session outage depends on.
func (s *Store) RecordRESTDemand(counts map[int64]int, observedAt, freshUntil time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for repoID, count := range counts {
		s.restDemand[repoID] = restDemandReading{count: count, observedAt: observedAt, freshUntil: freshUntil}
	}
	if queuedAt(s.restDemand, observedAt) > 0 {
		if s.demandSince.IsZero() {
			s.demandSince = observedAt
		}
	} else {
		s.demandSince = time.Time{}
	}
}

// RESTDemandSignal returns the REST demand signal the deaf watchdog keys on
// (#1011): whether any reading is still fresh at now, the fresh queued total,
// and when that total last went positive. It never reads GitHub.
func (s *Store) RESTDemandSignal(now time.Time) DemandSignal {
	s.mu.Lock()
	defer s.mu.Unlock()
	signal := DemandSignal{Queued: queuedAt(s.restDemand, now)}
	for _, reading := range s.restDemand {
		if !reading.freshUntil.IsZero() && !now.After(reading.freshUntil) {
			signal.Known = true
			break
		}
	}
	if signal.Queued > 0 {
		signal.Since = s.demandSince
	}
	return signal
}

// queuedAt sums every repository's fresh queued count at now.
func queuedAt(readings map[int64]restDemandReading, now time.Time) int {
	total := 0
	for _, reading := range readings {
		total += classifyRESTDemand(reading, now)
	}
	return total
}

// RepoRef is the minimal identity a REST demand read needs per repository:
// the ID PlanAdvertisements keys demand on, and the owner/name path the REST
// API requires. It is a controller-owned type so RESTDemandSource stays free
// of the credential-holding githubapp package.
type RepoRef struct {
	ID       int64
	FullName string
}

// RESTDemandResult is one read across the repositories queried. Counts holds
// an entry for every repository read successfully, zeros included, so
// RecordRESTDemand can clear a drained repository's reading immediately
// rather than waiting for it to age out. A repository that was refused,
// failed, or never reached has no entry, and keeps its previous reading.
type RESTDemandResult struct {
	Counts         map[int64]int
	APICalls       int
	RateRemaining  int
	RateLimitKnown bool
	// Refused names repositories the credential's own rule refused to include
	// in the token; DiscoveryPending means that was only because no complete
	// discovery has been recorded yet.
	Refused          []string
	DiscoveryPending bool
	// Failed counts repositories whose read failed; FailedFirst names the
	// first one with its safe error.
	Failed      int
	FailedFirst string
	// RateLimited means the read stopped early; no read should start before
	// RetryAfter.
	RateLimited bool
	RetryAfter  time.Time
}

// RESTDemandSource is the thin HTTP seam PollSweep's planner never touches
// directly. The production implementation wraps githubapp.App; component and
// planner tests use a fake, so the pure classifyRESTDemand decision above
// never needs a live GitHub credential to test.
type RESTDemandSource interface {
	QueuedPoolJobs(ctx context.Context, repos []RepoRef, label string) (RESTDemandResult, error)
}

// RESTDemandTracker periodically asks a RESTDemandSource which repositories
// this controller serves hold queued jobs against the pool's label, and
// records the result on Store. It runs independently of Daemon.Cycle: REST
// demand is layered ON TOP of the session-envelope planner, never a
// dependency of it, so a stalled or failing tracker must never block or fail
// a cycle (see Refresh's fail-open handling below).
type RESTDemandTracker struct {
	store    *Store
	source   RESTDemandSource
	label    string
	interval time.Duration
	log      *slog.Logger
	now      func() time.Time
	// skipUntil and awaitingDiscovery are touched only by Refresh and Run,
	// which the daemon drives from one goroutine.
	skipUntil         time.Time
	awaitingDiscovery bool
}

// restDemandDiscoveryRetry is how soon the tracker retries a read that found
// no complete discovery yet. The first read races the first cycle's
// discovery at startup, which is exactly when a job queued during the restart
// most needs reading; waiting a whole interval would waste that window.
const restDemandDiscoveryRetry = 15 * time.Second

// NewRESTDemandTracker builds a tracker for one platform's pool label
// ("slipway-"+platform, the same label Registry and the session manager already
// use -- see the reference in registry.go rather than a second literal here).
// interval must be at least a minute; RuntimeConfig.Validate enforces the
// same floor before this is ever constructed. A nil source builds a disabled
// tracker: Run returns immediately and Refresh does nothing.
func NewRESTDemandTracker(store *Store, source RESTDemandSource, platform string, interval time.Duration) *RESTDemandTracker {
	return &RESTDemandTracker{store: store, source: source, label: "slipway-" + platform, interval: interval,
		log: slog.Default(), now: time.Now}
}

// Refresh reads served-repository demand once. A failure here is deliberately
// swallowed rather than returned: the REST reader is an optimisation over the
// queue sessions, never a dependency of them, and the previous reading is
// left in place for Store.restDemandWant to age out on its own schedule.
func (t *RESTDemandTracker) Refresh(ctx context.Context) {
	if t.source == nil {
		return
	}
	t.awaitingDiscovery = false
	if now := t.now(); now.Before(t.skipUntil) {
		t.log.Info("REST demand read skipped: backing off after GitHub rate limiting. Keeping the previous reading.",
			"retry_after", t.skipUntil.UTC().Format(time.RFC3339))
		return
	}
	repos, err := t.store.Repositories(ctx)
	if err != nil {
		t.log.Warn("REST demand read skipped: served-repository list unavailable. Keeping the previous reading.", "error", err)
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
	result, err := t.source.QueuedPoolJobs(ctx, refs, t.label)
	if err != nil {
		t.log.Warn("REST demand read failed. Keeping the previous reading until it ages out.",
			"error", err, "repos_queried", len(refs))
		return
	}
	if len(result.Refused) > 0 {
		if result.DiscoveryPending {
			t.awaitingDiscovery = true
			t.log.Info("REST demand read deferred: no complete repository discovery yet, so no repository can be included in the actions:read token.",
				"repos_refused", len(result.Refused), "retry_in", restDemandDiscoveryRetry)
		} else {
			t.log.Warn("REST demand read refused repositories the actions:read credential rule does not allow; they were left out of the token.",
				"repos_refused", len(result.Refused), "refused", strings.Join(result.Refused, ","))
		}
	}
	now := t.now().UTC()
	t.store.RecordRESTDemand(result.Counts, now, now.Add(2*t.interval))
	if result.RateLimited {
		t.skipUntil = result.RetryAfter
	}
	withQueued := 0
	for _, count := range result.Counts {
		if count > 0 {
			withQueued++
		}
	}
	fields := []any{"repos_queried", len(refs), "repos_read", len(result.Counts), "failed", result.Failed,
		"api_calls", result.APICalls, "repos_with_queued_pool_jobs", withQueued}
	if result.Failed > 0 {
		fields = append(fields, "failed_first", result.FailedFirst)
	}
	if result.RateLimitKnown {
		fields = append(fields, "rate_limit_remaining", result.RateRemaining)
	}
	if result.RateLimited {
		fields = append(fields, "rate_limited_until", result.RetryAfter.UTC().Format(time.RFC3339))
	}
	t.log.Info("Read queued pool-job demand from the REST API.", fields...)
}

// nextDelay is the wait before the next Refresh: a short retry while the
// first discovery is still pending, the rest of a rate-limit backoff when that
// is longer than an interval, otherwise the interval.
func (t *RESTDemandTracker) nextDelay() time.Duration {
	if t.awaitingDiscovery {
		return restDemandDiscoveryRetry
	}
	if backoff := t.skipUntil.Sub(t.now()); backoff > t.interval {
		return backoff
	}
	return t.interval
}

// Run refreshes immediately, then after each nextDelay, until ctx is
// cancelled. A disabled tracker returns at once.
func (t *RESTDemandTracker) Run(ctx context.Context) {
	if t.source == nil {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			t.Refresh(ctx)
			timer.Reset(t.nextDelay())
		}
	}
}
