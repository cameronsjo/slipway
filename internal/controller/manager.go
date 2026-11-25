package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

// PollResult records one member of a complete capacity-advertisement sweep.
// Envelope is nil when GitHub returned no message.
type PollResult struct {
	Set         SetKey
	MaxCapacity int
	Envelope    *Envelope
	// Outcome is what GitHub's queue answered this poll -- an empty 202, a
	// delivered message, an SDK-silent 401 refresh, or anything else Poll
	// treated as a failure. Feeds the cycle line's `polls=` tally.
	Outcome PollOutcome
}

// SessionManager owns the live, process-local queue handles. Durable session
// identity remains in Store; a new manager therefore resumes active identities
// before it may create a session for a newly managed repository.
type SessionManager struct {
	store  *Store
	open   func(context.Context, SetKey) (*QueueSession, error)
	resume func(context.Context, SetKey) (*QueueSession, error)
	// retryClose sends one fresh DELETE for a set's stale close-intent
	// (homelab#1167). Sync calls it only once the intent is due.
	retryClose func(context.Context, SetKey) error

	mu       sync.Mutex
	sessions map[SetKey]*QueueSession
	// degraded holds the sets whose session could not be established by the
	// last Sync. They are skipped by PollSweep and AcquireNext for that cycle
	// and retried by the next Sync; every other set keeps working.
	degraded map[SetKey]error
	// pollFailures counts each set's consecutive failed polls, so a set that
	// fails every cycle (a poison envelope GitHub keeps redelivering) is named
	// on every cycle instead of hiding behind the other sets' traffic.
	pollFailures map[SetKey]int
	// pollFailed holds the sets whose poll failed in the most recent sweep.
	// AcquireNext leaves them out of reservation for that cycle, as it does
	// degraded sets: acquiring through a session whose poll just failed risks
	// the same stranded, charged reservation.
	pollFailed map[SetKey]bool
	// openBound caps one session open or resume, which runs detached from the
	// cycle deadline (see Sync). Zero means sessionOpenTimeout.
	openBound time.Duration
	// closeBound caps one session DELETE during RotateIdleSessions. Zero means
	// sessionCloseTimeout.
	closeBound time.Duration
}

// sessionCloseTimeout bounds one session DELETE at a deaf exit. Rotation stops
// at the first DELETE that fails or times out, so the whole rotation is bounded
// by the fast DELETEs plus at most one of these.
const sessionCloseTimeout = 30 * time.Second

// SessionRotation is what RotateIdleSessions did at a deaf exit (homelab#1138).
// Every list is ordered by repository and set.
type SessionRotation struct {
	// Rotated sets had their session DELETEd (204, or 404 because GitHub had
	// already dropped it) and recorded closed. The next process opens a new
	// session for each instead of resuming the old UUID.
	Rotated []SetKey
	// Kept sets were refused by the close rules -- assigned, acquired or running
	// statistics, an unreleased lease, or an unacknowledged message -- and no
	// DELETE was sent. The next process resumes them as before.
	Kept []SetKey
	// Failed holds the set whose DELETE outcome is uncertain (any status but
	// 204/404, or a timeout). Its close-intent is retained, so no later Sync
	// replaces it until a close-intent retry resolves it.
	Failed []SessionDegradation
	// Skipped sets were not attempted because an earlier DELETE failed in this
	// rotation. They keep their sessions and resume as before.
	Skipped []SetKey
	// Blocked is why nothing was attempted, empty when rotation ran:
	// "close-intent" while CloseIntent is non-empty, or "journal-unreadable"
	// when the check itself failed.
	Blocked string
	// CloseIntent lists the sets already holding a close-intent when rotation
	// was asked for. Sync retries each with one fresh DELETE per
	// closeIntentRetryAfter (homelab#1167), but while any set holds one,
	// rotation does nothing: each failed DELETE would otherwise strand one
	// more set per deaf exit.
	CloseIntent []SetKey
}

// sessionOpenTimeout bounds one session create or resume. Both run detached
// from the cycle context, so the cycle deadline cannot cut a create POST
// midway and strand its create-intent; this bound replaces it. Sync starts no
// new create when less than this remains before the cycle deadline.
const sessionOpenTimeout = 60 * time.Second

// SetPollError is one set's failed poll inside a sweep. PollSweep joins these
// for per-set failures only; any other error it returns is global. The daemon
// keeps provisioning for the sets that did poll when every error is one of
// these (see Daemon.Cycle).
type SetPollError struct {
	Set SetKey
	Err error
}

func (e *SetPollError) Error() string {
	return fmt.Sprintf("poll failed for repository %d scale set %d: %v", e.Set.RepositoryID, e.Set.ScaleSetID, e.Err)
}

func (e *SetPollError) Unwrap() error { return e.Err }

// ErrRestartRequired marks session-manager state that only a new process can
// clear: an in-memory handle that is missing, fenced, or disagrees with its
// journaled identity. Nothing inside a cycle repairs it, so every later cycle
// failed the same way -- 599 times over about ten hours on 2026-10-01 while the
// pool ran nothing (homelab#1211). The daemon exits on it, like the deaf exit,
// and the restart resumes each set from its durable record.
var ErrRestartRequired = errors.New("process session state needs a restart")

// onlySetPollErrors reports whether err is non-nil and made entirely of
// SetPollError values, the shape PollSweep returns when some sets failed and
// the sweep as a whole did not.
func onlySetPollErrors(err error) bool {
	if err == nil {
		return false
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		_, single := err.(*SetPollError)
		return single
	}
	for _, member := range joined.Unwrap() {
		if !onlySetPollErrors(member) {
			return false
		}
	}
	return true
}

// SessionDegradation names one set Sync skipped this cycle, and why.
type SessionDegradation struct {
	Set SetKey
	Err error
}

func NewSessionManager(store *Store, app *githubapp.App, installation githubapp.Installation) (*SessionManager, error) {
	if store == nil || app == nil || installation.ID != store.config.InstallationID {
		return nil, errors.New("session manager App installation differs from journal")
	}
	return newSessionManager(store,
		func(ctx context.Context, key SetKey) (*QueueSession, error) {
			return store.OpenQueueSession(ctx, key, app, installation)
		},
		func(ctx context.Context, key SetKey) (*QueueSession, error) {
			return store.ResumeQueueSession(ctx, key, app, installation)
		},
		func(ctx context.Context, key SetKey) error {
			return store.RetryCloseIntent(ctx, key, app, installation)
		})
}

func newSessionManager(
	store *Store,
	open func(context.Context, SetKey) (*QueueSession, error),
	resume func(context.Context, SetKey) (*QueueSession, error),
	retryClose func(context.Context, SetKey) error,
) (*SessionManager, error) {
	if store == nil || open == nil || resume == nil || retryClose == nil {
		return nil, errors.New("session manager requires store, open, resume and close-retry operations")
	}
	return &SessionManager{store: store, open: open, resume: resume, retryClose: retryClose, sessions: map[SetKey]*QueueSession{},
		degraded: map[SetKey]error{}, pollFailures: map[SetKey]int{}, pollFailed: map[SetKey]bool{}}, nil
}

func (s *Store) managedSessionKeys(ctx context.Context) ([]SetKey, error) {
	if err := s.discoveryCurrent(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,ss.set_id
FROM repositories r JOIN managed_sets m ON m.repo_id=r.id
JOIN scale_sets ss ON ss.repo_id=r.id AND ss.set_id=m.remote_id
WHERE r.eligible=1 AND m.phase='active'
ORDER BY r.id,ss.set_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []SetKey
	for rows.Next() {
		var key SetKey
		if err := rows.Scan(&key.RepositoryID, &key.ScaleSetID); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// Sync establishes one handle for every eligible, API-verified managed set.
//
// A failure is scoped to its own set. A create, resume or unresolved
// close-intent for one set used to fail Sync for every set, so a single 5xx
// during one repository's onboarding stopped polling and provisioning for all
// of them, on every cycle, across restarts (the state is in SQLite). Now that
// set is recorded in Degraded, logged, and skipped; the rest are synced.
//
// What stays global: an unready store, an unreadable journal, and a live
// handle that disagrees with its durable identity. Those say the process
// itself cannot be trusted, not that one repository is unreachable.
//
// A create-intent left by an uncertain create is not replayed at once. The
// store never repeats an uncertain session POST right away (a redirect, a
// wrong owner or a foreign queue origin may have answered it;
// TestSDKSessionIntentSurvivesUncertainCreation), and it cannot be healed by
// lookup the way a scale-set create-intent is (registry.go finds its set by
// name): the SDK has no read-by-owner call for sessions, and the intent row
// holds no session ID to resume. So the set stays skipped, and named on every
// cycle, until the intent has been idle for createIntentRetryAfter. Then it
// gets one fresh create through the normal open path (homelab#1235): a 201
// makes the session active, and any other outcome, a 409 included, keeps the
// intent restamped and waits a full window again. A 409 is not dropped here,
// unlike on an ordinary create: GitHub still holds a session for the set, and
// dropping would turn the hourly retry into a 409 POST on every cycle. A
// close-intent (a session DELETE whose outcome was lost) is handled the same
// way: skipped until it has been idle for closeIntentRetryAfter, then one
// fresh DELETE; a 204 or this DELETE's own 404 records it closed and the set
// gets a new session, anything else keeps it for another window. To keep the
// cycle deadline from causing an uncertain create, opens and resumes run
// detached from it under sessionOpenTimeout.
func (m *SessionManager) Sync(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store.mu.Lock()
	ready := m.store.ready
	m.store.mu.Unlock()
	if !ready {
		return ErrInventoryUnknown
	}
	keys, err := m.store.managedSessionKeys(ctx)
	if err != nil {
		return err
	}
	records, err := m.store.PendingSessions(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	degraded := make(map[SetKey]error)
	pending := make(map[SetKey]SessionRecord, len(records))
	desired := make(map[SetKey]bool, len(keys))
	for _, key := range keys {
		desired[key] = true
	}
	for _, record := range records {
		if !desired[record.Set] {
			continue
		}
		switch record.Phase {
		case "active":
			pending[record.Set] = record
		case "create-intent":
			// A due intent is left out of both maps, so the loop below sends it
			// through m.open, which claims the intent and retries it once.
			if !createIntentRetryDue(record.Updated, now) {
				degraded[record.Set] = fmt.Errorf("session creation outcome is uncertain; its create-intent is retried with one fresh create after %s idle, at %s",
					createIntentRetryAfter, record.Updated.Add(createIntentRetryAfter).UTC().Format(time.RFC3339))
			}
		default:
			// A close-intent: a session DELETE whose outcome was lost. Once due,
			// it gets one fresh DELETE here. On success the record is closed and
			// the set is left out of both maps, so the loop below creates a new
			// session for it this same cycle.
			if !closeIntentRetryDue(record.Updated, now) {
				degraded[record.Set] = fmt.Errorf("session close outcome is uncertain; its close-intent is retried with one fresh DELETE after %s idle, at %s",
					closeIntentRetryAfter, record.Updated.Add(closeIntentRetryAfter).UTC().Format(time.RFC3339))
				continue
			}
			// Sync holds m.mu across this DELETE, up to closeTimeout per due
			// set. That is bounded and happens at most once per set per
			// closeIntentRetryAfter, the same trade the open path makes.
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.closeTimeout())
			err := m.retryClose(closeCtx, record.Set)
			cancel()
			if err != nil {
				degraded[record.Set] = err
			}
		}
	}
	for key, live := range m.sessions {
		if !desired[key] {
			// Drop the handle, and the Administration:write client inside it.
			// Nothing polls it while its repository is out of the desired set,
			// and a re-entry resumes the durable record through a freshly built
			// client that re-checks eligibility (homelab#924).
			if live != nil {
				live.CloseIdleConnections()
			}
			delete(m.sessions, key)
			continue
		}
		record, ok := pending[key]
		if !ok || live == nil || live.closed || !live.matchesSDK() ||
			live.owner != record.Owner || live.identity.id.String() != record.SessionID {
			return fmt.Errorf("process session handle differs from durable identity; restart and reconcile: %w", ErrRestartRequired)
		}
	}
	for _, key := range keys {
		if live := m.sessions[key]; live != nil {
			continue
		}
		if _, skip := degraded[key]; skip {
			continue
		}
		_, exists := pending[key]
		if !exists && !m.roomToOpen(ctx) {
			degraded[key] = errors.New("session creation deferred: less than the open bound remains before the cycle deadline")
			continue
		}
		// Detached from the cycle deadline: a create POST cut midway leaves a
		// create-intent nothing may replay, so the set would stay skipped until
		// an operator cleared it. The open gets its own bound instead.
		openCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.openTimeout())
		var session *QueueSession
		if exists {
			session, err = m.resume(openCtx, key)
		} else {
			session, err = m.open(openCtx, key)
		}
		cancel()
		if err == nil && (session == nil || session.key != key || session.closed || !session.matchesSDK()) {
			err = errors.New("session constructor returned an invalid handle")
		}
		if err != nil {
			degraded[key] = err
			continue
		}
		m.sessions[key] = session
	}
	m.degraded = degraded
	for key, cause := range degraded {
		slog.Warn("Skipped a scale set this cycle because its queue session could not be established; the other sets are unaffected and it is retried next cycle.",
			"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID, "error", cause)
	}
	return nil
}

// RotateIdleSessions closes the session of every set with no assigned work, so
// the restart that follows a deaf exit opens a new session for it. Resuming
// PATCHes the same UUID, and on 2026-09-26 ten restarts in a row resumed the
// same UUIDs GitHub had stopped delivering to (homelab#1138).
//
// It adds no close mechanism of its own. Each set goes through
// QueueSession.Close, so the journal's close rules decide: beginClose refuses a
// set with assigned statistics, an unreleased lease or an unacknowledged
// message before any DELETE, and a DELETE with an uncertain outcome keeps its
// close-intent, which Sync already refuses to replace.
//
// Each set is attempted once per call and its handle leaves the manager either
// way, so a second call sends nothing. Rotation stops at the first uncertain
// DELETE: while GitHub is failing DELETEs, every further attempt could leave
// another set behind a close-intent, and the rest keep the sessions they have.
func (m *SessionManager) RotateIdleSessions(ctx context.Context) SessionRotation {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rotation SessionRotation
	records, err := m.store.PendingSessions(ctx)
	if err != nil {
		rotation.Blocked = "journal-unreadable"
		slog.Warn("Could not read the session journal at the deaf exit, so no session was rotated; every set resumes its session.", "error", err)
		return rotation
	}
	for _, record := range records {
		if record.Phase == "close-intent" {
			rotation.CloseIntent = append(rotation.CloseIntent, record.Set)
		}
	}
	if len(rotation.CloseIntent) > 0 {
		sortSetKeys(rotation.CloseIntent)
		rotation.Blocked = "close-intent"
		slog.Warn("A set already holds an unresolved close-intent, so no session was rotated at this deaf exit; every other set resumes its session.",
			"close_intent_sets", len(rotation.CloseIntent))
		return rotation
	}
	keys := make([]SetKey, 0, len(m.sessions))
	for key := range m.sessions {
		keys = append(keys, key)
	}
	sortSetKeys(keys)
	for i, key := range keys {
		handle := m.sessions[key]
		if handle == nil {
			continue
		}
		// Detached from the cycle deadline for the same reason opens are: a
		// DELETE cut midway leaves a close-intent. It gets its own bound.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.closeTimeout())
		err := handle.Close(closeCtx)
		cancel()
		switch {
		case err == nil:
			delete(m.sessions, key)
			rotation.Rotated = append(rotation.Rotated, key)
		case !handle.closed:
			// Close refused before sending anything; the handle is still active.
			rotation.Kept = append(rotation.Kept, key)
			slog.Info("Kept a scale set's queue session at the deaf exit because the close rules refused it; the restart resumes it.",
				"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID, "reason", err)
		default:
			delete(m.sessions, key)
			m.degraded[key] = err
			rotation.Failed = append(rotation.Failed, SessionDegradation{Set: key, Err: err})
			rotation.Skipped = append(rotation.Skipped, keys[i+1:]...)
			slog.Warn("A scale set's session DELETE failed at the deaf exit; its close-intent is kept, it is not replaced until that intent is resolved, and rotation stopped for the remaining sets.",
				"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID, "skipped_sets", len(rotation.Skipped), "error", err)
			return rotation
		}
	}
	return rotation
}

func (m *SessionManager) closeTimeout() time.Duration {
	if m.closeBound > 0 {
		return m.closeBound
	}
	return sessionCloseTimeout
}

func (m *SessionManager) openTimeout() time.Duration {
	if m.openBound > 0 {
		return m.openBound
	}
	return sessionOpenTimeout
}

// roomToOpen reports whether a new session create can finish inside its own
// bound before the cycle deadline. Starting one with less time left would let
// the next cycle overlap a create still in flight.
func (m *SessionManager) roomToOpen(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return !ok || time.Until(deadline) >= m.openTimeout()
}

// Degraded lists the sets the last Sync skipped, ordered by repository and set.
func (m *SessionManager) Degraded() []SessionDegradation {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SessionDegradation, 0, len(m.degraded))
	for key, cause := range m.degraded {
		out = append(out, SessionDegradation{Set: key, Err: cause})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Set.RepositoryID == out[j].Set.RepositoryID {
			return out[i].Set.ScaleSetID < out[j].Set.ScaleSetID
		}
		return out[i].Set.RepositoryID < out[j].Set.RepositoryID
	})
	return out
}

func (m *SessionManager) degradedKeys() []SetKey {
	keys := make([]SetKey, 0, len(m.degraded))
	for key := range m.degraded {
		keys = append(keys, key)
	}
	return keys
}

// unreservableKeys is every set AcquireNext must not reserve this cycle: the
// sets Sync skipped and the sets whose poll failed in the last sweep.
func (m *SessionManager) unreservableKeys() []SetKey {
	keys := m.degradedKeys()
	for key := range m.pollFailed {
		if _, degraded := m.degraded[key]; !degraded {
			keys = append(keys, key)
		}
	}
	return keys
}

func (m *SessionManager) completeSessions(ctx context.Context) ([]SetKey, map[SetKey]*QueueSession, error) {
	keys, err := m.store.managedSessionKeys(ctx)
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].RepositoryID == keys[j].RepositoryID {
			return keys[i].ScaleSetID < keys[j].ScaleSetID
		}
		return keys[i].RepositoryID < keys[j].RepositoryID
	})
	handles := make(map[SetKey]*QueueSession, len(keys))
	healthy := keys[:0:0]
	for _, key := range keys {
		if _, skipped := m.degraded[key]; skipped {
			continue
		}
		handle := m.sessions[key]
		if handle == nil || handle.closed || !handle.matchesSDK() {
			return nil, nil, fmt.Errorf("session manager contains a missing or fenced handle: %w", ErrRestartRequired)
		}
		handles[key] = handle
		healthy = append(healthy, key)
	}
	return healthy, handles, nil
}

// PollSweep advertises capacity once to every active repository session. A
// failed repository does not prevent zero-capacity lifecycle polling elsewhere;
// callers receive all completed results, the demand this sweep deferred, and
// the joined diagnostic.
func (m *SessionManager) PollSweep(ctx context.Context) ([]PollResult, int, error) {
	m.mu.Lock()
	keys, handles, err := m.completeSessions(ctx)
	skipped := m.degradedKeys()
	var noREST []SetKey
	for key := range m.pollFailed {
		if _, degraded := m.degraded[key]; !degraded {
			noREST = append(noREST, key)
		}
	}
	m.mu.Unlock()
	if err != nil {
		return nil, 0, err
	}
	plan, err := m.store.PlanAdvertisementsWith(ctx, keys, skipped, noREST)
	if err != nil {
		return nil, 0, err
	}
	results := make([]PollResult, len(keys))
	failures := make([]error, len(keys))
	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			envelope, pollErr := handles[key].Poll(ctx, plan.Capacity[key])
			results[i] = PollResult{Set: key, MaxCapacity: plan.Capacity[key], Envelope: envelope, Outcome: handles[key].LastPollOutcome()}
			if pollErr != nil {
				failures[i] = &SetPollError{Set: key, Err: pollErr}
			}
		}()
	}
	wg.Wait()
	m.mu.Lock()
	m.pollFailed = map[SetKey]bool{}
	for i, key := range keys {
		if failures[i] == nil {
			delete(m.pollFailures, key)
			continue
		}
		m.pollFailures[key]++
		m.pollFailed[key] = true
		slog.Warn("A scale set's poll failed; the other sets' results are still used this cycle.",
			"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID,
			"consecutive_failures", m.pollFailures[key], "error", failures[i])
	}
	m.mu.Unlock()
	return results, plan.Deferred, errors.Join(failures...)
}

// CloseIdleConnections closes idle HTTP connections on every managed queue
// session's transport, without touching session identity or in-flight polls.
// It exists so the close-connections recovery rung can rule out stale pooled
// HTTP/2 keep-alive connections (cause B in the 2026-09-23 investigation)
// before the destructive exit rung restarts the process. Returns how many
// sessions it touched, for the caller's log line.
func (m *SessionManager) CloseIdleConnections() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, session := range m.sessions {
		session.CloseIdleConnections()
	}
	return len(m.sessions)
}

// AcquireNext reserves a physical slot before issuing the one acquisition call
// through that repository's exact session. Any uncertain response remains
// charged by Store and is returned to the lifecycle reconciler.
func (m *SessionManager) AcquireNext(ctx context.Context) (Lease, error) {
	m.mu.Lock()
	_, handles, err := m.completeSessions(ctx)
	skip := m.unreservableKeys()
	m.mu.Unlock()
	if err != nil {
		return Lease{}, err
	}
	lease, err := m.store.ReserveNextExcept(ctx, skip)
	if err != nil {
		return Lease{}, err
	}
	handle := handles[lease.Set]
	if handle == nil {
		return lease, errors.New("reserved lease has no active repository session; reservation retained")
	}
	return lease, handle.Acquire(ctx, lease.ID)
}

// sortSetKeys orders keys by repository, then scale set.
func sortSetKeys(keys []SetKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].RepositoryID == keys[j].RepositoryID {
			return keys[i].ScaleSetID < keys[j].ScaleSetID
		}
		return keys[i].RepositoryID < keys[j].RepositoryID
	})
}
