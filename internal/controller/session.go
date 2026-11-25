package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
)

// createIntentRetryAfter is how long a create-intent left by an uncertain
// session POST waits, since it was last touched, before one fresh create is
// tried for it (homelab#1235). Neither GitHub nor the SDK documents how long an
// unpolled session lives, so the bound is conservative: sixty times the 60 s
// open bound. A session the lost POST did create answers the retry with a 409.
// Every attempt restamps the intent first, and only a 201 ends it, so a 409 or
// a retry that is itself uncertain waits a full window again.
const createIntentRetryAfter = time.Hour

// createIntentRetryDue reports whether a create-intent last touched at updated
// may be retried at now. A zero stamp is never due.
func createIntentRetryDue(updated, now time.Time) bool {
	return !updated.IsZero() && !now.Before(updated.Add(createIntentRetryAfter))
}

// closeIntentRetryAfter is the same bound for the close side: a close-intent
// whose session DELETE outcome was lost gets one fresh DELETE once it has been
// idle this long (homelab#1167, homelab#1235). Every attempt restamps the
// intent first, so a DELETE that fails again waits a full window, never a
// cycle.
const closeIntentRetryAfter = createIntentRetryAfter

// closeIntentRetryDue reports whether a close-intent last touched at updated
// may be retried at now. A zero stamp is never due.
func closeIntentRetryDue(updated, now time.Time) bool {
	return !updated.IsZero() && !now.Before(updated.Add(closeIntentRetryAfter))
}

// RetryCloseIntent builds the repository-bound SDK client the same way
// ResumeQueueSession does and retries the set's stale close-intent once.
func (s *Store) RetryCloseIntent(ctx context.Context, key SetKey, app *githubapp.App, installation githubapp.Installation) error {
	if app == nil || installation.ID != s.config.InstallationID {
		return errors.New("session App installation differs from journal")
	}
	var repo githubapp.Repository
	err := s.db.QueryRowContext(ctx, `SELECT r.id,r.full_name,r.private FROM repositories r JOIN scale_sets ss ON ss.repo_id=r.id
WHERE r.id=? AND ss.set_id=? AND r.eligible=1`, key.RepositoryID, key.ScaleSetID).Scan(&repo.ID, &repo.FullName, &repo.Private)
	if err != nil {
		return err
	}
	client, err := app.ScaleSetClient(installation, repo)
	if err != nil {
		return err
	}
	return s.retryCloseIntent(ctx, key, client)
}

// retryCloseIntent sends one fresh DELETE for the set's close-intent once it
// is due. It restamps the intent with a compare-and-swap before the request,
// so concurrent callers send at most one DELETE and a failed one waits a full
// window. A 204, or a 404 for this DELETE itself, records the session closed,
// which frees the set for an ordinary create. Anything else keeps the intent.
// The close rules that admitted the intent (drained set, no unreleased lease,
// no unacknowledged message) were enforced when beginClose wrote it.
func (s *Store) retryCloseIntent(ctx context.Context, key SetKey, client *scaleset.Client) error {
	if client == nil {
		return errors.New("missing scale-set client")
	}
	var row closeIntentRow
	if err := s.db.QueryRowContext(ctx, `SELECT owner,session_id,updated FROM sessions WHERE repo_id=? AND set_id=? AND phase='close-intent'`,
		key.RepositoryID, key.ScaleSetID).Scan(&row.owner, &row.sessionID, &row.updated); err != nil {
		return fmt.Errorf("session close outcome is uncertain; its close-intent could not be read: %w", err)
	}
	return s.retryReadCloseIntent(ctx, key, client, row)
}

// closeIntentRow is one read of a close-intent. Two callers that read the same
// row before either claims it hold equal values; the claim's compare-and-swap
// on updated is what lets only one of them send the DELETE.
type closeIntentRow struct {
	owner, sessionID, updated string
}

// retryReadCloseIntent is retryCloseIntent after the read, split out so a test
// can replay two racers that both read the row before either claimed it.
func (s *Store) retryReadCloseIntent(ctx context.Context, key SetKey, client *scaleset.Client, row closeIntentRow) error {
	owner, sessionID, updated := row.owner, row.sessionID, row.updated
	id, err := uuid.Parse(sessionID)
	if err != nil || id == uuid.Nil {
		return errors.New("session close outcome is uncertain; its close-intent has no session UUID to DELETE")
	}
	stamp, err := time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return fmt.Errorf("close-intent timestamp %q is unreadable; retain intent: %w", updated, err)
	}
	now := time.Now().UTC()
	if !closeIntentRetryDue(stamp, now) {
		return fmt.Errorf("session close outcome is uncertain; its close-intent is retried with one fresh DELETE after %s idle, at %s",
			closeIntentRetryAfter, stamp.Add(closeIntentRetryAfter).UTC().Format(time.RFC3339))
	}
	if err := changedOne(s.db.ExecContext(ctx, `UPDATE sessions SET updated=? WHERE owner=? AND phase='close-intent' AND updated=?`,
		timestamp(), owner, updated)); err != nil {
		return errors.New("session close outcome is uncertain; its close-intent retry was fenced by a concurrent claim")
	}
	q := &QueueSession{store: s, key: key, owner: owner, identity: sessionIdentity{id: id}}
	q.http = q.httpClient()
	defer q.http.HTTPClient.CloseIdleConnections()
	deleteErr := client.DeleteKnownMessageSession(ctx, key.ScaleSetID, id, scaleset.WithRetryableHTTPClint(q.http))
	logRetry := func(outcome string, cause error) {
		slog.Warn("Retried a stale session close-intent with one fresh DELETE.",
			"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID, "owner", owner, "session_id", id.String(),
			"intent_idle", now.Sub(stamp).Round(time.Second).String(), "outcome", outcome,
			"next_retry_after", closeIntentRetryAfter.String(), "error", cause)
	}
	outcome := "deleted-closed"
	if deleteErr != nil {
		// Only this DELETE's own 404 proves the session is gone; a 404 on the
		// admin-token refresh before it means the DELETE was never sent.
		if !errors.Is(deleteErr, scaleset.NotFoundError) || q.closeStatus.Load() != http.StatusNotFound {
			logRetry("uncertain-intent-kept", deleteErr)
			return fmt.Errorf("session close outcome is uncertain; the close-intent retry failed, intent kept, next retry after %s", closeIntentRetryAfter)
		}
		outcome = "already-gone-closed"
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if err := changedOne(s.db.ExecContext(saveCtx, `UPDATE sessions SET phase='closed',updated=? WHERE owner=? AND phase='close-intent'`, timestamp(), owner)); err != nil {
		logRetry("closed-commit-failed-intent-kept", err)
		return errors.New("session close outcome is uncertain; the DELETE succeeded but the close could not be recorded, intent kept")
	}
	logRetry(outcome, nil)
	return nil
}

// SessionRecord contains reconciliation identities, never queue URLs or tokens.
// An intent survives an uncertain API outcome and blocks replacement creation
// until createIntentRetryAfter has passed since Updated.
type SessionRecord struct {
	Owner       string
	Set         SetKey
	SessionID   string
	QueueSHA256 [32]byte
	Phase       string
	Updated     time.Time
}

func (s *Store) PendingSessions(ctx context.Context) ([]SessionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT owner,repo_id,set_id,session_id,queue_sha256,phase,updated FROM sessions WHERE phase!='closed' ORDER BY created,owner`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []SessionRecord
	for rows.Next() {
		var record SessionRecord
		var fingerprint []byte
		var updated string
		if err := rows.Scan(&record.Owner, &record.Set.RepositoryID, &record.Set.ScaleSetID, &record.SessionID, &fingerprint, &record.Phase, &updated); err != nil {
			return nil, err
		}
		if len(fingerprint) != 0 && len(fingerprint) != len(record.QueueSHA256) {
			return nil, errors.New("invalid journaled queue fingerprint")
		}
		stamp, err := time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			return nil, fmt.Errorf("invalid journaled session timestamp %q for owner %s: %w", updated, record.Owner, err)
		}
		record.Updated = stamp
		copy(record.QueueSHA256[:], fingerprint)
		records = append(records, record)
	}
	return records, rows.Err()
}

// QueueSession is the only controller-facing SDK session handle. Its SDK client
// stays private so typed messages cannot bypass durable receipt or scope checks.
type QueueSession struct {
	store             *Store
	key               SetKey
	owner             string
	queueOrigins      []string
	sdk               *scaleset.MessageSessionClient
	identity          sessionIdentity // Set during construction; immutable after publication.
	queueSHA256       [32]byte        // Persisted routing identity; the queue URL remains transient.
	initialStatistics scaleset.RunnerScaleSetStatistic
	http              *retryablehttp.Client
	fence             responseFence
	gate              sync.RWMutex // Close waits for in-flight polls/acquisitions.
	pollMu            sync.Mutex   // One cursor/ACK stream; acquisition can run during a poll.
	closed            bool         // Includes an uncertain close, which must not be replayed.
	lastOutcome       PollOutcome  // Set by Poll before it returns; see LastPollOutcome.
	// closeStatus is the HTTP status of this session's own DELETE, recorded by
	// captureResponse; zero when no DELETE response arrived. Close reads it to
	// tell GitHub's 404 for the DELETE from a 404 on the token refresh that
	// runs before it, which means the DELETE was never sent.
	closeStatus atomic.Int32
}

// OpenQueueSession constructs the parent client from the journal's repository,
// not from an opaque client supplied by the caller. It does perform a session
// POST; a production orchestrator must call it only after deployment approval.
func (s *Store) OpenQueueSession(ctx context.Context, key SetKey, app *githubapp.App, installation githubapp.Installation) (*QueueSession, error) {
	if app == nil || installation.ID != s.config.InstallationID {
		return nil, errors.New("session App installation differs from journal")
	}
	var repo githubapp.Repository
	err := s.db.QueryRowContext(ctx, `SELECT r.id,r.full_name,r.private FROM repositories r JOIN scale_sets ss ON ss.repo_id=r.id
WHERE r.id=? AND ss.set_id=? AND r.eligible=1`, key.RepositoryID, key.ScaleSetID).Scan(&repo.ID, &repo.FullName, &repo.Private)
	if err != nil {
		return nil, err
	}
	client, err := app.ScaleSetClient(installation, repo)
	if err != nil {
		return nil, err
	}
	return s.openQueueSession(ctx, key, client)
}

// ResumeQueueSession reconstructs the native SDK handle for one active,
// journaled session. It refreshes the same UUID and never creates a replacement.
// Complete physical inventory is required before the recovered handle can poll,
// acknowledge, or acquire work.
func (s *Store) ResumeQueueSession(ctx context.Context, key SetKey, app *githubapp.App, installation githubapp.Installation) (*QueueSession, error) {
	if app == nil || installation.ID != s.config.InstallationID {
		return nil, errors.New("session App installation differs from journal")
	}
	var repo githubapp.Repository
	err := s.db.QueryRowContext(ctx, `SELECT r.id,r.full_name,r.private FROM repositories r JOIN scale_sets ss ON ss.repo_id=r.id
WHERE r.id=? AND ss.set_id=? AND r.eligible=1`, key.RepositoryID, key.ScaleSetID).Scan(&repo.ID, &repo.FullName, &repo.Private)
	if err != nil {
		return nil, err
	}
	client, err := app.ScaleSetClient(installation, repo)
	if err != nil {
		return nil, err
	}
	return s.resumeQueueSession(ctx, key, client)
}

// openQueueSession also permits an actual SDK client pointed at a local test
// server. Production callers must use the repository-bound public constructor.
func (s *Store) openQueueSession(ctx context.Context, key SetKey, client *scaleset.Client, testOrigins ...string) (*QueueSession, error) {
	if client == nil {
		return nil, errors.New("missing scale-set client")
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return nil, err
	}
	if err := s.managedCurrent(ctx, key); err != nil {
		return nil, err
	}
	origins := githubQueueOrigins
	if len(testOrigins) > 0 {
		origins = testOrigins
	}
	retryOwner, intentAge, err := s.claimStaleCreateIntent(ctx, key)
	if err != nil {
		return nil, err
	}
	retrying := retryOwner != ""
	q := &QueueSession{store: s, key: key, owner: retryOwner, queueOrigins: origins}
	if !retrying {
		q.owner = "slipway-" + s.config.Platform + "-" + uuid.NewString()
		// Unique open-session index fences concurrent creates and process restarts.
		// The same transaction refuses revoked repositories and unsettled old work.
		err = changedOne(s.db.ExecContext(ctx, `INSERT INTO sessions(owner,repo_id,set_id,phase,created,updated)
SELECT ?,r.id,ss.set_id,'create-intent',?,? FROM repositories r JOIN scale_sets ss ON ss.repo_id=r.id
WHERE r.id=? AND ss.set_id=? AND r.eligible=1 AND NOT EXISTS (
 SELECT 1 FROM leases l WHERE l.repo_id=r.id AND l.set_id=ss.set_id AND l.phase!='released')`,
			q.owner, timestamp(), timestamp(), key.RepositoryID, key.ScaleSetID))
		if err != nil {
			return nil, errors.New("session creation fenced by journal, eligibility, or unsettled work")
		}
	}
	logRetry := func(outcome string, cause error) {
		if !retrying {
			return
		}
		slog.Warn("Retried a stale session create-intent with one fresh create.",
			"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID, "owner", q.owner,
			"intent_idle", intentAge.Round(time.Second).String(), "outcome", outcome,
			"next_retry_after", createIntentRetryAfter.String(), "error", cause)
	}
	q.http = q.httpClient()
	q.sdk, err = client.MessageSessionClient(ctx, key.ScaleSetID, q.owner, scaleset.WithRetryableHTTPClint(q.http))
	if errors.Is(err, scaleset.ConflictError) && q.identity.id == uuid.Nil && retrying {
		// A 409 on the retry of a stale intent means GitHub still holds a
		// session for this set, most likely the one the lost POST created (or
		// a 201 whose commit failed). Dropping the intent here would hand the
		// set to the ordinary open path, which would POST, get 409 and drop
		// again on every cycle for as long as GitHub keeps that session.
		// Keeping the intent, already restamped by the claim, holds the set to
		// one POST per createIntentRetryAfter instead.
		q.http.HTTPClient.CloseIdleConnections()
		logRetry("refused-409-session-held", err)
		return nil, fmt.Errorf("session creation refused with 409 on the create-intent retry: GitHub still holds a session for this set; intent kept, next retry after %s", createIntentRetryAfter)
	}
	if errors.Is(err, scaleset.ConflictError) && q.identity.id == uuid.Nil {
		// A 409 is a definite refusal, not an uncertain outcome: this client
		// sends one POST (RetryMax 0, redirects refused) and GitHub answered it
		// without creating a session, typically because it still holds one for
		// this set just after a deaf-exit rotation closed it (homelab#1138).
		// Keeping the intent would skip the set forever, since Sync never
		// replays a create-intent; dropping it lets the next Sync try again.
		q.http.HTTPClient.CloseIdleConnections()
		dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
		defer cancel()
		if dropErr := changedOne(s.db.ExecContext(dropCtx, `DELETE FROM sessions WHERE owner=? AND phase='create-intent' AND session_id=''`, q.owner)); dropErr != nil {
			return nil, errors.New("session creation refused with 409, but its intent could not be dropped; retain intent")
		}
		return nil, errors.New("session creation refused with 409; nothing was created and the set is retried next cycle")
	}
	if err != nil || q.identity.id == uuid.Nil || !q.matchesSDK() {
		q.http.HTTPClient.CloseIdleConnections()
		logRetry("uncertain-intent-kept", err)
		return nil, errors.New("session creation outcome uncertain; retain intent and reconcile before replacement")
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if err := q.commitSession(saveCtx); err != nil {
		q.http.HTTPClient.CloseIdleConnections()
		logRetry("created-commit-failed-intent-kept", err)
		return nil, errors.New("session identity could not be committed; retain creation intent")
	}
	logRetry("created-active", nil)
	return q, nil
}

// claimStaleCreateIntent finds the set's create-intent, if any. With none it
// returns an empty owner, and the caller creates a new intent as usual. With
// one that is not yet due it refuses, so an uncertain create is never replayed
// early. With one that is due it restamps the intent before any POST, under
// the same fences the original INSERT applied, and returns its owner and how
// long it had been idle; the caller then sends exactly one fresh create.
func (s *Store) claimStaleCreateIntent(ctx context.Context, key SetKey) (string, time.Duration, error) {
	var owner, updated string
	err := s.db.QueryRowContext(ctx, `SELECT owner,updated FROM sessions WHERE repo_id=? AND set_id=? AND phase='create-intent' AND session_id=''`,
		key.RepositoryID, key.ScaleSetID).Scan(&owner, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	stamp, err := time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return "", 0, fmt.Errorf("create-intent timestamp %q is unreadable; retain intent: %w", updated, err)
	}
	now := time.Now().UTC()
	if !createIntentRetryDue(stamp, now) {
		return "", 0, fmt.Errorf("session creation outcome is uncertain; its create-intent is retried with one fresh create after %s idle, at %s",
			createIntentRetryAfter, stamp.Add(createIntentRetryAfter).UTC().Format(time.RFC3339))
	}
	// Compare-and-swap on the old stamp: a concurrent claim changes it, so only
	// one caller sends the retry.
	if err := changedOne(s.db.ExecContext(ctx, `UPDATE sessions SET updated=?
WHERE owner=? AND phase='create-intent' AND session_id='' AND updated=?
AND EXISTS(SELECT 1 FROM repositories r JOIN scale_sets ss ON ss.repo_id=r.id
 WHERE r.id=sessions.repo_id AND ss.set_id=sessions.set_id AND r.eligible=1)
AND NOT EXISTS(SELECT 1 FROM leases l WHERE l.repo_id=sessions.repo_id AND l.set_id=sessions.set_id AND l.phase!='released')`,
		timestamp(), owner, updated)); err != nil {
		return "", 0, errors.New("session creation outcome is uncertain; its create-intent retry was fenced by journal, eligibility, or unsettled work")
	}
	return owner, now.Sub(stamp), nil
}

// resumeQueueSession permits the local SDK/TLS fixture used by controller tests.
// Production callers must use ResumeQueueSession so App authority stays bound to
// the selected installation and repository recorded in the journal.
func (s *Store) resumeQueueSession(ctx context.Context, key SetKey, client *scaleset.Client, testOrigins ...string) (*QueueSession, error) {
	if client == nil {
		return nil, errors.New("missing scale-set client")
	}
	s.mu.Lock()
	ready := s.ready
	s.mu.Unlock()
	if !ready {
		return nil, ErrInventoryUnknown
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return nil, err
	}
	if err := s.managedCurrent(ctx, key); err != nil {
		return nil, err
	}
	var record SessionRecord
	var fingerprint []byte
	err := s.db.QueryRowContext(ctx, `SELECT owner,repo_id,set_id,session_id,queue_sha256,phase FROM sessions
WHERE repo_id=? AND set_id=? AND phase='active'`, key.RepositoryID, key.ScaleSetID).Scan(
		&record.Owner, &record.Set.RepositoryID, &record.Set.ScaleSetID, &record.SessionID, &fingerprint, &record.Phase)
	if err != nil {
		return nil, errors.New("no resumable active session")
	}
	if len(fingerprint) != len(record.QueueSHA256) {
		return nil, errors.New("active session lacks its routing fingerprint; retain recovery fence")
	}
	copy(record.QueueSHA256[:], fingerprint)
	id, err := uuid.Parse(record.SessionID)
	if err != nil || id == uuid.Nil || record.Owner == "" || record.Set != key || record.QueueSHA256 == ([32]byte{}) {
		return nil, errors.New("active session journal identity is incomplete; retain recovery fence")
	}
	origins := githubQueueOrigins
	if len(testOrigins) > 0 {
		origins = testOrigins
	}
	q := &QueueSession{store: s, key: key, owner: record.Owner, queueOrigins: origins, identity: sessionIdentity{id: id}, queueSHA256: record.QueueSHA256}
	q.http = q.httpClient()
	q.sdk, err = client.ResumeMessageSessionClient(ctx, scaleset.SessionResumeIdentity{
		ScaleSetID:  key.ScaleSetID,
		Owner:       record.Owner,
		SessionID:   id,
		QueueSHA256: record.QueueSHA256,
	}, scaleset.WithRetryableHTTPClint(q.http))
	if err != nil || !q.matchesSDK() {
		q.http.HTTPClient.CloseIdleConnections()
		return nil, errors.New("known session recovery outcome uncertain; retain identity and do not create a replacement")
	}
	if err := q.replayPendingAcknowledgments(ctx); err != nil {
		q.http.HTTPClient.CloseIdleConnections()
		return nil, err
	}
	return q, nil
}

func (q *QueueSession) replayPendingAcknowledgments(ctx context.Context) error {
	rows, err := q.store.db.QueryContext(ctx, `SELECT message_id FROM messages
WHERE repo_id=? AND set_id=? AND session=? AND acked=0 ORDER BY rowid`,
		q.key.RepositoryID, q.key.ScaleSetID, q.identity.id.String())
	if err != nil {
		return err
	}
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := q.active(ctx); err != nil {
			return err
		}
		if err := q.sdk.DeleteMessage(ctx, id); err != nil {
			return errors.New("pending acknowledgment outcome remains uncertain; retain active session")
		}
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
		err := q.store.MarkAcknowledged(saveCtx, q.key, q.identity.id, id)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// CloseIdleConnections closes idle HTTP connections on this session's
// transport without touching session identity, in-flight polls, or the
// durable journal. It is safe to call while the session is otherwise idle;
// callers must not use it as a substitute for Close.
func (q *QueueSession) CloseIdleConnections() {
	q.http.HTTPClient.CloseIdleConnections()
}

// LastPollOutcome is what GitHub's queue answered the most recent completed
// Poll call: an empty 202, a delivered message, an SDK-silent 401 refresh, or
// anything else Poll treated as a failure. Callers must read it only after
// that Poll call has returned -- PollSweep does, once its wg.Wait() confirms
// every goroutine finished -- because pollMu no longer protects it once Poll
// has returned.
func (q *QueueSession) LastPollOutcome() PollOutcome {
	return q.lastOutcome
}

func (q *QueueSession) active(ctx context.Context) error {
	if q.closed || !q.matchesSDK() {
		return errors.New("session closed or identity changed; reconciliation required")
	}
	var active bool
	err := q.store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions WHERE owner=? AND repo_id=? AND set_id=? AND session_id=? AND phase='active')`,
		q.owner, q.key.RepositoryID, q.key.ScaleSetID, q.identity.id.String()).Scan(&active)
	if err != nil {
		return err
	}
	if !active {
		return errors.New("session is not durably active")
	}
	return nil
}

func (q *QueueSession) commitSession(ctx context.Context) error {
	tx, err := q.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := changedOne(tx.ExecContext(ctx, `UPDATE sessions SET session_id=?,queue_sha256=?,phase='active',updated=? WHERE owner=? AND phase='create-intent'`,
		q.identity.id.String(), q.queueSHA256[:], timestamp(), q.owner)); err != nil {
		return err
	}
	stats, _ := json.Marshal(q.initialStatistics)
	if err := changedOne(tx.ExecContext(ctx, `UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(stats), timestamp(), q.key.RepositoryID, q.key.ScaleSetID)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) recordStatistics(ctx context.Context, key SetKey, stats scaleset.RunnerScaleSetStatistic) error {
	if err := (Envelope{MessageID: 1, Statistics: stats}).validate(); err != nil {
		return err
	}
	data, _ := json.Marshal(stats)
	return changedOne(s.db.ExecContext(ctx, `UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(data), timestamp(), key.RepositoryID, key.ScaleSetID))
}

// Acquire accepts a reserved lease, never arbitrary request IDs. Store intent
// precedes the single SDK operation, including its documented 401 refresh path.
func (q *QueueSession) Acquire(ctx context.Context, leaseID string) error {
	q.gate.RLock()
	defer q.gate.RUnlock()
	if err := q.active(ctx); err != nil {
		return err
	}
	lease, err := q.store.Lease(ctx, leaseID)
	if err != nil {
		return err
	}
	if lease.Set != q.key {
		return errors.New("lease belongs to another repository or scale-set incarnation")
	}
	return q.store.AcquireReserved(ctx, leaseID, q.sdk)
}

// Close requires a drained set. An uncertain DELETE retains close-intent and
// cannot be retried through this handle. The known-session recovery path only
// resumes active sessions; close-intent reconciliation remains separate.
//
// A 404 answering the DELETE itself is not uncertain: GitHub no longer has this
// session UUID, which is the state the DELETE was asking for (homelab#1138). It is recorded closed like a
// 204. Every other failure, a timeout included, may or may not have reached
// GitHub and keeps the intent.
func (q *QueueSession) Close(ctx context.Context) error {
	q.gate.Lock()
	defer q.gate.Unlock()
	if err := q.active(ctx); err != nil {
		return err
	}
	if err := q.beginClose(ctx); err != nil {
		return err
	}
	q.closed = true
	defer q.http.HTTPClient.CloseIdleConnections()
	q.closeStatus.Store(0)
	if err := q.sdk.Close(ctx); err != nil {
		// Only the DELETE's own 404 proves the session is gone. The admin token
		// refresh runs first, through the parent client, and its 404 (the App
		// lost the repository) also matches NotFoundError while the DELETE was
		// never sent.
		if !errors.Is(err, scaleset.NotFoundError) || q.closeStatus.Load() != http.StatusNotFound {
			return errors.New("session close outcome uncertain; retain intent and reconcile")
		}
		slog.Info("Session DELETE answered 404; GitHub no longer has the session, so it is recorded closed.",
			"repository_id", q.key.RepositoryID, "scale_set_id", q.key.ScaleSetID)
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	return changedOne(q.store.db.ExecContext(saveCtx, `UPDATE sessions SET phase='closed',updated=? WHERE owner=? AND phase='close-intent'`, timestamp(), q.owner))
}

func (q *QueueSession) beginClose(ctx context.Context) error {
	// ReserveNext shares this lock; after commit its session-phase filter blocks
	// reservations until close is settled. Old offers are invalidated atomically.
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	tx, err := q.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var statsJSON string
	if err := tx.QueryRowContext(ctx, `SELECT statistics FROM scale_sets WHERE repo_id=? AND set_id=?`, q.key.RepositoryID, q.key.ScaleSetID).Scan(&statsJSON); err != nil {
		return err
	}
	var stats *scaleset.RunnerScaleSetStatistic
	if err := json.Unmarshal([]byte(statsJSON), &stats); err != nil || stats == nil || stats.TotalAcquiredJobs != 0 || stats.TotalAssignedJobs != 0 || stats.TotalRunningJobs != 0 {
		return errors.New("session still has assigned work or lacks a demand observation")
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE sessions SET phase='close-intent',updated=? WHERE owner=? AND phase='active'
AND NOT EXISTS(SELECT 1 FROM leases WHERE repo_id=? AND set_id=? AND phase!='released')
AND NOT EXISTS(SELECT 1 FROM messages WHERE repo_id=? AND set_id=? AND acked=0)`,
		timestamp(), q.owner, q.key.RepositoryID, q.key.ScaleSetID, q.key.RepositoryID, q.key.ScaleSetID)); err != nil {
		return errors.New("session cannot close with unsettled reservations or acknowledgments")
	}
	// An offer this session made is not reservable once it closes, but the job
	// behind it may still be queued at GitHub, and the replacement session is
	// how it gets offered again (homelab#1138). 'unavailable' would lose it:
	// mergeJobState only lets a later event through when it outranks the
	// previous one, and a re-offer repeats the same JobAvailable or JobAssigned.
	// So offered rows go to 'assigned-stale', the journal's existing state for
	// an offer it cannot vouch for: never reservable, counted as demand while
	// young, and outranked by any fresh offer (see jobStateRank). A row already
	// stale keeps its original stamp, as distrustResumedAssignments does. Every
	// other row still becomes 'unavailable'.
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET
 offered=CASE WHEN state IN ('queued','assigned') THEN ? ELSE offered END,
 state=CASE WHEN state IN ('queued','assigned') THEN ? ELSE 'unavailable' END
WHERE repo_id=? AND set_id=? AND state!=?`,
		timestamp(), jobStateAssignedStale, q.key.RepositoryID, q.key.ScaleSetID, jobStateAssignedStale); err != nil {
		return err
	}
	return tx.Commit()
}
