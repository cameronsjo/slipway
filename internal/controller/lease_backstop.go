package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Exits for lease states that had none (homelab#1095, review findings D7 and
// D9). Each one held a charged slot with nothing to retire it, invisible to
// the deaf check because none of them is provisioning or running.
const (
	// strandedLeaseWindow is how long a lease may sit before provisioning. In
	// the live path every one of these phases lasts a fraction of a cycle:
	// ReserveNext, the broker settle and beginJIT run back to back in one
	// ProvisionNext call. Ten minutes is only ever reached by a lease that
	// call abandoned.
	strandedLeaseWindow = 10 * time.Minute
	// unknownJITSettleWindow is how long after the JIT row was written an
	// empty runner lookup may stand as proof that an uncertain JIT POST
	// created nothing. The POST's own client timeout is about a minute; a
	// registration GitHub did create is listed within seconds of it.
	unknownJITSettleWindow = 15 * time.Minute
)

// ReleaseStrandedLeases releases leases that stopped before provisioning and
// can have created nothing outside this journal.
//
// NO DEADLINE BEFORE PROVISIONING (review D7). ReconcileDeadlines covers a
// provisioning worker's startup and a teardown's cleanup; nothing covered the
// phases before them. A lease stopped there when Transition refused (discovery
// went stale, or the repository's eligibility flipped between ReserveNext and
// AcquireReserved), when the reserved set had no live session, or when
// beginJIT refused and QuarantineBeforeBackendCreate then refused too, because
// it only quarantines a provisioning lease. Every one of those is a slot
// charged forever: ReserveNext never reuses a lease, it always mints a new one.
//
// WHAT MAKES RELEASE SAFE, per phase, and only these:
//   - reserved: AcquireReserved leaves this phase BEFORE any GitHub call, so a
//     reserved lease has made none.
//   - acquire-intent with request id 0, and acquired: the broker path makes no
//     acquisition call (the evidence is the controller's own inference), and
//     GitHub times out an assignment nothing picks up and offers it again, as
//     it does after any other cleanup that drops a claimed job.
//   - In all three, no JIT row and no worker row. beginJIT inserts the JIT row
//     in the same transaction that enters provisioning, before any
//     registration call, and BeginWorkerCreate writes the worker row before
//     Create. No row, so no registration and no worker can exist.
//
// Left alone on purpose: acquire-intent and acquire-unknown with a real request
// id. Their AcquireJobs POST has an uncertain outcome that only GitHub can
// settle, and that path is not live -- every assignment seen in production
// arrives with request id 0.
func (s *Store) ReleaseStrandedLeases(ctx context.Context, now time.Time) ([]string, error) {
	if now.IsZero() {
		return nil, errors.New("stranded-lease reconciliation requires current time")
	}
	now = now.UTC()
	rows, err := s.db.QueryContext(ctx, `SELECT l.id,l.phase,l.runner_request_id,l.repo_id,l.updated FROM leases l
WHERE (l.phase IN ('reserved','acquired') OR (l.phase='acquire-intent' AND l.runner_request_id=0))
AND NOT EXISTS(SELECT 1 FROM jit_transfers j WHERE j.lease_id=l.id)
AND NOT EXISTS(SELECT 1 FROM worker_resources w WHERE w.lease_id=l.id)
ORDER BY l.created,l.id`)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id, updated string
		phase       Phase
		requestID   int64
		repoID      int64
	}
	var candidates []candidate
	for rows.Next() {
		var found candidate
		if err := rows.Scan(&found.id, &found.phase, &found.requestID, &found.repoID, &found.updated); err != nil {
			rows.Close()
			return nil, err
		}
		// Parsed in Go: RFC3339Nano stamps of different widths do not sort as
		// text. An unparseable stamp is left alone rather than guessed at.
		at, err := time.Parse(time.RFC3339Nano, found.updated)
		if err != nil || now.Sub(at) < strandedLeaseWindow {
			continue
		}
		candidates = append(candidates, found)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var released []string
	for _, found := range candidates {
		changed, err := s.releaseStrandedLease(ctx, found.id, found.phase, found.updated, now)
		if err != nil {
			return nil, err
		}
		if !changed {
			continue
		}
		released = append(released, found.id)
		slog.Warn("Released a lease stranded before provisioning; nothing outside the journal existed for it.",
			"lease_id", found.id, "phase", found.phase, "repository_id", found.repoID,
			"runner_request_id", found.requestID, "stranded_since", found.updated)
	}
	return released, nil
}

// releaseStrandedLease releases one stranded lease and hands its job back to
// the offer path, in one transaction.
//
// The selection is repeated in the write, so a lease that moved on since the
// read -- beginJIT raced it, or it was updated -- is untouched.
//
// THE JOB ROW IS DEMOTED WITH IT. ReserveNext moved the job to 'reserved' (and
// the broker settle to 'acquired'), and mergeJobState never moves a job down a
// rank, so a re-offer of the job GitHub sends after the release would be
// ignored and the job never served. A broker job (request id 0) goes to
// 'assigned-stale', the same state a restart gives an assignment it cannot
// vouch for: only a fresh envelope makes it reservable again. A job with a
// real request id is left as it is; its offer is GitHub's to repeat. Either
// way only while no other live lease holds the job.
func (s *Store) releaseStrandedLease(ctx context.Context, leaseID string, phase Phase, updated string, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	stamp := now.Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, `UPDATE leases SET phase='released',diagnostic='stranded before provisioning; released by the pre-provisioning backstop',updated=?
WHERE id=? AND phase=? AND updated=?
AND NOT EXISTS(SELECT 1 FROM jit_transfers j WHERE j.lease_id=leases.id)
AND NOT EXISTS(SELECT 1 FROM worker_resources w WHERE w.lease_id=leases.id)`,
		stamp, leaseID, phase, updated)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed > 1 {
		return false, errors.New("stranded-lease release produced an invalid result")
	}
	if changed == 0 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?,offered=?
WHERE state IN ('reserved','acquired') AND runner_request_id=0
AND EXISTS(SELECT 1 FROM leases l WHERE l.id=? AND l.repo_id=jobs.repo_id AND l.set_id=jobs.set_id AND l.job_key=jobs.job_key)
AND NOT EXISTS(SELECT 1 FROM leases o WHERE o.repo_id=jobs.repo_id AND o.set_id=jobs.set_id AND o.job_key=jobs.job_key AND o.phase!='released')`,
		jobStateAssignedStale, stamp, leaseID); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// unknownJITCreationSettled reports whether an empty runner lookup may now be
// read as "the uncertain JIT POST created nothing".
//
// A 5xx or timeout from GenerateJitRunnerConfig leaves runner_id=0 and phase
// jit-intent. CleanupRegistration then refused on every cycle -- "empty lookup
// cannot settle unknown JIT creation" -- so the lease stayed quarantined for
// good (review D9). The refusal is right for a fresh POST, whose runner may not
// be listed yet. It is not right forever: the runner name is this lease's own
// UUID, so nothing else can create it, and once the POST is long finished an
// empty lookup is the answer.
func unknownJITCreationSettled(record JITRecord, now time.Time) bool {
	created, err := time.Parse(time.RFC3339Nano, record.Created)
	return err == nil && now.Sub(created) >= unknownJITSettleWindow
}

// RetireUnstartedProvisioning moves a provisioning lease that has a JIT row but
// never got a worker row into teardown.
//
// ONE PHASE LATER THAN THE STRANDED LEASES ABOVE, SAME SHAPE (review of
// homelab#1104, finding I1). beginJIT enters provisioning and writes the JIT
// row in one transaction; BeginWorkerCreate writes the worker row a moment
// later. A crash, a SIGKILL or a rollout between the two -- or a failed
// QuarantineBeforeBackendCreate write after Prepare errored -- leaves a lease
// that every exit skips: ReleaseStrandedLeases because the JIT row exists, and
// the startup deadline, the unused and terminated retirements and the vanish
// path because each of them starts from a worker row. The slot stayed charged
// and the deaf watchdog stayed off.
//
// WHY THIS IS SAFE. Create is only ever called after the worker row commits,
// so no worker row means no worker. The lease goes to cleanup-pending with its
// JIT revoked, exactly as DeferProvisionForCapacity leaves a lease whose
// worker was never created, and from there the existing path finishes it:
// PendingRegistrationCleanup's no-worker branch removes the registration (or,
// for an uncertain JIT POST, settles it after unknownJITSettleWindow), and
// only then does PendingFinishedCleanup release the slot. Nothing here
// releases anything.
//
// Not through the breaker: this is the controller's own interruption, not a
// host or image fault, and a rollout must not count toward tripping it.
func (s *Store) RetireUnstartedProvisioning(ctx context.Context, now time.Time) ([]string, error) {
	if now.IsZero() {
		return nil, errors.New("unstarted-provisioning reconciliation requires current time")
	}
	now = now.UTC()
	rows, err := s.db.QueryContext(ctx, `SELECT l.id,l.updated,j.created FROM leases l JOIN jit_transfers j ON j.lease_id=l.id
WHERE l.phase='provisioning'
AND NOT EXISTS(SELECT 1 FROM worker_resources w WHERE w.lease_id=l.id)
ORDER BY l.created,l.id`)
	if err != nil {
		return nil, err
	}
	type candidate struct{ id, updated string }
	var candidates []candidate
	for rows.Next() {
		var found candidate
		var created string
		if err := rows.Scan(&found.id, &found.updated, &created); err != nil {
			rows.Close()
			return nil, err
		}
		at, err := time.Parse(time.RFC3339Nano, created)
		if err != nil || now.Sub(at) < strandedLeaseWindow {
			continue
		}
		candidates = append(candidates, found)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var retired []string
	for _, found := range candidates {
		changed, err := s.retireUnstartedProvisioning(ctx, found.id, found.updated, now)
		if err != nil {
			return nil, err
		}
		if !changed {
			continue
		}
		retired = append(retired, found.id)
		slog.Warn("Moved a provisioning lease that never got a worker into teardown; its registration is removed before the slot is released.",
			"lease_id", found.id, "provisioning_since", found.updated)
	}
	return retired, nil
}

func (s *Store) retireUnstartedProvisioning(ctx context.Context, leaseID, updated string, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	stamp := now.Format(time.RFC3339Nano)
	const diagnostic = "provisioning stopped before a worker was created; retired by the unstarted-provisioning backstop"
	result, err := tx.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic=?,updated=?
WHERE id=? AND phase='provisioning' AND updated=?
AND NOT EXISTS(SELECT 1 FROM worker_resources w WHERE w.lease_id=leases.id)`, diagnostic, stamp, leaseID, updated)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed > 1 {
		return false, errors.New("unstarted-provisioning retirement produced an invalid result")
	}
	if changed == 0 {
		return false, nil
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE jit_transfers SET revoked=1,diagnostic=?,updated=? WHERE lease_id=?`,
		diagnostic, stamp, leaseID)); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
