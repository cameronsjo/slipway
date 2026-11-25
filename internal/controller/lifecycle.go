package controller

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"log/slog"
	"net/url"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

const (
	workerStartupTimeout   = 10 * time.Minute
	workerCleanupTimeout   = 2 * time.Minute
	unusedWorkerTimeout    = 5 * time.Minute
	provisionFailureWindow = 15 * time.Minute
	// provisionFailureThreshold is how many provisioning_failures rows inside
	// provisionFailureWindow trip the pool-wide breaker. Daemon.provision caps
	// its fan-out below it, so one pass alone cannot trip it.
	provisionFailureThreshold = 3
	// staleAssignmentWindow is how long a broker assignment inherited from a
	// previous process may keep counting as demand while it waits to be
	// re-confirmed. Same five minutes as unusedWorkerTimeout, and for the same
	// reason: it is this pool's bound on how long an unproven claim may hold a
	// resource. GitHub retries an unclaimed assignment about three times, so a
	// job that is genuinely still live re-announces itself well inside it.
	staleAssignmentWindow = unusedWorkerTimeout
)

type DeadlineAction struct {
	LeaseID string
	Kind    string
}

type WorkerRecord struct {
	LeaseID         string
	ResourceID      string
	Phase           string
	Created         string
	StartupDeadline string
	DestroyStarted  string
	CleanupDeadline string
	Diagnostic      string
	Updated         string
}

const workerColumns = `lease_id,resource_id,phase,created,startup_deadline,destroy_started,cleanup_deadline,diagnostic,updated`

func readWorker(row interface{ Scan(...any) error }) (WorkerRecord, error) {
	var record WorkerRecord
	err := row.Scan(&record.LeaseID, &record.ResourceID, &record.Phase, &record.Created,
		&record.StartupDeadline, &record.DestroyStarted, &record.CleanupDeadline,
		&record.Diagnostic, &record.Updated)
	return record, err
}

func (s *Store) Worker(ctx context.Context, leaseID string) (WorkerRecord, error) {
	return readWorker(s.db.QueryRowContext(ctx, `SELECT `+workerColumns+` FROM worker_resources WHERE lease_id=?`, leaseID))
}

func (s *Store) Workers(ctx context.Context) ([]WorkerRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+workerColumns+` FROM worker_resources ORDER BY created,lease_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []WorkerRecord
	for rows.Next() {
		record, err := readWorker(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func validBootstrapOrigin(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && (u.Path == "" || u.Path == "/")
}

// BeginWorkerCreate persists the exact platform resource identity before the
// adapter's single Create call. It requires a ready, unconsumed JIT transfer;
// neither the bootstrap capability nor its public origin enters SQLite.
func (s *Store) BeginWorkerCreate(ctx context.Context, leaseID, bootstrapURL string, token bootstrap.Token) (WorkerCreateIntent, error) {
	var intent WorkerCreateIntent
	if !validBootstrapOrigin(bootstrapURL) {
		return intent, errors.New("worker creation requires an HTTPS bootstrap origin")
	}
	if _, err := bootstrap.ParseToken(token.Encoded()); err != nil {
		return intent, errors.New("worker creation requires a valid one-use bootstrap capability")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return intent, ErrInventoryUnknown
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return intent, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return intent, err
	}
	defer tx.Rollback()
	lease, err := readLease(tx.QueryRowContext(ctx, `SELECT `+leaseColumns+` FROM leases WHERE id=?`, leaseID))
	if err != nil || lease.Phase != Provisioning {
		return intent, errors.New("worker creation requires a provisioning lease")
	}
	var jitPhase, expires string
	var tokenHash []byte
	if err := tx.QueryRowContext(ctx, `SELECT phase,token_hash,expires FROM jit_transfers WHERE lease_id=? AND revoked=0`, leaseID).
		Scan(&jitPhase, &tokenHash, &expires); err != nil || jitPhase != "ready" {
		return intent, errors.New("worker creation requires a ready JIT transfer")
	}
	now := time.Now().UTC()
	expiresAt, expiryErr := time.Parse(time.RFC3339Nano, expires)
	hash := token.Hash()
	if expiryErr != nil || !now.Before(expiresAt) || len(tokenHash) != len(hash) || subtle.ConstantTimeCompare(tokenHash, hash[:]) != 1 {
		return intent, errors.New("worker creation capability does not match the live JIT transfer")
	}
	deadline := now.Add(workerStartupTimeout)
	if err := changedOne(tx.ExecContext(ctx, `INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
SELECT l.id,l.runner_name,'create-intent',?,?,? FROM leases l JOIN repositories r ON r.id=l.repo_id
WHERE l.id=? AND l.phase='provisioning' AND r.eligible=1 AND EXISTS (
 SELECT 1 FROM managed_sets m WHERE m.repo_id=l.repo_id AND m.phase='active' AND m.remote_id=l.set_id)`,
		now.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), lease.ID)); err != nil {
		return intent, errors.New("worker creation already has a durable outcome or intent")
	}
	if err := tx.Commit(); err != nil {
		return intent, err
	}
	return WorkerCreateIntent{LeaseID: lease.ID, ResourceID: lease.RunnerName, Slot: lease.Slot,
		BootstrapURL: bootstrapURL, BootstrapToken: token, StartupDeadline: deadline}, nil
}

// ConfirmWorkerCreated accepts either the direct Create response or the same
// identity from a later complete inventory. A mismatched response leaves the
// create intent unchanged for reconciliation.
func (s *Store) ConfirmWorkerCreated(ctx context.Context, worker WorkerObservation) error {
	if worker.LeaseID == "" || worker.ResourceID == "" {
		return errors.New("worker observation lacks identity")
	}
	return changedOne(s.db.ExecContext(ctx, `UPDATE worker_resources SET phase='present',diagnostic='',updated=?
WHERE lease_id=? AND resource_id=? AND phase IN ('create-intent','present')`,
		timestamp(), worker.LeaseID, worker.ResourceID))
}

// BeginWorkerDestroy commits deletion intent and the two-minute cleanup deadline
// before the adapter call. It runs once for an exact resource. An uncertain call
// is reconciled through complete process-and-storage inventory, and a worker
// that inventory still lists has the same exact delete re-sent after
// redeleteBackoff (PendingWorkerRedeletes), never through this entry point.
func (s *Store) BeginWorkerDestroy(ctx context.Context, leaseID string) (WorkerObservation, error) {
	var worker WorkerObservation
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return worker, err
	}
	defer tx.Rollback()
	record, err := readWorker(tx.QueryRowContext(ctx, `SELECT `+workerColumns+` FROM worker_resources WHERE lease_id=?`, leaseID))
	if err != nil || (record.Phase != "create-intent" && record.Phase != "present") {
		return worker, errors.New("worker deletion requires one exact unsettled resource")
	}
	now := time.Now().UTC()
	if err := changedOne(tx.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',updated=?
WHERE id=? AND phase IN ('provisioning','running','quarantined','cleanup-pending')`, now.Format(time.RFC3339Nano), leaseID)); err != nil {
		return worker, err
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE worker_resources SET phase='destroy-intent',destroy_started=?,cleanup_deadline=?,updated=?
WHERE lease_id=? AND resource_id=? AND phase IN ('create-intent','present')`, now.Format(time.RFC3339Nano),
		now.Add(workerCleanupTimeout).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), leaseID, record.ResourceID)); err != nil {
		return worker, err
	}
	if err := tx.Commit(); err != nil {
		return worker, err
	}
	return WorkerObservation{LeaseID: leaseID, ResourceID: record.ResourceID}, nil
}

// ConfirmWorkerAbsent records only the backend half of cleanup evidence. GitHub
// registration absence and acquisition settlement remain separately required by
// FinishCleanup, so this operation cannot release capacity by itself.
func (s *Store) ConfirmWorkerAbsent(ctx context.Context, absence WorkerAbsence) error {
	// A missing identity is a caller defect, not a worker still tearing down,
	// so it must not match ErrAbsenceNotProven: Maintain skips that sentinel
	// and would retry the defect forever without ever failing a cycle.
	if absence.LeaseID == "" || absence.ResourceID == "" {
		return errors.New("worker absence requires a lease and resource identity")
	}
	if !absence.ProcessAbsent || !absence.StorageAbsent {
		return ErrAbsenceNotProven
	}
	return changedOne(s.db.ExecContext(ctx, `UPDATE worker_resources SET phase='absent',diagnostic='',updated=?
WHERE lease_id=? AND resource_id=? AND phase='destroy-intent'`, timestamp(), absence.LeaseID, absence.ResourceID))
}

func (s *Store) WorkerAbsent(ctx context.Context, leaseID string) (bool, error) {
	// NO WORKER ROW MEANS ABSENT, the same reading PendingFinishedCleanup's
	// LEFT JOIN takes. This returned sql.ErrNoRows for a lease fenced before
	// its worker row existed, so FinishWorkerCleanup failed and the slot stayed
	// charged forever -- the INNER JOIN's defect one layer down, and fixing
	// only the query would have moved the dead end rather than removed it.
	// The controller never wrote a create intent for such a lease, so nothing
	// was ever created; that is the strongest absence available.
	var absent bool
	err := s.db.QueryRowContext(ctx, `SELECT phase='absent' FROM worker_resources WHERE lease_id=?`, leaseID).Scan(&absent)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	return absent, err
}

// QuarantineProvisionFailure records a backend failure and keeps its exact
// lease charged. Three failures in fifteen minutes suppress new capacity for
// fifteen minutes while inventory reconciliation and cleanup remain available.
func (s *Store) QuarantineProvisionFailure(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	return s.quarantineCreate(ctx, leaseID, diagnostic, now, true)
}

// QuarantineInterruptedCreate is QuarantineProvisionFailure for a create the
// controller's own shutdown cut short. The lease and worker take the same
// quarantine, so Maintain retires the worker and deletes the registration
// exactly as it would after a failure, but no provisioning_failures row is
// written: a restart during a burst is not a broken backend, and those rows
// outlive the process, so four of them would hand the next pod a tripped
// breaker.
func (s *Store) QuarantineInterruptedCreate(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	return s.quarantineCreate(ctx, leaseID, diagnostic, now, false)
}

func (s *Store) quarantineCreate(ctx context.Context, leaseID, diagnostic string, now time.Time, countFailure bool) error {
	if now.IsZero() || diagnostic == "" {
		return errors.New("provisioning failure requires time and safe diagnostic")
	}
	now = now.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := changedOne(tx.ExecContext(ctx, `UPDATE leases SET phase='quarantined',diagnostic=?,updated=?
WHERE id=? AND phase='provisioning'`, diagnostic, now.Format(time.RFC3339Nano), leaseID)); err != nil {
		return err
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE worker_resources SET diagnostic=?,updated=?
WHERE lease_id=? AND phase IN ('create-intent','present')`, diagnostic, now.Format(time.RFC3339Nano), leaseID)); err != nil {
		return err
	}
	if !countFailure {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO provisioning_failures(lease_id,occurred,diagnostic) VALUES(?,?,?)`,
		leaseID, now.Format(time.RFC3339Nano), diagnostic); err != nil {
		return err
	}
	var recent int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM provisioning_failures WHERE occurred>=? AND occurred<=?`,
		now.Add(-provisionFailureWindow).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)).Scan(&recent); err != nil {
		return err
	}
	quarantinedAdmission := -1
	if recent >= provisionFailureThreshold {
		until := now.Add(provisionFailureWindow).Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `UPDATE platform SET provisioning_quarantine_until=CASE
WHEN provisioning_quarantine_until>? THEN provisioning_quarantine_until ELSE ? END WHERE id=1`, until, until); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&quarantinedAdmission); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if quarantinedAdmission >= 0 {
		s.admission = quarantinedAdmission
	}
	return nil
}

// QuarantineBeforeBackendCreate records a cleanup obligation after JIT work has
// started but before the platform Create call. With no worker intent, physical
// absence follows from the controller's durable ordering; a committed worker
// intent remains subject to normal backend retirement.
func (s *Store) QuarantineBeforeBackendCreate(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	return s.quarantineBeforeCreate(ctx, leaseID, diagnostic, now, true)
}

// QuarantineInterruptedBeforeBackendCreate is QuarantineBeforeBackendCreate
// for JIT work the controller's own shutdown cut short: the same fence and
// transfer revocation, without the provisioning_failures row
// (QuarantineInterruptedCreate says why).
func (s *Store) QuarantineInterruptedBeforeBackendCreate(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	return s.quarantineBeforeCreate(ctx, leaseID, diagnostic, now, false)
}

func (s *Store) quarantineBeforeCreate(ctx context.Context, leaseID, diagnostic string, now time.Time, countFailure bool) error {
	if now.IsZero() || diagnostic == "" {
		return errors.New("pre-create failure requires time and safe diagnostic")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stamp := now.UTC().Format(time.RFC3339Nano)
	// HARD-FAIL, matching the worker-row check further down rather than
	// contradicting it. Returning nil here made a pre-create quarantine a
	// SILENT NO-OP whenever the lease was not provisioning -- which is exactly
	// what happens when JIT preparation fails, because the lease is still
	// `acquired` at that point. The caller joined a nil, reported only the
	// original error, and the lease was never fenced at all.
	result, err := tx.ExecContext(ctx, `UPDATE leases SET phase='quarantined',diagnostic=?,updated=?
WHERE id=? AND phase='provisioning'`, diagnostic, stamp, leaseID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("pre-create quarantine found no provisioning lease to fence")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jit_transfers SET revoked=1,diagnostic=?,updated=? WHERE lease_id=?`, diagnostic, stamp, leaseID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE worker_resources SET diagnostic=?,updated=?
WHERE lease_id=? AND phase IN ('create-intent','present')`, diagnostic, stamp, leaseID); err != nil {
		return err
	}
	if !countFailure {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO provisioning_failures(lease_id,occurred,diagnostic) VALUES(?,?,?)`, leaseID, stamp, diagnostic); err != nil {
		return err
	}
	var recent int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM provisioning_failures WHERE occurred>=? AND occurred<=?`,
		now.UTC().Add(-provisionFailureWindow).Format(time.RFC3339Nano), stamp).Scan(&recent); err != nil {
		return err
	}
	quarantinedAdmission := -1
	if recent >= provisionFailureThreshold {
		until := now.UTC().Add(provisionFailureWindow).Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `UPDATE platform SET provisioning_quarantine_until=CASE
WHEN provisioning_quarantine_until>? THEN provisioning_quarantine_until ELSE ? END WHERE id=1`, until, until); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&quarantinedAdmission); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if quarantinedAdmission >= 0 {
		s.admission = quarantinedAdmission
	}
	return nil
}

// DeferProvisionForCapacity ends a provisioning attempt that found no room,
// without a failure row and without quarantine.
//
// A node with no free slot is the most ordinary thing that can happen to a
// pool, and treating it as a provisioning failure is what let three busy cycles
// inside fifteen minutes trip the breaker and silence the pool for another
// fifteen (MS-A2, 2026-09-21). Nothing physical was created -- the backend
// checks headroom before its create-attempt marker and before the Pod POST --
// so the only debt is the JIT that was already minted.
//
// WHAT ACTUALLY RELEASES THE SLOT, named rather than assumed. The lease is set
// to `cleanup-pending` and the worker row to `absent`, which puts it on two
// existing queues in a fixed order:
//
//	PendingRegistrationCleanup  l.phase IN (cleanup-pending, quarantined)
//	                            AND w.phase='absent' AND j.removal!='absent'
//	                            -> CleanupRegistration deletes the GitHub
//	                               runner and sets removal='absent'
//	PendingFinishedCleanup      l.phase IN (cleanup-pending, quarantined)
//	                            AND w.phase='absent' AND j.removal='absent'
//	                            -> FinishCleanup
//	FinishCleanup               UPDATE leases SET phase='released'
//	                            WHERE id=? AND phase IN (cleanup-pending, quarantined)
//
// The second query cannot select the lease until the first has proven the
// registration gone, so the slot never comes back while a one-use registration
// is still live. An earlier version of this function wrote `quarantined`, and
// that dead-ended: `BeginWorkerDestroy` is the only writer of
// `cleanup-pending` and it requires a worker in `create-intent` or `present`,
// which a worker that never existed can never be.
//
// No breaker, no failure row, no admission change. The next cycle simply tries
// again.
func (s *Store) DeferProvisionForCapacity(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	return s.rollBackBeforeCreate(ctx, leaseID, diagnostic, now, true)
}

// RollBackAfterWitnessRefusal is DeferProvisionForCapacity for a create whose
// pre-create inventory the host witness refused (homelab#1213). The ordering
// argument is the same -- the backend tags that refusal only before its
// create-attempt marker and Pod POST -- so the lease takes the same
// cleanup-pending rollback with no failure row. What it does NOT take is the
// capacity latch: a refusal says nothing about free slots, and the latch would
// hold reservations until an inventory showed one.
func (s *Store) RollBackAfterWitnessRefusal(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	return s.rollBackBeforeCreate(ctx, leaseID, diagnostic, now, false)
}

func (s *Store) rollBackBeforeCreate(ctx context.Context, leaseID, diagnostic string, now time.Time, capacityWait bool) error {
	if now.IsZero() || diagnostic == "" {
		return errors.New("pre-create rollback requires time and safe diagnostic")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stamp := now.UTC().Format(time.RFC3339Nano)
	// CLEANUP-PENDING, NOT QUARANTINED, and the distinction is the whole bug.
	//
	// Quarantine is for uncertainty an operator has to resolve: something may
	// physically exist and the controller cannot prove otherwise. A wait is not
	// uncertain -- nothing was created, and this transaction says so in the
	// same breath by rolling the worker row to `absent`.
	//
	// Routing it to quarantine also dead-ended it. `BeginWorkerDestroy` is the
	// ONLY writer of `cleanup-pending`, and it requires a worker in
	// `create-intent` or `present`; a worker that never existed and was rolled
	// straight to `absent` can never pass through it. `FinishCleanup` releases
	// only from `cleanup-pending`, so the lease stayed charged forever, across
	// restarts, with ready=true and free slots on the cycle line. That is how a
	// one-slot platform became permanently full after a single busy minute.
	//
	// From `cleanup-pending` the existing machinery finishes the job with no
	// new terminal path: PendingRegistrationCleanup (which already accepts
	// cleanup-pending with an absent worker) deletes the GitHub registration
	// and sets removal='absent', and only THEN does PendingFinishedCleanup
	// select it -- so the slot cannot come back while the one-use registration
	// is still live.
	result, err := tx.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic=?,updated=?
WHERE id=? AND phase='provisioning'`, diagnostic, stamp, leaseID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("pre-create rollback found no provisioning lease to roll back")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jit_transfers SET revoked=1,diagnostic=?,updated=? WHERE lease_id=?`,
		diagnostic, stamp, leaseID); err != nil {
		return err
	}
	// THE INTENT IS ROLLED BACK TO ABSENT, and that is a claim about ordering
	// rather than an optimistic guess. The backend refuses on headroom BEFORE
	// its create-attempt marker and BEFORE the Pod POST, so no Pod was ever
	// requested for this resource.
	//
	// Leaving it at `create-intent` is what produced the second failure of the
	// same incident: `Maintain` tried to destroy a quarantined worker that had
	// never existed and stopped at `worker UID is unknown; complete inventory
	// before deletion`, which then held the whole reservation. Absent is both
	// true and the phase `PendingRegistrationCleanup` looks for, so the unused
	// JIT registration is deleted by the existing cleanup rather than a new one.
	// CHECKED, not fired and forgotten. This rollback is what makes the lease
	// eligible for PendingRegistrationCleanup, and the JIT above has already
	// been marked revoked in the same transaction. If the worker row was not in
	// `create-intent` -- a concurrent confirm, or a caller reaching this from a
	// state it should not -- committing anyway would leave a revoked transfer
	// whose live GitHub registration nothing is queued to delete. Failing the
	// transaction rolls the revocation back with it, so the lease stays in a
	// state the ordinary paths still understand.
	rolled, err := tx.ExecContext(ctx, `UPDATE worker_resources SET phase='absent',diagnostic=?,updated=?
WHERE lease_id=? AND phase='create-intent'`, diagnostic, stamp, leaseID)
	if err != nil {
		return err
	}
	changed, err = rolled.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("pre-create rollback did not find a create-intent worker to roll back; refusing to leave a live registration uncleaned")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if capacityWait {
		s.capacityWait = true
	}
	return nil
}

// ReconcileDeadlines turns elapsed startup and cleanup clocks into durable,
// charged quarantine state. It never treats a timeout as absence or cleanup.
func (s *Store) ReconcileDeadlines(ctx context.Context, now time.Time) ([]DeadlineAction, error) {
	if now.IsZero() {
		return nil, errors.New("deadline reconciliation requires current time")
	}
	now = now.UTC()
	rows, err := s.db.QueryContext(ctx, `SELECT w.lease_id,'startup' FROM worker_resources w JOIN leases l ON l.id=w.lease_id
WHERE w.phase IN ('create-intent','present') AND l.phase='provisioning' AND w.startup_deadline<=?
UNION ALL SELECT w.lease_id,'cleanup' FROM worker_resources w JOIN leases l ON l.id=w.lease_id
WHERE w.phase='destroy-intent' AND l.phase='cleanup-pending' AND w.cleanup_deadline!='' AND w.cleanup_deadline<=?
ORDER BY 1,2`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var actions []DeadlineAction
	for rows.Next() {
		var action DeadlineAction
		if err := rows.Scan(&action.LeaseID, &action.Kind); err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, action := range actions {
		diagnostic := action.Kind + " deadline exceeded"
		if action.Kind == "startup" {
			if err := s.QuarantineProvisionFailure(ctx, action.LeaseID, diagnostic, now); err != nil {
				return nil, err
			}
			continue
		}
		if err := s.quarantineCleanupDeadline(ctx, action.LeaseID, diagnostic, now); err != nil {
			return nil, err
		}
	}
	return actions, nil
}

// RetireUnusedWorkers schedules a worker for normal cleanup when it consumed
// its one-use bootstrap payload but emitted no job-start evidence for five
// minutes. The lease remains charged until backend, registration and
// acquisition reconciliation independently satisfy FinishWorkerCleanup.
func (s *Store) RetireUnusedWorkers(ctx context.Context, now time.Time) ([]string, error) {
	if now.IsZero() {
		return nil, errors.New("unused-worker reconciliation requires current time")
	}
	now = now.UTC()
	cutoff := now.Add(-unusedWorkerTimeout).Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `SELECT l.id FROM leases l
JOIN worker_resources w ON w.lease_id=l.id JOIN jit_transfers j ON j.lease_id=l.id
WHERE l.phase='provisioning' AND w.phase='present' AND j.phase='stream-written' AND j.updated<=?
ORDER BY j.updated,l.id`, cutoff)
	if err != nil {
		return nil, err
	}
	var candidates []string
	for rows.Next() {
		var leaseID string
		if err := rows.Scan(&leaseID); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, leaseID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	stamp := now.Format(time.RFC3339Nano)
	var retired []string
	for _, leaseID := range candidates {
		result, err := s.db.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic='unused worker idle deadline exceeded',updated=?
WHERE id=? AND phase='provisioning'`, stamp, leaseID)
		if err != nil {
			return nil, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed > 1 {
			return nil, errors.New("unused-worker transition produced an invalid result")
		}
		if changed == 1 {
			retired = append(retired, leaseID)
		}
	}
	if len(retired) > 0 {
		s.mu.Lock()
		s.retiredUnused += len(retired)
		s.mu.Unlock()
	}
	return retired, nil
}

// RetireTerminatedWorkers schedules for cleanup every provisioning worker whose
// runner process the last trusted inventory saw FINISH. It is the same durable
// transition RetireUnusedWorkers makes, on positive evidence instead of a clock.
//
// The five-minute unused timeout is the only thing that used to release such a
// slot. A runner offered no work connects, idles and exits within about thirty
// seconds, so on a one-slot pool the pool sat idle-but-full for four and a half
// minutes out of every five while real jobs queued (2026-09-22).
//
// It is not a replacement for that timeout and does not make it redundant: the
// timeout still covers a worker that never terminates, and any adapter that
// cannot observe termination reports nothing here and keeps the timeout alone.
//
// The lease stays charged after this. Backend, registration and acquisition
// reconciliation must still independently satisfy FinishWorkerCleanup, exactly
// as for every other cleanup path.
func (s *Store) RetireTerminatedWorkers(ctx context.Context, now time.Time) ([]string, error) {
	if now.IsZero() {
		return nil, errors.New("terminated-worker reconciliation requires current time")
	}
	now = now.UTC()
	s.mu.Lock()
	terminated := make(map[string]int, len(s.terminated))
	for leaseID, exit := range s.terminated {
		terminated[leaseID] = exit
	}
	vanished := make(map[string]bool, len(s.vanishedProven))
	for leaseID := range s.vanishedProven {
		vanished[leaseID] = true
	}
	s.mu.Unlock()
	if len(terminated) == 0 && len(vanished) == 0 {
		return nil, nil
	}
	// RUNNING LEASES TOO (homelab#996). A running lease used to leave only on
	// its JobCompleted message. When that message is lost -- a swapped job whose
	// own lease was released first was the live case -- nothing else retired
	// it, and the slot stayed charged forever with the runner long exited.
	rows, err := s.db.QueryContext(ctx, `SELECT l.id,l.repo_id,l.runner_name,l.phase,w.created,j.phase FROM leases l
JOIN worker_resources w ON w.lease_id=l.id JOIN jit_transfers j ON j.lease_id=l.id
WHERE l.phase IN ('provisioning','running') AND w.phase='present' ORDER BY w.created,l.id`)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		leaseID    string
		repoID     int64
		runnerName string
		leasePhase Phase
		created    string
		jitPhase   string
	}
	var candidates []candidate
	for rows.Next() {
		var found candidate
		if err := rows.Scan(&found.leaseID, &found.repoID, &found.runnerName, &found.leasePhase, &found.created, &found.jitPhase); err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := terminated[found.leaseID]; ok || vanished[found.leaseID] {
			candidates = append(candidates, found)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	stamp := now.Format(time.RFC3339Nano)
	var retired []string
	var retiredUnused int
	for _, found := range candidates {
		exit, sawExit := terminated[found.leaseID]
		if !sawExit {
			// VANISHED, NOT TERMINATED (homelab#1094): complete inventories
			// stopped listing the worker with no exit ever observed. See
			// trackVanishedWorkers. Retired like an abnormal exit, never like a
			// clean one, because nothing proves the runner finished.
			changed, err := s.retireVanishedWorker(ctx, found.leaseID, found.leasePhase, now)
			if err != nil {
				return nil, err
			}
			if !changed {
				continue
			}
			retired = append(retired, found.leaseID)
			slog.Warn("Retired a worker that vanished from the backend inventory without a termination record.",
				"lease_id", found.leaseID, "runner_name", found.runnerName, "repository_id", found.repoID,
				"lease_phase", found.leasePhase, "seconds_alive", secondsSince(found.created, now))
			continue
		}
		if found.leasePhase == Running {
			// The worker ran a job and its runner has exited, whatever the exit
			// code: a failing job is not a provisioning failure, so no breaker
			// row. Ordinary cleanup, and the lease stays charged until teardown
			// proves absence exactly as for every other path.
			result, err := s.db.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic='runner exited before its completion was read',updated=?
WHERE id=? AND phase='running'`, stamp, found.leaseID)
			if err != nil {
				return nil, err
			}
			changed, err := result.RowsAffected()
			if err != nil || changed > 1 {
				return nil, errors.New("terminated running-worker transition produced an invalid result")
			}
			if changed == 1 {
				retired = append(retired, found.leaseID)
				// INFO, not WARN: a job that finished between polls reaches this
				// arm before its JobCompleted is read, which is ordinary. The
				// lost-completion case (homelab#996) is the same line with no
				// completion ever following it.
				slog.Info("Retired a running worker whose runner exited before its completion was read.",
					"lease_id", found.leaseID, "runner_name", found.runnerName, "repository_id", found.repoID,
					"seconds_alive", secondsSince(found.created, now), "exit_code", exit)
			}
			continue
		}
		// THE FAST PATH IS ONLY FOR A CLEAN, EXPECTED EXIT, and everything else
		// keeps the breaker it would otherwise bypass.
		//
		// The idle runner this whole change exists for consumed its bootstrap
		// (jit phase 'stream-written'), was offered nothing and exited 0. Nothing
		// is wrong with the host, so ordinary cleanup is right and no failure row
		// is owed.
		//
		// A worker that exited NON-ZERO, or that died before it ever fetched its
		// bootstrap payload, is a provisioning failure -- a bad image, a broken
		// runtime, a host fault. Routing it to ordinary cleanup wrote no
		// provisioning_failures row, so three-in-fifteen-minutes never armed and
		// the pool would mint, crash and mint again every cycle forever. That is
		// exactly the breaker's job, and it is the thing the ten-minute startup
		// deadline used to do here, slower but correctly.
		if exit != 0 || found.jitPhase != "stream-written" {
			diagnostic := "worker terminated before consuming its bootstrap"
			if found.jitPhase == "stream-written" {
				diagnostic = "worker terminated with a non-zero exit"
			}
			if err := s.QuarantineProvisionFailure(ctx, found.leaseID, diagnostic, now); err != nil {
				return nil, err
			}
			slog.Info("Quarantined a worker that terminated abnormally. The provisioning breaker counts it.",
				"lease_id", found.leaseID, "repository_id", found.repoID,
				"seconds_alive", secondsSince(found.created, now),
				"exit_code", exit, "jit_phase", found.jitPhase, "diagnostic", diagnostic)
			continue
		}
		// "Before a job start was observed", not "without acquiring work": a job
		// of a few seconds can finish before its JobStarted message is read, so
		// the lease is still provisioning here although the runner did run a
		// job (homelab#951). The completion message settles it later; this
		// transition only says what the controller had seen.
		result, err := s.db.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic='worker terminated before a job start was observed',updated=?
WHERE id=? AND phase='provisioning'`, stamp, found.leaseID)
		if err != nil {
			return nil, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed > 1 {
			return nil, errors.New("terminated-worker transition produced an invalid result")
		}
		if changed != 1 {
			continue
		}
		retired = append(retired, found.leaseID)
		retiredUnused++
		// The exit code is the diagnosis, and 0 is the interesting value: a
		// runner GitHub offered nothing exits cleanly. Without this line the
		// starvation looked like a healthy pool with a busy slot.
		slog.Info("Retired a worker that terminated before a job start was observed.",
			"lease_id", found.leaseID, "repository_id", found.repoID, "runner_name", found.runnerName,
			"seconds_alive", secondsSince(found.created, now),
			"exit_code", exit)
	}
	if retiredUnused > 0 {
		s.mu.Lock()
		s.retiredUnused += retiredUnused
		s.mu.Unlock()
	}
	return retired, nil
}

// secondsSince is the worker's age in whole seconds, or -1 when its creation
// instant cannot be parsed. A negative age reads as unknown rather than as a
// worker that lived no time at all.
func secondsSince(created string, now time.Time) int {
	at, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return -1
	}
	return int(now.Sub(at).Seconds())
}

func (s *Store) quarantineCleanupDeadline(ctx context.Context, leaseID, diagnostic string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stamp := now.Format(time.RFC3339Nano)
	if err := changedOne(tx.ExecContext(ctx, `UPDATE leases SET phase='quarantined',diagnostic=?,updated=?
WHERE id=? AND phase='cleanup-pending'`, diagnostic, stamp, leaseID)); err != nil {
		return err
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE worker_resources SET diagnostic=?,updated=?
WHERE lease_id=? AND phase='destroy-intent'`, diagnostic, stamp, leaseID)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) PendingWorkerRetirements(ctx context.Context) ([]WorkerObservation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT w.lease_id,w.resource_id FROM worker_resources w JOIN leases l ON l.id=w.lease_id
WHERE l.phase IN ('cleanup-pending','quarantined') AND w.phase IN ('create-intent','present') ORDER BY w.created,w.lease_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workers []WorkerObservation
	for rows.Next() {
		var worker WorkerObservation
		if err := rows.Scan(&worker.LeaseID, &worker.ResourceID); err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}
	return workers, rows.Err()
}

func (s *Store) PendingFinishedCleanup(ctx context.Context) ([]string, error) {
	// LEFT JOIN ON worker_resources, AND NO WORKER ROW MEANS ABSENT. The inner
	// join made a lease that was fenced before its worker row existed
	// unreleasable forever: `Prepare` failing after beginJIT has already moved
	// the lease to `provisioning` leaves exactly that shape, the registration
	// is cleaned, and the slot stays charged with ReserveNext returning
	// ErrCapacity indefinitely. "No row" is the strongest absence there is
	// here -- the controller never wrote an intent, so nothing was created.
	//
	// `j.removal='absent'` is the fence that stops a slot returning while a
	// one-use registration may still be live, and it still holds for every
	// lease that HAS a transfer row. A lease with NO transfer row is released
	// on the other two proofs (homelab#1095, review D6). That row is inserted
	// by beginJIT in the same transaction that enters provisioning, before any
	// registration call, so no row means no registration was ever attempted --
	// the same reading as "no worker row" above. The inner join made a lease
	// that reached cleanup before beginJIT unreleasable: an AcquireJobs decline
	// moves the lease straight to cleanup-pending with evidence 'declined' and
	// no transfer, and nothing else could ever select it.
	//
	// QUARANTINED LEASES ARE SELECTED TOO, under exactly the same proofs. Two
	// quarantines leave no worker for BeginWorkerDestroy to act on, so they
	// never reach cleanup-pending: QuarantineBeforeBackendCreate (no worker
	// row; ProvisionNext calls Create only after BeginWorkerCreate commits the
	// row, so no row means Create never ran) and quarantineCleanupDeadline
	// (worker left at destroy-intent until ConfirmWorkerAbsent proves both
	// process and storage absence, directly or by omission from a complete
	// inventory). Selecting only cleanup-pending held those slots forever. A
	// quarantined lease whose worker is create-intent, present or
	// destroy-intent still fails the worker condition and stays charged.
	rows, err := s.db.QueryContext(ctx, `SELECT l.id FROM leases l
LEFT JOIN worker_resources w ON w.lease_id=l.id LEFT JOIN jit_transfers j ON j.lease_id=l.id
WHERE l.phase IN ('cleanup-pending','quarantined') AND (w.phase IS NULL OR w.phase='absent') AND (j.lease_id IS NULL OR j.removal='absent')
AND EXISTS(SELECT 1 FROM acquisition_evidence e WHERE e.lease_id=l.id AND e.job_key=l.job_key
 AND e.outcome IN ('declined','accepted','completed')) ORDER BY l.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leaseIDs []string
	for rows.Next() {
		var leaseID string
		if err := rows.Scan(&leaseID); err != nil {
			return nil, err
		}
		leaseIDs = append(leaseIDs, leaseID)
	}
	return leaseIDs, rows.Err()
}

func (s *Store) PendingRegistrationCleanup(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT w.lease_id FROM worker_resources w
JOIN leases l ON l.id=w.lease_id JOIN jit_transfers j ON j.lease_id=w.lease_id
WHERE l.phase IN ('cleanup-pending','quarantined') AND w.phase='absent' AND j.removal!='absent'
UNION SELECT l.id FROM leases l JOIN jit_transfers j ON j.lease_id=l.id
WHERE l.phase IN ('cleanup-pending','quarantined') AND j.removal!='absent'
AND NOT EXISTS(SELECT 1 FROM worker_resources w WHERE w.lease_id=l.id)
ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leaseIDs []string
	for rows.Next() {
		var leaseID string
		if err := rows.Scan(&leaseID); err != nil {
			return nil, err
		}
		leaseIDs = append(leaseIDs, leaseID)
	}
	return leaseIDs, rows.Err()
}

// FinishWorkerCleanup derives physical absence from the durable trusted-backend
// observation. Its other two inputs must come from GitHub reconciliation. It
// cannot release a lease from a worker callback or a caller-supplied absence bit.
func (s *Store) FinishWorkerCleanup(ctx context.Context, leaseID string, runnerAbsent, acquisitionSettled bool) error {
	workerAbsent, err := s.WorkerAbsent(ctx, leaseID)
	if err != nil {
		return err
	}
	return s.FinishCleanup(ctx, leaseID, CleanupEvidence{
		WorkerAbsent: workerAbsent, RunnerAbsent: runnerAbsent, AcquisitionSettled: acquisitionSettled,
	})
}

// markRunnerAbsentForTest and DeferProvisionForCapacityWithoutWorkerForTest
// exist so tests can build the two shapes #857 describes without standing up a
// GitHub client. Both run the SAME statements production runs.
func (s *Store) markRunnerAbsentForTest(ctx context.Context, leaseID string) (bool, error) {
	err := changedOne(s.db.ExecContext(ctx,
		`UPDATE jit_transfers SET removal='absent',diagnostic='',updated=? WHERE lease_id=?`, timestamp(), leaseID))
	return err == nil, err
}

// DeferProvisionForCapacityWithoutWorkerForTest is the production deferral
// minus the worker rollback, which is exactly the state a pre-create failure
// leaves when Prepare fails after the lease is already provisioning: fenced,
// with no worker_resources row at all.
func (s *Store) DeferProvisionForCapacityWithoutWorkerForTest(ctx context.Context, leaseID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stamp := timestamp()
	if err := changedOne(s.db.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic=?,updated=?
WHERE id=? AND phase='provisioning'`, "pre-create failure with no worker row", stamp, leaseID)); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE jit_transfers SET revoked=1,updated=? WHERE lease_id=?`, stamp, leaseID)
	return err
}
