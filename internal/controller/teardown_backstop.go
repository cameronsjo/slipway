package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Two backstops for a slot that used to be lost for good (homelab#1094,
// review findings D3 and D4). Both end in the ordinary teardown: nothing here
// releases a lease, it only gives a stuck worker a way back into the path that
// proves absence and releases it.
const (
	// vanishedWorkerGrace is how long a present worker must be missing from
	// every complete inventory before it counts as gone. A Pod is listed from
	// the moment Create returns until something deletes it, so missing is
	// already strong evidence; the grace and the miss count below guard the one
	// weaker case, a listing that omits a live Pod (review T5).
	vanishedWorkerGrace = 2 * time.Minute
	// vanishedWorkerMisses is how many consecutive complete inventories must
	// agree that the worker is missing.
	vanishedWorkerMisses = 2
	// redeleteBackoff is how long after a delete was sent it may be sent again.
	// It is measured from destroy_started, which only BeginWorkerDestroy and a
	// re-send write, and not from `updated`, which the cleanup-deadline
	// quarantine also bumps (that doubled the first wait to about four
	// minutes). It matches the cleanup deadline, so the ordinary teardown
	// (delete, then absence on this cycle or the next) is never raced by a
	// second delete; only a teardown that has outlived its deadline is retried.
	redeleteBackoff = workerCleanupTimeout
)

// vanishedWorker is one present worker that complete inventories no longer
// list. Process-local, like terminated: a restart re-observes from scratch,
// which only restarts the grace, it never skips it.
type vanishedWorker struct {
	firstMissing time.Time
	misses       int
}

// trackVanishedWorkers compares the present, live workers the journal holds
// against the lease IDs a complete, identity-checked inventory listed. The
// caller holds s.mu and commits the returned maps only once the whole
// inventory has been accepted.
//
// A WORKER THAT VANISHES WITHOUT A TERMINATED STATUS HAD NO EXIT. The #996
// backstop retires a running lease only when an inventory positively reports
// its runner Terminated. A Pod deleted by hand, evicted and then garbage
// collected, or lost with its node is never reported that way -- it is simply
// not listed -- so its lease stayed running (or provisioning) forever, holding
// its slot and switching the deaf watchdog off while it did.
//
// Only workers in phase 'present' are considered. A create-intent worker may
// be missing because its Create never landed, and the startup deadline already
// covers it; a destroy-intent worker missing from a complete inventory is
// confirmed absent by ReconcileInventory itself.
func (s *Store) trackVanishedWorkers(ctx context.Context, seen map[string]bool, now time.Time) (map[string]vanishedWorker, map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT w.lease_id,w.resource_id FROM worker_resources w JOIN leases l ON l.id=w.lease_id
WHERE w.phase='present' AND l.phase IN ('provisioning','running') ORDER BY w.lease_id`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	tracked := map[string]vanishedWorker{}
	proven := map[string]bool{}
	for rows.Next() {
		var leaseID, resourceID string
		if err := rows.Scan(&leaseID, &resourceID); err != nil {
			return nil, nil, err
		}
		if seen[leaseID] {
			continue
		}
		entry, ok := s.vanished[leaseID]
		if !ok {
			entry = vanishedWorker{firstMissing: now}
			slog.Warn("A present worker is missing from a complete backend inventory; it is retired if it stays missing.",
				"lease_id", leaseID, "resource_id", resourceID, "grace", vanishedWorkerGrace.String())
		}
		entry.misses++
		tracked[leaseID] = entry
		if entry.misses >= vanishedWorkerMisses && now.Sub(entry.firstMissing) >= vanishedWorkerGrace {
			proven[leaseID] = true
		}
	}
	return tracked, proven, rows.Err()
}

// PendingWorkerRedeletes selects workers whose delete was sent at least
// redeleteBackoff ago and which a complete inventory still lists.
//
// A FAILED DELETE WAS NEVER SENT AGAIN (homelab#1094, review D4). Maintain
// moved the worker to destroy-intent and called Destroy once. If that call
// errored -- an API timeout, a 5xx, a failed lookup -- PendingWorkerRetirements
// never selected the row again, because it selects only create-intent and
// present. The only remaining exit was the Pod disappearing from inventory,
// which cannot happen when nothing deleted it: a Succeeded Pod stays listed.
// The lease aged into quarantined and stayed charged until someone deleted the
// Pod by hand.
//
// Re-sending is safe because the delete is exact. The Kubernetes adapter looks
// the Pod up by name, refuses a UID or label mismatch, deletes with a UID
// precondition, and reads a missing Pod as done; Tart stops and deletes by
// name and proves absence from its own listing. Neither can touch a different
// worker.
//
// Every row selected here is still listed: ReconcileInventory runs first in
// each cycle and confirms absence for any destroy-intent worker a complete
// inventory omits, so those leave this phase before Maintain reads it.
func (s *Store) PendingWorkerRedeletes(ctx context.Context, now time.Time) ([]WorkerObservation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT w.lease_id,w.resource_id,w.destroy_started FROM worker_resources w JOIN leases l ON l.id=w.lease_id
WHERE w.phase='destroy-intent' AND l.phase IN ('cleanup-pending','quarantined') ORDER BY w.created,w.lease_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workers []WorkerObservation
	for rows.Next() {
		var worker WorkerObservation
		var sent string
		if err := rows.Scan(&worker.LeaseID, &worker.ResourceID, &sent); err != nil {
			return nil, err
		}
		// Parsed rather than compared in SQL: RFC3339Nano drops trailing zero
		// fractions, so two stamps of different widths do not sort as text.
		// An unparseable stamp is retried; the row is a stuck teardown either way.
		if at, err := time.Parse(time.RFC3339Nano, sent); err == nil && now.Sub(at) < redeleteBackoff {
			continue
		}
		workers = append(workers, worker)
	}
	return workers, rows.Err()
}

// NoteWorkerRedelete records the attempt in destroy_started, which from here
// on reads "when the current delete was sent", before the adapter call, the same
// intent-first order every other lifecycle write keeps, so a crash mid-call
// still waits out the backoff instead of retrying in a tight loop.
func (s *Store) NoteWorkerRedelete(ctx context.Context, worker WorkerObservation, now time.Time) error {
	stamp := now.UTC().Format(time.RFC3339Nano)
	return changedOne(s.db.ExecContext(ctx, `UPDATE worker_resources SET destroy_started=?,updated=?
WHERE lease_id=? AND resource_id=? AND phase='destroy-intent'`, stamp, stamp, worker.LeaseID, worker.ResourceID))
}

// retireVanishedWorker moves a vanished worker's lease into teardown. A
// running lease goes to cleanup-pending, as a lost completion does: the job
// ran, and whether it passed is GitHub's record, not a provisioning fault. A
// provisioning lease is quarantined through the breaker, as an abnormal exit
// is: a worker that disappears before starting a job is the shape of an
// eviction or a host fault, and repeated ones must trip it.
//
// Either way the worker row is still 'present', so PendingWorkerRetirements
// selects it next and the ordinary Destroy and absence proof follow. The lease
// stays charged until they do.
func (s *Store) retireVanishedWorker(ctx context.Context, leaseID string, phase Phase, now time.Time) (bool, error) {
	if phase == Running {
		result, err := s.db.ExecContext(ctx, `UPDATE leases SET phase='cleanup-pending',diagnostic='worker vanished from the backend inventory before its completion was read',updated=?
WHERE id=? AND phase='running'`, now.Format(time.RFC3339Nano), leaseID)
		if err != nil {
			return false, err
		}
		// Zero rows is a completion that landed between the read and here,
		// which already moved the lease on: nothing to do, not an error.
		changed, err := result.RowsAffected()
		if err != nil || changed > 1 {
			return false, errors.New("vanished running-worker transition produced an invalid result")
		}
		return changed == 1, nil
	}
	if err := s.QuarantineProvisionFailure(ctx, leaseID, "worker vanished from the backend inventory before a job start was observed", now); err != nil {
		return false, err
	}
	return true, nil
}
