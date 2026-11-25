package controller

import (
	"context"
	"encoding/json"
	"time"
)

// This file holds the read-only accessors the discovery report composes from.
// Every one of them is additive over columns that already exist; none of them
// performs a GitHub call, takes a platform action, or writes to the journal, so
// a monitoring probe can never change pool state or consume rate limit.

// PlatformSlots reports the configured qualification ceiling and the slots held
// back for the pre-pool fleet. Both are journal-pinned at Open, so a report can
// publish the numbers its other counts are measured against.
func (s *Store) PlatformSlots() (capacity, legacy int) {
	return s.config.Capacity, s.config.LegacySlots
}

// Platform names the platform this controller serves. One controller serves
// exactly one, so it also identifies which report a probe reached.
func (s *Store) Platform() string { return s.config.Platform }

// Admission reports the process-local scheduling fence: whether a trusted
// inventory observation is in force and how many total commitments it allows.
// Both are deliberately false and zero on every process start.
func (s *Store) Admission() (ready bool, admission int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready, s.admission
}

// LeasePhaseCounts is the live lease population by durable phase. Released
// leases are excluded: they hold no slot and are not part of any verdict.
func (s *Store) LeasePhaseCounts(ctx context.Context) (map[Phase]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT phase,COUNT(*) FROM leases WHERE phase!='released' GROUP BY phase`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[Phase]int{}
	for rows.Next() {
		var phase Phase
		var count int
		if err := rows.Scan(&phase, &count); err != nil {
			return nil, err
		}
		counts[phase] = count
	}
	return counts, rows.Err()
}

// StuckCharged counts slots held by a lease that has sat in quarantined or
// acquire-unknown for longer than the provisioning-failure window. These never
// self-resolve — the pool refuses to free a slot without independent evidence —
// so they are capacity lost until an operator reconciles them, and they are
// invisible in the charged count, which a busy healthy pool also raises.
func (s *Store) StuckCharged(ctx context.Context, now time.Time) (int, error) {
	cutoff := now.UTC().Add(-provisionFailureWindow).Format(time.RFC3339Nano)
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases
WHERE phase IN ('quarantined','acquire-unknown') AND updated<=?`, cutoff).Scan(&count)
	return count, err
}

// ChargedSlots counts slots held by a lease in any non-released phase. A slot
// held by an uncertain operation is charged: the pool refuses to free one on a
// timeout, so charged plus free plus the legacy hold-back is the whole ceiling
// at all times.
func (s *Store) ChargedSlots(ctx context.Context) (int, error) {
	var charged int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&charged)
	return charged, err
}

// ActiveWorkerLeases counts leases whose worker is plausibly running a job
// right now: provisioning (booting to run it) or running (executing it).
// Neither phase needs anything from a queue session while it holds -- GitHub
// sends an envelope only on a job event, so the whole span between a job's
// start and its completion is silence that is not deafness. This is the count
// sessionsLookDeaf and sessionRecoveryRung must be fed; ChargedSlots is not,
// because every other non-released phase is deliberately excluded here:
//   - reserved, acquire-intent, acquire-unknown and acquired are
//     pre-provisioning. The controller reaches every one of them from its own
//     state machine, in response to a queue message it already has, and each
//     normally resolves in seconds -- not the minutes a real job runs. A
//     lease stuck there is itself a symptom worth a restart catching, not a
//     reason to suppress the check that would catch it.
//   - cleanup-pending is a job that already finished; its worker needs no
//     further GitHub traffic to tear itself down.
//   - quarantined is exactly the bug in #912's original gate: a lease stuck
//     there forever disabled the deaf check forever, on exactly the pool
//     that was broken. It never counts as active.
func (s *Store) ActiveWorkerLeases(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase IN ('provisioning','running')`).Scan(&count)
	return count, err
}

// ChargedByRepository is each repository's share of the charged slots.
func (s *Store) ChargedByRepository(ctx context.Context) (map[int64]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT repo_id,COUNT(*) FROM leases WHERE phase!='released' GROUP BY repo_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	charged := map[int64]int{}
	for rows.Next() {
		var repo int64
		var count int
		if err := rows.Scan(&repo, &count); err != nil {
			return nil, err
		}
		charged[repo] = count
	}
	return charged, rows.Err()
}

// WorkerCounts is the worker-resource population plus the two deadline counts.
// Booting, Running and Destroying are the resource's own phase joined to its
// lease, so a resource whose lease has moved on is not counted twice.
type WorkerCounts struct {
	Tracked        int
	Booting        int
	Running        int
	Destroying     int
	OverdueStartup int
	OverdueCleanup int
}

// Workers is deliberately unfiltered by deadline; a caller that wants the
// deadline counts passes the same instant the rest of the report was composed
// at, so two fields of one body cannot disagree about what time it is.
func (s *Store) WorkerCounts(ctx context.Context, now time.Time) (WorkerCounts, error) {
	var counts WorkerCounts
	stamp := now.UTC().Format(time.RFC3339Nano)
	err := s.db.QueryRowContext(ctx, `SELECT
 COUNT(*),
 COALESCE(SUM(w.phase IN ('create-intent','present') AND l.phase='provisioning'),0),
 COALESCE(SUM(w.phase='present' AND l.phase='running'),0),
 COALESCE(SUM(w.phase='destroy-intent'),0),
 COALESCE(SUM(w.phase IN ('create-intent','present') AND l.phase='provisioning' AND w.startup_deadline<=?),0),
 COALESCE(SUM(w.phase='destroy-intent' AND l.phase='cleanup-pending' AND w.cleanup_deadline!='' AND w.cleanup_deadline<=?),0)
FROM worker_resources w JOIN leases l ON l.id=w.lease_id`, stamp, stamp).Scan(
		&counts.Tracked, &counts.Booting, &counts.Running, &counts.Destroying,
		&counts.OverdueStartup, &counts.OverdueCleanup)
	return counts, err
}

// Demand is the offered-job population. Queued counts every OFFERED job, which
// is queued plus assigned -- both are selectable by a reservation. OldestOffer is the earliest durable
// offer instant among those jobs, empty when there is none. It publishes counts
// and instants only: no job identity, raw job ID or request ID ever leaves here.
type Demand struct {
	Queued      int
	ByRepo      map[int64]int
	OldestOffer string
}

// QueuedDemand counts only jobs a reservation could actually select: the
// repository is eligible and no live lease already holds the job. Counting
// the rest would report demand the scheduler is not declining to serve. Both
// offered states count, because ReserveNext selects both — a count restricted
// to 'queued' would read zero on a pool whose whole backlog is broker-assigned.
func (s *Store) QueuedDemand(ctx context.Context) (Demand, error) {
	demand := Demand{ByRepo: map[int64]int{}}
	const selectable = `FROM jobs j JOIN repositories r ON r.id=j.repo_id
WHERE r.eligible=1 AND j.state IN ('queued','assigned') AND NOT EXISTS (
 SELECT 1 FROM leases l WHERE l.repo_id=j.repo_id AND l.set_id=j.set_id AND l.job_key=j.job_key AND l.phase!='released')`
	rows, err := s.db.QueryContext(ctx, `SELECT j.repo_id,COUNT(*) `+selectable+` GROUP BY j.repo_id`)
	if err != nil {
		return demand, err
	}
	defer rows.Close()
	for rows.Next() {
		var repo, count int
		if err := rows.Scan(&repo, &count); err != nil {
			return demand, err
		}
		demand.ByRepo[int64(repo)] = count
		demand.Queued += count
	}
	if err := rows.Err(); err != nil {
		return demand, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(NULLIF(j.offered,'')),'') `+selectable).Scan(&demand.OldestOffer)
	return demand, err
}

// OpenSessionRepositories reports which repositories currently hold a verified
// message session. A repository with no session receives no offers, which is
// the first thing an operator checks when one is not being served.
func (s *Store) OpenSessionRepositories(ctx context.Context) (map[int64]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT repo_id FROM sessions WHERE phase='active'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	open := map[int64]bool{}
	for rows.Next() {
		var repo int64
		if err := rows.Scan(&repo); err != nil {
			return nil, err
		}
		open[repo] = true
	}
	return open, rows.Err()
}

// AssignedJobs is each repository's last reported assigned-job count. It is
// GitHub's own aggregate statistic, not a count of this pool's leases, and the
// two legitimately disagree while work is in flight.
func (s *Store) AssignedJobs(ctx context.Context) (map[int64]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT repo_id,statistics FROM scale_sets`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	assigned := map[int64]int{}
	for rows.Next() {
		var repo int64
		var raw string
		if err := rows.Scan(&repo, &raw); err != nil {
			return nil, err
		}
		var stats struct {
			TotalAssignedJobs int `json:"totalAssignedJobs"`
		}
		// A scale set that has never reported carries the literal 'null'. That is
		// an absent statistic, not a malformed one, and reads as zero.
		if err := json.Unmarshal([]byte(raw), &stats); err != nil {
			continue
		}
		assigned[repo] = stats.TotalAssignedJobs
	}
	return assigned, rows.Err()
}

// ProvisioningSuppression reports the pool-wide provisioning backoff: whether
// repeated failures currently suppress new reservations, and until when. It is
// distinct from an individual lease's quarantined phase.
func (s *Store) ProvisioningSuppression(ctx context.Context, now time.Time) (suppressed bool, until string, err error) {
	if err := s.db.QueryRowContext(ctx, `SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&until); err != nil {
		return false, "", err
	}
	if until == "" {
		return false, "", nil
	}
	deadline, parseErr := time.Parse(time.RFC3339Nano, until)
	if parseErr != nil {
		// An unparseable deadline is what ReconcileBackendInventory refuses to
		// schedule against, so it is reported as suppression rather than ignored.
		return true, until, nil
	}
	return now.Before(deadline), until, nil
}

// RetiredUnused is the number of workers retired for emitting no job start
// within the unused-worker timeout, cumulative since process start. Sustained
// growth means workers are booting and never picking up work.
func (s *Store) RetiredUnused() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retiredUnused
}

// recordEnvelopeReceived latches the instant this process last saw a queue
// envelope from any subscription. It only ever moves forward: a delayed or
// reordered write must not make a live session look older than it is. The
// set's own instant moves with it, for ProbeUnseenDemand.
func (s *Store) recordEnvelopeReceived(key SetKey, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if at.After(s.lastEnvelope) {
		s.lastEnvelope = at
	}
	s.recordSetEnvelope(key, at)
}

// Liveness reports the two instants sessionsLookDeaf needs: when this process
// last received a queue envelope from any subscription (zero if none yet), and
// when this process opened its journal.
func (s *Store) Liveness() (lastEnvelopeAt, processStarted time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastEnvelope, s.started
}
