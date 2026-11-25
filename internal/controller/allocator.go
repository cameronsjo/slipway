package controller

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

const persistenceTimeout = 5 * time.Second

type Phase string

const (
	Reserved           Phase = "reserved"
	AcquisitionIntent  Phase = "acquire-intent"
	AcquisitionUnknown Phase = "acquire-unknown"
	Acquired           Phase = "acquired"
	Provisioning       Phase = "provisioning"
	Running            Phase = "running"
	CleanupPending     Phase = "cleanup-pending"
	Quarantined        Phase = "quarantined"
	Released           Phase = "released"
)

// Lease holds one physical slot for one job. JobKey is the durable identity;
// RequestID is the runner request ID GitHub's AcquireJobs API needs and is zero
// for a job the broker assigned directly, which has no request to acquire.
type Lease struct {
	ID         string
	Slot       int
	Set        SetKey
	JobKey     string
	RequestID  int64
	Phase      Phase
	RunnerName string
	Created    string
	Updated    string
	Diagnostic string
}

const leaseColumns = `id,slot,repo_id,set_id,job_key,runner_request_id,phase,runner_name,created,updated,diagnostic`

func readLease(row interface{ Scan(...any) error }) (Lease, error) {
	var lease Lease
	err := row.Scan(&lease.ID, &lease.Slot, &lease.Set.RepositoryID, &lease.Set.ScaleSetID,
		&lease.JobKey, &lease.RequestID, &lease.Phase, &lease.RunnerName, &lease.Created, &lease.Updated, &lease.Diagnostic)
	return lease, err
}

func (s *Store) Lease(ctx context.Context, id string) (Lease, error) {
	return readLease(s.db.QueryRowContext(ctx, `SELECT `+leaseColumns+` FROM leases WHERE id=?`, id))
}

func (s *Store) ActiveLeases(ctx context.Context) ([]Lease, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+leaseColumns+` FROM leases WHERE phase!='released' ORDER BY slot`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leases []Lease
	for rows.Next() {
		lease, err := readLease(rows)
		if err != nil {
			return nil, err
		}
		leases = append(leases, lease)
	}
	return leases, rows.Err()
}

// State clauses for selectableJobsQuery. Fixed literals this package owns;
// nothing caller-supplied ever reaches them.
const (
	// reservableJobs is what ReserveNext may take right now.
	reservableJobs = `j.state IN ('queued','assigned')`
	// demandJobs adds 'assigned-stale'. Advertising for a stale row only asks
	// GitHub for work; it never mints a runner, and it is what invites the
	// reassignment that re-confirms the row. The caller filters those rows by
	// age in Go -- see journaledDemand -- because jobs.offered is RFC3339Nano,
	// which does not compare correctly as a string.
	demandJobs = `j.state IN ('queued','assigned','assigned-stale')`
)

// selectableJobsQuery is the ONE predicate for "which journaled jobs matter",
// shared by ReserveNext, offeredJobs and journaledDemand. Three copies drifted:
// journaledDemand counted rows on a retired or mismatched managed set that
// ReserveNext can never take, so a repository advertised capacity for work
// nothing would ever claim.
//
// selectList and tail are literals from this package. stateClause is one of the
// two constants above.
func selectableJobsQuery(selectList, stateClause, tail string) string {
	return `SELECT ` + selectList + ` FROM jobs j
JOIN repositories r ON r.id=j.repo_id JOIN scale_sets ss ON ss.repo_id=j.repo_id AND ss.set_id=j.set_id
WHERE r.eligible=1 AND ` + stateClause + ` AND NOT EXISTS (
 SELECT 1 FROM managed_sets m WHERE m.repo_id=j.repo_id AND (m.phase!='active' OR m.remote_id!=j.set_id)) AND NOT EXISTS (
 SELECT 1 FROM leases l WHERE l.repo_id=j.repo_id AND l.set_id=j.set_id AND l.job_key=j.job_key AND l.phase!='released')` + tail
}

// ReserveNext allocates one physical slot and selects one offered job in the
// same durable transaction. Round-robin is by repository, FIFO within a repo.
// A lone repo can consume every free slot; no repository receives a static quota.
//
// Both offered states are selectable, and an assigned job sorts strictly ahead
// of every queued one: GitHub has already bound an assigned job to this scale
// set and times the assignment out if nothing picks it up, while a queued job
// is still offered to anyone. FIFO by ordinal holds within each class.
func (s *Store) ReserveNext(ctx context.Context) (Lease, error) {
	return s.ReserveNextExcept(ctx, nil)
}

// ReserveNextExcept is ReserveNext with some scale sets left out of job
// selection. SessionManager passes the sets whose queue session could not be
// established this cycle: a job reserved there has no live session to acquire
// it through, and AcquireNext would then retain the reservation, stranding a
// charged slot. Every other set is still reservable.
func (s *Store) ReserveNextExcept(ctx context.Context, skip []SetKey) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return Lease{}, ErrInventoryUnknown
	}
	// A deferred sizing change is being adopted: this process has decided the
	// pool is idle and is about to exit so its supervisor restarts it onto the
	// new numbers. Handing out a slot here would re-open the journal it just
	// found quiescent. The latch is set under this same mutex, so there is no
	// window between that decision and this refusal.
	if s.draining {
		return Lease{}, ErrCapacity
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return Lease{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback()
	var cursor int64
	if err := tx.QueryRowContext(ctx, `SELECT cursor_repo FROM platform WHERE id=1`).Scan(&cursor); err != nil {
		return Lease{}, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT slot FROM leases WHERE phase!='released'`)
	if err != nil {
		return Lease{}, err
	}
	used := map[int]bool{}
	for rows.Next() {
		var slot int
		if err := rows.Scan(&slot); err != nil {
			rows.Close()
			return Lease{}, err
		}
		if slot < 0 || slot >= s.config.Capacity-s.config.LegacySlots || used[slot] {
			rows.Close()
			return Lease{}, errors.New("journal occupancy exceeds configured platform slots")
		}
		used[slot] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Lease{}, err
	}
	// ADMISSION IS A COUNT, SO IT MUST BE COMPARED WITH A COUNT. This used to
	// read `slot >= s.admission`, comparing the lowest free slot INDEX to the
	// number of commitments a trusted inventory allows. The two agree only
	// while the charged slots happen to be 0..n-1.
	//
	// They stop agreeing the moment a hole opens. On a full host
	// `admission == active`; with one worker charged on slot 1 and slot 0
	// released, the index test read `0 >= 1` -- false -- and reserved a lease
	// with zero free slots. Every cycle then claimed a real job from GitHub,
	// minted a real JIT registration, failed Create for headroom, deleted the
	// registration, reopened the hole and did it again, dropping the claimed
	// job each time. Before the capacity wait existed, three such cycles armed
	// the provisioning breaker and it stopped; with the breaker correctly off
	// this path, nothing bounded it.
	//
	// The count is `len(used)`, already built above and already proven free of
	// duplicates and out-of-range slots.
	if len(used) >= s.admission {
		return Lease{}, ErrCapacity
	}
	// THE FAILURE BREAKER IS CHECKED HERE, NOT ONLY THROUGH ADMISSION. A tripped
	// breaker freezes admission at the count of unreleased leases, but a
	// quarantined lease can now release (PendingFinishedCleanup) in the same
	// cycle's Maintain, before any new inventory refreezes the count. The
	// frozen admission then exceeds len(used) again and, under a steadily
	// failing JIT API or backend, the pool would claim a real job, mint a
	// registration, quarantine, release and repeat. The post-create path had
	// the same gap through BeginWorkerDestroy -> cleanup-pending -> released.
	// An unparsable deadline refuses, the same fail-closed reading
	// ReconcileBackendInventory and ProvisioningSuppression take.
	var breakerUntil string
	if err := tx.QueryRowContext(ctx, `SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&breakerUntil); err != nil {
		return Lease{}, err
	}
	if breakerUntil != "" {
		until, err := time.Parse(time.RFC3339Nano, breakerUntil)
		if err != nil || time.Now().Before(until) {
			return Lease{}, ErrCapacity
		}
	}
	// The platform-wide waiting latch. It is belt-and-braces beside the count
	// test above: if some future change reopens the reserve-then-fail loop,
	// this stops it spinning until an inventory actually observes a free slot.
	if s.capacityWait {
		return Lease{}, ErrCapacity
	}
	slot := 0
	for used[slot] {
		slot++
	}
	// The index still has to be inside the configured ceiling. The count test
	// makes this unreachable whenever admission is sane, so it is a guard
	// against an admission that is not.
	if slot >= s.config.Capacity-s.config.LegacySlots {
		return Lease{}, ErrCapacity
	}
	var key SetKey
	var selected string
	var requestID int64
	skipClause, args := "", []any{}
	for _, set := range skip {
		skipClause += " AND NOT (j.repo_id=? AND j.set_id=?)"
		args = append(args, set.RepositoryID, set.ScaleSetID)
	}
	args = append(args, cursor)
	err = tx.QueryRowContext(ctx, selectableJobsQuery(`j.repo_id,j.set_id,j.job_key,j.runner_request_id`, reservableJobs,
		skipClause+` AND NOT EXISTS (
 SELECT 1 FROM sessions s WHERE s.repo_id=j.repo_id AND s.set_id=j.set_id AND s.phase NOT IN ('active','closed'))
ORDER BY (j.state!='assigned'),(j.repo_id<=?),j.repo_id,j.ordinal LIMIT 1`), args...).Scan(&key.RepositoryID, &key.ScaleSetID, &selected, &requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return Lease{}, ErrNoDemand
	}
	if err != nil {
		return Lease{}, err
	}
	id := uuid.NewString()
	now := timestamp()
	lease := Lease{ID: id, Slot: slot, Set: key, JobKey: selected, RequestID: requestID, Phase: Reserved,
		RunnerName: "slipway-" + s.config.Platform + "-" + id, Created: now, Updated: now}
	_, err = tx.ExecContext(ctx, `INSERT INTO leases(id,slot,repo_id,set_id,job_key,runner_request_id,phase,runner_name,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		id, slot, key.RepositoryID, key.ScaleSetID, selected, requestID, Reserved, lease.RunnerName, now, now)
	if err != nil {
		return Lease{}, err
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE jobs SET state='reserved' WHERE repo_id=? AND set_id=? AND job_key=? AND state IN ('queued','assigned')`, key.RepositoryID, key.ScaleSetID, selected)); err != nil {
		return Lease{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE platform SET cursor_repo=? WHERE id=1`, key.RepositoryID); err != nil {
		return Lease{}, err
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, err
	}
	return lease, nil
}

// Transition never releases a slot. All phases, including quarantine, count
// equally toward admission. JIT and backend creation must follow the durable
// Provisioning transition and use RunnerName/ID as their stable identities.
func (s *Store) Transition(ctx context.Context, id string, from, to Phase) error {
	if to == AcquisitionIntent || to == Provisioning {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.ready {
			return ErrInventoryUnknown
		}
		if to == AcquisitionIntent {
			if err := s.discoveryCurrent(ctx); err != nil {
				return err
			}
		}
	}
	allowed := map[Phase][]Phase{
		Reserved:           {AcquisitionIntent, CleanupPending},
		AcquisitionIntent:  {Acquired, AcquisitionUnknown, CleanupPending},
		AcquisitionUnknown: {Acquired, CleanupPending, Quarantined},
		Acquired:           {Provisioning, CleanupPending, Quarantined},
		Provisioning:       {Running, CleanupPending, Quarantined},
		Running:            {CleanupPending, Quarantined},
		Quarantined:        {CleanupPending},
		CleanupPending:     {Quarantined},
	}
	for _, next := range allowed[from] {
		if next == to {
			if to == AcquisitionIntent {
				return changedOne(s.db.ExecContext(ctx, `UPDATE leases SET phase=?,updated=? WHERE id=? AND phase=?
AND EXISTS(SELECT 1 FROM repositories r WHERE r.id=leases.repo_id AND r.eligible=1)
AND NOT EXISTS(SELECT 1 FROM managed_sets m WHERE m.repo_id=leases.repo_id AND (m.phase!='active' OR m.remote_id!=leases.set_id))`, to, timestamp(), id, from))
			}
			return changedOne(s.db.ExecContext(ctx, `UPDATE leases SET phase=?,updated=? WHERE id=? AND phase=?`, to, timestamp(), id, from))
		}
	}
	return errors.New("invalid lease transition")
}

type AcquisitionAPI interface {
	AcquireJobs(context.Context, []int64) ([]int64, error)
}

// AcquireReserved issues at most one request for this operation. Its client must
// disable transport mutation retries; the production App client does so. SDK
// token refresh after a definite 401 is distinct from replaying an unknown POST.
func (s *Store) AcquireReserved(ctx context.Context, id string, api AcquisitionAPI) error {
	lease, err := s.Lease(ctx, id)
	if err != nil {
		return err
	}
	if err := s.Transition(ctx, id, Reserved, AcquisitionIntent); err != nil {
		return err
	}
	if lease.RequestID == 0 {
		// GitHub's broker assigned this job to the scale set itself and sent no
		// runner request ID, so there is no request to acquire and no AcquireJobs
		// call to make. The evidence recorded here is the controller's own
		// inference from that assignment, never GitHub's answer -- see
		// AcquisitionSourceBrokerAssignment. A probe row has no request either,
		// and no assignment at all; it is recorded under its own source.
		source := AcquisitionSourceBrokerAssignment
		if isUnseenJobKey(lease.JobKey) {
			source = AcquisitionSourceUnseenDemand
		}
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
		defer cancel()
		return s.SettleAcquisition(saveCtx, id, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: source,
		})
	}
	accepted, apiErr := api.AcquireJobs(ctx, []int64{lease.RequestID})
	// Keep a brief independent persistence opportunity if the API context was
	// cancelled. Even a failed write retains the earlier durable intent/slot.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if apiErr != nil || len(accepted) > 1 || (len(accepted) == 1 && accepted[0] != lease.RequestID) {
		err := s.Transition(saveCtx, id, AcquisitionIntent, AcquisitionUnknown)
		return errors.Join(errors.New("acquisition outcome unknown; reservation retained, do not retry"), err)
	}
	outcome := AcquisitionAccepted
	if len(accepted) == 0 {
		outcome = AcquisitionDeclined
	}
	return s.SettleAcquisition(saveCtx, id, AcquisitionSettlement{
		JobKey: lease.JobKey, Outcome: outcome, Source: AcquisitionSourceAPI,
	})
}

// CleanupEvidence is supplied by the lifecycle reconciler, not a worker report.
// WorkerAbsent includes VM processes and writable snapshots/disks; RunnerAbsent
// requires GitHub readback. AcquisitionSettled needs positive reconciliation of
// the operation, never an aggregate count or an expired local timeout alone.
type CleanupEvidence struct {
	WorkerAbsent       bool
	RunnerAbsent       bool
	AcquisitionSettled bool
}

func (s *Store) FinishCleanup(ctx context.Context, id string, evidence CleanupEvidence) error {
	if !evidence.WorkerAbsent || !evidence.RunnerAbsent || !evidence.AcquisitionSettled {
		return errors.New("cleanup incomplete or acquisition uncertain; reservation retained")
	}
	settled, err := s.AcquisitionSettled(ctx, id)
	if err != nil {
		return err
	}
	if !settled {
		return errors.New("cleanup lacks durable acquisition evidence; reservation retained")
	}
	// QUARANTINED IS RELEASABLE HERE on the same three proofs as cleanup-pending.
	// Quarantine means the controller could not yet prove absence; once all
	// three proofs exist, that uncertainty is resolved. Only cleanup-pending was
	// accepted before, and nothing moves a quarantined lease with no live worker
	// to cleanup-pending (BeginWorkerDestroy needs a create-intent or present
	// worker), so a pre-create or cleanup-deadline quarantine held its slot
	// forever (homelab#911 follow-up).
	return changedOne(s.db.ExecContext(ctx, `UPDATE leases SET phase=?,updated=? WHERE id=? AND phase IN (?,?)`,
		Released, timestamp(), id, CleanupPending, Quarantined))
}
