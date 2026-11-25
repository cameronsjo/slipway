package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

type leaseAllocator interface {
	AcquireNext(context.Context) (Lease, error)
}

type registrationBroker interface {
	Prepare(context.Context, string) (bootstrap.Token, error)
	CleanupRegistration(context.Context, string, bool) (bool, error)
	Expire()
}

// PoolLifecycle joins the durable broker state to one platform adapter. It does
// not own discovery or queue polling; the daemon runs Registry and SessionManager
// around these bounded lifecycle operations.
type PoolLifecycle struct {
	store        *Store
	allocator    leaseAllocator
	broker       registrationBroker
	backend      WorkerBackend
	bootstrapURL string
}

func NewPoolLifecycle(store *Store, sessions *SessionManager, broker *JITBroker, backend WorkerBackend, bootstrapURL string) (*PoolLifecycle, error) {
	if sessions == nil || broker == nil {
		return nil, errors.New("pool lifecycle requires sessions and JIT broker")
	}
	return newPoolLifecycle(store, sessions, broker, backend, bootstrapURL)
}

func newPoolLifecycle(store *Store, allocator leaseAllocator, broker registrationBroker, backend WorkerBackend, bootstrapURL string) (*PoolLifecycle, error) {
	if store == nil || allocator == nil || broker == nil || backend == nil || !validBootstrapOrigin(bootstrapURL) {
		return nil, errors.New("pool lifecycle requires store, sessions, broker, backend and HTTPS bootstrap origin")
	}
	return &PoolLifecycle{store: store, allocator: allocator, broker: broker, backend: backend, bootstrapURL: bootstrapURL}, nil
}

// ReconcileInventory is the first operation after every process start and each
// lifecycle tick. Lost create responses are confirmed by exact identity; missing
// resources with a durable delete intent become positive process/storage absence.
func (l *PoolLifecycle) ReconcileInventory(ctx context.Context) error {
	l.broker.Expire()
	inventory, err := l.backend.Inventory(ctx)
	if err != nil {
		_ = l.store.ReconcileBackendInventory(ctx, BackendInventory{Complete: false})
		// The fence is unchanged; only the cause now survives. It used to be
		// dropped, which left `trusted backend inventory failed` as the whole
		// of what an operator got -- true, useless, and the reason three blind
		// rollouts were needed to find one wrong `ctr` flag.
		//
		// WHAT THIS ERROR TEXT CAN CONTAIN, audited rather than assumed:
		//   - Fixed strings from this package and from the kubernetes backend.
		//   - `exec` exit statuses and tool base names from the host witness.
		//   - A witness reason code, which is a closed vocabulary.
		// It CANNOT contain a client-go error: every Kubernetes call on the
		// inventory path already replaces the API error with a fixed string of
		// its own (see Transport.listAllPods), so no API server URL, resource
		// name or status body reaches here. It cannot contain a bootstrap
		// token either -- tokens exist only on the Create and attach paths,
		// which Inventory never enters.
		//
		// If a future change puts a client-go error on this path, sanitize it
		// AT THAT CALL, the way listAllPods already does. Do not re-drop the
		// cause here; that is the defect this replaces.
		//
		// Log-only: the monitoring report's closed vocabulary
		// (InventoryDiagnostics) is untouched.
		return fmt.Errorf("trusted backend inventory failed; platform admission fenced: %w", err)
	}
	if err := l.store.ReconcileBackendInventory(ctx, inventory); err != nil {
		return err
	}
	fence := func(err error) error {
		_ = l.store.ReconcileBackendInventory(context.WithoutCancel(ctx), BackendInventory{Complete: false})
		return err
	}
	seen := make(map[string]WorkerObservation, len(inventory.Workers))
	for _, worker := range inventory.Workers {
		seen[worker.LeaseID] = worker
		record, err := l.store.Worker(ctx, worker.LeaseID)
		if err != nil {
			return fence(err)
		}
		if record.Phase == "create-intent" {
			if err := l.store.ConfirmWorkerCreated(ctx, worker); err != nil {
				return fence(err)
			}
		}
	}
	records, err := l.store.Workers(ctx)
	if err != nil {
		return fence(err)
	}
	for _, record := range records {
		if record.Phase != "destroy-intent" {
			continue
		}
		if _, present := seen[record.LeaseID]; present {
			continue
		}
		if err := l.store.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: record.LeaseID,
			ResourceID: record.ResourceID, ProcessAbsent: true, StorageAbsent: true}); err != nil {
			return fence(err)
		}
	}
	return nil
}

// Maintain applies durable deadlines before new queue work, then makes one
// deletion attempt for each newly quarantined resource. It retries registration
// reconciliation after exact physical absence, but it cannot release the lease
// until acquisition settlement is independently known.
//
// retained counts the workers whose teardown lacked exact absence this call,
// so their reservation stays charged into the next cycle. It is the cycle
// line's retained= field: a stuck teardown is otherwise one WARN per cycle and
// a slot quietly held until the cleanup-deadline quarantine (homelab#924).
func (l *PoolLifecycle) Maintain(ctx context.Context) (retained int, err error) {
	return l.maintain(ctx, true)
}

// MaintainWorkers is Maintain without the registration cleanup loop: deadlines,
// retirements, worker teardown and finished-lease release, all store and
// backend work, with no GitHub call (homelab#951). Daemon.Cycle runs it a
// second time in a cycle whose queue poll saw a job finish, so that worker's
// teardown starts at once. Running the full Maintain there would repeat
// CleanupRegistration -- a GitHub read and maybe a delete -- for every lease
// stuck in registration cleanup, every cycle that has a completion.
//
// Finished-lease release stays in: PendingFinishedCleanup selects only leases
// whose registration an earlier full pass already proved absent
// (j.removal='absent'), and FinishWorkerCleanup is a store write. So it cannot
// release anything the full pass would not, and it may free a slot that the
// first pass's registration cleanup finished moments before.
func (l *PoolLifecycle) MaintainWorkers(ctx context.Context) (retained int, err error) {
	return l.maintain(ctx, false)
}

func (l *PoolLifecycle) maintain(ctx context.Context, registration bool) (retained int, err error) {
	if _, err := l.store.ReconcileDeadlines(ctx, time.Now()); err != nil {
		return retained, err
	}
	// Leases that stopped before provisioning had no deadline at all
	// (homelab#1095). A store write only; no GitHub or backend call.
	if _, err := l.store.ReleaseStrandedLeases(ctx, time.Now()); err != nil {
		return retained, err
	}
	// Nor did a provisioning lease interrupted between its JIT row and its
	// worker row. This only moves it into the ordinary registration cleanup.
	if _, err := l.store.RetireUnstartedProvisioning(ctx, time.Now()); err != nil {
		return retained, err
	}
	// Before the timeout sweep, because it decides the same leases on stronger
	// evidence: a worker the last trusted inventory saw finish does not need
	// five more minutes of the clock to prove it is not going to start a job.
	if _, err := l.store.RetireTerminatedWorkers(ctx, time.Now()); err != nil {
		return retained, err
	}
	// An inherited broker assignment nothing re-confirmed is aged out by
	// journaledDemand, not here. It must not be DELETED: leases is a foreign key
	// into jobs and a released lease still references its job, so the delete
	// raised `FOREIGN KEY constraint failed` out of this function -- and a
	// Maintain error aborts the whole cycle below, stopping the pool.
	if _, err := l.store.RetireUnusedWorkers(ctx, time.Now()); err != nil {
		return retained, err
	}
	workers, err := l.store.PendingWorkerRetirements(ctx)
	if err != nil {
		return retained, err
	}
	// ONE WAIT BUDGET FOR ALL OF THIS PASS'S TEARDOWNS (#960 learning): see
	// maintainAbsenceBudget. Scoped to the destroy loop only.
	destroyCtx := WithAbsenceBudget(ctx, time.Now().Add(maintainAbsenceBudget))
	for _, worker := range workers {
		planned, err := l.store.BeginWorkerDestroy(ctx, worker.LeaseID)
		if err != nil {
			return retained, err
		}
		kept, err := l.teardown(destroyCtx, planned, false)
		if err != nil {
			return retained, err
		}
		retained += kept
	}
	// Workers whose delete is still unproven past the backoff (homelab#1094).
	// See PendingWorkerRedeletes: without this, one failed delete held its slot
	// until someone removed the Pod by hand.
	redeletes, err := l.store.PendingWorkerRedeletes(ctx, time.Now())
	if err != nil {
		return retained, err
	}
	for _, worker := range redeletes {
		if err := l.store.NoteWorkerRedelete(ctx, worker, time.Now()); err != nil {
			return retained, err
		}
		slog.Warn("Re-sending the delete for a worker whose teardown outlived its cleanup deadline.",
			"lease_id", worker.LeaseID, "resource_id", worker.ResourceID)
		kept, err := l.teardown(destroyCtx, worker, true)
		if err != nil {
			return retained, err
		}
		retained += kept
	}
	var stuck []error
	if registration {
		stuck, err = l.cleanupRegistrations(ctx)
		if err != nil {
			return retained, err
		}
	}
	leaseIDs, err := l.store.PendingFinishedCleanup(ctx)
	if err != nil {
		return retained, errors.Join(append(stuck, err)...)
	}
	for _, leaseID := range leaseIDs {
		if err := l.store.FinishWorkerCleanup(ctx, leaseID, true, true); err != nil {
			stuck = append(stuck, err)
		}
	}
	return retained, errors.Join(stuck...)
}

// teardown sends one worker's delete and records the absence it proves. It
// returns 1 when the worker's reservation stays retained into the next cycle.
//
// ONE WORKER'S FAILED DELETE MUST NOT STOP THE CYCLE (homelab#1094). A Destroy
// error used to return from Maintain, which failed the whole cycle and every
// repository's provisioning with it -- the #911 shape again, for a different
// error. The error is now logged with the worker's identity and counted in
// retained; the worker stays in destroy-intent with its lease charged, and
// PendingWorkerRedeletes sends the delete again after the backoff.
//
// A journal error is still returned: it says the controller's own state cannot
// be trusted, which is not one worker's trouble.
func (l *PoolLifecycle) teardown(ctx context.Context, worker WorkerObservation, retry bool) (int, error) {
	absence, err := l.backend.Destroy(ctx, worker)
	if err != nil {
		slog.Warn("Worker deletion outcome is uncertain; retaining its reservation and sending the delete again after the backoff.",
			"lease_id", worker.LeaseID, "resource_id", worker.ResourceID, "retry", retry,
			"backoff", redeleteBackoff.String(), "error", err)
		return 1, nil
	}
	if err := l.store.ConfirmWorkerAbsent(ctx, absence); err != nil {
		// WHICH proof was missing. The two are independent and they mean
		// different things: process absence is false while the Kata sandbox
		// or the Pod object still exists, storage absence while a writable
		// snapshot survives. Reported together, a Pod that is merely still
		// terminating looks identical to a snapshot that leaked -- the
		// first resolves itself on the next cycle and the second never does.
		if errors.Is(err, ErrAbsenceNotProven) {
			// ONE WORKER STILL TEARING DOWN MUST NOT ZERO OUT THE WHOLE
			// CYCLE'S ADVERTISEMENT. This used to return here, which failed
			// Maintain and, with it, every repository's advertisement for
			// the cycle -- on a two-slot pool with a one-minute cycle, a
			// single pod's ordinary termination cost a full minute of zero
			// capacity offered anywhere (homelab#911). The reservation stays
			// retained (the safety invariant), but only THIS worker's
			// retirement is deferred to the next cycle; other workers'
			// teardown, registration cleanup, and finished-lease cleanup
			// below all still run.
			slog.Warn("Worker teardown lacks exact absence; retaining its reservation for the next cycle.",
				"lease_id", worker.LeaseID, "resource_id", worker.ResourceID, "retry", retry,
				"process_absent", absence.ProcessAbsent, "storage_absent", absence.StorageAbsent)
			return 1, nil
		}
		return 0, fmt.Errorf(
			"quarantined worker deletion lacks exact absence; retain reservation (process_absent=%t storage_absent=%t): %w",
			absence.ProcessAbsent, absence.StorageAbsent, err)
	}
	if retry {
		slog.Info("A re-sent delete proved the worker absent.", "lease_id", worker.LeaseID, "resource_id", worker.ResourceID)
	}
	return 0, nil
}

// cleanupRegistrations deletes the GitHub registration of every lease whose
// worker is proven absent. It is the only part of Maintain that calls GitHub.
func (l *PoolLifecycle) cleanupRegistrations(ctx context.Context) ([]error, error) {
	leaseIDs, err := l.store.PendingRegistrationCleanup(ctx)
	if err != nil {
		return nil, err
	}
	// ONE STUCK LEASE MUST NOT STOP EVERY RELEASE. This returned on the first
	// lease it could not reconcile, so a single registration wedged at
	// `remove-intent` held every other lease's cleanup behind it -- and on a
	// one-slot pilot that is a silent, permanent outage that survives restart,
	// because the state is in SQLite. Each lease is now attempted and the
	// diagnostics are joined; the reservation is still retained for every lease
	// that did not reconcile, which is the property that mattered.
	var stuck []error
	for _, leaseID := range leaseIDs {
		absent, err := l.broker.CleanupRegistration(ctx, leaseID, true)
		if err != nil || !absent {
			stuck = append(stuck, fmt.Errorf(
				"quarantined runner registration absence is not reconciled for one lease; retain reservation: %w",
				cleanupCause(err)))
		}
	}
	return stuck, nil
}

// cleanupCause keeps a joined diagnostic honest when the broker refused
// without an error of its own. `absent=false, err=nil` is a real outcome --
// the registration is still there -- and reporting it as a nil cause would
// print a dangling colon and say nothing.
func cleanupCause(err error) error {
	if err != nil {
		return err
	}
	return errors.New("the broker reported the registration still present")
}

// ProvisionNext reserves and acquires through SessionManager, prepares one JIT
// transfer, persists backend intent, then calls Create exactly once. An
// AcquisitionUnknown result returns before JIT preparation: this controller
// has no safe way to prove that a new worker belongs to the uncertain POST, so
// the lease remains fenced for live/operator reconciliation. Any error after
// a later intent remains a charged reconciliation obligation.
func (l *PoolLifecycle) ProvisionNext(ctx context.Context) (Lease, error) {
	lease, err := l.allocator.AcquireNext(ctx)
	if err != nil {
		return lease, err
	}
	// A SHUTDOWN IS NOT A PROVISIONING FAILURE. SIGTERM cancels the daemon
	// context, and every create in flight then fails here at once. Each used
	// to write a provisioning_failures row, so a restart during a four-wide
	// burst wrote four, tripped the breaker, and the new pod inherited the
	// fifteen-minute quarantine from SQLite. Cancellation (not a deadline:
	// a refill deadline or the cycle budget expiring still counts) takes the same
	// quarantine, so the lease and its JIT are cleaned up as before, minus
	// the row.
	interrupted := func() bool { return errors.Is(ctx.Err(), context.Canceled) }
	quarantineBeforeCreate := func(diagnostic string) error {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
		defer cancel()
		if interrupted() {
			return l.store.QuarantineInterruptedBeforeBackendCreate(saveCtx, lease.ID, diagnostic+"; interrupted by controller shutdown", time.Now())
		}
		return l.store.QuarantineBeforeBackendCreate(saveCtx, lease.ID, diagnostic, time.Now())
	}
	token, err := l.broker.Prepare(ctx, lease.ID)
	if err != nil {
		return lease, errors.Join(err, quarantineBeforeCreate("JIT preparation failed before backend creation"))
	}
	intent, err := l.store.BeginWorkerCreate(ctx, lease.ID, l.bootstrapURL, token)
	if err != nil {
		return lease, errors.Join(err, quarantineBeforeCreate("worker create intent failed before backend creation"))
	}
	worker, createErr := l.backend.Create(ctx, intent)
	intent.BootstrapToken = bootstrap.Token{}
	quarantine := func(diagnostic string) error {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
		defer cancel()
		if interrupted() {
			return l.store.QuarantineInterruptedCreate(saveCtx, lease.ID, diagnostic+"; interrupted by controller shutdown", time.Now())
		}
		return l.store.QuarantineProvisionFailure(saveCtx, lease.ID, diagnostic, time.Now())
	}
	if errors.Is(createErr, ErrCapacity) {
		// A BUSY NODE IS NOT A BROKEN ONE. This used to take the path below:
		// the lease was quarantined, a provisioning_failures row was written,
		// and three of those inside fifteen minutes tripped a breaker that
		// silenced the whole pool for another fifteen. Measured on MS-A2
		// 2026-09-21, from a legacy CronJob rollover that briefly doubled one
		// pod's memory.
		//
		// The reservation is safe to give up because the backend checks
		// headroom BEFORE it marks a create attempt and before it issues the
		// Pod POST, so nothing physical exists. The JIT minted above is a real
		// GitHub registration, though, so this takes the SAME cleanup
		// obligation the other pre-create failures take -- the lease is
		// set to `cleanup-pending` for `Maintain` to revoke the transfer and
		// delete the registration, and the slot is released once that is
		// proven. NOT quarantined: quarantine is for uncertainty an operator
		// resolves, and a wait is not uncertain. What it
		// does NOT take is the failure row.
		deferErr := func() error {
			saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
			defer cancel()
			return l.store.DeferProvisionForCapacity(saveCtx, lease.ID, "headroom is exhausted; waiting for a slot", time.Now())
		}()
		if deferErr != nil {
			// The wait itself failed to persist, so the lease is still
			// provisioning with a minted JIT and nobody has been told. That IS
			// a cycle failure, and it must NOT carry ErrCapacity -- the daemon
			// treats that as an orderly stop and would swallow it.
			return lease, fmt.Errorf("worker creation deferred but the wait was not recorded: %v: %w",
				createErr, deferErr)
		}
		return lease, fmt.Errorf("worker creation deferred; no headroom this cycle: %w", createErr)
	}
	if errors.Is(createErr, ErrWitnessRefusedBeforeCreate) {
		// A WITNESS REFUSAL BEFORE THE POST IS NOT EVIDENCE THE CREATE PATH IS
		// BROKEN (homelab#1213). Three storage refusals during bursts on
		// 2026-10-01 each wrote a provisioning_failures row, the breaker opened,
		// and admission sat at zero for fifteen minutes while the cycle line
		// read ready=true. The backend tags this error only before its
		// create-attempt marker, so nothing physical exists, and the lease takes
		// the same rollback a capacity wait takes: cleanup-pending, intent
		// absent, JIT revoked and its registration deleted by Maintain. It does
		// not set the capacity latch; a refusal says nothing about free slots.
		//
		// Not counted, but not hidden either. Unlike ErrCapacity this is
		// returned as a cycle error, so it is logged with the witness's reason
		// and the pass stops; the next cycle retries. A witness that refuses on
		// every cycle is not left to the breaker: the cycle's own inventory
		// reads the same witness and fences ready=false.
		deferErr := func() error {
			saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
			defer cancel()
			return l.store.RollBackAfterWitnessRefusal(saveCtx, lease.ID, "host witness refused the pre-create inventory; retrying next cycle", time.Now())
		}()
		if deferErr != nil {
			return lease, fmt.Errorf("worker creation refused by the host witness but the rollback was not recorded: %v: %w",
				createErr, deferErr)
		}
		return lease, fmt.Errorf("worker creation not attempted; not counted toward the provisioning breaker: %w", createErr)
	}
	if createErr != nil {
		// The cause used to be dropped here, exactly as the inventory fence
		// dropped its own before #843. Creation is five distinct operations
		// (Transport.Create's stage tags) and this reported one sentence for
		// all of them, so a real failure on the first Linux job said only
		// `worker creation outcome uncertain`.
		//
		// WHAT THIS TEXT CAN CONTAIN, audited rather than assumed. Every error
		// Transport.Create returns is a fixed string, a create-stage tag, or
		// the trusted-inventory tree already audited for the fence path. The
		// two that could have carried more are deliberately constants at their
		// source: the Pod API create error, whose request body is a worker Pod
		// spec, and the bootstrap ATTACH error, whose bytes in flight are the
		// single-use capability itself. Neither is wrapped there and neither
		// may become wrapped; the leak canary pins both.
		//
		// The DIAGNOSTIC persisted to the journal stays the short constant. It
		// is a stored, operator-visible field with its own vocabulary, and this
		// change is log-only.
		quarantineErr := quarantine("worker creation outcome uncertain")
		return lease, errors.Join(
			fmt.Errorf("worker creation outcome uncertain; retain intent and reservation: %w", createErr),
			quarantineErr)
	}
	if err := l.store.ConfirmWorkerCreated(ctx, worker); err != nil {
		quarantineErr := quarantine("worker creation identity invalid")
		return lease, errors.Join(
			fmt.Errorf("worker creation identity invalid; retain intent and reservation: %w", err),
			quarantineErr)
	}
	// The one line that ties a worker to its lease at birth. Without it a lease
	// stuck in the journal could not be traced to its Pod from logs alone
	// (homelab#996).
	slog.Info("Created a worker for a lease.",
		"lease_id", lease.ID, "resource_id", worker.ResourceID, "runner_name", lease.RunnerName,
		"repository_id", lease.Set.RepositoryID, "scale_set_id", lease.Set.ScaleSetID, "job_key", lease.JobKey)
	return lease, nil
}

// DestroyWorker persists one delete intent before the adapter call.
// ReconcileInventory settles exact absence after an unknown response; Maintain's
// redelete loop, not this method, re-sends the delete after the backoff.
func (l *PoolLifecycle) DestroyWorker(ctx context.Context, leaseID string, acquisitionSettled bool) error {
	worker, err := l.store.BeginWorkerDestroy(ctx, leaseID)
	if err != nil {
		return err
	}
	absence, destroyErr := l.backend.Destroy(ctx, worker)
	if destroyErr != nil {
		return fmt.Errorf("worker deletion outcome uncertain; retain intent and reservation: %w", destroyErr)
	}
	if err := l.store.ConfirmWorkerAbsent(ctx, absence); err != nil {
		return fmt.Errorf(
			"worker deletion lacks exact absence; retain reservation (process_absent=%t storage_absent=%t): %w",
			absence.ProcessAbsent, absence.StorageAbsent, err)
	}
	return l.FinishReconciledCleanup(ctx, leaseID, acquisitionSettled)
}

// FinishReconciledCleanup removes the exact GitHub registration only after the
// backend's durable absence observation, then combines all three cleanup proofs.
func (l *PoolLifecycle) FinishReconciledCleanup(ctx context.Context, leaseID string, acquisitionSettled bool) error {
	workerAbsent, err := l.store.WorkerAbsent(ctx, leaseID)
	if err != nil || !workerAbsent {
		return errors.New("worker absence is not durably reconciled; retain reservation")
	}
	runnerAbsent, err := l.broker.CleanupRegistration(ctx, leaseID, true)
	if err != nil || !runnerAbsent {
		return errors.New("runner registration absence is not reconciled; retain reservation")
	}
	return l.store.FinishWorkerCleanup(ctx, leaseID, true, acquisitionSettled)
}
