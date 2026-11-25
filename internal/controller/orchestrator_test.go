package controller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

type allocatorFunc func(context.Context) (Lease, error)

func (f allocatorFunc) AcquireNext(ctx context.Context) (Lease, error) { return f(ctx) }

type brokerFixture struct {
	prepare func(context.Context, string) (bootstrap.Token, error)
	cleanup func(context.Context, string, bool) (bool, error)
	expires int
}

func (b *brokerFixture) Prepare(ctx context.Context, id string) (bootstrap.Token, error) {
	return b.prepare(ctx, id)
}
func (b *brokerFixture) CleanupRegistration(ctx context.Context, id string, absent bool) (bool, error) {
	return b.cleanup(ctx, id, absent)
}
func (b *brokerFixture) Expire() { b.expires++ }

type backendFixture struct {
	inventory    BackendInventory
	inventoryErr error
	create       func(context.Context, WorkerCreateIntent) (WorkerObservation, error)
	destroy      func(context.Context, WorkerObservation) (WorkerAbsence, error)
	creates      int
	destroys     int
}

func (b *backendFixture) Inventory(context.Context) (BackendInventory, error) {
	return b.inventory, b.inventoryErr
}
func (b *backendFixture) Create(ctx context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
	b.creates++
	return b.create(ctx, intent)
}
func (b *backendFixture) Destroy(ctx context.Context, worker WorkerObservation) (WorkerAbsence, error) {
	b.destroys++
	return b.destroy(ctx, worker)
}

func acquiredLease(t *testing.T, s *Store) Lease {
	t.Helper()
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
		JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI,
	}); err != nil {
		t.Fatal(err)
	}
	lease.Phase = Acquired
	return lease
}

func lifecycleFixtures(t *testing.T, s *Store, lease Lease) (*brokerFixture, *backendFixture, *PoolLifecycle) {
	t.Helper()
	allocator := allocatorFunc(func(context.Context) (Lease, error) { return lease, nil })
	broker := &brokerFixture{}
	broker.prepare = func(ctx context.Context, id string) (bootstrap.Token, error) {
		if id != lease.ID {
			t.Fatal("broker received wrong lease")
		}
		token, err := bootstrap.NewToken()
		if err != nil {
			return token, err
		}
		if err := s.Transition(ctx, id, Acquired, Provisioning); err != nil {
			return token, err
		}
		hash := token.Hash()
		now := timestamp()
		_, err = s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,created,expires,updated)
VALUES(?,?,'ready',?,?,?)`, id, hash[:], now, time.Now().Add(workerStartupTimeout).UTC().Format(time.RFC3339Nano), now)
		return token, err
	}
	broker.cleanup = func(context.Context, string, bool) (bool, error) { return true, nil }
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
	lifecycle, err := newPoolLifecycle(s, allocator, broker, backend, "https://controller.example")
	if err != nil {
		t.Fatal(err)
	}
	return broker, backend, lifecycle
}

func TestPoolLifecyclePersistsCreateBeforeBackendAndReconcilesLostResponse(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		record, err := s.Worker(ctx, lease.ID)
		if err != nil || record.Phase != "create-intent" || record.ResourceID != lease.RunnerName {
			t.Fatalf("backend call preceded durable intent: %+v %v", record, err)
		}
		if intent.LeaseID != lease.ID || intent.ResourceID != lease.RunnerName {
			t.Fatalf("backend received wrong identity: %+v", intent)
		}
		return WorkerObservation{}, errors.New("lost create response")
	}
	if got, err := lifecycle.ProvisionNext(ctx); err == nil || got.ID != lease.ID {
		t.Fatalf("lost create response: %+v %v", got, err)
	}
	if backend.creates != 1 {
		t.Fatal("backend create was not called exactly once")
	}
	if record, err := s.Worker(ctx, lease.ID); err != nil || record.Phase != "create-intent" {
		t.Fatalf("uncertain create intent was not retained: %+v %v", record, err)
	}
	backend.inventory = BackendInventory{Complete: true,
		Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if record, err := s.Worker(ctx, lease.ID); err != nil || record.Phase != "present" {
		t.Fatalf("inventory did not settle exact create: %+v %v", record, err)
	}
	if backend.creates != 1 || broker.expires != 1 {
		t.Fatal("reconciliation replayed create or skipped capability expiry")
	}
}

func TestPoolLifecycleDestroyIsNotReplayedAndInventorySettlesAbsence(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
	}
	if _, err := lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		record, err := s.Worker(ctx, worker.LeaseID)
		if err != nil || record.Phase != "destroy-intent" {
			t.Fatalf("backend deletion preceded durable intent: %+v %v", record, err)
		}
		return WorkerAbsence{}, errors.New("lost delete response")
	}
	if err := lifecycle.DestroyWorker(ctx, lease.ID, true); err == nil {
		t.Fatal("lost delete response was accepted")
	}
	if err := lifecycle.DestroyWorker(ctx, lease.ID, true); err == nil || backend.destroys != 1 {
		t.Fatalf("uncertain delete was replayed: calls=%d err=%v", backend.destroys, err)
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	broker.cleanup = func(_ context.Context, id string, workerAbsent bool) (bool, error) {
		if id != lease.ID || !workerAbsent {
			t.Fatal("registration cleanup lacked exact physical absence")
		}
		return true, nil
	}
	if err := lifecycle.FinishReconciledCleanup(ctx, lease.ID, true); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("reconciled cleanup did not release lease: %+v %v", stored, err)
	}
}

func TestPoolLifecycleQuarantinesAndRetiresUncertainCreate(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, errors.New("lost create response")
	}
	if _, err := lifecycle.ProvisionNext(ctx); err == nil {
		t.Fatal("uncertain create was accepted")
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Quarantined {
		t.Fatalf("uncertain create was not quarantined: %+v %v", stored, err)
	}
	var budget time.Time
	var budgeted bool
	backend.destroy = func(destroyCtx context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		budget, budgeted = AbsenceBudget(destroyCtx)
		record, err := s.Worker(ctx, lease.ID)
		if err != nil || record.Phase != "destroy-intent" || worker.LeaseID != lease.ID || worker.ResourceID != lease.RunnerName {
			t.Fatalf("retirement preceded durable delete intent: %+v %+v %v", worker, record, err)
		}
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
	cleanupCalls := 0
	broker.cleanup = func(_ context.Context, id string, absent bool) (bool, error) {
		cleanupCalls++
		if id != lease.ID || !absent {
			return false, nil
		}
		_, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, id)
		return err == nil, err
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if backend.destroys != 1 || cleanupCalls != 1 {
		t.Fatalf("destroy=%d cleanup=%d", backend.destroys, cleanupCalls)
	}
	// #960: Destroy runs under the pass's shared absence budget, no longer than
	// maintainAbsenceBudget from now.
	if !budgeted || time.Until(budget) > maintainAbsenceBudget || time.Until(budget) < -time.Minute {
		t.Fatalf("Destroy ran without the shared absence budget: budget=%v set=%t", budget, budgeted)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("reconciled uncertain create retained its slot: %+v %v", stored, err)
	}
	if absent, err := s.WorkerAbsent(ctx, lease.ID); err != nil || !absent {
		t.Fatalf("retirement did not persist physical absence: %t %v", absent, err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil || backend.destroys != 1 {
		t.Fatalf("maintenance replayed deletion: calls=%d err=%v", backend.destroys, err)
	}
}

// acquiredLeases is the multi-repository sibling of acquiredLease: it drives
// n leases through Reserved/AcquisitionIntent/Acquired on n distinct
// repositories so a fixture can hold several charged reservations at once.
func acquiredLeases(t *testing.T, s *Store, n int) []Lease {
	t.Helper()
	keys := prepare(t, s, n)
	markManaged(t, s, keys)
	for _, key := range keys {
		offer(t, s, key, 1)
	}
	leases := make([]Lease, 0, n)
	for range n {
		lease := mustReserve(t, s)
		if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
			t.Fatal(err)
		}
		if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI,
		}); err != nil {
			t.Fatal(err)
		}
		lease.Phase = Acquired
		leases = append(leases, lease)
	}
	return leases
}

// TestMaintainRetainsOneWorkerReservationWithoutFailingTheCycle is homelab#911:
// one quarantined worker whose backend Destroy cannot yet prove BOTH physical
// process and storage absence (it is still tearing down) must not abort
// Maintain. The reservation for THAT worker stays charged -- the safety
// invariant -- but every other worker's teardown in the same cycle still
// completes, and Maintain itself must report success so the daemon goes on to
// advertise capacity for every other repository this cycle.
func TestMaintainRetainsOneWorkerReservationWithoutFailingTheCycle(t *testing.T) {
	s, _ := openTestStore(t, 2)
	leases := acquiredLeases(t, s, 2)
	stillTearingDown, fullyGone := leases[0], leases[1]

	queue := append([]Lease{}, leases...)
	allocator := allocatorFunc(func(context.Context) (Lease, error) {
		if len(queue) == 0 {
			return Lease{}, errors.New("fixture exhausted")
		}
		next := queue[0]
		queue = queue[1:]
		return next, nil
	})
	broker := &brokerFixture{}
	broker.prepare = func(ctx context.Context, id string) (bootstrap.Token, error) {
		token, err := bootstrap.NewToken()
		if err != nil {
			return token, err
		}
		if err := s.Transition(ctx, id, Acquired, Provisioning); err != nil {
			return token, err
		}
		hash := token.Hash()
		now := timestamp()
		_, err = s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,created,expires,updated)
VALUES(?,?,'ready',?,?,?)`, id, hash[:], now, time.Now().Add(workerStartupTimeout).UTC().Format(time.RFC3339Nano), now)
		return token, err
	}
	cleanupCalls := 0
	broker.cleanup = func(_ context.Context, id string, absent bool) (bool, error) {
		cleanupCalls++
		if !absent {
			return false, nil
		}
		_, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, id)
		return err == nil, err
	}
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 2}}
	// Every backend Create fails, uncertainly, which is how the existing
	// fixtures (see TestPoolLifecycleQuarantinesAndRetiresUncertainCreate)
	// drive a lease to Quarantined and its worker to a pending retirement --
	// the state Maintain's destroy loop acts on.
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, errors.New("lost create response")
	}
	lifecycle, err := newPoolLifecycle(s, allocator, broker, backend, "https://controller.example")
	if err != nil {
		t.Fatal(err)
	}
	for range leases {
		if _, err := lifecycle.ProvisionNext(ctx); err == nil {
			t.Fatal("uncertain create was accepted")
		}
	}
	for _, lease := range leases {
		if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Quarantined {
			t.Fatalf("lease %s was not quarantined: %+v %v", lease.ID, stored, err)
		}
	}
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		if worker.LeaseID == stillTearingDown.ID {
			// Neither proof holds yet: the pod is still terminating. This is
			// the exact shape the daemon observed in production, and it must
			// retain the reservation, not fail the cycle.
			return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID}, nil
		}
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}

	retained, err := lifecycle.Maintain(ctx)
	if err != nil {
		t.Fatalf("one worker's unproven absence failed the whole cycle: %v", err)
	}
	// homelab#924: the retained reservation used to be one WARN line and
	// nothing else. The count is what the cycle line prints as retained=.
	if retained != 1 {
		t.Fatalf("Maintain reported %d retained reservations, want exactly the one still tearing down", retained)
	}
	if backend.destroys != 2 {
		t.Fatalf("not every worker's teardown was attempted: destroys=%d", backend.destroys)
	}
	if stored, err := s.Lease(ctx, stillTearingDown.ID); err != nil || stored.Phase == Released {
		t.Fatalf("unproven worker's reservation was released early: %+v %v", stored, err)
	}
	if record, err := s.Worker(ctx, stillTearingDown.ID); err != nil || record.Phase != "destroy-intent" {
		t.Fatalf("unproven worker was not left for the next cycle's retry: %+v %v", record, err)
	}
	if stored, err := s.Lease(ctx, fullyGone.ID); err != nil || stored.Phase != Released {
		t.Fatalf("the other worker's teardown did not complete this cycle: %+v %v", stored, err)
	}
	if cleanupCalls == 0 {
		t.Fatal("registration cleanup for the fully-absent worker never ran")
	}

	// The retained worker is already at 'destroy-intent', so Maintain's own
	// destroy loop will not touch it again (BeginWorkerDestroy only starts a
	// fresh retirement from 'create-intent'/'present' -- see
	// TestPoolLifecycleDestroyIsNotReplayedAndInventorySettlesAbsence, same
	// invariant). Its retry path is the *next* cycle's ReconcileInventory: once
	// the backend's trusted listing no longer includes the resource, a durable
	// delete intent plus its absence from inventory is exact proof, and that
	// resolves the retained reservation without ever calling backend.Destroy
	// again.
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatalf("next-cycle reconcile failed: %v", err)
	}
	if backend.destroys != 2 {
		t.Fatalf("reconcile replayed a destroy call instead of trusting inventory absence: destroys=%d", backend.destroys)
	}
	if retained, err := lifecycle.Maintain(ctx); err != nil || retained != 0 {
		t.Fatalf("cleanup after settled absence: retained=%d err=%v", retained, err)
	}
	if stored, err := s.Lease(ctx, stillTearingDown.ID); err != nil || stored.Phase != Released {
		t.Fatalf("retained worker was not released once absence was finally proven: %+v %v", stored, err)
	}
}

// TestAGenuineDestroyErrorIsRetainedLoudlyNotSilently replaces the test that
// pinned the opposite (homelab#1094). A real backend failure used to fail
// Maintain, and so the cycle, so that it would be reported. But the worker was
// never selected again after that one failure, so the report was one cycle
// line and the slot was lost for good; and with retries, failing the cycle on
// each one would stop every repository's provisioning every backoff.
//
// What the old test guarded still holds: the failure is not silent. It is a
// WARN naming the lease and the error, it counts in retained (the cycle line's
// retained= field), and it is not misread as ErrAbsenceNotProven.
func TestAGenuineDestroyErrorIsRetainedLoudlyNotSilently(t *testing.T) {
	logs := captureLogs(t)
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, errors.New("lost create response")
	}
	if _, err := lifecycle.ProvisionNext(ctx); err == nil {
		t.Fatal("uncertain create was accepted")
	}
	backend.destroy = func(_ context.Context, _ WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{}, errors.New("backend deletion failed")
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	retained, err := lifecycle.Maintain(ctx)
	if err != nil {
		t.Fatalf("one worker's destroy error failed the whole cycle: %v", err)
	}
	if retained != 1 {
		t.Fatalf("the failed destroy was not counted as retained: %d", retained)
	}
	if !strings.Contains(logs.String(), "Worker deletion outcome is uncertain") ||
		!strings.Contains(logs.String(), "lease_id="+lease.ID) || !strings.Contains(logs.String(), "backend deletion failed") {
		t.Fatalf("the destroy error was not logged with its lease and cause: %s", logs.String())
	}
	if strings.Contains(logs.String(), "lacks exact absence") {
		t.Fatalf("a genuine backend error was misread as the retained-reservation condition: %s", logs.String())
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase == Released {
		t.Fatalf("a worker whose delete failed was released: %+v %v", stored, err)
	}
}

func TestPoolLifecycleCleansRegistrationWhenCreateIntentFails(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker, backend, lifecycle := lifecycleFixtures(t, s, lease)
	if _, err := s.db.Exec(`CREATE TRIGGER reject_worker_intent BEFORE INSERT ON worker_resources BEGIN SELECT RAISE(FAIL,'fixture refusal'); END`); err != nil {
		t.Fatal(err)
	}
	cleanupCalls := 0
	broker.cleanup = func(_ context.Context, id string, absent bool) (bool, error) {
		cleanupCalls++
		if id != lease.ID || !absent {
			return false, errors.New("wrong cleanup identity")
		}
		_, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, id)
		return err == nil, err
	}
	if _, err := lifecycle.ProvisionNext(ctx); err == nil {
		t.Fatal("failed worker intent was accepted")
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Quarantined {
		t.Fatalf("pre-create failure was not quarantined: %+v %v", stored, err)
	}
	if _, err := s.Worker(ctx, lease.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed intent created a worker record: %v", err)
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if cleanupCalls != 1 || backend.destroys != 0 {
		t.Fatalf("cleanup calls=%d destroy calls=%d", cleanupCalls, backend.destroys)
	}
}

func TestPoolLifecycleRetriesRegistrationCleanupAfterPhysicalAbsence(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
	}
	if _, err := lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
	cleanupCalls := 0
	broker.cleanup = func(_ context.Context, _ string, _ bool) (bool, error) {
		cleanupCalls++
		if cleanupCalls == 1 {
			return false, errors.New("temporary lookup failure")
		}
		_, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, lease.ID)
		return err == nil, err
	}
	if err := lifecycle.DestroyWorker(ctx, lease.ID, false); err == nil {
		t.Fatal("unverified registration cleanup was accepted")
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if cleanupCalls != 2 || backend.destroys != 1 {
		t.Fatalf("cleanup calls=%d destroy calls=%d", cleanupCalls, backend.destroys)
	}
}

func TestPoolLifecycleRetiresCompletedWorkerAndReleasesSettledSlot(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
	}
	if _, err := lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
	broker.cleanup = func(_ context.Context, id string, absent bool) (bool, error) {
		if id != lease.ID || !absent {
			return false, errors.New("wrong cleanup identity")
		}
		_, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, id)
		return err == nil, err
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("settled completed worker retained its slot: %+v %v", stored, err)
	}
}

func TestPoolLifecycleInventoryFailureClosesAdmission(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.inventoryErr = errors.New("backend unavailable")
	if err := lifecycle.ReconcileInventory(ctx); err == nil {
		t.Fatal("backend inventory failure was hidden")
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("inventory failure did not close admission: %v", err)
	}
}

// A BUSY NODE IS NOT A BROKEN ONE. Measured on MS-A2 2026-09-21: a legacy
// CronJob rollover briefly doubled one pod's memory, the pool's Create found no
// free slot, and the controller recorded a provisioning failure. Three of those
// inside fifteen minutes tripped the breaker and silenced the pool for another
// fifteen.
func TestNoHeadroomIsAWaitAndNeverCountsTowardTheBreaker(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("Kubernetes/Kata headroom is exhausted (short=memory): %w", ErrCapacity)
	}
	_, err := lifecycle.ProvisionNext(ctx)
	if err == nil {
		t.Fatal("a deferral reported success")
	}
	// The daemon stops its provisioning loop on ErrCapacity WITHOUT reporting a
	// failed cycle, so the sentinel has to survive the wrapping.
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("the capacity sentinel did not survive: %v", err)
	}
	if !strings.Contains(err.Error(), "short=memory") {
		t.Fatalf("the binding dimension was lost: %v", err)
	}

	var failures int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provisioning_failures`).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 0 {
		t.Fatalf("a capacity wait recorded %d provisioning failure(s); three of these trip the breaker", failures)
	}
	var quarantineUntil string
	if err := s.db.QueryRowContext(ctx, `SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&quarantineUntil); err != nil {
		t.Fatal(err)
	}
	if quarantineUntil != "" {
		t.Fatalf("a capacity wait armed the breaker: %q", quarantineUntil)
	}

	// The minted JIT is a live GitHub registration, so the lease must carry a
	// cleanup obligation -- and it must be one that ENDS. `cleanup-pending`,
	// not `quarantined`: quarantine is for uncertainty an operator resolves,
	// and nothing here is uncertain. A quarantined deferral was a slot that
	// never came back (see TestACapacityDeferralReleasesItsSlot...).
	lease, err = s.Lease(ctx, lease.ID)
	if err != nil || lease.Phase != CleanupPending {
		t.Fatalf("the lease did not take a terminating cleanup obligation: %+v %v", lease, err)
	}
	pending, err := s.PendingRegistrationCleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != lease.ID {
		t.Fatalf("the unused JIT registration is not queued for cleanup: %v", pending)
	}
}

func TestADeferralThatCannotBeRecordedIsStillAFailedCycle(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("headroom is exhausted (short=cpu): %w", ErrCapacity)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := lifecycle.ProvisionNext(ctx)
	if err == nil {
		t.Fatal("an unrecorded deferral reported success")
	}
	// ErrCapacity must NOT survive here. The daemon treats it as an orderly
	// stop, which would swallow a lease left provisioning with a minted JIT.
	if errors.Is(err, ErrCapacity) {
		t.Fatalf("an unrecorded deferral would be swallowed as an orderly stop: %v", err)
	}
}
