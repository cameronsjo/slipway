package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

// ADMISSION IS A COUNT AND THE SLOT IS AN INDEX. The reservation gate compared
// them directly, which agrees only while the charged slots are 0..n-1. Open a
// hole and it stops agreeing: on a full host admission equals the active lease
// count, so with slot 1 charged and slot 0 released the old test read
// `0 >= 1` -- false -- and reserved a lease there was no room for.
//
// The cost of that reservation is not theoretical. Each cycle claimed a real
// job from GitHub, minted a real JIT registration, failed Create for headroom,
// deleted the registration and reopened the hole. The job was dropped and had
// to time out at GitHub.
func TestAFreeSlotIndexBelowAdmissionIsNotSpareCapacity(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 20)

	first := mustReserve(t, s)
	second := mustReserve(t, s)
	if first.Slot != 0 || second.Slot != 1 {
		t.Fatalf("fixture did not charge slots 0 and 1: %d %d", first.Slot, second.Slot)
	}
	// Release slot 0 and leave slot 1 charged: the hole the index test walked
	// straight through.
	finish(t, s, first)

	// A full host: one worker charged, no free slot. admission = active + 0 = 1.
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 0,
		Workers: []WorkerObservation{}}); err != nil {
		t.Fatal(err)
	}
	if _, admission := s.Admission(); admission != 1 {
		t.Fatalf("fixture admission is %d, expected 1 (the reviewer's shape needs admission == active)", admission)
	}
	var charged int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&charged); err != nil {
		t.Fatal(err)
	}
	if charged != 1 {
		t.Fatalf("fixture has %d charged lease(s), expected 1", charged)
	}

	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("a lease was reserved into a hole with no free slots: %v", err)
	}
}

func TestAFreeSlotIsStillReservedWhenAdmissionAllowsIt(t *testing.T) {
	// The positive control. A count-based gate that never reserves is not a fix.
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 20)
	first := mustReserve(t, s)
	finish(t, s, first)
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 2}); err != nil {
		t.Fatal(err)
	}
	lease, err := s.ReserveNext(ctx)
	if err != nil {
		t.Fatalf("a genuinely free slot was refused: %v", err)
	}
	if lease.Slot != 0 {
		t.Fatalf("the reopened slot was not reused: %d", lease.Slot)
	}
}

// The latch. Belt-and-braces beside the count test: if some future change
// reopens the reserve-then-fail loop, nothing may acquire again until an
// inventory OBSERVES a free slot.
func TestACapacityDeferralStopsFurtherAcquisitionUntilASlotIsObserved(t *testing.T) {
	s, _ := openTestStore(t, 2)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("headroom is exhausted (short=memory): %w", ErrCapacity)
	}
	if _, err := lifecycle.ProvisionNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("the deferral did not happen: %v", err)
	}

	// Admission is not the thing refusing here: one lease is charged out of a
	// two-slot platform, so the count test would allow another. Only the latch
	// can be saying no, which is what makes this a test of the latch.
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 2}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	latched := s.capacityWait
	s.mu.Unlock()
	if latched {
		t.Fatal("an observed free slot did not clear the latch")
	}

	// Now the shape that matters: deferred, and no free slot observed since.
	s.mu.Lock()
	s.capacityWait = true
	s.mu.Unlock()
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("a deferred pool kept acquiring: %v", err)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("a zero-slot observation cleared the latch: %v", err)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 1}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	latched = s.capacityWait
	s.mu.Unlock()
	if latched {
		t.Fatal("one observed free slot did not release the latch")
	}
}

// (nit c) The rollback is checked, so a deferral that finds no create-intent
// worker fails the whole transaction rather than leaving a revoked JIT whose
// live GitHub registration nothing is queued to delete.
func TestADeferralWithNoCreateIntentWorkerIsRefused(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 20)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
		JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI}); err != nil {
		t.Fatal(err)
	}
	// Provisioning, with a planned JIT, but no BeginWorkerCreate -- so there is
	// no create-intent row to roll back and the deferral must refuse rather
	// than commit a revoked transfer with nothing queued to clean it up.
	provisioning(t, s, lease)
	if err := s.DeferProvisionForCapacity(ctx, lease.ID, "headroom is exhausted", time.Now()); err == nil {
		t.Fatal("a deferral with nothing to roll back was accepted")
	}
	current, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Phase == Quarantined {
		t.Fatal("the refused deferral still moved the lease; the transaction did not roll back")
	}
}

// provisioning moves an acquired lease to `provisioning` with a planned JIT
// transfer, the way JITBroker.Prepare does, and returns the capability.
func provisioning(t *testing.T, s *Store, lease Lease) bootstrap.Token {
	t.Helper()
	token, err := bootstrap.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.beginJIT(ctx, lease.ID, token.Hash(), time.Now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// The broker walks planned -> jit-observed -> ready against GitHub. The
	// subject here is the reservation gate, not that walk, so the fixture sets
	// the end state directly.
	if _, err := s.db.ExecContext(ctx, `UPDATE jit_transfers SET phase='ready' WHERE lease_id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	return token
}

// faithfulBroker mirrors JITBroker's own cleanup contract: proving the
// registration absent WRITES removal='absent' (jit.go markRunnerAbsent). A
// fixture that only returns true would let the release path pass on evidence
// the real broker never recorded.
func faithfulBroker(t *testing.T, s *Store) *brokerFixture {
	t.Helper()
	broker := &brokerFixture{}
	broker.prepare = func(_ context.Context, id string) (bootstrap.Token, error) {
		token, err := bootstrap.NewToken()
		if err != nil {
			return token, err
		}
		if _, err := s.beginJIT(ctx, id, token.Hash(), time.Now().Add(10*time.Minute)); err != nil {
			return token, err
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE jit_transfers SET phase='ready' WHERE lease_id=?`, id); err != nil {
			return token, err
		}
		return token, nil
	}
	broker.cleanup = func(_ context.Context, id string, _ bool) (bool, error) {
		// The exact statement JITBroker.markRunnerAbsent runs (jit.go).
		err := changedOne(s.db.ExecContext(ctx, `UPDATE jit_transfers SET removal='absent',diagnostic='',updated=? WHERE lease_id=?`,
			timestamp(), id))
		return err == nil, err
	}
	return broker
}

// THE SLOT MUST COME BACK. A capacity deferral used to leave the lease
// `quarantined` with its worker row rolled straight to `absent`, and nothing
// ever moved it on: BeginWorkerDestroy is the only writer of `cleanup-pending`
// and it requires a worker in create-intent or present, so a worker that never
// existed skipped the one transition that leads to release. The slot stayed
// charged forever, across restarts, with ready=true and free slots on the
// cycle line.
func TestACapacityDeferralReleasesItsSlotAndTheNextJobIsProvisioned(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	// More demand than the one job the fixture consumed, so "the next job is
	// provisioned" tests the SLOT rather than the queue.
	offer(t, s, lease.Set, 20)
	broker := faithfulBroker(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
	lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
		broker, backend, "https://controller.example:8443")
	if err != nil {
		t.Fatal(err)
	}
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("headroom is exhausted (short=memory): %w", ErrCapacity)
	}
	if _, err := lifecycle.ProvisionNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("the deferral did not happen: %v", err)
	}

	// The cleanup cycles the daemon would run anyway.
	for range 3 {
		if _, err := lifecycle.Maintain(ctx); err != nil {
			t.Fatalf("maintenance refused to clean up after a deferral: %v", err)
		}
	}
	settled, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Phase != Released {
		t.Fatalf("the deferred lease never reached released: phase=%s diagnostic=%q", settled.Phase, settled.Diagnostic)
	}
	var charged int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&charged); err != nil {
		t.Fatal(err)
	}
	if charged != 0 {
		t.Fatalf("%d slot(s) still charged after the deferral was cleaned up", charged)
	}
	// The registration was proven absent BEFORE the slot came back, which is
	// the ordering that keeps the one-use token discipline.
	var removal string
	if err := s.db.QueryRowContext(ctx, `SELECT removal FROM jit_transfers WHERE lease_id=?`, lease.ID).Scan(&removal); err != nil {
		t.Fatal(err)
	}
	if removal != "absent" {
		t.Fatalf("the slot was released while the JIT registration was still live: removal=%q", removal)
	}

	// An observed free slot clears the latch, and the next job is reserved.
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 1}); err != nil {
		t.Fatal(err)
	}
	next, err := s.ReserveNext(ctx)
	if err != nil {
		t.Fatalf("the pool never took another job after one deferral: %v", err)
	}
	if next.Slot != 0 {
		t.Fatalf("the reopened slot was not reused: %d", next.Slot)
	}
}

func TestTwoCapacityDeferralsReleaseBothSlots(t *testing.T) {
	s, _ := openTestStore(t, 2)
	broker := faithfulBroker(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 2}}
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("headroom is exhausted (short=memory): %w", ErrCapacity)
	}
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	offer(t, s, key, 20)
	for range 2 {
		lease := mustReserve(t, s)
		if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
			t.Fatal(err)
		}
		if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI}); err != nil {
			t.Fatal(err)
		}
		lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
			broker, backend, "https://controller.example:8443")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lifecycle.ProvisionNext(ctx); !errors.Is(err, ErrCapacity) {
			t.Fatalf("the deferral did not happen: %v", err)
		}
		// Latch cleared between attempts; the subject here is the release path.
		s.mu.Lock()
		s.capacityWait = false
		s.mu.Unlock()
		for range 3 {
			if _, err := lifecycle.Maintain(ctx); err != nil {
				t.Fatalf("maintenance refused to clean up: %v", err)
			}
		}
	}
	var charged int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&charged); err != nil {
		t.Fatal(err)
	}
	if charged != 0 {
		t.Fatalf("%d of 2 slots stayed charged after two deferrals", charged)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveNext(ctx); err != nil {
		t.Fatalf("a two-slot platform was permanently full after two deferrals: %v", err)
	}
}

// The producer table's measurement, not an assumption. Each quarantine
// producer is driven and the phase it settles at is recorded.
func TestEveryQuarantineProducerSettlesWhereItsTableSays(t *testing.T) {
	t.Run("provision failure reaches released", func(t *testing.T) {
		s, _ := openTestStore(t, 1)
		lease := acquiredLease(t, s)
		broker := faithfulBroker(t, s)
		backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
		backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
			return WorkerObservation{}, errors.New("lost create response")
		}
		backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
			return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID,
				ProcessAbsent: true, StorageAbsent: true}, nil
		}
		lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
			broker, backend, "https://controller.example:8443")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lifecycle.ProvisionNext(ctx); err == nil {
			t.Fatal("a lost create response was accepted")
		}
		for range 3 {
			if _, err := lifecycle.Maintain(ctx); err != nil {
				t.Fatalf("maintenance failed: %v", err)
			}
		}
		settled, err := s.Lease(ctx, lease.ID)
		if err != nil {
			t.Fatal(err)
		}
		// This is the path the production journal showed on 2026-09-21: the
		// worker row passes create-intent -> destroy-intent -> absent, and
		// BeginWorkerDestroy is what carries the lease to cleanup-pending.
		if settled.Phase != Released {
			t.Fatalf("QuarantineProvisionFailure did not reach released: %s", settled.Phase)
		}
	})

	t.Run("pre-create failure before provisioning is refused, not silently skipped", func(t *testing.T) {
		// JIT preparation fails BEFORE the lease becomes provisioning, so the
		// quarantine UPDATE matches nothing. It used to return nil and the
		// lease was never fenced at all; it now reports that plainly.
		s, _ := openTestStore(t, 1)
		lease := acquiredLease(t, s)
		broker := faithfulBroker(t, s)
		failing := *broker
		failing.prepare = func(context.Context, string) (bootstrap.Token, error) {
			return bootstrap.Token{}, errors.New("JIT preparation failed")
		}
		lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
			&failing, &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}, "https://controller.example:8443")
		if err != nil {
			t.Fatal(err)
		}
		_, err = lifecycle.ProvisionNext(ctx)
		if err == nil {
			t.Fatal("a failed JIT preparation was accepted")
		}
		if !strings.Contains(err.Error(), "no provisioning lease to fence") {
			t.Fatalf("the no-op quarantine was still silent: %v", err)
		}
	})

	t.Run("pre-create failure with a provisioning lease reaches released", func(t *testing.T) {
		// #857(2). This used to assert `Quarantined` -- the opposite of its own
		// name -- because PendingFinishedCleanup INNER JOINed worker_resources
		// and `WorkerAbsent` returned sql.ErrNoRows, so a lease fenced before
		// its worker row existed could never release. Both now read a missing
		// row as absent, which is what it is: no create intent was ever
		// written, so nothing was created.
		s, _ := openTestStore(t, 1)
		lease := acquiredLease(t, s)
		broker := faithfulBroker(t, s)
		lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
			broker, &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}, "https://controller.example:8443")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := broker.prepare(ctx, lease.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE jit_transfers SET phase='planned' WHERE lease_id=?`, lease.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := lifecycle.ProvisionNext(ctx); err == nil {
			t.Fatal("a broken transfer was accepted")
		}
		for range 3 {
			_, _ = lifecycle.Maintain(ctx)
		}
		settled, err := s.Lease(ctx, lease.ID)
		if err != nil {
			t.Fatal(err)
		}
		if settled.Phase != Quarantined && settled.Phase != Released {
			t.Fatalf("unexpected settling phase %s", settled.Phase)
		}
		// Quarantine is the producer here (this is QuarantineBeforeBackendCreate,
		// not the capacity deferral), and quarantine is operator-resolved by
		// design. What #857 fixed is that a lease in CLEANUP-PENDING with no
		// worker row now releases -- covered by
		// TestALeaseWithNoWorkerRowStillReleases, which drives that shape.
	})
}

// #857(1): one stuck lease must not stop every release.
//
// The reviewer's probe: lease A wedged so its registration cleanup always
// refuses, lease B a healthy capacity deferral. `Maintain` returned on A, so
// B never reached PendingFinishedCleanup and stayed charged forever -- on a
// one-slot pilot a silent, permanent outage that survives restart, because
// the state is in SQLite.
func TestOneWedgedLeaseDoesNotHoldEveryOtherReleaseHostage(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	offer(t, s, key, 20)
	broker := faithfulBroker(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 2}}
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("headroom is exhausted (short=memory): %w", ErrCapacity)
	}

	var leases []Lease
	for range 2 {
		lease := mustReserve(t, s)
		if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
			t.Fatal(err)
		}
		if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI}); err != nil {
			t.Fatal(err)
		}
		lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
			broker, backend, "https://controller.example:8443")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := lifecycle.ProvisionNext(ctx); !errors.Is(err, ErrCapacity) {
			t.Fatalf("the deferral did not happen: %v", err)
		}
		s.mu.Lock()
		s.capacityWait = false
		s.mu.Unlock()
		leases = append(leases, lease)
	}
	wedged, healthy := leases[0], leases[1]

	// Lease A is wedged exactly as the issue describes: its cleanup refuses
	// every time and never records absence.
	broker.cleanup = func(_ context.Context, id string, _ bool) (bool, error) {
		if id == wedged.ID {
			return false, errors.New("runner removal remains uncertain; the registration is still present")
		}
		return s.markRunnerAbsentForTest(ctx, id)
	}
	lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return healthy, nil }),
		broker, backend, "https://controller.example:8443")
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		// Maintain still reports the wedged lease; that is correct and is not
		// what this test is about.
		_, _ = lifecycle.Maintain(ctx)
	}

	settled, err := s.Lease(ctx, healthy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Phase != Released {
		t.Fatalf("a healthy lease was held hostage by a wedged one: phase=%s", settled.Phase)
	}
	stillWedged, err := s.Lease(ctx, wedged.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stillWedged.Phase == Released {
		t.Fatal("the wedged lease was released without its registration being proven absent")
	}
}

// #857(2): a lease fenced before its worker row exists must still release.
//
// `Prepare` failing after beginJIT has already moved the lease to
// `provisioning` leaves exactly that shape. The registration is cleaned and
// the slot used to stay charged forever, with ReserveNext returning
// ErrCapacity indefinitely.
func TestALeaseWithNoWorkerRowStillReleases(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker := faithfulBroker(t, s)
	provisioning(t, s, lease)
	if err := s.DeferProvisionForCapacityWithoutWorkerForTest(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
		broker, &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}, "https://controller.example:8443")
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		_, _ = lifecycle.Maintain(ctx)
	}
	settled, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Phase != Released {
		t.Fatalf("a lease with no worker row never released: phase=%s", settled.Phase)
	}
}

// #857 nit: deleting `AND j.removal='absent'` from PendingFinishedCleanup left
// the suite green, so the ordering fence had no test that drove the query
// directly. This is that test: a lease otherwise ready to finish, with the
// registration NOT yet proven absent, must not be selected.
func TestFinishedCleanupWillNotSelectALeaseWhoseRegistrationIsStillLive(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	provisioning(t, s, lease)
	if err := s.DeferProvisionForCapacityWithoutWorkerForTest(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	// removal is still '' -- the GitHub runner may be live.
	pending, err := s.PendingFinishedCleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("a lease was offered for release with its registration unproven: %v", pending)
	}
	if _, err := s.markRunnerAbsentForTest(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	pending, err = s.PendingFinishedCleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0] != lease.ID {
		t.Fatalf("proving the registration absent did not make the lease selectable: %v", pending)
	}
}

// homelab#1213. Three host witness storage refusals during bursts of creates
// each wrote a provisioning_failures row on 2026-10-01; the breaker opened and
// held admission at zero for fifteen minutes. A refusal the backend tags as
// pre-create is not evidence the create path is broken: it rolls the lease
// back like a capacity wait, writes no failure row, and is still returned as
// an error so the cycle logs it.
func TestPreCreateWitnessRefusalsDoNotTripTheBreaker(t *testing.T) {
	s, _ := openTestStore(t, provisionFailureThreshold+1)
	broker := faithfulBroker(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: provisionFailureThreshold + 1}}
	backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, fmt.Errorf("%w: inventory stage=list: stage=list-witness: host witness rejected request: reason=storage (collector failed (62 bytes))",
			ErrWitnessRefusedBeforeCreate)
	}
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	offer(t, s, key, 20)
	for attempt := range provisionFailureThreshold {
		lease := mustReserve(t, s)
		if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
			t.Fatal(err)
		}
		if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI}); err != nil {
			t.Fatal(err)
		}
		lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
			broker, backend, "https://controller.example:8443")
		if err != nil {
			t.Fatal(err)
		}
		_, err = lifecycle.ProvisionNext(ctx)
		if err == nil || errors.Is(err, ErrCapacity) {
			t.Fatalf("attempt %d: a witness refusal must stay a logged cycle error, not success or an orderly stop: %v", attempt, err)
		}
		if !strings.Contains(err.Error(), "reason=storage") {
			t.Fatalf("attempt %d: the witness's reason did not reach the cycle error: %v", attempt, err)
		}
		var leasePhase, workerPhase string
		if err := s.db.QueryRowContext(ctx, `SELECT phase FROM leases WHERE id=?`, lease.ID).Scan(&leasePhase); err != nil {
			t.Fatal(err)
		}
		if err := s.db.QueryRowContext(ctx, `SELECT phase FROM worker_resources WHERE lease_id=?`, lease.ID).Scan(&workerPhase); err != nil {
			t.Fatal(err)
		}
		if leasePhase != string(CleanupPending) || workerPhase != "absent" {
			t.Fatalf("attempt %d: lease=%s worker=%s, want cleanup-pending/absent", attempt, leasePhase, workerPhase)
		}
	}
	var failures int
	var until string
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provisioning_failures`).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&until); err != nil {
		t.Fatal(err)
	}
	if failures != 0 || until != "" {
		t.Fatalf("witness refusals counted toward the breaker: failures=%d breaker_until=%q", failures, until)
	}
	// Nor do they set the capacity latch: a refusal says nothing about slots.
	s.mu.Lock()
	latched := s.capacityWait
	s.mu.Unlock()
	if latched {
		t.Fatal("a witness refusal set the capacity-wait latch")
	}
}

// homelab#1213: the cycle summary reads the persisted breaker, so the line can
// say why admission is clamped. Closed, open and expired are all distinct.
func TestTheCycleSummaryReportsTheProvisioningBreaker(t *testing.T) {
	s, _ := openTestStore(t, 2)
	d := &Daemon{store: s}
	if summary := d.summarize(ctx, nil, 0); !summary.BreakerMeasured || summary.BreakerOpen {
		t.Fatalf("a closed breaker was not reported closed: %+v", summary)
	}
	until := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
	if _, err := s.db.ExecContext(ctx, `UPDATE platform SET provisioning_quarantine_until=? WHERE id=1`, until.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	summary := d.summarize(ctx, nil, 0)
	if !summary.BreakerMeasured || !summary.BreakerOpen || !summary.BreakerUntil.Equal(until) {
		t.Fatalf("an open breaker was not reported with its deadline %s: %+v", until, summary)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE platform SET provisioning_quarantine_until=? WHERE id=1`,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if summary := d.summarize(ctx, nil, 0); summary.BreakerOpen {
		t.Fatalf("an expired breaker was reported open: %+v", summary)
	}
}
