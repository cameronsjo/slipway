package controller

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/google/uuid"
)

func readyWorkerLease(t *testing.T, s *Store) (Lease, bootstrap.Token) {
	t.Helper()
	leases, tokens := readyWorkerLeases(t, s, 1)
	return leases[0], tokens[0]
}

// readyWorkerLeases prepares one repository and brings count leases to the
// provisioning phase with a ready JIT transfer each, so a single store can
// exercise several provisioning outcomes without repeating discovery. All
// count leases are held at once, so count may not exceed the platform ceiling.
func readyWorkerLeases(t *testing.T, s *Store, count int) ([]Lease, []bootstrap.Token) {
	t.Helper()
	next := readyWorkerSequence(t, s, count)
	var leases []Lease
	var tokens []bootstrap.Token
	for range count {
		lease, token := next(t)
		leases = append(leases, lease)
		tokens = append(tokens, token)
	}
	return leases, tokens
}

// readyWorkerSequence prepares one repository with demand for count workers and
// returns a generator for the next provisioning-phase lease. Taking them one at
// a time lets a test drive more provisioning outcomes than the platform holds
// concurrently, which reserving all of them up front cannot do with a
// capacity of two.
func readyWorkerSequence(t *testing.T, s *Store, count int) func(*testing.T) (Lease, bootstrap.Token) {
	t.Helper()
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	offer(t, s, key, count)
	return func(t *testing.T) (Lease, bootstrap.Token) {
		t.Helper()
		lease := mustReserve(t, s)
		if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
			t.Fatal(err)
		}
		if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI,
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.Transition(ctx, lease.ID, Acquired, Provisioning); err != nil {
			t.Fatal(err)
		}
		token, err := bootstrap.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		now := timestamp()
		hash := token.Hash()
		if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,created,expires,updated)
VALUES(?,?,'ready',?,?,?)`, lease.ID, hash[:], now, time.Now().Add(workerStartupTimeout).UTC().Format(time.RFC3339Nano), now); err != nil {
			t.Fatal(err)
		}
		lease.Phase = Provisioning
		return lease, token
	}
}

// releaseQuarantinedWorker frees a quarantined lease's slot through the normal
// cleanup path, so a two-slot platform can record more provisioning failures
// over time than it can hold at once.
func releaseQuarantinedWorker(t *testing.T, s *Store, lease Lease) {
	t.Helper()
	if err := s.Transition(ctx, lease.ID, Quarantined, CleanupPending); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginWorkerDestroy(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: lease.ID, ResourceID: lease.RunnerName,
		ProcessAbsent: true, StorageAbsent: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishWorkerCleanup(ctx, lease.ID, true, true); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("quarantined lease was not released by cleanup: %+v %v", stored, err)
	}
}

func TestWorkerCreateAndDestroyIntentsPrecedeBackendEffects(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease, token := readyWorkerLease(t, s)
	before := time.Now()
	intent, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token)
	if err != nil {
		t.Fatal(err)
	}
	if intent.LeaseID != lease.ID || intent.ResourceID != lease.RunnerName || intent.Slot != lease.Slot ||
		intent.BootstrapToken.Hash() != token.Hash() || intent.StartupDeadline.Before(before.Add(workerStartupTimeout-time.Second)) {
		t.Fatalf("invalid create intent: %+v", intent)
	}
	record, err := s.Worker(ctx, lease.ID)
	if err != nil || record.Phase != "create-intent" || record.ResourceID != lease.RunnerName {
		t.Fatalf("create intent was not durable: %+v %v", record, err)
	}
	if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err == nil {
		t.Fatal("worker create intent was replayable")
	}
	if err := s.ConfirmWorkerCreated(ctx, WorkerObservation{LeaseID: lease.ID, ResourceID: "wrong"}); err == nil {
		t.Fatal("mismatched create response was accepted")
	}
	if record, _ := s.Worker(ctx, lease.ID); record.Phase != "create-intent" {
		t.Fatal("mismatched response changed durable intent")
	}
	worker := WorkerObservation{LeaseID: lease.ID, ResourceID: lease.RunnerName}
	if err := s.ConfirmWorkerCreated(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	destroyed, err := s.BeginWorkerDestroy(ctx, lease.ID)
	if err != nil || destroyed != worker {
		t.Fatalf("destroy intent: %+v %v", destroyed, err)
	}
	if _, err := s.BeginWorkerDestroy(ctx, lease.ID); err == nil {
		t.Fatal("uncertain worker deletion was replayable")
	}
	if err := s.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: lease.ID, ResourceID: lease.RunnerName, ProcessAbsent: true}); err == nil {
		t.Fatal("storage absence was not required")
	}
	if err := s.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: lease.ID, ResourceID: lease.RunnerName, ProcessAbsent: true, StorageAbsent: true}); err != nil {
		t.Fatal(err)
	}
	if absent, err := s.WorkerAbsent(ctx, lease.ID); err != nil || !absent {
		t.Fatalf("worker absence: %t %v", absent, err)
	}
	if err := s.FinishCleanup(ctx, lease.ID, CleanupEvidence{WorkerAbsent: true}); err == nil {
		t.Fatal("physical absence alone released the reservation")
	}
	if err := s.FinishWorkerCleanup(ctx, lease.ID, false, true); err == nil {
		t.Fatal("runner presence was ignored")
	}
	if err := s.FinishWorkerCleanup(ctx, lease.ID, true, true); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("complete cleanup did not release lease: %+v %v", stored, err)
	}
}

func TestWorkerCreateIntentSurvivesRestartWithoutCredentialPersistence(t *testing.T) {
	s, dir := openTestStore(t, 1)
	lease, token := readyWorkerLease(t, s)
	if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(token.Encoded())) || bytes.Contains(data, []byte("controller.example")) {
			t.Fatalf("bootstrap credential or route entered %s", file.Name())
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	if _, err := restarted.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("restart bypassed inventory fence: %v", err)
	}
	if err := restarted.ReconcileBackendInventory(ctx, BackendInventory{Complete: true,
		Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ConfirmWorkerCreated(ctx, WorkerObservation{LeaseID: lease.ID, ResourceID: lease.RunnerName}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err == nil {
		t.Fatal("restart repeated worker creation")
	}
}

func TestWorkerIntentRejectsInvalidBootstrapAndRevocation(t *testing.T) {
	for _, scenario := range []string{"http", "path", "zero token", "wrong token", "expired", "revoked", "unmanaged"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			lease, token := readyWorkerLease(t, s)
			origin := "https://controller.example"
			switch scenario {
			case "http":
				origin = "http://controller.example"
			case "path":
				origin += "/secret"
			case "zero token":
				token = bootstrap.Token{}
			case "wrong token":
				var err error
				token, err = bootstrap.NewToken()
				if err != nil {
					t.Fatal(err)
				}
			case "expired":
				if _, err := s.db.Exec(`UPDATE jit_transfers SET expires=?`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := s.Discover(ctx, nil); err != nil {
					t.Fatal(err)
				}
			case "unmanaged":
				if _, err := s.db.Exec(`DELETE FROM managed_sets`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.BeginWorkerCreate(ctx, lease.ID, origin, token); err == nil {
				t.Fatal("invalid worker create input was accepted")
			}
			if _, err := s.Worker(ctx, lease.ID); err == nil {
				t.Fatal("invalid create input wrote an intent")
			}
		})
	}
}

func TestWorkerDeadlinesAndProvisioningCircuitBreaker(t *testing.T) {
	s, _ := openTestStore(t, 2)
	lease, token := readyWorkerLease(t, s)
	if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := s.db.Exec(`UPDATE worker_resources SET startup_deadline=? WHERE lease_id=?`, now.Add(-time.Second).Format(time.RFC3339Nano), lease.ID); err != nil {
		t.Fatal(err)
	}
	for _, occurred := range []time.Time{now.Add(-2 * time.Minute), now.Add(-time.Minute)} {
		if _, err := s.db.Exec(`INSERT INTO provisioning_failures(lease_id,occurred,diagnostic) VALUES(?,?,?)`, lease.ID, occurred.Format(time.RFC3339Nano), "fixture failure"); err != nil {
			t.Fatal(err)
		}
	}
	actions, err := s.ReconcileDeadlines(ctx, now)
	if err != nil || len(actions) != 1 || actions[0] != (DeadlineAction{LeaseID: lease.ID, Kind: "startup"}) {
		t.Fatalf("startup actions=%+v err=%v", actions, err)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Quarantined || stored.Diagnostic != "startup deadline exceeded" {
		t.Fatalf("startup timeout was not quarantined: %+v %v", stored, err)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 5,
		Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}); err != nil {
		t.Fatal(err)
	}
	offer(t, s, lease.Set, 2)
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("three recent failures did not suppress new capacity: %v", err)
	}
	if _, err := s.db.Exec(`UPDATE platform SET provisioning_quarantine_until=?`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 5,
		Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveNext(ctx); err != nil {
		t.Fatalf("elapsed circuit breaker did not restore observed headroom: %v", err)
	}
	if _, err := s.BeginWorkerDestroy(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE worker_resources SET cleanup_deadline=? WHERE lease_id=?`, now.Add(-time.Second).Format(time.RFC3339Nano), lease.ID); err != nil {
		t.Fatal(err)
	}
	actions, err = s.ReconcileDeadlines(ctx, now)
	if err != nil || len(actions) != 1 || actions[0] != (DeadlineAction{LeaseID: lease.ID, Kind: "cleanup"}) {
		t.Fatalf("cleanup actions=%+v err=%v", actions, err)
	}
	stored, err = s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Quarantined || stored.Diagnostic != "cleanup deadline exceeded" {
		t.Fatalf("cleanup timeout was not quarantined: %+v %v", stored, err)
	}
}

func TestUnusedWorkerRetiresOnlyAfterFiveIdleMinutes(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease, token := readyWorkerLease(t, s)
	if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfirmWorkerCreated(ctx, WorkerObservation{LeaseID: lease.ID, ResourceID: lease.RunnerName}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := s.db.Exec(`UPDATE jit_transfers SET phase='stream-written',updated=? WHERE lease_id=?`,
		now.Add(-unusedWorkerTimeout+time.Second).Format(time.RFC3339Nano), lease.ID); err != nil {
		t.Fatal(err)
	}
	if retired, err := s.RetireUnusedWorkers(ctx, now); err != nil || len(retired) != 0 {
		t.Fatalf("worker retired before idle deadline: %v %v", retired, err)
	}
	if _, err := s.db.Exec(`UPDATE jit_transfers SET updated=? WHERE lease_id=?`,
		now.Add(-unusedWorkerTimeout).Format(time.RFC3339Nano), lease.ID); err != nil {
		t.Fatal(err)
	}
	retired, err := s.RetireUnusedWorkers(ctx, now)
	if err != nil || len(retired) != 1 || retired[0] != lease.ID {
		t.Fatalf("idle worker was not scheduled for cleanup: %v %v", retired, err)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != CleanupPending || stored.Diagnostic != "unused worker idle deadline exceeded" {
		t.Fatalf("idle worker cleanup state: %+v %v", stored, err)
	}
}

// A restart must carry the charged lease across and invent nothing for it: a
// worker resource conjured at open would read as a live worker that the backend
// has never heard of.
func TestRestartRetainsLeaseAndInventsNoWorkerResource(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	if _, err := s.db.Exec(`DROP TABLE worker_resources; DROP TABLE provisioning_failures`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if current, err := restarted.Lease(ctx, lease.ID); err != nil || current.ID != lease.ID {
		t.Fatalf("restart changed lease: %+v %v", current, err)
	}
	if _, err := restarted.Worker(ctx, lease.ID); err == nil {
		t.Fatal("restart invented a worker resource")
	}
}

// lifecycleHarness drives one charged lease through PoolLifecycle with the
// existing broker/backend fakes. Every assertion below uses the coordinator's
// public operations plus the durable journal, not its internal helpers.
type lifecycleHarness struct {
	store     *Store
	dir       string
	lease     Lease
	broker    *brokerFixture
	backend   *backendFixture
	lifecycle *PoolLifecycle
}

func newLifecycleHarness(t *testing.T, capacity int) *lifecycleHarness {
	t.Helper()
	store, dir := openTestStore(t, capacity)
	lease := acquiredLease(t, store)
	broker, backend, lifecycle := lifecycleFixtures(t, store, lease)
	return &lifecycleHarness{store: store, dir: dir, lease: lease, broker: broker, backend: backend, lifecycle: lifecycle}
}

// provision runs one successful create so later scenarios start from a present
// worker whose identity the backend has already acknowledged.
func (h *lifecycleHarness) provision(t *testing.T) {
	t.Helper()
	h.backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
	}
	if _, err := h.lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
}

func (h *lifecycleHarness) destroySucceeds() {
	h.backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
}

func (h *lifecycleHarness) registrationBecomesAbsent() {
	h.broker.cleanup = func(_ context.Context, id string, workerAbsent bool) (bool, error) {
		if !workerAbsent {
			return false, errors.New("registration cleanup lacked physical absence")
		}
		_, err := h.store.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, id)
		return err == nil, err
	}
}

// inventory publishes one complete listing; named leases are reported present.
func (h *lifecycleHarness) inventory(freeSlots int, leaseIDs ...string) {
	listing := BackendInventory{Complete: true, FreeSlots: freeSlots}
	for _, id := range leaseIDs {
		record, err := h.store.Worker(ctx, id)
		if err != nil {
			continue
		}
		listing.Workers = append(listing.Workers, WorkerObservation{LeaseID: id, ResourceID: record.ResourceID})
	}
	h.backend.inventory = listing
}

func (h *lifecycleHarness) assertCharged(t *testing.T, why string) {
	t.Helper()
	if _, err := h.store.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("%s released the slot: %v", why, err)
	}
}

func (h *lifecycleHarness) assertWorkerPhase(t *testing.T, want, why string) {
	t.Helper()
	record, err := h.store.Worker(ctx, h.lease.ID)
	if err != nil || record.Phase != want {
		t.Fatalf("%s: worker phase %+v (want %s) %v", why, record, want, err)
	}
}

func (h *lifecycleHarness) assertLeasePhase(t *testing.T, want Phase, why string) {
	t.Helper()
	stored, err := h.store.Lease(ctx, h.lease.ID)
	if err != nil || stored.Phase != want {
		t.Fatalf("%s: lease phase %+v (want %s) %v", why, stored, want, err)
	}
}

// restart reopens the same journal with fresh process state and fresh fakes.
// The replacement backend fails the test if a create or delete is replayed.
func (h *lifecycleHarness) restart(t *testing.T) *lifecycleHarness {
	t.Helper()
	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(h.dir, h.store.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	broker := &brokerFixture{
		prepare: func(context.Context, string) (bootstrap.Token, error) {
			t.Error("restart prepared another JIT transfer for a settled lease")
			return bootstrap.Token{}, errors.New("unexpected prepare")
		},
		cleanup: func(context.Context, string, bool) (bool, error) { return true, nil },
	}
	// No restart path may call the platform again before inventory settles the
	// outstanding intent. These two guards document that: a passing restart
	// test never reaches them, and a replay turns them red.
	backend := &backendFixture{
		inventory: BackendInventory{Complete: true},
		create: func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
			t.Error("restart replayed the backend create call")
			return WorkerObservation{}, errors.New("unexpected create")
		},
		destroy: func(context.Context, WorkerObservation) (WorkerAbsence, error) {
			t.Error("restart replayed the backend delete call")
			return WorkerAbsence{}, errors.New("unexpected destroy")
		},
	}
	allocator := allocatorFunc(func(context.Context) (Lease, error) { return Lease{}, ErrNoDemand })
	lifecycle, err := newPoolLifecycle(restarted, allocator, broker, backend, "https://controller.example")
	if err != nil {
		t.Fatal(err)
	}
	return &lifecycleHarness{store: restarted, dir: h.dir, lease: h.lease, broker: broker, backend: backend, lifecycle: lifecycle}
}

// Scenario 1: cancelling an in-flight provisioning call must not release the
// slot or drop the durable create intent. It writes no failure record: a
// cancelled context is the controller shutting down, and a restart during a
// burst must not hand the next pod a tripped breaker (homelab#1196 review I1).
func TestPoolLifecycleCanceledProvisioningRetainsChargedIntent(t *testing.T) {
	h := newLifecycleHarness(t, 1)
	provisionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.backend.create = func(callCtx context.Context, _ WorkerCreateIntent) (WorkerObservation, error) {
		cancel()
		return WorkerObservation{}, callCtx.Err()
	}
	if _, err := h.lifecycle.ProvisionNext(provisionCtx); err == nil {
		t.Fatal("canceled provisioning reported success")
	}
	h.assertWorkerPhase(t, "create-intent", "cancellation")
	h.assertLeasePhase(t, Quarantined, "cancellation")
	h.assertCharged(t, "cancellation")
	if h.backend.creates != 1 {
		t.Fatalf("cancellation replayed or skipped the create call: %d", h.backend.creates)
	}
	var failures int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM provisioning_failures WHERE lease_id=?`, h.lease.ID).Scan(&failures); err != nil || failures != 0 {
		t.Fatalf("canceled provisioning recorded %d failures, want 0: %v", failures, err)
	}
}

// Scenario 2: the process restarts after the create intent is durable but
// before the backend response settled. Inventory alone must settle it.
func TestPoolLifecycleRestartAtCreateIntentSettlesFromInventory(t *testing.T) {
	h := newLifecycleHarness(t, 1)
	h.backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, errors.New("process lost the create response")
	}
	if _, err := h.lifecycle.ProvisionNext(ctx); err == nil {
		t.Fatal("lost create response reported success")
	}
	h.assertWorkerPhase(t, "create-intent", "lost create response")

	restarted := h.restart(t)
	if _, err := restarted.store.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("restart allocated before trusted inventory: %v", err)
	}
	restarted.inventory(0, h.lease.ID)
	if err := restarted.lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	restarted.assertWorkerPhase(t, "present", "restart at create intent")
	restarted.assertLeasePhase(t, Quarantined, "restart at create intent")
	restarted.assertCharged(t, "restart at create intent")
	if restarted.backend.creates != 0 {
		t.Fatalf("restart replayed create: %d", restarted.backend.creates)
	}
}

// Scenario 3: the process restarts after the delete intent is durable but
// before the backend response settled. A complete listing that omits the exact
// resource is the only thing that may record process-and-storage absence.
func TestPoolLifecycleRestartAtDeleteIntentSettlesAbsenceFromInventory(t *testing.T) {
	h := newLifecycleHarness(t, 1)
	h.provision(t)
	if err := h.store.Transition(ctx, h.lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	h.backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{}, errors.New("process lost the delete response")
	}
	if err := h.lifecycle.DestroyWorker(ctx, h.lease.ID, true); err == nil {
		t.Fatal("lost delete response reported success")
	}
	h.assertWorkerPhase(t, "destroy-intent", "lost delete response")

	restarted := h.restart(t)
	restarted.inventory(0)
	if err := restarted.lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	restarted.assertWorkerPhase(t, "absent", "restart at delete intent")
	restarted.assertLeasePhase(t, CleanupPending, "restart at delete intent")
	restarted.assertCharged(t, "physical absence alone")
	if restarted.backend.destroys != 0 {
		t.Fatalf("restart replayed delete: %d", restarted.backend.destroys)
	}
	restarted.registrationBecomesAbsent()
	if err := restarted.lifecycle.FinishReconciledCleanup(ctx, h.lease.ID, true); err != nil {
		t.Fatal(err)
	}
	restarted.assertLeasePhase(t, Released, "three independent cleanup proofs")
}

// Scenario 4: the platform reports the resource later than expected. Before the
// ten-minute startup deadline the lease stays charged and untouched; after it
// the deadline becomes durable quarantine, never absence.
func TestPoolLifecycleDelayedCreationIsChargedUntilStartupDeadline(t *testing.T) {
	t.Run("late but before the deadline", func(t *testing.T) {
		h := newLifecycleHarness(t, 1)
		h.backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
			return WorkerObservation{}, errors.New("create response never arrived")
		}
		if _, err := h.lifecycle.ProvisionNext(ctx); err == nil {
			t.Fatal("lost create response reported success")
		}
		// The platform has not materialized the resource yet, so a complete
		// listing omits it. That is not absence and frees nothing.
		h.inventory(0)
		if err := h.lifecycle.ReconcileInventory(ctx); err != nil {
			t.Fatal(err)
		}
		h.assertWorkerPhase(t, "create-intent", "delayed creation")
		if absent, err := h.store.WorkerAbsent(ctx, h.lease.ID); err != nil || absent {
			t.Fatalf("delayed creation was read as absence: %t %v", absent, err)
		}
		h.assertCharged(t, "delayed creation before the deadline")

		// The resource appears in a later listing, still inside the ten-minute
		// startup deadline, and that listing alone settles the create intent.
		h.inventory(0, h.lease.ID)
		if err := h.lifecycle.ReconcileInventory(ctx); err != nil {
			t.Fatal(err)
		}
		h.assertWorkerPhase(t, "present", "late inventory")
		h.assertCharged(t, "late inventory")
		if h.backend.creates != 1 {
			t.Fatalf("late inventory replayed create: %d", h.backend.creates)
		}
		var expired int
		if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM provisioning_failures WHERE lease_id=? AND diagnostic=?`,
			h.lease.ID, "startup deadline exceeded").Scan(&expired); err != nil || expired != 0 {
			t.Fatalf("a worker inside its startup deadline recorded %d expiries: %v", expired, err)
		}
	})

	t.Run("after the deadline", func(t *testing.T) {
		h := newLifecycleHarness(t, 1)
		h.provision(t)
		if _, err := h.store.db.Exec(`UPDATE worker_resources SET startup_deadline=? WHERE lease_id=?`,
			time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), h.lease.ID); err != nil {
			t.Fatal(err)
		}
		h.destroySucceeds()
		h.registrationBecomesAbsent()
		if _, err := h.lifecycle.Maintain(ctx); err != nil {
			t.Fatal(err)
		}
		var diagnostic string
		if err := h.store.db.QueryRow(`SELECT diagnostic FROM provisioning_failures WHERE lease_id=?`, h.lease.ID).Scan(&diagnostic); err != nil ||
			diagnostic != "startup deadline exceeded" {
			t.Fatalf("startup deadline was not recorded as a provisioning failure: %q %v", diagnostic, err)
		}
		if h.backend.destroys != 1 {
			t.Fatalf("expired worker received %d delete attempts", h.backend.destroys)
		}
		if absent, err := h.store.WorkerAbsent(ctx, h.lease.ID); err != nil || !absent {
			t.Fatalf("retirement lacked trusted physical absence: %t %v", absent, err)
		}
	})
}

// Scenario 5: an intended resource missing from a complete listing stays
// charged. Absence requires a durable delete intent, never a silent gap.
func TestPoolLifecycleMissingInventoryKeepsResourceCharged(t *testing.T) {
	for _, phase := range []string{"create-intent", "present"} {
		t.Run(phase, func(t *testing.T) {
			h := newLifecycleHarness(t, 1)
			if phase == "create-intent" {
				h.backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
					return WorkerObservation{}, errors.New("lost create response")
				}
				if _, err := h.lifecycle.ProvisionNext(ctx); err == nil {
					t.Fatal("lost create response reported success")
				}
			} else {
				h.provision(t)
			}
			h.inventory(0)
			if err := h.lifecycle.ReconcileInventory(ctx); err != nil {
				t.Fatal(err)
			}
			h.assertWorkerPhase(t, phase, "missing inventory")
			if absent, err := h.store.WorkerAbsent(ctx, h.lease.ID); err != nil || absent {
				t.Fatalf("missing inventory recorded absence: %t %v", absent, err)
			}
			h.assertCharged(t, "missing inventory")
			if err := h.lifecycle.FinishReconciledCleanup(ctx, h.lease.ID, true); err == nil {
				t.Fatal("missing inventory satisfied cleanup")
			}
		})
	}
}

// Scenario 6: a resource no lease owns, or one whose identity was renamed or
// duplicated, must fence admission and must never be charged to a lease.
func TestPoolLifecycleForeignInventoryIsRejectedAndNeverCharged(t *testing.T) {
	// unowned names the lease id the listing invented, where one exists; the
	// journal must still have no record of it after the listing is rejected.
	foreign := map[string]struct {
		build   func(Lease, WorkerRecord) []WorkerObservation
		unowned string
	}{
		"unowned resource": {unowned: "foreign-lease", build: func(Lease, WorkerRecord) []WorkerObservation {
			return []WorkerObservation{{LeaseID: "foreign-lease", ResourceID: "slipway-linux-foreign"}}
		}},
		"renamed resource": {build: func(lease Lease, _ WorkerRecord) []WorkerObservation {
			return []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName + "-renamed"}}
		}},
		"duplicate lease": {build: func(lease Lease, record WorkerRecord) []WorkerObservation {
			return []WorkerObservation{{LeaseID: lease.ID, ResourceID: record.ResourceID}, {LeaseID: lease.ID, ResourceID: record.ResourceID}}
		}},
		"duplicate resource": {unowned: "second-lease", build: func(lease Lease, record WorkerRecord) []WorkerObservation {
			return []WorkerObservation{{LeaseID: lease.ID, ResourceID: record.ResourceID}, {LeaseID: "second-lease", ResourceID: record.ResourceID}}
		}},
		"empty identity": {build: func(lease Lease, _ WorkerRecord) []WorkerObservation {
			return []WorkerObservation{{LeaseID: lease.ID, ResourceID: ""}}
		}},
		"incomplete listing": {},
	}
	for name, scenario := range foreign {
		t.Run(name, func(t *testing.T) {
			h := newLifecycleHarness(t, 1)
			h.provision(t)
			record, err := h.store.Worker(ctx, h.lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario.build == nil {
				h.backend.inventory = BackendInventory{Complete: false}
			} else {
				h.backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: scenario.build(h.lease, record)}
			}
			if err := h.lifecycle.ReconcileInventory(ctx); err == nil {
				t.Fatal("foreign inventory was accepted")
			}
			if _, err := h.store.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
				t.Fatalf("foreign inventory left admission open: %v", err)
			}
			h.assertWorkerPhase(t, "present", "foreign inventory")
			if absent, err := h.store.WorkerAbsent(ctx, h.lease.ID); err != nil || absent {
				t.Fatalf("foreign inventory recorded absence: %t %v", absent, err)
			}
			if scenario.unowned != "" {
				if _, err := h.store.Worker(ctx, scenario.unowned); err == nil {
					t.Fatalf("%s was charged to a lease", scenario.unowned)
				}
			}
		})
	}
}

// Scenario 7: a backend create error keeps the slot charged. Only a durable
// delete intent plus trusted absence, registration removal and acquisition
// settlement may free it.
func TestPoolLifecycleFailedProvisioningHoldsSlotUntilAbsenceIsProven(t *testing.T) {
	h := newLifecycleHarness(t, 1)
	h.backend.create = func(context.Context, WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{}, errors.New("backend refused the worker")
	}
	if _, err := h.lifecycle.ProvisionNext(ctx); err == nil {
		t.Fatal("failed provisioning reported success")
	}
	h.assertLeasePhase(t, Quarantined, "failed provisioning")
	h.assertCharged(t, "failed provisioning")

	h.inventory(0)
	if err := h.lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	h.assertCharged(t, "a complete listing that omits the resource")
	h.assertWorkerPhase(t, "create-intent", "failed provisioning")

	h.destroySucceeds()
	h.registrationBecomesAbsent()
	if _, err := h.lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	h.assertLeasePhase(t, Released, "proven absence and settled acquisition")
	if _, err := h.store.ReserveNext(ctx); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("released slot was not reusable: %v", err)
	}
}

// Scenario 8: a failed delete, and a missed two-minute cleanup deadline, both
// retain the reservation instead of releasing it.
func TestPoolLifecycleRetainsReservationWhenCleanupFailsOrExpires(t *testing.T) {
	t.Run("delete fails", func(t *testing.T) {
		h := newLifecycleHarness(t, 1)
		h.provision(t)
		if err := h.store.Transition(ctx, h.lease.ID, Provisioning, CleanupPending); err != nil {
			t.Fatal(err)
		}
		h.backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
			return WorkerAbsence{}, errors.New("delete failed")
		}
		if err := h.lifecycle.DestroyWorker(ctx, h.lease.ID, true); err == nil {
			t.Fatal("failed delete reported success")
		}
		h.assertWorkerPhase(t, "destroy-intent", "failed delete")
		h.assertLeasePhase(t, CleanupPending, "failed delete")
		h.assertCharged(t, "failed delete")
		if err := h.lifecycle.DestroyWorker(ctx, h.lease.ID, true); err == nil || h.backend.destroys != 1 {
			t.Fatalf("failed delete was replayed: calls=%d err=%v", h.backend.destroys, err)
		}
	})

	t.Run("cleanup deadline expires", func(t *testing.T) {
		h := newLifecycleHarness(t, 1)
		h.provision(t)
		if err := h.store.Transition(ctx, h.lease.ID, Provisioning, CleanupPending); err != nil {
			t.Fatal(err)
		}
		h.backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
			return WorkerAbsence{}, errors.New("delete outcome unknown")
		}
		if err := h.lifecycle.DestroyWorker(ctx, h.lease.ID, true); err == nil {
			t.Fatal("unknown delete reported success")
		}
		if _, err := h.store.db.Exec(`UPDATE worker_resources SET cleanup_deadline=? WHERE lease_id=?`,
			time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), h.lease.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.lifecycle.Maintain(ctx); err != nil {
			t.Fatal(err)
		}
		stored, err := h.store.Lease(ctx, h.lease.ID)
		if err != nil || stored.Phase != Quarantined || stored.Diagnostic != "cleanup deadline exceeded" {
			t.Fatalf("expired cleanup was not retained as quarantine: %+v %v", stored, err)
		}
		if absent, err := h.store.WorkerAbsent(ctx, h.lease.ID); err != nil || absent {
			t.Fatalf("expired cleanup was read as absence: %t %v", absent, err)
		}
		h.assertCharged(t, "expired cleanup")
	})
}

// Scenario 9: three provisioning failures inside fifteen minutes suppress new
// capacity; two inside the window plus one outside it do not.
func TestProvisioningQuarantineWindowSuppressesNewCapacity(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		offsets     []time.Duration
		quarantined bool
	}{
		{name: "three within fifteen minutes", offsets: []time.Duration{-10 * time.Minute, -5 * time.Minute, 0}, quarantined: true},
		{name: "third failure at the window edge", offsets: []time.Duration{-14 * time.Minute, -7 * time.Minute, 0}, quarantined: true},
		{name: "two within fifteen minutes and one just outside", offsets: []time.Duration{-16 * time.Minute, -5 * time.Minute, 0}, quarantined: false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, _ := openTestStore(t, 2)
			// Three quarantined leases do not fit in a two-slot platform, so
			// the failures happen the way they would in production at this
			// ceiling: each lease is failed, checked, and physically cleaned up
			// before the next one is reserved. The last one stays charged.
			next := readyWorkerSequence(t, store, 3)
			now := time.Now().UTC()
			var last Lease
			for i := range 3 {
				lease, token := next(t)
				if _, err := store.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
					t.Fatal(err)
				}
				if err := store.QuarantineProvisionFailure(ctx, lease.ID, "backend refused the worker", now.Add(scenario.offsets[i])); err != nil {
					t.Fatal(err)
				}
				stored, err := store.Lease(ctx, lease.ID)
				if err != nil || stored.Phase != Quarantined {
					t.Fatalf("failed provisioning released its lease: %+v %v", stored, err)
				}
				last = lease
				if i < 2 {
					releaseQuarantinedWorker(t, store, lease)
				}
			}
			var until string
			if err := store.db.QueryRow(`SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&until); err != nil {
				t.Fatal(err)
			}
			if (until != "") != scenario.quarantined {
				t.Fatalf("quarantine deadline %q for %s", until, scenario.name)
			}
			listing := BackendInventory{Complete: true, FreeSlots: 1,
				Workers: []WorkerObservation{{LeaseID: last.ID, ResourceID: last.RunnerName}}}
			if err := store.ReconcileBackendInventory(ctx, listing); err != nil {
				t.Fatal(err)
			}
			offer(t, store, last.Set, 5)
			_, err := store.ReserveNext(ctx)
			if scenario.quarantined && !errors.Is(err, ErrCapacity) {
				t.Fatalf("quarantine window did not suppress new capacity: %v", err)
			}
			if !scenario.quarantined && err != nil {
				t.Fatalf("failures outside the window suppressed capacity: %v", err)
			}
			stored, err := store.Lease(ctx, last.ID)
			if err != nil || stored.Phase != Quarantined {
				t.Fatalf("failed provisioning released its lease: %+v %v", stored, err)
			}
		})
	}
}

// C2: one journal is one platform instance. The platform identity is durable
// and a restart configured for another platform must refuse the journal rather
// than adopt its leases. Repositories arrive from discovery, so the two
// instances differ only by their platform configuration.
func TestPlatformIdentityIsRetainedAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "linux-state")
	linux := Config{Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist}
	store, err := Open(dir, linux)
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, store, 1)[0]
	offer(t, store, key, 1)
	lease := mustReserve(t, store)
	if !bytes.HasPrefix([]byte(lease.RunnerName), []byte("slipway-linux-")) {
		t.Fatalf("resource identity lacks its platform: %s", lease.RunnerName)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir, Config{Platform: "macos", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist}); err == nil {
		other.Close()
		t.Fatal("a macOS instance adopted the Linux journal")
	}
	restarted, err := Open(dir, linux)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	stored, err := restarted.Lease(ctx, lease.ID)
	if err != nil || stored.RunnerName != lease.RunnerName {
		t.Fatalf("restart changed the durable resource identity: %+v %v", stored, err)
	}
	var platform string
	if err := restarted.db.QueryRow(`SELECT name FROM platform WHERE id=1`).Scan(&platform); err != nil || platform != "linux" {
		t.Fatalf("restart changed the platform identity: %q %v", platform, err)
	}
	macDir := filepath.Join(t.TempDir(), "macos-state")
	macos, err := Open(macDir, Config{Platform: "macos", InstallationID: 42, Capacity: 2, LegacySlots: 1, RepositoryAllowlist: testAllowlist})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { macos.Close() })
	macKey := prepare(t, macos, 1)[0]
	offer(t, macos, macKey, 1)
	if macLease := mustReserve(t, macos); !bytes.HasPrefix([]byte(macLease.RunnerName), []byte("slipway-macos-")) {
		t.Fatalf("second platform instance reused the first identity: %s", macLease.RunnerName)
	}
}

// A missing identity is a caller defect. It must not read as a worker still
// tearing down, or Maintain would skip it every cycle instead of failing.
func TestConfirmWorkerAbsentSeparatesMissingIdentityFromUnprovenAbsence(t *testing.T) {
	store, _ := openTestStore(t, 2)
	ctx := context.Background()
	for _, absence := range []WorkerAbsence{
		{ResourceID: "r", ProcessAbsent: true, StorageAbsent: true},
		{LeaseID: "l", ProcessAbsent: true, StorageAbsent: true},
	} {
		err := store.ConfirmWorkerAbsent(ctx, absence)
		if err == nil || errors.Is(err, ErrAbsenceNotProven) {
			t.Fatalf("missing identity %+v: err=%v, want a defect error that is not ErrAbsenceNotProven", absence, err)
		}
	}
	for _, absence := range []WorkerAbsence{
		{LeaseID: "l", ResourceID: "r", StorageAbsent: true},
		{LeaseID: "l", ResourceID: "r", ProcessAbsent: true},
	} {
		if err := store.ConfirmWorkerAbsent(ctx, absence); !errors.Is(err, ErrAbsenceNotProven) {
			t.Fatalf("unproven absence %+v: err=%v, want ErrAbsenceNotProven", absence, err)
		}
	}
}

// TestQuarantinedLeaseIsReleasedOnceAbsenceIsProven is the homelab#911
// follow-up. Two quarantines leave no worker for BeginWorkerDestroy, so they
// never reach cleanup-pending, and the cleanup queries once accepted only
// cleanup-pending: an agent-board lease quarantined after `JIT preflight lookup
// failed` held one of two slots indefinitely. Each case runs the two calls
// Maintain makes for finished cleanup -- PendingFinishedCleanup, then
// FinishWorkerCleanup with the registration proof it vouches for -- and pins
// whether the lease is released. The negative cases keep every other fence.
func TestQuarantinedLeaseIsReleasedOnceAbsenceIsProven(t *testing.T) {
	createWorker := func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
		t.Helper()
		if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfirmWorkerCreated(ctx, WorkerObservation{LeaseID: lease.ID, ResourceID: lease.RunnerName}); err != nil {
			t.Fatal(err)
		}
	}
	// quarantineAtCleanupDeadline builds the second dead end: a destroy that
	// outlived its two-minute deadline, leaving the worker at destroy-intent.
	quarantineAtCleanupDeadline := func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
		t.Helper()
		createWorker(t, s, lease, token)
		if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
			t.Fatal(err)
		}
		if _, err := s.BeginWorkerDestroy(ctx, lease.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReconcileDeadlines(ctx, time.Now().Add(workerCleanupTimeout+time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	quarantineBeforeCreate := func(t *testing.T, s *Store, lease Lease, _ bootstrap.Token) {
		t.Helper()
		if err := s.QuarantineBeforeBackendCreate(ctx, lease.ID, "JIT preflight lookup failed", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	registrationGone := func(t *testing.T, s *Store, lease Lease) {
		t.Helper()
		if _, err := s.markRunnerAbsentForTest(ctx, lease.ID); err != nil {
			t.Fatal(err)
		}
	}
	for _, testCase := range []struct {
		name  string
		build func(*testing.T, *Store, Lease, bootstrap.Token)
		// worker is the worker_resources phase the case must have built, or ""
		// for no row at all, so a fixture drifting into another shape fails
		// here instead of passing for the wrong reason.
		worker   string
		released bool
	}{
		{
			name: "pre-create quarantine with no worker row and the registration gone",
			build: func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
				quarantineBeforeCreate(t, s, lease, token)
				registrationGone(t, s, lease)
			},
			worker: "", released: true,
		},
		{
			name: "cleanup-deadline quarantine once inventory proves the worker absent",
			build: func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
				quarantineAtCleanupDeadline(t, s, lease, token)
				// The call ReconcileInventory makes for a destroy-intent worker a
				// complete inventory no longer lists.
				if err := s.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: lease.ID, ResourceID: lease.RunnerName,
					ProcessAbsent: true, StorageAbsent: true}); err != nil {
					t.Fatal(err)
				}
				registrationGone(t, s, lease)
			},
			worker: "absent", released: true,
		},
		{
			name: "cleanup-deadline quarantine while the worker is still destroy-intent",
			build: func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
				quarantineAtCleanupDeadline(t, s, lease, token)
				registrationGone(t, s, lease)
			},
			worker: "destroy-intent", released: false,
		},
		{
			name: "provisioning-failure quarantine with the worker present",
			build: func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
				createWorker(t, s, lease, token)
				if err := s.QuarantineProvisionFailure(ctx, lease.ID, "worker terminated with a non-zero exit", time.Now()); err != nil {
					t.Fatal(err)
				}
				registrationGone(t, s, lease)
			},
			worker: "present", released: false,
		},
		{
			name: "provisioning-failure quarantine with the worker only at create-intent",
			build: func(t *testing.T, s *Store, lease Lease, token bootstrap.Token) {
				if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
					t.Fatal(err)
				}
				if err := s.QuarantineProvisionFailure(ctx, lease.ID, "worker creation outcome uncertain", time.Now()); err != nil {
					t.Fatal(err)
				}
				registrationGone(t, s, lease)
			},
			worker: "create-intent", released: false,
		},
		{
			name:   "pre-create quarantine while the registration is not yet proven gone",
			build:  quarantineBeforeCreate,
			worker: "", released: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			lease, token := readyWorkerLease(t, s)
			testCase.build(t, s, lease, token)
			if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Quarantined {
				t.Fatalf("fixture did not quarantine the lease: %+v %v", stored, err)
			}
			record, err := s.Worker(ctx, lease.ID)
			switch {
			case testCase.worker == "" && !errors.Is(err, sql.ErrNoRows):
				t.Fatalf("fixture wrote a worker row it must not have: %+v %v", record, err)
			case testCase.worker != "" && (err != nil || record.Phase != testCase.worker):
				t.Fatalf("fixture worker is %+v %v, want phase %q", record, err, testCase.worker)
			}
			selected, err := s.PendingFinishedCleanup(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, leaseID := range selected {
				if err := s.FinishWorkerCleanup(ctx, leaseID, true, true); err != nil {
					t.Fatalf("selected lease %s did not release: %v", leaseID, err)
				}
			}
			stored, err := s.Lease(ctx, lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			if released := stored.Phase == Released; released != testCase.released {
				t.Fatalf("lease phase %s after cleanup (selected %v), want released=%t", stored.Phase, selected, testCase.released)
			}
		})
	}
}

// TestReleasingAQuarantinedLeaseKeepsTheBreakerClosed is the review finding on
// the quarantine-release fix. A tripped breaker freezes admission at the
// count of unreleased leases; releasing a quarantined lease in the same
// cycle's Maintain, before an inventory refreezes it, left len(used) below the
// frozen admission, so ReserveNext claimed another real job straight into the
// failing path. Both quarantine producers are driven to three failures, the
// last lease is released through its normal cleanup, and ReserveNext must
// refuse until the breaker's deadline passes.
func TestReleasingAQuarantinedLeaseKeepsTheBreakerClosed(t *testing.T) {
	releasePreCreate := func(t *testing.T, s *Store, lease Lease) {
		t.Helper()
		if _, err := s.markRunnerAbsentForTest(ctx, lease.ID); err != nil {
			t.Fatal(err)
		}
		selected, err := s.PendingFinishedCleanup(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, leaseID := range selected {
			if err := s.FinishWorkerCleanup(ctx, leaseID, true, true); err != nil {
				t.Fatal(err)
			}
		}
		if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
			t.Fatalf("pre-create quarantine was not released: %+v %v", stored, err)
		}
	}
	for _, path := range []struct {
		name    string
		fail    func(*testing.T, *Store, Lease, bootstrap.Token, time.Time)
		release func(*testing.T, *Store, Lease)
	}{
		{
			name: "pre-create quarantine",
			fail: func(t *testing.T, s *Store, lease Lease, _ bootstrap.Token, now time.Time) {
				t.Helper()
				if err := s.QuarantineBeforeBackendCreate(ctx, lease.ID, "JIT preflight lookup failed", now); err != nil {
					t.Fatal(err)
				}
			},
			release: releasePreCreate,
		},
		{
			name: "post-create quarantine",
			fail: func(t *testing.T, s *Store, lease Lease, token bootstrap.Token, now time.Time) {
				t.Helper()
				if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
					t.Fatal(err)
				}
				if err := s.QuarantineProvisionFailure(ctx, lease.ID, "backend refused the worker", now); err != nil {
					t.Fatal(err)
				}
			},
			release: releaseQuarantinedWorker,
		},
	} {
		t.Run(path.name, func(t *testing.T) {
			s, _ := openTestStore(t, 2)
			next := readyWorkerSequence(t, s, 3)
			now := time.Now().UTC()
			var last Lease
			for i := range 3 {
				lease, token := next(t)
				path.fail(t, s, lease, token, now)
				last = lease
				if i < 2 {
					path.release(t, s, lease)
				}
			}
			var until string
			if err := s.db.QueryRow(`SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&until); err != nil || until == "" {
				t.Fatalf("fixture did not trip the breaker: %q %v", until, err)
			}
			// The trip froze admission at the one unreleased lease. Releasing it
			// is what Maintain now does, with no inventory in between.
			path.release(t, s, last)
			if _, admission := s.Admission(); admission != 1 {
				t.Fatalf("fixture admission is %d, not the frozen 1 this case tests", admission)
			}
			// Fresh request IDs: offer reuses 1..n, which the three leases consumed.
			if err := s.RecordEnvelope(ctx, last.Set, uuid.New(), Envelope{MessageID: 2,
				Statistics: scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 2},
				Events:     []Event{event(last.Set, 10, scaleset.MessageTypeJobAvailable), event(last.Set, 11, scaleset.MessageTypeJobAvailable)},
			}); err != nil {
				t.Fatal(err)
			}
			if lease, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
				t.Fatalf("a released quarantine reopened provisioning under an active breaker: %+v %v", lease, err)
			}
			if _, err := s.db.Exec(`UPDATE platform SET provisioning_quarantine_until=? WHERE id=1`,
				now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ReserveNext(ctx); err != nil {
				t.Fatalf("an elapsed breaker still refused capacity: %v", err)
			}
		})
	}
}
