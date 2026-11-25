package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// captureLogs sends slog output to a buffer for the test's duration.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

// swappedPair builds two provisioning leases with present workers and JIT
// runners, the fixture TestCrossedRequestsSettleByObservedRunnerAndAdvanceOwnWorkers uses.
func swappedPair(t *testing.T, s *Store) (SetKey, Lease, Lease) {
	t.Helper()
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 2)
	requestLease := mustReserve(t, s)
	runnerLease := mustReserve(t, s)
	for _, lease := range []Lease{requestLease, runnerLease} {
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
		now := timestamp()
		if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?, ?,?,?,?)`, lease.ID, []byte("test"), "stream-written", 100+lease.Slot, now, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
VALUES(?,?,?,?,?,?)`, lease.ID, lease.RunnerName, "present", now, now, now); err != nil {
			t.Fatal(err)
		}
	}
	return key, requestLease, runnerLease
}

// homelab#996: job A was acquired by lease Y and ran on worker X. Y is released
// before A's JobCompleted arrives. The completion must still move X to
// cleanup-pending; it used to be dropped at the job-key lookup, leaving X
// running forever.
func TestACompletionWhoseJobLeaseIsReleasedStillAdvancesTheWorkerThatRanIt(t *testing.T) {
	logs := captureLogs(t)
	s, _ := openTestStore(t, 2)
	key, jobLease, worker := swappedPair(t, s)

	started := event(key, jobLease.RequestID, scaleset.MessageTypeJobStarted)
	started.RunnerID, started.RunnerName = 100+worker.Slot, worker.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 10, Events: []Event{started}}); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, worker.ID); err != nil || stored.Phase != Running {
		t.Fatalf("fixture: the start did not advance worker X: %+v %v", stored, err)
	}
	if _, err := s.db.Exec(`UPDATE leases SET phase='released' WHERE id=?`, jobLease.ID); err != nil {
		t.Fatal(err)
	}

	completed := event(key, jobLease.RequestID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID, completed.RunnerName = started.RunnerID, started.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 11, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Lease(ctx, worker.ID)
	if err != nil || stored.Phase != CleanupPending {
		t.Fatalf("a completion whose job lease was released left worker X at %+v %v", stored, err)
	}
	if !strings.Contains(logs.String(), "job lease is already released; advancing the worker that ran it") ||
		!strings.Contains(logs.String(), "runner_name="+worker.RunnerName) {
		t.Fatalf("the job-lease miss was not logged with the runner: %s", logs.String())
	}
}

// Negative case: a completion for a runner no lease owns changes nothing, and
// says so.
func TestACompletionForAnUnownedRunnerChangesNothingAndLogs(t *testing.T) {
	logs := captureLogs(t)
	s, _ := openTestStore(t, 2)
	key, jobLease, worker := swappedPair(t, s)
	if _, err := s.db.Exec(`UPDATE leases SET phase='released' WHERE id=?`, jobLease.ID); err != nil {
		t.Fatal(err)
	}
	completed := event(key, jobLease.RequestID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID, completed.RunnerName = 999, "slipway-linux-not-ours"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 11, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, worker.ID); err != nil || stored.Phase != Provisioning {
		t.Fatalf("an unowned runner's completion moved worker X: %+v %v", stored, err)
	}
	if !strings.Contains(logs.String(), "matched neither the job's lease nor any owned runner") {
		t.Fatalf("the unmatched completion was not logged: %s", logs.String())
	}
}

// runningWorker provisions one lease through the real lifecycle and moves it
// to running, as a JobStarted would.
func runningWorker(t *testing.T) (*Store, *PoolLifecycle, *backendFixture, Lease) {
	t.Helper()
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
	}
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
	}
	lifecycle, err := newPoolLifecycle(s, allocatorFunc(func(context.Context) (Lease, error) { return lease, nil }),
		faithfulBroker(t, s), backend, "https://controller.example:8443")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, Provisioning, Running); err != nil {
		t.Fatal(err)
	}
	// faithfulBroker leaves runner_id at 0; a real JIT always carries GitHub's
	// positive runner id, and acquisition evidence refuses anything else.
	if _, err := s.db.Exec(`UPDATE jit_transfers SET runner_id=4242 WHERE lease_id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	current, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	return s, lifecycle, backend, current
}

// homelab#996 backstop: a running lease whose runner container the trusted
// inventory saw Terminated is retired even when no JobCompleted ever arrives,
// and its charge is released only through the ordinary teardown.
func TestARunningWorkerWhoseRunnerExitedIsRetiredWithoutACompletion(t *testing.T) {
	logs := captureLogs(t)
	s, lifecycle, backend, lease := runningWorker(t)
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
		{LeaseID: lease.ID, ResourceID: lease.RunnerName, Terminated: true, ExitCode: 1}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireTerminatedWorkers(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	retired, err := s.Lease(ctx, lease.ID)
	if err != nil || retired.Phase != CleanupPending {
		t.Fatalf("a running worker whose runner exited was not retired: %+v %v", retired, err)
	}
	if backend.destroys != 0 {
		t.Fatal("retirement destroyed the worker itself; teardown belongs to Maintain")
	}
	if !strings.Contains(logs.String(), "Retired a running worker whose runner exited before its completion was read") ||
		!strings.Contains(logs.String(), "lease_id="+lease.ID) || !strings.Contains(logs.String(), "runner_name="+lease.RunnerName) {
		t.Fatalf("the backstop retirement was not logged as its own case: %s", logs.String())
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	released, err := s.Lease(ctx, lease.ID)
	if err != nil || released.Phase != Released || backend.destroys != 1 {
		t.Fatalf("the retired worker was not torn down and released by Maintain: %+v destroys=%d %v",
			released, backend.destroys, err)
	}
}

// Negative case: a running lease whose runner is still running keeps its slot.
func TestARunningWorkerWhoseRunnerIsAliveIsNotRetired(t *testing.T) {
	s, lifecycle, backend, lease := runningWorker(t)
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
		{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Running || backend.destroys != 0 {
		t.Fatalf("a live running worker lost its slot: %+v destroys=%d %v", stored, backend.destroys, err)
	}
}

// The fallback advances a worker on runner identity alone, so an event
// without a runner id (0) must not advance anything.
func TestAReleasedJobsCompletionWithoutARunnerIDAdvancesNothing(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key, jobLease, worker := swappedPair(t, s)
	started := event(key, jobLease.RequestID, scaleset.MessageTypeJobStarted)
	started.RunnerID, started.RunnerName = 100+worker.Slot, worker.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 10, Events: []Event{started}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE leases SET phase='released' WHERE id=?`, jobLease.ID); err != nil {
		t.Fatal(err)
	}
	// A JIT row whose runner_id is 0 would otherwise match an id-less event.
	if _, err := s.db.Exec(`UPDATE jit_transfers SET runner_id=0 WHERE lease_id=?`, worker.ID); err != nil {
		t.Fatal(err)
	}
	completed := event(key, jobLease.RequestID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID, completed.RunnerName = 0, worker.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 11, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, worker.ID); err != nil || stored.Phase != Running {
		t.Fatalf("an id-less completion advanced worker X: %+v %v", stored, err)
	}
}

// The backstop retires a running lease; its JobCompleted then arrives, either
// while the lease is still cleanup-pending or after it was released. The inbox
// transaction commits, nothing is resurrected, and the next cycle is clean.
func TestACompletionArrivingAfterTheBackstopRetiredTheLeaseIsHarmless(t *testing.T) {
	for _, released := range []bool{false, true} {
		name := "while cleanup-pending"
		if released {
			name = "after release"
		}
		t.Run(name, func(t *testing.T) {
			s, lifecycle, backend, lease := runningWorker(t)
			backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
				{LeaseID: lease.ID, ResourceID: lease.RunnerName, Terminated: true, ExitCode: 0}}}
			if err := lifecycle.ReconcileInventory(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := s.RetireTerminatedWorkers(ctx, time.Now()); err != nil {
				t.Fatal(err)
			}
			want := CleanupPending
			if released {
				if _, err := lifecycle.Maintain(ctx); err != nil {
					t.Fatal(err)
				}
				want = Released
			}
			if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != want {
				t.Fatalf("fixture: lease is %+v %v, want %s", stored, err, want)
			}
			var runnerID int
			if err := s.db.QueryRow(`SELECT runner_id FROM jit_transfers WHERE lease_id=?`, lease.ID).Scan(&runnerID); err != nil {
				t.Fatal(err)
			}
			completed := event(lease.Set, lease.RequestID, scaleset.MessageTypeJobCompleted)
			completed.RunnerID, completed.RunnerName = runnerID, lease.RunnerName
			if err := s.RecordEnvelope(ctx, lease.Set, uuid.New(), Envelope{MessageID: 50, Events: []Event{completed}}); err != nil {
				t.Fatalf("the late completion failed the inbox transaction: %v", err)
			}
			if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != want {
				t.Fatalf("the late completion moved the lease to %+v %v, want %s", stored, err, want)
			}
			backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
			if err := lifecycle.ReconcileInventory(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := lifecycle.Maintain(ctx); err != nil {
				t.Fatalf("the cycle after a late completion errored: %v", err)
			}
		})
	}
}
