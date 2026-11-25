package controller

import (
	"context"
	"testing"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

func TestAcquisitionEvidenceRequiresExactDurableOutcome(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
		JobKey: lease.JobKey, Outcome: AcquisitionDeclined, Source: AcquisitionSourceAPI,
	}); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != CleanupPending {
		t.Fatalf("definite decline did not enter cleanup: %+v %v", stored, err)
	}
	evidence, err := s.AcquisitionEvidence(ctx, lease.ID)
	if err != nil || evidence.Outcome != AcquisitionDeclined || evidence.JobKey != lease.JobKey {
		t.Fatalf("decline evidence missing or wrong: %+v %v", evidence, err)
	}
	if err := s.FinishCleanup(ctx, lease.ID, CleanupEvidence{WorkerAbsent: true, RunnerAbsent: true}); err == nil {
		t.Fatal("missing caller settlement flag released the lease")
	}
	if err := s.FinishCleanup(ctx, lease.ID, CleanupEvidence{WorkerAbsent: true, RunnerAbsent: true, AcquisitionSettled: true}); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerCompletionSettlesUnknownOnlyForExactJITIdentity(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, AcquisitionIntent, AcquisitionUnknown); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?, ?,?,?,?)`, lease.ID, []byte("test"), "stream-written", 77, timestamp(), timestamp(), timestamp()); err != nil {
		t.Fatal(err)
	}
	wrong := event(key, lease.RequestID, scaleset.MessageTypeJobCompleted)
	wrong.RunnerID = 78
	wrong.RunnerName = lease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2, Events: []Event{wrong}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcquisitionEvidence(ctx, lease.ID); err == nil {
		t.Fatal("mismatched runner settled unknown acquisition")
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != AcquisitionUnknown {
		t.Fatalf("mismatched runner changed lease: %+v %v", stored, err)
	}

	completed := event(key, lease.RequestID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID = 77
	completed.RunnerName = lease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 3, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	evidence, err := s.AcquisitionEvidence(ctx, lease.ID)
	if err != nil || evidence.Outcome != AcquisitionCompleted || evidence.RunnerID != 77 || evidence.RunnerName != lease.RunnerName {
		t.Fatalf("completion did not settle exact runner: %+v %v", evidence, err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != AcquisitionUnknown {
		t.Fatalf("completed unknown lease was silently released from its fence: %+v %v", stored, err)
	}
}

func TestCompletionEvidenceUpgradesAcceptedEvidenceMonotonically(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(_ context.Context, ids []int64) ([]int64, error) { return ids, nil })); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?, ?,?,?,?)`, lease.ID, []byte("test"), "stream-written", 77, timestamp(), timestamp(), timestamp()); err != nil {
		t.Fatal(err)
	}
	completed := event(key, lease.RequestID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID = 77
	completed.RunnerName = lease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 4, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	evidence, err := s.AcquisitionEvidence(ctx, lease.ID)
	if err != nil || evidence.Outcome != AcquisitionCompleted {
		t.Fatalf("accepted evidence did not upgrade: %+v %v", evidence, err)
	}
}

func TestCrossedRequestsSettleByObservedRunnerAndAdvanceOwnWorkers(t *testing.T) {
	s, _ := openTestStore(t, 2)
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

	// Request A runs on worker B, and request B runs on worker A. Neither
	// request identity nor the runner that motivated its reservation is reused
	// as a binding assumption.
	startedA := event(key, requestLease.RequestID, scaleset.MessageTypeJobStarted)
	startedA.RunnerID, startedA.RunnerName = 100+runnerLease.Slot, runnerLease.RunnerName
	startedB := event(key, runnerLease.RequestID, scaleset.MessageTypeJobStarted)
	startedB.RunnerID, startedB.RunnerName = 100+requestLease.Slot, requestLease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 10, Events: []Event{startedA, startedB}}); err != nil {
		t.Fatal(err)
	}
	for _, lease := range []Lease{requestLease, runnerLease} {
		stored, err := s.Lease(ctx, lease.ID)
		if err != nil || stored.Phase != Running {
			t.Fatalf("start did not advance observed runner %s: %+v %v", lease.ID, stored, err)
		}
	}

	completedA := event(key, requestLease.RequestID, scaleset.MessageTypeJobCompleted)
	completedA.RunnerID, completedA.RunnerName = startedA.RunnerID, startedA.RunnerName
	completedB := event(key, runnerLease.RequestID, scaleset.MessageTypeJobCompleted)
	completedB.RunnerID, completedB.RunnerName = startedB.RunnerID, startedB.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 11, Events: []Event{completedA, completedB}}); err != nil {
		t.Fatal(err)
	}
	evidenceA, err := s.AcquisitionEvidence(ctx, requestLease.ID)
	if err != nil || evidenceA.Outcome != AcquisitionCompleted || evidenceA.RunnerLeaseID != runnerLease.ID {
		t.Fatalf("request A evidence did not retain observed runner B: %+v %v", evidenceA, err)
	}
	evidenceB, err := s.AcquisitionEvidence(ctx, runnerLease.ID)
	if err != nil || evidenceB.Outcome != AcquisitionCompleted || evidenceB.RunnerLeaseID != requestLease.ID {
		t.Fatalf("request B evidence did not retain observed runner A: %+v %v", evidenceB, err)
	}
	for _, lease := range []Lease{requestLease, runnerLease} {
		stored, err := s.Lease(ctx, lease.ID)
		if err != nil || stored.Phase != CleanupPending {
			t.Fatalf("completion did not independently schedule runner cleanup %s: %+v %v", lease.ID, stored, err)
		}
	}
}

func TestUnknownCrossedCompletionSettlesEvidenceButRetainsUncreatedLease(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 2)
	unknownLease := mustReserve(t, s)
	observedLease := mustReserve(t, s)
	if err := s.Transition(ctx, unknownLease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, unknownLease.ID, AcquisitionIntent, AcquisitionUnknown); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, observedLease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleAcquisition(ctx, observedLease.ID, AcquisitionSettlement{
		JobKey: observedLease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, observedLease.ID, Acquired, Provisioning); err != nil {
		t.Fatal(err)
	}
	now := timestamp()
	if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?, ?,?,?,?)`, observedLease.ID, []byte("test"), "stream-written", 404, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
VALUES(?,?,?,?,?,?)`, observedLease.ID, observedLease.RunnerName, "present", now, now, now); err != nil {
		t.Fatal(err)
	}
	completed := event(key, unknownLease.RequestID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID, completed.RunnerName = 404, observedLease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 20, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	evidence, err := s.AcquisitionEvidence(ctx, unknownLease.ID)
	if err != nil || evidence.Outcome != AcquisitionCompleted || evidence.RunnerLeaseID != observedLease.ID {
		t.Fatalf("unknown request evidence was not tied to observed runner: %+v %v", evidence, err)
	}
	if stored, err := s.Lease(ctx, unknownLease.ID); err != nil || stored.Phase != AcquisitionUnknown {
		t.Fatalf("unknown request escaped its charged fence: %+v %v", stored, err)
	}
	if err := s.FinishCleanup(ctx, unknownLease.ID, CleanupEvidence{WorkerAbsent: true, RunnerAbsent: true, AcquisitionSettled: true}); err == nil {
		t.Fatal("unknown request without its own worker evidence was released")
	}
	if stored, err := s.Lease(ctx, observedLease.ID); err != nil || stored.Phase != CleanupPending {
		t.Fatalf("observed runner was not independently moved to cleanup: %+v %v", stored, err)
	}
}
