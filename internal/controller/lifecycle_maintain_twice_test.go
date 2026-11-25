package controller

import (
	"context"
	"testing"
)

// homelab#951: Daemon.Cycle now runs MaintainWorkers after the cycle's Maintain
// when its sweep delivered a JobCompleted. That is safe only if the pair is idempotent
// on the real store. The case that matters is the one the dockerd sidecar
// produces: the first pass begins the delete, absence is not yet proven, the
// reservation is retained. The second pass must neither delete the worker
// again nor release, re-count or error on it.
func TestMaintainTwiceNeverDestroysAWorkerTwice(t *testing.T) {
	h := newLifecycleHarness(t, 1)
	h.provision(t)
	// JobCompleted moves the lease here with its worker still present
	// (reconcileObservedRunnerTx).
	if err := h.store.Transition(ctx, h.lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	h.backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		// The Pod is still terminating: the sidecar outlives the runner.
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: false, StorageAbsent: true}, nil
	}

	retained, err := h.lifecycle.Maintain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retained != 1 || h.backend.destroys != 1 {
		t.Fatalf("first pass: retained=%d destroys=%d, want 1 and 1", retained, h.backend.destroys)
	}
	h.assertWorkerPhase(t, "destroy-intent", "first pass")

	retained, err = h.lifecycle.MaintainWorkers(ctx)
	if err != nil {
		t.Fatalf("the second pass failed on a worker already being destroyed: %v", err)
	}
	if retained != 0 || h.backend.destroys != 1 {
		t.Fatalf("second pass: retained=%d destroys=%d, want 0 and still 1", retained, h.backend.destroys)
	}
	h.assertWorkerPhase(t, "destroy-intent", "second pass")
	h.assertLeasePhase(t, CleanupPending, "second pass")
	h.assertCharged(t, "a worker whose absence is not proven")
}

// The second pass must make no GitHub call: registration cleanup runs once a
// cycle, in Maintain, however many completions the cycle saw (#951 review).
func TestMaintainWorkersNeverCallsTheRegistrationCleanup(t *testing.T) {
	h := newLifecycleHarness(t, 1)
	h.provision(t)
	if err := h.store.Transition(ctx, h.lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	h.destroySucceeds()
	calls := 0
	h.broker.cleanup = func(context.Context, string, bool) (bool, error) {
		calls++
		return false, nil // still present: the lease stays in registration cleanup
	}
	if _, err := h.lifecycle.MaintainWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("MaintainWorkers made %d registration cleanup call(s)", calls)
	}
	h.assertWorkerPhase(t, "absent", "worker-only pass")
	if _, err := h.lifecycle.Maintain(ctx); err == nil {
		t.Fatal("a registration still present was not reported")
	}
	if calls != 1 {
		t.Fatalf("Maintain made %d registration cleanup call(s), want 1: the control that the fixture reaches the loop", calls)
	}
	if _, err := h.lifecycle.MaintainWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("a worker-only pass after Maintain made a registration call: %d", calls)
	}
	h.assertCharged(t, "a registration not proven absent")
}
