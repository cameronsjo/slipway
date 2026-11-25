package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ageWorker moves a worker's last-attempt stamp back, as the passage of time
// would, so the redelete backoff can be crossed without sleeping.
func ageWorker(t *testing.T, s *Store, leaseID string, by time.Duration) {
	t.Helper()
	stamp := time.Now().UTC().Add(-by).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE worker_resources SET destroy_started=?,updated=? WHERE lease_id=?`, stamp, stamp, leaseID); err != nil {
		t.Fatal(err)
	}
}

func workerPhase(t *testing.T, s *Store, leaseID string) string {
	t.Helper()
	record, err := s.Worker(ctx, leaseID)
	if err != nil {
		t.Fatal(err)
	}
	return record.Phase
}

// retiredRunningWorker is runningWorker after the #996 backstop retired it:
// cleanup-pending with its worker still present and listed as terminated.
func retiredRunningWorker(t *testing.T) (*Store, *PoolLifecycle, *backendFixture, Lease) {
	t.Helper()
	s, lifecycle, backend, lease := runningWorker(t)
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
		{LeaseID: lease.ID, ResourceID: lease.RunnerName, Terminated: true, ExitCode: 0}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RetireTerminatedWorkers(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	return s, lifecycle, backend, lease
}

// homelab#1094 (D4): a delete that errors no longer fails the cycle, keeps the
// slot charged, and is sent again once the backoff has passed -- not before.
func TestAFailedDeleteIsSentAgainAfterTheBackoffAndReleasesTheSlot(t *testing.T) {
	logs := captureLogs(t)
	s, lifecycle, backend, lease := retiredRunningWorker(t)
	succeed := backend.destroy
	backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{}, errors.New("Kubernetes Pod delete outcome is uncertain")
	}
	retained, err := lifecycle.Maintain(ctx)
	if err != nil {
		t.Fatalf("one worker's failed delete failed the whole cycle: %v", err)
	}
	if retained != 1 || backend.destroys != 1 || workerPhase(t, s, lease.ID) != "destroy-intent" {
		t.Fatalf("the failed delete did not retain its slot: retained=%d destroys=%d phase=%s",
			retained, backend.destroys, workerPhase(t, s, lease.ID))
	}
	if !strings.Contains(logs.String(), "Worker deletion outcome is uncertain") || !strings.Contains(logs.String(), "lease_id="+lease.ID) {
		t.Fatalf("the failed delete was not logged with its lease: %s", logs.String())
	}

	// Inside the backoff nothing is re-sent: the ordinary teardown is never raced.
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil || backend.destroys != 1 {
		t.Fatalf("a delete was re-sent inside the backoff: destroys=%d err=%v", backend.destroys, err)
	}

	backend.destroy = succeed
	ageWorker(t, s, lease.ID, redeleteBackoff+time.Second)
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	released, err := s.Lease(ctx, lease.ID)
	if err != nil || released.Phase != Released || backend.destroys != 2 {
		t.Fatalf("the re-sent delete did not release the slot: %+v destroys=%d %v", released, backend.destroys, err)
	}
	if !strings.Contains(logs.String(), "Re-sending the delete for a worker") || !strings.Contains(logs.String(), "A re-sent delete proved the worker absent") {
		t.Fatalf("the retry was not logged: %s", logs.String())
	}
}

// The quarantined shape: the cleanup deadline passed while the Pod stayed, so
// the lease is quarantined with its worker in destroy-intent. That is exactly
// the lease that used to be charged forever; the retry reaches it too.
func TestAQuarantinedTeardownIsRetriedAndReleased(t *testing.T) {
	s, lifecycle, backend, lease := retiredRunningWorker(t)
	succeed := backend.destroy
	backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{}, errors.New("Kubernetes owned Pod lookup failed; deletion outcome is unresolved")
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReconcileDeadlines(ctx, time.Now().Add(workerCleanupTimeout+time.Second)); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Quarantined {
		t.Fatalf("fixture: the cleanup deadline did not quarantine the lease: %+v %v", stored, err)
	}
	backend.destroy = succeed
	ageWorker(t, s, lease.ID, redeleteBackoff+time.Second)
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("a quarantined teardown was not retried to release: %+v %v", stored, err)
	}
}

// A retry that still cannot prove absence keeps the slot and waits a whole
// backoff again; it does not re-send on every cycle.
func TestARetryWithoutAbsenceWaitsAnotherBackoff(t *testing.T) {
	s, lifecycle, backend, lease := retiredRunningWorker(t)
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: false, StorageAbsent: true}, nil
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	ageWorker(t, s, lease.ID, redeleteBackoff+time.Second)
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	retained, err := lifecycle.Maintain(ctx)
	if err != nil || retained != 1 || backend.destroys != 2 {
		t.Fatalf("the retry did not run once and retain: retained=%d destroys=%d err=%v", retained, backend.destroys, err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil || backend.destroys != 2 {
		t.Fatalf("a retry was re-sent without waiting the backoff: destroys=%d err=%v", backend.destroys, err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase == Released {
		t.Fatalf("a worker without proven absence was released: %+v %v", stored, err)
	}
}

// A destroy-intent worker a complete inventory no longer lists is settled by
// ReconcileInventory, as before; the retry never sees it.
func TestAWorkerThatLeftTheInventoryIsNotRedeleted(t *testing.T) {
	s, lifecycle, backend, lease := retiredRunningWorker(t)
	backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{}, errors.New("Kubernetes Pod delete outcome is uncertain")
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	ageWorker(t, s, lease.ID, redeleteBackoff+time.Second)
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released || backend.destroys != 1 {
		t.Fatalf("an absent worker was redeleted or not released: %+v destroys=%d %v", stored, backend.destroys, err)
	}
}

// vanish runs one complete inventory that lists nothing, and returns the
// store's proof set for the lease afterwards.
func vanish(t *testing.T, s *Store, lifecycle *PoolLifecycle, backend *backendFixture, leaseID string) bool {
	t.Helper()
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vanishedProven[leaseID]
}

// backdateVanish moves the first-missing instant back past the grace.
func backdateVanish(s *Store, leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.vanished[leaseID]
	entry.firstMissing = entry.firstMissing.Add(-vanishedWorkerGrace - time.Second)
	s.vanished[leaseID] = entry
}

// homelab#1094 (D3): a running worker whose Pod vanished with no Terminated
// status is retired once it has been missing past the grace, and the ordinary
// teardown releases its slot.
func TestAVanishedRunningWorkerIsRetiredAfterTheGraceAndReleased(t *testing.T) {
	logs := captureLogs(t)
	s, lifecycle, backend, lease := runningWorker(t)
	if vanish(t, s, lifecycle, backend, lease.ID) {
		t.Fatal("one missing inventory proved a vanish")
	}
	backdateVanish(s, lease.ID)
	if !vanish(t, s, lifecycle, backend, lease.ID) {
		t.Fatal("two missing inventories past the grace did not prove a vanish")
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	released, err := s.Lease(ctx, lease.ID)
	if err != nil || released.Phase != Released || backend.destroys != 1 {
		t.Fatalf("a vanished running worker was not retired and released: %+v destroys=%d %v", released, backend.destroys, err)
	}
	if !strings.Contains(logs.String(), "Retired a worker that vanished from the backend inventory") ||
		!strings.Contains(logs.String(), "lease_phase=running") {
		t.Fatalf("the vanish was not logged as its own case: %s", logs.String())
	}
}

// The grace alone is not enough: two missing inventories are, and one that is
// very late is still one.
func TestOneLateMissingInventoryDoesNotProveAVanish(t *testing.T) {
	s, lifecycle, backend, lease := runningWorker(t)
	if vanish(t, s, lifecycle, backend, lease.ID) {
		t.Fatal("fixture: the first miss proved a vanish")
	}
	backdateVanish(s, lease.ID)
	s.mu.Lock()
	entry := s.vanished[lease.ID]
	s.mu.Unlock()
	if entry.misses != 1 {
		t.Fatalf("fixture: misses=%d", entry.misses)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Running {
		t.Fatalf("a single missing inventory retired a running worker: %+v %v", stored, err)
	}
}

// Negative case: a worker listed again resets its grace, and an incomplete
// inventory neither advances nor resets it.
func TestAWorkerSeenAgainResetsItsGraceAndAnIncompleteInventoryIsNeutral(t *testing.T) {
	s, lifecycle, backend, lease := runningWorker(t)
	vanish(t, s, lifecycle, backend, lease.ID)
	backdateVanish(s, lease.ID)

	backend.inventory = BackendInventory{Complete: false}
	backend.inventoryErr = errors.New("witness unavailable")
	if err := lifecycle.ReconcileInventory(ctx); err == nil {
		t.Fatal("fixture: the incomplete inventory was accepted")
	}
	backend.inventoryErr = nil
	s.mu.Lock()
	entry, tracked := s.vanished[lease.ID]
	s.mu.Unlock()
	if !tracked || entry.misses != 1 {
		t.Fatalf("an incomplete inventory changed the tracking: %+v tracked=%t", entry, tracked)
	}

	backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
		{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	_, tracked = s.vanished[lease.ID]
	s.mu.Unlock()
	if tracked {
		t.Fatal("a worker listed again kept its vanish tracking")
	}
	if vanish(t, s, lifecycle, backend, lease.ID) {
		t.Fatal("a worker's earlier grace carried over after it was seen again")
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Running {
		t.Fatalf("a worker that came back lost its slot: %+v %v", stored, err)
	}
}

// A provisioning worker that vanishes goes through the breaker, as an abnormal
// exit does: repeated evictions must trip it.
func TestAVanishedProvisioningWorkerIsQuarantinedThroughTheBreaker(t *testing.T) {
	s, lifecycle, backend, lease := runningWorker(t)
	if _, err := s.db.Exec(`UPDATE leases SET phase='provisioning' WHERE id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	vanish(t, s, lifecycle, backend, lease.ID)
	backdateVanish(s, lease.ID)
	if !vanish(t, s, lifecycle, backend, lease.ID) {
		t.Fatal("fixture: the vanish was not proven")
	}
	if _, err := s.RetireTerminatedWorkers(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Quarantined || !strings.Contains(stored.Diagnostic, "vanished") {
		t.Fatalf("a vanished provisioning worker was not quarantined: %+v %v", stored, err)
	}
	var failures int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM provisioning_failures WHERE lease_id=?`, lease.ID).Scan(&failures); err != nil || failures != 1 {
		t.Fatalf("the vanish was not counted by the breaker: failures=%d %v", failures, err)
	}
}

// The backoff runs from when the delete was sent, not from the row's last
// write: the cleanup-deadline quarantine touches `updated`, and keying on it
// pushed the first retry out to twice the backoff.
func TestTheQuarantineWriteDoesNotRestartTheRedeleteBackoff(t *testing.T) {
	s, lifecycle, backend, lease := retiredRunningWorker(t)
	backend.destroy = func(context.Context, WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{}, errors.New("Kubernetes Pod delete outcome is uncertain")
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	sent := time.Now().UTC().Add(-redeleteBackoff - time.Second).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE worker_resources SET destroy_started=? WHERE lease_id=?`, sent, lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.quarantineCleanupDeadline(ctx, lease.ID, "cleanup deadline passed", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingWorkerRedeletes(ctx, time.Now().UTC())
	if err != nil || len(pending) != 1 || pending[0].LeaseID != lease.ID {
		t.Fatalf("a fresh quarantine write held back a delete sent a full backoff ago: %v %v", pending, err)
	}
}
