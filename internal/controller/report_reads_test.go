package controller

import (
	"testing"
	"time"
)

// ActiveWorkerLeases must count only provisioning and running -- the phases
// where a worker plausibly holds a job -- and exclude every other non-released
// phase, above all quarantined. #912's original gate fed ChargedSlots (every
// non-released phase) into sessionsLookDeaf, so one lease stuck in quarantined
// disabled the deaf check forever; this pins the narrower count that fixes it.
func TestActiveWorkerLeasesCountsOnlyProvisioningAndRunning(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)

	tests := []struct {
		phase Phase
		want  int
	}{
		{Reserved, 0},
		{AcquisitionIntent, 0},
		{AcquisitionUnknown, 0},
		{Acquired, 0},
		{Provisioning, 1},
		{Running, 1},
		{CleanupPending, 0},
		{Quarantined, 0}, // The exact bug #912 shipped: this must never count.
	}
	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			if tt.phase != Reserved {
				if _, err := s.db.Exec(`UPDATE leases SET phase=? WHERE id=?`, tt.phase, lease.ID); err != nil {
					t.Fatal(err)
				}
			}
			active, err := s.ActiveWorkerLeases(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if active != tt.want {
				t.Fatalf("ActiveWorkerLeases() with lease phase %q = %d, want %d", tt.phase, active, tt.want)
			}
		})
	}
}

// B2: the report's slot arithmetic has to hold — charged plus free plus the
// legacy hold-back is the whole ceiling — and each repository's share has to
// add up to the pool-wide count.
func TestChargedSlotsAndPhaseCountsAgreeWithTheLeaseJournal(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	offer(t, s, keys[0], 1)
	offer(t, s, keys[1], 1)

	if charged, err := s.ChargedSlots(ctx); err != nil || charged != 0 {
		t.Fatalf("charged slots on an idle pool = %d (%v), want 0", charged, err)
	}
	first := mustReserve(t, s)
	second := mustReserve(t, s)

	charged, err := s.ChargedSlots(ctx)
	if err != nil || charged != 2 {
		t.Fatalf("charged slots = %d (%v), want 2", charged, err)
	}
	phases, err := s.LeasePhaseCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if phases[Reserved] != 2 {
		t.Fatalf("reserved leases = %d, want 2", phases[Reserved])
	}
	byRepo, err := s.ChargedByRepository(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if byRepo[first.Set.RepositoryID] != 1 || byRepo[second.Set.RepositoryID] != 1 {
		t.Fatalf("per-repository charged slots = %v, want one each", byRepo)
	}

	finish(t, s, first)
	if charged, err := s.ChargedSlots(ctx); err != nil || charged != 1 {
		t.Fatalf("charged slots after release = %d (%v), want 1", charged, err)
	}
	if phases, err := s.LeasePhaseCounts(ctx); err != nil || phases[Released] != 0 {
		t.Fatalf("released leases were counted as live: %v (%v)", phases, err)
	}
}

// StuckCharged is capacity lost until an operator reconciles it, and it is
// invisible in the charged count, which a busy healthy pool also raises. Only a
// lease that has sat past the provisioning-failure window counts.
func TestStuckChargedCountsOnlyLeasesPastTheProvisioningWindow(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 2)
	lease := mustReserve(t, s)

	now := time.Now().UTC()
	if _, err := s.db.Exec(`UPDATE leases SET phase='quarantined',updated=? WHERE id=?`,
		now.Add(-provisionFailureWindow+time.Minute).Format(time.RFC3339Nano), lease.ID); err != nil {
		t.Fatal(err)
	}
	if stuck, err := s.StuckCharged(ctx, now); err != nil || stuck != 0 {
		t.Fatalf("a lease inside the window counted as stuck: %d (%v)", stuck, err)
	}
	if _, err := s.db.Exec(`UPDATE leases SET updated=? WHERE id=?`,
		now.Add(-provisionFailureWindow-time.Minute).Format(time.RFC3339Nano), lease.ID); err != nil {
		t.Fatal(err)
	}
	if stuck, err := s.StuckCharged(ctx, now); err != nil || stuck != 1 {
		t.Fatalf("a lease past the window was not counted as stuck: %d (%v)", stuck, err)
	}
}

// QueuedDemand counts only requests a reservation could actually select.
// Counting the rest would publish demand the scheduler is not declining to
// serve, which is the difference between an outage and a working pool.
func TestQueuedDemandCountsOnlySelectableOffers(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 3)

	demand, err := s.QueuedDemand(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if demand.Queued != 3 || demand.ByRepo[keys[0].RepositoryID] != 3 {
		t.Fatalf("queued demand = %d (%v), want 3", demand.Queued, demand.ByRepo)
	}
	if demand.OldestOffer == "" {
		t.Fatal("queued demand carries no durable offer instant")
	}
	oldest, err := time.Parse(time.RFC3339Nano, demand.OldestOffer)
	if err != nil {
		t.Fatalf("the oldest offer instant is not RFC3339Nano: %v", err)
	}

	// A reservation takes one request out of the queue.
	mustReserve(t, s)
	after, err := s.QueuedDemand(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Queued != 2 {
		t.Fatalf("queued demand after one reservation = %d, want 2", after.Queued)
	}
	if next, err := time.Parse(time.RFC3339Nano, after.OldestOffer); err != nil || next.Before(oldest) {
		t.Fatalf("the oldest offer instant moved backwards: %v (%v)", after.OldestOffer, err)
	}
}

// An ineligible repository still holds its leases, but its queued requests are
// not demand the pool is declining to serve.
func TestQueuedDemandExcludesAnIneligibleRepository(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 2)
	if _, err := s.db.Exec(`UPDATE repositories SET eligible=0 WHERE id=?`, keys[0].RepositoryID); err != nil {
		t.Fatal(err)
	}
	demand, err := s.QueuedDemand(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if demand.Queued != 0 || demand.OldestOffer != "" {
		t.Fatalf("an ineligible repository's offers were counted: %+v", demand)
	}
}

func TestAssignedJobsAndOpenSessionsAreReadPerRepository(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 1)

	assigned, err := s.AssignedJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := assigned[keys[0].RepositoryID]; !present {
		t.Fatalf("no assigned-job statistic for the offered repository: %v", assigned)
	}
	open, err := s.OpenSessionRepositories(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if open[keys[0].RepositoryID] {
		t.Fatal("a repository with no session record reported an open session")
	}
	if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,session_id,phase,created,updated)
VALUES('owner-1',?,?,'11111111-1111-1111-1111-111111111111','active','t','t')`, keys[0].RepositoryID, keys[0].ScaleSetID); err != nil {
		t.Fatal(err)
	}
	if open, err = s.OpenSessionRepositories(ctx); err != nil || !open[keys[0].RepositoryID] {
		t.Fatalf("an active session was not reported: %v (%v)", open, err)
	}
}

func TestProvisioningSuppressionReportsThePoolWideBackoff(t *testing.T) {
	s, _ := openTestStore(t, 2)
	now := time.Now().UTC()

	suppressed, until, err := s.ProvisioningSuppression(ctx, now)
	if err != nil || suppressed || until != "" {
		t.Fatalf("an unsuppressed pool reported %v/%q (%v)", suppressed, until, err)
	}
	deadline := now.Add(time.Minute).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE platform SET provisioning_quarantine_until=? WHERE id=1`, deadline); err != nil {
		t.Fatal(err)
	}
	if suppressed, until, err = s.ProvisioningSuppression(ctx, now); err != nil || !suppressed || until != deadline {
		t.Fatalf("a live backoff reported %v/%q (%v)", suppressed, until, err)
	}
	if suppressed, _, err = s.ProvisioningSuppression(ctx, now.Add(2*time.Minute)); err != nil || suppressed {
		t.Fatalf("an elapsed backoff still reported suppression (%v)", err)
	}
}

// B7: the fence and its reason are process-local and reset on restart, which is
// correct — a persisted inventory would outlive its own validity.
func TestInventoryObservationRecordsEveryOutcome(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 1)

	// A process that has taken no backend inventory has none to report. That is
	// the honest state of a fresh controller, and the startup grace in the
	// composer is what keeps it from reading as a fault.
	if observation := s.Inventory(); observation.Complete || observation.ObservedAt != "" {
		t.Fatalf("a process with no backend inventory reported %+v", observation)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 2}); err != nil {
		t.Fatal(err)
	}
	healthy := s.Inventory()
	if !healthy.Complete || healthy.FreeSlots != 2 || healthy.ObservedAt == "" || healthy.UntrackedSeen || healthy.Diagnostic != "" {
		t.Fatalf("a clean inventory was recorded as %+v", healthy)
	}

	_ = s.ReconcileBackendInventory(ctx, BackendInventory{Complete: false})
	incomplete := s.Inventory()
	if incomplete.Complete || incomplete.UntrackedSeen || incomplete.Diagnostic != inventoryIncomplete {
		t.Fatalf("an incomplete inventory was recorded as %+v", incomplete)
	}

	_ = s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 2,
		Workers: []WorkerObservation{{LeaseID: "ghost", ResourceID: "ghost"}}})
	untracked := s.Inventory()
	if !untracked.UntrackedSeen || untracked.Complete || untracked.Diagnostic != inventoryUntracked {
		t.Fatalf("an untracked resource was recorded as %+v", untracked)
	}
	if ready, admission := s.Admission(); ready || admission != 0 {
		t.Fatalf("an untracked resource left admission open: ready=%v admission=%d", ready, admission)
	}
}

// B8: issuance reaches the report as counts. Nothing here names a lease, a
// capability, a runner or a payload.
func TestBootstrapCountersAreCumulativeSinceStart(t *testing.T) {
	s, _ := openTestStore(t, 2)
	broker := newJITBroker(s, nil)

	if counters := broker.Counters(); counters != (BootstrapCounters{}) {
		t.Fatalf("a fresh broker reported %+v, want every counter at zero", counters)
	}
	broker.secrets["lease-a"] = &jitSecret{expires: time.Now().Add(-time.Minute)}
	broker.secrets["lease-b"] = &jitSecret{expires: time.Now().Add(time.Hour)}
	if outstanding := broker.Counters().Outstanding; outstanding != 2 {
		t.Fatalf("outstanding capabilities = %d, want 2", outstanding)
	}

	broker.Expire()
	expired := broker.Counters()
	if expired.Expired != 1 {
		t.Fatalf("expired = %d, want 1", expired.Expired)
	}
	if expired.Outstanding != 1 {
		t.Fatalf("outstanding after expiry = %d, want the unexpired one", expired.Outstanding)
	}

	broker.count(1, 0, 0)
	broker.count(0, 0, 1)
	final := broker.Counters()
	if final.Delivered != 1 || final.Failed != 1 || final.Expired != 1 {
		t.Fatalf("counters = %+v, want one of each", final)
	}
}

// The unused-worker retirement counter is cumulative since process start, like
// every other counter the report publishes.
func TestRetiredUnusedCounterStartsAtZero(t *testing.T) {
	s, _ := openTestStore(t, 2)
	if retired := s.RetiredUnused(); retired != 0 {
		t.Fatalf("a fresh process reported %d retired workers", retired)
	}
	if _, err := s.RetireUnusedWorkers(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if retired := s.RetiredUnused(); retired != 0 {
		t.Fatalf("a sweep with nothing to retire counted %d", retired)
	}
}

func TestWorkerCountsSeparateTheTwoDeadlines(t *testing.T) {
	s, _ := openTestStore(t, 2)
	now := time.Now().UTC()
	counts, err := s.WorkerCounts(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if counts != (WorkerCounts{}) {
		t.Fatalf("an idle pool reported %+v, want every worker count at zero", counts)
	}
}
