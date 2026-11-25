package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// reopenTestStore closes a store and opens the same state directory again,
// which is the only way to exercise the resume path: the distrust decision runs
// once, inside Open's migration transaction.
func reopenTestStore(t *testing.T, s *Store, dir string, capacity int) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Config{Platform: "linux", InstallationID: 42, Capacity: capacity, RepositoryAllowlist: testAllowlist})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if err := reopened.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	return reopened
}

// brokerAssign journals the github.com broker shape: a lone JobAssigned with
// runnerRequestId 0, identified only by its string job ID. This is the shape
// AcquireReserved settles on its own inference, with no GitHub call.
func brokerAssign(t *testing.T, s *Store, key SetKey, jobID string, messageID int) {
	t.Helper()
	envelope := Envelope{
		MessageID:  messageID,
		Statistics: scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		Events: []Event{{JobMessageBase: scaleset.JobMessageBase{
			JobMessageType:  scaleset.JobMessageType{MessageType: scaleset.MessageTypeJobAssigned},
			RunnerRequestID: 0,
			OwnerName:       "owner",
			RepositoryName:  fmt.Sprintf("repo%d", key.RepositoryID),
			JobID:           jobID,
		}}},
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), envelope); err != nil {
		t.Fatal(err)
	}
}

func jobState(t *testing.T, s *Store, key SetKey, jobKey string) string {
	t.Helper()
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`,
		key.RepositoryID, key.ScaleSetID, jobKey).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

// TestResumeDistrustsBrokerAssignedJobsUntilReconfirmed is the allocator half
// of the 2026-09-22 starvation. A broker-assigned job survived a controller
// restart in state 'assigned', ReserveNext re-selected it, and AcquireReserved
// certified acceptance without asking GitHub anything -- so the pool minted a
// JIT runner about every four minutes for a job GitHub had already re-routed or
// completed. Each runner connected, was offered nothing, and exited 0.
//
// The three arms are the whole claim: the fixture IS reservable before a
// restart (so the refusal below is the restart, not the fixture), a restart
// alone does not make it reservable again, and a fresh envelope does.
func TestResumeDistrustsBrokerAssignedJobsUntilReconfirmed(t *testing.T) {
	const jobID = "workflow-job-broker-1"

	t.Run("trusted-before-restart", func(t *testing.T) {
		s, _ := openTestStore(t, 1)
		key := prepare(t, s, 1)[0]
		brokerAssign(t, s, key, jobID, 1)
		lease := mustReserve(t, s)
		if lease.RequestID != 0 || lease.Set != key {
			t.Fatalf("fixture did not journal the broker shape: %+v", lease)
		}
	})

	t.Run("no-redelivery-refuses", func(t *testing.T) {
		s, dir := openTestStore(t, 1)
		key := prepare(t, s, 1)[0]
		brokerAssign(t, s, key, jobID, 1)
		restarted := reopenTestStore(t, s, dir, 1)

		if state := jobState(t, restarted, key, jobKeyStringPrefix+jobID); state != jobStateAssignedStale {
			t.Fatalf("resume left a pre-restart broker assignment trusted: state=%q", state)
		}
		// A fake session that never re-delivers. AcquireJobs must not be reached
		// at all -- not because it would refuse, but because no reservation was
		// handed out for it to act on.
		calls := 0
		api := acquireFunc(func(context.Context, []int64) ([]int64, error) {
			calls++
			return nil, nil
		})
		lease, err := restarted.ReserveNext(ctx)
		if err == nil {
			t.Fatalf("a pre-restart broker assignment was reserved after resume: %+v", lease)
		}
		if err != ErrNoDemand {
			t.Fatalf("expected no demand after resume, got: %v", err)
		}
		if lease.ID != "" {
			if err := restarted.AcquireReserved(ctx, lease.ID, api); err != nil {
				t.Fatal(err)
			}
		}
		if calls != 0 {
			t.Fatalf("acquisition was attempted %d times for an unconfirmed assignment", calls)
		}
	})

	t.Run("redelivery-serves", func(t *testing.T) {
		s, dir := openTestStore(t, 1)
		key := prepare(t, s, 1)[0]
		brokerAssign(t, s, key, jobID, 1)
		restarted := reopenTestStore(t, s, dir, 1)

		// The confirmation signal that actually exists: GitHub retries an
		// assignment nobody picked up by assigning the same job again, which
		// arrives as a fresh JobAssigned in a new envelope.
		brokerAssign(t, restarted, key, jobID, 2)
		if state := jobState(t, restarted, key, jobKeyStringPrefix+jobID); state != jobStateAssigned {
			t.Fatalf("a fresh JobAssigned did not re-confirm the stale row: state=%q", state)
		}
		lease := mustReserve(t, restarted)
		if lease.Set != key || lease.JobKey != jobKeyStringPrefix+jobID {
			t.Fatalf("re-confirmed assignment was not served: %+v", lease)
		}
		// Only one durable row exists for this job: re-confirmation must resolve
		// back to it rather than opening a second one beside it.
		var rows int
		if err := restarted.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE repo_id=? AND set_id=?`,
			key.RepositoryID, key.ScaleSetID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("re-confirmation split one job across %d rows", rows)
		}
	})

	t.Run("completion-drops", func(t *testing.T) {
		s, dir := openTestStore(t, 1)
		key := prepare(t, s, 1)[0]
		brokerAssign(t, s, key, jobID, 1)
		restarted := reopenTestStore(t, s, dir, 1)

		// The other half of "the envelope says the job is gone": a completion
		// outranks the stale row and leaves it terminal, so it is never served.
		completion := Envelope{MessageID: 3, Statistics: scaleset.RunnerScaleSetStatistic{},
			Events: []Event{{JobMessageBase: scaleset.JobMessageBase{
				JobMessageType:  scaleset.JobMessageType{MessageType: scaleset.MessageTypeJobCompleted},
				RunnerRequestID: 0, OwnerName: "owner", RepositoryName: fmt.Sprintf("repo%d", key.RepositoryID),
				JobID: jobID}, Result: "succeeded"}}}
		if err := restarted.RecordEnvelope(ctx, key, uuid.New(), completion); err != nil {
			t.Fatal(err)
		}
		if state := jobState(t, restarted, key, jobKeyStringPrefix+jobID); state != jobStateTerminal {
			t.Fatalf("a completion did not retire the stale row: state=%q", state)
		}
		if lease, err := restarted.ReserveNext(ctx); err != ErrNoDemand {
			t.Fatalf("a completed job was reserved after resume: %+v %v", lease, err)
		}
	})
}

// TestStaleAssignmentDemandIsBoundedByAWindow is the review's Critical finding.
// A stale row counts as demand so the planner keeps asking GitHub about it --
// but when GitHub has re-routed the job, no envelope ever names it again, and
// an unbounded row then claims the pool's demand forever. Measured before the
// bound: repo1=1 repo2=0 on ten consecutive sweeps, which is the starvation
// shape this branch exists to remove, recreated by its own fix.
//
// Both sides of the window are asserted. Inside it the row is demand, because a
// job GitHub is still retrying must be advertised for. Outside it the row is
// gone and the neighbour holding real work gets the slot.
func TestStaleAssignmentDemandIsBoundedByAWindow(t *testing.T) {
	const jobID = "workflow-job-phantom"
	newFixture := func(t *testing.T) (*Store, []SetKey) {
		t.Helper()
		s, dir := openTestStore(t, 1)
		keys := prepare(t, s, 2)
		brokerAssign(t, s, keys[0], jobID, 1)
		restarted := reopenTestStore(t, s, dir, 1)
		activateTestSessions(t, restarted, keys)
		// The neighbour holds real, reservable work the whole time.
		offer(t, restarted, keys[1], 1)
		if state := jobState(t, restarted, keys[0], jobKeyStringPrefix+jobID); state != jobStateAssignedStale {
			t.Fatalf("fixture: state=%q", state)
		}
		return restarted, keys
	}

	// demandFor runs journaledDemand at a given stale cutoff, which is the one
	// decision the bound makes.
	demandFor := func(t *testing.T, s *Store, cutoff time.Time) map[int64]int {
		t.Helper()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		demand, err := journaledDemand(ctx, tx, cutoff)
		if err != nil {
			t.Fatal(err)
		}
		return demand
	}

	t.Run("inside-the-window-it-is-still-demand", func(t *testing.T) {
		s, keys := newFixture(t)
		demand := demandFor(t, s, time.Now().UTC().Add(-staleAssignmentWindow))
		if demand[keys[0].RepositoryID] != 1 {
			t.Fatalf("a fresh stale row stopped counting as demand: %+v", demand)
		}
		if state := jobState(t, s, keys[0], jobKeyStringPrefix+jobID); state != jobStateAssignedStale {
			t.Fatalf("row did not survive its window: state=%q", state)
		}
	})

	t.Run("outside-the-window-the-neighbour-is-served", func(t *testing.T) {
		s, keys := newFixture(t)
		// Age the stamp past the window rather than waiting five minutes. This is
		// the same column distrustResumedAssignments writes.
		if _, err := s.db.Exec(`UPDATE jobs SET offered=? WHERE state=?`,
			time.Now().UTC().Add(-2*staleAssignmentWindow).Format(time.RFC3339Nano), jobStateAssignedStale); err != nil {
			t.Fatal(err)
		}
		if demand := demandFor(t, s, time.Now().UTC().Add(-staleAssignmentWindow)); demand[keys[0].RepositoryID] != 0 {
			t.Fatalf("an aged-out phantom still counted as demand: %+v", demand)
		}
		// THE ROW IS STILL THERE. Deleting it violates the leases foreign key.
		if state := jobState(t, s, keys[0], jobKeyStringPrefix+jobID); state != jobStateAssignedStale {
			t.Fatalf("the aged-out row was removed rather than ignored: state=%q", state)
		}
		// Ten sweeps, because one proves nothing about a rotation: the probe that
		// found this watched repo2 read 0 on every one of ten.
		served := 0
		for sweep := 1; sweep <= 10; sweep++ {
			plan, err := s.PlanAdvertisements(ctx, keys)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Capacity[keys[1]] > 0 {
				served++
			}
		}
		if served == 0 {
			t.Fatal("the repository with real queued work was advertised 0 on all ten sweeps")
		}
	})

	t.Run("a-reconfirmed-row-counts-again-and-is-reservable", func(t *testing.T) {
		s, keys := newFixture(t)
		if _, err := s.db.Exec(`UPDATE jobs SET offered=? WHERE state=?`,
			time.Now().UTC().Add(-2*staleAssignmentWindow).Format(time.RFC3339Nano), jobStateAssignedStale); err != nil {
			t.Fatal(err)
		}
		// A retained row is what a later event resolves back to, and the
		// re-confirmation must refresh the stamp as well as the state -- otherwise
		// a revived job would be aged out again on the next sweep.
		brokerAssign(t, s, keys[0], jobID, 2)
		if state := jobState(t, s, keys[0], jobKeyStringPrefix+jobID); state != jobStateAssigned {
			t.Fatalf("re-confirmed row: state=%q", state)
		}
		if demand := demandFor(t, s, time.Now().UTC().Add(-staleAssignmentWindow)); demand[keys[0].RepositoryID] != 1 {
			t.Fatalf("a re-confirmed assignment did not count as demand: %+v", demand)
		}
		if lease, err := s.ReserveNext(ctx); err != nil || lease.JobKey != jobKeyStringPrefix+jobID {
			t.Fatalf("a re-confirmed assignment was not reservable: %+v %v", lease, err)
		}
	})
}

// TestAStaleRowWithAReleasedLeaseIsSafeToAgeOut is the reviewer's probe shape,
// and it is the reason the aged-out row is retained rather than deleted.
//
// leases(repo_id,set_id,job_key) is a foreign key into jobs, leases are never
// deleted (FinishCleanup only moves one to phase 'released'), and foreign_keys
// is ON. So a job that was served, released, cancelled, re-assigned and then
// inherited across a restart has a RELEASED lease still pointing at it --
// invisible to any `phase!='released'` guard. Deleting it raised
// `FOREIGN KEY constraint failed (787)` out of Maintain, and a Maintain error
// aborts the whole cycle before polling, provisioning or draining: the pool
// stops until someone edits the state directory by hand.
func TestAStaleRowWithAReleasedLeaseIsSafeToAgeOut(t *testing.T) {
	const jobID = "workflow-job-released-lease"
	s, dir := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	brokerAssign(t, s, keys[0], jobID, 1)

	// Serve it, then release the lease through the ordinary cleanup path.
	lease := mustReserve(t, s)
	finish(t, s, lease)
	released, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if released.Phase != Released {
		t.Fatalf("fixture lease is not released: phase=%s", released.Phase)
	}

	// GitHub cancels the assignment and assigns the same job again, which is the
	// retry the broker performs when nobody picks one up.
	cancelled := Envelope{MessageID: 2, Statistics: scaleset.RunnerScaleSetStatistic{},
		Events: []Event{{JobMessageBase: scaleset.JobMessageBase{
			JobMessageType:  scaleset.JobMessageType{MessageType: scaleset.MessageTypeJobCompleted},
			RunnerRequestID: 0, OwnerName: "owner", RepositoryName: fmt.Sprintf("repo%d", keys[0].RepositoryID),
			JobID: jobID}, Result: "canceled"}}}
	if err := s.RecordEnvelope(ctx, keys[0], uuid.New(), cancelled); err != nil {
		t.Fatal(err)
	}
	brokerAssign(t, s, keys[0], jobID, 3)

	restarted := reopenTestStore(t, s, dir, 1)
	activateTestSessions(t, restarted, keys)
	if state := jobState(t, restarted, keys[0], jobKeyStringPrefix+jobID); state != jobStateAssignedStale {
		t.Fatalf("fixture: state=%q", state)
	}
	// A released lease still references the job.
	var referencing int
	if err := restarted.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE job_key=?`,
		jobKeyStringPrefix+jobID).Scan(&referencing); err != nil {
		t.Fatal(err)
	}
	if referencing == 0 {
		t.Fatal("fixture has no lease referencing the job; it cannot test the foreign key")
	}

	// Age it out and drive a full maintenance cycle. Nothing may error.
	if _, err := restarted.db.Exec(`UPDATE jobs SET offered=? WHERE state=?`,
		time.Now().UTC().Add(-2*staleAssignmentWindow).Format(time.RFC3339Nano), jobStateAssignedStale); err != nil {
		t.Fatal(err)
	}
	broker := faithfulBroker(t, restarted)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
	}
	backend.destroy = func(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
		return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID,
			ProcessAbsent: true, StorageAbsent: true}, nil
	}
	lifecycle, err := newPoolLifecycle(restarted, allocatorFunc(func(context.Context) (Lease, error) { return Lease{}, ErrNoDemand }),
		broker, backend, "https://controller.example:8443")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatalf("maintenance failed on a stale row with a released lease: %v", err)
	}
	// The planner must also run clean and stop counting the phantom as demand.
	// Asserted on the demand itself rather than the plan: with both
	// repositories idle, either one legitimately taking the rotating
	// exploratory slot would read like a phantom win.
	if _, err := restarted.PlanAdvertisements(ctx, keys); err != nil {
		t.Fatalf("the sweep failed after the row aged out: %v", err)
	}
	tx, err := restarted.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	demand, err := journaledDemand(ctx, tx, time.Now().UTC().Add(-staleAssignmentWindow))
	if err != nil {
		t.Fatal(err)
	}
	if demand[keys[0].RepositoryID] != 0 {
		t.Fatalf("the aged-out phantom still counted as demand: %+v", demand)
	}
}

// TestUpgradePathRefusesASkippedStep pins the migration gate. The old test was
// `version == journalVersion-1`, which at journalVersion 14 would have accepted
// a version-12 journal and stamped it 14 -- skipping 12->13 silently, with no
// row rewritten and no error.
func TestUpgradePathRefusesASkippedStep(t *testing.T) {
	if path, err := upgradePath(journalVersionStaleAssignments-1, journalVersionStaleAssignments); err != nil ||
		len(path) != 1 || path[0] != journalVersionStaleAssignments-1 {
		t.Fatalf("the declared 12->13 step did not produce a one-step path: %v %v", path, err)
	}
	// Every declared step, in order, from the version the deployed journal is at.
	if path, err := upgradePath(journalVersionStaleAssignments-1, journalVersion); err != nil ||
		len(path) != 3 || path[0] != 12 || path[1] != 13 || path[2] != 14 {
		t.Fatalf("the 12 -> 15 chain did not produce all three declared steps: %v %v", path, err)
	}
	// The hypothetical NEXT constant, with no step declared for the current one.
	path, err := upgradePath(journalVersionStaleAssignments-1, journalVersion+1)
	if err == nil {
		t.Fatalf("a journal was carried across an undeclared step: %v", path)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("no migration step is declared for version %d", journalVersion)) {
		t.Fatalf("the refusal does not name the missing step: %v", err)
	}
	if !strings.Contains(err.Error(), "move the controller state directory aside") {
		t.Fatalf("the refusal does not tell the operator what to do: %v", err)
	}
	// A fresh journal and a current one both need no steps.
	for _, from := range []int{0, journalVersion} {
		if path, err := upgradePath(from, journalVersion); err != nil || len(path) != 0 {
			t.Fatalf("version %d wanted upgrade steps %v (%v)", from, path, err)
		}
	}
}

// TestAnIdleRepositoryIsReachedOnceDemandIsServed pins the two ways a
// repository advertising 0 still receives work. GitHub assigns a job only to a
// scale set that asked for capacity, so such a repository needs either a probe
// or a demand signal that does not depend on being advertised.
//
// The review's third finding once made the probe compete with a backlog on a
// one-slot pool. That cost the backlog its slot on most sweeps once the pool
// served 48 repositories (2026-09-24), so demand is now served first. What keeps
// the idle repository reachable is (1) a REST demand reading, which enters it
// into the demand rotation, and (2) the probe, whenever headroom exceeds the
// repositories with unmet demand.
func TestAnIdleRepositoryIsReachedOnceDemandIsServed(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)
	setAdmission := func(free int) {
		t.Helper()
		if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: free}); err != nil {
			t.Fatal(err)
		}
		if _, admission := s.Admission(); admission != free {
			t.Fatalf("fixture admission is %d, not %d", admission, free)
		}
	}
	// A holds a backlog; B has nothing journaled.
	offer(t, s, keys[0], 3)
	setAdmission(1)

	// One slot and one repository with demand: the backlog keeps it.
	for sweep := 1; sweep <= 3; sweep++ {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Capacity[keys[0]] != 1 || plan.Capacity[keys[1]] != 0 {
			t.Fatalf("sweep %d spent the only slot on a probe while demand waited: %+v", sweep, plan.Capacity)
		}
	}

	// A REST reading for B puts it in the demand rotation, so the two alternate.
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{keys[1].RepositoryID: 1}, now, now.Add(time.Hour))
	served := map[SetKey]int{}
	for sweep := 1; sweep <= 4; sweep++ {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Capacity[keys[0]]+plan.Capacity[keys[1]] != 1 {
			t.Fatalf("sweep %d did not promise the one free slot exactly once: %+v", sweep, plan.Capacity)
		}
		for _, key := range keys {
			served[key] += plan.Capacity[key]
		}
	}
	if served[keys[0]] != 2 || served[keys[1]] != 2 {
		t.Fatalf("REST demand did not share the slot fairly across four sweeps: %+v", served)
	}

	// No REST reading, but headroom above demand: B gets the probe.
	s.RecordRESTDemand(map[int64]int{keys[1].RepositoryID: 0}, now, now.Add(time.Hour))
	setAdmission(2)
	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Capacity[keys[0]] != 1 || plan.Capacity[keys[1]] != 1 {
		t.Fatalf("spare headroom did not probe the idle repository: %+v", plan.Capacity)
	}
}

// TestACrashedWorkerArmsTheProvisioningBreaker is the review's second finding.
// Retiring a terminated worker through ordinary cleanup writes no
// provisioning_failures row, so a bad image would mint, crash and mint again
// every cycle with the three-in-fifteen-minutes breaker never arming. The fast
// path is only for the clean, expected exit this branch was written for.
func TestACrashedWorkerArmsTheProvisioningBreaker(t *testing.T) {
	for _, arm := range []struct {
		name     string
		jitPhase string
		exit     int
		breaker  bool
	}{
		{name: "idle-runner-exit-zero", jitPhase: "stream-written", exit: 0, breaker: false},
		{name: "crashed-after-bootstrap", jitPhase: "stream-written", exit: 1, breaker: true},
		{name: "crashed-before-bootstrap", jitPhase: "planned", exit: 1, breaker: true},
		// Exit 0 but the bootstrap was never consumed: the runner cannot have
		// been offered anything, so this is a provisioning fault too.
		{name: "vanished-before-bootstrap", jitPhase: "planned", exit: 0, breaker: true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			lease := acquiredLease(t, s)
			broker := faithfulBroker(t, s)
			backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
			backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
				return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
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
			if _, err := lifecycle.ProvisionNext(ctx); err != nil {
				t.Fatal(err)
			}
			current, err := s.Lease(ctx, lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE jit_transfers SET phase=? WHERE lease_id=?`, arm.jitPhase, lease.ID); err != nil {
				t.Fatal(err)
			}
			backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
				{LeaseID: lease.ID, ResourceID: current.RunnerName, Terminated: true, ExitCode: arm.exit}}}
			if err := lifecycle.ReconcileInventory(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := lifecycle.Maintain(ctx); err != nil {
				t.Fatal(err)
			}
			var failures int
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM provisioning_failures`).Scan(&failures); err != nil {
				t.Fatal(err)
			}
			if arm.breaker && failures == 0 {
				t.Fatal("an abnormal termination armed no provisioning failure; the breaker can never trip")
			}
			if !arm.breaker && failures != 0 {
				t.Fatalf("an ordinary idle exit was counted as a provisioning failure: failures=%d", failures)
			}
		})
	}
}

// TestPlannerPrefersJournaledDemandOverFrozenStatistics is the planner half of
// the same outage. scale_sets.statistics is written on session CREATE, on a
// refresh that carries statistics, and on a poll envelope -- and github.com's
// refresh carries none. After a resume, TotalAssignedJobs is frozen at whatever
// the previous process last saw, and planCapacity hands headroom strictly by
// that number: the frozen count took the one slot every sweep while the
// repository with real queued jobs was advertised 0 for 100 minutes.
func TestPlannerPrefersJournaledDemandOverFrozenStatistics(t *testing.T) {
	s, dir := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)

	// Repository A: a frozen non-zero statistic and no job rows at all.
	frozen, err := json.Marshal(scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(frozen), timestamp(), keys[0].RepositoryID, keys[0].ScaleSetID); err != nil {
		t.Fatal(err)
	}
	// Repository B: one real queued job row and a zero statistic.
	offer(t, s, keys[1], 1)
	idle, err := json.Marshal(scaleset.RunnerScaleSetStatistic{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(idle), timestamp(), keys[1].RepositoryID, keys[1].ScaleSetID); err != nil {
		t.Fatal(err)
	}

	// The restart is what freezes both statistics: every observation above was
	// made by the previous process, and github.com's refresh carries none, so
	// nothing re-observes them on resume.
	restarted := reopenTestStore(t, s, dir, 1)
	// Asserted over a full rotation, not on one sweep. A, having no journaled
	// demand, legitimately takes the sweep's single exploratory slot when the
	// cursor reaches it; what the frozen statistic must NOT buy it is the slot
	// on EVERY sweep, which is what starved the repository holding real work for
	// 100 minutes. B has one queued row, so it must be served at least once here.
	servedB, servedA := 0, 0
	for sweep := 1; sweep <= 4; sweep++ {
		plan, err := restarted.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Capacity[keys[1]] > 0 {
			servedB++
		}
		if plan.Capacity[keys[0]] > 0 {
			servedA++
		}
	}
	if servedB == 0 {
		t.Fatalf("the repository with a real queued job was advertised 0 on all four sweeps (A took %d)", servedA)
	}
	if servedA == 4 {
		t.Fatal("a frozen statistic won the only slot on every sweep")
	}

	// A statistic observed by THIS process is trusted again, so the fix narrows
	// what is believed rather than discarding the statistic outright.
	if _, err := restarted.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(frozen), timestamp(), keys[0].RepositoryID, keys[0].ScaleSetID); err != nil {
		t.Fatal(err)
	}
	freshA := 0
	for sweep := 1; sweep <= 4; sweep++ {
		fresh, err := restarted.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		freshA += fresh.Capacity[keys[0]]
	}
	// With the statistic trusted again A has real demand, so it competes for the
	// slot on every sweep rather than only when the exploratory turn reaches it.
	if freshA < 2 {
		t.Fatalf("a statistic observed by this process was ignored: A served %d of four sweeps", freshA)
	}
}

// TestTerminatedWorkerReleasesItsSlotInOneMaintain pins the third leg. A runner
// offered no work connects, idles and exits within about thirty seconds, but
// RetireUnusedWorkers keys on jit_transfers.updated with a five-minute timeout
// -- so on a one-slot pool the slot stayed charged for four and a half minutes
// out of every five while real jobs queued.
func TestTerminatedWorkerReleasesItsSlotInOneMaintain(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker := faithfulBroker(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
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
	if _, err := lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
	provisioning, err := s.Lease(ctx, lease.ID)
	if err != nil || provisioning.Phase != Provisioning {
		t.Fatalf("fixture lease is not provisioning: %+v %v", provisioning, err)
	}

	// The unused-worker timeout cannot reach this lease: its JIT transfer was
	// written seconds ago, so any release below is the termination evidence.
	if retired, err := s.RetireUnusedWorkers(ctx, time.Now()); err != nil || len(retired) != 0 {
		t.Fatalf("the five-minute timeout already claimed the lease: %v %v", retired, err)
	}

	// The runner exited 0, which is what a runner offered no job does. That
	// zero is the diagnosis, not a success.
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
		{LeaseID: lease.ID, ResourceID: provisioning.RunnerName, Terminated: true, ExitCode: 0}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatalf("one maintenance pass refused to retire a terminated worker: %v", err)
	}
	settled, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Phase != Released {
		t.Fatalf("a terminated worker did not release its slot in one Maintain: phase=%s diagnostic=%q",
			settled.Phase, settled.Diagnostic)
	}
}

// TestALiveWorkerIsNeverRetiredAsTerminated is the negative control for the
// arm above. Terminated is positive evidence and nothing else may stand in for
// it: a worker the adapter reports as merely present keeps its slot, which is
// what stops a backend that cannot observe termination from having every
// in-flight job retired underneath it.
func TestALiveWorkerIsNeverRetiredAsTerminated(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	broker := faithfulBroker(t, s)
	backend := &backendFixture{inventory: BackendInventory{Complete: true, FreeSlots: 1}}
	backend.create = func(_ context.Context, intent WorkerCreateIntent) (WorkerObservation, error) {
		return WorkerObservation{LeaseID: intent.LeaseID, ResourceID: intent.ResourceID}, nil
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
	if _, err := lifecycle.ProvisionNext(ctx); err != nil {
		t.Fatal(err)
	}
	current, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 0, Workers: []WorkerObservation{
		{LeaseID: lease.ID, ResourceID: current.RunnerName}}}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	settled, err := s.Lease(ctx, lease.ID)
	if err != nil || settled.Phase != Provisioning {
		t.Fatalf("a live worker lost its slot: %+v %v", settled, err)
	}
}
