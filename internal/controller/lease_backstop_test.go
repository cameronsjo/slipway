package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

// strand puts a fresh lease into phase, with the given request id, last
// updated `age` ago -- the shape a ProvisionNext call leaves when it gives up.
func strand(t *testing.T, s *Store, phase Phase, requestID int64, age time.Duration) Lease {
	t.Helper()
	lease := mustReserve(t, s)
	stamp := time.Now().UTC().Add(-age).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE leases SET phase=?,runner_request_id=?,updated=? WHERE id=?`, phase, requestID, stamp, lease.ID); err != nil {
		t.Fatal(err)
	}
	return lease
}

func leasePhase(t *testing.T, s *Store, id string) Phase {
	t.Helper()
	lease, err := s.Lease(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return lease.Phase
}

// homelab#1095 (D7): every pre-provisioning phase that can have created
// nothing outside the journal is released once it is stranded, and the log
// names the lease.
func TestLeasesStrandedBeforeProvisioningAreReleased(t *testing.T) {
	for _, tc := range []struct {
		phase     Phase
		requestID int64
	}{
		{Reserved, 3},
		{Reserved, 0},
		{AcquisitionIntent, 0},
		{Acquired, 0},
		{Acquired, 3},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			logs := captureLogs(t)
			s, _ := openTestStore(t, 1)
			offer(t, s, prepare(t, s, 1)[0], 2)
			lease := strand(t, s, tc.phase, tc.requestID, strandedLeaseWindow+time.Second)
			if _, err := s.ReserveNext(ctx); err == nil {
				t.Fatal("fixture: a second slot was reserved on a one-slot pool")
			}
			released, err := s.ReleaseStrandedLeases(ctx, time.Now())
			if err != nil || len(released) != 1 || released[0] != lease.ID || leasePhase(t, s, lease.ID) != Released {
				t.Fatalf("a stranded %s lease was not released: %v %v", tc.phase, released, err)
			}
			if !strings.Contains(logs.String(), "Released a lease stranded before provisioning") || !strings.Contains(logs.String(), "lease_id="+lease.ID) {
				t.Fatalf("the release was not logged with its lease: %s", logs.String())
			}
			// The slot is free again.
			if _, err := s.ReserveNext(ctx); err != nil {
				t.Fatalf("the released slot could not be reserved again: %v", err)
			}
		})
	}
}

// The negative cases, each for the reason the release would be unsafe or early.
func TestLeasesThatMayHaveActedOutsideTheJournalAreNotReleased(t *testing.T) {
	t.Run("inside the window", func(t *testing.T) {
		s, _ := openTestStore(t, 1)
		offer(t, s, prepare(t, s, 1)[0], 1)
		lease := strand(t, s, Acquired, 0, strandedLeaseWindow-time.Minute)
		if released, err := s.ReleaseStrandedLeases(ctx, time.Now()); err != nil || len(released) != 0 || leasePhase(t, s, lease.ID) != Acquired {
			t.Fatalf("a lease inside the window was released: %v %v", released, err)
		}
	})
	for _, phase := range []Phase{AcquisitionIntent, AcquisitionUnknown} {
		t.Run(string(phase)+" with a real request id", func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			offer(t, s, prepare(t, s, 1)[0], 1)
			lease := strand(t, s, phase, 3, time.Hour)
			if released, err := s.ReleaseStrandedLeases(ctx, time.Now()); err != nil || len(released) != 0 || leasePhase(t, s, lease.ID) != phase {
				t.Fatalf("an uncertain AcquireJobs POST was released on a clock: %v %v", released, err)
			}
		})
	}
	t.Run("acquired with a JIT row", func(t *testing.T) {
		s, _ := openTestStore(t, 1)
		offer(t, s, prepare(t, s, 1)[0], 1)
		lease := strand(t, s, Acquired, 0, time.Hour)
		now := timestamp()
		if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,created,expires,updated) VALUES(?,?,'jit-intent',?,?,?)`,
			lease.ID, []byte("test"), now, now, now); err != nil {
			t.Fatal(err)
		}
		if released, err := s.ReleaseStrandedLeases(ctx, time.Now()); err != nil || len(released) != 0 || leasePhase(t, s, lease.ID) != Acquired {
			t.Fatalf("a lease with a registration intent was released: %v %v", released, err)
		}
	})
}

// The live path: beginJIT refuses (here, a stale inventory), so Prepare fails
// and QuarantineBeforeBackendCreate refuses a lease that is not provisioning.
// The lease is left acquired with no JIT row -- charged, with no exit, until
// the backstop -- and Maintain releases it.
func TestABeginJITRefusalNoLongerStrandsTheSlot(t *testing.T) {
	s, _, lease, b, _ := jitFixture(t)
	if err := s.ReconcileInventory(ctx, false, nil); err == nil {
		t.Fatal("fixture: an unknown inventory was accepted")
	}
	if _, err := b.Prepare(ctx, lease.ID); err == nil {
		t.Fatal("fixture: beginJIT did not refuse")
	}
	if err := s.QuarantineBeforeBackendCreate(ctx, lease.ID, "JIT preparation failed before backend creation", time.Now()); err == nil {
		t.Fatal("fixture: the pre-create quarantine accepted an acquired lease; the stranded shape no longer exists")
	}
	if leasePhase(t, s, lease.ID) != Acquired {
		t.Fatal("fixture: the lease left acquired")
	}
	later := time.Now().Add(strandedLeaseWindow + time.Second)
	if released, err := s.ReleaseStrandedLeases(ctx, later); err != nil || len(released) != 1 {
		t.Fatalf("the stranded acquired lease was not released: %v %v", released, err)
	}
}

// homelab#1095 (D6): an AcquireJobs decline moves the lease to cleanup-pending
// with no JIT row. Maintain now releases it; it used to be unselectable.
func TestADeclinedLeaseIsReleasedByMaintain(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	if _, err := s.db.Exec(`UPDATE leases SET phase='acquire-intent' WHERE id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DELETE FROM acquisition_evidence WHERE lease_id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
		JobKey: lease.JobKey, Outcome: AcquisitionDeclined, Source: AcquisitionSourceAPI,
	}); err != nil {
		t.Fatal(err)
	}
	if leasePhase(t, s, lease.ID) != CleanupPending {
		t.Fatal("fixture: the decline did not enter cleanup")
	}
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if leasePhase(t, s, lease.ID) != Released {
		t.Fatal("a declined lease with no JIT row was not released")
	}
}

// The fence still holds wherever a JIT row exists: a registration that is not
// proven absent keeps the lease charged.
func TestALeaseWithALiveRegistrationIsStillNotReleased(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	now := timestamp()
	if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated) VALUES(?,?,'jit-observed',77,?,?,?)`,
		lease.ID, []byte("test"), now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE leases SET phase='cleanup-pending' WHERE id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingFinishedCleanup(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("a lease whose registration is not proven absent was selected for release: %v %v", pending, err)
	}
}

// homelab#1095 (D9): a JIT POST with an unknown outcome and an empty lookup is
// refused while fresh -- the runner may not be listed yet -- and settled as
// absent once the window has passed. Nothing is removed either way.
func TestAnUnknownJITCreationSettlesAsAbsentAfterTheWindow(t *testing.T) {
	logs := captureLogs(t)
	s, _, lease, b, f := jitFixture(t)
	f.lostCreate, f.lateCreate = true, true
	if _, err := b.Prepare(ctx, lease.ID); err == nil {
		t.Fatal("fixture: the lost mint was accepted")
	}
	if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	if absent, err := b.CleanupRegistration(ctx, lease.ID, true); absent || err == nil {
		t.Fatalf("a fresh unknown creation was settled on one empty lookup: absent=%t err=%v", absent, err)
	}
	old := time.Now().UTC().Add(-unknownJITSettleWindow - time.Second).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE jit_transfers SET created=? WHERE lease_id=?`, old, lease.ID); err != nil {
		t.Fatal(err)
	}
	absent, err := b.CleanupRegistration(ctx, lease.ID, true)
	if err != nil || !absent || f.removes != 0 {
		t.Fatalf("an old unknown creation was not settled as absent: absent=%t err=%v removes=%d", absent, err, f.removes)
	}
	record, err := s.JIT(ctx, lease.ID)
	if err != nil || record.Removal != "absent" {
		t.Fatalf("the settled absence was not recorded: %+v %v", record, err)
	}
	if !strings.Contains(logs.String(), "An uncertain JIT creation is settled as absent") || !strings.Contains(logs.String(), "lease_id="+lease.ID) {
		t.Fatalf("the settlement was not logged: %s", logs.String())
	}
}

// A runner that does appear, however late, is still reconciled by identity and
// never read as absent.
func TestALateRunnerIsNeverSettledAsAbsent(t *testing.T) {
	s, _, lease, b, f := jitFixture(t)
	f.lostCreate, f.lateCreate = true, true
	if _, err := b.Prepare(ctx, lease.ID); err == nil {
		t.Fatal("fixture: the lost mint was accepted")
	}
	if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-unknownJITSettleWindow - time.Second).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE jit_transfers SET created=? WHERE lease_id=?`, old, lease.ID); err != nil {
		t.Fatal(err)
	}
	f.runner = &scaleset.RunnerReference{ID: 77, Name: lease.RunnerName, RunnerScaleSetID: lease.Set.ScaleSetID}
	absent, err := b.CleanupRegistration(ctx, lease.ID, true)
	if err != nil || !absent || f.removes != 1 {
		t.Fatalf("a late runner was not removed by identity: absent=%t err=%v removes=%d", absent, err, f.removes)
	}
}

// Review of homelab#1104 (N3): the released lease's job is handed back. A
// broker job goes to assigned-stale, so a fresh envelope can revive it; before,
// it stayed 'reserved', which outranks every re-offer, and was never served.
// A job with a real request id is left for GitHub to offer again.
func TestAStrandedBrokerJobIsHandedBackToTheOfferPath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requestID int64
		want      string
	}{
		{"broker assignment", 0, jobStateAssignedStale},
		{"API request", 1, string(Reserved)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			offer(t, s, prepare(t, s, 1)[0], 1)
			lease := strand(t, s, Acquired, 0, strandedLeaseWindow+time.Second)
			if _, err := s.db.Exec(`UPDATE jobs SET runner_request_id=? WHERE job_key=?`, tc.requestID, lease.JobKey); err != nil {
				t.Fatal(err)
			}
			if released, err := s.ReleaseStrandedLeases(ctx, time.Now()); err != nil || len(released) != 1 {
				t.Fatalf("fixture: the stranded lease was not released: %v %v", released, err)
			}
			if got := jobState(t, s, lease.Set, lease.JobKey); got != tc.want {
				t.Fatalf("the released lease's job is %q, want %q", got, tc.want)
			}
		})
	}
}

// Review of homelab#1104 (I1): a crash between beginJIT and BeginWorkerCreate
// leaves a provisioning lease with a JIT row and no worker. After the window it
// is moved into the ordinary registration cleanup -- not released outright --
// and a lease that does have a worker row is never touched.
func TestAProvisioningLeaseThatNeverGotAWorkerEntersRegistrationCleanup(t *testing.T) {
	logs := captureLogs(t)
	s, _, lease, b, f := jitFixture(t)
	if _, err := b.Prepare(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if leasePhase(t, s, lease.ID) != Provisioning {
		t.Fatal("fixture: Prepare did not enter provisioning")
	}
	if retired, err := s.RetireUnstartedProvisioning(ctx, time.Now()); err != nil || len(retired) != 0 {
		t.Fatalf("a lease inside the window was retired: %v %v", retired, err)
	}
	later := time.Now().Add(strandedLeaseWindow + time.Second)
	retired, err := s.RetireUnstartedProvisioning(ctx, later)
	if err != nil || len(retired) != 1 || leasePhase(t, s, lease.ID) != CleanupPending {
		t.Fatalf("the interrupted provisioning lease was not moved into cleanup: %v %v", retired, err)
	}
	if !strings.Contains(logs.String(), "never got a worker into teardown") || !strings.Contains(logs.String(), "lease_id="+lease.ID) {
		t.Fatalf("the retirement was not logged with its lease: %s", logs.String())
	}
	pending, err := s.PendingRegistrationCleanup(ctx)
	if err != nil || len(pending) != 1 || pending[0] != lease.ID {
		t.Fatalf("the lease did not reach registration cleanup: %v %v", pending, err)
	}
	absent, err := b.CleanupRegistration(ctx, lease.ID, true)
	if err != nil || !absent || f.removes != 1 {
		t.Fatalf("its registration was not removed by identity: absent=%t err=%v removes=%d", absent, err, f.removes)
	}
}

func TestAProvisioningLeaseWithAWorkerRowIsLeftToItsDeadlines(t *testing.T) {
	s, _, lease, b, _ := jitFixture(t)
	if _, err := b.Prepare(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	now := timestamp()
	if _, err := s.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated) VALUES(?,?,'create-intent',?,?,?)`,
		lease.ID, lease.RunnerName, now, now, now); err != nil {
		t.Fatal(err)
	}
	if retired, err := s.RetireUnstartedProvisioning(ctx, time.Now().Add(time.Hour)); err != nil || len(retired) != 0 || leasePhase(t, s, lease.ID) != Provisioning {
		t.Fatalf("a lease with a worker row was retired by the no-worker backstop: %v %v", retired, err)
	}
}
