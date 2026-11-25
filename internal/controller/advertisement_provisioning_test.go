package controller

import "testing"

// TestAdvertisementKeepsAProvisioningLeaseSlot is the 2026-09-22 outage probe.
// It reproduces the live shape: two repositories share a one-slot pool, one
// repository holds a lease that is reserved and then provisioning -- the JIT
// runner has been minted but has not connected -- and the other is idle with no
// assigned jobs. The planner must keep the provisioning repository's advertised
// capacity at its live lease count for every phase before 'released', and the
// exploratory slot handed to the idle neighbour must never take it away.
//
// Advertising 0 to the repository whose JIT runner is still booting is what the
// outage hypothesis predicted; this test is what decides whether the planner
// does that.
func TestAdvertisementKeepsAProvisioningLeaseSlot(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		t.Run(map[int]string{1: "one-slot", 2: "spare-headroom"}[capacity], func(t *testing.T) {
			s, _ := openTestStore(t, capacity)
			keys := prepare(t, s, 2)
			activateTestSessions(t, s, keys)
			offer(t, s, keys[0], 1)
			lease := mustReserve(t, s)
			if lease.Set != keys[0] {
				t.Fatalf("fixture reserved the wrong repository: %+v", lease.Set)
			}
			// Walk the lease to the phase the failing workers sat in: acquired,
			// then provisioning, with no runner connected yet.
			if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
				t.Fatal(err)
			}
			if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
				JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI}); err != nil {
				t.Fatal(err)
			}
			if err := s.Transition(ctx, lease.ID, Acquired, Provisioning); err != nil {
				t.Fatal(err)
			}
			current, err := s.Lease(ctx, lease.ID)
			if err != nil || current.Phase != Provisioning {
				t.Fatalf("lease did not reach provisioning: %+v %v", current, err)
			}
			// Two sweeps: the first moves the durable exploratory cursor, and the
			// second is the one that could rotate the slot away from the busy
			// repository.
			for sweep := range 2 {
				plan, err := s.PlanAdvertisements(ctx, keys)
				if err != nil {
					t.Fatalf("sweep %d: %v", sweep, err)
				}
				if plan.Capacity[keys[0]] < 1 {
					t.Fatalf("sweep %d advertised %d to the repository holding a provisioning lease; its JIT runner is told its scale set wants no runners: %+v",
						sweep, plan.Capacity[keys[0]], plan.Capacity)
				}
			}
		})
	}
}
