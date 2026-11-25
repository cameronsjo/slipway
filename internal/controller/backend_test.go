package controller

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/actions/scaleset"
)

// Headroom has to be the binding constraint, not the platform ceiling. With
// FreeSlots equal to the ceiling the inventory term never decides anything and
// the test passes with the clamp deleted, so FreeSlots is one below it here:
// the first reservation is the positive control, the second is the refusal.
func TestBackendInventoryHeadroomConstrainsReservations(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 3)
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveNext(ctx); err != nil {
		t.Fatalf("the one observed free slot was refused: %v", err)
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("current backend headroom was not enforced: %v", err)
	}
}

// Two assigned jobs are inside the configured capacity of two and outside the one
// slot the backend actually has free, so only the headroom term can hold the
// plan down. With FreeSlots at the ceiling both would be advertised and nothing
// would be deferred.
func TestBackendInventoryHeadroomConstrainsAdvertisements(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	activateTestSessions(t, s, []SetKey{key})
	statistics, err := json.Marshal(scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=? WHERE repo_id=?`, string(statistics), key.RepositoryID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 1}); err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanAdvertisements(ctx, []SetKey{key})
	if err != nil {
		t.Fatalf("assigned work beyond current backend headroom failed the sweep: %v", err)
	}
	if plan.Capacity[key] != 1 || plan.Deferred != 1 {
		t.Fatalf("assigned work beyond current backend headroom was advertised: %+v", plan)
	}
}

func TestBackendInventoryRequiresOwnedStableIdentities(t *testing.T) {
	s, _ := openTestStore(t, 2)
	lease, token := readyWorkerLease(t, s)
	if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
		t.Fatal(err)
	}
	valid := BackendInventory{Complete: true, FreeSlots: 1,
		Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}
	if err := s.ReconcileBackendInventory(ctx, valid); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Workers = []WorkerObservation{{LeaseID: lease.ID, ResourceID: "renamed-worker"}}
	if err := s.ReconcileBackendInventory(ctx, invalid); err == nil {
		t.Fatal("renamed backend resource was trusted")
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("failed inventory did not close admission: %v", err)
	}
}

func TestBackendInventoryRejectsResourceBeforeProvisioningIntent(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, AcquisitionIntent, Acquired); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, Acquired, Provisioning); err != nil {
		t.Fatal(err)
	}
	inventory := BackendInventory{Complete: true,
		Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}
	if err := s.ReconcileBackendInventory(ctx, inventory); err == nil {
		t.Fatal("physical resource without provisioning intent was adopted")
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("premature resource did not close admission: %v", err)
	}
}

func TestBackendInventoryRejectsUnknownDuplicateAndIncompleteListings(t *testing.T) {
	for _, scenario := range []string{"incomplete", "unknown", "duplicate lease", "duplicate resource"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			lease, token := readyWorkerLease(t, s)
			if _, err := s.BeginWorkerCreate(ctx, lease.ID, "https://controller.example", token); err != nil {
				t.Fatal(err)
			}
			inventory := BackendInventory{Complete: true, Workers: []WorkerObservation{{LeaseID: lease.ID, ResourceID: lease.RunnerName}}}
			switch scenario {
			case "incomplete":
				inventory.Complete = false
			case "unknown":
				inventory.Workers[0].LeaseID = "unknown"
			case "duplicate lease":
				inventory.Workers = append(inventory.Workers, WorkerObservation{LeaseID: lease.ID, ResourceID: "other"})
			case "duplicate resource":
				inventory.Workers = append(inventory.Workers, WorkerObservation{LeaseID: "unknown", ResourceID: lease.RunnerName})
			}
			if err := s.ReconcileBackendInventory(ctx, inventory); err == nil {
				t.Fatal("invalid backend listing was trusted")
			}
			if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
				t.Fatalf("invalid listing did not retain fence: %v", err)
			}
		})
	}
}
