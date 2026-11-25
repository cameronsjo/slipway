package tart

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestInventoryJoinsLedgerListingAndDirectory(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.writeVM(testResourceID, "darwin")
	fix.writeVM("ci-guest", "darwin")
	fix.runner.set("tart list --format json",
		listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: true}), nil)

	inventory, err := fix.backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	if !inventory.Complete {
		t.Fatal("a readable host produced an incomplete inventory")
	}
	if len(inventory.Workers) != 1 || inventory.Workers[0].LeaseID != testLeaseID ||
		inventory.Workers[0].ResourceID != testResourceID {
		t.Fatalf("inventory did not report the owned worker: %+v", inventory.Workers)
	}
	// Two running darwin guests against a physical capacity of two, and this
	// controller owns one, so nothing more fits.
	if inventory.FreeSlots != 0 {
		t.Fatalf("free slots should be zero with an owned VM present, saw %d", inventory.FreeSlots)
	}
}

// Any one source failing makes the whole inventory incomplete. A partial join
// cannot tell "absent" from "not read", and the controller fences on
// incomplete rather than acting on a guess.
func TestInventoryIsIncompleteWhenAnySourceFails(t *testing.T) {
	t.Run("tart listing fails", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.runner.set("tart list --format json", "", errors.New("tart is not running"))
		inventory, err := fix.backend.Inventory(context.Background())
		if err == nil || inventory.Complete {
			t.Fatalf("a failed listing produced a complete inventory: %+v", inventory)
		}
	})
	t.Run("tart listing is not JSON", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.runner.set("tart list --format json", "not json", nil)
		if _, err := fix.backend.Inventory(context.Background()); err == nil {
			t.Fatal("an unparsable listing was accepted")
		}
	})
	t.Run("listing reports no running state", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.runner.set("tart list --format json", `[{"Source":"local","Name":"ci-guest"}]`, nil)
		if _, err := fix.backend.Inventory(context.Background()); err == nil {
			t.Fatal("a listing with no running state was accepted")
		}
	})
	t.Run("ledger entry is malformed", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.writeLedgerEntry(testResourceID, testLeaseID)
		path := filepath.Join(fix.config.LedgerDir, testResourceID+".json")
		if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := fix.backend.Inventory(context.Background()); err == nil {
			t.Fatal("a malformed ledger entry was skipped instead of refused")
		}
	})
	// A blocking read would hang here forever, and with it every Create and
	// Inventory: a FIFO has no writer, so it never reaches end of file.
	t.Run("ledger entry is a FIFO", func(t *testing.T) {
		fix := newFixture(t, nil)
		if err := os.MkdirAll(fix.config.LedgerDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(filepath.Join(fix.config.LedgerDir, testResourceID+".json"), 0o600); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := fix.backend.Inventory(context.Background())
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("a FIFO ledger entry was accepted")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Inventory hung reading a FIFO ledger entry")
		}
	})
	t.Run("a running VM has no readable config", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.runner.set("tart list --format json",
			listing(vm{Name: "ci-guest", Running: true}, vm{Name: "ghost", Running: true}), nil)
		if _, err := fix.backend.Inventory(context.Background()); err == nil {
			t.Fatal("a running VM with no readable OS was counted as absent")
		}
	})
}

// A pool-shaped VM with no ledger entry must reach the controller as a worker
// whose lease it cannot find, which is how ReconcileBackendInventory decides a
// resource is untracked and fences.
func TestForeignPoolShapedVMIsReportedAsUntracked(t *testing.T) {
	foreign := "slipway-macos-" + mustUUID(t)
	t.Run("listed by tart", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.writeVM("ci-guest", "darwin")
		fix.writeVM(foreign, "darwin")
		fix.runner.set("tart list --format json",
			listing(vm{Name: "ci-guest", Running: true}, vm{Name: foreign, Running: true}), nil)
		assertUntracked(t, fix, foreign)
	})
	t.Run("orphaned VM directory only", func(t *testing.T) {
		fix := newFixture(t, nil)
		fix.writeVM("ci-guest", "darwin")
		fix.writeVM(foreign, "darwin")
		fix.runner.set("tart list --format json", listing(vm{Name: "ci-guest", Running: true}), nil)
		assertUntracked(t, fix, foreign)
	})
}

func assertUntracked(t *testing.T, fix *fixture, foreign string) {
	t.Helper()
	inventory, err := fix.backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	if len(inventory.Workers) != 1 {
		t.Fatalf("expected one reported worker, saw %+v", inventory.Workers)
	}
	worker := inventory.Workers[0]
	if worker.ResourceID != foreign {
		t.Fatalf("the foreign VM was not reported: %+v", worker)
	}
	// The identity must be non-empty (an empty one is a different diagnostic)
	// and must not be a lease the controller could ever have minted.
	if worker.LeaseID == "" || worker.LeaseID != foreign {
		t.Fatalf("the foreign VM was reported with a resolvable lease identity: %+v", worker)
	}
}

// Two ledger entries naming the same lease is a duplicate identity. Reporting
// it would give the controller two workers for one lease, which it reads as a
// journal bug rather than a platform one.
func TestInventoryRefusesADuplicateLeaseInTheLedger(t *testing.T) {
	fix := newFixture(t, nil)
	first, second := "slipway-macos-"+mustUUID(t), "slipway-macos-"+mustUUID(t)
	fix.writeLedgerEntry(first, testLeaseID)
	fix.writeLedgerEntry(second, testLeaseID)
	fix.writeVM("ci-guest", "darwin")
	if _, err := fix.backend.Inventory(context.Background()); err == nil {
		t.Fatal("a duplicated lease in the ledger was accepted")
	}
}

// FreeSlots is the physical number: the host ceiling minus every RUNNING
// DARWIN guest. tart list carries no OS field, so the OS comes from each VM's
// own config.json and a running Linux VM does not charge a macOS slot.
func TestFreeSlotsCountsOnlyRunningDarwinGuests(t *testing.T) {
	cases := []struct {
		name     string
		listing  string
		vms      map[string]string
		expected int
	}{
		{"legacy guest only", listing(vm{Name: "ci-guest", Running: true}),
			map[string]string{"ci-guest": "darwin"}, 1},
		{"legacy guest plus a running linux VM", listing(vm{Name: "ci-guest", Running: true}, vm{Name: "builder", Running: true}),
			map[string]string{"ci-guest": "darwin", "builder": "linux"}, 1},
		{"a second darwin VM", listing(vm{Name: "ci-guest", Running: true}, vm{Name: "other", Running: true}),
			map[string]string{"ci-guest": "darwin", "other": "darwin"}, 0},
		{"a stopped darwin VM charges nothing", listing(vm{Name: "ci-guest", Running: true}, vm{Name: "other", Running: false}),
			map[string]string{"ci-guest": "darwin", "other": "darwin"}, 1},
		{"nothing running", listing(), nil, 2},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, nil)
			for name, guestOS := range testCase.vms {
				fix.writeVM(name, guestOS)
			}
			fix.runner.set("tart list --format json", testCase.listing, nil)
			inventory, err := fix.backend.Inventory(context.Background())
			if err != nil {
				t.Fatalf("inventory failed: %v", err)
			}
			if inventory.FreeSlots != testCase.expected {
				t.Fatalf("expected %d free slots, saw %d", testCase.expected, inventory.FreeSlots)
			}
		})
	}
}

// FreeSlots must never advertise a VM that Create's owned-VM cap would refuse.
// With nothing owned it stays the physical number, exactly as it was when the
// cap was hard-coded; once VMs are owned it is clamped to the cap's headroom.
// The owned VMs here are ledgered but not running, so they charge the cap and
// no physical slot: the clamp alone is what holds the number down.
func TestFreeSlotsRespectTheOwnedVMCap(t *testing.T) {
	cases := []struct {
		name     string
		maxOwned int
		owned    int
		expected int
	}{
		{"cap of one, nothing owned keeps the physical number", 1, 0, 2},
		{"cap of one, one owned", 1, 1, 0},
		{"cap of two, nothing owned", 2, 0, 2},
		{"cap of two, one owned", 2, 1, 1},
		{"cap of two, two owned", 2, 2, 0},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = testCase.maxOwned })
			entries := []vm{}
			for range testCase.owned {
				name := "slipway-macos-" + mustUUID(t)
				fix.writeLedgerEntry(name, mustUUID(t))
				fix.writeVM(name, "darwin")
				entries = append(entries, vm{Name: name, Running: false})
			}
			fix.runner.set("tart list --format json", listing(entries...), nil)
			inventory, err := fix.backend.Inventory(context.Background())
			if err != nil {
				t.Fatalf("inventory failed: %v", err)
			}
			if inventory.FreeSlots != testCase.expected {
				t.Fatalf("expected %d free slots, saw %d", testCase.expected, inventory.FreeSlots)
			}
		})
	}
}

// Two running pool VMs under a cap of two fill both of Apple's slots. This is
// the steady state ADR-0018 enables once ci-guest is parked.
func TestFreeSlotsAreZeroWithTwoRunningOwnedVMs(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = 2 })
	first, second := "slipway-macos-"+mustUUID(t), "slipway-macos-"+mustUUID(t)
	for _, name := range []string{first, second} {
		fix.writeLedgerEntry(name, mustUUID(t))
		fix.writeVM(name, "darwin")
	}
	fix.runner.set("tart list --format json",
		listing(vm{Name: first, Running: true}, vm{Name: second, Running: true}), nil)
	inventory, err := fix.backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	if inventory.FreeSlots != 0 || len(inventory.Workers) != 2 {
		t.Fatalf("expected two workers and no free slot, saw %d free and %+v", inventory.FreeSlots, inventory.Workers)
	}
}

// The memory gate forces free slots to zero before the pool can page out the
// inference server, so the controller reads "no capacity" rather than being
// refused later at Create.
func TestFreeSlotsAreZeroWhenTheMemoryGateFails(t *testing.T) {
	for name, output := range map[string]string{"under the floor": starvedVMStat, "unreadable": ""} {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.runner.set("tart list --format json", listing(), nil)
			fix.runner.set("vm_stat", output, nil)
			inventory, err := fix.backend.Inventory(context.Background())
			if err != nil {
				t.Fatalf("inventory failed: %v", err)
			}
			if inventory.FreeSlots != 0 {
				t.Fatalf("the memory gate did not zero free slots, saw %d", inventory.FreeSlots)
			}
			if !inventory.Complete {
				t.Fatal("a memory refusal should report no capacity, not an unknown inventory")
			}
		})
	}
}

func TestVMStatParsingRejectsOutputItCannotRead(t *testing.T) {
	cases := map[string]string{
		"no page size":    "Pages free: 10.\nPages inactive: 10.\n",
		"no free pages":   "(page size of 16384 bytes)\nPages inactive: 10.\n",
		"no inactive":     "(page size of 16384 bytes)\nPages free: 10.\n",
		"non-numeric":     "(page size of 16384 bytes)\nPages free: many.\nPages inactive: 10.\n",
		"zero page size":  "(page size of 0 bytes)\nPages free: 10.\nPages inactive: 10.\n",
		"totally garbage": "the boundary is fine, trust me",
	}
	for name, output := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.runner.set("vm_stat", output, nil)
			if _, err := fix.backend.memoryAvailable(context.Background()); err == nil {
				t.Fatal("unreadable memory statistics were treated as a pass")
			}
		})
	}
}
