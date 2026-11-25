package tart

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDestroyProvesBothAbsencesAndOnlyThenClearsTheLedger(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set("tart list --format json", listing(vm{Name: "ci-guest", Running: true}), nil)

	absence, err := fix.backend.Destroy(context.Background(), workerObservation(testLeaseID, testResourceID))
	if err != nil {
		t.Fatalf("destroy failed: %v", err)
	}
	if !absence.ProcessAbsent || !absence.StorageAbsent {
		t.Fatalf("a fully deleted VM did not prove both absences: %+v", absence)
	}
	if fix.ledgerHas(testResourceID) {
		t.Fatal("the ledger entry survived a proven destroy")
	}
	if !fix.runner.ran("tart stop") || !fix.runner.ran("tart delete") {
		t.Fatal("destroy skipped stop or delete")
	}
}

// Each absence is proven independently. A failed delete that leaves the VM
// directory behind keeps StorageAbsent false and keeps the slot charged, which
// is what stops an orphaned clone from silently freeing capacity.
func TestDestroyReportsEachAbsenceIndependently(t *testing.T) {
	cases := []struct {
		name          string
		listing       string
		leaveDisk     bool
		deleteErr     error
		processAbsent bool
		storageAbsent bool
	}{
		{"delete failed, VM directory remains", listing(vm{Name: "ci-guest", Running: true}), true,
			errors.New("tart delete failed"), true, false},
		{"still listed by tart", listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: false}),
			false, nil, false, true},
		{"listed and on disk", listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: false}),
			true, errors.New("tart delete failed"), false, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.writeLedgerEntry(testResourceID, testLeaseID)
			if testCase.leaveDisk {
				fix.writeVM(testResourceID, "darwin")
			}
			fix.runner.set("tart list --format json", testCase.listing, nil)
			if testCase.deleteErr != nil {
				fix.runner.set("tart delete", "", testCase.deleteErr)
			}
			absence, err := fix.backend.Destroy(context.Background(), workerObservation(testLeaseID, testResourceID))
			if err != nil {
				t.Fatalf("destroy returned an error instead of a partial absence: %v", err)
			}
			if absence.ProcessAbsent != testCase.processAbsent || absence.StorageAbsent != testCase.storageAbsent {
				t.Fatalf("absence reported %+v, expected process=%v storage=%v",
					absence, testCase.processAbsent, testCase.storageAbsent)
			}
			if !fix.ledgerHas(testResourceID) {
				t.Fatal("an unproven destroy cleared the ledger and released the slot")
			}
		})
	}
}

func TestDestroyRefusesANonCanonicalIdentity(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Destroy(context.Background(), workerObservation(testLeaseID, "ci-guest")); err == nil {
		t.Fatal("destroy accepted a VM name outside the pool's own namespace")
	}
	if fix.runner.ran("tart stop") || fix.runner.ran("tart delete") {
		t.Fatal("destroy reached tart with a name it should have refused")
	}
}

// The re-assert loop stops ONLY VMs named in this controller's ledger. The
// legacy guest is never touched: losing it to a pool fault would take the whole
// legacy fleet down.
func TestReassertStopsOnlyItsOwnVMs(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)

	fix.backend.reassertOnce(context.Background())

	var stopped []string
	for _, call := range fix.runner.recorded() {
		if len(call.Argv) >= 3 && call.Argv[1] == "stop" {
			stopped = append(stopped, call.Argv[2])
		}
	}
	if len(stopped) != 1 || stopped[0] != testResourceID {
		t.Fatalf("re-assert stopped the wrong set of VMs: %v", stopped)
	}
}

// Positive control: a healthy boundary stops nothing. Without it, a loop that
// always stopped everything would pass the test above.
func TestReassertStopsNothingWhileTheBoundaryHolds(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.backend.reassertOnce(context.Background())
	if fix.runner.ran("tart stop") {
		t.Fatal("re-assert stopped a VM while the boundary was intact")
	}
}

// A ledger it cannot read is not a licence to stop anything. Stopping on an
// unreadable ledger would let one bad file take out the legacy guest.
func TestReassertStopsNothingWhenItCannotReadItsOwnLedger(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.LedgerDir = "/dev/null/ledger" })
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)
	fix.backend.reassertOnce(context.Background())
	if fix.runner.ran("tart stop") {
		t.Fatal("re-assert stopped a VM without knowing which ones it owns")
	}
}

// Resumability: the loop derives owned VMs from the ledger, not from the child
// handles this process holds, so a controller restart that left a VM running
// still re-asserts for it.
func TestReassertWorksForAVMThisProcessNeverStarted(t *testing.T) {
	fix := newFixture(t, nil)
	fix.backend.mu.Lock()
	children := len(fix.backend.children)
	fix.backend.mu.Unlock()
	if children != 0 {
		t.Fatal("the fixture already holds a child handle; this test would prove nothing")
	}
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)
	fix.backend.reassertOnce(context.Background())
	if !fix.runner.ran("tart stop " + testResourceID) {
		t.Fatal("re-assert did not stop a VM it had no in-memory handle for")
	}
}

// The loop itself runs on its interval; reassertOnce carries the decision.
func TestReassertLoopFiresOnItsInterval(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.ReassertInterval = 5 * time.Millisecond })
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fix.runner.ran("tart stop " + testResourceID) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the re-assert loop never fired within two seconds")
}

func TestStopTimeoutIsBoundedByConfiguration(t *testing.T) {
	fix := newFixture(t, nil)
	if fix.config.StopTimeout <= 0 {
		t.Fatal("stop timeout is unbounded")
	}
	if _, err := New(context.Background(), newFakeRunner(), func() Config {
		c := fix.config
		c.StopTimeout = 0
		return c
	}(), nil); err == nil || !strings.Contains(err.Error(), "positive durations") {
		t.Fatalf("an unbounded stop timeout was accepted: %v", err)
	}
}

// A boundary failure usually persists. Re-issuing `tart stop` every 60 seconds
// buries the one line that matters under a repeating one, and keeps asking tart
// to stop a VM it has already stopped.
func TestReassertStopsAnOwnedVMOnceNotEveryTick(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)

	for range 5 {
		fix.backend.reassertOnce(context.Background())
	}

	stops := 0
	for _, call := range fix.runner.recorded() {
		if len(call.Argv) >= 3 && call.Argv[1] == "stop" {
			stops++
		}
	}
	if stops != 1 {
		t.Fatalf("five failing ticks issued %d stops, expected exactly one", stops)
	}
}

// I-B. The flag must record a stop that WORKED. Setting it before knowing the
// outcome is how a wedged VM ends up running unfenced forever: the first
// `tart stop` fails, the flag says "handled", and no later tick tries again.
func TestReassertRetriesAStopThatFailed(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)
	fix.runner.set("tart stop", "", errBrokenStage)
	// Still RUNNING after the failed stop: the one case that is a real failure.
	fix.runner.set("tart list --format json",
		listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: true}), nil)

	fix.backend.reassertOnce(context.Background())
	fix.backend.reassertOnce(context.Background())

	stops := 0
	for _, call := range fix.runner.recorded() {
		if len(call.Argv) >= 3 && call.Argv[1] == "stop" {
			stops++
		}
	}
	if stops != 2 {
		t.Fatalf("a failing stop was issued %d times across two ticks, expected one per tick", stops)
	}
	fix.backend.mu.Lock()
	remembered := fix.backend.stoppedByGate[testResourceID]
	fix.backend.mu.Unlock()
	if remembered {
		t.Fatal("a failed stop was recorded as handled")
	}
}

// A partially failed Destroy leaves the VM running, so it must not leave the
// flag set either -- no later tick would try to stop it.
func TestAPartiallyFailedDestroyClearsTheStoppedOnceMemory(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)
	fix.backend.reassertOnce(context.Background())
	fix.backend.mu.Lock()
	set := fix.backend.stoppedByGate[testResourceID]
	fix.backend.mu.Unlock()
	if !set {
		t.Fatal("the successful stop was not recorded, so this test proves nothing")
	}
	// Destroy fails to remove it: still listed, still on disk.
	fix.writeVM(testResourceID, "darwin")
	fix.runner.set("tart list --format json",
		listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: false}), nil)
	fix.runner.set("tart delete", "", errBrokenStage)
	if _, err := fix.backend.Destroy(context.Background(), workerObservation(testLeaseID, testResourceID)); err != nil {
		t.Fatalf("destroy returned an error instead of a partial absence: %v", err)
	}
	fix.backend.mu.Lock()
	remembered := fix.backend.stoppedByGate[testResourceID]
	fix.backend.mu.Unlock()
	if remembered {
		t.Fatal("a partially failed Destroy left the VM marked as already stopped")
	}
}

// And Destroy clears that memory, so a later VM with the same name is stopped
// again if its own boundary fails.
func TestDestroyClearsTheStoppedOnceMemory(t *testing.T) {
	fix := newFixture(t, nil)
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.set(pfInfoKey(fix.config), "Status: Disabled\n", nil)
	fix.backend.reassertOnce(context.Background())
	fix.runner.set("tart list --format json", listing(vm{Name: "ci-guest", Running: true}), nil)
	if _, err := fix.backend.Destroy(context.Background(), workerObservation(testLeaseID, testResourceID)); err != nil {
		t.Fatalf("destroy failed: %v", err)
	}
	fix.backend.mu.Lock()
	remembered := fix.backend.stoppedByGate[testResourceID]
	fix.backend.mu.Unlock()
	if remembered {
		t.Fatal("Destroy left the stopped-once memory set, so a later VM of the same name would never be stopped")
	}
}

// The first live run logged `tart could not stop its own VM` while `tart list`
// showed it stopped: killing the owned `tart run` child already stops the VM,
// so the `tart stop` that follows fails with "not running". A failed stop is
// not evidence the VM is running -- read the state back.
func TestAFailedStopIsAcceptedWhenTheVMIsVerifiablyNotRunning(t *testing.T) {
	cases := map[string]struct {
		listing string
		listErr error
		stopped bool
	}{
		"listed as stopped": {
			listing: listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: false}),
			stopped: true,
		},
		"absent from the listing": {
			listing: listing(vm{Name: "ci-guest", Running: true}),
			stopped: true,
		},
		"still listed as running": {
			listing: listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: true}),
			stopped: false,
		},
		"listing is unreadable": {
			listing: "", listErr: errBrokenStage, stopped: false,
		},
		"listing reports no running state": {
			listing: `[{"Source":"local","Name":"` + testResourceID + `"}]`, stopped: false,
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.writeVM(testResourceID, "darwin")
			fix.runner.set("tart stop", "", errBrokenStage)
			fix.runner.set("tart list --format json", testCase.listing, testCase.listErr)

			err := fix.backend.stopOwned(context.Background(), testResourceID)
			if testCase.stopped && err != nil {
				t.Fatalf("a VM that is not running was reported unstoppable: %v", err)
			}
			if !testCase.stopped && err == nil {
				t.Fatal("a VM whose state is running or unknown was reported stopped")
			}
		})
	}
}

// Positive control: a stop that succeeds needs no read-back.
func TestASuccessfulStopNeedsNoListingReadBack(t *testing.T) {
	fix := newFixture(t, nil)
	if err := fix.backend.stopOwned(context.Background(), testResourceID); err != nil {
		t.Fatalf("a successful stop was reported as a failure: %v", err)
	}
}
