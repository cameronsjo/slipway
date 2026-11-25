package tart

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// I3. With an empty environment, tart reads its DEFAULT home. Nothing then tied
// the VMs this adapter clones, lists and deletes to the tart_home the inventory
// reads config.json out of -- two views of "which VMs exist" that can disagree,
// which is the whole class of bug the three-source join exists to catch.
func TestEveryTartInvocationCarriesTartHome(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if _, err := fix.backend.Inventory(context.Background()); err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	if _, err := fix.backend.Destroy(context.Background(), workerObservation(testLeaseID, testResourceID)); err != nil {
		t.Fatalf("destroy failed: %v", err)
	}

	tartCalls := 0
	for _, call := range fix.runner.recorded() {
		if call.Argv[0] != fix.config.TartBinary {
			continue
		}
		tartCalls++
		if !slices.Contains(call.Env, "TART_HOME="+fix.config.TartHome) {
			t.Fatalf("a tart invocation did not pin TART_HOME: %v env=%v", call.Argv, call.Env)
		}
		if !slices.Contains(call.Env, "TART_NO_AUTO_PRUNE=1") {
			t.Fatalf("a tart invocation did not disable auto-prune: %v", call.Argv)
		}
		if !slices.Contains(call.Env, "HOME="+filepath.Dir(fix.config.TartHome)) {
			t.Fatalf("a tart invocation did not pin HOME: %v env=%v", call.Argv, call.Env)
		}
		if !hasPathEntry(call.Env, filepath.Dir(fix.config.TartBinary)) {
			t.Fatalf("a tart invocation did not put the tart binary's directory on PATH: %v", call.Env)
		}
	}
	if tartCalls < 6 {
		t.Fatalf("only %d tart invocations were recorded; this assertion needs the full sequence", tartCalls)
	}
}

// The non-tart host commands get a PATH and nothing else. A tart variable
// leaking into a sudo call would be noise at best and a surprise at worst.
func TestHostCommandsCarryOnlyAPath(t *testing.T) {
	fix := newFixture(t, nil)
	if err := fix.backend.assertBoundary(context.Background()); err != nil {
		t.Fatalf("assert failed: %v", err)
	}
	if _, err := fix.backend.memoryAvailable(context.Background()); err != nil {
		t.Fatalf("memory read failed: %v", err)
	}
	checked := 0
	for _, call := range fix.runner.recorded() {
		if call.Argv[0] != fix.config.SudoBinary && call.Argv[0] != fix.config.VMStatBinary {
			continue
		}
		checked++
		if len(call.Env) != 1 || !strings.HasPrefix(call.Env[0], "PATH=") {
			t.Fatalf("a host command carried an unexpected environment: %v env=%v", call.Argv, call.Env)
		}
	}
	if checked < 4 {
		t.Fatalf("only %d host commands were recorded; expected the three pf reads and vm_stat", checked)
	}
}

// No environment anywhere is inherited from the controller process. An
// inherited variable is one the operator did not choose and cannot see.
func TestNoInvocationInheritsTheControllerEnvironment(t *testing.T) {
	t.Setenv("SJO_TEST_CANARY", "inherited")
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if os.Getenv("SJO_TEST_CANARY") != "inherited" {
		t.Fatal("the canary is not set, so this test proves nothing")
	}
	for _, call := range fix.runner.recorded() {
		for _, variable := range call.Env {
			if strings.HasPrefix(variable, "SJO_TEST_CANARY=") {
				t.Fatalf("a child inherited the controller environment: %v", call.Argv)
			}
		}
	}
}

// A symlinked VM directory must be SEEN. os.ReadDir reports a symlink as a
// symlink, so filtering on IsDir alone made a VM planted behind a link
// invisible to the whole three-source join: present on disk, absent from
// inventory, and therefore unfenced.
func TestASymlinkedVMDirectoryIsReportedAsUntracked(t *testing.T) {
	fix := newFixture(t, nil)
	planted := "slipway-macos-" + mustUUID(t)
	real := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(fix.config.TartHome, "vms", planted)); err != nil {
		t.Fatal(err)
	}
	inventory, err := fix.backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	found := false
	for _, worker := range inventory.Workers {
		if worker.ResourceID == planted {
			found = true
			if worker.LeaseID != planted {
				t.Fatalf("a symlinked VM was reported with a resolvable lease: %+v", worker)
			}
		}
	}
	if !found {
		t.Fatalf("a symlinked VM directory was invisible to inventory: %+v", inventory.Workers)
	}
}

// Even with a ledger entry: the bytes behind a symlink are chosen by whoever
// made the link, not by the adapter.
func TestASymlinkedVMIsUntrackedEvenWithALedgerEntry(t *testing.T) {
	fix := newFixture(t, nil)
	real := filepath.Join(t.TempDir(), "elsewhere")
	if err := os.MkdirAll(real, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(fix.config.TartHome, "vms", testResourceID)); err != nil {
		t.Fatal(err)
	}
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	inventory, err := fix.backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	for _, worker := range inventory.Workers {
		if worker.ResourceID == testResourceID && worker.LeaseID == testLeaseID {
			t.Fatal("a symlinked VM was trusted because the ledger named it")
		}
	}
}

// The resource name must belong to the lease, not merely be well-formed.
// allocator.go mints "slipway-" + platform + "-" + lease UUID, so the two halves
// are one fact and checking them separately admits a mismatched pair.
func TestIdentityRequiresTheResourceNameToBelongToTheLease(t *testing.T) {
	fix := newFixture(t, nil)
	other := "slipway-macos-" + mustUUID(t)
	if err := fix.backend.validateIdentity(testLeaseID, other); err == nil {
		t.Fatal("a well-formed resource name belonging to another lease was accepted")
	}
	if err := fix.backend.validateIdentity(testLeaseID, testResourceID); err != nil {
		t.Fatalf("the controller's own pairing was refused: %v", err)
	}
}

func hasPathEntry(env []string, directory string) bool {
	for _, variable := range env {
		if value, found := strings.CutPrefix(variable, "PATH="); found {
			return slices.Contains(strings.Split(value, ":"), directory)
		}
	}
	return false
}
