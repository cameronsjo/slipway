package tart

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

func workerObservation(leaseID, resourceID string) controller.WorkerObservation {
	return controller.WorkerObservation{LeaseID: leaseID, ResourceID: resourceID}
}

func TestCreateHappyPathDeliversTheJITOnceAndReportsTheWorker(t *testing.T) {
	fix := newFixture(t, nil)
	observed, err := fix.backend.Create(context.Background(), fix.intent())
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if observed.LeaseID != testLeaseID || observed.ResourceID != testResourceID {
		t.Fatalf("create returned the wrong identity: %+v", observed)
	}
	if fix.jitCalls != 1 {
		t.Fatalf("the bootstrap capability was spent %d times, expected exactly one", fix.jitCalls)
	}
	if !fix.ledgerHas(testResourceID) {
		t.Fatal("a successful create left no ledger entry")
	}
	for _, stage := range []string{"tart clone", "tart set", "tart run", "tart ip"} {
		if !fix.runner.ran(stage) {
			t.Fatalf("create skipped %q", stage)
		}
	}
}

// tart clone copies the base image's MAC and ECID verbatim. A clone started
// without new ones puts a second VM carrying ci-guest's MAC on the same bridge,
// which happened once during the plan's own probe. Both flags, and before run.
func TestCreateRandomizesMACAndSerialBeforeStartingTheVM(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	setIndex, runIndex := fix.runner.indexOf("tart set"), fix.runner.indexOf("tart run")
	if setIndex < 0 || runIndex < 0 || setIndex > runIndex {
		t.Fatalf("tart set did not run before tart run (set=%d run=%d)", setIndex, runIndex)
	}
	argv := fix.runner.recorded()[setIndex].Argv
	for _, flag := range []string{"--random-mac", "--random-serial"} {
		if !slices.Contains(argv, flag) {
			t.Fatalf("tart set did not request %s: %v", flag, argv)
		}
	}
	if !slices.Contains(argv, "--cpu") || !slices.Contains(argv, "4") ||
		!slices.Contains(argv, "--memory") || !slices.Contains(argv, "8192") {
		t.Fatalf("tart set did not pin the configured cpu and memory: %v", argv)
	}
}

// The owned child carries TART_NO_AUTO_PRUNE so a pool clone can never evict
// the base image the 04:30 ci-guest reprovision clones from.
func TestCreateStartsTheVMWithAutoPruneDisabled(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	index := fix.runner.indexOf("tart run")
	call := fix.runner.recorded()[index]
	if !slices.Contains(call.Env, "TART_NO_AUTO_PRUNE=1") {
		t.Fatalf("tart run child did not disable auto-prune: %v", call.Env)
	}
	if !slices.Contains(call.Argv, "--net-softnet") || !slices.Contains(call.Argv, "--no-graphics") {
		t.Fatalf("tart run did not use the measured isolation flags: %v", call.Argv)
	}
}

// A bridge-level address check passes on the legacy guest's address whenever
// ci-guest is already up. This reads the new VM's OWN address.
func TestCreateRefusesAnAddressThatIsNotThisVMsOwnGuestAddress(t *testing.T) {
	cases := []struct {
		name    string
		output  string
		err     error
		expects string
	}{
		{"outside the subnet", "192.168.1.50\n", nil, "inside 192.168.2.0/24"},
		{"the gateway itself", "192.168.2.1\n", nil, "other than the gateway"},
		{"empty", "\n", nil, "no readable address"},
		{"command failed", "", errors.New("tart ip failed"), "no readable address"},
		{"not IPv4", "fe80::1\n", nil, "an IPv4 address"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.runner.set("tart ip", testCase.output, testCase.err)
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatal("create succeeded with an unacceptable guest address")
			}
			if !strings.Contains(err.Error(), testCase.expects) {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			// The VM exists, so it stays charged and reported.
			assertStillChargedAndReported(t, fix)
			if fix.jitCalls != 0 {
				t.Fatal("a JIT was fetched for a VM whose address was refused")
			}
		})
	}
}

// Every failure after the clone must leave the VM reported by Inventory and
// return an error. Never a success, never a silently released slot.
func TestCreateFailuresAfterCloneLeaveTheSlotChargedAndTheVMReported(t *testing.T) {
	cases := []struct {
		name     string
		break_   func(*fixture)
		stage    string
		deadline time.Duration
	}{
		{"agent never becomes ready", func(f *fixture) {
			f.runner.set("tart exec "+testResourceID+" /usr/bin/true", "", errors.New("guest agent silent"))
		}, "agent-ready", 20 * time.Millisecond},
		{"runner tarball push fails", func(f *fixture) {
			f.runner.set("tart exec -i "+testResourceID+" /usr/bin/tar", "", errors.New("tar failed"))
		}, "runner-tarball", time.Minute},
		{"guest launcher push fails", func(f *fixture) {
			f.runner.set("tart exec -i "+testResourceID+" /bin/dd", "", errors.New("dd failed"))
		}, "guest-launcher", time.Minute},
		{"JIT stdin write fails", func(f *fixture) {
			f.runner.streamErr = errors.New("tart exec -i could not be started")
		}, "jit-delivery", time.Minute},
		{"the launcher never confirms the launch", func(f *fixture) {
			// The real shape: `tart exec -i` does not return until the job
			// ends, so a launcher that dies silently never returns either.
			f.runner.streamBlocks = true
		}, "jit-delivery", time.Minute},
		{"the launcher prints the wrong line", func(f *fixture) {
			f.runner.streamLine = "READY"
		}, "jit-delivery", time.Minute},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, nil)
			testCase.break_(fix)
			intent := fix.intent()
			intent.StartupDeadline = time.Now().Add(testCase.deadline)
			observed, err := fix.backend.Create(context.Background(), intent)
			if err == nil {
				t.Fatal("create returned success on a broken path")
			}
			if observed != (controller.WorkerObservation{}) {
				t.Fatalf("a failed create returned a worker observation: %+v", observed)
			}
			if !strings.Contains(err.Error(), testCase.stage) {
				t.Fatalf("failed at the wrong stage: %v", err)
			}
			assertStillChargedAndReported(t, fix)
		})
	}
}

// A JIT fetch refusal is its own case: the capability is burned and the VM is
// still charged.
func TestCreateWithARefusedJITFetchKeepsTheVMCharged(t *testing.T) {
	fix := newFixture(t, nil)
	intent := fix.intent()
	// A capability the listener has never heard of. The listener refuses it the
	// same way it refuses an expired or replayed one.
	intent.BootstrapToken = mustDifferentToken(t, fix.token)
	_, err := fix.backend.Create(context.Background(), intent)
	if err == nil {
		t.Fatal("create succeeded with a capability the listener refuses")
	}
	if !strings.Contains(err.Error(), "jit-fetch") {
		t.Fatalf("failed at the wrong stage: %v", err)
	}
	assertStillChargedAndReported(t, fix)
}

// The owned-VM cap counts every OTHER ledger entry, because Create writes its
// own entry before it reads the ledger. A refusal never clones and never
// leaves this VM's entry behind.
func TestCreateRefusesPastTheOwnedVMCap(t *testing.T) {
	cases := []struct {
		name        string
		maxOwned    int
		othersOwned int
	}{
		{"default cap of one, one other owned", 1, 1},
		{"cap of two, two others owned", 2, 2},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = testCase.maxOwned })
			for range testCase.othersOwned {
				fix.writeLedgerEntry("slipway-macos-"+mustUUID(t), mustUUID(t))
			}
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatalf("an owned VM past a cap of %d was created", testCase.maxOwned)
			}
			if !strings.Contains(err.Error(), "owned-vm-cap") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			// A full cap is a wait, not a fault: without ErrCapacity the
			// controller writes a provisioning failure row per refusal and
			// three of them open the breaker (homelab#1255 review).
			if !errors.Is(err, controller.ErrCapacity) {
				t.Fatalf("a cap refusal does not carry controller.ErrCapacity: %v", err)
			}
			assertCapacityDeferredLogged(t, fix.logs.String(), "owned-vm-cap", "WARN")
			if fix.runner.ran("tart clone") {
				t.Fatal("the cap refused after cloning")
			}
			if fix.ledgerHas(testResourceID) {
				t.Fatal("a cap refusal left this VM's ledger entry charging a slot")
			}
		})
	}
}

// The cap is race-free only because Create writes its own ledger entry BEFORE
// it reads the ledger. A sibling Create that lands between the two must be
// seen. The sibling's entry is written from inside the memory gate's vm_stat
// call, which sits between the write and the read; moving the read ahead of
// the write would miss it and admit a second VM under a cap of one.
func TestCreateSeesASiblingLedgerEntryWrittenMidCreate(t *testing.T) {
	fix := newFixture(t, nil)
	healthy := fix.runner.response("vm_stat")
	fix.runner.respond("vm_stat", func() ([]byte, error) {
		fix.writeLedgerEntry("slipway-macos-"+mustUUID(t), mustUUID(t))
		return healthy()
	})
	_, err := fix.backend.Create(context.Background(), fix.intent())
	if err == nil || !strings.Contains(err.Error(), "owned-vm-cap") {
		t.Fatalf("a sibling ledgered mid-Create was not counted against a cap of one: %v", err)
	}
	if fix.runner.ran("tart clone") {
		t.Fatal("the cap refused after cloning")
	}
}

// `tart set --random-mac` exiting 0 does not prove the MAC changed. The clone
// must not boot carrying a MAC that ci-guest or a sibling pool VM already has,
// and a config it cannot read is refused rather than assumed fresh. Every case
// is after the clone, so the ledger entry stays and the controller destroys it.
func TestCreateRefusesACloneWithoutADistinctMAC(t *testing.T) {
	sibling := "slipway-macos-" + mustUUID(t)
	cases := []struct {
		name  string
		setup func(fix *fixture)
	}{
		{"MAC left at the base image's, which ci-guest carries", func(fix *fixture) {
			fix.runner.respond("tart set", func() ([]byte, error) {
				writeVMConfig(t, fix.config.TartHome, testResourceID, "darwin", baseImageMAC)
				return nil, nil
			})
		}},
		{"MAC equal to a sibling pool VM's", func(fix *fixture) {
			fix.writeLedgerEntry(sibling, mustUUID(t))
			writeVMConfig(t, fix.config.TartHome, sibling, "darwin", freshMAC)
		}},
		{"the clone's config is missing", func(fix *fixture) {
			fix.runner.set("tart set", "", nil)
		}},
		{"a sibling pool VM's config has no MAC", func(fix *fixture) {
			fix.writeLedgerEntry(sibling, mustUUID(t))
			writeVMConfig(t, fix.config.TartHome, sibling, "darwin", "")
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = 2 })
			testCase.setup(fix)
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil || !strings.Contains(err.Error(), "distinct-mac") {
				t.Fatalf("expected a distinct-mac failure, saw %v", err)
			}
			if fix.runner.ran("tart run") {
				t.Fatal("a clone without a distinct MAC was booted")
			}
			if !fix.ledgerHas(testResourceID) {
				t.Fatal("a post-clone failure released the slot")
			}
		})
	}
}

// A directory under vms/ that this adapter does not own must not block every
// Create: one that vanished mid-scan (the 04:30 reprovision deleting ci-guest),
// one whose config is broken, and one whose config is a FIFO, which would hang
// the daemon cycle if read with a blocking open. A symlinked entry is skipped
// even when it points at a config carrying this clone's MAC, because Inventory
// already fences it and its bytes are chosen by whoever planted it.
func TestCreateIgnoresVMDirectoriesItDoesNotOwn(t *testing.T) {
	cases := map[string]func(t *testing.T, fix *fixture){
		"a directory with no config": func(t *testing.T, fix *fixture) {
			if err := os.MkdirAll(filepath.Join(fix.config.TartHome, "vms", "half-deleted"), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		"a config with no MAC": func(t *testing.T, fix *fixture) {
			writeVMConfig(t, fix.config.TartHome, "builder", "linux", "")
		},
		"a config that is a FIFO": func(t *testing.T, fix *fixture) {
			directory := filepath.Join(fix.config.TartHome, "vms", "planted")
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Mkfifo(filepath.Join(directory, "config.json"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"a real directory whose config.json links to this clone's MAC": func(t *testing.T, fix *fixture) {
			target := t.TempDir()
			writeVMConfig(t, target, "decoy", "darwin", freshMAC)
			directory := filepath.Join(fix.config.TartHome, "vms", "relinked")
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(target, "vms", "decoy", "config.json"),
				filepath.Join(directory, "config.json")); err != nil {
				t.Fatal(err)
			}
		},
		"a ledger-owned sibling whose config is gone": func(t *testing.T, fix *fixture) {
			sibling := "slipway-macos-" + mustUUID(t)
			fix.writeLedgerEntry(sibling, mustUUID(t))
			if err := os.MkdirAll(filepath.Join(fix.config.TartHome, "vms", sibling), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		"a symlink to a config carrying this clone's MAC": func(t *testing.T, fix *fixture) {
			target := t.TempDir()
			writeVMConfig(t, target, "decoy", "darwin", freshMAC)
			if err := os.Symlink(filepath.Join(target, "vms", "decoy"),
				filepath.Join(fix.config.TartHome, "vms", "linked")); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = 2 })
			setup(t, fix)
			done := make(chan error, 1)
			go func() {
				_, err := fix.backend.Create(context.Background(), fix.intent())
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("a VM directory this adapter does not own blocked Create: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Create hung reading a VM directory it does not own")
			}
		})
	}
}

// Two pool VMs, each with its own randomized MAC, is the case ADR-0018 is for.
func TestCreateAdmitsASecondVMWithItsOwnMAC(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = 2 })
	sibling := "slipway-macos-" + mustUUID(t)
	fix.writeLedgerEntry(sibling, mustUUID(t))
	writeVMConfig(t, fix.config.TartHome, sibling, "darwin", "02:5a:00:00:00:02")
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("a second VM with a distinct MAC was refused: %v", err)
	}
}

// Under the cap, Create gets past it. A cap of two admits a second VM next to
// one already owned; this is the case the hard-coded cap refused.
func TestCreateAdmitsAVMUnderTheOwnedVMCap(t *testing.T) {
	cases := []struct {
		name        string
		maxOwned    int
		othersOwned int
	}{
		{"default cap of one, nothing else owned", 1, 0},
		{"cap of two, nothing else owned", 2, 0},
		{"cap of two, one other owned", 2, 1},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, func(c *Config) { c.MaxOwnedVMs = testCase.maxOwned })
			for range testCase.othersOwned {
				fix.writeLedgerEntry("slipway-macos-"+mustUUID(t), mustUUID(t))
			}
			observed, err := fix.backend.Create(context.Background(), fix.intent())
			if err != nil {
				t.Fatalf("create under the cap failed: %v", err)
			}
			if observed.ResourceID != testResourceID || !fix.runner.ran("tart clone") {
				t.Fatalf("create under the cap did not produce the VM: %+v", observed)
			}
		})
	}
}

// Memory under the floor is a shortage and carries controller.ErrCapacity, so
// it never counts toward the provisioning breaker. An unreadable vm_stat is a
// broken probe, not a shortage, and stays a plain refusal that does count.
func TestCreateRefusesWhenTheHostMemoryGateFails(t *testing.T) {
	cases := map[string]struct {
		output   string
		capacity bool
	}{
		"under the floor": {starvedVMStat, true},
		"unreadable":      {"", false},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.runner.set("vm_stat", testCase.output, nil)
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatal("create succeeded with the memory gate failing")
			}
			if !strings.Contains(err.Error(), "memory-gate") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			if got := errors.Is(err, controller.ErrCapacity); got != testCase.capacity {
				t.Fatalf("errors.Is(err, controller.ErrCapacity) = %v, want %v: %v", got, testCase.capacity, err)
			}
			if testCase.capacity {
				assertCapacityDeferredLogged(t, fix.logs.String(), "memory-gate", "ERROR")
			} else if strings.Contains(fix.logs.String(), CapacityDeferredMessage) {
				t.Fatal("an unreadable vm_stat was logged as a capacity deferral")
			}
			if fix.runner.ran("tart clone") || fix.ledgerHas(testResourceID) {
				t.Fatal("a memory refusal still charged the slot or cloned")
			}
		})
	}
}

// ErrCapacity tells the controller nothing exists. A ledger entry Create could
// not remove still charges a slot, so that refusal must stay a plain error and
// reach the controller's failure path. The entry is swapped for a non-empty
// directory between the write and the removal, which os.Remove refuses even
// for root.
func TestACapacityRefusalThatCannotRemoveItsLedgerEntryIsNotErrCapacity(t *testing.T) {
	fix := newFixture(t, nil)
	fix.runner.respond("vm_stat", func() ([]byte, error) {
		path, err := fix.backend.ledgerPath(testResourceID)
		if err != nil {
			return nil, err
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Join(path, "pinned"), 0o700); err != nil {
			return nil, err
		}
		return []byte(starvedVMStat), nil
	})
	_, err := fix.backend.Create(context.Background(), fix.intent())
	if err == nil || !strings.Contains(err.Error(), "memory-gate") {
		t.Fatalf("expected a memory-gate refusal, got %v", err)
	}
	if !strings.Contains(err.Error(), "could not be removed") {
		t.Fatalf("the failed ledger removal was dropped: %v", err)
	}
	if errors.Is(err, controller.ErrCapacity) {
		t.Fatalf("a refusal that left its ledger entry behind carries controller.ErrCapacity: %v", err)
	}
}

// Identity is validated against the controller's real runner-name format:
// controller/allocator.go mints "slipway-" + platform + "-" + lease UUID.
func TestCreateRefusesNonCanonicalIdentities(t *testing.T) {
	cases := map[string]controller.WorkerCreateIntent{
		"resource name is not a pool name": {LeaseID: testLeaseID, ResourceID: "ci-guest"},
		"resource name has upper case":     {LeaseID: testLeaseID, ResourceID: "slipway-macos-ABC"},
		"resource name escapes the prefix": {LeaseID: testLeaseID, ResourceID: "slipway-linux-" + testLeaseID},
		"lease is not a UUID":              {LeaseID: "not-a-uuid", ResourceID: testResourceID},
	}
	for name, intent := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			intent.BootstrapURL = fix.config.BootstrapURL
			intent.BootstrapToken = fix.token
			intent.StartupDeadline = time.Now().Add(time.Minute)
			if _, err := fix.backend.Create(context.Background(), intent); err == nil {
				t.Fatal("a non-canonical identity was accepted")
			}
			if fix.runner.ran("tart clone") {
				t.Fatal("a non-canonical identity reached tart clone")
			}
		})
	}
}

// The real controller name satisfies the regex. Without this, the refusals
// above would pass against a pattern that matches nothing at all.
func TestTheControllersOwnMacOSRunnerNameIsAccepted(t *testing.T) {
	name := "slipway-macos-" + mustUUID(t)
	if !resourceNameRE.MatchString(name) {
		t.Fatalf("the controller's own macOS runner name %q fails the adapter's pattern", name)
	}
}

func assertStillChargedAndReported(t *testing.T, fix *fixture) {
	t.Helper()
	if !fix.ledgerHas(testResourceID) {
		t.Fatal("a post-clone failure released the slot")
	}
	fix.writeVM(testResourceID, "darwin")
	fix.runner.set("tart list --format json",
		listing(vm{Name: "ci-guest", Running: true}, vm{Name: testResourceID, Running: true}), nil)
	inventory, err := fix.backend.Inventory(context.Background())
	if err != nil {
		t.Fatalf("inventory failed after a post-clone failure: %v", err)
	}
	found := false
	for _, worker := range inventory.Workers {
		if worker.ResourceID == testResourceID && worker.LeaseID == testLeaseID {
			found = true
		}
	}
	if !found {
		t.Fatalf("the VM was not reported after a post-clone failure: %+v", inventory.Workers)
	}
}

// assertCapacityDeferredLogged pins the one record a capacity deferral leaves:
// it writes no failure row, so the tart README tells operators to grep this
// line by CapacityDeferredMessage and its check field. A renamed message or a
// dropped field would silently blind that grep.
func assertCapacityDeferredLogged(t *testing.T, logs, check, level string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("fixture log line is not JSON: %q: %v", line, err)
		}
		if record["msg"] != CapacityDeferredMessage {
			continue
		}
		if record["check"] != check || record["level"] != level || record["counts_toward_breaker"] != false {
			t.Fatalf("capacity deferral logged as %v, want check=%s level=%s counts_toward_breaker=false", record, check, level)
		}
		return
	}
	t.Fatalf("no %q line with check=%s was logged:\n%s", CapacityDeferredMessage, check, logs)
}

// The README's "Reading a capacity refusal" section is the operator's grep for
// a deferral, which writes no failure row. It must quote the message and the
// field exactly as the code logs them, or the grep counts zero while
// deferrals happen.
func TestTheReadmeQuotesTheCapacityDeferralLogLine(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"`" + CapacityDeferredMessage + "`", "grep -F '" + CapacityDeferredMessage[strings.Index(CapacityDeferredMessage, "deferred"):] + "'", "check=memory-gate", "counts_toward_breaker=false"} {
		if !strings.Contains(string(readme), want) {
			t.Fatalf("README.md does not quote %q", want)
		}
	}
}
