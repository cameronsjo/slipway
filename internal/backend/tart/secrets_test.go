package tart

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The capability and the JIT must appear in exactly one place: the stdin of the
// single `tart exec -i <vm> <launcher>` call. Not in any argv, not in any
// child environment, not in any other stdin, not in the ledger, not in the
// structured log, not in an error string.
//
// The weakness of this test is named in the plan and repeated here: the fake
// records what the adapter constructs, which is call construction only. M4
// repeats the sweep against the real host during the pilot job.
func TestCapabilityAndJITReachOnlyTheLauncherStdin(t *testing.T) {
	fix := newFixture(t, nil)
	capability := fix.token.Encoded()
	jit := string(testJIT)

	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	launcherCall := "tart exec -i " + testResourceID + " " + fix.config.GuestLauncherPath
	carriers := 0
	for _, call := range fix.runner.recorded() {
		full := key(call.Argv)
		isLauncher := full == launcherCall || strings.HasPrefix(full, launcherCall+" ")

		for _, argument := range call.Argv {
			if strings.Contains(argument, capability) {
				t.Fatalf("the bootstrap capability appeared in an argv: %v", call.Argv)
			}
			if strings.Contains(argument, jit) {
				t.Fatalf("the JIT appeared in an argv: %v", call.Argv)
			}
		}
		for _, variable := range call.Env {
			if strings.Contains(variable, capability) || strings.Contains(variable, jit) {
				t.Fatalf("a secret appeared in a child environment: %v", call.Env)
			}
		}
		stdin := string(call.Stdin)
		if strings.Contains(stdin, capability) {
			t.Fatalf("the bootstrap capability was written to a child's stdin: %v", call.Argv)
		}
		if strings.Contains(stdin, jit) {
			if !isLauncher {
				t.Fatalf("the JIT was written to the stdin of %v, which is not the launcher", call.Argv)
			}
			carriers++
		}
	}
	if carriers != 1 {
		t.Fatalf("the JIT reached %d stdin destinations, expected exactly the launcher's one", carriers)
	}

	// The ledger holds the lease UUID and nothing else.
	entries, err := os.ReadDir(fix.config.LedgerDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no ledger entry was written, so this half of the assertion proved nothing")
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(fix.config.LedgerDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), capability) || strings.Contains(string(data), jit) {
			t.Fatalf("a secret was persisted to the ledger: %s", entry.Name())
		}
	}

	assertLogsAreClean(t, fix, capability, jit)
}

// The same sweep on every failure path: an error message and a log record are
// the two places a secret most easily leaks back out.
func TestFailurePathsPutNoSecretInLogsOrErrorText(t *testing.T) {
	breaks := map[string]func(*fixture){
		"pf gate":     func(f *fixture) { f.runner.set(pfInfoKey(f.config), "Status: Disabled\n", nil) },
		"memory gate": func(f *fixture) { f.runner.set("vm_stat", starvedVMStat, nil) },
		"guest address": func(f *fixture) {
			f.runner.set("tart ip", "192.168.1.9\n", nil)
		},
		"jit delivery": func(f *fixture) {
			f.runner.streamErr = errors.New("tart exec -i could not be started")
		},
	}
	for name, breakIt := range breaks {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			breakIt(fix)
			capability, jit := fix.token.Encoded(), string(testJIT)
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatal("create succeeded on a path this test expects to fail")
			}
			if strings.Contains(err.Error(), capability) || strings.Contains(err.Error(), jit) {
				t.Fatalf("a secret appeared in the error text: %v", err)
			}
			assertLogsAreClean(t, fix, capability, jit)
		})
	}
}

func assertLogsAreClean(t *testing.T, fix *fixture, capability, jit string) {
	t.Helper()
	logged := fix.logs.String()
	if logged == "" {
		t.Fatal("no log records were captured, so the log assertion proved nothing")
	}
	if strings.Contains(logged, capability) {
		t.Fatal("the bootstrap capability was written to a log record")
	}
	if strings.Contains(logged, jit) {
		t.Fatal("the JIT was written to a log record")
	}
	// The tarball and launcher bytes are not secrets, but nothing needs them in
	// a log either, and a change that starts echoing command output would put
	// both a payload and a capability there.
	if strings.Contains(logged, "runner tarball bytes") || strings.Contains(logged, "guest launcher bytes") {
		t.Fatal("artifact contents were written to a log record")
	}
}

// bootstrap.Token redacts itself in every formatting verb. The adapter relies
// on that rather than on care at each call site, so it is worth pinning here.
func TestTokenStaysRedactedWhenFormatted(t *testing.T) {
	fix := newFixture(t, nil)
	token := fix.intent().BootstrapToken
	for _, rendered := range []string{
		fmt.Sprintf("%v", token), fmt.Sprintf("%s", token), fmt.Sprintf("%+v", token),
	} {
		if strings.Contains(rendered, fix.token.Encoded()) {
			t.Fatalf("a formatted token revealed its value: %q", rendered)
		}
	}
}
