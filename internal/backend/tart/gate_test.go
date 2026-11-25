package tart

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// The gate has four assertions. Each one gets two negative controls: the
// command failing, and the command succeeding with output that says nothing.
// "Unmeasured is a refusal" is the property under test, and an empty read-back
// is the case a naive implementation passes.
func TestGateRefusesEveryFailedAndEveryUnreadableReadBack(t *testing.T) {
	cases := []struct {
		name    string
		prefix  func(Config) string
		output  string
		err     error
		refusal string
	}{
		{"pf status command fails", pfInfoKey, "", errors.New("sudo refused"), "pf-status"},
		{"pf status reads empty", pfInfoKey, "   \n", nil, "pf-status"},
		{"pf status reports disabled", pfInfoKey, "Status: Disabled\n", nil, "pf-status"},
		{"anchor rules command fails", pfAnchorKey, "", errors.New("sudo refused"), "pf-anchor-rules"},
		{"anchor rules read empty", pfAnchorKey, "", nil, "pf-anchor-rules"},
		{"anchor rules below the floor", pfAnchorKey, "block drop all\nblock drop all\npass all\n", nil, "pf-anchor-rules"},
		{"main ruleset command fails", pfMainKey, "", errors.New("sudo refused"), "pf-main-rules"},
		{"main ruleset reads empty", pfMainKey, "\n\n", nil, "pf-main-rules"},
		{"anchor is not referenced", pfMainKey, "anchor \"com.apple.internet-sharing/*\" all\n", nil, "pf-anchor-referenced"},
		{"anchor is ordered after internet sharing", pfMainKey,
			"anchor \"com.apple.internet-sharing/*\" all\nanchor \"tart-ci\" all\n", nil, "pf-anchor-order"},
		{"anchor is ordered after another filter anchor", pfMainKey,
			"anchor \"com.apple/*\" all\nanchor \"tart-ci\" all\n", nil, "pf-anchor-order"},
		{"inline pass rule precedes anchor", pfMainKey,
			"pass in quick on bridge100 all\nanchor \"tart-ci\" all\n", nil, "pf-anchor-order"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.runner.set(testCase.prefix(fix.config), testCase.output, testCase.err)

			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatal("Create succeeded with a broken pf boundary")
			}
			if !strings.Contains(err.Error(), testCase.refusal) {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			// Refused before any VM exists: no clone, no run, and the ledger
			// entry written first is taken back so the slot is not charged.
			if fix.runner.ran("tart clone") || fix.runner.ran("tart run") {
				t.Fatal("a VM was created despite the gate refusing")
			}
			if fix.ledgerHas(testResourceID) {
				t.Fatal("a pre-clone refusal left a ledger entry charging the slot")
			}
		})
	}
}

// The gate must never reach the legacy scripts. Their failure paths act on
// ci-guest by name: tart-pf-assert.sh stops it, and tart-ci-run's bridge check
// passes vacuously whenever ci-guest is already up.
func TestNoInvocationEverNamesALegacyCIGuestScript(t *testing.T) {
	fix := newFixture(t, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err != nil {
		t.Fatalf("happy-path create failed: %v", err)
	}
	if _, err := fix.backend.Inventory(context.Background()); err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	if _, err := fix.backend.Destroy(context.Background(),
		workerObservation(testLeaseID, testResourceID)); err != nil {
		t.Fatalf("destroy failed: %v", err)
	}
	forbidden := []string{"tart-pf-assert.sh", "tart-ci-run", "tart-ci-reprovision.sh"}
	for _, call := range fix.runner.recorded() {
		for _, argument := range call.Argv {
			for _, name := range forbidden {
				if strings.Contains(argument, name) {
					t.Fatalf("the pool invoked the legacy script %q: %v", name, call.Argv)
				}
			}
		}
	}
	if len(fix.runner.recorded()) == 0 {
		t.Fatal("nothing was recorded, so this assertion proved nothing")
	}
}

// The gate uses the three argument-exact NOPASSWD forms and nothing else. A
// fourth form, or a different anchor name, is refused by sudo on the real host,
// which would look like an unreadable boundary rather than a configuration bug.
func TestGateUsesOnlyTheThreeGrantedSudoArgvForms(t *testing.T) {
	fix := newFixture(t, nil)
	if err := fix.backend.assertBoundary(context.Background()); err != nil {
		t.Fatalf("healthy boundary was refused: %v", err)
	}
	granted := map[string]bool{
		"/usr/bin/sudo -n /sbin/pfctl -s info":             true,
		"/usr/bin/sudo -n /sbin/pfctl -s rules":            true,
		"/usr/bin/sudo -n /sbin/pfctl -a tart-ci -s rules": true,
	}
	seen := map[string]bool{}
	for _, call := range fix.runner.recorded() {
		if call.Argv[0] != fix.config.SudoBinary {
			continue
		}
		form := strings.Join(call.Argv, " ")
		if !granted[form] {
			t.Fatalf("gate used a sudo form the host does not grant: %q", form)
		}
		seen[form] = true
	}
	if len(seen) != len(granted) {
		t.Fatalf("expected all three granted forms, saw %d", len(seen))
	}
}

func pfInfoKey(c Config) string { return "sudo -n " + c.PfctlBinary + " -s info" }
func pfAnchorKey(c Config) string {
	return "sudo -n " + c.PfctlBinary + " -a " + c.PfAnchor + " -s rules"
}
func pfMainKey(c Config) string { return "sudo -n " + c.PfctlBinary + " -s rules" }
