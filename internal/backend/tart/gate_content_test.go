package tart

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// C1(a). A prefix match on `anchor "` + name has no closing quote, so the line
// `anchor "tart-ci-old" all` satisfies both the reference and the ordering
// check while the real tart-ci anchor is absent from the ruleset entirely.
// The boundary is not in force and the gate is green.
func TestAnchorReferenceDoesNotMatchALongerAnchorName(t *testing.T) {
	fix := newFixture(t, nil)
	fix.runner.set(pfMainKey(fix.config), `scrub-anchor "com.apple/*" all
anchor "tart-ci-old" all
anchor "com.apple.internet-sharing/*" all
`, nil)
	_, err := fix.backend.Create(context.Background(), fix.intent())
	if err == nil {
		t.Fatal("a ruleset containing only tart-ci-old passed the gate")
	}
	if !strings.Contains(err.Error(), "pf-anchor-referenced") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// C1(b). Ordering must be judged on the REAL anchor's line. With a decoy first,
// internet-sharing second and the real anchor third, a prefix match reports
// position 1 and passes, while tart-ci's blocks actually lose to Apple's passes.
func TestAnchorOrderIsJudgedOnTheRealAnchorNotADecoy(t *testing.T) {
	fix := newFixture(t, nil)
	fix.runner.set(pfMainKey(fix.config), `anchor "tart-ci-old" all
anchor "com.apple.internet-sharing/*" all
anchor "tart-ci" all
`, nil)
	_, err := fix.backend.Create(context.Background(), fix.intent())
	if err == nil {
		t.Fatal("an anchor ordered behind internet-sharing passed the gate")
	}
	if !strings.Contains(err.Error(), "pf-anchor-order") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// Positive control for both of the above. The real host's spelling -- our
// anchor as a bare name, Apple's as a wildcard subtree -- must still pass, or
// the two refusals prove nothing but that the matcher is broken.
func TestTheRealHostsRulesetSpellingStillPasses(t *testing.T) {
	fix := newFixture(t, nil)
	if err := fix.backend.assertBoundary(context.Background()); err != nil {
		t.Fatalf("the real host's ruleset spelling was refused: %v", err)
	}
}

// A commented-out anchor reference is not a reference.
func TestCommentedAnchorLinesAreIgnored(t *testing.T) {
	fix := newFixture(t, nil)
	fix.runner.set(pfMainKey(fix.config), `# anchor "tart-ci" all
anchor "com.apple.internet-sharing/*" all
`, nil)
	if err := fix.backend.assertBoundary(context.Background()); err == nil {
		t.Fatal("a commented-out anchor line counted as a reference")
	}
}

// C1(c). The floor was 3 while the reviewed anchor carries 8 and
// tart-pf-assert.sh demands >=8. An anchor holding only its three IPv6 rules --
// every IPv4 block gone -- cleared a floor of 3.
func TestBlockRuleFloorMatchesTheAssertScript(t *testing.T) {
	if MinAnchorBlockRules != 8 {
		t.Fatalf("the adapter's floor is %d; tart-pf-assert.sh demands >=8", MinAnchorBlockRules)
	}
	fix := newFixture(t, nil)
	// Three IPv6 blocks and nothing else: the shape the old floor admitted.
	fix.runner.set(pfAnchorKey(fix.config), strings.Repeat("block drop quick inet6 from any to any\n", 3), nil)
	_, err := fix.backend.Create(context.Background(), fix.intent())
	if err == nil {
		t.Fatal("an anchor with only its three IPv6 rules passed the gate")
	}
	if !strings.Contains(err.Error(), "pf-anchor-rules") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// C1, the content half. Counting rules cannot tell the reviewed ruleset from a
// different one with the same number of rules, so the anchor file is hashed.
func TestAnchorContentIsPinnedByHash(t *testing.T) {
	cases := map[string]func(*testing.T, *fixture) string{
		"content changed": func(t *testing.T, fix *fixture) string {
			writeArtifact(t, fix.config.PfAnchorFile, []byte("pass all\n"))
			return "pf-anchor-content"
		},
		"file missing": func(t *testing.T, fix *fixture) string {
			if err := os.Remove(fix.config.PfAnchorFile); err != nil {
				t.Fatal(err)
			}
			return "pf-anchor-content"
		},
		"file is a symlink": func(t *testing.T, fix *fixture) string {
			real := filepath.Join(filepath.Dir(fix.config.PfAnchorFile), "real.anchor")
			data, err := os.ReadFile(fix.config.PfAnchorFile)
			if err != nil {
				t.Fatal(err)
			}
			writeArtifact(t, real, data)
			if err := os.Remove(fix.config.PfAnchorFile); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, fix.config.PfAnchorFile); err != nil {
				t.Fatal(err)
			}
			return "pf-anchor-content"
		},
		"file is a directory": func(t *testing.T, fix *fixture) string {
			if err := os.Remove(fix.config.PfAnchorFile); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(fix.config.PfAnchorFile, 0o700); err != nil {
				t.Fatal(err)
			}
			return "pf-anchor-content"
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			expect := breakIt(t, fix)
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatal("the gate passed with an anchor whose content it could not vouch for")
			}
			if !strings.Contains(err.Error(), expect) {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
			if fix.runner.ran("tart clone") {
				t.Fatal("a content refusal happened after cloning")
			}
		})
	}
}

// The hash must never be read from the file it is checking. Pinning it to the
// value tart-pf-assert.sh already enforces is what keeps the pool's gate and
// the legacy gate from drifting into disagreeing about the same boundary.
func TestExampleConfigAnchorHashEqualsTheAssertScripts(t *testing.T) {
	script := readFixtureWithRepoSync(t, assertScriptPath, assertScriptRepoPath)
	var fromScript string
	for _, line := range strings.Split(string(script), "\n") {
		if after, found := strings.CutPrefix(strings.TrimSpace(line), "EXPECT_HASH="); found {
			fromScript = strings.TrimSpace(after)
			break
		}
	}
	if !sha256RE.MatchString(fromScript) {
		t.Fatalf("could not parse EXPECT_HASH out of %s, got %q", assertScriptPath, fromScript)
	}
	example, err := os.ReadFile("../../../config/macos-controller.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(example), fromScript) {
		t.Fatalf("the example config does not carry the assert script's EXPECT_HASH %s", fromScript)
	}
	// And that value must really be the reviewed anchor's hash, or both gates
	// agree on a number that describes nothing.
	anchor := readFixtureWithRepoSync(t, reviewedAnchorPath, reviewedAnchorRepoPath)
	if digestOf(anchor) != fromScript {
		t.Fatalf("EXPECT_HASH %s is not the hash of %s", fromScript, reviewedAnchorPath)
	}
}

// I1. A wedged pfctl with no timeout blocks the re-assert loop forever, with no
// log line and no refusal: the boundary fails open and nothing says so.
func TestAWedgedPfReadIsARefusalNotAHang(t *testing.T) {
	for _, blocked := range []func(Config) string{pfInfoKey, pfAnchorKey, pfMainKey} {
		fix := newFixture(t, func(c *Config) { c.GateReadTimeout = 50 * time.Millisecond })
		fix.runner.setBlocking(blocked(fix.config))
		done := make(chan error, 1)
		go func() { done <- fix.backend.assertBoundary(context.Background()) }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("a wedged %q passed the gate", blocked(fix.config))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("a wedged %q hung the gate past its timeout", blocked(fix.config))
		}
	}
}

// And a wedged read in the loop stops the owned VM, because a refusal is a
// refusal however it was reached.
func TestAWedgedPfReadInTheLoopStopsTheOwnedVM(t *testing.T) {
	fix := newFixture(t, func(c *Config) { c.GateReadTimeout = 50 * time.Millisecond })
	fix.writeLedgerEntry(testResourceID, testLeaseID)
	fix.runner.setBlocking(pfInfoKey(fix.config))
	fix.backend.reassertOnce(context.Background())
	if !fix.runner.ran("tart stop " + testResourceID) {
		t.Fatal("a wedged pf read left the owned VM running")
	}
}
