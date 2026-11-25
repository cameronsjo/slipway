package tart

import (
	"context"
	"strings"
	"testing"
)

// I-A. Two more spellings that a looser matcher reads as "our anchor is
// referenced, and early". Neither evaluates the reviewed rules.
func TestAnchorReferenceRejectsLoadDirectivesAndSubAnchors(t *testing.T) {
	cases := map[string]string{
		// A load directive names the anchor but evaluates nothing. pf loads the
		// file here; whether the anchor is ever *run* is a separate line.
		"load directive only": `load anchor "tart-ci" from "/etc/pf.anchors/tart-ci"
anchor "com.apple.internet-sharing/*" all
`,
		// pf evaluates the sub-anchor, not tart-ci. The reviewed blocks still
		// never run, and the previous "equal or child path" rule accepted it.
		"sub-anchor stands in for ours": `anchor "tart-ci/evil" all
anchor "com.apple.internet-sharing/*" all
`,
		// Ordering judged on a sub-anchor is the same bug one step along.
		"sub-anchor early, ours late": `anchor "tart-ci/evil" all
anchor "com.apple.internet-sharing/*" all
anchor "tart-ci" all
`,
	}
	for name, ruleset := range cases {
		t.Run(name, func(t *testing.T) {
			fix := newFixture(t, nil)
			fix.runner.set(pfMainKey(fix.config), ruleset, nil)
			_, err := fix.backend.Create(context.Background(), fix.intent())
			if err == nil {
				t.Fatal("a ruleset that never evaluates the reviewed anchor passed the gate")
			}
			if !strings.Contains(err.Error(), "pf-anchor-referenced") &&
				!strings.Contains(err.Error(), "pf-anchor-order") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}
}

// The wildcard subtree form is how the real host spells Apple's anchor, so it
// must still match. Without this the ordering check silently stops finding the
// conflicting anchor and passes for the wrong reason.
func TestAnchorTokenMatchesTheWildcardSubtreeFormExactly(t *testing.T) {
	line := `anchor "com.apple.internet-sharing/*" all`
	if index := anchorLineIndex(line, "com.apple.internet-sharing"); index != 0 {
		t.Fatalf("the real host's wildcard spelling did not match: %d", index)
	}
	if index := anchorLineIndex(line, "com.apple.internet"); index != -1 {
		t.Fatalf("a shorter name matched a longer anchor: %d", index)
	}
	if index := anchorLineIndex(`  anchor "tart-ci" all`, "tart-ci"); index != 0 {
		t.Fatal("a leading-whitespace anchor line did not match")
	}
}

// N-1. The hash pins the FILE; pf evaluates the LOADED ruleset. A ruleset
// loaded from somewhere else can carry every reviewed block plus one leading
// pass that wins before any of them, and the hash check cannot see it.
func TestALoadedRulesetWithAnExtraPassIsRefused(t *testing.T) {
	fix := newFixture(t, nil)
	fix.runner.set(pfAnchorKey(fix.config),
		"pass in quick on bridge100 all\n"+healthyAnchorRules, nil)
	_, err := fix.backend.Create(context.Background(), fix.intent())
	if err == nil {
		t.Fatal("a loaded ruleset with a leading pass-all passed the gate")
	}
	if !strings.Contains(err.Error(), "pf-anchor-loaded") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	if fix.runner.ran("tart clone") {
		t.Fatal("the refusal happened after cloning")
	}
}

// And a loaded ruleset missing one of the reviewed blocks, which clears the
// floor of 8 only if something else was added.
func TestALoadedRulesetThatDoesNotMatchTheFileIsRefused(t *testing.T) {
	fix := newFixture(t, nil)
	// Swap one block for a pass: still 14 rules, still above the floor.
	swapped := strings.Replace(healthyAnchorRules,
		"block drop in quick inet from 192.168.2.0/24 to 100.64.0.0/10",
		"pass in quick inet from 192.168.2.0/24 to 100.64.0.0/10", 1)
	fix.runner.set(pfAnchorKey(fix.config), swapped, nil)
	if _, err := fix.backend.Create(context.Background(), fix.intent()); err == nil {
		t.Fatal("a loaded ruleset with a block swapped for a pass passed the gate")
	}
}

// The counter must read the REAL reviewed anchor as the hand-written expanded
// fixture does. pf expands a braced list into one rule per element, so the
// file's three pass lines (two two-element lists each in two of them) load as
// six. If these two disagree the gate false-refuses on a healthy host.
func TestPinnedRuleCountsMatchTheHandWrittenExpansion(t *testing.T) {
	data := readFixtureWithRepoSync(t, reviewedAnchorPath, reviewedAnchorRepoPath)
	pinned := pinnedRuleCounts(string(data))
	loadedBlocks, loadedPasses := countLoadedRules(healthyAnchorRules)
	if pinned.blocks != loadedBlocks || pinned.passes != loadedPasses {
		t.Fatalf("the file counts %d block / %d pass but the expanded fixture has %d / %d",
			pinned.blocks, pinned.passes, loadedBlocks, loadedPasses)
	}
	if pinned.blocks != 8 || pinned.passes != 6 {
		t.Fatalf("the reviewed anchor should load as 8 block and 6 pass rules, counted %d and %d",
			pinned.blocks, pinned.passes)
	}
}

// Tables are declarations, not rules, and pfctl prints them under -s Tables
// rather than -s rules. Counting them would make the two sides disagree.
func TestRuleCountingIgnoresTablesCommentsAndBlanks(t *testing.T) {
	source := `# a comment mentioning block drop and pass
table <hostaddrs> persist { 1.2.3.4, 5.6.7.8 }
table <hostaddrs6> persist

pass in quick on bridge100 proto { tcp, udp } from any to any port { 53, 853 }
block drop in quick on bridge100 all
`
	counts := pinnedRuleCounts(source)
	// One pass line with two two-element lists loads as four rules.
	if counts.passes != 4 || counts.blocks != 1 {
		t.Fatalf("expected 4 pass and 1 block, counted %d and %d", counts.passes, counts.blocks)
	}
	blocks, passes := countLoadedRules(source)
	if blocks != 1 || passes != 1 {
		t.Fatalf("the loaded counter should count printed lines: expected 1 and 1, saw %d and %d", blocks, passes)
	}
}
