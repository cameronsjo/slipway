package tart

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// The gate is read-only. Its three commands are exactly the three read-only
// NOPASSWD entries installed on the M1 Max:
//
//	cameron ALL=(root) NOPASSWD: /sbin/pfctl -s info
//	cameron ALL=(root) NOPASSWD: /sbin/pfctl -s rules
//	cameron ALL=(root) NOPASSWD: /sbin/pfctl -a tart-ci -s rules
//
// Anything else -- another anchor name, an extra flag, a different order -- is
// refused by sudo, which surfaces here as an unreadable read-back and therefore
// as a refusal. That is the intended shape: the pool never gains a general
// pfctl grant, and a configuration naming an anchor the sudoers file does not
// permit cannot start a VM.
func (b *Backend) pfInfoArgv() []string {
	return []string{b.config.SudoBinary, "-n", b.config.PfctlBinary, "-s", "info"}
}

func (b *Backend) pfAnchorRulesArgv() []string {
	return []string{b.config.SudoBinary, "-n", b.config.PfctlBinary, "-a", b.config.PfAnchor, "-s", "rules"}
}

func (b *Backend) pfMainRulesArgv() []string {
	return []string{b.config.SudoBinary, "-n", b.config.PfctlBinary, "-s", "rules"}
}

// assertBoundary runs the gate. Any command failure, any timeout, and any empty
// or unreadable output refuses: an unmeasured boundary is not a pass. It
// mutates nothing, so it cannot race Apple's own ruleset reload and cannot
// touch the legacy guest.
//
// Five assertions over three sudo commands and one plain file read:
//
//  1. pf is enabled;
//  2. the anchor carries at least MinAnchorBlockRules `block drop` rules;
//  3. the anchor file's content hashes to the reviewed value;
//  4. the anchor is referenced from the main ruleset;
//  5. the anchor is ordered ahead of the conflicting anchor.
//
// Assertions 4 and 5 read one `pfctl -s rules` capture, because two captures
// can disagree with each other and the second would silently win.
func (b *Backend) assertBoundary(ctx context.Context) error {
	info, err := b.gateRead(ctx, b.pfInfoArgv())
	if err != nil || len(strings.TrimSpace(string(info))) == 0 {
		return b.gateRefusal("pf-status", "readable pf status", "no readable output", err)
	}
	status := firstLine(string(info))
	if !strings.Contains(status, "Enabled") {
		return b.gateRefusal("pf-status", "Enabled", status, nil)
	}

	anchorRules, err := b.gateRead(ctx, b.pfAnchorRulesArgv())
	if err != nil || len(strings.TrimSpace(string(anchorRules))) == 0 {
		return b.gateRefusal("pf-anchor-rules", "readable anchor rules", "no readable output", err)
	}
	blocks := countLinesContaining(string(anchorRules), "block drop")
	if blocks < b.config.PfMinBlockRules {
		return b.gateRefusal("pf-anchor-rules",
			fmt.Sprintf("at least %d block drop rules in anchor %q", b.config.PfMinBlockRules, b.config.PfAnchor),
			fmt.Sprintf("%d", blocks), nil)
	}

	// Counting rules cannot tell the reviewed ruleset from a different one with
	// the same number of rules, so pin the content itself. Same value, same
	// file, and the same reason as tart-pf-assert.sh's own B4 hash check.
	pinned, err := b.assertAnchorContent()
	if err != nil {
		return err
	}

	// The hash pins the FILE; pf evaluates the LOADED ruleset, and nothing so
	// far says those are the same thing. A ruleset loaded from somewhere else
	// can carry the reviewed blocks plus a leading `pass in quick on bridge100
	// all` that wins every match before them. Compare counts, not text, so
	// pfctl's print formatting is not pinned.
	loadedBlocks, loadedPasses := countLoadedRules(string(anchorRules))
	if loadedBlocks != pinned.blocks || loadedPasses != pinned.passes {
		return b.gateRefusal("pf-anchor-loaded",
			fmt.Sprintf("%d block and %d pass rules, matching the pinned anchor", pinned.blocks, pinned.passes),
			fmt.Sprintf("%d block and %d pass rules", loadedBlocks, loadedPasses), nil)
	}

	mainRules, err := b.gateRead(ctx, b.pfMainRulesArgv())
	if err != nil || len(strings.TrimSpace(string(mainRules))) == 0 {
		return b.gateRefusal("pf-main-rules", "readable main ruleset", "no readable output", err)
	}
	ours := anchorLineIndex(string(mainRules), b.config.PfAnchor)
	if ours < 0 {
		return b.gateRefusal("pf-anchor-referenced",
			fmt.Sprintf("anchor %q referenced from the main ruleset", b.config.PfAnchor), "not referenced", nil)
	}
	// An anchor can be loaded and never evaluated, and first match wins, so
	// ANY earlier filter anchor or inline pass rule wins silently before
	// tart-ci's `quick` blocks (homelab#875 I5). Check both the named
	// conflicting anchor and any preceding filter anchor / inline pass rule.
	conflicting := anchorLineIndex(string(mainRules), b.config.PfConflictingAnchor)
	if conflicting >= 0 && ours > conflicting {
		return b.gateRefusal("pf-anchor-order",
			fmt.Sprintf("anchor %q ahead of %q", b.config.PfAnchor, b.config.PfConflictingAnchor),
			fmt.Sprintf("line %d is after line %d", ours+1, conflicting+1), nil)
	}
	if earlierIdx, earlierLine := earlierFilterBypass(string(mainRules), ours); earlierIdx >= 0 {
		return b.gateRefusal("pf-anchor-order",
			fmt.Sprintf("anchor %q ahead of all other filter anchors and pass rules", b.config.PfAnchor),
			fmt.Sprintf("line %d (%s) precedes line %d", earlierIdx+1, earlierLine, ours+1), nil)
	}
	return nil
}

// earlierFilterBypass reports the first line before anchorIdx that is either a
// filter `anchor` or an inline `pass` rule, either of which could match and
// pass guest traffic before tart-ci's `quick` blocks run (homelab#875 I5).
func earlierFilterBypass(mainRules string, anchorIdx int) (int, string) {
	lines := strings.Split(mainRules, "\n")
	for i := 0; i < anchorIdx && i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if ruleVerb(trimmed) == "pass" || strings.HasPrefix(trimmed, "anchor ") {
			return i, trimmed
		}
	}
	return -1, ""
}

// gateRead bounds one pf read. Without this a wedged pfctl blocks the re-assert
// loop forever with no log line and no refusal, which is the fail-open shape
// the loop exists to prevent. A timeout is a refusal, not a retry.
func (b *Backend) gateRead(ctx context.Context, argv []string) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, b.config.GateReadTimeout)
	defer cancel()
	out, err := b.runner.Run(readCtx, argv, b.hostEnv(), nil)
	if readCtx.Err() != nil {
		return nil, errors.New("pf read did not answer within the gate read timeout")
	}
	return out, err
}

// assertAnchorContent hashes the anchor file itself. The file is world-readable
// at /etc/pf.anchors/tart-ci, so this needs no sudo grant. A symlink or a
// non-regular file is refused rather than followed: the point is to pin the
// bytes the kernel loaded, and a symlink is a second name someone else controls.
func (b *Backend) assertAnchorContent() (ruleCounts, error) {
	var counts ruleCounts
	info, err := os.Lstat(b.config.PfAnchorFile)
	if err != nil {
		return counts, b.gateRefusal("pf-anchor-content", "a readable reviewed anchor file", "unreadable", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return counts, b.gateRefusal("pf-anchor-content", "a regular anchor file", "a symlink", nil)
	}
	if !info.Mode().IsRegular() {
		return counts, b.gateRefusal("pf-anchor-content", "a regular anchor file", "a non-regular file", nil)
	}
	data, err := os.ReadFile(b.config.PfAnchorFile)
	if err != nil {
		return counts, b.gateRefusal("pf-anchor-content", "a readable reviewed anchor file", "unreadable", err)
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != b.config.PfAnchorSHA256 {
		// Prefixes only. The full hashes are in the config and the assert
		// script; a log line does not need to restate them.
		return counts, b.gateRefusal("pf-anchor-content",
			"anchor content "+b.config.PfAnchorSHA256[:16], "anchor content "+got[:16], nil)
	}
	return pinnedRuleCounts(string(data)), nil
}

// ruleCounts is how many filter rules a ruleset carries, by verb.
type ruleCounts struct{ blocks, passes int }

// countLoadedRules counts what `pfctl -a <anchor> -s rules` printed. Tables are
// not rules and are not printed by -s rules at all (they appear under
// -s Tables), so only pass and block lines are counted.
func countLoadedRules(text string) (blocks, passes int) {
	for _, line := range strings.Split(text, "\n") {
		switch verb := ruleVerb(line); verb {
		case "block":
			blocks++
		case "pass":
			passes++
		}
	}
	return blocks, passes
}

// pinnedRuleCounts counts the same thing in the anchor FILE, which is the
// unexpanded source. pf expands a braced list into one rule per element and
// takes the cartesian product across lists, so
// `pass ... proto { tcp, udp } ... port { 53, 853 }` loads as four rules. The
// reviewed anchor uses two such lists today, so ignoring expansion would make
// the loaded and pinned counts disagree on a perfectly healthy host -- a false
// refusal, which is the failure mode this gate can least afford.
//
// A list shape this arithmetic cannot model would also produce a false refusal
// rather than a false pass. The anchor is hash-pinned, so it cannot change
// without a deliberate two-file edit that would surface the disagreement.
func pinnedRuleCounts(text string) ruleCounts {
	var counts ruleCounts
	for _, line := range strings.Split(text, "\n") {
		verb := ruleVerb(line)
		if verb != "block" && verb != "pass" {
			continue
		}
		expanded := expansionFactor(line)
		if verb == "block" {
			counts.blocks += expanded
			continue
		}
		counts.passes += expanded
	}
	return counts
}

// ruleVerb reports the leading keyword of a filter rule line, or "" for a
// comment, a table declaration, a macro, or blank space.
func ruleVerb(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return ""
	}
	first, _, _ := strings.Cut(trimmed, " ")
	switch first {
	case "block", "pass":
		return first
	}
	return ""
}

// expansionFactor is the number of rules pf loads for one source line: the
// product of the lengths of its braced lists, and 1 when it has none.
func expansionFactor(line string) int {
	factor := 1
	rest := line
	for {
		open := strings.IndexByte(rest, '{')
		if open < 0 {
			return factor
		}
		close := strings.IndexByte(rest[open:], '}')
		if close < 0 {
			return factor
		}
		items := 0
		for _, item := range strings.Split(rest[open+1:open+close], ",") {
			if strings.TrimSpace(item) != "" {
				items++
			}
		}
		if items > 0 {
			factor *= items
		}
		rest = rest[open+close:]
	}
}

// gateRefusal names the broken expectation and what was actually seen. It never
// carries command output verbatim beyond the small derived values above.
func (b *Backend) gateRefusal(check, expected, seen string, cause error) error {
	b.log.Error("tart pf boundary refused worker creation",
		slog.String("check", check), slog.String("expected", expected),
		slog.String("seen", seen), slog.Bool("command_failed", cause != nil))
	return fmt.Errorf("tart pf boundary check %s failed: expected %s, saw %s", check, expected, seen)
}

func firstLine(text string) string {
	if index := strings.IndexByte(text, '\n'); index >= 0 {
		return strings.TrimSpace(text[:index])
	}
	return strings.TrimSpace(text)
}

func countLinesContaining(text, needle string) int {
	var count int
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, needle) {
			count++
		}
	}
	return count
}

// anchorLineIndex finds the first line that EVALUATES the named anchor.
//
// Two readings have already been wrong here, and both let a gate pass with no
// boundary in force. A prefix match on `anchor "` + name accepted
// `anchor "tart-ci-old" all`, reporting the real anchor as referenced when it
// was absent and as early when it was last. Loosening that to "equal or a child
// path" then accepted `anchor "tart-ci/evil"` -- pf evaluates the SUB-anchor
// there, not tart-ci, so the reviewed rules still never run.
//
// So: the trimmed line must START with `anchor "`, which excludes
// `load anchor "tart-ci" from "..."` (a load directive, which evaluates
// nothing), and the quoted token must equal the name or the name plus exactly
// `/*`. The wildcard form is required because the real host prints Apple's as
// `anchor "com.apple.internet-sharing/*" all`, and the exact form is required
// because anything else is a different anchor.
func anchorLineIndex(text, name string) int {
	for index, line := range strings.Split(text, "\n") {
		token, ok := anchorToken(strings.TrimSpace(line))
		if !ok {
			continue
		}
		if token == name || token == name+"/*" {
			return index
		}
	}
	return -1
}

// anchorToken extracts the quoted name from a line that begins `anchor "`.
// A comment, a `load anchor` directive, or anything with text before the
// keyword is not an anchor this gate will reason about.
func anchorToken(line string) (string, bool) {
	const prefix = `anchor "`
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	rest := line[len(prefix):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return "", false
	}
	after := rest[end+1:]
	if after != "" && after[0] != ' ' && after[0] != '\t' {
		return "", false
	}
	return rest[:end], true
}
