package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const sandboxIDLabel = "io.cri-containerd.sandbox-id"

// maxMeasuredAgeSeconds caps how stale a continuously measured fingerprint may
// be allowed to get. One hour is far above any sane measurement interval (the
// guard attestation timer runs every 30 seconds against a 120 second bound) and
// far below a value that would accept whatever mtime happens to be there.
const maxMeasuredAgeSeconds = 3600

// HostStorageConfig binds inventory to the dedicated production snapshotter.
// Its containerd namespace may contain other snapshotters, but every container
// using DedicatedSnapshotter must map to the durable worker identity ledger.
type HostStorageConfig struct {
	CTRExecutable         string
	ContainerdAddress     string
	ContainerdNamespace   string
	DedicatedSnapshotter  string
	ProcessIdentitySource *HostProcessCollector
	RuntimeFingerprints   []RuntimeFingerprint
}

type RuntimeFingerprint struct {
	Purpose string `json:"purpose"`
	Path    string `json:"path"`
	SHA256  string `json:"sha256"`
	// MaxAgeSeconds bounds how old the file may be, for fingerprints whose
	// target is CONTINUOUSLY MEASURED rather than static.
	//
	// The other three targets are static files: a containerd config that
	// changes is a config that changed, and its mtime says nothing useful. The
	// nft guard is different. Its fingerprint is written by a host-side timer
	// that re-measures the live ruleset, so the file's content is only evidence
	// about the guard AT THE MOMENT IT WAS WRITTEN. A content hash alone cannot
	// tell an intact guard from a measurer that stopped running an hour ago and
	// left its last good answer behind -- both hash the same, and the second one
	// certifies a guard nobody has looked at since.
	//
	// Zero means no bound, which is correct for a static target and wrong for a
	// measured one. Nothing here infers which is which: the config says so.
	MaxAgeSeconds int `json:"max_age_seconds,omitempty"`
}

type HostStorageCollector struct {
	config HostStorageConfig
	run    hostCommandRunner
	// rereadDelay pauses briefly before the single CRI re-read on
	// errUnknownWorkerIdentity so a mid-create sandbox finishes registering
	// with containerd. Zero skips the wait (used by unit tests).
	rereadDelay time.Duration
	// log receives one WARN per tolerated skip. A nil log is silent, which is
	// what every existing test and the off-witness callers get; the witness
	// binary sets it.
	log *WitnessLogger
}

const defaultIdentityRereadDelay = 500 * time.Millisecond

// SetLogger wires the witness's logger into the collector so a tolerated skip
// is visible. The skip was SILENT when it landed (#847 security review,
// Important 2): a pool quietly dropping containers from its inventory looks
// exactly like a pool with fewer containers, and the only difference an
// operator could have seen was a fence that no longer happened.
func (c *HostStorageCollector) SetLogger(log *WitnessLogger) {
	if c != nil {
		c.log = log
	}
}

var _ StorageSource = (*HostStorageCollector)(nil)

func NewHostStorageCollector(config HostStorageConfig) (*HostStorageCollector, error) {
	if config.CTRExecutable == "" || !filepath.IsAbs(config.CTRExecutable) || config.ContainerdAddress == "" ||
		config.ContainerdNamespace == "" || config.DedicatedSnapshotter == "" || config.DedicatedSnapshotter == "overlayfs" ||
		config.ProcessIdentitySource == nil || len(config.RuntimeFingerprints) != 6 {
		return nil, errors.New("host storage collector configuration is incomplete")
	}
	// nft-guard is the fourth. Before it existed, nothing in the controller or
	// the witness read the firewall at all -- the guard could be flushed on the
	// host and every certification still passed, which made A6's fence leg an
	// unbacked claim.
	// nft-guard-tool is the measurement itself. Pinning the guard fingerprint
	// while leaving the script that produces it unpinned would be a control
	// with an unguarded input: replacing guard-fingerprint.py with one line
	// printing the expected hex keeps both halves green forever, which is the
	// precise failure this entry exists to end. It is the same precondition as
	// `nft flush ruleset`, and that one is treated as worth defending.
	// cni-conflist pins 10-sjo-runner-pool.conflist (homelab#875 I3), where
	// portIsolation and macspoofchk live and which places workers on sjo-canary0.
	required := map[string]bool{
		"containerd-config": false, "kata-config": false, "kata-artifacts": false,
		"nft-guard": false, "nft-guard-tool": false, "cni-conflist": false,
	}
	// A measured target with no age bound is the fail-open shape this whole
	// entry exists to prevent, so it is refused at startup rather than trusted
	// to be set correctly. Listed by purpose: a future measured fingerprint has
	// to be added here deliberately, and a static one is unaffected.
	measured := map[string]bool{"nft-guard": true}
	seenPaths := make(map[string]bool)
	for _, fingerprint := range config.RuntimeFingerprints {
		if !filepath.IsAbs(fingerprint.Path) || len(fingerprint.SHA256) != 64 || strings.Trim(fingerprint.SHA256, "0123456789abcdef") != "" {
			return nil, errors.New("host storage collector runtime fingerprint is invalid")
		}
		// An all-zero digest is the placeholder the reviewable bundle ships
		// with, and it is well-formed hex of the right length, so every check
		// above accepts it. verifyRuntimeFingerprints would refuse it later,
		// once the file is read -- but by then it looks like tampering rather
		// than an unmeasured host, and it fails at the first certification
		// instead of at startup. Refuse it here, by name.
		if strings.Trim(fingerprint.SHA256, "0") == "" {
			return nil, fmt.Errorf("runtime fingerprint %q is still the all-zero placeholder; measure it on the host first", fingerprint.Purpose)
		}
		if _, ok := required[fingerprint.Purpose]; !ok || required[fingerprint.Purpose] || seenPaths[fingerprint.Path] {
			return nil, errors.New("host storage collector runtime fingerprint purpose is missing or duplicated")
		}
		if measured[fingerprint.Purpose] && fingerprint.MaxAgeSeconds <= 0 {
			return nil, fmt.Errorf("runtime fingerprint %q is continuously measured and needs max_age_seconds; without it a stopped measurer certifies forever", fingerprint.Purpose)
		}
		// An upper limit as well as a lower one. Rejecting only <= 0 leaves the
		// same fail-open shape one level down: a bound of 315360000 is present,
		// positive, passes every check and every test asserting "has a bound",
		// and is satisfied by any mtime a running host can produce -- a
		// disabled check wearing a configured one's clothes. The ceiling is
		// generous against any plausible measurement interval and still refuses
		// a value that could only have been chosen to switch the bound off.
		if measured[fingerprint.Purpose] && fingerprint.MaxAgeSeconds > maxMeasuredAgeSeconds {
			return nil, fmt.Errorf("runtime fingerprint %q has max_age_seconds %d, above the %d ceiling; a bound that large accepts any mtime and is not a bound",
				fingerprint.Purpose, fingerprint.MaxAgeSeconds, maxMeasuredAgeSeconds)
		}
		if !measured[fingerprint.Purpose] && fingerprint.MaxAgeSeconds != 0 {
			return nil, fmt.Errorf("runtime fingerprint %q is a static target and must not carry max_age_seconds", fingerprint.Purpose)
		}
		required[fingerprint.Purpose] = true
		seenPaths[fingerprint.Path] = true
	}
	for _, present := range required {
		if !present {
			return nil, errors.New("host storage collector runtime fingerprint set is incomplete")
		}
	}
	return &HostStorageCollector{config: config, run: runHostCommand, rereadDelay: defaultIdentityRereadDelay}, nil
}

type containerdContainerInfo struct {
	ID          string `json:"ID"`
	SnapshotKey string `json:"SnapshotKey"`
	Snapshotter string `json:"Snapshotter"`
	// SandboxID is how containerd 2.3 records which Pod sandbox owns a
	// container. It is a TOP-LEVEL field on `ctr containers info`, not a label,
	// and it is set on every container whose CRI kind is `container` and empty
	// on every sandbox. See resolveSandboxID for why reading only the label was
	// wrong.
	SandboxID string            `json:"SandboxID"`
	Labels    map[string]string `json:"Labels"`
}

func (c *HostStorageCollector) ListWritableStorage(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	if c == nil || ctx == nil || namespace != c.config.ProcessIdentitySource.config.Namespace || ownerUID != c.config.ProcessIdentitySource.config.OwnerUID {
		return WitnessScope{Complete: false}, errors.New("host storage collector scope mismatch")
	}
	if err := c.verifyRuntimeFingerprints(); err != nil {
		// Marked so the witness log and the socket reason code can separate a
		// fingerprint refusal from a storage refusal. They send an operator to
		// different places: the host's files or the measuring timer, versus
		// containerd.
		return WitnessScope{Complete: false}, &fingerprintFailure{cause: err}
	}
	scope, err := c.inventoryPass(ctx, namespace, ownerUID)
	if !errors.Is(err, errUnknownWorkerIdentity) {
		return scope, err
	}
	// ONE MORE PASS, AND ONLY FOR THIS REFUSAL (homelab#1213).
	//
	// A pass reads the CRI Pod list first and the snapshot list second. A worker
	// created between the two reads has a writable snapshot and a containerd
	// record naming its sandbox, but no ledger entry yet, because the Pod list
	// that would have recorded it was taken a moment too early. During a burst
	// of creates that is ordinary: on 2026-10-01 it refused the second create of
	// three bursts inside fifteen minutes, and the controller counted each one as
	// a provisioning failure until its breaker opened.
	//
	// So the whole pass runs once more, starting from a fresh Pod list. A worker
	// that was only racing the first read is in that list now and is attributed
	// normally. Storage that truly has no owner is still unowned on the second
	// pass and is refused with the same error: this re-read changes WHEN the
	// answer is taken, never what counts as an owner.
	//
	// Once, not a loop. A pass that keeps finding storage it cannot attribute is
	// not observing a race, and the refusal is the answer.
	c.log.IdentityReread(WitnessReasonStorage)
	if c.rereadDelay > 0 {
		if err := sleepContext(ctx, c.rereadDelay); err != nil {
			return WitnessScope{Complete: false}, err
		}
	}
	return c.inventoryPass(ctx, namespace, ownerUID)
}

// errUnknownWorkerIdentity is the refusal for writable storage on the dedicated
// snapshotter whose container names a sandbox the identity ledger does not
// hold. ListWritableStorage re-reads once on exactly this error and nothing
// else; its text is unchanged from before it was a sentinel, so the witness log
// and the socket's byte-count failure read as they always have.
var errUnknownWorkerIdentity = errors.New("dedicated snapshotter container has no durable worker identity")

// inventoryPass is one complete storage inventory: a fresh CRI Pod list into
// the identity ledger, then the dedicated snapshotter's writable snapshots
// attributed against it. It takes and releases the process collector's lock
// itself, so ListWritableStorage can run it twice.
func (c *HostStorageCollector) inventoryPass(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	// Refresh the identity ledger from CRI first. This makes an unknown
	// dedicated-snapshotter container a hard failure instead of inventing owner
	// labels from containerd fields.
	if _, err := c.config.ProcessIdentitySource.ListProcesses(ctx, namespace, ownerUID); err != nil {
		return WitnessScope{Complete: false}, err
	}
	processes := c.config.ProcessIdentitySource
	processes.mu.Lock()
	defer processes.mu.Unlock()
	ledger, err := processes.loadLedger()
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	// THE DEDICATED SNAPSHOTTER IS THE SUBJECT, NOT THE NODE'S CONTAINER SET.
	//
	// This used to list every container on the node and exec
	// `ctr containers info <id>` once per container to find the handful on the
	// dedicated snapshotter. Measured on MS-A2 2026-09-21: 114 containers, 5.1 s,
	// paid on every inventory including inside worker creation, and it blew the
	// witness timeout (#845). The cost grew with every unrelated workload.
	//
	// The listing this collector ALREADY made is the better starting point: the
	// dedicated snapshotter's own snapshots. Writable storage is exactly an
	// `Active` snapshot there, and containerd's CRI plugin keys a container's
	// snapshot on the container id -- measured on the same host, where every
	// container's `SnapshotKey` equalled its `ID`. So the Active keys ARE the
	// candidate container set, and it is bounded by the pool (three slots), not
	// by the node.
	//
	// It is also STRICTER than what it replaces, in the direction that matters.
	// The old loop asked "does this container have a ledger entry?" and never
	// asked the converse, so a writable snapshot with no owning container was
	// invisible (#845, Important 1). Here every Active key must resolve to a
	// ledger entry -- either one that already claims it, or the container named
	// after it -- and anything else fails closed.
	//
	// WHAT IT NO LONGER SEES: a container recorded on the dedicated snapshotter
	// whose snapshot no longer exists. That container holds no writable storage,
	// which is the only thing this collector witnesses, and a live one is still
	// caught by the process leg.
	writable, unpacking, allKeys, err := c.snapshotKeys(ctx)
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	// SURFACED, not silently tolerated. A pull landing on the dedicated
	// snapshotter is expected and harmless, but it is also the state in which
	// this collector sees storage it deliberately does not account for, and an
	// operator reading a pool that is briefly doing nothing deserves to know
	// which of the two it is.
	c.log.Unpacking(WitnessReasonStorage, len(unpacking))
	existing := make(map[string]bool, len(allKeys))
	for _, key := range allKeys {
		if key == "" || strings.ContainsAny(key, "\x00\r\n") || existing[key] {
			return WitnessScope{Complete: false}, errors.New("containerd returned an invalid snapshot listing")
		}
		existing[key] = true
	}
	// A key the ledger already attributes needs no exec at all. That is not an
	// optimisation bolted on top: it is what keeps an ORPHAN witnessable. A
	// worker whose Pod and containerd record are gone while its writable
	// snapshot survives is the exact thing this collector is here to report, and
	// asking containerd to describe a container that no longer exists would turn
	// it into an error instead of a witness. The ledger is the durable identity;
	// once it has claimed a snapshot key, the key stays attributed.
	//
	// In steady state this makes the whole inventory ONE exec.
	attributed := make(map[string]bool)
	for _, entry := range ledger.Entries {
		for _, key := range entry.SnapshotKeys {
			attributed[key] = true
		}
	}
	changed := false
	skipped := 0
	for _, key := range writable {
		if attributed[key] {
			continue
		}
		if !validContainerdID(key) {
			// Not a skip. A writable snapshot on the dedicated snapshotter whose
			// key cannot be a container identity is storage this witness cannot
			// account for, which is the refusal this whole collector exists for.
			return WitnessScope{Complete: false},
				fmt.Errorf("dedicated snapshotter holds a writable snapshot whose key is %s, not a containerd container identity", snapshotKeyShape(key))
		}
		output, err := c.command(ctx, "containers", "info", key)
		if err != nil {
			// A container that vanished between the listing and this info is
			// ordinary on this node: workers are one-job and tear down mid-pass,
			// so `ctr: container "..." not found` arrives and used to fence the
			// whole pool. Tolerating it is correctness under churn, not leniency
			// -- and the tolerance is proven, not assumed: the SNAPSHOT must be
			// gone too. See vanishedFromDedicatedSnapshots for why the stderr
			// text is never the evidence, and why the snapshot rather than the
			// container is what gets re-read.
			vanished, listErr := c.vanishedFromDedicatedSnapshots(ctx, key)
			if listErr != nil {
				// The re-list is the proof. Without it nothing distinguishes
				// "gone" from "containerd is unwell", so the ORIGINAL failure
				// stands -- wrapped, so the tool, exit status and stderr still
				// reach the witness log.
				return WitnessScope{Complete: false},
					fmt.Errorf("containerd container inspection failed and the re-list could not confirm it vanished: %w", err)
			}
			if !vanished {
				// The snapshot is still there and nothing owns it. That is the
				// orphaned-storage case, and it is a refusal rather than the
				// original error, because the original error is about a lookup
				// while this is about the host.
				return WitnessScope{Complete: false},
					fmt.Errorf("a writable snapshot on the dedicated snapshotter has no owning container: %w", err)
			}
			skipped++
			// One line per skip, not one per pass. The count and the bound
			// travel with it, so a log filling with these says how close the
			// node is to the refusal below rather than only that churn
			// happened. The id is containerd's own 64-hex identity: not a name,
			// not a namespace, and nothing about another workload's contents.
			c.log.Skipped(WitnessReasonStorage, key, skipped, maxVanishedContainers)
			if skipped > maxVanishedContainers {
				// Past this the pass is no longer witnessing a stable set. An
				// unbounded skip loop would keep declaring a complete scope
				// over a snapshot set it never saw whole.
				return WitnessScope{Complete: false},
					errors.New("containerd container set is churning faster than one inventory pass can witness")
			}
			continue
		}
		var info containerdContainerInfo
		if err := json.Unmarshal(output, &info); err != nil || info.ID != key || info.Labels == nil {
			return WitnessScope{Complete: false}, errors.New("containerd container information is malformed")
		}
		if info.Snapshotter != c.config.DedicatedSnapshotter || info.SnapshotKey != key {
			// The container answering to this id disclaims the snapshot. Two
			// answers about who owns writable storage is not a case to pick a
			// winner in.
			return WitnessScope{Complete: false},
				errors.New("containerd container does not claim the dedicated writable snapshot named after it")
		}
		sandboxID, err := resolveSandboxID(key, info, ledger)
		if err != nil {
			return WitnessScope{Complete: false}, err
		}
		entry, ok := ledger.Entries[sandboxID]
		if !ok {
			return WitnessScope{Complete: false}, errUnknownWorkerIdentity
		}
		if !containsString(entry.SnapshotKeys, key) {
			entry.SnapshotKeys = append(entry.SnapshotKeys, key)
			sort.Strings(entry.SnapshotKeys)
			ledger.Entries[sandboxID] = entry
			changed = true
		}
	}
	if c.pruneVanishedIdentities(ledger, processes, existing, skipped) {
		changed = true
	}
	if changed {
		if err := processes.saveLedger(ledger); err != nil {
			return WitnessScope{Complete: false}, err
		}
	}
	witnesses := make([]PhysicalWitness, 0, len(ledger.Entries))
	for _, entry := range ledger.Entries {
		present := false
		for _, key := range entry.SnapshotKeys {
			present = present || existing[key]
		}
		if present {
			witnesses = append(witnesses, PhysicalWitness{Namespace: entry.Namespace, Name: entry.Name, UID: entry.PodUID,
				Labels: processes.labels(entry), RuntimeClassName: processes.config.RuntimeClass})
		}
	}
	sort.Slice(witnesses, func(left, right int) bool { return witnesses[left].Name < witnesses[right].Name })
	return WitnessScope{Complete: true, Items: witnesses}, nil
}

// VerifyRuntimeFingerprints runs the same comparison every request runs, so the
// witness can state at startup whether the host it came up on is the host it
// was measured against. It changes no behaviour: a mismatch still fences at the
// first request, this only makes it visible before one.
func (c *HostStorageCollector) VerifyRuntimeFingerprints() error {
	return c.verifyRuntimeFingerprints()
}

// RuntimeFingerprintCount reports how many targets the startup line covers.
func (c *HostStorageCollector) RuntimeFingerprintCount() int {
	if c == nil {
		return 0
	}
	return len(c.config.RuntimeFingerprints)
}

func (c *HostStorageCollector) verifyRuntimeFingerprints() error {
	for _, fingerprint := range c.config.RuntimeFingerprints {
		info, err := os.Lstat(fingerprint.Path)
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("dedicated runtime fingerprint target is unavailable or unsafe")
		}
		file, err := os.Open(fingerprint.Path)
		if err != nil {
			return errors.New("dedicated runtime fingerprint target cannot be read")
		}
		digest := sha256.New()
		_, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || fmt.Sprintf("%x", digest.Sum(nil)) != fingerprint.SHA256 {
			return errors.New("dedicated runtime fingerprint changed")
		}
		if err := checkFingerprintFreshness(fingerprint, info.ModTime(), time.Now()); err != nil {
			return err
		}
	}
	return nil
}

// checkFingerprintFreshness bounds the age of a continuously measured
// fingerprint. Split out from verifyRuntimeFingerprints so the clock is an
// argument: a test can pin "now" instead of sleeping, and the skew branch below
// is reachable without setting a file's mtime into the future on a real disk.
func checkFingerprintFreshness(fingerprint RuntimeFingerprint, modified, now time.Time) error {
	if fingerprint.MaxAgeSeconds <= 0 {
		return nil
	}
	age := now.Sub(modified)
	// A future mtime refuses rather than reading as maximally fresh. Negative
	// age would otherwise satisfy any bound, so clock skew or a touched file
	// would be the easiest way to defeat this check -- and it is the direction
	// an accident takes, not just an attacker.
	if age < 0 {
		return errors.New("dedicated runtime fingerprint is dated in the future")
	}
	if age > time.Duration(fingerprint.MaxAgeSeconds)*time.Second {
		// Deliberately does not say "changed". The guard may be perfectly
		// intact; what failed is that nobody has measured it recently enough
		// for this file to be evidence either way.
		return errors.New("dedicated runtime fingerprint is stale; the host measurement is not running")
	}
	return nil
}

// resolveSandboxID maps a dedicated-snapshotter container to the durable worker
// identity that owns it.
//
// This used to read the io.cri-containerd.sandbox-id LABEL and nothing else.
// That label is present on NONE of the containers this host runs -- measured
// over 40 real containers on the MS-A2 under containerd 2.3.2-k3s2, zero
// carried it. containerd records the ownership as a TOP-LEVEL SandboxID field
// instead: set on all 23 containers whose io.cri-containerd.kind is
// `container`, empty on the 17 whose kind is `sandbox`. So the first real
// worker's app containers would every one of them have looked ownerless and
// fenced the pool, while the code read as if it had an ownership check.
//
// The label is still read after the field, because a containerd that sets the
// label and not the field is a shape this must keep working on, and dropping
// the older reader to adopt the newer one would trade one silent failure for
// another. When both are present they must AGREE: two different answers about
// which sandbox owns a writable snapshot is not a case to pick a winner in.
func resolveSandboxID(containerID string, info containerdContainerInfo, ledger processLedger) (string, error) {
	label := info.Labels[sandboxIDLabel]
	if info.SandboxID != "" && label != "" && info.SandboxID != label {
		return "", errors.New("containerd reports two different sandbox owners for one container")
	}
	// A sandbox is its own worker identity and carries no SandboxID of its own,
	// so its ledger entry is keyed on the container ID directly.
	if _, ok := ledger.Entries[containerID]; ok {
		return containerID, nil
	}
	if info.SandboxID != "" {
		return info.SandboxID, nil
	}
	return label, nil
}

// maxVanishedContainers bounds how many mid-pass disappearances one inventory
// may absorb. Eight is a judgement: the node churns a handful of containers a
// minute under the legacy CronJob fleets, and a pass that loses more than that
// is not observing a set stable enough to certify. Raising it trades witnessing
// accuracy for fewer fences.
const maxVanishedContainers = 8

// pruneVanishedIdentities drops ledger entries for workers that are provably
// gone on all three legs: no Pod sandbox, no Kata runtime process, no writable
// snapshot. Nothing did this before, so `identities.json` only ever grew.
//
// PROVABLY is the whole bar, and it is why this takes the skip count. An entry
// is dropped only when THIS pass observed every leg whole:
//
//   - skipped > 0 means the snapshot set churned mid-pass, so absence of a key
//     is not evidence the storage is gone.
//   - liveObserved false means the process leg did not finish, so absence from
//     liveSandboxes says nothing at all.
//
// Either way the ledger is left exactly as it was. A stale entry costs a little
// file; a wrongly pruned one erases the identity that makes an orphan
// attributable, which is the thing the ledger exists for.
//
// The caller holds the process collector's mutex, which is what makes reading
// liveSandboxes here safe.
func (c *HostStorageCollector) pruneVanishedIdentities(ledger processLedger, processes *HostProcessCollector,
	existing map[string]bool, skipped int) bool {
	if skipped > 0 || !processes.liveObserved {
		return false
	}
	pruned := 0
	for sandboxID, entry := range ledger.Entries {
		if processes.liveSandboxes[sandboxID] {
			continue
		}
		storagePresent := false
		for _, key := range entry.SnapshotKeys {
			storagePresent = storagePresent || existing[key]
		}
		if storagePresent {
			continue
		}
		delete(ledger.Entries, sandboxID)
		pruned++
		c.log.Pruned(WitnessReasonStorage, sandboxID, pruned)
	}
	return pruned > 0
}

// vanishedFromDedicatedSnapshots asks containerd once more whether a writable
// snapshot it just failed to attribute is still there.
//
// WHY A RE-LIST AND NOT THE STDERR. `ctr` says
// `container "a68343d9...": not found` on a vanished container, and reading
// that would make the tolerance depend on one tool's error prose -- a denylist
// of strings that is never finished, and which a locale, a version bump or an
// unrelated failure quietly widens into "skip anything". The listing is the
// authority on what exists, so the listing is what is asked.
//
// WHY THE SNAPSHOT AND NOT THE CONTAINER. The snapshot is the subject: a
// container whose id is absent tells you the lookup was honest, but it does not
// tell you whether the writable storage named after it is still on the host.
// Only the snapshot going away with it proves a teardown rather than an orphan.
//
// FAIL CLOSED, BOTH WAYS. A re-list that FAILS proves nothing and is reported
// as an error by the caller. A re-list that SUCCEEDS and still names the key
// means writable storage exists that nothing owns, which is a refusal. Only
// "the listing worked and the key is gone" is a skip.
func (c *HostStorageCollector) vanishedFromDedicatedSnapshots(ctx context.Context, key string) (bool, error) {
	_, _, current, err := c.snapshotKeys(ctx)
	if err != nil {
		return false, err
	}
	for _, existing := range current {
		if existing == key {
			return false, nil
		}
	}
	return true, nil
}

// snapshotKeys lists the writable snapshots of the dedicated snapshotter.
//
// `ctr snapshots list` takes NO FLAGS on containerd 2.3 -- `--quiet` is not one
// of them, and passing it exits 1 with `flag provided but not defined: -quiet`
// rather than degrading to a full listing. Every inventory therefore failed and
// the controller fenced the pool on `trusted backend inventory failed` each
// cycle. `ctr containers list --quiet` DOES exist on the same binary, which is
// why only this one call was wrong.
// It returns three sets: the WRITABLE keys (`Active` rows a container owns),
// the UNPACKING keys (`Active` rows containerd's image unpacker owns while a
// pull is in flight), and the ATTRIBUTABLE keys — every key except the
// unpacking ones.
//
// The split is what lets the inventory above start from the snapshots. An
// `Active` row is a read-write layer; a `Committed` row is an image layer that
// no container owns; an unpack row is neither a container's storage nor an
// orphan, and asking either question about it fences the pool for the length of
// a multi-gigabyte pull.
func (c *HostStorageCollector) snapshotKeys(ctx context.Context) (writable, unpacking, attributable []string, err error) {
	output, err := c.command(ctx, "snapshots", "--snapshotter", c.config.DedicatedSnapshotter, "list")
	if err != nil {
		return nil, nil, nil, err
	}
	return parseSnapshotTable(string(output))
}

// snapshotKinds are containerd's three snapshot kinds, spelled as
// `ctr snapshots list` prints them. The set is closed on purpose: a kind this
// does not know is a format change or a containerd this was never read against,
// and guessing whether it is writable would decide the pool's fence on a string
// nobody measured.
var snapshotKinds = map[string]bool{"Active": true, "Committed": true, "View": true}

// unpackKeyPrefix and unpackKeyPattern describe the one snapshot key on this
// snapshotter that CONTAINS A SPACE.
//
// Read out of the pinned containerd v2.3.2 source rather than guessed:
//
//	core/snapshots/snapshotter.go
//	    UnpackKeyPrefix = "extract"
//	    UnpackKeyFormat = UnpackKeyPrefix + "-%s %s"
//	core/unpack/unpacker.go
//	    key = fmt.Sprintf(snapshots.UnpackKeyFormat, uniquePart(), chainID)
//	    func uniquePart() string {  // "%d-%s": Nanosecond(), base64url(3 bytes)
//
// So a key is `extract-<1..9 digits>-<4 base64url chars> <chainID digest>`, and
// the snapshotter holds it `Active` for the whole length of a layer extraction.
//
// WHY THIS IS A POOL-WIDE AVAILABILITY MATTER, not a parsing nicety. CRI picks
// the snapshotter from the runtime handler at pull time as well as at
// container-create time, so any image pulled for the pool's handler unpacks
// HERE. Before this, the two row shapes an extraction produces both reached a
// hard failure -- a root-layer row looked like an Active key that is not a
// container id, and a child-layer row looked like a table row with too many
// fields -- so the witness returned Complete:false, and admission stayed fenced
// for the whole pull, with an error naming orphaned storage rather than a pull.
const unpackKeyPrefix = "extract"

var unpackKeyPattern = regexp.MustCompile(`^extract-[0-9]{1,9}-[A-Za-z0-9_-]{4} [a-z0-9]+:[0-9a-f]{32,128}$`)

// snapshotKeyShape names what a key LOOKS like, for an error message. The raw
// key never reaches a log or an error: it is attacker-adjacent (any workload
// that can create a snapshot on this snapshotter names it) and unbounded in
// length. A shape class is enough to send an operator to the right place.
func snapshotKeyShape(key string) string {
	switch {
	case unpackKeyPattern.MatchString(key):
		return "an image unpack key"
	case validContainerdID(key):
		return "a containerd container id"
	case strings.HasPrefix(key, "sha256:"):
		return "an image layer digest"
	case strings.HasPrefix(key, unpackKeyPrefix+"-"):
		return "an unpack-like key that does not match containerd's unpack format"
	case strings.ContainsAny(key, " \t"):
		return "a key containing whitespace"
	default:
		return "an unrecognised key"
	}
}

// parseSnapshotTable reads the KEY column out of `ctr snapshots list`.
//
// The output is a padded three-column table with trailing spaces, and PARENT is
// EMPTY for a root snapshot -- so a row can carry two fields or three and the
// key is simply the first. Worker snapshots are the `Active` rows, whose keys
// are CRI container IDs rather than sha256: digests, so nothing here may assume
// a digest shape.
//
// The header is asserted rather than skipped. A column reordering or rename
// would otherwise be read as data: the parser would happily return `KEY` and
// then whatever now sits in the first column, which is an inventory that looks
// complete and names the wrong snapshots. A format change must fence the pool,
// not parse.
// EVERY ROW IS PARSED FROM THE RIGHT, because only the right-hand end is fixed.
// KIND is always the last field. PARENT is the field before it WHEN PRESENT and
// is empty for a root snapshot. KEY is whatever is left -- and KEY is the one
// column that can carry a space, because containerd's image unpacker names its
// in-flight snapshots `extract-<unique> <chainID>`.
//
// Reading left to right cannot express that. `fields[0]` is half an unpack key,
// and a field COUNT cannot tell a three-field root-unpack row from a
// three-field ordinary row, or accept the four-field child-unpack row at all.
// Both shapes previously reached a hard failure and fenced the pool for the
// length of a pull.
//
// The two-token key is recognised by its prefix, not by arithmetic: a
// single-token key on this snapshotter is a container id or an image digest,
// and neither can begin with `extract-`. Anything that begins with `extract-`
// and then fails the exact format is NOT quietly accepted -- it falls through
// to the writable set, where the container-id gate refuses it by shape.
func parseSnapshotTable(output string) (writable, unpacking, attributable []string, err error) {
	writable, unpacking, attributable = []string{}, []string{}, []string{}
	headerSeen := false
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if !headerSeen {
			if len(fields) != 3 || fields[0] != "KEY" || fields[1] != "PARENT" || fields[2] != "KIND" {
				return nil, nil, nil, errors.New("containerd snapshot listing does not carry the expected KEY PARENT KIND header")
			}
			headerSeen = true
			continue
		}
		kind := fields[len(fields)-1]
		if !snapshotKinds[kind] {
			return nil, nil, nil, errors.New("containerd snapshot listing carries an unknown snapshot kind")
		}
		rest := fields[:len(fields)-1]
		keyTokens := 1
		if len(rest) > 0 && strings.HasPrefix(rest[0], unpackKeyPrefix+"-") {
			keyTokens = 2
		}
		// The key, then AT MOST one PARENT token. Anything wider is a table this
		// parser has not read before, and a format change must fence rather than
		// parse.
		if len(rest) < keyTokens || len(rest) > keyTokens+1 {
			return nil, nil, nil, errors.New("containerd snapshot listing row does not carry a key and a kind")
		}
		key := strings.Join(rest[:keyTokens], " ")
		if kind == "Active" && unpackKeyPattern.MatchString(key) {
			// A PULL IN PROGRESS. Not a container's storage and not an orphan:
			// containerd owns it, it is deleted or committed when the layer
			// finishes, and no ledger entry will ever name it. It is kept out of
			// the attributable set so nothing can attribute it, and out of the
			// writable set so nothing demands a container for it.
			unpacking = append(unpacking, key)
			continue
		}
		attributable = append(attributable, key)
		if kind == "Active" {
			writable = append(writable, key)
		}
	}
	// A header with no rows is a real, valid, empty listing -- a pool with no
	// workers. No header at all is a command that printed something else.
	if !headerSeen {
		return nil, nil, nil, errors.New("containerd snapshot listing has no header; the command printed something else")
	}
	return writable, unpacking, attributable, nil
}

func (c *HostStorageCollector) command(ctx context.Context, arguments ...string) ([]byte, error) {
	base := []string{"--address", c.config.ContainerdAddress, "--namespace", c.config.ContainerdNamespace}
	return c.run(ctx, c.config.CTRExecutable, append(base, arguments...)...)
}

func validContainerdID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
