package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testContainerA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

// testImageLayer is a Committed row: an image layer nothing owns. It is in the
// fixture because the inventory must walk past it without asking containerd to
// attribute it -- a Committed key is not writable storage and has no container.
// The value is a real key, copied out of capturedSnapshotTable.
const testImageLayer = "sha256:68b14877de21ededb2711dcda9ab929d66a57f9fe2d6abb9a1593372648ccea8"

// testChainID is a real chain ID from the same capture. It is the second half
// of a MODELLED unpack key; the key as a whole never appeared on the host.
const testChainID = "sha256:b9a65b3c65ab22d490085bd0bf5490e2409da8748b406870f2463bdc41cd6795"

func storageCollectorFixture(t *testing.T) (*HostStorageCollector, *string, map[string]containerdContainerInfo, *string) {
	t.Helper()
	processes, criOutput, _ := processCollectorFixture(t)
	*criOutput = podList(testSandboxA, processes.config.RuntimeHandler)
	artifactDirectory := filepath.Dir(processes.config.LedgerFile)
	if err := os.MkdirAll(artifactDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	contents := []byte("pinned runtime configuration")
	var fingerprints []RuntimeFingerprint
	for _, purpose := range []string{"containerd-config", "kata-config", "kata-artifacts", "nft-guard", "nft-guard-tool", "cni-conflist"} {
		artifact := filepath.Join(artifactDirectory, purpose)
		if err := os.WriteFile(artifact, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		entry := RuntimeFingerprint{Purpose: purpose, Path: artifact, SHA256: fmt.Sprintf("%x", sha256.Sum256(contents))}
		// nft-guard is written by a host-side timer rather than being a static
		// file, so it carries an age bound and the others must not.
		if purpose == "nft-guard" {
			entry.MaxAgeSeconds = 120
		}
		fingerprints = append(fingerprints, entry)
	}
	collector, err := NewHostStorageCollector(HostStorageConfig{
		CTRExecutable: "/usr/local/bin/ctr", ContainerdAddress: "/run/k3s/containerd/containerd.sock",
		ContainerdNamespace: "k8s.io", DedicatedSnapshotter: "sjo-devmapper", ProcessIdentitySource: processes,
		RuntimeFingerprints: fingerprints,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot keys ARE the container ids. That is containerd's CRI plugin's
	// own convention and it was measured on the host: every container in
	// ctr-capture-2026-09-21.txt reports `SnapshotKey` equal to its `ID`.
	snapshots := snapshotTable(activeSnapshot(testSandboxA), activeSnapshot(testContainerA), committedSnapshot(testImageLayer))
	infos := map[string]containerdContainerInfo{
		testSandboxA: {ID: testSandboxA, SnapshotKey: testSandboxA, Snapshotter: "sjo-devmapper", Labels: map[string]string{}},
		// The real containerd 2.3 shape: the app container's owner arrives in
		// the top-level SandboxID field and the legacy label is absent, which
		// is what every container on the MS-A2 actually looks like.
		testContainerA: {ID: testContainerA, SnapshotKey: testContainerA, Snapshotter: "sjo-devmapper",
			SandboxID: testSandboxA, Labels: map[string]string{}},
	}
	collector.run = func(_ context.Context, executable string, arguments ...string) ([]byte, error) {
		if executable != collector.config.CTRExecutable || len(arguments) < 6 || arguments[0] != "--address" ||
			arguments[1] != collector.config.ContainerdAddress || arguments[2] != "--namespace" || arguments[3] != "k8s.io" {
			return nil, errors.New("unexpected containerd command")
		}
		suffix := strings.Join(arguments[4:], " ")
		switch suffix {
		// `containers list` is DELIBERATELY not answered here. The inventory
		// costs one exec per container on the node when it walks that listing
		// (#845: 114 containers, 5.1 s, past the witness timeout), so a
		// regression that reinstates it must fail the suite rather than pass
		// quietly on a fixture that happens to know the answer.
		case "snapshots --snapshotter sjo-devmapper list":
			return []byte(snapshots), nil
		}
		if len(arguments) == 7 && arguments[4] == "containers" && arguments[5] == "info" {
			value, ok := infos[arguments[6]]
			if !ok {
				return nil, errors.New("unknown container")
			}
			return json.Marshal(value)
		}
		return nil, errors.New("unexpected containerd command")
	}
	return collector, criOutput, infos, &snapshots
}

func TestHostStorageCollectorRejectsChangedRuntimeFingerprint(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	path := collector.config.RuntimeFingerprints[0].Path
	if err := os.WriteFile(path, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("changed runtime configuration fingerprint was accepted")
	}
}

func TestHostStorageCollectorPersistsWritableOrphanIdentity(t *testing.T) {
	collector, criOutput, _, snapshots := storageCollectorFixture(t)
	current, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !current.Complete || len(current.Items) != 1 || current.Items[0].Labels[LabelLeaseID] != testLeaseA {
		t.Fatalf("current writable storage was not witnessed: %+v %v", current, err)
	}
	*criOutput = `{"items":[]}`
	collector.run = storageRunnerWithoutContainers(collector, snapshots)
	orphan, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || len(orphan.Items) != 1 || orphan.Items[0].UID != "pod-uid-a" {
		t.Fatalf("orphan writable snapshots lost durable identity: %+v %v", orphan, err)
	}
	*snapshots = snapshotTable(committedSnapshot(testImageLayer))
	absent, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !absent.Complete || len(absent.Items) != 0 {
		t.Fatalf("removed writable snapshots remained present: %+v %v", absent, err)
	}
}

func storageRunnerWithoutContainers(collector *HostStorageCollector, snapshots *string) hostCommandRunner {
	return func(_ context.Context, executable string, arguments ...string) ([]byte, error) {
		if executable != collector.config.CTRExecutable || len(arguments) < 6 {
			return nil, errors.New("unexpected containerd command")
		}
		switch strings.Join(arguments[4:], " ") {
		case "containers list --quiet":
			return []byte{}, nil
		case "snapshots --snapshotter sjo-devmapper list":
			return []byte(*snapshots), nil
		default:
			return nil, errors.New("unexpected containerd command")
		}
	}
}

func TestHostStorageCollectorRejectsUnownedOrMalformedDedicatedStorage(t *testing.T) {
	collector, _, infos, _ := storageCollectorFixture(t)
	unknownSandbox := strings.Repeat("d", 64)
	infos[testContainerA] = containerdContainerInfo{ID: testContainerA, SnapshotKey: testContainerA, Snapshotter: "sjo-devmapper",
		Labels: map[string]string{sandboxIDLabel: unknownSandbox}}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("unowned dedicated-snapshotter container was accepted")
	}
	infos[testContainerA] = containerdContainerInfo{ID: testContainerA, Snapshotter: "sjo-devmapper", Labels: map[string]string{sandboxIDLabel: testSandboxA}}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("dedicated container without a writable snapshot was accepted")
	}
}

func TestHostStorageCollectorFailsClosedWhenEitherInventoryFails(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	collector.config.ProcessIdentitySource.run = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("CRI unavailable")
	}
	if result, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("CRI failure produced a complete storage scope")
	}
	collector, _, _, _ = storageCollectorFixture(t)
	collector.run = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("containerd unavailable")
	}
	if result, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("containerd failure produced a complete storage scope")
	}
}

// host-witness-config.json ships three all-zero SHA-256 placeholders, and an
// all-zero digest is well-formed hex of the right length -- every other check
// in NewHostStorageCollector accepts it. verifyRuntimeFingerprints would refuse
// it eventually, once the file is read, but by then it reads as tampering
// rather than as a host nobody measured, and it fails at the first
// certification instead of at startup.
//
// The positive control matters here: a constructor that rejected every
// fingerprint would pass the placeholder case on its own.
func TestHostStorageCollectorRejectsPlaceholderRuntimeFingerprints(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	base := collector.config

	if _, err := NewHostStorageCollector(base); err != nil {
		t.Fatalf("measured fingerprints were rejected: %v", err)
	}

	for index := range base.RuntimeFingerprints {
		config := base
		config.RuntimeFingerprints = append([]RuntimeFingerprint(nil), base.RuntimeFingerprints...)
		purpose := config.RuntimeFingerprints[index].Purpose
		config.RuntimeFingerprints[index].SHA256 = strings.Repeat("0", 64)
		_, err := NewHostStorageCollector(config)
		if err == nil {
			t.Fatalf("accepted the all-zero placeholder for %q", purpose)
		}
		if !strings.Contains(err.Error(), purpose) {
			t.Fatalf("refusal for %q does not name it: %v", purpose, err)
		}
	}
}

// --- the nft guard's age bound -----------------------------------------------
//
// The guard fingerprint is the one target that is CONTINUOUSLY MEASURED: a
// host-side timer re-reads the live ruleset and rewrites the file. That makes
// its content evidence only about the moment it was written, so the content
// hash alone cannot separate an intact guard from a measurer that died an hour
// ago holding its last good answer. These pin the half that can tell them
// apart.

func TestGuardFingerprintIsRefusedWhenStale(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	guard := guardFingerprint(t, collector)

	// One second past the bound, not a wild value: the boundary is where a
	// wrong comparison operator hides.
	stale := time.Now().Add(-time.Duration(guard.MaxAgeSeconds)*time.Second - time.Second)
	if err := os.Chtimes(guard.Path, stale, stale); err != nil {
		t.Fatal(err)
	}
	err := collector.verifyRuntimeFingerprints()
	if err == nil {
		t.Fatal("a stale guard attestation was accepted; a stopped measurer would certify forever")
	}
	// The distinction matters operationally: the guard may be perfectly fine
	// and the measurement stopped. An operator told "changed" looks at the
	// firewall instead of at the timer.
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("refusal does not say the measurement is stale: %v", err)
	}
}

func TestGuardFingerprintIsAcceptedInsideTheBound(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	guard := guardFingerprint(t, collector)

	fresh := time.Now().Add(-time.Duration(guard.MaxAgeSeconds)*time.Second + 2*time.Second)
	if err := os.Chtimes(guard.Path, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	if err := collector.verifyRuntimeFingerprints(); err != nil {
		t.Fatalf("a fresh guard attestation was refused: %v", err)
	}
}

func TestGuardFingerprintDatedInTheFutureIsRefused(t *testing.T) {
	// Negative age satisfies any upper bound, so without this branch a clock
	// skew or a touched file reads as maximally fresh -- the cheapest way to
	// defeat the check, and the direction an accident takes too.
	guard := RuntimeFingerprint{Purpose: "nft-guard", Path: "/unused", SHA256: strings.Repeat("a", 64), MaxAgeSeconds: 120}
	now := time.Now()
	err := checkFingerprintFreshness(guard, now.Add(time.Minute), now)
	if err == nil {
		t.Fatal("an attestation dated in the future was accepted")
	}
	if !strings.Contains(err.Error(), "future") {
		t.Fatalf("refusal does not name the future timestamp: %v", err)
	}
}

func TestStaticFingerprintsAreNotAgeBounded(t *testing.T) {
	// A containerd config that has not been rewritten in a year is not a
	// problem, and bounding it would fence the pool for no reason. Only the
	// measured target carries a bound.
	static := RuntimeFingerprint{Purpose: "containerd-config", Path: "/unused", SHA256: strings.Repeat("a", 64)}
	now := time.Now()
	if err := checkFingerprintFreshness(static, now.Add(-365*24*time.Hour), now); err != nil {
		t.Fatalf("a static fingerprint was age-bounded: %v", err)
	}
}

func TestMeasuredFingerprintWithoutAnAgeBoundIsRefusedAtStartup(t *testing.T) {
	// The fail-open shape this entry exists to prevent. Refused at startup, so
	// it cannot be introduced by editing config and noticed only after a flush
	// went uncaught.
	collector, _, _, _ := storageCollectorFixture(t)
	config := collector.config
	config.RuntimeFingerprints = append([]RuntimeFingerprint(nil), collector.config.RuntimeFingerprints...)
	for index := range config.RuntimeFingerprints {
		if config.RuntimeFingerprints[index].Purpose == "nft-guard" {
			config.RuntimeFingerprints[index].MaxAgeSeconds = 0
		}
	}
	_, err := NewHostStorageCollector(config)
	if err == nil {
		t.Fatal("accepted a measured fingerprint with no age bound")
	}
	if !strings.Contains(err.Error(), "max_age_seconds") {
		t.Fatalf("refusal does not name the missing bound: %v", err)
	}
}

func TestStaticFingerprintWithAnAgeBoundIsRefusedAtStartup(t *testing.T) {
	// The other direction. An age bound on a static file is a config error that
	// would fence the pool at an arbitrary future moment, which is worse than
	// failing at startup where someone is watching.
	collector, _, _, _ := storageCollectorFixture(t)
	config := collector.config
	config.RuntimeFingerprints = append([]RuntimeFingerprint(nil), collector.config.RuntimeFingerprints...)
	for index := range config.RuntimeFingerprints {
		if config.RuntimeFingerprints[index].Purpose == "kata-config" {
			config.RuntimeFingerprints[index].MaxAgeSeconds = 60
		}
	}
	_, err := NewHostStorageCollector(config)
	if err == nil {
		t.Fatal("accepted an age bound on a static fingerprint")
	}
	if !strings.Contains(err.Error(), "static") {
		t.Fatalf("refusal does not name the static target: %v", err)
	}
}

func TestMissingGuardAttestationIsRefused(t *testing.T) {
	// The measurer removes the file when it cannot compute a fingerprint, so
	// absence is its way of refusing. The witness must read that as a refusal
	// and not as "nothing to check".
	collector, _, _, _ := storageCollectorFixture(t)
	guard := guardFingerprint(t, collector)
	if err := os.Remove(guard.Path); err != nil {
		t.Fatal(err)
	}
	if err := collector.verifyRuntimeFingerprints(); err == nil {
		t.Fatal("a missing guard attestation was accepted")
	}
}

func TestWitnessRequiresTheGuardFingerprint(t *testing.T) {
	// Dropping the entry must not be a way to switch the check off.
	collector, _, _, _ := storageCollectorFixture(t)
	config := collector.config
	var kept []RuntimeFingerprint
	for _, entry := range collector.config.RuntimeFingerprints {
		if entry.Purpose != "nft-guard" {
			kept = append(kept, entry)
		}
	}
	config.RuntimeFingerprints = kept
	if _, err := NewHostStorageCollector(config); err == nil {
		t.Fatal("accepted a fingerprint set with no nft-guard entry")
	}
}

func TestAnEffectivelyInfiniteAgeBoundIsRefused(t *testing.T) {
	// Rejecting only <= 0 left the same fail-open shape one level down: ten
	// years is present, positive, passes every check, and is satisfied by any
	// mtime a running host can produce. A disabled bound wearing a configured
	// bound's clothes.
	collector, _, _, _ := storageCollectorFixture(t)
	config := collector.config
	config.RuntimeFingerprints = append([]RuntimeFingerprint(nil), collector.config.RuntimeFingerprints...)
	for index := range config.RuntimeFingerprints {
		if config.RuntimeFingerprints[index].Purpose == "nft-guard" {
			config.RuntimeFingerprints[index].MaxAgeSeconds = 315360000
		}
	}
	_, err := NewHostStorageCollector(config)
	if err == nil {
		t.Fatal("accepted an age bound no realistic staleness could exceed")
	}
	if !strings.Contains(err.Error(), "ceiling") {
		t.Fatalf("refusal does not name the ceiling: %v", err)
	}
}

func TestTheMeasurementToolIsItselfPinned(t *testing.T) {
	// The control's input. Pinning the guard fingerprint while leaving the
	// script that produces it unpinned means replacing guard-fingerprint.py
	// with one line printing the expected hex keeps every gate green forever --
	// the precise failure the guard entry exists to end.
	collector, _, _, _ := storageCollectorFixture(t)
	config := collector.config
	var kept []RuntimeFingerprint
	for _, entry := range collector.config.RuntimeFingerprints {
		if entry.Purpose != "nft-guard-tool" {
			kept = append(kept, entry)
		}
	}
	config.RuntimeFingerprints = kept
	if _, err := NewHostStorageCollector(config); err == nil {
		t.Fatal("accepted a fingerprint set that does not pin the measurement tool")
	}
}

func TestAChangedMeasurementToolIsRefused(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	for _, entry := range collector.config.RuntimeFingerprints {
		if entry.Purpose != "nft-guard-tool" {
			continue
		}
		if err := os.WriteFile(entry.Path, []byte("print('deadbeef')"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := collector.verifyRuntimeFingerprints(); err == nil {
		t.Fatal("a rewritten measurement tool was accepted")
	}
}

func guardFingerprint(t *testing.T, collector *HostStorageCollector) RuntimeFingerprint {
	t.Helper()
	for _, entry := range collector.config.RuntimeFingerprints {
		if entry.Purpose == "nft-guard" {
			return entry
		}
	}
	t.Fatal("fixture has no nft-guard fingerprint")
	return RuntimeFingerprint{}
}

// --- `ctr snapshots list` has no flags on containerd 2.3 ---------------------
//
// The witness issued `snapshots --snapshotter <name> list --quiet`. On the real
// host that exits 1 with `ctr: flag provided but not defined: -quiet`, so every
// inventory failed and the controller fenced the pool on `trusted backend
// inventory failed` each cycle. `ctr containers list --quiet` does exist on the
// same binary, which is what made the wrong call look plausible.

// capturedSnapshotTable is COPIED VERBATIM from a real listing:
// ~/.cache/sjo-pool/captures/snap-capture-2026-09-21.txt, taken on MS-A2 at
// 2026-09-21T18:05:47Z while the node was idle. Five of its 59 rows, chosen to
// carry both field counts: three with a PARENT and two root rows whose PARENT
// is EMPTY, trailing spaces included.
//
// It replaced a hand-written table that was NAMED as the real listing and was
// not. Nothing was wrong with the values, but a fixture whose name asserts
// provenance it does not have is how three outages here began, so the name now
// says which it is -- and everything still synthesized says MODELLED.
//
// WHAT THIS CAPTURE DOES NOT CONTAIN, and why the unpack fixtures below are
// modelled rather than captured: the node was idle, so all 59 rows are
// `Committed` and none is an unpack row. A capture of a pull in flight would
// need a multi-gigabyte pull running at the moment the listing is taken.
const capturedSnapshotTable = "" +
	"KEY                                                                     PARENT                                                                  KIND      \n" +
	"sha256:01b297bf1886dfdba65c6bc900aa3968d9a8bdb6dc2214f49ce760f3cc744673 sha256:b9a65b3c65ab22d490085bd0bf5490e2409da8748b406870f2463bdc41cd6795 Committed \n" +
	"sha256:34884abbe92863fce933ed7c39c0e045631af0ed86d5cc0dfbdf9fdca426ce3c                                                                         Committed \n" +
	"sha256:163fc5bbb3f436d383bd1623777808ed8854bc2df32ee6cd768013d46bf835fc sha256:c2a1c6d74f9a5d22585111a764cf06ff661ba26a0ab0f15939b56f39b68d9ac1 Committed \n" +
	"sha256:68b14877de21ededb2711dcda9ab929d66a57f9fe2d6abb9a1593372648ccea8                                                                         Committed \n" +
	"sha256:1ad6ffc92e47981ac1c5792b7d1e9f2bf7351cd75340b967697e24291d9a953e sha256:518d48404740296c1e659525ff822a69794e867a1d94703ddae04e3ec2259860 Committed \n"

const snapshotTableHeader = "KEY                                                                     PARENT                                                                  KIND      "

// snapshotTable renders MODELLED rows in the captured table's shape: a header,
// then one row per key with an optional PARENT and a KIND, trailing spaces
// included. Fixtures go through the same renderer the assertions describe, so a
// fixture cannot drift into a shape the parser finds easier than the host's.
//
// The KIND is per row and never defaulted. A helper that stamped one kind on
// every fixture is how the writable set would stop being exercised: the whole
// inventory keys on Active, and a table of Committed rows would exercise the
// walk-past branch only.
//
// A KEY MAY CONTAIN A SPACE. containerd's image unpacker names its in-flight
// snapshots `extract-<unique> <chainID>`, so the renderer must be able to emit
// one -- a helper that could not would make the unpack cases untestable through
// the same path every other fixture takes.
type snapshotFixtureRow struct{ key, parent, kind string }

func activeSnapshot(key string) snapshotFixtureRow {
	return snapshotFixtureRow{key: key, kind: "Active"}
}

func committedSnapshot(key string) snapshotFixtureRow {
	return snapshotFixtureRow{key: key, kind: "Committed"}
}

// modelledUnpackSnapshot is the shape containerd's unpacker prepares, MODELLED
// from the pinned v2.3.2 source and not captured: core/snapshots/snapshotter.go
// `UnpackKeyFormat = "extract-%s %s"`, filled by core/unpack/unpacker.go with
// `uniquePart()` (`"%d-%s"`: Nanosecond, then base64url of three bytes) and the
// layer's chain ID. The 2026-09-21 host capture has no such row because the node
// was idle; capturing one needs a multi-gigabyte pull running at that instant.
func modelledUnpackSnapshot(unique, chainID, parent string) snapshotFixtureRow {
	return snapshotFixtureRow{key: "extract-" + unique + " " + chainID, parent: parent, kind: "Active"}
}

func snapshotTable(rows ...snapshotFixtureRow) string {
	table := snapshotTableHeader + "\n"
	for _, row := range rows {
		parent := row.parent
		if parent == "" {
			parent = "                                                                       "
		}
		table += row.key + " " + parent + " " + row.kind + " \n"
	}
	return table
}

func TestSnapshotTableParsesTheCapturedListing(t *testing.T) {
	writable, unpacking, attributable, err := parseSnapshotTable(capturedSnapshotTable)
	if err != nil {
		t.Fatalf("a real `ctr snapshots list` output was refused: %v", err)
	}
	// Every captured row is Committed -- the node was idle -- so the writable and
	// unpacking sets are empty and all five keys are attributable. That is the
	// state this collector spends almost all of its life in.
	if len(attributable) != 5 {
		t.Fatalf("parsed %d keys from the capture, want 5: %q", len(attributable), attributable)
	}
	if len(writable) != 0 || len(unpacking) != 0 {
		t.Fatalf("an idle capture produced writable=%q unpacking=%q", writable, unpacking)
	}
	for _, key := range attributable {
		if !strings.HasPrefix(key, "sha256:") || len(key) != len("sha256:")+64 {
			t.Fatalf("captured key %q is not a bare layer digest; the columns were split wrong", key)
		}
	}
	// Two of the five rows are ROOT snapshots with an EMPTY PARENT, so they carry
	// two fields rather than three. If the kind were read from fields[2] instead
	// of the last field it would be missing there.
	roots := 0
	for _, line := range strings.Split(capturedSnapshotTable, "\n")[1:] {
		if fields := strings.Fields(line); len(fields) == 2 {
			roots++
		}
	}
	if roots != 2 {
		t.Fatalf("the capture fixture carries %d two-field rows; it is meant to carry 2", roots)
	}
}

func TestSnapshotTableReadsTheKindOffATwoFieldRow(t *testing.T) {
	// A root snapshot has no PARENT, so its row carries two fields. An Active
	// root snapshot must still land in the writable set.
	writable, _, all, err := parseSnapshotTable(snapshotTableHeader + "\n" + testSandboxA + "   Active  \n")
	if err != nil {
		t.Fatalf("a two-field Active row was refused: %v", err)
	}
	if len(all) != 1 || len(writable) != 1 || writable[0] != testSandboxA {
		t.Fatalf("two-field Active row parsed as all=%q writable=%q", all, writable)
	}
}

func TestSnapshotTableRefusesAnUnknownKind(t *testing.T) {
	// The kind decides whether a key must be attributed to a worker. A kind this
	// was never read against is a containerd whose writable set nobody measured,
	// and guessing would decide the pool's fence on an unmeasured string.
	if _, _, _, err := parseSnapshotTable(snapshotTableHeader + "\n" + testSandboxA + "   parent   Mutable \n"); err == nil {
		t.Fatal("accepted a snapshot listing carrying an unknown kind")
	}
}

func TestSnapshotTableHeaderOnlyIsAnEmptyListing(t *testing.T) {
	// A pool with no workers. An empty listing is a real answer, and refusing
	// it would fence the pool every time it was idle.
	writable, _, all, err := parseSnapshotTable(snapshotTableHeader + "\n")
	if err != nil {
		t.Fatalf("an empty snapshot listing was refused: %v", err)
	}
	if len(all) != 0 || len(writable) != 0 {
		t.Fatalf("header-only listing parsed all=%q writable=%q", all, writable)
	}
}

func TestSnapshotTableRefusesAChangedHeader(t *testing.T) {
	// A format change must fail closed. Skipping the first line instead of
	// asserting it would read a reordered table as data: the parser would
	// return whatever now sits in the first column and the inventory would look
	// complete while naming the wrong snapshots.
	for _, header := range []string{
		"KEY                    KIND                   PARENT",
		"NAME                   PARENT                 KIND",
		"KEY                    PARENT",
		"",
	} {
		if _, _, _, err := parseSnapshotTable(header + "\nsome-key   some-parent   Committed\n"); err == nil {
			t.Fatalf("accepted a listing headed %q", header)
		}
	}
}

// The regression test for the defect itself. The bug was in the ARGUMENTS, not
// in any value the collector returns, so only the recorded argv can pin it: a
// test that asserted on the parsed keys alone would stay green against the
// broken call as long as the fake runner answered it.
func TestSnapshotListingIsNeverInvokedWithQuiet(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	inner := collector.run
	var calls [][]string
	collector.run = func(ctx context.Context, executable string, arguments ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), arguments...))
		// Answer BOTH spellings, so this test fails on the recorded argv rather
		// than on the fixture refusing the call. A fake that only knows the
		// correct spelling turns a wrong-flag regression into a generic
		// "inventory failed", which is the same red for a dozen other causes.
		if len(arguments) > 0 && arguments[len(arguments)-1] == "--quiet" &&
			strings.Contains(strings.Join(arguments, " "), "snapshots") {
			return inner(ctx, executable, arguments[:len(arguments)-1]...)
		}
		return inner(ctx, executable, arguments...)
	}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("inventory failed: %v", err)
	}

	seen := false
	for _, call := range calls {
		joined := strings.Join(call, " ")
		if !strings.Contains(joined, "snapshots") {
			continue
		}
		seen = true
		if strings.Contains(joined, "--quiet") {
			t.Fatalf("the snapshot listing was invoked with a flag `ctr snapshots list` does not accept: %q", joined)
		}
		if !strings.HasSuffix(joined, "snapshots --snapshotter sjo-devmapper list") {
			t.Fatalf("unexpected snapshot listing invocation: %q", joined)
		}
	}
	// Without this the test passes when no snapshot listing happens at all.
	if !seen {
		t.Fatal("no snapshot listing was issued; the assertion above reached nothing")
	}
}

// --- containerd 2.3 records sandbox ownership in a field, not a label --------

func TestSandboxOwnershipResolvesFromTheContainerdField(t *testing.T) {
	// The shape the real host produces: SandboxID set, the legacy label absent
	// on all 40 containers measured. Before the fix this container had no
	// durable worker identity and fenced the pool.
	collector, _, _, _ := storageCollectorFixture(t)
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete || len(scope.Items) != 1 {
		t.Fatalf("a worker app container carrying only SandboxID was not witnessed: %+v %v", scope, err)
	}
	if scope.Items[0].Labels[LabelLeaseID] != testLeaseA {
		t.Fatalf("witness lost the lease identity: %+v", scope.Items[0])
	}
}

func TestSandboxOwnershipStillResolvesFromTheLegacyLabel(t *testing.T) {
	// A containerd that sets the label and not the field must keep working.
	// Dropping the old reader to adopt the new one would trade one silent
	// failure for another.
	collector, _, infos, _ := storageCollectorFixture(t)
	infos[testContainerA] = containerdContainerInfo{ID: testContainerA, SnapshotKey: testContainerA,
		Snapshotter: "sjo-devmapper", Labels: map[string]string{sandboxIDLabel: testSandboxA}}
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete || len(scope.Items) != 1 {
		t.Fatalf("the legacy sandbox-id label stopped resolving: %+v %v", scope, err)
	}
}

func TestDisagreeingSandboxOwnersAreRefused(t *testing.T) {
	// Two different answers about which sandbox owns a writable snapshot is not
	// a case to pick a winner in.
	collector, _, infos, _ := storageCollectorFixture(t)
	infos[testContainerA] = containerdContainerInfo{ID: testContainerA, SnapshotKey: testContainerA,
		Snapshotter: "sjo-devmapper", SandboxID: testSandboxA,
		Labels: map[string]string{sandboxIDLabel: strings.Repeat("e", 64)}}
	_, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err == nil {
		t.Fatal("a container naming two different sandbox owners was accepted")
	}
	if !strings.Contains(err.Error(), "two different sandbox owners") {
		t.Fatalf("refusal does not name the disagreement: %v", err)
	}
}

func TestContainerWithNeitherOwnerIsStillRefused(t *testing.T) {
	// The behaviour the fix must not relax: a dedicated-snapshotter container
	// with no recoverable owner is an unaccounted writable snapshot.
	collector, _, infos, _ := storageCollectorFixture(t)
	infos[testContainerA] = containerdContainerInfo{ID: testContainerA, SnapshotKey: testContainerA,
		Snapshotter: "sjo-devmapper", Labels: map[string]string{}}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("a dedicated-snapshotter container with no sandbox owner was accepted")
	}
}

// --- a worker that tears down mid-pass -------------------------------------
//
// Measured on the live MS-A2 with the 2026-09-21.3 images: the witness logged
// `collector=storage tool=sjo-host-ctr exit_status=1 stderr="ctr: container
// \"a68343d9...\" in namespace \"k8s.io\": not found"` and the controller fenced.
// Workers are one-job, so a snapshot's container disappearing between the
// listing and its info call is ORDINARY, and fencing the pool on it is wrong.
// Tolerating it blindly is also wrong: these pin the proof that makes the
// difference, and the proof is now the SNAPSHOT rather than the container --
// storage that outlives its container is the orphan this witness exists for.

// storageRunnerWithVanishingContainer answers the fixture's commands, but
// refuses `containers info` for the named ids. relist controls what the SECOND
// snapshot listing returns and whether it succeeds -- which is the whole
// evidence the tolerance rests on.
func storageRunnerWithVanishingContainer(collector *HostStorageCollector, infos map[string]containerdContainerInfo,
	snapshots *string, missing map[string]bool, relist func() ([]byte, error)) (hostCommandRunner, *int) {
	listCalls := 0
	return func(_ context.Context, executable string, arguments ...string) ([]byte, error) {
		if executable != collector.config.CTRExecutable || len(arguments) < 6 {
			return nil, errors.New("unexpected containerd command")
		}
		suffix := strings.Join(arguments[4:], " ")
		if suffix == "snapshots --snapshotter sjo-devmapper list" {
			listCalls++
			if listCalls == 1 {
				return []byte(*snapshots), nil
			}
			return relist()
		}
		if len(arguments) == 7 && arguments[4] == "containers" && arguments[5] == "info" {
			id := arguments[6]
			if missing[id] {
				return nil, newHostCommandFailure("sjo-host-ctr", errors.New("exit status 1"))
			}
			value, ok := infos[id]
			if !ok {
				return nil, errors.New("unknown container")
			}
			return json.Marshal(value)
		}
		return nil, errors.New("unexpected containerd command")
	}, &listCalls
}

func TestVanishedContainerIsSkippedAndThePassCompletes(t *testing.T) {
	collector, _, infos, snapshots := storageCollectorFixture(t)
	// The app container tears down; its writable snapshot goes with it, so the
	// re-listing no longer names the key.
	collector.run, _ = storageRunnerWithVanishingContainer(collector, infos, snapshots,
		map[string]bool{testContainerA: true},
		func() ([]byte, error) {
			return []byte(snapshotTable(activeSnapshot(testSandboxA), committedSnapshot(testImageLayer))), nil
		})
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete {
		t.Fatalf("a worker that tore down mid-pass fenced the pool: %+v %v", scope, err)
	}
}

func TestWritableStorageThatOutlivesItsContainerIsRefused(t *testing.T) {
	// THE DISCRIMINATING TEST. Without the "snapshot still listed" check, every
	// failed info call becomes a skip -- a containerd that is unwell reports a
	// complete scope over writable storage it never attributed, which is exactly
	// the unaccounted storage this witness exists to prevent. It is also the
	// orphan shape #845 named: a snapshot no container owns.
	collector, _, infos, snapshots := storageCollectorFixture(t)
	collector.run, _ = storageRunnerWithVanishingContainer(collector, infos, snapshots,
		map[string]bool{testContainerA: true},
		func() ([]byte, error) { return []byte(*snapshots), nil })
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err == nil || scope.Complete {
		t.Fatal("a writable snapshot whose container failed inspection was skipped while still listed")
	}
	if !strings.Contains(err.Error(), "no owning container") {
		t.Fatalf("refusal does not name the orphaned storage: %v", err)
	}
	// The original failure must survive, so the witness log still names the
	// tool and its exit status.
	var command *hostCommandFailure
	if !errors.As(err, &command) || command.Tool != "sjo-host-ctr" {
		t.Fatalf("the original host command failure was lost: %v", err)
	}
}

func TestAFailedRelistIsNotEvidenceOfAnythingAndRefuses(t *testing.T) {
	// The re-list IS the proof. If it cannot be taken, nothing distinguishes
	// "gone" from "containerd is unwell", so the original failure stands.
	collector, _, infos, snapshots := storageCollectorFixture(t)
	collector.run, _ = storageRunnerWithVanishingContainer(collector, infos, snapshots,
		map[string]bool{testContainerA: true},
		func() ([]byte, error) { return nil, errors.New("containerd unavailable") })
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err == nil || scope.Complete {
		t.Fatal("a failed re-list was read as proof the snapshot vanished")
	}
	var command *hostCommandFailure
	if !errors.As(err, &command) {
		t.Fatalf("the original host command failure was lost behind the re-list failure: %v", err)
	}
}

func TestTooMuchChurnRefusesRatherThanWitnessingAMovingSet(t *testing.T) {
	// A bound, not a loop. Past it the pass is no longer observing a stable
	// snapshot set, and declaring a complete scope over one is a claim the
	// witness cannot support.
	collector, _, infos, _ := storageCollectorFixture(t)
	churning := make([]snapshotFixtureRow, 0, maxVanishedContainers+2)
	missing := make(map[string]bool, maxVanishedContainers+2)
	for index := range maxVanishedContainers + 1 {
		id := fmt.Sprintf("%064x", index+1)
		churning = append(churning, activeSnapshot(id))
		missing[id] = true
	}
	first := snapshotTable(append(churning, activeSnapshot(testSandboxA))...)
	listCalls := 0
	collector.run = func(_ context.Context, executable string, arguments ...string) ([]byte, error) {
		suffix := strings.Join(arguments[4:], " ")
		if suffix == "snapshots --snapshotter sjo-devmapper list" {
			listCalls++
			if listCalls == 1 {
				return []byte(first), nil
			}
			// Every re-list agrees they are gone, so only the bound can stop this.
			return []byte(snapshotTable(activeSnapshot(testSandboxA))), nil
		}
		if len(arguments) == 7 && arguments[4] == "containers" && arguments[5] == "info" {
			if missing[arguments[6]] {
				return nil, newHostCommandFailure("sjo-host-ctr", errors.New("exit status 1"))
			}
			value, ok := infos[arguments[6]]
			if !ok {
				return nil, errors.New("unknown container")
			}
			return json.Marshal(value)
		}
		return nil, errors.New("unexpected containerd command")
	}
	_, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err == nil {
		t.Fatalf("accepted a pass that lost %d snapshots", maxVanishedContainers+1)
	}
	if !strings.Contains(err.Error(), "churning") {
		t.Fatalf("refusal does not name the churn an operator has to act on: %v", err)
	}
}

func TestTheVanishDecisionNeverReadsTheToolsStderr(t *testing.T) {
	// The skip must not depend on `ctr`'s error prose. A denylist of strings is
	// never finished, and a locale or a version bump silently widens it into
	// "skip anything". Here the vanished container's stderr says nothing about
	// being missing, and it is STILL skipped -- because the re-list is what
	// decided.
	collector, _, infos, _ := storageCollectorFixture(t)
	listCalls := 0
	collector.run = func(_ context.Context, executable string, arguments ...string) ([]byte, error) {
		suffix := strings.Join(arguments[4:], " ")
		if suffix == "snapshots --snapshotter sjo-devmapper list" {
			listCalls++
			if listCalls == 1 {
				return []byte(snapshotTable(activeSnapshot(testSandboxA), activeSnapshot(testContainerA),
					committedSnapshot(testImageLayer))), nil
			}
			return []byte(snapshotTable(activeSnapshot(testSandboxA), committedSnapshot(testImageLayer))), nil
		}
		if len(arguments) == 7 && arguments[4] == "containers" && arguments[5] == "info" {
			if arguments[6] == testContainerA {
				return nil, newHostCommandFailure("sjo-host-ctr", errors.New("exit status 1"))
			}
			value, ok := infos[arguments[6]]
			if !ok {
				return nil, errors.New("unknown container")
			}
			return json.Marshal(value)
		}
		return nil, errors.New("unexpected containerd command")
	}
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete {
		t.Fatalf("the skip depended on the tool's stderr text rather than the re-list: %+v %v", scope, err)
	}
}

// --- the exec cost (#845) ---------------------------------------------------

// TestInventoryNeverWalksTheNodesContainerList is the regression test for the
// defect itself. The bug was one exec PER CONTAINER ON THE NODE, and no value
// the collector returns records that -- only the recorded argv can pin it. A
// test asserting on the witnessed scope alone stays green against the slow
// implementation, which is how it shipped.
func TestInventoryNeverWalksTheNodesContainerList(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	inner := collector.run
	var calls [][]string
	collector.run = func(ctx context.Context, executable string, arguments ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), arguments...))
		// Answer `containers list` here rather than leaving the fixture to
		// refuse it, so a regression fails on the RECORDED ARGV instead of on a
		// generic "inventory failed" that a dozen other causes also produce.
		if strings.HasSuffix(strings.Join(arguments, " "), "containers list --quiet") {
			return []byte(testSandboxA + "\n" + testContainerA + "\n"), nil
		}
		return inner(ctx, executable, arguments...)
	}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("inventory failed: %v", err)
	}
	for _, call := range calls {
		if strings.HasSuffix(strings.Join(call, " "), "containers list --quiet") {
			t.Fatal("the inventory listed every container on the node; that is one exec per container (#845)")
		}
	}
}

// TestSteadyStateInventoryIsOneExec pins the other half. Once the ledger
// attributes a worker's snapshot keys, re-attributing them costs nothing: the
// pass is a single snapshot listing. Without the ledger short-circuit the cost
// would be one exec per worker container per cycle, which is the same defect
// with a smaller constant.
func TestSteadyStateInventoryIsOneExec(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("first inventory failed: %v", err)
	}
	inner := collector.run
	execs := 0
	collector.run = func(ctx context.Context, executable string, arguments ...string) ([]byte, error) {
		execs++
		return inner(ctx, executable, arguments...)
	}
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete || len(scope.Items) != 1 {
		t.Fatalf("second inventory failed: %+v %v", scope, err)
	}
	if execs != 1 {
		t.Fatalf("steady-state inventory took %d execs, want 1", execs)
	}
}

// --- pruning a worker identity that is gone on every leg (#845) -------------

func TestLedgerEntryIsPrunedOnlyWhenEveryLegAgreesItIsGone(t *testing.T) {
	collector, criOutput, _, snapshots := storageCollectorFixture(t)
	processes := collector.config.ProcessIdentitySource
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("first inventory failed: %v", err)
	}
	ledger, err := processes.loadLedger()
	if err != nil || len(ledger.Entries) != 1 {
		t.Fatalf("the worker identity was not recorded: %+v %v", ledger, err)
	}

	// The Pod is gone but the writable storage is not. Pruning here would erase
	// the identity that makes the orphan attributable, which is the one thing
	// the ledger is for.
	*criOutput = `{"items":[]}`
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("orphan inventory failed: %v", err)
	}
	if ledger, err = processes.loadLedger(); err != nil || len(ledger.Entries) != 1 {
		t.Fatalf("an identity with live writable storage was pruned: %+v %v", ledger, err)
	}

	// Storage gone too. Now every leg agrees.
	*snapshots = snapshotTable(committedSnapshot(testImageLayer))
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete || len(scope.Items) != 0 {
		t.Fatalf("final inventory failed: %+v %v", scope, err)
	}
	if ledger, err = processes.loadLedger(); err != nil || len(ledger.Entries) != 0 {
		t.Fatalf("an identity gone on every leg was kept: %+v %v", ledger, err)
	}
}

func TestPruneIsRefusedWhenThePassSkippedAnything(t *testing.T) {
	// A skip means the snapshot set churned mid-pass, so absence of a key is not
	// evidence the storage is gone. An entry may only be dropped on a pass that
	// observed every leg whole.
	collector, criOutput, infos, _ := storageCollectorFixture(t)
	processes := collector.config.ProcessIdentitySource
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("first inventory failed: %v", err)
	}
	*criOutput = `{"items":[]}`
	// A second, unrelated worker's snapshot appears and then vanishes mid-pass:
	// one tolerated skip. The first worker's storage is gone, so without the
	// skip guard its entry would be pruned.
	// Deliberately not a repeat of "a": that is testSandboxA, which the ledger
	// already attributes, so the pass would short-circuit past it and take no
	// skip at all. The first draft of this test did exactly that and stayed
	// green with the guard deleted.
	churning := strings.Repeat("9", 64)
	listCalls := 0
	collector.run = func(_ context.Context, _ string, arguments ...string) ([]byte, error) {
		suffix := strings.Join(arguments[4:], " ")
		if suffix == "snapshots --snapshotter sjo-devmapper list" {
			listCalls++
			if listCalls == 1 {
				return []byte(snapshotTable(activeSnapshot(churning))), nil
			}
			return []byte(snapshotTable(committedSnapshot(testImageLayer))), nil
		}
		if len(arguments) == 7 && arguments[4] == "containers" && arguments[5] == "info" {
			if arguments[6] == churning {
				return nil, newHostCommandFailure("sjo-host-ctr", errors.New("exit status 1"))
			}
			value, ok := infos[arguments[6]]
			if !ok {
				return nil, errors.New("unknown container")
			}
			return json.Marshal(value)
		}
		return nil, errors.New("unexpected containerd command")
	}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("churning inventory failed: %v", err)
	}
	ledger, err := processes.loadLedger()
	if err != nil || len(ledger.Entries) != 1 {
		t.Fatalf("a pass that skipped a snapshot still pruned: %+v %v", ledger, err)
	}
}

func TestPruneIsRefusedWhenTheProcessLegDidNotComplete(t *testing.T) {
	// liveObserved false means the process leg did not finish, so absence from
	// liveSandboxes says nothing at all. The storage collector fails the whole
	// pass in that case, and the ledger must be untouched when it does.
	collector, _, _, snapshots := storageCollectorFixture(t)
	processes := collector.config.ProcessIdentitySource
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("first inventory failed: %v", err)
	}
	*snapshots = snapshotTable(committedSnapshot(testImageLayer))
	processes.run = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("CRI unavailable")
	}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("a failed process leg produced a complete storage scope")
	}
	if processes.liveObserved {
		t.Fatal("a failed ListProcesses left the previous pass's liveness standing")
	}
	ledger, err := processes.loadLedger()
	if err != nil || len(ledger.Entries) != 1 {
		t.Fatalf("an identity was pruned on an incomplete observation: %+v %v", ledger, err)
	}
}

// --- an image unpack lands on this snapshotter (#870 review, Critical) ------
//
// containerd's image unpacker prepares an `Active` snapshot per layer and names
// it `extract-<unique> <chainID>` -- THE KEY CONTAINS A SPACE. Read out of the
// pinned v2.3.2 source, core/snapshots/snapshotter.go:
//
//	UnpackKeyPrefix = "extract"
//	UnpackKeyFormat = UnpackKeyPrefix + "-%s %s"
//
// filled in core/unpack/unpacker.go by
// `fmt.Sprintf(snapshots.UnpackKeyFormat, uniquePart(), chainID)`.
//
// CRI picks the snapshotter from the runtime handler at PULL time as well as at
// container-create time, so any image pulled for the pool's handler unpacks
// here -- which the first worker Pod after a worker-image change does, because
// the host pre-pull lands on the default snapshotter instead.
//
// Before the fix both row shapes fenced the pool for the length of the pull,
// and neither error said "pull": the root-layer row read as an Active key that
// is not a container id, and the child-layer row read as a table row with too
// many fields. These are the reviewer's two probes.
//
// THE UNPACK FIXTURES ARE MODELLED, NOT CAPTURED. The 2026-09-21 host capture
// was taken on an idle node: 59 rows, every one Committed, no unpack row in it.

func TestARootLayerUnpackRowIsNotAnOrphanedSnapshot(t *testing.T) {
	// Three fields: `extract-<unique>` `<chainID>` `Active`, no PARENT. Reading
	// left to right makes fields[0] half a key and the row look ordinary.
	table := snapshotTable(modelledUnpackSnapshot("810352139-Xy_9", testChainID, ""))
	writable, unpacking, attributable, err := parseSnapshotTable(table)
	if err != nil {
		t.Fatalf("a root-layer unpack row was refused: %v", err)
	}
	if len(writable) != 0 {
		t.Fatalf("an unpack snapshot was offered for attribution as writable storage: %q", writable)
	}
	if len(attributable) != 0 {
		t.Fatalf("an unpack snapshot reached the attributable set: %q", attributable)
	}
	if len(unpacking) != 1 || unpacking[0] != "extract-810352139-Xy_9 "+testChainID {
		t.Fatalf("unpacking set is %q, want the one whole key including its space", unpacking)
	}
}

func TestAChildLayerUnpackRowIsNotAMalformedTable(t *testing.T) {
	// Four fields: `extract-<unique>` `<chainID>` `<parent>` `Active`. A field
	// count cannot accept this and reject a genuinely malformed row; only
	// parsing from the right can.
	parent := "sha256:01b297bf1886dfdba65c6bc900aa3968d9a8bdb6dc2214f49ce760f3cc744673"
	table := snapshotTable(modelledUnpackSnapshot("22417-A_b9", testChainID, parent))
	writable, unpacking, attributable, err := parseSnapshotTable(table)
	if err != nil {
		t.Fatalf("a child-layer unpack row was refused: %v", err)
	}
	if len(writable) != 0 || len(attributable) != 0 || len(unpacking) != 1 {
		t.Fatalf("child-layer unpack row parsed as writable=%q unpacking=%q attributable=%q",
			writable, unpacking, attributable)
	}
}

func TestAPullInFlightDoesNotFenceTheInventory(t *testing.T) {
	// The whole point, end to end: while a pull is extracting onto the dedicated
	// snapshotter, the pass must still complete and still witness the worker.
	collector, _, _, snapshots := storageCollectorFixture(t)
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("first inventory failed: %v", err)
	}
	*snapshots = snapshotTable(
		activeSnapshot(testSandboxA), activeSnapshot(testContainerA), committedSnapshot(testImageLayer),
		modelledUnpackSnapshot("810352139-Xy_9", testChainID, ""),
		modelledUnpackSnapshot("810352140-Zq_1", testChainID, testImageLayer),
	)
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete {
		t.Fatalf("an image unpack fenced the pool: %+v %v", scope, err)
	}
	if len(scope.Items) != 1 {
		t.Fatalf("the worker stopped being witnessed during a pull: %+v", scope)
	}
}

func TestAnUnpackSnapshotIsNeverPrunedAgainst(t *testing.T) {
	// An unpack key can never match a ledger entry's snapshot keys, but it must
	// also not be able to keep one alive by accident -- so it is kept out of the
	// attributable set the prune reads. Here the worker's own storage is gone and
	// only a pull remains: the entry must still be pruned.
	collector, criOutput, _, snapshots := storageCollectorFixture(t)
	processes := collector.config.ProcessIdentitySource
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("first inventory failed: %v", err)
	}
	*criOutput = `{"items":[]}`
	*snapshots = snapshotTable(modelledUnpackSnapshot("810352139-Xy_9", testChainID, ""))
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatalf("inventory during a pull failed: %v", err)
	}
	ledger, err := processes.loadLedger()
	if err != nil || len(ledger.Entries) != 0 {
		t.Fatalf("an identity gone on every leg survived because a pull was in flight: %+v %v", ledger, err)
	}
}

func TestAnUnpackLikeKeyThatIsNotTheRealFormatIsStillRefused(t *testing.T) {
	// The recognition must be the EXACT format, not the `extract-` prefix. A
	// prefix test alone would let anything calling itself `extract-…` opt out of
	// attribution, which is the invariant this collector exists for.
	table := snapshotTable(snapshotFixtureRow{key: "extract-not-a-real-unique " + testChainID, kind: "Active"})
	writable, unpacking, _, err := parseSnapshotTable(table)
	if err != nil {
		t.Fatalf("the row was refused at parse time rather than at attribution: %v", err)
	}
	if len(unpacking) != 0 {
		t.Fatalf("a key that only looks like an unpack key was excused: %q", unpacking)
	}
	if len(writable) != 1 {
		t.Fatalf("the key did not reach the writable set for the container-id gate: %q", writable)
	}
}

func TestAnUnaccountableWritableKeyNamesItsShapeAndNotItself(t *testing.T) {
	// The refusal must still happen, and must say what it saw WITHOUT echoing
	// the key: any workload that can create a snapshot here names its own, and
	// the name is unbounded.
	collector, _, _, snapshots := storageCollectorFixture(t)
	secret := "extract-not-a-real-unique-and-rather-long-too " + testChainID
	*snapshots = snapshotTable(snapshotFixtureRow{key: secret, kind: "Active"})
	_, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err == nil {
		t.Fatal("an Active key that is not a container identity was accepted")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "not-a-real-unique") {
		t.Fatalf("the refusal echoed the raw key: %v", err)
	}
	if !strings.Contains(err.Error(), "does not match containerd's unpack format") {
		t.Fatalf("the refusal does not name the shape it saw: %v", err)
	}
}

func TestTheUnpackKeyPatternMatchesTheFormatItWasReadFrom(t *testing.T) {
	// uniquePart() is `fmt.Sprintf("%d-%s", t.Nanosecond(), base64.URLEncoding
	// .EncodeToString(b[:3]))`: 1..9 digits, then exactly four base64url
	// characters including the `-` and `_` alphabet. These are the ends of that
	// range, not a middle example.
	for _, unique := range []string{"0-AAAA", "999999999-__--", "1-Zz_9"} {
		if !unpackKeyPattern.MatchString("extract-" + unique + " " + testChainID) {
			t.Fatalf("a well-formed unpack key was not recognised: unique=%q", unique)
		}
	}
	for name, key := range map[string]string{
		"no space":        "extract-1-AAAA" + testChainID,
		"five b64 chars":  "extract-1-AAAAA " + testChainID,
		"three b64 chars": "extract-1-AAA " + testChainID,
		"no digits":       "extract--AAAA " + testChainID,
		"not a digest":    "extract-1-AAAA notadigest",
		"trailing junk":   "extract-1-AAAA " + testChainID + " extra",
	} {
		if unpackKeyPattern.MatchString(key) {
			t.Fatalf("%s was accepted as an unpack key", name)
		}
	}
}

// TestPruneRefusesWhenTheProcessLegWasNeverObserved enters the OTHER half of
// the prune guard.
//
// It calls pruneVanishedIdentities directly, and that is the point rather than
// a shortcut: ListWritableStorage returns as soon as ListProcesses fails, so
// today nothing can reach the prune with liveObserved false. The half is
// defence against a future reordering, and an untested half of a guard is
// indistinguishable from a deleted one (#870 review, nit).
func TestPruneRefusesWhenTheProcessLegWasNeverObserved(t *testing.T) {
	collector, _, _, _ := storageCollectorFixture(t)
	processes := collector.config.ProcessIdentitySource
	ledger := processLedger{Version: processLedgerVersion, Entries: map[string]processLedgerEntry{
		testSandboxA: {Namespace: "github-runners", Name: "slipway-linux-" + testLeaseA, PodUID: "pod-uid-a", LeaseID: testLeaseA},
	}}

	processes.liveObserved, processes.liveSandboxes = false, map[string]bool{}
	if collector.pruneVanishedIdentities(ledger, processes, map[string]bool{}, 0) {
		t.Fatal("an unobserved process leg was read as agreement that the worker is gone")
	}
	if len(ledger.Entries) != 1 {
		t.Fatalf("the ledger was modified on an incomplete observation: %+v", ledger)
	}

	// The positive control. Same ledger, same empty storage, same zero skips --
	// only the observation flag differs, so nothing else can explain the refusal
	// above.
	processes.liveObserved = true
	if !collector.pruneVanishedIdentities(ledger, processes, map[string]bool{}, 0) {
		t.Fatal("a fully observed pass did not prune an identity gone on every leg")
	}
	if len(ledger.Entries) != 0 {
		t.Fatalf("the entry survived a complete observation: %+v", ledger)
	}
}

// --- a worker created between the Pod read and the snapshot read (#1213) ----
//
// A pass reads the CRI Pod list, then the dedicated snapshotter. A worker whose
// sandbox is created between the two has a writable snapshot and a containerd
// record naming its sandbox, but no ledger entry. On 2026-10-01 that refused
// the second create of three bursts, and the controller's breaker opened.

const (
	testContainerB = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	testLeaseB     = "22222222-2222-4222-8222-222222222222"
)

// storageFixtureWithSecondWorker adds worker B's sandbox and app container to
// the snapshot listing, and answers the CRI Pod list from podsForRead, which
// receives the 1-based read number. It returns the read counter.
func storageFixtureWithSecondWorker(t *testing.T, podsForRead func(read int) string) (*HostStorageCollector, *int) {
	t.Helper()
	collector, _, infos, snapshots := storageCollectorFixture(t)
	*snapshots = snapshotTable(activeSnapshot(testSandboxA), activeSnapshot(testContainerA),
		activeSnapshot(testSandboxB), activeSnapshot(testContainerB), committedSnapshot(testImageLayer))
	infos[testSandboxB] = containerdContainerInfo{ID: testSandboxB, SnapshotKey: testSandboxB, Snapshotter: "sjo-devmapper",
		Labels: map[string]string{}}
	infos[testContainerB] = containerdContainerInfo{ID: testContainerB, SnapshotKey: testContainerB, Snapshotter: "sjo-devmapper",
		SandboxID: testSandboxB, Labels: map[string]string{}}
	processes := collector.config.ProcessIdentitySource
	inner := processes.run
	reads := 0
	processes.run = func(ctx context.Context, executable string, arguments ...string) ([]byte, error) {
		if _, err := inner(ctx, executable, arguments...); err != nil {
			return nil, err
		}
		reads++
		return []byte(podsForRead(reads)), nil
	}
	return collector, &reads
}

func podsAAndB(runtimeHandler string) string {
	workerB := `{"id":"` + testSandboxB + `","metadata":{"name":"slipway-linux-` + testLeaseB +
		`","uid":"pod-uid-b","namespace":"github-runners"},"runtimeHandler":"` + runtimeHandler + `"}`
	return `{"items":[` + podItem(testSandboxA, runtimeHandler, "pod-uid-a") + `,` + workerB + `]}`
}

func TestAWorkerCreatedBetweenThePodAndSnapshotReadsIsAttributed(t *testing.T) {
	var handler string
	collector, reads := storageFixtureWithSecondWorker(t, func(read int) string {
		if read == 1 {
			// The first Pod list was taken before worker B's sandbox existed.
			return podList(testSandboxA, handler)
		}
		return podsAAndB(handler)
	})
	handler = collector.config.ProcessIdentitySource.config.RuntimeHandler
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete || len(scope.Items) != 2 {
		t.Fatalf("a worker that only raced the first Pod read was refused: %+v %v", scope, err)
	}
	if scope.Items[1].Labels[LabelLeaseID] != testLeaseB {
		t.Fatalf("worker B's storage was not attributed to its lease: %+v", scope.Items)
	}
	if *reads != 2 {
		t.Fatalf("the pass read the Pod list %d times, want 2 (one re-read)", *reads)
	}
}

func TestStorageWhoseOwnerNeverAppearsIsStillRefusedAfterOneReread(t *testing.T) {
	// The safety half. Worker B's sandbox is never in the Pod list, so its
	// storage has no durable identity on either pass and the refusal stands --
	// after exactly one re-read, not a loop.
	var handler string
	collector, reads := storageFixtureWithSecondWorker(t, func(int) string { return podList(testSandboxA, handler) })
	handler = collector.config.ProcessIdentitySource.config.RuntimeHandler
	scope, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a")
	if err == nil || scope.Complete {
		t.Fatal("writable storage whose sandbox no Pod list ever named was accepted")
	}
	if !errors.Is(err, errUnknownWorkerIdentity) {
		t.Fatalf("the refusal is not the unknown-identity refusal: %v", err)
	}
	if *reads != 2 {
		t.Fatalf("the pass read the Pod list %d times, want 2 (one re-read, then refuse)", *reads)
	}
}

func TestOtherStorageRefusalsAreNotReread(t *testing.T) {
	// The re-read is for the one race it was written for. A refusal of any
	// other kind is decided on the first pass.
	collector, _, infos, _ := storageCollectorFixture(t)
	infos[testContainerA] = containerdContainerInfo{ID: testContainerA, SnapshotKey: testContainerA,
		Snapshotter: "sjo-devmapper", SandboxID: testSandboxA,
		Labels: map[string]string{sandboxIDLabel: strings.Repeat("e", 64)}}
	processes := collector.config.ProcessIdentitySource
	inner := processes.run
	reads := 0
	processes.run = func(ctx context.Context, executable string, arguments ...string) ([]byte, error) {
		reads++
		return inner(ctx, executable, arguments...)
	}
	if _, err := collector.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("disagreeing sandbox owners were accepted")
	}
	if reads != 1 {
		t.Fatalf("a refusal other than an unknown identity re-read the Pod list (%d reads)", reads)
	}
}
