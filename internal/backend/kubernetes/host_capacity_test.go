package kubernetes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func hostCapacityFixture(t *testing.T) (*HostCapacityCollector, *string) {
	t.Helper()
	directory := t.TempDir()
	cache := filepath.Join(directory, "cache")
	logs := filepath.Join(directory, "logs")
	for _, path := range []string{cache, logs} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(cache, "image"), []byte("image"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, "controller.log"), []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}
	meminfo := filepath.Join(directory, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal:       67108864 kB\nMemAvailable:   50331648 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(directory, "thin-pool-report.json")
	collector, err := NewHostCapacityCollector(HostCapacityConfig{
		Namespace: "github-runners", ThinPoolReportPath: report, ThinPoolReportMaxAgeSeconds: 120,
		VolumeGroup: "ubuntu-vg", ThinPool: "sjo_runner_pool",
		ImageCachePath: cache, ImageCacheBudget: 20 << 30, LogPath: logs, LogBudget: 4 << 30,
		ProcMeminfo:          meminfo,
		ContainerRootfsBytes: 20 << 30,
		StorageReserveBytes:  8 << 30, RuntimeOverhead: Footprint{CPUMilli: 500, MemoryBytes: 1 << 30, StorageBytes: 2 << 30},
		MaxMetadataPercent: 80,
	})
	if err != nil {
		t.Fatal(err)
	}
	output := `{"report":[{"lv":[{"vg_name":"ubuntu-vg","lv_name":"sjo_runner_pool","lv_size":"107374182400","data_percent":"25.00","metadata_percent":"5.00","lv_attr":"twi-aotz--"}]}]}`
	writeThinPoolReport(t, report, output)
	// The fixture's report is rewritten by tests that change it, so every write
	// goes through one helper that also resets the mtime. A test that changed
	// the content and forgot the mtime would otherwise fail on staleness and
	// read as a content refusal.
	t.Cleanup(func() { collector.now = time.Now })
	return collector, &output
}

// writeThinPoolReport publishes a report the reader will accept: mode 0444,
// owned by whoever runs the test, in a directory with no group or other write
// bit. That is the same shape the host timer publishes, minus the chown to
// root which a test runner cannot do.
func writeThinPoolReport(t *testing.T, path, body string) {
	t.Helper()
	// Remove first: the published mode is 0444, so a second WriteFile to the
	// same path is EACCES. The host script renames a fresh file over the old
	// one for the same reason.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatal(err)
	}
}

func TestHostCapacityCollectorMeasuresBoundedHostDimensions(t *testing.T) {
	collector, _ := hostCapacityFixture(t)
	snapshot, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Complete || snapshot.WritableStorageBytesTotal != 100<<30 || snapshot.WritableStorageBytesUsed != 25<<30 ||
		snapshot.WritableStorageBytesOtherWorkloads != 8<<30 || snapshot.ImageCacheBytesUsed != 5 || snapshot.LogBytesUsed != 3 ||
		snapshot.MemoryBytesAvailable != 48<<30 || snapshot.ContainerRootfsBytes != 20<<30 || snapshot.CPUMilliTotal != 0 || snapshot.MemoryBytesTotal != 0 {
		t.Fatalf("unexpected host capacity snapshot: %+v", snapshot)
	}
}

func TestHostCapacityCollectorRejectsMetadataPressureAndBudgetOverflow(t *testing.T) {
	collector, output := hostCapacityFixture(t)
	*output = `{"report":[{"lv":[{"vg_name":"ubuntu-vg","lv_name":"sjo_runner_pool","lv_size":"107374182400","data_percent":"25.00","metadata_percent":"80.00","lv_attr":"twi-aotz--"}]}]}`
	writeThinPoolReport(t, collector.config.ThinPoolReportPath, *output)
	if result, err := collector.ObserveCapacity(context.Background(), "github-runners"); err == nil || result.Complete {
		t.Fatal("unsafe thin-pool metadata pressure was accepted")
	}
	collector, _ = hostCapacityFixture(t)
	collector.config.ImageCacheBudget = 4
	if result, err := collector.ObserveCapacity(context.Background(), "github-runners"); err == nil || result.Complete {
		t.Fatal("image cache beyond its admission budget was accepted")
	}
}

func TestConservativeCapacitySourceRejectsOverlapAndComposesDimensions(t *testing.T) {
	kube := &fakeCapacitySource{snapshot: CapacitySnapshot{Complete: true, CPUMilliTotal: 16000, CPUMilliOtherWorkloads: 2000,
		MemoryBytesTotal: 64 << 30, MemoryBytesOtherWorkloads: 8 << 30}}
	host := &fakeCapacitySource{snapshot: CapacitySnapshot{Complete: true, CPUMilliRuntimeOverhead: 500, MemoryBytesAvailable: 48 << 30, MemoryBytesRuntimeOverhead: 1 << 30,
		WritableStorageBytesTotal: 100 << 30, WritableStorageBytesUsed: 20 << 30, WritableStorageBytesOtherWorkloads: 8 << 30,
		ContainerRootfsBytes:                20 << 30,
		WritableStorageBytesRuntimeOverhead: 2 << 30, ImageCacheBytesTotal: 20 << 30, ImageCacheBytesUsed: 4 << 30,
		LogBytesTotal: 4 << 30, LogBytesUsed: 128 << 20}}
	source := &ConservativeCapacitySource{Kubernetes: kube, Host: host}
	snapshot, err := source.ObserveCapacity(context.Background(), "github-runners")
	if err != nil || !snapshot.Complete || snapshot.CPUMilliTotal != 16000 || snapshot.WritableStorageBytesUsed != 20<<30 {
		t.Fatalf("capacity dimensions were not composed: %+v %v", snapshot, err)
	}
	host.snapshot.CPUMilliTotal = 1
	if result, err := source.ObserveCapacity(context.Background(), "github-runners"); err == nil || result.Complete {
		t.Fatal("overlapping host and Kubernetes capacity fields were accepted")
	}
}

func TestTrustedHeadroomUsesLargerOfReservedAndObservedStorage(t *testing.T) {
	budget := AdmissionBudget{MaxWorkers: 3, PerWorker: Footprint{CPUMilli: 1000, MemoryBytes: 1 << 30, StorageBytes: 20 << 30},
		ContainerRootfsBytes:     10 << 30,
		PerWorkerImageCacheBytes: 1 << 30, PerWorkerLogBytes: 128 << 20}
	snapshot := CapacitySnapshot{Complete: true, CPUMilliTotal: 10000, MemoryBytesTotal: 16 << 30, MemoryBytesAvailable: 15 << 30,
		WritableStorageBytesTotal: 100 << 30, WritableStorageBytesUsed: 70 << 30, WritableStorageBytesOtherWorkloads: 5 << 30,
		ContainerRootfsBytes: 10 << 30,
		ImageCacheBytesTotal: 10 << 30, LogBytesTotal: 2 << 30}
	headroom, err := calculateTrustedHeadroom(snapshot, budget, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Seventy GiB used minus the active worker's reserved 20GiB leaves 50GiB
	// observed other use, so only one further 20GiB worker fits.
	if headroom.WritableStorageBytes != 30<<30 || headroom.FreeSlots != 1 {
		t.Fatalf("observed thin-pool use was not charged conservatively: %+v", headroom)
	}
}

// --- the thin-pool report reader -------------------------------------------
//
// The witness stopped running `lvs` because on the real MS-A2 it could not.
// Measured with a faithful stand-in (systemd-run, ReadOnlyPaths=/,
// DevicePolicy=closed, the witness's capability set): read-only / makes lvs
// exit 5 on "File locking initialisation failed", and no device-mapper access
// makes it exit ZERO with an empty LV list. A root host timer publishes the
// report now; these pin what the reader will and will not accept from it.

// realThinPoolReport is the report the host actually produces, captured with
// neither limit in place. Values are the measured ones.
const realThinPoolReport = `{"report":[{"lv":[{"vg_name":"ubuntu-vg","lv_name":"sjo_runner_probe","lv_size":"68719476736","data_percent":"4.91","metadata_percent":"7.00","lv_attr":"twi-a-tz--"}]}],"log":[]}`

func TestThinPoolReaderAcceptsTheRealHostReport(t *testing.T) {
	collector, _ := hostCapacityFixture(t)
	collector.config.ThinPool = "sjo_runner_probe"
	writeThinPoolReport(t, collector.config.ThinPoolReportPath, realThinPoolReport)
	snapshot, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err != nil {
		t.Fatalf("the real host report was refused: %v", err)
	}
	if snapshot.WritableStorageBytesTotal != 68719476736 {
		t.Fatalf("thin-pool size was not read: %+v", snapshot)
	}
	// 4.91% of 64 GiB, rounded up, is what the admission arithmetic charges.
	if snapshot.WritableStorageBytesUsed != 3374126308 {
		t.Fatalf("thin-pool usage was not read: %d", snapshot.WritableStorageBytesUsed)
	}
}

// The device-cgroup shape, and the sharpest of these. lvs exits ZERO here, so
// nothing upstream of the parse can catch it.
func TestThinPoolReaderRefusesAnEmptyLogicalVolumeList(t *testing.T) {
	collector, _ := hostCapacityFixture(t)
	writeThinPoolReport(t, collector.config.ThinPoolReportPath, `{"report":[{"lv":[]}],"log":[]}`)
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil {
		t.Fatal("a report naming no logical volume was accepted")
	}
	if !strings.Contains(err.Error(), "device mapper") {
		t.Fatalf("refusal does not name the cause an operator has to fix: %v", err)
	}
}

func TestThinPoolReaderRefusesAStaleReport(t *testing.T) {
	// A stopped timer otherwise leaves the last good report attesting forever.
	// One second past the bound, not a wild value: the boundary is where a
	// wrong comparison operator hides.
	collector, _ := hostCapacityFixture(t)
	path := collector.config.ThinPoolReportPath
	stale := time.Now().Add(-time.Duration(collector.config.ThinPoolReportMaxAgeSeconds)*time.Second - time.Second)
	if err := os.Chtimes(path, stale, stale); err != nil {
		t.Fatal(err)
	}
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil {
		t.Fatal("a stale thin-pool report was accepted")
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("refusal does not say the measurement stopped: %v", err)
	}
}

func TestThinPoolReaderAcceptsAReportInsideTheBound(t *testing.T) {
	// The positive control. Without it, a reader that refused everything would
	// pass the staleness test above on its own.
	collector, _ := hostCapacityFixture(t)
	path := collector.config.ThinPoolReportPath
	fresh := time.Now().Add(-time.Duration(collector.config.ThinPoolReportMaxAgeSeconds)*time.Second + 2*time.Second)
	if err := os.Chtimes(path, fresh, fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.ObserveCapacity(context.Background(), "github-runners"); err != nil {
		t.Fatalf("a report inside the age bound was refused: %v", err)
	}
}

func TestThinPoolReaderRefusesAFutureReport(t *testing.T) {
	// Negative age satisfies any upper bound, so a touched file or a clock skew
	// would otherwise be the cheapest way to switch the freshness check off.
	collector, _ := hostCapacityFixture(t)
	path := collector.config.ThinPoolReportPath
	ahead := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, ahead, ahead); err != nil {
		t.Fatal(err)
	}
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("a report dated in the future was accepted: %v", err)
	}
}

func TestThinPoolReaderRefusesASymlink(t *testing.T) {
	// A symlink could aim the read at any file the witness can open, including
	// one an unprivileged writer controls.
	collector, _ := hostCapacityFixture(t)
	path := collector.config.ThinPoolReportPath
	elsewhere := filepath.Join(filepath.Dir(path), "elsewhere.json")
	if err := os.WriteFile(elsewhere, []byte(realThinPoolReport), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, path); err != nil {
		t.Fatal(err)
	}
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("a symlinked thin-pool report was accepted: %v", err)
	}
}

func TestThinPoolReaderRefusesAWritableReportOrDirectory(t *testing.T) {
	// A group- or world-writable file can be rewritten by whoever holds that
	// bit; a writable DIRECTORY lets the file be renamed away and replaced no
	// matter how strict its own mode is, which is why both are checked.
	collector, _ := hostCapacityFixture(t)
	path := collector.config.ThinPoolReportPath
	if err := os.Chmod(path, 0o466); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.ObserveCapacity(context.Background(), "github-runners"); err == nil {
		t.Fatal("a group and world writable report was accepted")
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o777); err != nil {
		t.Fatal(err)
	}
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("a world writable report directory was accepted: %v", err)
	}
}

func TestThinPoolReaderRefusesAnOversizedReport(t *testing.T) {
	collector, _ := hostCapacityFixture(t)
	oversized := `{"padding":"` + strings.Repeat("x", maxThinPoolReportBytes) + `"}`
	writeThinPoolReport(t, collector.config.ThinPoolReportPath, oversized)
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil || !strings.Contains(err.Error(), "size ceiling") {
		t.Fatalf("an oversized report was accepted: %v", err)
	}
}

func TestThinPoolReaderRefusesAMissingReport(t *testing.T) {
	// The shape an uninstalled timer produces, which is the realistic one.
	collector, _ := hostCapacityFixture(t)
	if err := os.Remove(collector.config.ThinPoolReportPath); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.ObserveCapacity(context.Background(), "github-runners"); err == nil {
		t.Fatal("a missing thin-pool report was accepted")
	}
}

func TestThinPoolReaderRefusesAForeignOwner(t *testing.T) {
	// The discriminating ownership case, and the only one that needs root:
	// chown to another uid is a privileged operation. Skipped rather than
	// omitted, so the gap is visible in the test output instead of looking
	// like coverage. The always-runnable half is that the reader accepts only
	// root or its own uid, which every other test here exercises.
	if os.Geteuid() != 0 {
		t.Skip("chown to a foreign uid needs root; the CI runner is unprivileged")
	}
	collector, _ := hostCapacityFixture(t)
	path := collector.config.ThinPoolReportPath
	if err := os.Chown(path, 65534, 65534); err != nil {
		t.Fatal(err)
	}
	_, err := collector.ObserveCapacity(context.Background(), "github-runners")
	if err == nil || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("a report owned by another uid was accepted: %v", err)
	}
}

func TestHostCapacityConfigRefusesAnAbsentOrUnboundedReport(t *testing.T) {
	// Both halves are refused at startup rather than at the first request: an
	// age bound of zero is the fail-open shape, and a relative path is a config
	// that would read something other than the timer's output.
	collector, _ := hostCapacityFixture(t)
	base := collector.config
	if _, err := NewHostCapacityCollector(base); err != nil {
		t.Fatalf("the fixture configuration was rejected: %v", err)
	}
	for name, mutate := range map[string]func(*HostCapacityConfig){
		"no path":       func(c *HostCapacityConfig) { c.ThinPoolReportPath = "" },
		"relative path": func(c *HostCapacityConfig) { c.ThinPoolReportPath = "thin-pool-report.json" },
		"no age bound":  func(c *HostCapacityConfig) { c.ThinPoolReportMaxAgeSeconds = 0 },
		"infinite bound": func(c *HostCapacityConfig) {
			c.ThinPoolReportMaxAgeSeconds = maxMeasuredAgeSeconds + 1
		},
	} {
		config := base
		mutate(&config)
		if _, err := NewHostCapacityCollector(config); err == nil {
			t.Fatalf("accepted a capacity configuration with %s", name)
		}
	}
}

// headroomBudget and headroomSnapshot are a deliberately roomy baseline: every
// dimension fits three workers, so a table row that squeezes exactly one
// dimension proves that dimension is the one being named.
func headroomBudget() AdmissionBudget {
	return AdmissionBudget{MaxWorkers: 3, PerWorker: Footprint{CPUMilli: 1000, MemoryBytes: 1 << 30, StorageBytes: 20 << 30},
		ContainerRootfsBytes: 10 << 30, PerWorkerImageCacheBytes: 1 << 30, PerWorkerLogBytes: 128 << 20}
}

func headroomSnapshot() CapacitySnapshot {
	return CapacitySnapshot{Complete: true,
		CPUMilliTotal: 10000, MemoryBytesTotal: 16 << 30, MemoryBytesAvailable: 15 << 30,
		WritableStorageBytesTotal: 100 << 30, ContainerRootfsBytes: 10 << 30,
		ImageCacheBytesTotal: 10 << 30, LogBytesTotal: 2 << 30}
}

func TestTrustedHeadroomNamesTheBindingDimension(t *testing.T) {
	budget := headroomBudget()
	for _, testCase := range []struct {
		name    string
		squeeze func(*CapacitySnapshot)
		slots   int
		binding string
	}{
		// Nothing squeezed: the configured ceiling binds before any resource.
		{"ceiling", func(*CapacitySnapshot) {}, 3, DimensionSlots},
		{"cpu", func(s *CapacitySnapshot) { s.CPUMilliOtherWorkloads = 9000 }, 1, DimensionCPU},
		{"memory", func(s *CapacitySnapshot) { s.MemoryBytesAvailable = 1 << 30 }, 1, DimensionMemory},
		{"storage", func(s *CapacitySnapshot) { s.WritableStorageBytesOtherWorkloads = 80 << 30 }, 1, DimensionStorage},
		{"cache", func(s *CapacitySnapshot) { s.ImageCacheBytesUsed = 9 << 30 }, 1, DimensionCache},
		{"logs", func(s *CapacitySnapshot) { s.LogBytesUsed = 2<<30 - 128<<20 }, 1, DimensionLogs},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := headroomSnapshot()
			testCase.squeeze(&snapshot)
			headroom, err := calculateTrustedHeadroom(snapshot, budget, 0)
			if err != nil || !headroom.Complete {
				t.Fatalf("a readable host was refused: %+v %v", headroom, err)
			}
			if headroom.FreeSlots != testCase.slots {
				t.Fatalf("free slots = %d, expected %d", headroom.FreeSlots, testCase.slots)
			}
			if headroom.Binding != testCase.binding {
				t.Fatalf("binding dimension = %q, expected %q", headroom.Binding, testCase.binding)
			}
		})
	}
}

// PART 3, AND THE INVARIANT IT PROTECTS. Never admit on negative headroom;
// never fence the inventory of workers the controller owns because of it. An
// oversubscribed host is a KNOWN state and retirement is the way out of it --
// refusing the whole inventory fenced the controller out of its own recovery
// (MS-A2 2026-09-21, one worker running, legacy rollover, free memory negative).
func TestOversubscriptionFencesAdmissionButNotRetirement(t *testing.T) {
	budget := headroomBudget()
	snapshot := headroomSnapshot()
	// Worse than full: memory is overdrawn by more than a worker's worth, and
	// storage is overdrawn by less, so the report must name memory.
	snapshot.MemoryBytesOtherWorkloads = 20 << 30
	snapshot.WritableStorageBytesOtherWorkloads = 101 << 30
	headroom, err := calculateTrustedHeadroom(snapshot, budget, 1)
	if err != nil {
		t.Fatalf("an oversubscribed host was reported as unreadable: %v", err)
	}
	if !headroom.Complete {
		t.Fatal("an oversubscribed host fenced the whole inventory; Maintain cannot retire the worker that would free it")
	}
	if headroom.FreeSlots != 0 {
		t.Fatalf("admission was not fenced: free slots = %d", headroom.FreeSlots)
	}
	if headroom.Binding != DimensionMemory {
		t.Fatalf("the worst-overdrawn dimension was not named: %q", headroom.Binding)
	}
	for name, value := range map[string]int64{
		"cpu": headroom.CPUMilli, "memory": headroom.MemoryBytes, "storage": headroom.WritableStorageBytes,
		"cache": headroom.ImageCacheBytes, "logs": headroom.LogBytes,
	} {
		if value < 0 {
			t.Fatalf("%s was reported negative rather than clamped: %d", name, value)
		}
	}
}

// The other direction, and it is the one that must not relax: a host that
// cannot be READ is still fenced completely. Oversubscribed is a measurement;
// unreadable is the absence of one.
func TestAnUnreadableHostIsStillFencedCompletely(t *testing.T) {
	budget := headroomBudget()
	for _, testCase := range []struct {
		name   string
		mutate func(*CapacitySnapshot)
	}{
		{"incomplete snapshot", func(s *CapacitySnapshot) { s.Complete = false }},
		{"negative input", func(s *CapacitySnapshot) { s.MemoryBytesTotal = -1 }},
		{"cache used beyond capacity", func(s *CapacitySnapshot) { s.ImageCacheBytesUsed = s.ImageCacheBytesTotal + 1 }},
		{"storage used beyond capacity", func(s *CapacitySnapshot) { s.WritableStorageBytesUsed = s.WritableStorageBytesTotal + 1 }},
		{"rootfs disagrees with the budget", func(s *CapacitySnapshot) { s.ContainerRootfsBytes = 1 << 30 }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			snapshot := headroomSnapshot()
			testCase.mutate(&snapshot)
			headroom, err := calculateTrustedHeadroom(snapshot, budget, 0)
			if err == nil || headroom.Complete {
				t.Fatalf("an unreadable host was accepted as merely full: %+v %v", headroom, err)
			}
		})
	}
}

// homelab#983. With dockerd's data root on its own thin device, a worker's
// disks carry real job data, and a new thin device is only clean if the pool
// zeroes newly provisioned blocks. lvs reports that as `z` at position 8 of
// lv_attr (index 7). The witness refuses a pool without it -- and refuses an
// attribute string too short to say, rather than reading absence as yes.
func TestThinPoolWitnessRequiresZeroingOfNewBlocks(t *testing.T) {
	report := func(attr string) string {
		return `{"report":[{"lv":[{"vg_name":"ubuntu-vg","lv_name":"sjo_runner_pool","lv_size":"107374182400",` +
			`"data_percent":"25.00","metadata_percent":"5.00","lv_attr":"` + attr + `"}]}]}`
	}
	for _, testCase := range []struct {
		attr   string
		accept bool
	}{
		{"twi-aotz--", true},  // the live pool, measured 2026-09-25
		{"twi-a-tz--", true},  // the probe pool's captured report
		{"twi-aot---", false}, // zeroing off
		{"twi-aotZ--", false}, // not the lowercase flag lvs prints
		{"twi-aot", false},    // too short to carry position 8
		{"t", false},
		{"", false},
	} {
		t.Run(testCase.attr, func(t *testing.T) {
			collector, _ := hostCapacityFixture(t)
			writeThinPoolReport(t, collector.config.ThinPoolReportPath, report(testCase.attr))
			snapshot, err := collector.ObserveCapacity(context.Background(), "github-runners")
			if testCase.accept {
				if err != nil || !snapshot.Complete {
					t.Fatalf("a zeroing pool was refused: %v", err)
				}
				return
			}
			if err == nil || snapshot.Complete {
				t.Fatalf("lv_attr %q was accepted", testCase.attr)
			}
		})
	}
}
