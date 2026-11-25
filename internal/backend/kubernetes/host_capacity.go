package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var lvmNameRE = regexp.MustCompile(`^[A-Za-z0-9_+.-]+$`)

// maxThinPoolReportBytes bounds the attestation file. The real report is under
// 300 bytes; 64 KiB is far above any honest one and far below a file that could
// exhaust the witness reading it.
const maxThinPoolReportBytes = 64 << 10

// thinPoolFutureSkew is how far ahead of the reader's clock a report may be
// dated. A future mtime satisfies any age bound forever, so it cannot simply be
// accepted, but the writer and the reader are two processes with one clock and
// a second or two of ordinary skew is not evidence of anything.
const thinPoolFutureSkew = 5 * time.Second

type HostCapacityConfig struct {
	Namespace string
	// ThinPoolReportPath replaced LVSExecutable. The witness no longer runs
	// `lvs` at all -- see readThinPoolReport for the two measured reasons it
	// could not.
	ThinPoolReportPath          string
	ThinPoolReportMaxAgeSeconds int
	VolumeGroup                 string
	ThinPool                    string
	ImageCachePath              string
	ImageCacheBudget            int64
	LogPath                     string
	LogBudget                   int64
	ProcMeminfo                 string
	StorageReserveBytes         int64
	ContainerRootfsBytes        int64
	RuntimeOverhead             Footprint
	MaxMetadataPercent          float64
}

// HostCapacityCollector reports the dedicated thin pool and bounded cache/log
// paths. CPU/RAM allocatable values and non-pool Pod reservations deliberately
// come from KubernetesCapacitySource instead.
type HostCapacityCollector struct {
	config HostCapacityConfig
	now    func() time.Time
}

var _ CapacitySource = (*HostCapacityCollector)(nil)

func NewHostCapacityCollector(config HostCapacityConfig) (*HostCapacityCollector, error) {
	if config.Namespace == "" || config.ThinPoolReportPath == "" || !filepath.IsAbs(config.ThinPoolReportPath) ||
		config.ThinPoolReportMaxAgeSeconds <= 0 || config.ThinPoolReportMaxAgeSeconds > maxMeasuredAgeSeconds ||
		!lvmNameRE.MatchString(config.VolumeGroup) || !lvmNameRE.MatchString(config.ThinPool) ||
		config.ImageCachePath == "" || !filepath.IsAbs(config.ImageCachePath) || config.ImageCacheBudget <= 0 ||
		config.LogPath == "" || !filepath.IsAbs(config.LogPath) || config.LogBudget <= 0 || config.StorageReserveBytes < 0 ||
		config.ProcMeminfo == "" || !filepath.IsAbs(config.ProcMeminfo) ||
		config.ContainerRootfsBytes <= 0 ||
		config.RuntimeOverhead.CPUMilli < 0 || config.RuntimeOverhead.MemoryBytes < 0 || config.RuntimeOverhead.StorageBytes < 0 ||
		config.MaxMetadataPercent <= 0 || config.MaxMetadataPercent >= 100 {
		return nil, errors.New("host capacity collector configuration is incomplete")
	}
	return &HostCapacityCollector{config: config, now: time.Now}, nil
}

type lvsReport struct {
	Report []struct {
		LV []struct {
			VGName          string `json:"vg_name"`
			LVName          string `json:"lv_name"`
			LVSize          string `json:"lv_size"`
			DataPercent     string `json:"data_percent"`
			MetadataPercent string `json:"metadata_percent"`
			LVAttr          string `json:"lv_attr"`
		} `json:"lv"`
	} `json:"report"`
}

func (c *HostCapacityCollector) ObserveCapacity(ctx context.Context, namespace string) (CapacitySnapshot, error) {
	if c == nil || ctx == nil || namespace != c.config.Namespace {
		return CapacitySnapshot{Complete: false}, errors.New("host capacity scope mismatch")
	}
	output, err := readThinPoolReport(c.config.ThinPoolReportPath,
		time.Duration(c.config.ThinPoolReportMaxAgeSeconds)*time.Second, c.now())
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	var report lvsReport
	if err := json.Unmarshal(output, &report); err != nil || len(report.Report) != 1 {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin-pool report is malformed")
	}
	// An EMPTY LV list gets its own refusal because it has its own cause and it
	// is the one that already bit us: lvs with no access to /dev/mapper/control
	// exits ZERO and prints {"report":[{"lv":[]}]}. Reported as "malformed" it
	// reads as a parsing problem, which is how three rollouts went looking in
	// the wrong place.
	if len(report.Report[0].LV) == 0 {
		return CapacitySnapshot{Complete: false},
			errors.New("dedicated thin-pool report names no logical volume; lvs ran without access to the device mapper")
	}
	if len(report.Report[0].LV) != 1 {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin-pool report names more than one logical volume")
	}
	lv := report.Report[0].LV[0]
	if lv.VGName != c.config.VolumeGroup || lv.LVName != c.config.ThinPool || len(lv.LVAttr) < 1 || lv.LVAttr[0] != 't' {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin-pool identity or type changed")
	}
	// Zeroing, position 8 of lv_attr (index 7): `z` means the pool zeroes each
	// newly provisioned block before a thin device can read it. Since
	// homelab#983 dockerd's data root is on its own thin device, so a worker's
	// disks carry real job data, and without zeroing the next worker's fresh
	// device could read the last one's blocks. A string too short to carry the
	// flag is refused too: absence is not the flag.
	if len(lv.LVAttr) < 8 || lv.LVAttr[7] != 'z' {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin pool does not zero new blocks (lv_attr position 8 is not z)")
	}
	total, err := parsePositiveInt(lv.LVSize)
	if err != nil {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin-pool size is invalid")
	}
	dataPercent, err := parsePercent(lv.DataPercent)
	if err != nil {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin-pool data usage is invalid")
	}
	metadataPercent, err := parsePercent(lv.MetadataPercent)
	if err != nil || metadataPercent >= c.config.MaxMetadataPercent {
		return CapacitySnapshot{Complete: false}, errors.New("dedicated thin-pool metadata headroom is unsafe")
	}
	used := int64(math.Ceil(float64(total) * dataPercent / 100))
	imageUsed, err := boundedTreeUsage(c.config.ImageCachePath, c.config.ImageCacheBudget)
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	logUsed, err := boundedTreeUsage(c.config.LogPath, c.config.LogBudget)
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	imageTotal, err := boundedPathTotal(c.config.ImageCachePath, imageUsed, c.config.ImageCacheBudget)
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	logTotal, err := boundedPathTotal(c.config.LogPath, logUsed, c.config.LogBudget)
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	memoryAvailable, err := readMemoryAvailable(c.config.ProcMeminfo)
	if err != nil {
		return CapacitySnapshot{Complete: false}, err
	}
	return CapacitySnapshot{Complete: true,
		CPUMilliRuntimeOverhead: c.config.RuntimeOverhead.CPUMilli, MemoryBytesAvailable: memoryAvailable,
		MemoryBytesRuntimeOverhead: c.config.RuntimeOverhead.MemoryBytes,
		WritableStorageBytesTotal:  total, WritableStorageBytesUsed: used,
		WritableStorageBytesOtherWorkloads: c.config.StorageReserveBytes, WritableStorageBytesRuntimeOverhead: c.config.RuntimeOverhead.StorageBytes,
		ContainerRootfsBytes: c.config.ContainerRootfsBytes,
		ImageCacheBytesTotal: imageTotal, ImageCacheBytesUsed: imageUsed,
		LogBytesTotal: logTotal, LogBytesUsed: logUsed}, nil
}

// readThinPoolReport reads the thin-pool attestation a root host timer writes,
// and refuses anything it cannot prove is that file.
//
// WHY THE WITNESS NO LONGER RUNS `lvs`. Both failures were measured on the real
// MS-A2 with a faithful stand-in (systemd-run, ReadOnlyPaths=/,
// DevicePolicy=closed, the witness's capability set):
//
//  1. With / read-only, lvs exits 5 with "File locking initialisation failed".
//  2. Without device access -- the witness pod is not privileged, so the device
//     cgroup denies /dev/mapper/control -- lvs exits ZERO and prints
//     {"report":[{"lv":[]}]}. An empty LV list. That is the dangerous one: a
//     successful-looking run whose only symptom was
//     "dedicated thin-pool report is malformed" one layer up.
//
// The fix is not to privilege the witness. It mirrors the nft-guard
// attestation exactly: a root systemd timer on the host runs lvs and publishes
// the JSON; the witness reads a file, which is something it already does for
// four fingerprints. The witness gains no device, no capability and no binary.
//
// The empty-LV shape stays refused here on its own merits -- the host script
// will not publish one -- because "the file exists and parses" must never be
// enough on its own.
//
// WHAT IS PROVEN BEFORE PARSING, and why each one:
//   - Lstat says regular file: a symlink could aim the read anywhere.
//   - The SAME fd is fstat'd after opening: checking the path and then opening
//     it is two different objects if anything swaps the name in between.
//   - Owner: the reader's own uid, which main pins to root before the witness
//     starts. Anyone who can write this file can forge capacity.
//   - No group or other write bit, and the same for the parent directory: a
//     writable parent means the file can be replaced wholesale regardless of
//     its own mode.
//   - Size ceiling, so a reader cannot be made to swallow a large file.
//   - Age, because a stopped timer otherwise leaves the last good report
//     attesting forever. This is the same control the guard attestation uses,
//     and the same reason.
func readThinPoolReport(path string, maxAge time.Duration, now time.Time) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("dedicated thin-pool report is unavailable")
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("dedicated thin-pool report is not a regular file")
	}
	if err := checkThinPoolDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("dedicated thin-pool report cannot be opened")
	}
	defer file.Close()
	// Everything below reads the OPEN FILE, not the name. A check against the
	// path followed by an open against the path is a race with whoever can
	// rename it.
	opened, err := file.Stat()
	if err != nil {
		return nil, errors.New("dedicated thin-pool report cannot be inspected")
	}
	if !opened.Mode().IsRegular() {
		return nil, errors.New("dedicated thin-pool report is not a regular file")
	}
	if err := checkThinPoolOwnership(opened); err != nil {
		return nil, err
	}
	if opened.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("dedicated thin-pool report is group or world writable")
	}
	if opened.Size() > maxThinPoolReportBytes {
		return nil, errors.New("dedicated thin-pool report exceeds its size ceiling")
	}
	age := now.Sub(opened.ModTime())
	if age < -thinPoolFutureSkew {
		return nil, errors.New("dedicated thin-pool report is dated in the future")
	}
	if age > maxAge {
		return nil, errors.New("dedicated thin-pool report is stale; the host measurement is not running")
	}
	// LimitReader one byte past the ceiling, so a file that grew between the
	// fstat and the read is caught rather than silently truncated.
	data, err := io.ReadAll(io.LimitReader(file, maxThinPoolReportBytes+1))
	if err != nil {
		return nil, errors.New("dedicated thin-pool report cannot be read")
	}
	if len(data) > maxThinPoolReportBytes {
		return nil, errors.New("dedicated thin-pool report exceeds its size ceiling")
	}
	return data, nil
}

// checkThinPoolDirectory refuses a parent directory anyone but its owner can
// write. A perfectly-moded file inside a world-writable directory can be
// renamed away and replaced, so the file's own mode proves nothing without this.
func checkThinPoolDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() {
		return errors.New("dedicated thin-pool report directory is unavailable")
	}
	if err := checkThinPoolOwnership(info); err != nil {
		return errors.New("dedicated thin-pool report directory has an unexpected owner")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("dedicated thin-pool report directory is group or world writable")
	}
	return nil
}

// checkThinPoolOwnership accepts root, or the reader's own uid.
//
// In production those are the same condition: the witness binary refuses to
// start unless its euid is 0 (cmd/runner-pool-host-witness), so "owned by the
// reader" IS "owned by root". Writing it this way rather than hardcoding 0
// means the property under test -- nobody less privileged than the reader can
// write this file -- is verifiable on an ordinary CI runner instead of only
// under root. The discriminating case, a file owned by some OTHER uid, is
// refused by both readings and has its own root-only test.
func checkThinPoolOwnership(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("dedicated thin-pool report ownership cannot be determined")
	}
	if stat.Uid != 0 && stat.Uid != uint32(os.Geteuid()) {
		return errors.New("dedicated thin-pool report has an unexpected owner")
	}
	return nil
}

func readMemoryAvailable(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, errors.New("host MemAvailable observation failed")
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "MemAvailable:" || fields[2] != "kB" {
			continue
		}
		kib, err := parsePositiveInt(fields[1])
		if err != nil || kib > math.MaxInt64/1024 {
			return 0, errors.New("host MemAvailable value is invalid")
		}
		return kib * 1024, nil
	}
	return 0, errors.New("host MemAvailable value is missing")
}

func parsePositiveInt(value string) (int64, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed <= 0 {
		return 0, errors.New("value must be a positive integer")
	}
	return parsed, nil
}

func parsePercent(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 || parsed > 100 {
		return 0, errors.New("value must be a finite percent")
	}
	return parsed, nil
}

func boundedTreeUsage(root string, budget int64) (int64, error) {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return 0, fmt.Errorf("bounded usage root %s is unavailable or unsafe", filepath.Base(root))
	}
	var used int64
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() < 0 || used > budget-info.Size() {
			return errors.New("bounded usage exceeds configured budget")
		}
		used += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure bounded host path: %w", err)
	}
	return used, nil
}

// boundedPathTotal gives each measured path at most half of the filesystem's
// currently available bytes. This remains safe when cache and logs share one
// filesystem, while the configured budgets retain a smaller explicit ceiling.
func boundedPathTotal(root string, used, budget int64) (int64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(root, &stats); err != nil || stats.Bsize <= 0 {
		return 0, fmt.Errorf("measure filesystem headroom for %s", filepath.Base(root))
	}
	blockSize := uint64(stats.Bsize)
	if uint64(stats.Bavail) > uint64(math.MaxInt64)/blockSize {
		return 0, errors.New("filesystem headroom exceeds supported range")
	}
	available := int64(uint64(stats.Bavail) * blockSize)
	if used < 0 || used > budget || available/2 > math.MaxInt64-used {
		return 0, errors.New("bounded path usage or headroom is invalid")
	}
	total := used + available/2
	if total > budget {
		total = budget
	}
	if total <= 0 || total < used {
		return 0, errors.New("bounded path has no safe capacity")
	}
	return total, nil
}
