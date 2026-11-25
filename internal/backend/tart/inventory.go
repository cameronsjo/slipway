package tart

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/cameronsjo/slipway/internal/controller"
)

// tartListEntry is the subset of `tart list --format json` this adapter reads.
// Running state is carried by different fields across Tart versions, so both
// are optional and either one answers the question; neither present means the
// listing could not be understood and the whole inventory is incomplete.
type tartListEntry struct {
	Name    string  `json:"Name"`
	State   *string `json:"State"`
	Running *bool   `json:"Running"`
}

func (e tartListEntry) running() (bool, bool) {
	if e.State != nil {
		return strings.EqualFold(*e.State, "running"), true
	}
	if e.Running != nil {
		return *e.Running, true
	}
	return false, false
}

// vmConfig is the subset of ~/.tart/vms/<name>/config.json this adapter reads.
// "os" is the only field that says whether a VM charges one of Apple's two
// macOS guest slots; `tart list` carries no OS field at all.
type vmConfig struct {
	OS         string `json:"os"`
	MACAddress string `json:"macAddress"`
}

// Inventory joins three independent sources: the ledger (what this controller
// believes it owns), `tart list` (what Tart's database says exists), and the
// VM directory listing (what is on disk). Complete is false if any of them
// failed, because a partial join cannot distinguish "absent" from "unread".
func (b *Backend) Inventory(ctx context.Context) (controller.BackendInventory, error) {
	if ctx == nil {
		return controller.BackendInventory{}, errors.New("tart inventory requires a context")
	}
	owned, err := b.readLedger()
	if err != nil {
		b.log.Error("tart inventory incomplete", slog.String("source", "ledger"))
		return controller.BackendInventory{}, err
	}
	listed, err := b.listVMs(ctx)
	if err != nil {
		b.log.Error("tart inventory incomplete", slog.String("source", "tart-list"))
		return controller.BackendInventory{}, err
	}
	onDisk, linked, err := b.vmDirectories()
	if err != nil {
		b.log.Error("tart inventory incomplete", slog.String("source", "vm-directory"))
		return controller.BackendInventory{}, err
	}

	names := map[string]bool{}
	for name := range listed {
		if strings.HasPrefix(name, ownedPrefix) {
			names[name] = true
		}
	}
	for _, name := range onDisk {
		if strings.HasPrefix(name, ownedPrefix) {
			names[name] = true
		}
	}
	for name := range owned {
		names[name] = true
	}

	workers := make([]controller.WorkerObservation, 0, len(names))
	seenLease := map[string]bool{}
	ownedPresent := 0
	for _, name := range sortedKeys(names) {
		leaseID, tracked := owned[name]
		// A symlinked VM directory is never this controller's, whatever the
		// ledger says: the bytes it points at are chosen by whoever made the
		// link, not by the adapter.
		if linked[name] {
			b.log.Warn("tart inventory found a symlinked VM directory; reporting it as untracked",
				slog.String("resource_id", name))
			tracked = false
		}
		if !tracked {
			// No ledger entry. Reporting the VM name as the lease identity
			// makes it untracked by construction: the controller looks the
			// lease up in its journal, finds nothing, and fences. It can never
			// collide with a real lease, which is a bare UUID.
			b.log.Warn("tart inventory found a pool-shaped VM with no ledger entry",
				slog.String("resource_id", name))
			workers = append(workers, controller.WorkerObservation{LeaseID: name, ResourceID: name})
			continue
		}
		if seenLease[leaseID] {
			return controller.BackendInventory{}, errors.New("tart ledger maps one lease to more than one VM")
		}
		seenLease[leaseID] = true
		ownedPresent++
		workers = append(workers, controller.WorkerObservation{LeaseID: leaseID, ResourceID: name})
	}

	free, err := b.freeSlots(ctx, listed, ownedPresent)
	if err != nil {
		return controller.BackendInventory{}, err
	}
	return controller.BackendInventory{Complete: true, FreeSlots: free, Workers: workers}, nil
}

// freeSlots reports the physical number: the host ceiling minus every running
// darwin guest, the legacy ci-guest included. It is forced to zero when the
// memory gate refuses or when this controller already owns MaxOwnedVMs, so the
// pool reads as "no capacity" before it pages out the inference server and
// before it can be asked for a VM Create would refuse. While it owns fewer, it
// is clamped to the owned-VM headroom: an owned VM that is not running yet
// charges no physical slot but does charge the cap. With nothing owned it stays
// the physical number, as it was before the cap was configurable, so at a cap
// of 1 it can still read 2 while ci-guest is stopped; the controller bounds
// admission by its own pool slots, and -max-owned-vms runs at 1 while they are
// fewer (EffectiveMaxOwnedVMs). Where the controller still admits past the cap,
// Create refuses with controller.ErrCapacity, so the refusal is a wait and never
// a provisioning failure.
func (b *Backend) freeSlots(ctx context.Context, listed map[string]tartListEntry, ownedPresent int) (int, error) {
	runningDarwin := 0
	for name, entry := range listed {
		running, known := entry.running()
		if !known {
			return 0, errors.New("tart listing does not report running state")
		}
		if !running {
			continue
		}
		isDarwin, err := b.vmIsDarwin(name)
		if err != nil {
			return 0, err
		}
		if isDarwin {
			runningDarwin++
		}
	}
	free := b.config.PhysicalVMCapacity - runningDarwin
	if free < 0 {
		free = 0
	}
	// Zero owned keeps the physical number, as before the cap was configurable.
	// The controller already bounds admission by its own pool slots.
	if ownedPresent > 0 {
		free = min(free, max(b.config.MaxOwnedVMs-ownedPresent, 0))
	}
	if free > 0 {
		ok, err := b.memoryAvailable(ctx)
		if err != nil || !ok {
			b.log.Warn("tart free slots forced to zero by the host memory gate",
				slog.Int64("floor_bytes", b.config.MemoryFloorBytes), slog.Bool("read_failed", err != nil))
			free = 0
		}
	}
	return free, nil
}

func (b *Backend) listVMs(ctx context.Context) (map[string]tartListEntry, error) {
	out, err := b.runner.Run(ctx, []string{b.config.TartBinary, "list", "--format", "json"}, b.tartEnv(), nil)
	if err != nil {
		return nil, errors.New("tart listing failed")
	}
	var entries []tartListEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		return nil, errors.New("tart listing is not valid JSON")
	}
	listed := make(map[string]tartListEntry, len(entries))
	for _, entry := range entries {
		if entry.Name == "" {
			return nil, errors.New("tart listing contains an unnamed VM")
		}
		if _, duplicate := listed[entry.Name]; duplicate {
			return nil, errors.New("tart listing contains a duplicate VM name")
		}
		listed[entry.Name] = entry
	}
	return listed, nil
}

func (b *Backend) vmDirectories() ([]string, map[string]bool, error) {
	entries, err := os.ReadDir(filepath.Join(b.config.TartHome, "vms"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, map[string]bool{}, nil
	}
	if err != nil {
		return nil, nil, errors.New("tart VM directory is unreadable")
	}
	// os.ReadDir reports each entry's own type (lstat semantics), so a
	// symlinked VM directory is a symlink here, not a directory. Skipping it
	// would make a VM planted behind a symlink invisible to the whole
	// three-source join -- present on disk, absent from inventory, unfenced.
	// Both shapes are collected; linked reports which are symlinks, and the
	// caller treats those as unsafe regardless of the ledger.
	names := make([]string, 0, len(entries))
	linked := map[string]bool{}
	for _, entry := range entries {
		switch {
		case entry.Type()&os.ModeSymlink != 0:
			linked[entry.Name()] = true
			names = append(names, entry.Name())
		case entry.IsDir():
			names = append(names, entry.Name())
		}
	}
	return names, linked, nil
}

// vmIsDarwin reads the guest OS from the VM's own config.json. A VM listed by
// Tart with no readable config is an error: guessing "not darwin" would
// under-count the slots Apple is actually charging.
func (b *Backend) vmIsDarwin(name string) (bool, error) {
	config, err := b.readVMConfig(name)
	if errors.Is(err, os.ErrNotExist) {
		// A remote base image is listed by Tart and has no local VM directory.
		// It is not running, so it charges no slot; callers only reach here for
		// running entries, where an absent directory is genuinely unreadable.
		return false, errors.New("running tart VM has no local configuration to read its OS from")
	}
	if err != nil {
		return false, err
	}
	if config.OS == "" {
		return false, errors.New("tart VM configuration does not name its guest OS")
	}
	return strings.EqualFold(config.OS, "darwin"), nil
}

// maxSmallFileBytes bounds every config.json and ledger read. Tart writes well
// under 4 KiB, and a ledger entry is one UUID.
const maxSmallFileBytes = 64 << 10

// errUnreadableSmallFile is readSmallRegularFile's refusal for anything but a
// bounded regular file.
var errUnreadableSmallFile = errors.New("not a readable regular file within the size bound")

// readSmallRegularFile opens without blocking, does not follow a symlink at
// path, and refuses anything but a regular file under maxSmallFileBytes, so a
// FIFO, device or link planted at that path cannot hang the daemon cycle or
// choose the bytes read. A missing file is returned as os.ErrNotExist so
// callers can tell "gone" from "bad".
func readSmallRegularFile(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, errUnreadableSmallFile
	}
	defer func() { _ = file.Close() }() // read-only; a close error loses nothing
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxSmallFileBytes {
		return nil, errUnreadableSmallFile
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSmallFileBytes+1))
	if err != nil || len(data) > maxSmallFileBytes {
		return nil, errUnreadableSmallFile
	}
	return data, nil
}

// readVMConfig reads <tart_home>/vms/<name>/config.json through
// readSmallRegularFile.
func (b *Backend) readVMConfig(name string) (vmConfig, error) {
	data, err := readSmallRegularFile(filepath.Join(b.config.TartHome, "vms", name, "config.json"))
	if errors.Is(err, os.ErrNotExist) {
		return vmConfig{}, os.ErrNotExist
	}
	if err != nil {
		return vmConfig{}, errors.New("tart VM configuration is unreadable")
	}
	var config vmConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return vmConfig{}, errors.New("tart VM configuration is malformed")
	}
	return config, nil
}

// memoryAvailable reads free plus inactive pages from vm_stat and compares the
// total against the configured floor. Inactive pages count because macOS
// reclaims them without paging; free alone reads near zero on a healthy host.
func (b *Backend) memoryAvailable(ctx context.Context) (bool, error) {
	out, err := b.runner.Run(ctx, []string{b.config.VMStatBinary}, b.hostEnv(), nil)
	if err != nil || len(strings.TrimSpace(string(out))) == 0 {
		return false, errors.New("host memory statistics are unreadable")
	}
	text := string(out)
	pageSize, err := parsePageSize(text)
	if err != nil {
		return false, err
	}
	free, err := parseVMStatPages(text, "Pages free:")
	if err != nil {
		return false, err
	}
	inactive, err := parseVMStatPages(text, "Pages inactive:")
	if err != nil {
		return false, err
	}
	available := (free + inactive) * pageSize
	if available < b.config.MemoryFloorBytes {
		b.log.Warn("tart memory gate refuses: available host memory is under the floor",
			slog.Int64("available_bytes", available), slog.Int64("floor_bytes", b.config.MemoryFloorBytes))
		return false, nil
	}
	return true, nil
}

func parsePageSize(text string) (int64, error) {
	const marker = "page size of "
	index := strings.Index(text, marker)
	if index < 0 {
		return 0, errors.New("host memory statistics do not state a page size")
	}
	rest := text[index+len(marker):]
	end := strings.Index(rest, " ")
	if end < 0 {
		return 0, errors.New("host memory statistics do not state a page size")
	}
	size, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil || size <= 0 {
		return 0, errors.New("host memory page size is not a positive number")
	}
	return size, nil
}

func parseVMStatPages(text, label string) (int64, error) {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), label) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), label))
		value = strings.TrimSuffix(value, ".")
		pages, err := strconv.ParseInt(value, 10, 64)
		if err != nil || pages < 0 {
			return 0, errors.New("host memory statistic " + label + " is not a count")
		}
		return pages, nil
	}
	return 0, errors.New("host memory statistics are missing " + label)
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
