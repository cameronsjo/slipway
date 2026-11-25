package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
)

const processLedgerVersion = 1

type hostCommandRunner func(context.Context, string, ...string) ([]byte, error)

// HostProcessConfig binds the CRI and /proc observer to one dedicated runtime
// and controller ownership scope. The ledger contains identities only; it
// never stores App, registration, JIT, Kubernetes, or bootstrap credentials.
type HostProcessConfig struct {
	CRIExecutable   string
	RuntimeEndpoint string
	ProcRoot        string
	LedgerFile      string
	Namespace       string
	OwnerUID        string
	RuntimeHandler  string
	RuntimeClass    string
}

type HostProcessCollector struct {
	config HostProcessConfig
	run    hostCommandRunner
	mu     sync.Mutex
	// liveSandboxes records which ledger sandbox IDs the last COMPLETED
	// ListProcesses saw alive -- a CRI Pod sandbox, a Kata runtime process, or
	// both. liveObserved says whether that observation finished; a failed pass
	// leaves it false and nothing downstream may draw a conclusion from the map.
	//
	// This exists so the storage collector can prune a ledger entry that is
	// provably gone. "Provably" is the whole point: an entry may only be dropped
	// when all three legs agree it is absent, and a pass that could not observe
	// one of them is not agreement.
	liveSandboxes map[string]bool
	liveObserved  bool
	// readProcFile reads one /proc/<pid>/cmdline. Nil is os.ReadFile; tests set
	// it because a regular file cannot be made to fail with ESRCH.
	readProcFile func(string) ([]byte, error)
}

var _ ProcessSource = (*HostProcessCollector)(nil)

func NewHostProcessCollector(config HostProcessConfig) (*HostProcessCollector, error) {
	if config.CRIExecutable == "" || !filepath.IsAbs(config.CRIExecutable) || config.RuntimeEndpoint == "" ||
		config.ProcRoot == "" || !filepath.IsAbs(config.ProcRoot) || config.LedgerFile == "" || !filepath.IsAbs(config.LedgerFile) ||
		config.Namespace == "" || config.OwnerUID == "" || config.RuntimeHandler == "" || config.RuntimeClass == "" {
		return nil, errors.New("host process collector configuration is incomplete")
	}
	endpoint, err := url.Parse(config.RuntimeEndpoint)
	if err != nil || endpoint.Scheme != "unix" || endpoint.Host != "" || !filepath.IsAbs(endpoint.Path) || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("host process collector requires an absolute Unix CRI endpoint")
	}
	if config.RuntimeHandler == "runc" || config.RuntimeClass == "kata-qemu" || config.RuntimeClass == "sjo-kata-qemu-probe" {
		return nil, errors.New("host process collector requires the dedicated production Kata runtime")
	}
	return &HostProcessCollector{config: config, run: runHostCommand}, nil
}

func runHostCommand(ctx context.Context, executable string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = []string{"LANG=C", "PATH=/usr/sbin:/usr/bin:/sbin:/bin"}
	// Output() populates exec.ExitError.Stderr, and this used to throw it away.
	// newHostCommandFailure reads it -- bounded and sanitized -- so the first
	// line of a tool's refusal reaches the witness log instead of being
	// discarded at the one place it existed.
	output, err := command.Output()
	if err != nil {
		return nil, newHostCommandFailure(filepath.Base(executable), err)
	}
	return output, nil
}

type criPodList struct {
	Items []criPod `json:"items"`
}

type criPod struct {
	ID       string `json:"id"`
	Metadata struct {
		Name      string `json:"name"`
		UID       string `json:"uid"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	RuntimeHandler string `json:"runtimeHandler"`
}

type processLedger struct {
	Version int                           `json:"version"`
	Entries map[string]processLedgerEntry `json:"entries"`
}

type processLedgerEntry struct {
	Namespace    string   `json:"namespace"`
	Name         string   `json:"name"`
	PodUID       string   `json:"pod_uid"`
	LeaseID      string   `json:"lease_id"`
	SnapshotKeys []string `json:"snapshot_keys,omitempty"`
}

func (c *HostProcessCollector) ListProcesses(ctx context.Context, namespace, ownerUID string) (WitnessScope, error) {
	if c == nil || ctx == nil || namespace != c.config.Namespace || ownerUID != c.config.OwnerUID {
		return WitnessScope{Complete: false}, errors.New("host process collector scope mismatch")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Cleared up front rather than on each failure return. An early return that
	// forgot to clear it would leave the previous pass's liveness standing in for
	// this one's, which is the exact shape "never prune on an incomplete
	// observation" exists to refuse.
	c.liveObserved = false
	ledger, err := c.loadLedger()
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	output, err := c.run(ctx, c.config.CRIExecutable, "--runtime-endpoint", c.config.RuntimeEndpoint, "pods", "-o", "json")
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	var pods criPodList
	if err := json.Unmarshal(output, &pods); err != nil || pods.Items == nil {
		return WitnessScope{Complete: false}, errors.New("CRI Pod sandbox listing is malformed")
	}
	current := make(map[string]bool)
	names := make(map[string]string)
	changed := false
	for _, pod := range pods.Items {
		if pod.Metadata.Namespace != namespace || !strings.HasPrefix(pod.Metadata.Name, "slipway-linux-") {
			continue
		}
		if pod.RuntimeHandler != c.config.RuntimeHandler {
			return WitnessScope{Complete: false}, errors.New("managed worker sandbox used an unexpected runtime handler")
		}
		entry, err := c.entryFromPod(pod)
		if err != nil {
			return WitnessScope{Complete: false}, err
		}
		if existing, ok := names[entry.Name]; ok && existing != pod.ID {
			return WitnessScope{Complete: false}, errors.New("multiple CRI sandboxes claim one worker name")
		}
		names[entry.Name] = pod.ID
		current[pod.ID] = true
		if known, ok := ledger.Entries[pod.ID]; ok && !sameProcessEntryIdentity(known, entry) {
			return WitnessScope{Complete: false}, errors.New("CRI sandbox identity changed across observations")
		} else if !ok {
			ledger.Entries[pod.ID] = entry
			changed = true
		}
	}
	if changed {
		if err := c.saveLedger(ledger); err != nil {
			return WitnessScope{Complete: false}, err
		}
	}
	processes, err := c.readRuntimeProcesses(ledger.Entries)
	if err != nil {
		return WitnessScope{Complete: false}, err
	}
	live := make(map[string]bool, len(ledger.Entries))
	for sandboxID := range ledger.Entries {
		if current[sandboxID] || processes[sandboxID] {
			live[sandboxID] = true
		}
	}
	c.liveSandboxes, c.liveObserved = live, true
	witnesses := make([]PhysicalWitness, 0, len(ledger.Entries))
	for sandboxID, entry := range ledger.Entries {
		if !live[sandboxID] {
			continue
		}
		witnesses = append(witnesses, PhysicalWitness{Namespace: entry.Namespace, Name: entry.Name, UID: entry.PodUID,
			Labels: c.labels(entry), RuntimeClassName: c.config.RuntimeClass})
	}
	sort.Slice(witnesses, func(left, right int) bool { return witnesses[left].Name < witnesses[right].Name })
	return WitnessScope{Complete: true, Items: witnesses}, nil
}

func sameProcessEntryIdentity(left, right processLedgerEntry) bool {
	return left.Namespace == right.Namespace && left.Name == right.Name && left.PodUID == right.PodUID && left.LeaseID == right.LeaseID
}

func (c *HostProcessCollector) entryFromPod(pod criPod) (processLedgerEntry, error) {
	if len(pod.ID) != 64 {
		return processLedgerEntry{}, errors.New("managed CRI sandbox ID is not a full containerd identity")
	}
	for _, character := range pod.ID {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return processLedgerEntry{}, errors.New("managed CRI sandbox ID is not lowercase hexadecimal")
		}
	}
	leaseID := strings.TrimPrefix(pod.Metadata.Name, "slipway-linux-")
	parsed, err := uuid.Parse(leaseID)
	if err != nil || parsed.String() != leaseID || pod.Metadata.UID == "" {
		return processLedgerEntry{}, errors.New("managed CRI sandbox has invalid Pod or lease identity")
	}
	return processLedgerEntry{Namespace: c.config.Namespace, Name: pod.Metadata.Name, PodUID: pod.Metadata.UID, LeaseID: leaseID}, nil
}

func (c *HostProcessCollector) labels(entry processLedgerEntry) map[string]string {
	return map[string]string{LabelManagedBy: ManagedByValue, LabelPlatform: PlatformLinux, LabelOwnerUID: c.config.OwnerUID,
		LabelLeaseID: entry.LeaseID, LabelResourceID: entry.Name}
}

func (c *HostProcessCollector) readRuntimeProcesses(ledger map[string]processLedgerEntry) (map[string]bool, error) {
	entries, err := os.ReadDir(c.config.ProcRoot)
	if err != nil {
		return nil, fmt.Errorf("read host process table: %w", err)
	}
	known := make(map[string]bool)
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "" || strings.Trim(entry.Name(), "0123456789") != "" {
			continue
		}
		readFile := c.readProcFile
		if readFile == nil {
			readFile = os.ReadFile
		}
		commandLine, err := readFile(filepath.Join(c.config.ProcRoot, entry.Name(), "cmdline"))
		if processExited(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read host process %s: %w", entry.Name(), err)
		}
		fields := bytes.Split(commandLine, []byte{0})
		if len(fields) == 0 || !isKataRuntimeProcess(filepath.Base(string(fields[0]))) {
			continue
		}
		for sandboxID := range ledger {
			if bytes.Contains(commandLine, []byte(sandboxID)) {
				known[sandboxID] = true
			}
		}
	}
	return known, nil
}

// processExited reports whether a /proc read failed only because the process
// exited between the directory listing and the read. Two errors mean that, and
// they depend on WHEN in its exit the read lands:
//
//   - ENOENT (os.ErrNotExist): the /proc/<pid> entry is already gone, so the
//     open fails.
//   - ESRCH: the open succeeded, but the process was reaped before the read.
//     The kernel's cmdline reader looks the task up again and returns
//     "no such process". The witness logged exactly that on MS-A2 2026-10-01
//     (`read host process <pid>: ... cmdline: no such process`) and refused the
//     pass (homelab#1213).
//
// Both describe a process that no longer exists, which is not a Kata runtime
// process for any sandbox. Every other read error still refuses: a /proc this
// collector cannot read is not evidence of anything.
func processExited(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

func isKataRuntimeProcess(name string) bool {
	return strings.HasPrefix(name, "qemu-system-") || name == "containerd-shim-kata-v2" || name == "virtiofsd"
}

func (c *HostProcessCollector) loadLedger() (processLedger, error) {
	ledger := processLedger{Version: processLedgerVersion, Entries: make(map[string]processLedgerEntry)}
	data, err := os.ReadFile(c.config.LedgerFile)
	if errors.Is(err, os.ErrNotExist) {
		return ledger, nil
	}
	if err != nil {
		return processLedger{}, fmt.Errorf("read host process ledger: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ledger); err != nil || ledger.Version != processLedgerVersion || ledger.Entries == nil {
		return processLedger{}, errors.New("host process ledger is malformed")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return processLedger{}, errors.New("host process ledger contains trailing data")
	}
	for sandboxID, entry := range ledger.Entries {
		pod := criPod{ID: sandboxID, RuntimeHandler: c.config.RuntimeHandler}
		pod.Metadata.Name, pod.Metadata.UID, pod.Metadata.Namespace = entry.Name, entry.PodUID, entry.Namespace
		if entry.Namespace != c.config.Namespace || entry.LeaseID == "" {
			return processLedger{}, errors.New("host process ledger contains a foreign scope")
		}
		validated, err := c.entryFromPod(pod)
		if err != nil || !sameProcessEntryIdentity(validated, entry) {
			return processLedger{}, errors.New("host process ledger contains an invalid identity")
		}
		seenSnapshots := make(map[string]bool)
		for _, key := range entry.SnapshotKeys {
			if key == "" || strings.ContainsAny(key, "\x00\r\n") || seenSnapshots[key] {
				return processLedger{}, errors.New("host process ledger contains an invalid snapshot identity")
			}
			seenSnapshots[key] = true
		}
	}
	return ledger, nil
}

func (c *HostProcessCollector) saveLedger(ledger processLedger) error {
	directory := filepath.Dir(c.config.LedgerFile)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create host process ledger directory: %w", err)
	}
	next := c.config.LedgerFile + ".next"
	file, err := os.OpenFile(next, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create host process ledger candidate: %w", err)
	}
	encoder := json.NewEncoder(file)
	writeErr := encoder.Encode(ledger)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write host process ledger: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close host process ledger: %w", closeErr)
	}
	if err := os.Rename(next, c.config.LedgerFile); err != nil {
		return fmt.Errorf("commit host process ledger: %w", err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open host process ledger directory: %w", err)
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return fmt.Errorf("sync host process ledger directory: %w", err)
	}
	return closeErr
}
