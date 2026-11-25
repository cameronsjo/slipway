package kubernetes

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

const (
	testSandboxA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testSandboxB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testLeaseA   = "11111111-1111-4111-8111-111111111111"
)

func processCollectorFixture(t *testing.T) (*HostProcessCollector, *string, string) {
	t.Helper()
	directory := t.TempDir()
	procRoot := filepath.Join(directory, "proc")
	if err := os.Mkdir(procRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	collector, err := NewHostProcessCollector(HostProcessConfig{
		CRIExecutable: "/usr/local/bin/crictl", RuntimeEndpoint: "unix:///run/k3s/containerd/containerd.sock",
		ProcRoot: procRoot, LedgerFile: filepath.Join(directory, "state", "processes.json"), Namespace: "github-runners",
		OwnerUID: "controller-a", RuntimeHandler: "sjo-kata-qemu-runner", RuntimeClass: "sjo-kata-qemu-runner",
	})
	if err != nil {
		t.Fatal(err)
	}
	output := `{"items":[]}`
	collector.run = func(_ context.Context, executable string, arguments ...string) ([]byte, error) {
		if executable != collector.config.CRIExecutable || strings.Join(arguments, " ") != "--runtime-endpoint unix:///run/k3s/containerd/containerd.sock pods -o json" {
			return nil, errors.New("unexpected host command")
		}
		return []byte(output), nil
	}
	return collector, &output, procRoot
}

func podList(sandboxID, runtimeHandler string) string {
	return `{"items":[` + podItem(sandboxID, runtimeHandler, "pod-uid-a") + `]}`
}

func podItem(sandboxID, runtimeHandler, podUID string) string {
	return `{"id":"` + sandboxID + `","metadata":{"name":"slipway-linux-` + testLeaseA + `","uid":"` + podUID + `","namespace":"github-runners"},"runtimeHandler":"` + runtimeHandler + `"}`
}

func TestHostProcessCollectorRetainsCRIAndProcessOrphans(t *testing.T) {
	collector, output, procRoot := processCollectorFixture(t)
	*output = podList(testSandboxA, collector.config.RuntimeHandler)
	current, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a")
	if err != nil || !current.Complete || len(current.Items) != 1 || current.Items[0].UID != "pod-uid-a" || current.Items[0].Labels[LabelLeaseID] != testLeaseA {
		t.Fatalf("current sandbox was not witnessed: %+v %v", current, err)
	}
	processDirectory := filepath.Join(procRoot, "123")
	if err := os.Mkdir(processDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(processDirectory, "cmdline"), []byte("qemu-system-x86_64\x00-name\x00sandbox-"+testSandboxA+"\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	*output = `{"items":[]}`
	orphan, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a")
	if err != nil || len(orphan.Items) != 1 || orphan.Items[0].Name != current.Items[0].Name {
		t.Fatalf("orphan QEMU process lost its durable identity: %+v %v", orphan, err)
	}
	if err := os.Remove(filepath.Join(processDirectory, "cmdline")); err != nil {
		t.Fatal(err)
	}
	absent, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a")
	if err != nil || !absent.Complete || len(absent.Items) != 0 {
		t.Fatalf("complete process absence was not reported: %+v %v", absent, err)
	}
}

func TestHostProcessCollectorRejectsFallbackDuplicateAndChangedIdentity(t *testing.T) {
	collector, output, _ := processCollectorFixture(t)
	*output = podList(testSandboxA, "runc")
	if _, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("ordinary-container fallback was accepted")
	}
	*output = `{"items":[` + podItem(testSandboxA, collector.config.RuntimeHandler, "pod-uid-a") + `,` +
		podItem(testSandboxB, collector.config.RuntimeHandler, "pod-uid-a") + `]}`
	if _, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("duplicate worker-name sandboxes were accepted")
	}
	*output = podList(testSandboxA, collector.config.RuntimeHandler)
	if _, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err != nil {
		t.Fatal(err)
	}
	*output = strings.Replace(podList(testSandboxA, collector.config.RuntimeHandler), "pod-uid-a", "pod-uid-b", 1)
	if _, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil {
		t.Fatal("changed sandbox-to-Pod identity was accepted")
	}
}

func TestHostProcessCollectorFailsClosedForScopeCommandAndLedgerErrors(t *testing.T) {
	collector, output, _ := processCollectorFixture(t)
	if _, err := collector.ListProcesses(context.Background(), "other", "controller-a"); err == nil {
		t.Fatal("foreign process scope was accepted")
	}
	*output = podList(testSandboxA, collector.config.RuntimeHandler)
	collector.run = func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("CRI unavailable") }
	if result, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("CRI command failure produced a complete scope")
	}
	if err := os.MkdirAll(filepath.Dir(collector.config.LedgerFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(collector.config.LedgerFile, []byte(`{"version":1,"entries":{"bad":{}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("malformed durable ledger produced a complete scope")
	}
}

func TestHostWitnessSourcesRejectsMissingCollector(t *testing.T) {
	var sources HostWitnessSources
	if result, err := sources.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("missing process collector was accepted")
	}
	if result, err := sources.ListWritableStorage(context.Background(), "github-runners", "controller-a"); err == nil || result.Complete {
		t.Fatal("missing storage collector was accepted")
	}
	if result, err := sources.ObserveCapacity(context.Background(), "github-runners"); err == nil || result.Complete {
		t.Fatal("missing capacity collector was accepted")
	}
}

// --- a process that exits between the /proc listing and its cmdline read ----
//
// homelab#1213. The witness refused a pass on MS-A2 with `read host process
// <pid>: ... cmdline: no such process`: the /proc entry was listed, the open
// succeeded, and the process was reaped before the read, which returns ESRCH
// rather than ENOENT. Both mean the process is gone.

func TestAProcessReapedBeforeItsCmdlineReadIsSkipped(t *testing.T) {
	collector, output, procRoot := processCollectorFixture(t)
	*output = podList(testSandboxA, collector.config.RuntimeHandler)
	for _, pid := range []string{"123", "456"} {
		if err := os.Mkdir(filepath.Join(procRoot, pid), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	reaped := filepath.Join(procRoot, "456", "cmdline")
	collector.readProcFile = func(path string) ([]byte, error) {
		if path == reaped {
			return nil, &os.PathError{Op: "read", Path: path, Err: syscall.ESRCH}
		}
		return []byte("qemu-system-x86_64\x00-name\x00sandbox-" + testSandboxA + "\x00"), nil
	}
	scope, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a")
	if err != nil || !scope.Complete || len(scope.Items) != 1 {
		t.Fatalf("a process reaped mid-read refused the pass: %+v %v", scope, err)
	}
}

func TestAnUnreadableProcessIsStillRefused(t *testing.T) {
	// The tolerance is for a process that is gone, nothing wider. A cmdline the
	// collector cannot read for any other reason is not evidence of absence.
	collector, output, procRoot := processCollectorFixture(t)
	*output = podList(testSandboxA, collector.config.RuntimeHandler)
	if err := os.Mkdir(filepath.Join(procRoot, "789"), 0o700); err != nil {
		t.Fatal(err)
	}
	collector.readProcFile = func(path string) ([]byte, error) {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.EACCES}
	}
	if scope, err := collector.ListProcesses(context.Background(), "github-runners", "controller-a"); err == nil || scope.Complete {
		t.Fatal("an unreadable /proc entry was read as an absent process")
	}
}
