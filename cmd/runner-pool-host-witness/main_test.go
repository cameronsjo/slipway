package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backendkube "github.com/cameronsjo/slipway/internal/backend/kubernetes"
)

func validConfig() configFile {
	config := configFile{
		SocketPath: "/run/sjo-runner-pool/witness.sock", SocketGID: 121, ControllerUID: 1000,
		Namespace: "github-runners", OwnerUID: "controller-a", RequestTimeout: "5s",
		CRIExecutable: "/usr/local/bin/crictl", RuntimeEndpoint: "unix:///run/k3s/containerd/containerd.sock",
		ProcRoot: "/host/proc", IdentityLedger: "/var/lib/sjo-runner-pool/identities.json",
		RuntimeHandler: "sjo-kata-qemu-runner", RuntimeClass: "sjo-kata-qemu-runner",
		CTRExecutable: "/usr/local/bin/ctr", ContainerdAddress: "/run/k3s/containerd/containerd.sock",
		ContainerdNS: "k8s.io", Snapshotter: "sjo-devmapper",
		RuntimeFingerprints: []backendkube.RuntimeFingerprint{
			{Purpose: "containerd-config", Path: "/etc/sjo-runner-pool/containerd.toml", SHA256: strings.Repeat("a", 64)},
			{Purpose: "kata-config", Path: "/etc/sjo-runner-pool/kata.toml", SHA256: strings.Repeat("b", 64)},
			{Purpose: "kata-artifacts", Path: "/etc/sjo-runner-pool/artifacts.sha256", SHA256: strings.Repeat("c", 64)},
			// The guard fingerprint is written by a host timer rather than being
			// a static file, so it carries the age bound the witness enforces.
			{Purpose: "nft-guard", Path: "/opt/sjo-runner-pool/net/guard-fingerprint.current", SHA256: strings.Repeat("d", 64), MaxAgeSeconds: 120},
			// The measurement itself, pinned static: a tool that can be
			// replaced makes the fingerprint it produces meaningless.
			{Purpose: "nft-guard-tool", Path: "/opt/sjo-runner-pool/net/guard-fingerprint.py", SHA256: strings.Repeat("e", 64)},
			// The CNI conflist placing workers on the guarded bridge (homelab#875 I3).
			{Purpose: "cni-conflist", Path: "/opt/sjo-runner-pool/kata-qemu-runner/cni/10-sjo-runner-pool.conflist", SHA256: strings.Repeat("f", 64)}},
		ThinPoolReportPath: "/host/opt/sjo-runner-pool/storage/thin-pool-report.json", ThinPoolReportMaxAge: 120,
		VolumeGroup: "ubuntu-vg", ThinPool: "sjo_runner_pool",
		ImageCachePath:   "/var/lib/rancher/k3s/agent/containerd/io.containerd.content.v1.content",
		ImageCacheBudget: 20 << 30, LogPath: "/var/log/pods/github-runners", LogBudget: 4 << 30,
		ProcMeminfo: "/host/proc/meminfo", StorageReserve: 8 << 30, MaxMetadataPercent: 80,
		ContainerRootfsBytes: 20 << 30,
	}
	config.RuntimeOverhead.CPUMilli = 500
	config.RuntimeOverhead.MemoryBytes = 1 << 30
	config.RuntimeOverhead.StorageBytes = 2 << 30
	return config
}

func TestBuildServerRequiresEveryPrivilegedCollector(t *testing.T) {
	server, err := buildServer(validConfig(), nil)
	if err != nil || server == nil {
		t.Fatalf("complete host witness did not compose: %T %v", server, err)
	}
	config := validConfig()
	config.RuntimeHandler = "runc"
	if _, err := buildServer(config, nil); err == nil {
		t.Fatal("ordinary-container runtime was accepted")
	}
	config = validConfig()
	config.Snapshotter = "overlayfs"
	if _, err := buildServer(config, nil); err == nil {
		t.Fatal("ordinary-container snapshotter was accepted")
	}
}

func TestHostConfigRejectsUnknownAndTrailingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.json")
	data, err := json.Marshal(validConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte(` {}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("trailing host configuration was accepted")
	}
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("unknown host configuration field was accepted")
	}
}

func TestHostWitnessExampleMatchesStrictSchemaAndIsInert(t *testing.T) {
	config, err := loadConfig("../../config/linux-host-witness.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(config.RuntimeFingerprints) != 6 {
		t.Fatal("host example omitted a required runtime fingerprint")
	}
	guardBounded := false
	for _, fingerprint := range config.RuntimeFingerprints {
		if fingerprint.SHA256 != strings.Repeat("0", 64) {
			t.Fatal("host example contains a deployable-looking runtime fingerprint")
		}
		if fingerprint.Purpose == "nft-guard" {
			guardBounded = fingerprint.MaxAgeSeconds > 0
		}
	}
	// The example is what an operator copies. A guard entry shipped without its
	// age bound would be copied into a live config that the witness then
	// refuses to start -- or worse, silently would not, if the bound ever became
	// optional.
	if !guardBounded {
		t.Fatal("host example ships the guard fingerprint without an age bound")
	}
}
