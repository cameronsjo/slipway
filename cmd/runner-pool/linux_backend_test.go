package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func validLinuxPlatformConfig() linuxPlatformConfig {
	return linuxPlatformConfig{
		Namespace: "github-runners", OwnerUID: "controller-a", NodeName: "homelab-ms-a2",
		RuntimeClassName: "sjo-kata-qemu-runner",
		RunnerImage:      "registry.example.invalid/slipway/worker@sha256:" + strings.Repeat("a", 64),
		DockerImage:      "docker.io/library/docker@sha256:" + strings.Repeat("b", 64),
		WorkerCommand:    "/usr/local/bin/runner-pool-worker", RunnerRoot: "/actions-runner", CAFile: "/etc/runner-pool/ca.pem",
		WitnessSocket: "/run/sjo-runner-pool/witness.sock", WitnessServerUID: 0, WitnessTimeout: "5s",
		RunnerResources: linuxContainerResources{
			Requests: linuxFootprint{CPUMilli: 500, MemoryBytes: 2 << 30, StorageBytes: 3 << 30},
			Limits:   linuxFootprint{CPUMilli: 2000, MemoryBytes: 2 << 30, StorageBytes: 3 << 30}},
		DockerResources: linuxContainerResources{
			Requests: linuxFootprint{CPUMilli: 1500, MemoryBytes: 6 << 30, StorageBytes: 4 << 30},
			Limits:   linuxFootprint{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 4 << 30}},
		RuntimeOverhead:          linuxFootprint{CPUMilli: 250, MemoryBytes: 512 << 20, StorageBytes: 512 << 20},
		Volumes:                  linuxVolumes{WorkBytes: 2 << 30, SocketBytes: 16 << 20, TmpBytes: 256 << 20},
		PerWorkerImageCacheBytes: 2 << 30, PerWorkerLogBytes: 256 << 20,
		ContainerRootfsBytes: 20 << 30,
	}
}

func linuxRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{Platform: "linux", Capacity: 2, BootstrapURL: "https://slipway.example.invalid:8443", RepositoryAllowlist: []string{"cameronsjo/workout-tracker"}}
}

// S9, homelab#926 and homelab#960: the Linux ceiling is sixteen concurrent
// workers, one per slot address .2 to .17 in the worker /27. Seventeen is
// refused by name, not relative to the constant, so raising the ceiling past
// the slot set turns this test red instead of moving with it. Capacity sixteen
// is the positive control: a test that only asserts a refusal cannot tell a
// working bound from a config that never validates, and it goes red if the
// ceiling falls back to four.
func TestLinuxCapacityOfSeventeenIsRefusedByRuntimeValidation(t *testing.T) {
	base := RuntimeConfig{
		Platform:                 "linux",
		InstallationRepo:         "cameronsjo/homelab",
		AppIDFile:                "/run/app/id",
		AppKeyFile:               "/run/app/key",
		StateDir:                 "/var/lib/runner-pool",
		LegacySlots:              0,
		BootstrapURL:             "https://runner-pool.example:8443",
		BootstrapAddress:         "127.0.0.1:8443",
		BootstrapCertificateFile: "/run/tls/cert",
		BootstrapKeyFile:         "/run/tls/key",
		CycleInterval:            time.Second,
		RepositoryAllowlist:      []string{"cameronsjo/workout-tracker"},
	}

	accepted := base
	accepted.Capacity = 16
	if _, err := accepted.Validate(); err != nil {
		t.Fatalf("the Linux ceiling of sixteen was refused: %v", err)
	}

	refused := base
	refused.Capacity = 17
	_, err := refused.Validate()
	if err == nil {
		t.Fatal("a Linux capacity of 17 passed RuntimeConfig validation")
	}
	if !strings.Contains(err.Error(), "linux capacity must be 0..16") {
		t.Fatalf("capacity 17 was refused for the wrong reason: %v", err)
	}
}

func TestComposeLinuxBackendRequiresCompletePinnedConfiguration(t *testing.T) {
	config := validLinuxPlatformConfig()
	backend, err := composeLinuxBackend(context.Background(), linuxRuntimeConfig(), config,
		&rest.Config{Host: "https://kubernetes.default.svc"}, fake.NewSimpleClientset())
	if err != nil || backend == nil {
		t.Fatalf("complete Linux backend did not compose: %T %v", backend, err)
	}
	config.RunnerImage = "runner:latest"
	if _, err := composeLinuxBackend(context.Background(), linuxRuntimeConfig(), config,
		&rest.Config{Host: "https://kubernetes.default.svc"}, fake.NewSimpleClientset()); err == nil {
		t.Fatal("mutable worker image was accepted by production composition")
	}
}

func TestLinuxPlatformConfigRejectsUnknownAndTrailingData(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "linux.json")
	data, err := json.Marshal(validLinuxPlatformConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLinuxPlatformConfig(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte(` {}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLinuxPlatformConfig(path); err == nil {
		t.Fatal("trailing platform configuration was accepted")
	}
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLinuxPlatformConfig(path); err == nil {
		t.Fatal("unknown platform configuration field was accepted")
	}
}

func TestProductionFactoryKeepsMacOSFailClosed(t *testing.T) {
	if _, err := (platformBackendFactory{}).New(context.Background(), RuntimeConfig{Platform: "macos"}, nil); err == nil {
		t.Fatal("macOS daemon acquired an unconfigured backend")
	}
}

func TestLinuxControllerExampleMatchesStrictSchemaAndIsInert(t *testing.T) {
	config, err := loadLinuxPlatformConfig("../../config/linux-controller.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(config.RunnerImage, "example.invalid/") || !strings.HasSuffix(config.RunnerImage, strings.Repeat("0", 64)) {
		t.Fatal("controller example no longer carries an inert image placeholder")
	}
}
