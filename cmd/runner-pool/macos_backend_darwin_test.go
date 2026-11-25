//go:build darwin

package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backendtart "github.com/cameronsjo/slipway/internal/backend/tart"
)

func validMacOSPlatformConfig(t *testing.T) macOSPlatformConfig {
	t.Helper()
	root := t.TempDir()
	return macOSPlatformConfig{
		TartBinary: "/opt/homebrew/bin/tart", SudoBinary: "/usr/bin/sudo",
		PfctlBinary: "/sbin/pfctl", VMStatBinary: "/usr/bin/vm_stat",
		PfAnchor: "tart-ci", PfConflictingAnchor: "com.apple.internet-sharing", PfMinBlockRules: 8,
		PfAnchorFile: "/etc/pf.anchors/tart-ci", PfAnchorSHA256: strings.Repeat("0", 64),
		BaseImageRef: "ghcr.io/cirruslabs/macos-tahoe-xcode@sha256:" + strings.Repeat("a", 64),
		TartHome:     filepath.Join(root, "tart"), LedgerDir: filepath.Join(root, "ledger"),
		GuestSubnet: "192.168.2.0/24", GuestGateway: "192.168.2.1",
		VMCPUCount: 4, VMMemoryMiB: 8192,
		PhysicalVMCapacity: 2, MemoryFloorBytes: 12 << 30,
		RunnerTarballPath: filepath.Join(root, "runner.tar.gz"), RunnerTarballSHA256: strings.Repeat("0", 64),
		GuestRunnerRoot: "/Users/admin/sjo-actions-runner",
		LauncherPath:    filepath.Join(root, "launcher"), LauncherSHA256: strings.Repeat("0", 64),
		GuestLauncherPath: "/Users/admin/sjo-runner-pool-guest-launch", GuestUser: "admin",
		BootstrapCAFile:     filepath.Join(root, "ca.pem"),
		StartupPollInterval: "2s", ReassertInterval: "60s", StopTimeout: "60s",
		GateReadTimeout: "10s", LaunchHandshakeTimeout: "60s",
	}
}

func macOSRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{Platform: "macos", Capacity: 2, LegacySlots: 1,
		BootstrapURL: "https://slipway-macos.example.invalid:8443", BootstrapAddress: "127.0.0.1:8443",
		RepositoryAllowlist: []string{"cameronsjo/workout-tracker"}}
}

// The init() in macos_backend_darwin.go is what makes a darwin daemon able to
// serve macOS at all. If it is ever dropped, linux_backend.go still refuses
// macOS and the daemon fails to start rather than running unbacked.
func TestDarwinInitInstallsTheMacOSFactory(t *testing.T) {
	if _, ok := productionBackendFactory.(macOSBackendFactory); !ok {
		t.Fatalf("the darwin production factory is %T, not the macOS one", productionBackendFactory)
	}
}

// Everything that is not macOS is delegated to the existing Linux factory, so
// a darwin build keeps exactly the behaviour it had before this file existed.
func TestMacOSFactoryDelegatesEveryOtherPlatform(t *testing.T) {
	for _, platform := range []string{"linux", "windows", ""} {
		delegated, delegatedErr := platformBackendFactory{}.New(context.Background(), RuntimeConfig{Platform: platform}, nil)
		viaMacOS, macOSErr := macOSBackendFactory{}.New(context.Background(), RuntimeConfig{Platform: platform}, nil)
		if (delegated == nil) != (viaMacOS == nil) {
			t.Fatalf("platform %q: delegation changed the backend result", platform)
		}
		if (delegatedErr == nil) != (macOSErr == nil) {
			t.Fatalf("platform %q: delegation changed the error result", platform)
		}
	}
}

func TestMacOSFactoryRefusesAMissingPlatformConfigFile(t *testing.T) {
	runtime := macOSRuntimeConfig()
	runtime.PlatformConfigFile = ""
	if _, err := (macOSBackendFactory{}).New(context.Background(), runtime, nil); err == nil {
		t.Fatal("the macOS daemon composed a backend with no platform configuration")
	}
}

func TestMacOSPlatformConfigRejectsUnknownAndTrailingData(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "macos.json")
	data, err := json.Marshal(validMacOSPlatformConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMacOSPlatformConfig(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, []byte(` {}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMacOSPlatformConfig(path); err == nil {
		t.Fatal("trailing platform configuration was accepted")
	}
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMacOSPlatformConfig(path); err == nil {
		t.Fatal("unknown platform configuration field was accepted")
	}
}

// The bind address comes from the daemon flag, not the platform file, so the
// adapter's loopback refusal is checked against the address the listener
// actually binds.
func TestComposeMacOSBackendRefusesANonLoopbackDaemonBind(t *testing.T) {
	runtime := macOSRuntimeConfig()
	runtime.BootstrapAddress = "0.0.0.0:8443"
	_, err := composeMacOSBackend(context.Background(), runtime, validMacOSPlatformConfig(t), 1, stubRunner{}, nil)
	if err == nil {
		t.Fatal("a non-loopback daemon bind composed a macOS backend")
	}
	if !strings.Contains(err.Error(), "127.0.0.1 or ::1") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

func TestComposeMacOSBackendRefusesNonPositiveDurations(t *testing.T) {
	for name, adjust := range map[string]func(*macOSPlatformConfig){
		"startup poll": func(c *macOSPlatformConfig) { c.StartupPollInterval = "0s" },
		"re-assert":    func(c *macOSPlatformConfig) { c.ReassertInterval = "-1s" },
		"stop timeout": func(c *macOSPlatformConfig) { c.StopTimeout = "soon" },
	} {
		t.Run(name, func(t *testing.T) {
			config := validMacOSPlatformConfig(t)
			adjust(&config)
			if _, err := composeMacOSBackend(context.Background(), macOSRuntimeConfig(), config, 1, stubRunner{}, nil); err == nil {
				t.Fatal("a non-positive duration was accepted")
			}
		})
	}
}

// The flag reaches the adapter. A cap of 2 while the effective sizing still
// reserves the legacy slot (a deferred -legacy-slots 0) must NOT stop the
// daemon: a refusal there restarts into the same refusal and the journal never
// drains. An out-of-range cap still reaches the adapter's own range check.
func TestComposeMacOSBackendCarriesTheOwnedVMCap(t *testing.T) {
	if _, err := composeMacOSBackend(context.Background(), macOSRuntimeConfig(), validMacOSPlatformConfig(t), 2,
		stubRunner{}, nil); err != nil {
		t.Fatalf("a cap of 2 under a deferred legacy slot refused startup: %v", err)
	}
	// The lowered cap is what the adapter receives, not only what the pure
	// function returns.
	lowered, err := macOSTartConfig(macOSRuntimeConfig(), validMacOSPlatformConfig(t), 2, nil)
	if err != nil || lowered.MaxOwnedVMs != 1 {
		t.Fatalf("a cap of 2 with -legacy-slots 1 in effect gave the adapter %d (err %v), want 1", lowered.MaxOwnedVMs, err)
	}
	parked := macOSRuntimeConfig()
	parked.LegacySlots = 0
	raised, err := macOSTartConfig(parked, validMacOSPlatformConfig(t), 2, nil)
	if err != nil || raised.MaxOwnedVMs != 2 {
		t.Fatalf("a cap of 2 with -legacy-slots 0 in effect gave the adapter %d (err %v), want 2", raised.MaxOwnedVMs, err)
	}
	if _, err := composeMacOSBackend(context.Background(), macOSRuntimeConfig(), validMacOSPlatformConfig(t), 3,
		stubRunner{}, nil); err == nil || !strings.Contains(err.Error(), "max owned VMs") {
		t.Fatalf("a cap of 3 did not reach the adapter's range check: %v", err)
	}
	runtime := macOSRuntimeConfig()
	runtime.LegacySlots = 0
	if _, err := composeMacOSBackend(context.Background(), runtime, validMacOSPlatformConfig(t), 0,
		stubRunner{}, nil); err == nil || !strings.Contains(err.Error(), "max owned VMs") {
		t.Fatalf("a cap of 0 did not reach the adapter's range check: %v", err)
	}
	if flag.Lookup("max-owned-vms") == nil || flag.Lookup("max-owned-vms").DefValue != "1" {
		t.Fatal("the darwin daemon does not register -max-owned-vms with a default of 1")
	}
}

func TestMacOSControllerExampleMatchesStrictSchemaAndIsInert(t *testing.T) {
	config, err := loadMacOSPlatformConfig("../../config/macos-controller.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(config.BaseImageRef, "example.invalid/") ||
		!strings.HasSuffix(config.BaseImageRef, strings.Repeat("0", 64)) {
		t.Fatal("the macOS controller example no longer carries an inert image placeholder")
	}
	if config.RunnerTarballSHA256 != strings.Repeat("0", 64) || config.LauncherSHA256 != strings.Repeat("0", 64) {
		t.Fatal("the macOS controller example carries a real artifact digest")
	}
	if config.PfAnchor != "tart-ci" {
		t.Fatalf("the example names an anchor the host sudoers file does not grant: %q", config.PfAnchor)
	}
	if config.PfMinBlockRules < 8 {
		t.Fatalf("the example's block-rule floor is %d; tart-pf-assert.sh demands >=8", config.PfMinBlockRules)
	}
	if config.PfAnchorFile != "/etc/pf.anchors/tart-ci" {
		t.Fatalf("the example does not pin the reviewed anchor file: %q", config.PfAnchorFile)
	}
	// The anchor hash is the one field in the example that is deliberately NOT
	// an inert placeholder: it must be the real reviewed value, so the pool's
	// gate and tart-pf-assert.sh cannot drift apart.
	if config.PfAnchorSHA256 == strings.Repeat("0", 64) {
		t.Fatal("the example carries a placeholder anchor hash instead of the reviewed one")
	}
}

type stubRunner struct{}

func (stubRunner) Run(context.Context, []string, []string, []byte) ([]byte, error) { return nil, nil }
func (stubRunner) Start(context.Context, []string, []string) (backendtart.Process, error) {
	return nil, nil
}
func (stubRunner) StartStream(context.Context, []string, []string, []byte) (backendtart.StreamProcess, error) {
	return nil, nil
}
