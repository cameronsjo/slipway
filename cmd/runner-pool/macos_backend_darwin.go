//go:build darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"time"

	backendtart "github.com/cameronsjo/slipway/internal/backend/tart"
	"github.com/cameronsjo/slipway/internal/controller"
)

// This file is the whole macOS surface of the daemon, and it is darwin-tagged
// on purpose: no byte of it reaches the linux/amd64 controller, witness, worker
// or host-tool binary, whose reproduced hashes are pinned in image/*/image-lock.json.
//
// It replaces the package variable rather than editing platformBackendFactory,
// so linux_backend.go keeps no build tag and keeps refusing macOS. If this
// init() is ever dropped, a darwin daemon started with -platform=macos fails to
// start instead of running unbacked.
func init() { productionBackendFactory = macOSBackendFactory{} }

type macOSBackendFactory struct{}

// maxOwnedVMs is the tart adapter's owned-VM cap (ADR-0018). It is a daemon
// flag registered here, not in main.go, so the pinned linux binaries stay
// byte-identical, and so it sits in the same plist as -legacy-slots, the knob
// it must agree with. It is not a platform-file field: install.sh derives the
// file's exact key set from this file's json tags.
var maxOwnedVMs = flag.Int("max-owned-vms", 1,
	"macOS daemon: pool VMs the tart adapter may own at once, 1..2; runs at 1 until the effective -legacy-slots leaves that many pool slots")

func (macOSBackendFactory) New(ctx context.Context, runtime RuntimeConfig, store *controller.Store) (controller.WorkerBackend, error) {
	if runtime.Platform != backendtart.PlatformMacOS {
		return platformBackendFactory{}.New(ctx, runtime, store)
	}
	if runtime.PlatformConfigFile == "" {
		return nil, errors.New("macOS daemon requires platform-config-file")
	}
	platform, err := loadMacOSPlatformConfig(runtime.PlatformConfigFile)
	if err != nil {
		return nil, err
	}
	return composeMacOSBackend(ctx, runtime, platform, *maxOwnedVMs, backendtart.ExecRunner{}, slog.Default())
}

// macOSPlatformConfig mirrors loadLinuxPlatformConfig's shape: a strict decoder
// over an explicit non-secret file. No field defaults. A silently defaulted
// memory floor or pf anchor is a gate that was never configured.
type macOSPlatformConfig struct {
	TartBinary   string `json:"tart_binary"`
	SudoBinary   string `json:"sudo_binary"`
	PfctlBinary  string `json:"pfctl_binary"`
	VMStatBinary string `json:"vm_stat_binary"`

	PfAnchor            string `json:"pf_anchor"`
	PfConflictingAnchor string `json:"pf_conflicting_anchor"`
	PfMinBlockRules     int    `json:"pf_min_block_rules"`
	PfAnchorFile        string `json:"pf_anchor_file"`
	PfAnchorSHA256      string `json:"pf_anchor_sha256"`

	BaseImageRef string `json:"base_image_ref"`
	TartHome     string `json:"tart_home"`
	LedgerDir    string `json:"ledger_dir"`

	GuestSubnet  string `json:"guest_subnet"`
	GuestGateway string `json:"guest_gateway"`

	VMCPUCount  int `json:"vm_cpu_count"`
	VMMemoryMiB int `json:"vm_memory_mib"`

	PhysicalVMCapacity int   `json:"physical_vm_capacity"`
	MemoryFloorBytes   int64 `json:"memory_floor_bytes"`

	RunnerTarballPath   string `json:"runner_tarball_path"`
	RunnerTarballSHA256 string `json:"runner_tarball_sha256"`
	GuestRunnerRoot     string `json:"guest_runner_root"`

	LauncherPath      string `json:"launcher_path"`
	LauncherSHA256    string `json:"launcher_sha256"`
	GuestLauncherPath string `json:"guest_launcher_path"`
	GuestUser         string `json:"guest_user"`

	BootstrapCAFile string `json:"bootstrap_ca_file"`

	StartupPollInterval    string `json:"startup_poll_interval"`
	ReassertInterval       string `json:"reassert_interval"`
	StopTimeout            string `json:"stop_timeout"`
	GateReadTimeout        string `json:"gate_read_timeout"`
	LaunchHandshakeTimeout string `json:"launch_handshake_timeout"`
}

func loadMacOSPlatformConfig(path string) (macOSPlatformConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return macOSPlatformConfig{}, errors.New("read macOS platform configuration")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config macOSPlatformConfig
	if err := decoder.Decode(&config); err != nil {
		return macOSPlatformConfig{}, errors.New("decode macOS platform configuration")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return macOSPlatformConfig{}, errors.New("macOS platform configuration contains trailing data")
	}
	return config, nil
}

// composeMacOSBackend takes the bootstrap origin and bind address from the
// daemon flags, not from the platform file, so the adapter's loopback refusal
// is checked against the address the listener actually binds.
func composeMacOSBackend(ctx context.Context, runtime RuntimeConfig, platform macOSPlatformConfig, maxOwned int,
	runner backendtart.Runner, logger *slog.Logger) (controller.WorkerBackend, error) {
	config, err := macOSTartConfig(runtime, platform, maxOwned, logger)
	if err != nil {
		return nil, err
	}
	return backendtart.New(ctx, runner, config, logger)
}

// macOSTartConfig builds the adapter config, owned-VM cap included, without
// starting anything, so a test can read the cap the adapter would receive.
func macOSTartConfig(runtime RuntimeConfig, platform macOSPlatformConfig, maxOwned int,
	logger *slog.Logger) (backendtart.Config, error) {
	if effective, lowered := backendtart.EffectiveMaxOwnedVMs(runtime.Capacity, runtime.LegacySlots, maxOwned, platform.PhysicalVMCapacity); lowered {
		if logger != nil {
			logger.Warn("Lowered the owned-VM cap to 1: -legacy-slots in effect leaves fewer pool slots than -max-owned-vms. A pending -legacy-slots change applies at the next idle restart; with none pending, the plist needs -legacy-slots 0.",
				"requested_max_owned_vms", maxOwned, "effective_max_owned_vms", effective,
				"capacity", runtime.Capacity, "legacy_slots", runtime.LegacySlots)
		}
		maxOwned = effective
	}
	poll, err := positiveDuration(platform.StartupPollInterval)
	if err != nil {
		return backendtart.Config{}, errors.New("macOS startup poll interval must be a positive duration")
	}
	reassert, err := positiveDuration(platform.ReassertInterval)
	if err != nil {
		return backendtart.Config{}, errors.New("macOS re-assert interval must be a positive duration")
	}
	stop, err := positiveDuration(platform.StopTimeout)
	if err != nil {
		return backendtart.Config{}, errors.New("macOS stop timeout must be a positive duration")
	}
	gateRead, err := positiveDuration(platform.GateReadTimeout)
	if err != nil {
		return backendtart.Config{}, errors.New("macOS gate read timeout must be a positive duration")
	}
	handshake, err := positiveDuration(platform.LaunchHandshakeTimeout)
	if err != nil {
		return backendtart.Config{}, errors.New("macOS launch handshake timeout must be a positive duration")
	}
	return backendtart.Config{
		TartBinary: platform.TartBinary, SudoBinary: platform.SudoBinary,
		PfctlBinary: platform.PfctlBinary, VMStatBinary: platform.VMStatBinary,
		PfAnchor: platform.PfAnchor, PfConflictingAnchor: platform.PfConflictingAnchor,
		PfMinBlockRules: platform.PfMinBlockRules,
		PfAnchorFile:    platform.PfAnchorFile, PfAnchorSHA256: platform.PfAnchorSHA256,
		BaseImageRef: platform.BaseImageRef, TartHome: platform.TartHome, LedgerDir: platform.LedgerDir,
		GuestSubnet: platform.GuestSubnet, GuestGateway: platform.GuestGateway,
		VMCPUCount: platform.VMCPUCount, VMMemoryMiB: platform.VMMemoryMiB,
		PhysicalVMCapacity: platform.PhysicalVMCapacity, MaxOwnedVMs: maxOwned,
		MemoryFloorBytes:  platform.MemoryFloorBytes,
		RunnerTarballPath: platform.RunnerTarballPath, RunnerTarballSHA256: platform.RunnerTarballSHA256,
		GuestRunnerRoot: platform.GuestRunnerRoot,
		LauncherPath:    platform.LauncherPath, LauncherSHA256: platform.LauncherSHA256,
		GuestLauncherPath: platform.GuestLauncherPath, GuestUser: platform.GuestUser,
		BootstrapURL: runtime.BootstrapURL, BootstrapBindAddress: runtime.BootstrapAddress,
		BootstrapCAFile:     platform.BootstrapCAFile,
		StartupPollInterval: poll, ReassertInterval: reassert, StopTimeout: stop,
		GateReadTimeout: gateRead, LaunchHandshakeTimeout: handshake,
	}, nil
}

func positiveDuration(value string) (time.Duration, error) {
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, errors.New("duration must be positive")
	}
	return parsed, nil
}
