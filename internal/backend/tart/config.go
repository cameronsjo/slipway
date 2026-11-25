package tart

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// PlatformMacOS is the controller platform string this adapter serves. The
// controller mints every runner name as "slipway-" + platform + "-" + lease UUID
// (see controller/allocator.go ReserveNext), so every owned VM name is
// slipway-macos-<uuid> and resourceNameRE below is that shape, not a guess.
const PlatformMacOS = "macos"

// MacOSCapacityCeiling is Apple's per-host limit on concurrently running macOS
// guests. It bounds the physical slot arithmetic; the owned-VM cap in Create
// (Config.MaxOwnedVMs) is a separate limit that may never exceed the physical
// capacity.
const MacOSCapacityCeiling = 2

// MinAnchorBlockRules is the floor the reviewed anchor must clear, and it is
// not a round number: hosts/m1max/pf/tart-pf-assert.sh demands `>=8` because
// homelab#417 added the bridge100 antispoof block, making the reviewed content
// 4 IPv4 + 1 antispoof + 3 IPv6. A floor of 3 is satisfied by an anchor
// carrying only its three IPv6 rules -- every IPv4 block missing, and the gate
// green. Config.validate refuses anything lower so the two gates cannot drift.
const MinAnchorBlockRules = 8

// MaxGateReadTimeout bounds how long a single pf read may hang. A wedged pfctl
// with no timeout stops the re-assert loop forever and silently, which is the
// fail-open shape the loop exists to prevent.
const MaxGateReadTimeout = 30 * time.Second

var (
	resourceNameRE = regexp.MustCompile(`^slipway-macos-[a-z0-9-]{1,48}$`)
	// ownedPrefix is the listing filter. Anything matching it with no ledger
	// entry is reported to the controller as untracked, never ignored.
	ownedPrefix = "slipway-macos-"
	sha256RE    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// Anchored to a registry-reference shape so a value cannot begin with "-"
	// and be read as a flag by whatever eventually consumes it.
	digestRefRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/:-]*@sha256:[0-9a-f]{64}$`)
)

// forbiddenPrograms are the legacy scripts the pool must never invoke. Their
// failure paths act on ci-guest by name: a failed assert during a pool start
// would stop the production legacy guest, and a failed re-assert would leave a
// pool VM running unfenced. See the plan's Decision 1.
var forbiddenPrograms = []string{"tart-pf-assert.sh", "tart-ci-run", "tart-ci-reprovision.sh"}

// Config is immutable platform configuration for one macOS controller. Every
// field is supplied explicitly; nothing defaults, because a silently defaulted
// gate or memory floor is a gate that was never configured.
type Config struct {
	TartBinary   string
	SudoBinary   string
	PfctlBinary  string
	VMStatBinary string

	PfAnchor            string
	PfConflictingAnchor string
	PfMinBlockRules     int
	// PfAnchorFile and PfAnchorSHA256 pin the anchor's CONTENT, not merely that
	// it parses and is referenced. Counting block rules cannot tell a reviewed
	// ruleset from a different one with the same number of rules. The file is
	// world-readable at /etc/pf.anchors/tart-ci, so this needs no sudo.
	PfAnchorFile   string
	PfAnchorSHA256 string

	BaseImageRef string
	TartHome     string
	LedgerDir    string

	GuestSubnet  string
	GuestGateway string

	VMCPUCount  int
	VMMemoryMiB int

	PhysicalVMCapacity int
	// MaxOwnedVMs is how many pool VMs this adapter may own at once. It was a
	// hard-coded one while the nightly ci-guest reprovision needed the second
	// Apple slot (ADR-0018). It is 1..PhysicalVMCapacity and never defaults
	// here; it comes from the darwin daemon flag -max-owned-vms, default 1.
	MaxOwnedVMs      int
	MemoryFloorBytes int64

	RunnerTarballPath   string
	RunnerTarballSHA256 string
	GuestRunnerRoot     string

	LauncherPath      string
	LauncherSHA256    string
	GuestLauncherPath string
	GuestUser         string

	BootstrapURL         string
	BootstrapBindAddress string
	BootstrapCAFile      string

	StartupPollInterval    time.Duration
	ReassertInterval       time.Duration
	StopTimeout            time.Duration
	GateReadTimeout        time.Duration
	LaunchHandshakeTimeout time.Duration
}

func (c Config) validate() error {
	for _, item := range []struct {
		name  string
		value string
	}{
		{"tart binary", c.TartBinary}, {"sudo binary", c.SudoBinary},
		{"pfctl binary", c.PfctlBinary}, {"vm_stat binary", c.VMStatBinary},
		{"tart home", c.TartHome}, {"ledger directory", c.LedgerDir},
		{"runner tarball", c.RunnerTarballPath}, {"guest runner root", c.GuestRunnerRoot},
		{"launcher", c.LauncherPath}, {"guest launcher path", c.GuestLauncherPath},
		{"pf anchor file", c.PfAnchorFile}, {"bootstrap CA file", c.BootstrapCAFile},
	} {
		if item.value == "" || !filepath.IsAbs(item.value) || filepath.Clean(item.value) != item.value {
			return fmt.Errorf("tart backend %s must be an absolute clean path", item.name)
		}
	}
	for _, program := range []string{c.TartBinary, c.SudoBinary, c.PfctlBinary, c.VMStatBinary} {
		if isForbiddenProgram(program) {
			return errors.New("tart backend must not be configured with a legacy ci-guest script")
		}
	}
	if c.PfAnchor == "" || c.PfConflictingAnchor == "" {
		return errors.New("tart backend requires a pf anchor and a conflicting anchor to order against")
	}
	if c.PfMinBlockRules < MinAnchorBlockRules {
		return fmt.Errorf("tart pf block-rule floor must be at least %d to match hosts/m1max/pf/tart-pf-assert.sh, saw %d",
			MinAnchorBlockRules, c.PfMinBlockRules)
	}
	if !sha256RE.MatchString(c.PfAnchorSHA256) {
		return errors.New("tart backend requires the reviewed pf anchor's lowercase hex sha256")
	}
	if !digestRefRE.MatchString(c.BaseImageRef) {
		return errors.New("tart base image must be pinned by sha256 digest")
	}
	if !sha256RE.MatchString(c.RunnerTarballSHA256) || !sha256RE.MatchString(c.LauncherSHA256) {
		return errors.New("tart runner tarball and guest launcher must carry lowercase hex sha256 pins")
	}
	if _, _, err := net.ParseCIDR(c.GuestSubnet); err != nil {
		return errors.New("tart guest subnet must be a CIDR block")
	}
	gateway := net.ParseIP(c.GuestGateway)
	if gateway == nil || gateway.To4() == nil {
		return errors.New("tart guest gateway must be an IPv4 address")
	}
	if c.VMCPUCount < 1 || c.VMMemoryMiB < 1 {
		return errors.New("tart VM cpu count and memory must be positive")
	}
	if c.PhysicalVMCapacity < 1 || c.PhysicalVMCapacity > MacOSCapacityCeiling {
		return fmt.Errorf("tart physical VM capacity must be 1..%d", MacOSCapacityCeiling)
	}
	if c.MaxOwnedVMs < 1 || c.MaxOwnedVMs > c.PhysicalVMCapacity {
		return fmt.Errorf("tart max owned VMs must be 1..physical VM capacity (%d), saw %d",
			c.PhysicalVMCapacity, c.MaxOwnedVMs)
	}
	if c.MemoryFloorBytes <= 0 {
		return errors.New("tart memory floor must be positive")
	}
	if c.GuestUser == "" || !guestUserRE.MatchString(c.GuestUser) {
		return errors.New("tart guest user must be a plain lowercase account name")
	}
	if err := validateBootstrapOrigin(c.BootstrapURL); err != nil {
		return err
	}
	bindPort, err := validateLoopbackBind(c.BootstrapBindAddress)
	if err != nil {
		return err
	}
	// The adapter dials the bind address and presents the advertised hostname
	// as the TLS server name. If the two ports disagree, it would dial a port
	// nothing is listening on, or -- worse -- one something else is.
	if origin, err := url.Parse(c.BootstrapURL); err != nil || origin.Port() != bindPort {
		return fmt.Errorf("tart bootstrap origin port must equal the bind port %s", bindPort)
	}
	if c.StartupPollInterval <= 0 || c.ReassertInterval <= 0 || c.StopTimeout <= 0 ||
		c.GateReadTimeout <= 0 || c.LaunchHandshakeTimeout <= 0 {
		return errors.New("tart startup poll, re-assert interval, stop, gate-read and launch-handshake timeouts must be positive durations")
	}
	if c.GateReadTimeout > MaxGateReadTimeout {
		return fmt.Errorf("tart gate read timeout must be at most %s, saw %s", MaxGateReadTimeout, c.GateReadTimeout)
	}
	return nil
}

var guestUserRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,30}$`)

func validateBootstrapOrigin(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") ||
		u.Hostname() == "" || u.Port() == "" || net.ParseIP(u.Hostname()) != nil {
		return errors.New("tart backend requires an HTTPS bootstrap origin with a DNS hostname and explicit port")
	}
	return nil
}

// validateLoopbackBind is the construction-time refusal the plan's Constraints
// require. The guest cannot reach the host at all through the pf anchor, so the
// bootstrap listener has no reason to leave loopback; a LAN or tailnet bind
// would publish the JIT endpoint to the whole estate for no gain.
//
// Exactly 127.0.0.1 or ::1, not merely "a loopback address": 127.0.0.0/8 is
// wholly loopback, so an IsLoopback test alone admits 127.0.0.2 and friends,
// which the adapter would then fail to dial because it dials the bind address
// it was given. One address, checked once.
func validateLoopbackBind(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return "", errors.New("tart backend requires a bootstrap bind address with an explicit port")
	}
	ip := net.ParseIP(host)
	if ip == nil || (!ip.Equal(net.IPv4(127, 0, 0, 1)) && !ip.Equal(net.IPv6loopback)) {
		return "", fmt.Errorf("tart backend refuses a bootstrap bind address that is not exactly 127.0.0.1 or ::1: saw %q", host)
	}
	return port, nil
}

func isForbiddenProgram(argument string) bool {
	for _, name := range forbiddenPrograms {
		if strings.Contains(argument, name) {
			return true
		}
	}
	return false
}

// EffectiveMaxOwnedVMs lowers a requested owned-VM cap above one to 1 when the
// controller's pool slots (capacity minus legacy slots) cannot use it. A
// legacy slot still reserved means ci-guest is expected to hold a VM, the
// state in which a second pool VM starves the 04:30 reprovision. It lowers
// rather than refuses because the darwin daemon builds the backend from the
// store's EFFECTIVE sizing (buildDaemon), not the flags: while the journal is
// busy, Open defers -legacy-slots 0 and keeps legacy_slots=1, so a refusal
// would exit, launchd would restart into the same refusal, and no controller
// would ever drain the journal to apply the new sizing (store.go's deferral
// exists to prevent exactly that loop). The drain restart that applies the
// sizing also raises the cap. Caps outside 1..physical pass through untouched
// so Validate refuses them instead of a silent lowering.
//
// It lives here rather than in the darwin-tagged daemon file so its tests run
// in the Linux-only CI; the linux binaries do not import this package.
func EffectiveMaxOwnedVMs(capacity, legacySlots, requested, physical int) (int, bool) {
	if poolSlots := capacity - legacySlots; requested > 1 && requested <= physical && requested > poolSlots {
		return 1, true
	}
	return requested, false
}
