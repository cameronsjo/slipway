package tart

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The bootstrap listener has no reason to leave loopback: the pf anchor blocks
// every guest-to-host packet, so a LAN or tailnet bind publishes the JIT
// endpoint to the estate and buys nothing. The daemon refuses to start.
func TestConstructionRefusesANonLoopbackBootstrapBind(t *testing.T) {
	// 127.0.0.2 is in the loopback block, so an IsLoopback test alone accepts
	// it. The adapter dials the bind address it was given, so "some loopback
	// address" is not good enough: it must be the one address.
	refused := []string{
		"0.0.0.0:8443", "192.168.1.8:8443", "100.64.0.10:8443", "[::]:8443",
		"127.0.0.2:8443", "127.0.0.1", "bootstrap.test:8443",
	}
	for _, address := range refused {
		t.Run(address, func(t *testing.T) {
			_, err := newBackendWith(t, func(c *Config) { c.BootstrapBindAddress = address })
			if err == nil {
				t.Fatalf("a bind address outside 127.0.0.1 and ::1 was accepted: %q", address)
			}
			if !strings.Contains(err.Error(), "127.0.0.1 or ::1") && !strings.Contains(err.Error(), "explicit port") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}
	// Positive control. Without it a validator that refused everything would
	// pass the loop above.
	for _, address := range []string{"127.0.0.1:9443", "[::1]:9443"} {
		t.Run("accepted "+address, func(t *testing.T) {
			_, err := newBackendWith(t, func(c *Config) {
				c.BootstrapBindAddress = address
				c.BootstrapURL = "https://bootstrap.test:9443"
			})
			if err != nil {
				t.Fatalf("a loopback bind %q was refused: %v", address, err)
			}
		})
	}
}

// The adapter dials the bind address and presents the origin's hostname as the
// TLS name. If the ports disagree it dials a port nothing is listening on, or
// one that something else is.
func TestConstructionRefusesAnOriginPortThatIsNotTheBindPort(t *testing.T) {
	_, err := newBackendWith(t, func(c *Config) { c.BootstrapURL = "https://bootstrap.test:9999" })
	if err == nil {
		t.Fatal("an origin port that is not the bind port was accepted")
	}
	if !strings.Contains(err.Error(), "must equal the bind port") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// The pool must never be configurable into the legacy scripts, whose failure
// paths act on ci-guest by name.
func TestConstructionRefusesALegacyScriptAsAProgram(t *testing.T) {
	cases := map[string]func(*Config){
		"tart binary":  func(c *Config) { c.TartBinary = "/usr/local/bin/tart-ci-run" },
		"pfctl binary": func(c *Config) { c.PfctlBinary = "/usr/local/libexec/tart-pf-assert.sh" },
		"sudo binary":  func(c *Config) { c.SudoBinary = "/usr/local/libexec/tart-ci-reprovision.sh" },
	}
	for name, adjust := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newBackendWith(t, adjust); err == nil {
				t.Fatal("a legacy ci-guest script was accepted as a program")
			}
		})
	}
}

func TestConstructionRequiresPinnedArtifactsAndAPositiveGate(t *testing.T) {
	cases := map[string]func(*Config){
		"base image is a moving tag":                 func(c *Config) { c.BaseImageRef = "ghcr.io/cirruslabs/macos-tahoe-xcode:latest" },
		"runner tarball is unpinned":                 func(c *Config) { c.RunnerTarballSHA256 = "" },
		"launcher is unpinned":                       func(c *Config) { c.LauncherSHA256 = "not-a-digest" },
		"no pf anchor":                               func(c *Config) { c.PfAnchor = "" },
		"no conflicting anchor":                      func(c *Config) { c.PfConflictingAnchor = "" },
		"zero block-rule floor":                      func(c *Config) { c.PfMinBlockRules = 0 },
		"block-rule floor below the reviewed anchor": func(c *Config) { c.PfMinBlockRules = MinAnchorBlockRules - 1 },
		"no anchor file":                             func(c *Config) { c.PfAnchorFile = "" },
		"relative anchor file":                       func(c *Config) { c.PfAnchorFile = "etc/pf.anchors/tart-ci" },
		"no anchor hash":                             func(c *Config) { c.PfAnchorSHA256 = "" },
		"malformed anchor hash":                      func(c *Config) { c.PfAnchorSHA256 = "DEADBEEF" },
		"zero gate read timeout":                     func(c *Config) { c.GateReadTimeout = 0 },
		"gate read timeout over the ceiling":         func(c *Config) { c.GateReadTimeout = MaxGateReadTimeout + time.Second },
		"zero handshake timeout":                     func(c *Config) { c.LaunchHandshakeTimeout = 0 },
		"image ref starting with a dash":             func(c *Config) { c.BaseImageRef = "-oProxyCommand@sha256:" + strings.Repeat("a", 64) },
		"zero memory floor":                          func(c *Config) { c.MemoryFloorBytes = 0 },
		"capacity above Apple's two":                 func(c *Config) { c.PhysicalVMCapacity = 3 },
		"zero capacity":                              func(c *Config) { c.PhysicalVMCapacity = 0 },
		"guest subnet is not a CIDR":                 func(c *Config) { c.GuestSubnet = "192.168.2.1" },
		"gateway is not an IPv4":                     func(c *Config) { c.GuestGateway = "fe80::1" },
		"bootstrap origin is plain":                  func(c *Config) { c.BootstrapURL = "http://bootstrap.test:8443" },
		"bootstrap origin has no port":               func(c *Config) { c.BootstrapURL = "https://bootstrap.test" },
		"bootstrap origin is an IP":                  func(c *Config) { c.BootstrapURL = "https://127.0.0.1:8443" },
		"relative runner root":                       func(c *Config) { c.GuestRunnerRoot = "sjo-actions-runner" },
		"guest user is not plain":                    func(c *Config) { c.GuestUser = "root; rm -rf /" },
		"zero re-assert interval":                    func(c *Config) { c.ReassertInterval = 0 },
	}
	for name, adjust := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := newBackendWith(t, adjust); err == nil {
				t.Fatalf("an invalid configuration was accepted: %s", name)
			}
		})
	}
}

// The owned-VM cap has no default inside the adapter, and it can never exceed
// the slots the host physically has.
func TestConstructionRefusesAnOwnedVMCapOutsideOneToPhysicalCapacity(t *testing.T) {
	for name, adjust := range map[string]func(*Config){
		"zero":                     func(c *Config) { c.MaxOwnedVMs = 0 },
		"negative":                 func(c *Config) { c.MaxOwnedVMs = -1 },
		"above physical capacity":  func(c *Config) { c.PhysicalVMCapacity = 1; c.MaxOwnedVMs = 2 },
		"above Apple's host limit": func(c *Config) { c.MaxOwnedVMs = MacOSCapacityCeiling + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newBackendWith(t, adjust)
			if err == nil {
				t.Fatal("an out-of-range owned-VM cap was accepted")
			}
			if !strings.Contains(err.Error(), "max owned VMs") {
				t.Fatalf("refused for the wrong reason: %v", err)
			}
		})
	}
	for _, accepted := range []int{1, 2} {
		if _, err := newBackendWith(t, func(c *Config) { c.MaxOwnedVMs = accepted }); err != nil {
			t.Fatalf("an owned-VM cap of %d was refused: %v", accepted, err)
		}
	}
}

// newBackendWith builds a backend from the fixture's known-good configuration
// with one field changed, without starting the fixture's listener twice.
func newBackendWith(t *testing.T, adjust func(*Config)) (*Backend, error) {
	t.Helper()
	base := newFixture(t, nil)
	config := base.config
	adjust(&config)
	return New(context.Background(), newFakeRunner(), config, nil)
}

// The cap runs at 1 until the effective pool slots allow more, and an
// out-of-range cap passes through for Validate to refuse (ADR-0018 decision 2).
func TestEffectiveMaxOwnedVMsRunsAtOneUntilThePoolSlotsAllowMore(t *testing.T) {
	cases := []struct {
		name                  string
		capacity, legacySlots int
		requested             int
		want                  int
		lowered               bool
	}{
		{"default cap with ci-guest's slot reserved", 2, 1, 1, 1, false},
		{"default cap with no pool slots", 2, 2, 1, 1, false},
		{"default cap after ci-guest is parked", 2, 0, 1, 1, false},
		{"cap of two after ci-guest is parked", 2, 0, 2, 2, false},
		{"cap of two while the journal still reserves ci-guest's slot", 2, 1, 2, 1, true},
		{"cap of two on a capacity of one", 1, 0, 2, 1, true},
		{"cap above physical passes through for Validate to refuse", 2, 1, 3, 3, false},
		{"cap of zero passes through for Validate to refuse", 2, 0, 0, 0, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, lowered := EffectiveMaxOwnedVMs(testCase.capacity, testCase.legacySlots, testCase.requested, 2)
			if got != testCase.want || lowered != testCase.lowered {
				t.Fatalf("EffectiveMaxOwnedVMs = (%d, %v), want (%d, %v)", got, lowered, testCase.want, testCase.lowered)
			}
		})
	}
}
