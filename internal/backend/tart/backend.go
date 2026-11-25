package tart

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/google/uuid"
)

// Backend implements controller.WorkerBackend for one Tart host.
type Backend struct {
	runner Runner
	config Config
	log    *slog.Logger

	mu sync.Mutex
	// children holds every owned child per VM: the `tart run` process and the
	// `tart exec -i` delivery process, which does not return until the job
	// ends. Destroy reaps both.
	children map[string][]Process
	// stoppedByGate remembers VMs the re-assert loop has already stopped, so a
	// persistent boundary failure does not re-stop and re-log the same VM every
	// tick. Destroy is what clears it.
	stoppedByGate map[string]bool
}

var _ controller.WorkerBackend = (*Backend)(nil)

// New validates configuration and starts the re-assert loop. The loop derives
// the VMs it owns from the ledger on every tick rather than from the child
// handles in this process, so it resumes after a controller restart that left a
// VM running.
func New(ctx context.Context, runner Runner, config Config, logger *slog.Logger) (*Backend, error) {
	if ctx == nil {
		return nil, errors.New("tart backend requires a context")
	}
	if runner == nil {
		return nil, errors.New("tart backend requires a command runner")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	backend := &Backend{runner: runner, config: config, log: logger,
		children: map[string][]Process{}, stoppedByGate: map[string]bool{}}
	go backend.reassertLoop(ctx)
	return backend, nil
}

// tartEnv is the complete environment for every tart invocation. TART_HOME is
// the point: with an empty environment tart reads its default home, so nothing
// tied the VMs this adapter lists, clones and deletes to the tart_home the
// inventory reads config.json out of. Two views of "which VMs exist" that can
// disagree is the whole class of bug the three-source join exists to catch.
//
// HOME is derived from tart_home rather than configured separately, so the two
// cannot drift; PATH is derived from the tart binary's own directory plus the
// system defaults, which is what softnet needs to resolve.
func (b *Backend) tartEnv() []string {
	return []string{
		"TART_HOME=" + b.config.TartHome,
		"HOME=" + filepath.Dir(b.config.TartHome),
		"PATH=" + filepath.Dir(b.config.TartBinary) + ":/usr/bin:/bin:/usr/sbin:/sbin",
		"TART_NO_AUTO_PRUNE=1",
	}
}

// hostEnv is for the non-tart host commands: pfctl through sudo, and vm_stat.
// They need a PATH and nothing else.
func (b *Backend) hostEnv() []string {
	return []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin"}
}

// Create runs the plan's fixed order. Every step after `tart clone` leaves the
// VM discoverable by Inventory and returns an error; it never returns success
// on a partial create, because a charged-but-invisible VM is the one state the
// controller cannot reconcile.
func (b *Backend) Create(ctx context.Context, intent controller.WorkerCreateIntent) (controller.WorkerObservation, error) {
	if ctx == nil {
		return controller.WorkerObservation{}, errors.New("tart worker creation requires a context")
	}
	if err := b.validateIntent(intent); err != nil {
		return controller.WorkerObservation{}, err
	}
	name := intent.ResourceID

	// 1. Ledger first. A clone that survives a crash between these two steps is
	//    still owned; the reverse order would leak an untracked VM.
	if err := b.writeLedger(name, intent.LeaseID); err != nil {
		return controller.WorkerObservation{}, err
	}

	// 2. Memory gate, 3. owned-VM cap.
	available, err := b.memoryAvailable(ctx)
	if err != nil {
		return b.createRefusal(name, "memory-gate", "free plus inactive memory at or above the floor",
			"unreadable", err)
	}
	if !available {
		return b.capacityRefusal(name, "memory-gate", "free plus inactive memory at or above the floor",
			"below the floor", slog.LevelError)
	}
	owned, err := b.readLedger()
	if err != nil {
		return controller.WorkerObservation{}, err
	}
	// The ledger is read after this VM's own entry is written, so two
	// concurrent Creates each see the other: at the cap both refuse, and
	// neither can slip past it between a check and a write.
	others := slices.Sorted(maps.Keys(owned))
	others = slices.DeleteFunc(others, func(existing string) bool { return existing == name })
	if len(others) >= b.config.MaxOwnedVMs {
		return b.capacityRefusal(name, "owned-vm-cap",
			fmt.Sprintf("fewer than %d other pool VMs", b.config.MaxOwnedVMs),
			"pool VMs "+strings.Join(others, ", "), slog.LevelWarn)
	}

	// 4. The pf gate. Read-only, four assertions, unmeasured is a refusal.
	if err := b.assertBoundary(ctx); err != nil {
		// A ledger entry for a VM that was never cloned would charge a slot
		// forever, so a failure to remove it is itself an error worth raising.
		return controller.WorkerObservation{}, errors.Join(err, b.removeLedger(name))
	}

	// 5. Clone. From here on a failure keeps the ledger entry so Inventory
	//    reports the VM and the controller drives Destroy.
	if _, err := b.runner.Run(ctx, []string{b.config.TartBinary, "clone", b.config.BaseImageRef, name}, b.tartEnv(), nil); err != nil {
		_, refusal := b.createRefusal(name, "clone", "a fresh clone of the pinned base image", "clone failed", err)
		return controller.WorkerObservation{}, refusal
	}

	// 6. Fresh identity. `tart clone` copies the source image's MAC and ECID
	//    verbatim, so an un-randomized clone puts a second VM with ci-guest's
	//    MAC on the same bridge. Measured on 2026-09-20; see
	//    docs/evidence/2026-09-20-m1max-pool-vm-isolation.
	setArgv := []string{b.config.TartBinary, "set", name, "--random-mac", "--random-serial",
		"--cpu", strconv.Itoa(b.config.VMCPUCount), "--memory", strconv.Itoa(b.config.VMMemoryMiB)}
	if _, err := b.runner.Run(ctx, setArgv, b.tartEnv(), nil); err != nil {
		// No child is running yet, but the clone exists, so it stays charged.
		return b.createFailure(name, "set-identity", "randomized MAC and serial with pinned cpu and memory", err)
	}
	// 6b. Read the MAC back. `tart set` exiting 0 is not proof it randomized
	//     anything, and with more than one pool VM a repeated MAC is a
	//     pool-to-pool collision on the bridge, not only one with ci-guest.
	if err := b.assertDistinctMAC(name); err != nil {
		return b.createFailure(name, "distinct-mac", "a MAC no other local VM carries", err)
	}

	// 7. Start as an owned child. TART_NO_AUTO_PRUNE keeps a clone from ever
	//    evicting the base image the nightly ci-guest reprovision clones from.
	runArgv := []string{b.config.TartBinary, "run", name, "--no-graphics", "--net-softnet"}
	child, err := b.runner.Start(ctx, runArgv, b.tartEnv())
	if err != nil {
		return b.createFailure(name, "run", "an owned tart run child", err)
	}
	b.adopt(name, child)

	// 8. Wait for the guest agent.
	if err := b.waitForAgent(ctx, name, intent.StartupDeadline); err != nil {
		_, refusal := b.createFailure(name, "agent-ready", "a guest agent answering tart exec before the startup deadline", err)
		return controller.WorkerObservation{}, b.stopAfterRefusal(ctx, name, refusal)
	}

	// 9. This VM's own address, not the bridge's. A bridge-level check passes
	//    on the legacy guest's address whenever ci-guest is already up.
	if err := b.assertGuestAddress(ctx, name); err != nil {
		return controller.WorkerObservation{}, b.stopAfterRefusal(ctx, name, err)
	}

	// 10. Fresh runner tree and the guest launcher, both sha256-verified on the
	//     host before they are streamed in. The image's own ~/actions-runner is
	//     never used: it is three minors behind and is not a tree
	//     ValidateRunnerRoot would accept.
	if err := b.stageGuest(ctx, name); err != nil {
		return controller.WorkerObservation{}, b.stopAfterRefusal(ctx, name, err)
	}

	// 11. Host-side one-use fetch, then straight into the launcher's stdin. The
	//     capability never enters the guest.
	payload, err := b.fetchJIT(ctx, intent.LeaseID, intent.BootstrapToken)
	if err != nil {
		_, refusal := b.createFailure(name, "jit-fetch", "one JIT response from the loopback bootstrap listener", err)
		return controller.WorkerObservation{}, b.stopAfterRefusal(ctx, name, refusal)
	}
	defer clear(payload)
	if err := b.deliverJIT(ctx, name, payload, intent.BootstrapToken); err != nil {
		return controller.WorkerObservation{}, err
	}
	b.log.Info("tart worker created", slog.String("resource_id", name), slog.String("lease_id", intent.LeaseID))
	return controller.WorkerObservation{LeaseID: intent.LeaseID, ResourceID: name}, nil
}

// deliverJIT hands the payload to the guest launcher and waits only for the
// launcher's handshake, not for the launcher to return.
//
// `tart exec -i <vm> <launcher>` does not return until the JOB ends, because
// the launcher execs the native runner in place. Waiting on it would block the
// whole sequential daemon cycle for the length of the job -- no inventory, no
// cleanup, no deadline handling -- and any job longer than the ten-minute
// startup deadline would be quarantined while it was running correctly. It
// would also accumulate the job's entire stdout in controller memory.
//
// So the child is started, never waited on: its output goes nowhere except one
// bounded handshake line, and the adapter owns it alongside the `tart run`
// child until Destroy reaps both.
func (b *Backend) deliverJIT(ctx context.Context, name string, payload []byte, token bootstrap.Token) error {
	launchArgv := []string{b.config.TartBinary, "exec", "-i", name,
		b.config.GuestLauncherPath, "-runner-root", b.config.GuestRunnerRoot, "-guest-user", b.config.GuestUser}
	delivery, err := b.runner.StartStream(ctx, launchArgv, b.tartEnv(), payload)
	if err != nil {
		_, refusal := b.createFailure(name, "jit-delivery", "the guest launcher accepting the JIT on stdin", err)
		return b.stopAfterRefusal(ctx, name, refusal)
	}
	b.adopt(name, delivery)
	handshakeCtx, cancel := context.WithTimeout(ctx, b.config.LaunchHandshakeTimeout)
	defer cancel()
	line, err := delivery.Handshake(handshakeCtx)
	if err != nil || line != handshakeLine {
		seen := "no handshake"
		if err == nil {
			seen = "a different line"
		}
		// The launcher's stderr is otherwise discarded, which made the first
		// live failure undiagnosable without a hand reproduction. Its messages
		// are fixed strings we wrote, so the first line names the step.
		b.log.Error("tart guest launcher did not confirm the launch",
			slog.String("resource_id", name), slog.String("expected", handshakeLine),
			slog.String("seen", seen),
			slog.String("launcher_stderr", safeDiagnostic(delivery.Diagnostic(), payload, token)))
		return b.stopAfterRefusal(ctx, name,
			errors.New("tart worker creation failed at jit-delivery: expected the guest launcher to signal "+handshakeLine))
	}
	return nil
}

// safeDiagnostic is the control that makes surfacing the launcher's stderr
// safe. The launcher's messages are fixed strings we wrote, but this path must
// not DEPEND on that: a launcher that echoed what it was fed would otherwise
// put the JIT or the capability into a controller log. Presence is reported;
// the value never is.
func safeDiagnostic(diagnostic string, payload []byte, token bootstrap.Token) string {
	if diagnostic == "" {
		return ""
	}
	if strings.Contains(diagnostic, token.Encoded()) || strings.Contains(diagnostic, string(payload)) {
		return "[redacted: the launcher echoed bootstrap material to stderr]"
	}
	return diagnostic
}

// stopAfterRefusal is what Decision 1 asks for on any refusal once a VM is
// running: stop it now rather than leaving it up until some later cycle
// notices. The ledger entry deliberately stays, so the slot remains charged and
// Inventory keeps reporting the VM until Destroy proves it gone.
func (b *Backend) stopAfterRefusal(ctx context.Context, name string, refusal error) error {
	// The stop's own failure is logged inside stopOwned and does not replace
	// the refusal: the caller needs to know WHY creation was refused, and the
	// VM stays charged either way.
	_ = b.stopOwned(ctx, name)
	return refusal
}

// stopOwned kills this adapter's children for a VM and asks tart to stop it,
// bounded. It never touches a VM this controller does not own. It returns the
// stop's outcome, because a caller that records "stopped" on a stop that failed
// turns a persistent failure into a VM nobody ever tries to stop again.
func (b *Backend) stopOwned(ctx context.Context, name string) error {
	b.stopChildren(name)
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.config.StopTimeout)
	defer cancel()
	if _, err := b.runner.Run(stopCtx, []string{b.config.TartBinary, "stop", name}, b.tartEnv(), nil); err == nil {
		return nil
	}
	// A failed `tart stop` is not evidence the VM is running. Killing the owned
	// `tart run` child stops the VM, so the stop that follows fails with "not
	// running" -- which the first live run logged as an unstoppable VM while
	// `tart list` showed it stopped. Read the state back rather than trusting
	// the exit code.
	listed, listErr := b.listVMs(stopCtx)
	if listErr != nil {
		b.log.Error("tart stop failed and the listing is unreadable, so the VM's state is unknown",
			slog.String("resource_id", name))
		return errors.New("tart could not stop VM " + name + " and could not read its state")
	}
	entry, present := listed[name]
	if !present {
		// Gone entirely: not running, by definition.
		return nil
	}
	running, known := entry.running()
	if !known {
		b.log.Error("tart stop failed and the listing does not report running state",
			slog.String("resource_id", name))
		return errors.New("tart could not stop VM " + name + " and could not read its state")
	}
	if running {
		b.log.Error("tart could not stop its own VM; it stays charged and will be retried",
			slog.String("resource_id", name))
		return errors.New("tart could not stop VM " + name)
	}
	b.log.Info("tart stop reported failure but the VM is not running; treating it as stopped",
		slog.String("resource_id", name))
	return nil
}

func (b *Backend) adopt(name string, child Process) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.children[name] = append(b.children[name], child)
}

// Destroy stops and deletes, then proves each absence independently. The ledger
// entry is removed only when both are absent, so an orphaned VM directory left
// by a failed delete keeps the slot charged.
func (b *Backend) Destroy(ctx context.Context, worker controller.WorkerObservation) (controller.WorkerAbsence, error) {
	if ctx == nil {
		return controller.WorkerAbsence{}, errors.New("tart worker deletion requires a context")
	}
	if err := b.validateIdentity(worker.LeaseID, worker.ResourceID); err != nil {
		return controller.WorkerAbsence{}, err
	}
	name := worker.ResourceID
	b.stopChildren(name)

	stopCtx, cancel := context.WithTimeout(ctx, b.config.StopTimeout)
	_, stopErr := b.runner.Run(stopCtx, []string{b.config.TartBinary, "stop", name}, b.tartEnv(), nil)
	cancel()
	if stopErr != nil {
		b.log.Warn("tart stop did not succeed; proceeding to delete and proving absence",
			slog.String("resource_id", name))
	}
	_, deleteErr := b.runner.Run(ctx, []string{b.config.TartBinary, "delete", name}, b.tartEnv(), nil)
	if deleteErr != nil {
		b.log.Warn("tart delete did not succeed; proving absence independently",
			slog.String("resource_id", name))
	}

	listed, err := b.listVMs(ctx)
	if err != nil {
		return controller.WorkerAbsence{}, err
	}
	_, stillListed := listed[name]
	directories, _, err := b.vmDirectories()
	if err != nil {
		return controller.WorkerAbsence{}, err
	}
	stillOnDisk := false
	for _, directory := range directories {
		if directory == name {
			stillOnDisk = true
		}
	}
	// Whatever Destroy achieved, the re-assert loop must be free to stop this
	// VM again: a partially failed Destroy leaves it running, and a flag left
	// set would mean no later tick ever tries.
	b.mu.Lock()
	delete(b.stoppedByGate, name)
	b.mu.Unlock()

	absence := controller.WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: name,
		ProcessAbsent: !stillListed, StorageAbsent: !stillOnDisk}
	if absence.ProcessAbsent && absence.StorageAbsent {
		if err := b.removeLedger(name); err != nil {
			return controller.WorkerAbsence{}, err
		}
		b.mu.Lock()
		delete(b.children, name)
		b.mu.Unlock()
		return absence, nil
	}
	b.log.Error("tart worker is not fully absent; the slot stays charged",
		slog.String("resource_id", name), slog.Bool("process_absent", absence.ProcessAbsent),
		slog.Bool("storage_absent", absence.StorageAbsent))
	return absence, nil
}

// reassertLoop re-runs the four read-backs while this controller owns a VM. On
// any failure it stops only the VMs named in its own ledger. It never stops a
// VM it does not own -- ci-guest above all, whose loss would take the legacy
// fleet down for a fault in the pool.
func (b *Backend) reassertLoop(ctx context.Context) {
	ticker := time.NewTicker(b.config.ReassertInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.reassertOnce(ctx)
		}
	}
}

func (b *Backend) reassertOnce(ctx context.Context) {
	owned, err := b.readLedger()
	if err != nil {
		b.log.Error("tart re-assert could not read its own ledger; no VM was stopped")
		return
	}
	if len(owned) == 0 {
		return
	}
	if err := b.assertBoundary(ctx); err == nil {
		return
	}
	for _, name := range sortedNames(owned) {
		// Stop once per VM, not once per tick -- but only once it has actually
		// stopped. Recording the attempt before knowing the outcome is how a
		// wedged VM ends up running unfenced forever: the first `tart stop`
		// fails, the flag says "handled", and no tick ever tries again. A
		// failed stop is retried and re-logged every tick instead.
		b.mu.Lock()
		already := b.stoppedByGate[name]
		b.mu.Unlock()
		if already {
			continue
		}
		b.log.Error("tart re-assert failed; stopping this controller's own VM",
			slog.String("resource_id", name))
		if err := b.stopOwned(ctx, name); err != nil {
			continue
		}
		b.mu.Lock()
		b.stoppedByGate[name] = true
		b.mu.Unlock()
	}
}

func (b *Backend) stopChildren(name string) {
	b.mu.Lock()
	children := b.children[name]
	b.mu.Unlock()
	for _, child := range children {
		_ = child.Stop()
	}
}

func (b *Backend) waitForAgent(ctx context.Context, name string, deadline time.Time) error {
	if deadline.IsZero() {
		return errors.New("tart worker creation requires a startup deadline")
	}
	b.mu.Lock()
	children := append([]Process{}, b.children[name]...)
	b.mu.Unlock()
	for {
		if _, err := b.runner.Run(ctx, []string{b.config.TartBinary, "exec", name, "/usr/bin/true"}, b.tartEnv(), nil); err == nil {
			return nil
		}
		// A `tart run` child that died is never going to answer. Noticing it
		// here turns a ten-minute wait into an immediate refusal.
		for _, child := range children {
			select {
			case <-child.Done():
				return errors.New("the tart run child exited before the guest agent answered")
			default:
			}
		}
		if !time.Now().Before(deadline) {
			return errors.New("guest agent did not answer before the startup deadline")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(b.config.StartupPollInterval):
		}
	}
}

// assertGuestAddress reads this VM's own address and destroys it unless the
// address is IPv4, inside the configured guest subnet, and is not the gateway.
func (b *Backend) assertGuestAddress(ctx context.Context, name string) (err error) {
	out, runErr := b.runner.Run(ctx, []string{b.config.TartBinary, "ip", name}, b.tartEnv(), nil)
	address := firstLine(string(out))
	_, subnet, parseErr := net.ParseCIDR(b.config.GuestSubnet)
	ip := net.ParseIP(address)
	switch {
	case runErr != nil || address == "":
		err = b.addressRefusal(name, "an address inside "+b.config.GuestSubnet, "no readable address")
	case parseErr != nil:
		err = b.addressRefusal(name, "a parsable guest subnet", "unparsable configuration")
	case ip == nil || ip.To4() == nil:
		err = b.addressRefusal(name, "an IPv4 address", "a non-IPv4 value")
	case !subnet.Contains(ip):
		err = b.addressRefusal(name, "an address inside "+b.config.GuestSubnet, "an address outside it")
	case ip.Equal(net.ParseIP(b.config.GuestGateway)):
		err = b.addressRefusal(name, "an address other than the gateway", "the gateway address")
	}
	// The ledger entry stays on every branch here: the VM exists, so it must
	// remain visible to Inventory until Destroy proves it gone.
	return err
}

// assertDistinctMAC compares this VM's configured MAC with every other VM
// directory under tart_home, ci-guest and sibling pool VMs alike.
//
// This VM's own MAC must be readable: a comparison that could not be made is
// not a comparison that passed. A ledger-owned sibling whose config is present
// is held to that too; one whose config is gone cannot boot, so it is skipped
// like any vanished peer. Any other peer is skipped when its config is gone (the
// 04:30 reprovision deletes ci-guest while a Create may be scanning) or
// unreadable, because junk this adapter does not own must not block every
// Create. Symlinked entries are skipped too: Inventory already reports them as
// untracked and the controller fences them, and reading through one would let
// whoever planted it choose the bytes.
func (b *Backend) assertDistinctMAC(name string) error {
	own, err := b.vmMAC(name)
	if err != nil {
		return err
	}
	owned, err := b.readLedger()
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(b.config.TartHome, "vms"))
	if err != nil {
		return errors.New("tart VM directory is unreadable")
	}
	for _, entry := range entries {
		other := entry.Name()
		if other == name || !entry.IsDir() {
			continue
		}
		mac, err := b.vmMAC(other)
		_, sibling := owned[other]
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case err != nil && sibling:
			return err
		case err != nil:
			b.log.Warn("tart MAC check skipped a VM directory it cannot read",
				slog.String("resource_id", name), slog.String("other_vm", other))
			continue
		case bytes.Equal(mac, own):
			b.log.Error("tart clone carries a MAC another local VM already has",
				slog.String("resource_id", name), slog.String("other_vm", other))
			return fmt.Errorf("tart VM %s shares its MAC with %s", name, other)
		}
	}
	return nil
}

// vmMAC returns os.ErrNotExist unwrapped when the VM's config is absent.
func (b *Backend) vmMAC(name string) (net.HardwareAddr, error) {
	config, err := b.readVMConfig(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("tart VM %s: %w", name, err)
	}
	mac, err := net.ParseMAC(config.MACAddress)
	if err != nil {
		return nil, fmt.Errorf("tart VM %s configuration has no readable MAC", name)
	}
	return mac, nil
}

func (b *Backend) addressRefusal(name, expected, seen string) error {
	b.log.Error("tart guest address refused", slog.String("resource_id", name),
		slog.String("expected", expected), slog.String("seen", seen))
	return fmt.Errorf("tart guest address check failed: expected %s, saw %s", expected, seen)
}

// stageGuest verifies both host artifacts by sha256 before streaming either of
// them in, then unpacks the runner into a fresh directory. The tarball path and
// digest are configuration; a mismatch refuses rather than installing whatever
// is on disk.
func (b *Backend) stageGuest(ctx context.Context, name string) error {
	tarball, err := readVerified(b.config.RunnerTarballPath, b.config.RunnerTarballSHA256)
	if err != nil {
		return b.stagingRefusal(name, "runner-tarball", err)
	}
	launcher, err := readVerified(b.config.LauncherPath, b.config.LauncherSHA256)
	if err != nil {
		return b.stagingRefusal(name, "guest-launcher", err)
	}
	steps := []struct {
		stage string
		argv  []string
		stdin []byte
	}{
		{"guest-runner-root", []string{b.config.TartBinary, "exec", name, "/bin/mkdir", "-p", b.config.GuestRunnerRoot}, nil},
		{"runner-tarball", []string{b.config.TartBinary, "exec", "-i", name, "/usr/bin/tar", "-xzf", "-", "-C", b.config.GuestRunnerRoot}, tarball},
		{"guest-launcher", []string{b.config.TartBinary, "exec", "-i", name, "/bin/dd", "of=" + b.config.GuestLauncherPath}, launcher},
		{"guest-launcher-mode", []string{b.config.TartBinary, "exec", name, "/bin/chmod", "0755", b.config.GuestLauncherPath}, nil},
	}
	for _, step := range steps {
		if _, err := b.runner.Run(ctx, step.argv, b.tartEnv(), step.stdin); err != nil {
			return b.stagingRefusal(name, step.stage, err)
		}
	}
	return nil
}

func (b *Backend) stagingRefusal(name, stage string, cause error) error {
	b.log.Error("tart guest staging refused", slog.String("resource_id", name),
		slog.String("stage", stage), slog.Bool("command_failed", cause != nil))
	return errors.New("tart guest staging failed at " + stage)
}

// readVerified reads a host artifact and checks it against its configured
// digest before any of it reaches a guest.
func readVerified(path, expected string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("host artifact is unreadable")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != expected {
		return nil, errors.New("host artifact does not match its configured sha256")
	}
	return data, nil
}

// createRefusal is a refusal made before the clone: no VM exists, so the ledger
// entry is removed and the slot is released.
func (b *Backend) createRefusal(name, check, expected, seen string, cause error) (controller.WorkerObservation, error) {
	// No VM exists, so the ledger entry must go or it charges a slot forever.
	// A failure to remove it is joined onto the refusal rather than dropped.
	removeErr := b.removeLedger(name)
	b.log.Error("tart worker creation refused before clone", slog.String("resource_id", name),
		slog.String("check", check), slog.String("expected", expected), slog.String("seen", seen),
		slog.Bool("command_failed", cause != nil))
	return controller.WorkerObservation{}, errors.Join(
		fmt.Errorf("tart worker creation refused at %s: expected %s, saw %s", check, expected, seen), removeErr)
}

// capacityRefusal is createRefusal for a shortage rather than a fault: the
// owned-VM cap is full or memory is below the floor. It wraps
// controller.ErrCapacity, so the lifecycle defers the lease without writing a
// provisioning failure row. Plain refusals there tripped the provisioning
// breaker: with -legacy-slots 0 and -max-owned-vms 1 (ADR-0018's half-applied
// rollback) FreeSlots reads 2 with nothing owned, the controller admits two
// leases in one pass, and every second Create refused at the cap (homelab#1255
// review). That is only true while nothing exists, so a ledger entry that
// could not be removed, which still charges a slot, keeps the plain error.
//
// Because a deferral writes no failure row, this log line is the only record
// of it. Operators grep it by CapacityDeferredMessage and its check field (the
// tart README, "Reading a capacity refusal"). A memory-gate deferral logs at
// Error, as the plain refusal did: memory below the floor is the second slot's
// rollback trigger (plan 2026-09-30-m1max-second-slot Step 6), not routine. An
// owned-VM cap deferral is routine and logs at Warn.
func (b *Backend) capacityRefusal(name, check, expected, seen string, level slog.Level) (controller.WorkerObservation, error) {
	refusal := fmt.Errorf("tart worker creation refused at %s: expected %s, saw %s", check, expected, seen)
	if removeErr := b.removeLedger(name); removeErr != nil {
		b.log.Error("tart worker creation refused before clone, and its ledger entry could not be removed",
			slog.String("resource_id", name), slog.String("check", check),
			slog.String("expected", expected), slog.String("seen", seen))
		return controller.WorkerObservation{}, errors.Join(refusal, removeErr)
	}
	b.log.Log(context.Background(), level, CapacityDeferredMessage,
		slog.String("resource_id", name), slog.String("check", check),
		slog.String("expected", expected), slog.String("seen", seen),
		slog.Bool("counts_toward_breaker", false))
	return controller.WorkerObservation{}, fmt.Errorf("%w: %w", refusal, controller.ErrCapacity)
}

// CapacityDeferredMessage is the fixed log message of a Create deferred for
// capacity (capacityRefusal). Docs and runbooks grep for it, so it is a
// constant pinned by a test, not free text.
const CapacityDeferredMessage = "tart worker creation deferred before clone: no capacity this cycle"

// createFailure is a failure after the clone. The ledger entry is deliberately
// left in place, so Inventory keeps reporting the VM and the controller drives
// Destroy. It never returns a WorkerObservation.
func (b *Backend) createFailure(name, stage, expected string, cause error) (controller.WorkerObservation, error) {
	b.log.Error("tart worker creation failed after clone; the VM stays reported and charged",
		slog.String("resource_id", name), slog.String("stage", stage),
		slog.String("expected", expected), slog.Bool("command_failed", cause != nil))
	return controller.WorkerObservation{}, fmt.Errorf("tart worker creation failed at %s: expected %s", stage, expected)
}

func (b *Backend) validateIntent(intent controller.WorkerCreateIntent) error {
	if err := b.validateIdentity(intent.LeaseID, intent.ResourceID); err != nil {
		return err
	}
	if intent.BootstrapURL != b.config.BootstrapURL || intent.StartupDeadline.IsZero() {
		return errors.New("tart worker create intent does not match immutable bootstrap configuration")
	}
	if _, err := bootstrap.ParseToken(intent.BootstrapToken.Encoded()); err != nil {
		return errors.New("tart worker create intent lacks a valid bootstrap capability")
	}
	return nil
}

// validateIdentity pins the exact relationship the controller creates:
// allocator.go ReserveNext mints RunnerName as "slipway-" + platform + "-" + lease
// UUID. Checking the two halves separately would admit a resource name that is
// well-formed but belongs to a different lease.
func (b *Backend) validateIdentity(leaseID, resourceID string) error {
	parsed, err := uuid.Parse(leaseID)
	if err != nil || parsed.String() != leaseID || !resourceNameRE.MatchString(resourceID) {
		return errors.New("tart worker identity is not canonical")
	}
	if resourceID != ownedPrefix+leaseID {
		return errors.New("tart worker resource name does not belong to its lease")
	}
	return nil
}

func sortedNames(owned map[string]string) []string {
	set := make(map[string]bool, len(owned))
	for name := range owned {
		set[name] = true
	}
	return sortedKeys(set)
}
