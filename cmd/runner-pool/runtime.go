package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/cameronsjo/slipway/internal/githubapp"
)

const (
	defaultDaemonInterval = time.Minute
	// Each Kata VM charges 30.5GiB of thin pool (three 10GiB writable disks plus
	// 0.5GiB, homelab#983), so sixteen simultaneous workers charge 488GiB of the
	// 576GiB pool homelab#983 grew to, leaving image-cache headroom. The worker
	// /27 has sixteen slot addresses, .2 to .17 (homelab#960). The Linux ceiling
	// is therefore sixteen. The controller's memory arithmetic admits about
	// twelve, so the ceiling alone changes nothing; -capacity does.
	linuxCapacityCeiling = 16
	macOSCapacityCeiling = 2
	// The repository cap's default and ceiling, restated here because the
	// controller's own copies are unexported and this check runs before the
	// journal is opened. controller.Open re-validates against its own pair.
	defaultMaxRepositories = 80
	maxRepositoriesCeiling = 120
	// defaultSessionDeafAfter is how long the controller tolerates zero queue
	// envelopes from every subscription before treating its sessions as dead.
	// On 2026-09-23 a controller's sessions were silent for ~9 hours with GitHub
	// holding fresh available jobs the whole time; this bounds how long that
	// blindness can now last before a restart.
	defaultSessionDeafAfter = 30 * time.Minute
	// defaultFailedCycleRestartAfter is how long cycles may keep failing in the
	// session stage, with queued pool jobs waiting, before the controller exits
	// for a restart (homelab#1211). Ten one-minute cycles rides out a GitHub blip and
	// still ends a stall like 2026-10-01 (about ten hours, 599 failed cycles)
	// well inside the 30-minute deaf window it never reached.
	defaultFailedCycleRestartAfter = 10 * time.Minute
	// defaultRESTDemandInterval is how often the daemon asks the REST API which
	// served repositories hold queued jobs against its pool label, independent
	// of the queue-session envelopes (homelab#913). Five minutes bounds the API
	// cost stated in the issue (~576 calls/hour at 48 repos with no queued
	// runs) while staying well inside the ~48-minute discovery latency the
	// exploratory rotation alone left a deaf-session outage with.
	defaultRESTDemandInterval = 5 * time.Minute
	// minRESTDemandInterval is the floor Validate enforces. Below a minute the
	// read cost stops being the "few minutes" the issue sized the feature
	// against, and RESTDemandTracker's freshness window (2x interval) would
	// start approaching the queue-session poll's own cadence.
	minRESTDemandInterval = time.Minute
	// defaultQueueJanitorInterval is how often the queue janitor passes over
	// the served repositories' queued runs. Ten minutes is short against a
	// backlog measured in hours and long against the janitor's own API cost.
	defaultQueueJanitorInterval = 10 * time.Minute
	// defaultReapInterval is how often the reaper tears down finished workers
	// and refills their slots between cycles (homelab#1190). A cycle spends
	// ~50 s in the broker long-poll; reaping only once per cycle held each
	// finished slot a median 80 s. An idle pass makes no GitHub call beyond one
	// registration pass per controller.registrationGap, so a short interval
	// costs Kubernetes list calls; only a refill spends REST quota, on the same
	// acquire and JIT calls the cycle would have made for that slot.
	defaultReapInterval = 10 * time.Second
	// minReapInterval stops a typo from turning the reaper into a hot loop of
	// Kubernetes lists.
	minReapInterval = 2 * time.Second
	// maxReapInterval stops a typo (10m for 10s, say) from quietly turning the
	// reaper into a no-op; the documented off switch is a negative value.
	maxReapInterval = 5 * time.Minute
	// minQueueJanitorInterval is the floor Validate enforces, for the same
	// reason as the REST demand floor: below it the pass is a poll.
	minQueueJanitorInterval = time.Minute
)

// deafBasis names which rule judged the sessions deaf (#1011): "demand" when a
// fresh REST reading showed queued pool jobs, "idle" when no reading was fresh
// and the idle fallback applied.
func deafBasis(demandKnown bool) string {
	if demandKnown {
		return "demand"
	}
	return "idle"
}

// errSessionsDeaf marks the deliberate exit a deaf-session restart takes, so
// main can choose a distinct non-zero exit code from an ordinary daemon
// failure. errors.Is sees through the bootstrap-shutdown join below.
var errSessionsDeaf = errors.New("runner-pool queue sessions look deaf; exiting for supervisor restart")

// errRestartRequired and errCyclesStalled take the deaf exit (code 3, sessions
// resumed on restart) for the two ways a cycle can fail with no remedy short of
// a new process: session state the manager itself says it cannot trust, and a
// run of cycles that failed in the session stage while jobs waited (homelab#1211).
var (
	errRestartRequired = errors.New("runner-pool session state needs a process restart; exiting for supervisor restart")
	errCyclesStalled   = errors.New("runner-pool cycles keep failing in the session stage while jobs are queued; exiting for supervisor restart")
)

// exitsForRestart reports whether err is one of the deliberate exits a
// supervisor restart is meant to cure, which main maps to exitSessionsDeaf.
func exitsForRestart(err error) bool {
	return errors.Is(err, errSessionsDeaf) || errors.Is(err, errRestartRequired) || errors.Is(err, errCyclesStalled)
}

// RuntimeConfig is the process-local composition contract for one platform
// controller. A daemon is intentionally configured once per platform; the
// repository list is discovered from the selected or all-repositories App
// installation.
type RuntimeConfig struct {
	Platform                 string
	InstallationRepo         string
	AppIDFile                string
	AppKeyFile               string
	StateDir                 string
	Capacity                 int
	LegacySlots              int
	BootstrapURL             string
	BootstrapAddress         string
	BootstrapCertificateFile string
	BootstrapKeyFile         string
	PlatformConfigFile       string
	CycleInterval            time.Duration
	// RepositoryAllowlist pins the repositories permitted to receive an
	// slipway-linux scale set. EMPTY means every private, active repository in the
	// App installation, which is the default onboarding posture; see
	// controller.eligibleForPool for what still refuses regardless.
	RepositoryAllowlist []string
	// MaxRepositories bounds the repositories one controller serves, and so the
	// sessions it polls per cycle. Zero adopts the controller default.
	MaxRepositories int
	// SessionDeafAfter is how long this process may go without receiving a
	// queue envelope from any subscription before its sessions are judged
	// dead and it exits for its supervisor to restart it. Zero adopts
	// defaultSessionDeafAfter.
	SessionDeafAfter time.Duration
	// FailedCycleRestartAfter is how long cycles may keep failing in the
	// session stage, while a fresh REST reading shows queued pool jobs, before
	// the process exits for its supervisor to restart it. Zero adopts
	// defaultFailedCycleRestartAfter.
	FailedCycleRestartAfter time.Duration
	// RESTDemandInterval is how often the daemon reads queued-pool-job demand
	// from the REST API, independent of the queue-session envelopes. Zero
	// adopts defaultRESTDemandInterval.
	RESTDemandInterval time.Duration
	// QueueJanitor is the queue janitor's mode: off, dry-run or enforce.
	// Validate turns any other value into off -- it fails closed rather than
	// refusing to start, because the janitor is optional -- and records the
	// rejected value so buildDaemon can say so.
	QueueJanitor string
	// QueueJanitorInterval is the time between janitor passes. Zero adopts
	// defaultQueueJanitorInterval.
	QueueJanitorInterval time.Duration
	// ReapInterval is the time between reaper passes (homelab#1190). Zero
	// adopts defaultReapInterval; a negative value turns the reaper off, so
	// finished workers are reaped only by the cycle, as before.
	ReapInterval time.Duration
	// CreateConcurrency is how many worker creations one provisioning pass
	// runs at once (homelab#1190 item 2). Zero adopts 1, the serial loop; 1 is
	// also the rollback. Only the linux (Kubernetes) backend counts creates in
	// flight against its headroom, so macos refuses anything above 1.
	CreateConcurrency int
	// The janitor's OWN GitHub App: its App ID, private key and installation
	// ID, each a file in its own Secret. All three empty, or any of them
	// unreadable, means no janitor App is configured and the janitor is off.
	// The pool App's files are never used for the janitor.
	JanitorAppIDFile          string
	JanitorAppKeyFile         string
	JanitorInstallationIDFile string

	queueJanitorRejected string
}

// splitRepositoryAllowlist turns the comma-separated flag value into entries.
// Normalization and validation belong to controller.parseRepositoryAllowlist,
// which is the one place the fail-closed rules live.
func splitRepositoryAllowlist(value string) []string {
	var entries []string
	for _, entry := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(entry); trimmed != "" {
			entries = append(entries, trimmed)
		}
	}
	return entries
}

// Validate applies platform defaults and checks the values needed before the
// controller opens its durable journal or starts its bootstrap listener. A
// negative capacity or legacy-slot value means "use the platform default";
// zero remains a valid explicit minimum.
func (c RuntimeConfig) Validate() (RuntimeConfig, error) {
	c.Platform = strings.ToLower(strings.TrimSpace(c.Platform))
	var ceiling, defaultCapacity, defaultLegacy int
	switch c.Platform {
	case "linux":
		ceiling, defaultCapacity, defaultLegacy = linuxCapacityCeiling, linuxCapacityCeiling, 0
	case "macos":
		ceiling, defaultCapacity, defaultLegacy = macOSCapacityCeiling, macOSCapacityCeiling, 1
	default:
		return RuntimeConfig{}, errors.New("platform must be linux or macos")
	}
	if c.Capacity < 0 {
		if c.Capacity < -1 {
			return RuntimeConfig{}, errors.New("capacity must be -1 (default) or a nonnegative value")
		}
		c.Capacity = defaultCapacity
	}
	if c.LegacySlots < 0 {
		if c.LegacySlots < -1 {
			return RuntimeConfig{}, errors.New("legacy slots must be -1 (default) or a nonnegative value")
		}
		c.LegacySlots = defaultLegacy
	}
	if c.Capacity > ceiling || c.LegacySlots < 0 || c.LegacySlots > c.Capacity {
		return RuntimeConfig{}, fmt.Errorf("%s capacity must be 0..%d and legacy slots must fit within it", c.Platform, ceiling)
	}
	if c.InstallationRepo == "" || !validInstallationRepo(c.InstallationRepo) {
		return RuntimeConfig{}, errors.New("installation-repo must be an owner/repository name")
	}
	if c.AppIDFile == "" || c.AppKeyFile == "" || c.StateDir == "" {
		return RuntimeConfig{}, errors.New("daemon requires App files and state-dir")
	}
	if err := validateBootstrapOrigin(c.BootstrapURL); err != nil {
		return RuntimeConfig{}, err
	}
	if c.BootstrapAddress == "" || c.BootstrapCertificateFile == "" || c.BootstrapKeyFile == "" {
		return RuntimeConfig{}, errors.New("daemon requires bootstrap address and TLS files")
	}
	if c.CycleInterval == 0 {
		c.CycleInterval = defaultDaemonInterval
	}
	if c.CycleInterval < 0 {
		return RuntimeConfig{}, errors.New("daemon cycle interval must be positive")
	}
	// Last, so it cannot mask a more specific diagnostic. An empty allowlist is
	// legal and means every private, active repository in the installation, so
	// the bound that used to come from the list comes from this cap instead.
	// controller.Open re-derives and re-validates it; this check exists so a
	// bad value is refused before the journal is opened, not after.
	if c.MaxRepositories == 0 {
		c.MaxRepositories = defaultMaxRepositories
	}
	if c.MaxRepositories < 1 || c.MaxRepositories > maxRepositoriesCeiling {
		return RuntimeConfig{}, fmt.Errorf("max-repositories must be 1..%d; it bounds how many repositories the pool may onboard, and each one it serves is a concurrent long poll every cycle", maxRepositoriesCeiling)
	}
	if c.SessionDeafAfter == 0 {
		c.SessionDeafAfter = defaultSessionDeafAfter
	}
	if c.SessionDeafAfter < 0 {
		return RuntimeConfig{}, errors.New("session-deaf-after must be positive; it bounds how long the controller tolerates silent queue sessions before restarting")
	}
	if cycleDeadline(c.SessionDeafAfter) <= c.CycleInterval {
		return RuntimeConfig{}, fmt.Errorf("session-deaf-after %s leaves a cycle deadline of %s, not longer than the %s cycle interval; raise session-deaf-after",
			c.SessionDeafAfter, cycleDeadline(c.SessionDeafAfter), c.CycleInterval)
	}
	if c.FailedCycleRestartAfter == 0 {
		c.FailedCycleRestartAfter = defaultFailedCycleRestartAfter
	}
	if c.FailedCycleRestartAfter <= c.CycleInterval {
		return RuntimeConfig{}, fmt.Errorf("failed-cycle-restart-after %s must be longer than the %s cycle interval; it bounds how long failing cycles may stall a pool with queued jobs before a restart",
			c.FailedCycleRestartAfter, c.CycleInterval)
	}
	if c.RESTDemandInterval == 0 {
		c.RESTDemandInterval = defaultRESTDemandInterval
	}
	if c.RESTDemandInterval < minRESTDemandInterval {
		return RuntimeConfig{}, fmt.Errorf("rest-demand-interval must be at least %s", minRESTDemandInterval)
	}
	mode, known := controller.ParseQueueJanitorMode(c.QueueJanitor)
	if !known && strings.TrimSpace(c.QueueJanitor) != "" {
		c.queueJanitorRejected = c.QueueJanitor
	}
	c.QueueJanitor = string(mode)
	if c.QueueJanitorInterval == 0 {
		c.QueueJanitorInterval = defaultQueueJanitorInterval
	}
	if c.QueueJanitorInterval < minQueueJanitorInterval {
		return RuntimeConfig{}, fmt.Errorf("queue-janitor-interval must be at least %s", minQueueJanitorInterval)
	}
	if c.ReapInterval == 0 {
		c.ReapInterval = defaultReapInterval
	}
	if c.ReapInterval > 0 && (c.ReapInterval < minReapInterval || c.ReapInterval > maxReapInterval) {
		return RuntimeConfig{}, fmt.Errorf("reap-interval must be between %s and %s, or negative to turn the reaper off", minReapInterval, maxReapInterval)
	}
	if c.CreateConcurrency == 0 {
		c.CreateConcurrency = 1
	}
	if c.CreateConcurrency < 1 || c.CreateConcurrency > controller.MaxCreateConcurrency {
		return RuntimeConfig{}, fmt.Errorf("create-concurrency must be 1..%d; 1 creates workers one at a time", controller.MaxCreateConcurrency)
	}
	if c.Platform != "linux" && c.CreateConcurrency != 1 {
		return RuntimeConfig{}, fmt.Errorf("create-concurrency must be 1 on %s: only the linux backend counts creates in flight against its headroom", c.Platform)
	}
	return c, nil
}

func validInstallationRepo(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != "" &&
		!strings.ContainsAny(name, " \t\r\n")
}

func validateBootstrapOrigin(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" ||
		u.Port() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") || net.ParseIP(u.Hostname()) != nil {
		return errors.New("bootstrap-url must be an HTTPS DNS origin with an explicit port")
	}
	return nil
}

// BackendFactory is the seam for platform adapters. Kubernetes/Kata and Tart
// implementations can be supplied without changing daemon composition or the
// lifecycle state machine.
type BackendFactory interface {
	New(context.Context, RuntimeConfig, *controller.Store) (controller.WorkerBackend, error)
}

// BackendFactoryFunc makes small test and platform adapter factories easy to
// inject while retaining the named interface at the composition boundary.
type BackendFactoryFunc func(context.Context, RuntimeConfig, *controller.Store) (controller.WorkerBackend, error)

func (f BackendFactoryFunc) New(ctx context.Context, config RuntimeConfig, store *controller.Store) (controller.WorkerBackend, error) {
	if f == nil {
		return nil, errors.New("worker backend factory is nil")
	}
	return f(ctx, config, store)
}

// productionBackendFactory selects the reviewed platform adapter. Linux has a
// concrete Kubernetes/Kata factory; unsupported platforms and incomplete files
// fail closed. Tests inject BackendFactory directly into buildDaemon.
var productionBackendFactory BackendFactory = platformBackendFactory{}

type daemonCycle interface {
	Cycle(context.Context) (controller.CycleResult, error)
}

type bootstrapServer interface {
	Serve(context.Context) error
}

// daemonRuntime owns only process scheduling and shutdown. The controller
// cycle remains responsible for ordering discovery, inventory, queue work and
// provisioning; one failed cycle is observed and retried on the next tick.
type daemonRuntime struct {
	cycle     daemonCycle
	server    bootstrapServer
	interval  time.Duration
	observe   func(error)
	summarize func(controller.CycleSummary)
	// deadline bounds one Cycle call; zero leaves it unbounded (the component
	// tests). A call past it returns the context's deadline error, is logged
	// like any failed cycle, and the next tick starts a fresh one.
	deadline time.Duration
	// background starts once, alongside the bootstrap server, and runs for the
	// life of runCtx. Nil unless buildDaemon wires one in (the REST demand
	// tracker, homelab#913): it is not part of the sequential cycle Cycle()
	// drives, so it does not belong in the constructor every existing test
	// already calls with a fixed argument count.
	background func(context.Context)
	// failedCycleRestartAfter is the stalled-cycle backstop's window
	// (homelab#1211); zero turns the backstop off, which is what every
	// component test that does not set it gets. buildDaemon sets it from
	// RuntimeConfig.FailedCycleRestartAfter.
	failedCycleRestartAfter time.Duration
	// now is the backstop's clock; nil means time.Now.
	now func() time.Time
}

// stopReason names why Run is stopping on its own: a planned sizing adoption
// exits 0, and every other reason takes the deaf exit's code.
type stopReason int

const (
	stopNone stopReason = iota
	stopForSizing
	stopForDeafness
	stopForRestartRequired
	stopForStalledCycles
)

// cycleStall tracks the run of consecutive cycles that stopped in the session
// stage (CycleResult.SessionStageFailed, homelab#1211). Such a cycle never
// reaches the step the deaf check measures, so the deaf exit cannot fire
// however long the run lasts. Discovery, inventory and Maintain failures are
// not counted: they degrade on purpose (homelab#1096), keep local teardown
// running, and a restart would only add a startup that can fail with exit 1.
type cycleStall struct {
	count int
	since time.Time
}

// observe records one cycle's outcome. Any cycle that did not stop in the
// session stage ends the run: a polled cycle, a cycle over zero sets because
// every set is degraded and waits on an operator, and a cycle that failed
// somewhere a restart does not cure.
func (s *cycleStall) observe(sessionStageFailed bool, now time.Time) {
	if !sessionStageFailed {
		*s = cycleStall{}
		return
	}
	if s.count == 0 {
		s.since = now
	}
	s.count++
}

// stalled reports whether the run has lasted the window with queued pool jobs
// a fresh REST reading can vouch for. Without known demand an idle pool, or a
// GitHub outage that also stops the REST reads, keeps the process running: a
// restart would not deliver a job that does not exist.
func (s cycleStall) stalled(window time.Duration, now time.Time, summary controller.CycleSummary) bool {
	return window > 0 && s.count > 0 && now.Sub(s.since) >= window &&
		summary.DemandKnown && summary.QueuedDemand > 0
}

// cycleDeadline bounds one Cycle call at half the deaf-session window (15
// minutes by default). Cycle had no deadline and the Kubernetes calls under it
// no timeout, so one hung call stopped the pool silently: the deaf check runs
// inside Cycle and could not fire (review R3, D8). Half the window keeps a
// hung cycle from outliving the deaf watchdog, and still leaves about twice
// the measured worst healthy cycle at four slots: four sequential teardowns at
// the ~80 s absence-wait bound, a ~70 s long-poll sweep and creates, about
// seven minutes. At twelve slots the teardown waits alone can reach 16
// minutes, which is why #960 must bound them before that raise.
func cycleDeadline(deafAfter time.Duration) time.Duration { return deafAfter / 2 }

func newDaemonRuntime(cycle daemonCycle, server bootstrapServer, interval time.Duration, observe func(error)) (*daemonRuntime, error) {
	if cycle == nil || server == nil || interval <= 0 {
		return nil, errors.New("daemon runtime requires a cycle, bootstrap server and positive interval")
	}
	if observe == nil {
		observe = func(err error) { log.Printf("runner-pool cycle failed: %v", err) }
	}
	return &daemonRuntime{cycle: cycle, server: server, interval: interval, observe: observe,
		summarize: func(summary controller.CycleSummary) { log.Print(formatCycleSummary(summary)) }}, nil
}

// formatCycleSummary is one line per cycle, success included. Silence on
// success cost an operator hours: the controller advertised capacity 0 to every
// scale set, GitHub offered it nothing, and no log line, journal row or health
// field said so. `admission=0` is the tell, and this is the only place it is
// visible.
// formatLastEnvelopeAge renders the last_envelope_age cycle field. Three states
// must stay distinguishable because they point an operator in three directions:
// "unknown" (the cycle failed before measuring -- read the error, not this
// field), "none" (measured, and nothing has arrived since this process started),
// and a rounded duration.
func formatLastEnvelopeAge(summary controller.CycleSummary) string {
	switch {
	case !summary.LastEnvelopeMeasured:
		return "unknown"
	case summary.LastEnvelopeAge < 0:
		return "none"
	default:
		return summary.LastEnvelopeAge.Round(time.Second).String()
	}
}

// formatDeafSilence is the deaf exit line's "no envelope ..." phrase. It used to
// print LastEnvelopeAge alone, which is -1 when this process has received no
// envelope at all and rounded to "for 0s" -- the reading on every deaf exit of
// 2026-10-01, since each restarted process heard nothing. The silence the
// verdict was actually judged on is the watchdog's own clock (Silence).
func formatDeafSilence(summary controller.CycleSummary) string {
	silence := "unmeasured"
	if summary.SilenceJudged {
		silence = summary.Silence.Round(time.Second).String()
	}
	switch {
	case !summary.LastEnvelopeMeasured:
		return "(last envelope unknown; silence " + silence + ")"
	case summary.LastEnvelopeAge < 0:
		return "since this process started (silence " + silence + ")"
	default:
		return "for " + summary.LastEnvelopeAge.Round(time.Second).String() + " (silence " + silence + ")"
	}
}

// formatPollOutcomes renders the polls= cycle field: how many of this cycle's
// polls GitHub answered each way. Fixed field order (202, 200, 401, other,
// refused, quarantined) so a log query can grep a specific bucket without
// parsing the whole value.
func formatPollOutcomes(outcomes controller.PollOutcomes) string {
	return fmt.Sprintf("202:%d,200:%d,401:%d,other:%d,refused:%d,quarantined:%d",
		outcomes.Empty, outcomes.Message, outcomes.Refreshed, outcomes.Other, outcomes.Refused, outcomes.Quarantined)
}

// maxListedSets bounds a per-set list on the cycle line, so a pool-wide
// failure across all 49 sets cannot turn one line into several kilobytes.
const maxListedSets = 5

// formatSetKeys renders repo/set pairs, the first maxListedSets of them, then
// `+N` for the rest.
func formatSetKeys(keys []controller.SetKey) string {
	var out strings.Builder
	for i, key := range keys {
		if i == maxListedSets {
			fmt.Fprintf(&out, ",+%d", len(keys)-maxListedSets)
			break
		}
		if i > 0 {
			out.WriteString(",")
		}
		fmt.Fprintf(&out, "%d/%d", key.RepositoryID, key.ScaleSetID)
	}
	return out.String()
}

// formatDeafRotation renders what the deaf exit's session rotation did
// (homelab#1138): counts always, so `rotated=0` is visible, and the sets
// behind each non-zero count. `failed` sets kept a close-intent and are
// skipped by every later Sync until it is resolved; `skipped` sets were not
// attempted because a DELETE failed first, and resume as before.
func formatDeafRotation(rotation controller.SessionRotation) string {
	failed := make([]controller.SetKey, 0, len(rotation.Failed))
	for _, set := range rotation.Failed {
		failed = append(failed, set.Set)
	}
	var out strings.Builder
	for _, group := range []struct {
		name string
		sets []controller.SetKey
	}{{"rotated", rotation.Rotated}, {"kept", rotation.Kept}, {"failed", failed}, {"skipped", rotation.Skipped}} {
		fmt.Fprintf(&out, " %s=%d", group.name, len(group.sets))
		if len(group.sets) > 0 {
			fmt.Fprintf(&out, " %s_sets=%s", group.name, formatSetKeys(group.sets))
		}
	}
	if rotation.Blocked != "" {
		// Nothing was attempted: a set already holds a close-intent (or the
		// journal could not be read), and rotating more could strand another.
		fmt.Fprintf(&out, " rotation_blocked=%s close_intent=%d", rotation.Blocked, len(rotation.CloseIntent))
		if len(rotation.CloseIntent) > 0 {
			fmt.Fprintf(&out, " close_intent_sets=%s", formatSetKeys(rotation.CloseIntent))
		}
	}
	return out.String()
}

// formatBreakerUntil is the provisioning breaker's deadline in UTC RFC3339,
// `none` when it is closed, `unknown` when the cycle never read it, and
// `invalid` when the stored deadline does not parse.
func formatBreakerUntil(summary controller.CycleSummary) string {
	switch {
	case !summary.BreakerMeasured:
		return "unknown"
	case !summary.BreakerOpen:
		return "none"
	case summary.BreakerUntil.IsZero():
		return "invalid"
	default:
		return summary.BreakerUntil.UTC().Format(time.RFC3339)
	}
}

func formatCycleSummary(summary controller.CycleSummary) string {
	var sets strings.Builder
	for i, advertised := range summary.Advertised {
		if i > 0 {
			sets.WriteString(",")
		}
		fmt.Fprintf(&sets, "%d/%d=%d", advertised.Set.RepositoryID, advertised.Set.ScaleSetID, advertised.MaxCapacity)
	}
	if sets.Len() == 0 {
		sets.WriteString("none")
	}
	reason := summary.Binding
	if reason == "" {
		reason = "unknown"
	}
	// An open breaker outranks the host's binding dimension: it is what holds
	// admission at the active count, whatever the host has free. On 2026-10-01
	// it held admission at zero for fifteen minutes while this field read
	// `slots` (homelab#1213).
	if summary.BreakerMeasured && summary.BreakerOpen {
		reason = "breaker"
	}
	age := formatLastEnvelopeAge(summary)
	// retained= is unconditional, zero included, and follows every field an
	// operator or log query already reads by position. A field shown only when
	// non-zero would make "nothing retained" look like "not reported"
	// (homelab#924).
	retained := "unknown"
	if summary.RetainedMeasured {
		retained = strconv.Itoa(summary.Retained)
	}
	line := fmt.Sprintf("runner-pool cycle: ready=%t admission=%d free=%d reason=%s charged=%d polled=%d advertised=%s messages=%d polls=%s last_envelope_age=%s retained=%s",
		summary.Ready, summary.Admission, summary.FreeSlots, reason, summary.Charged,
		len(summary.Advertised), sets.String(), summary.Messages, formatPollOutcomes(summary.Polls), age, retained)
	// degraded= and pollfail= are unconditional for the same reason as
	// retained=: a set skipped or unpolled this cycle must never read as "not
	// reported" (homelab#1000). The set lists follow only when non-zero.
	line += fmt.Sprintf(" degraded=%d pollfail=%d", summary.DegradedSessions, summary.PollFailures)
	// breaker_until= is unconditional for the same reason (homelab#1213): the
	// breaker is persisted and outlives restarts, and a field shown only while
	// it is open would make "closed" and "this build does not report it" read
	// the same.
	line += " breaker_until=" + formatBreakerUntil(summary)
	if len(summary.DegradedSets) > 0 {
		line += " degraded_sets=" + formatSetKeys(summary.DegradedSets)
	}
	if len(summary.PollFailedSets) > 0 {
		line += " pollfail_sets=" + formatSetKeys(summary.PollFailedSets)
	}
	// Appended, never interleaved: the field order above is what an operator and
	// any log query already read. Each field below appears only when it carries
	// information, so a quiet cycle keeps the line it has always had.
	//
	// Only when non-zero: deferred demand is what the pool could not advertise
	// this cycle, so a standing `deferred=` is the tell that jobs are queued
	// behind admission rather than lost.
	if summary.Deferred > 0 {
		line += fmt.Sprintf(" deferred=%d", summary.Deferred)
	}
	// The pending fields appear only while a sizing change is deferred, which is
	// what makes them worth noticing.
	if summary.SizingPending {
		line += fmt.Sprintf(" pending_capacity=%d pending_legacy_slots=%d",
			summary.PendingCapacity, summary.PendingLegacySlots)
	}
	// The draining cycle stops before PollSweep, so it reads `polled=0
	// advertised=none` -- the silent-outage signature. This marker is what tells
	// the two apart.
	if summary.Draining {
		line += " draining=true"
	}
	// Onboarding, and only while it is happening. With an empty allowlist the
	// pool serves every private repository the App installation can see, so
	// `creating=` is how an operator watches that one-time sweep progress,
	// `create_deferred=` says the per-cycle creation budget is still working
	// through it, and `over_cap=` says -max-repositories refused repositories
	// the pool would otherwise serve. A finished, bounded pool prints none of them.
	//
	// These trail the draining marker deliberately: the sizing and draining
	// fields are the outage signature an operator greps for, and onboarding is a
	// transient that must not push them around on the line.
	if summary.Management.Creating > 0 {
		line += fmt.Sprintf(" creating=%d", summary.Management.Creating)
	}
	if summary.Management.CreateDeferred > 0 {
		line += fmt.Sprintf(" create_deferred=%d", summary.Management.CreateDeferred)
	}
	if summary.Management.OverCap > 0 {
		line += fmt.Sprintf(" over_cap=%d over_cap_first=%s", summary.Management.OverCap, summary.Management.OverCapFirstName)
	}
	// A remote scale set outliving its repository's eligibility. The controller
	// never deletes one, so this persists across cycles AND restarts until an
	// operator clears it by hand -- which is exactly why it is on this line:
	// nothing else anywhere mentions the leftover resource.
	if summary.Management.Orphaned > 0 {
		line += fmt.Sprintf(" orphaned=%d orphaned_first=%s", summary.Management.Orphaned, summary.Management.OrphanedFirstName)
	}
	// Only when non-zero, and last: a probe is rare, and the per-probe INFO line
	// carries the counts behind it (homelab#1216).
	if summary.UnseenProbes > 0 {
		line += fmt.Sprintf(" unseen_probes=%d", summary.UnseenProbes)
	}
	return line
}

// Run starts the authenticated bootstrap server, performs one immediate
// reconciliation, then schedules sequential bounded cycles. Cancellation is
// propagated to both components and waits for the server's graceful shutdown.
func (r *daemonRuntime) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("daemon runtime requires a context")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErr := make(chan error, 1)
	go func() { serverErr <- r.server.Serve(runCtx) }()
	// The background task shares the store the caller closes once Run returns,
	// so every return path cancels it and waits for it to finish first. This
	// defer runs before the `defer cancel()` above; cancelling twice is harmless.
	backgroundDone := make(chan struct{})
	if r.background != nil {
		go func() {
			defer close(backgroundDone)
			r.background(runCtx)
		}()
	} else {
		close(backgroundDone)
	}
	defer func() {
		cancel()
		<-backgroundDone
	}()

	// cycle reports whether the process should now stop, and why. A sizing
	// restart is a planned adoption of a deferred change; a deaf restart is a
	// fault -- GitHub stopped talking to every subscription and a pod restart
	// is the only remedy proven on 2026-09-23. A restart-required error and a
	// stalled run of failing cycles are faults of the same kind: no cycle can
	// clear them, so they take the deaf exit (homelab#1211). The caller needs
	// to know which fired to pick the exit code and the log line.
	var pending controller.CycleSummary
	var pendingRotation controller.SessionRotation
	var pendingErr error
	var stall cycleStall
	now := r.now
	if now == nil {
		now = time.Now
	}
	cycle := func() stopReason {
		if runCtx.Err() != nil {
			return stopNone
		}
		cycleCtx, cancelCycle := runCtx, context.CancelFunc(func() {})
		if r.deadline > 0 {
			cycleCtx, cancelCycle = context.WithTimeout(runCtx, r.deadline)
		}
		result, err := r.cycle.Cycle(cycleCtx)
		cancelCycle()
		// The summary is emitted for a failed cycle too. A cycle that fails
		// after PollSweep still advertised something, and that is exactly the
		// number worth having beside the error.
		if r.summarize != nil {
			r.summarize(result.Summary)
		}
		if result.ConnectionsClosed > 0 {
			log.Printf("Queue sessions quiet for %s with no worker running; closing idle HTTP connections on %d sessions to rule out stale keep-alive connections before a restart.",
				result.Summary.SessionDeafWindow/2, result.ConnectionsClosed)
		}
		if err != nil {
			r.observe(err)
		}
		pending = result.Summary
		pendingRotation = result.Rotation
		pendingErr = err
		at := now()
		stall.observe(result.SessionStageFailed, at)
		switch {
		case result.SessionsDeaf:
			return stopForDeafness
		case errors.Is(err, controller.ErrRestartRequired):
			return stopForRestartRequired
		case stall.stalled(r.failedCycleRestartAfter, at, result.Summary):
			return stopForStalledCycles
		case result.RestartForSizing:
			return stopForSizing
		}
		return stopNone
	}
	stop := func(reason stopReason) error {
		var shutdownErr error
		switch reason {
		case stopForDeafness:
			log.Printf("Queue sessions look deaf: no envelope from any subscription %s. "+
				"Exiting so the Deployment restarts the controller. Sets with no assigned work had their sessions closed and get new ones on restart; the rest resume the same session (homelab#1138). window=%s subscriptions=%d basis=%s queued_pool_jobs=%d%s",
				formatDeafSilence(pending), pending.SessionDeafWindow, len(pending.Advertised),
				deafBasis(pending.DemandKnown), pending.QueuedDemand, formatDeafRotation(pendingRotation))
			shutdownErr = errSessionsDeaf
		case stopForRestartRequired:
			log.Printf("Session state needs a process restart: %v. "+
				"Exiting so the Deployment restarts the controller; every set resumes its journaled session (homelab#1211). consecutive_failed_cycles=%d queued_pool_jobs=%d demand_known=%t",
				pendingErr, stall.count, pending.QueuedDemand, pending.DemandKnown)
			shutdownErr = errRestartRequired
		case stopForStalledCycles:
			log.Printf("Cycles have failed in the session stage for %s with queued pool jobs waiting; last error: %v. "+
				"Exiting so the Deployment restarts the controller; every set resumes its journaled session (homelab#1211). consecutive_failed_cycles=%d window=%s queued_pool_jobs=%d",
				now().Sub(stall.since).Round(time.Second), pendingErr, stall.count, r.failedCycleRestartAfter, pending.QueuedDemand)
			shutdownErr = errCyclesStalled
		default:
			log.Printf("runner-pool sizing change pending and journal idle; restarting to adopt pending_capacity=%d pending_legacy_slots=%d",
				pending.PendingCapacity, pending.PendingLegacySlots)
		}
		cancel()
		if err := <-serverErr; err != nil && ctx.Err() == nil {
			return errors.Join(shutdownErr, fmt.Errorf("bootstrap server shutdown: %w", err))
		}
		return shutdownErr
	}
	if reason := cycle(); reason != stopNone {
		return stop(reason)
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-serverErr:
			cancel()
			if ctx.Err() != nil {
				return nil
			}
			if err == nil {
				return errors.New("bootstrap server stopped unexpectedly")
			}
			return fmt.Errorf("bootstrap server: %w", err)
		case <-ctx.Done():
			cancel()
			err := <-serverErr
			if err != nil {
				return fmt.Errorf("bootstrap server shutdown: %w", err)
			}
			return nil
		case <-ticker.C:
			if reason := cycle(); reason != stopNone {
				return stop(reason)
			}
		}
	}
}

// buildDaemon composes the real App-bound controller. Backend creation is the
// only platform-specific dependency and is intentionally injected.
func buildDaemon(ctx context.Context, config RuntimeConfig, backends BackendFactory) (*daemonRuntime, func() error, error) {
	config, err := config.Validate()
	if err != nil {
		return nil, nil, err
	}
	if backends == nil {
		return nil, nil, errors.New("daemon requires a worker backend factory")
	}
	app, err := githubapp.New(config.AppIDFile, config.AppKeyFile)
	if err != nil {
		return nil, nil, err
	}
	installation, err := app.InstallationFor(ctx, config.InstallationRepo)
	if err != nil {
		return nil, nil, err
	}
	store, err := controller.Open(config.StateDir, controller.Config{
		Platform: config.Platform, InstallationID: installation.ID,
		Capacity: config.Capacity, LegacySlots: config.LegacySlots,
		RepositoryAllowlist: config.RepositoryAllowlist,
		MaxRepositories:     config.MaxRepositories,
	})
	if err != nil {
		return nil, nil, err
	}
	// THE BACKEND MUST BE BUILT FROM THE STORE'S EFFECTIVE SIZING, NOT THE FLAGS'.
	// composeLinuxBackend passes config.Capacity into both the Kubernetes worker
	// Config and the AdmissionBudget, and NewTrustedInventory asserts the two
	// agree. When Open defers a sizing change, the store runs on the journal's
	// numbers while the flags still say something else -- so a deferred LOWERING
	// would give the controller two slots and the backend a budget for one, and
	// every second worker would be refused for headroom that logically exists.
	// Re-reading the effective numbers here keeps one source of truth.
	config.Capacity, config.LegacySlots = store.PlatformSlots()
	closeStore := func() error { return store.Close() }
	fail := func(err error) (*daemonRuntime, func() error, error) {
		_ = closeStore()
		return nil, nil, err
	}
	registry, err := controller.NewRegistry(store, app, config.InstallationRepo)
	if err != nil {
		return fail(err)
	}
	sessions, err := controller.NewSessionManager(store, app, installation)
	if err != nil {
		return fail(err)
	}
	broker, err := controller.NewJITBroker(store, app, installation)
	if err != nil {
		return fail(err)
	}
	backend, err := backends.New(ctx, config, store)
	if err != nil {
		return fail(err)
	}
	lifecycle, err := controller.NewPoolLifecycle(store, sessions, broker, backend, config.BootstrapURL)
	if err != nil {
		return fail(err)
	}
	daemon, err := controller.NewDaemon(store, registry, sessions, lifecycle, config.SessionDeafAfter)
	if err != nil {
		return fail(err)
	}
	if err := daemon.SetCreateConcurrency(config.CreateConcurrency); err != nil {
		return fail(err)
	}
	log.Printf("runner-pool worker creation: concurrency=%d (first create of each pass runs alone, then at most %d at once, fewer when free slots are short)",
		config.CreateConcurrency, min(config.CreateConcurrency, controller.MaxFanOut))
	server, err := bootstrap.NewServer(bootstrap.ServerConfig{
		Address: config.BootstrapAddress, CertificateFile: config.BootstrapCertificateFile,
		KeyFile: config.BootstrapKeyFile,
	}, broker)
	if err != nil {
		return fail(err)
	}
	runtime, err := newDaemonRuntime(daemon, server, config.CycleInterval, nil)
	if err != nil {
		return fail(err)
	}
	runtime.deadline = cycleDeadline(config.SessionDeafAfter)
	runtime.failedCycleRestartAfter = config.FailedCycleRestartAfter
	// The REST demand reader (homelab#913) runs independent of the sequential
	// cycle above: it is an optimisation layered over the queue sessions, never
	// a dependency of them, so it is wired as daemonRuntime.background rather
	// than a step inside Daemon.Cycle.
	// An installation without actions:read gets a disabled tracker and one
	// error line, never a refusal to start: REST demand is optional.
	restTracker, err := controller.NewGitHubRESTDemandTracker(store, app, installation, config.Platform, config.RESTDemandInterval, nil)
	if err != nil {
		return fail(err)
	}
	// The queue janitor is a second, independent background task under its
	// own App. It is optional: every way it can be unconfigured leaves it off
	// with one log line and never stops the controller starting.
	janitor, err := buildQueueJanitor(ctx, config, installation, store, slog.Default())
	if err != nil {
		return fail(err)
	}
	tasks := []func(context.Context){restTracker.Run, janitor.Run}
	if config.ReapInterval > 0 {
		log.Printf("runner-pool reaper on: interval=%s refill_cap=%d", config.ReapInterval, controller.ReapRefillCap)
		tasks = append(tasks, func(ctx context.Context) { runReaper(ctx, daemon, config.ReapInterval) })
	} else {
		log.Printf("runner-pool reaper off: interval=%s; finished workers are reaped once per cycle", config.ReapInterval)
	}
	runtime.background = runConcurrently(tasks...)
	return runtime, closeStore, nil
}

// runConcurrently runs every task for the life of ctx and returns once all
// of them have, so daemonRuntime's wait on its background task still covers
// each one.
func runConcurrently(tasks ...func(context.Context)) func(context.Context) {
	return func(ctx context.Context) {
		var group sync.WaitGroup
		for _, task := range tasks {
			group.Go(func() { task(ctx) })
		}
		group.Wait()
	}
}

// buildQueueJanitor builds the queue janitor from its own App's files. The
// pool App is used only for the account the janitor installation must share.
// Nothing here returns an error for configuration: an unknown mode, absent
// or unreadable App files, or an installation lookup that fails each leave
// the janitor off with one log line.
func buildQueueJanitor(ctx context.Context, config RuntimeConfig, pool githubapp.Installation, store *controller.Store, logger *slog.Logger) (*controller.QueueJanitor, error) {
	mode := controller.QueueJanitorMode(config.QueueJanitor)
	off := func(message string, args ...any) (*controller.QueueJanitor, error) {
		logger.Warn(message, args...)
		return controller.NewQueueJanitor(store, nil, config.Platform, controller.QueueJanitorOff, config.QueueJanitorInterval), nil
	}
	if config.queueJanitorRejected != "" {
		return off("Queue janitor off: -queue-janitor is not off, dry-run or enforce, so it fails closed to off.",
			"rejected_value", config.queueJanitorRejected)
	}
	if mode == controller.QueueJanitorOff {
		return controller.NewGitHubQueueJanitor(store, nil, githubapp.Installation{}, config.Platform, mode, config.QueueJanitorInterval, logger)
	}
	if config.JanitorAppIDFile == "" || config.JanitorAppKeyFile == "" || config.JanitorInstallationIDFile == "" {
		return off("Queue janitor off: no janitor GitHub App is configured. It needs -janitor-app-id-file, -janitor-app-key-file and -janitor-installation-id-file.",
			"mode", mode)
	}
	app, err := githubapp.New(config.JanitorAppIDFile, config.JanitorAppKeyFile)
	if err != nil {
		return off("Queue janitor off: the janitor GitHub App's files could not be read; its Secret is optional and may be absent.",
			"mode", mode, "error", err)
	}
	raw, err := os.ReadFile(config.JanitorInstallationIDFile)
	if err != nil {
		return off("Queue janitor off: the janitor installation ID file could not be read.", "mode", mode)
	}
	id, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || id <= 0 {
		return off("Queue janitor off: the janitor installation ID file does not hold a positive integer.", "mode", mode)
	}
	installation, err := app.JanitorInstallation(ctx, id, pool.Account.Login)
	if err != nil {
		return off("Queue janitor off: the janitor installation could not be verified.", "mode", mode, "installation", id, "error", err)
	}
	return controller.NewGitHubQueueJanitor(store, app, installation, config.Platform, mode, config.QueueJanitorInterval, logger)
}

func runDaemon(config RuntimeConfig) error {
	return runDaemonWithFactory(config, productionBackendFactory)
}

func runDaemonWithFactory(config RuntimeConfig, backends BackendFactory) error {
	ctx, stop := signalContext()
	defer stop()
	runtime, closeStore, err := buildDaemon(ctx, config, backends)
	if err != nil {
		return err
	}
	defer closeStore()
	return runtime.Run(ctx)
}

func signalContext() (context.Context, func()) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// reaper is the part of the daemon the reap loop drives; an interface so the
// loop is testable without a store.
type reaper interface {
	Reap(context.Context) (controller.ReapResult, error)
}

// runReaper runs Reap every interval for the life of ctx (homelab#1190).
// Reap stands aside while a cycle holds the lifecycle, so this never queues
// behind the cycle; its errors are logged and retried next tick, never fatal:
// the cycle still reaps on its own schedule if the reaper keeps failing.
func runReaper(ctx context.Context, daemon reaper, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		result, err := daemon.Reap(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("runner-pool reap failed after refilling %d slot(s) retained=%d; the next cycle still reaps: %v", len(result.Provisioned), result.Retained, err)
			continue
		}
		if len(result.Provisioned) > 0 {
			log.Printf("runner-pool reap: refilled %d slot(s) between cycles retained=%d", len(result.Provisioned), result.Retained)
		}
	}
}
