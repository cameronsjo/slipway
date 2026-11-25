package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

type fakeRuntimeCycle struct {
	calls  chan struct{}
	err    error
	result controller.CycleResult
}

func (f *fakeRuntimeCycle) Cycle(context.Context) (controller.CycleResult, error) {
	select {
	case f.calls <- struct{}{}:
	default:
	}
	return f.result, f.err
}

type fakeRuntimeServer struct {
	started chan struct{}
	err     error
}

func (f *fakeRuntimeServer) Serve(ctx context.Context) error {
	close(f.started)
	if f.err != nil {
		return f.err
	}
	<-ctx.Done()
	return nil
}

func TestRuntimeConfigDefaultsPerPlatform(t *testing.T) {
	base := RuntimeConfig{
		Platform:                 "linux",
		InstallationRepo:         "cameronsjo/homelab",
		AppIDFile:                "/run/app/id",
		AppKeyFile:               "/run/app/key",
		StateDir:                 "/var/lib/runner-pool",
		Capacity:                 -1,
		LegacySlots:              -1,
		BootstrapURL:             "https://runner-pool.example:8443",
		BootstrapAddress:         "127.0.0.1:8443",
		BootstrapCertificateFile: "/run/tls/cert",
		BootstrapKeyFile:         "/run/tls/key",
		RepositoryAllowlist:      []string{"cameronsjo/workout-tracker"},
	}
	linux, err := base.Validate()
	if err != nil {
		t.Fatalf("Validate() linux: %v", err)
	}
	if linux.Capacity != linuxCapacityCeiling || linux.LegacySlots != 0 || linux.CycleInterval != defaultDaemonInterval {
		t.Fatalf("unexpected linux defaults: %+v", linux)
	}

	base.Platform = "macos"
	macOS, err := base.Validate()
	if err != nil {
		t.Fatalf("Validate() macos: %v", err)
	}
	if macOS.Capacity != macOSCapacityCeiling || macOS.LegacySlots != 1 {
		t.Fatalf("unexpected macOS defaults: %+v", macOS)
	}

	base.Platform = "linux"
	base.Capacity = 0
	base.LegacySlots = 0
	base.CycleInterval = time.Second
	zero, err := base.Validate()
	if err != nil {
		t.Fatalf("Validate() explicit zero: %v", err)
	}
	if zero.Capacity != 0 || zero.LegacySlots != 0 || zero.CycleInterval != time.Second {
		t.Fatalf("explicit zero values were replaced: %+v", zero)
	}

	// The default posture: no allowlist, serving every private, active
	// repository, bounded by the repository cap rather than by a list.
	base.RepositoryAllowlist = nil
	open, err := base.Validate()
	if err != nil {
		t.Fatalf("Validate() rejected an empty allowlist: %v", err)
	}
	if open.MaxRepositories != defaultMaxRepositories {
		t.Fatalf("an unset repository cap did not adopt the default: %+v", open)
	}
}

func TestRuntimeConfigRejectsUnsafeOrUnboundedValues(t *testing.T) {
	base := RuntimeConfig{
		Platform:                 "linux",
		InstallationRepo:         "cameronsjo/homelab",
		AppIDFile:                "/run/app/id",
		AppKeyFile:               "/run/app/key",
		StateDir:                 "/var/lib/runner-pool",
		Capacity:                 -1,
		LegacySlots:              -1,
		BootstrapURL:             "https://runner-pool.example:8443",
		BootstrapAddress:         "127.0.0.1:8443",
		BootstrapCertificateFile: "/run/tls/cert",
		BootstrapKeyFile:         "/run/tls/key",
		CycleInterval:            time.Second,
		RepositoryAllowlist:      []string{"cameronsjo/workout-tracker"},
	}
	tests := []struct {
		name string
		edit func(*RuntimeConfig)
		want string
	}{
		{"unknown platform", func(c *RuntimeConfig) { c.Platform = "windows" }, "platform"},
		{"capacity above ceiling", func(c *RuntimeConfig) { c.Capacity = linuxCapacityCeiling + 1 }, "capacity"},
		{"capacity invalid sentinel", func(c *RuntimeConfig) { c.Capacity = -2 }, "capacity"},
		{"legacy slots above capacity", func(c *RuntimeConfig) { c.LegacySlots = 1; c.Capacity = 0 }, "legacy"},
		{"legacy slots invalid sentinel", func(c *RuntimeConfig) { c.LegacySlots = -2 }, "legacy"},
		{"invalid repository", func(c *RuntimeConfig) { c.InstallationRepo = "homelab" }, "owner/repository"},
		{"IP bootstrap origin", func(c *RuntimeConfig) { c.BootstrapURL = "https://192.0.2.1:8443" }, "DNS"},
		{"missing bootstrap port", func(c *RuntimeConfig) { c.BootstrapURL = "https://runner-pool.example" }, "explicit port"},
		{"nonpositive interval", func(c *RuntimeConfig) { c.CycleInterval = -time.Second }, "interval"},
		{"reap interval below the floor", func(c *RuntimeConfig) { c.ReapInterval = time.Second }, "reap-interval"},
		{"reap interval above the ceiling", func(c *RuntimeConfig) { c.ReapInterval = 10 * time.Minute }, "reap-interval"},
		{"negative create concurrency", func(c *RuntimeConfig) { c.CreateConcurrency = -1 }, "create-concurrency"},
		{"create concurrency above the ceiling", func(c *RuntimeConfig) { c.CreateConcurrency = controller.MaxCreateConcurrency + 1 }, "create-concurrency"},
		// Only the Kubernetes backend counts creates in flight against its
		// headroom; tart would let two creates take one slot.
		{"concurrent creates on macos", func(c *RuntimeConfig) { c.Platform = "macos"; c.Capacity = -1; c.CreateConcurrency = 2 }, "create-concurrency"},
		// An absent allowlist is now legal -- it serves every private, active
		// repository -- so the bound it used to provide is this cap, and a value
		// one cycle cannot poll is refused before the journal opens.
		{"repository cap above ceiling", func(c *RuntimeConfig) { c.MaxRepositories = maxRepositoriesCeiling + 1 }, "max-repositories"},
		{"negative repository cap", func(c *RuntimeConfig) { c.MaxRepositories = -1 }, "max-repositories"},
		// The cycle deadline is half the deaf window; it must outlast one tick.
		{"deaf window too short for the cycle", func(c *RuntimeConfig) { c.SessionDeafAfter = time.Second }, "cycle deadline"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			tc.edit(&config)
			_, err := config.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestDaemonRuntimeCyclesImmediatelyContinuesAfterCycleErrorAndShutsDown(t *testing.T) {
	cycleErr := errors.New("transient cycle failure")
	cycle := &fakeRuntimeCycle{calls: make(chan struct{}, 16), err: cycleErr}
	server := &fakeRuntimeServer{started: make(chan struct{})}
	observed := make(chan error, 16)
	runtime, err := newDaemonRuntime(cycle, server, 5*time.Millisecond, func(err error) { observed <- err })
	if err != nil {
		t.Fatalf("newDaemonRuntime() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	for range 3 {
		select {
		case <-cycle.calls:
		case <-time.After(time.Second):
			t.Fatal("daemon did not run three sequential cycles")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() after cancellation error = %v", err)
	}
	select {
	case err := <-observed:
		if !errors.Is(err, cycleErr) {
			t.Fatalf("observed error = %v, want %v", err, cycleErr)
		}
	case <-time.After(time.Second):
		t.Fatal("cycle failure was not observed")
	}
}

func TestDaemonRuntimeReturnsBootstrapFailure(t *testing.T) {
	serverErr := errors.New("listener failed")
	cycle := &fakeRuntimeCycle{calls: make(chan struct{}, 1)}
	server := &fakeRuntimeServer{started: make(chan struct{}), err: serverErr}
	runtime, err := newDaemonRuntime(cycle, server, time.Hour, nil)
	if err != nil {
		t.Fatalf("newDaemonRuntime() error = %v", err)
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, serverErr) {
		t.Fatalf("Run() error = %v, want %v", err, serverErr)
	}
}

func TestDaemonRuntimeRequiresDependencies(t *testing.T) {
	server := &fakeRuntimeServer{started: make(chan struct{})}
	cycle := &fakeRuntimeCycle{calls: make(chan struct{}, 1)}
	for name, tc := range map[string]struct {
		c        daemonCycle
		s        bootstrapServer
		interval time.Duration
	}{
		"nil cycle":     {nil, server, time.Second},
		"nil server":    {cycle, nil, time.Second},
		"zero interval": {cycle, server, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newDaemonRuntime(tc.c, tc.s, tc.interval, nil); err == nil {
				t.Fatal("newDaemonRuntime() unexpectedly accepted invalid dependencies")
			}
		})
	}
}

func TestTheCycleLineNamesAdmissionAndPerSetCapacity(t *testing.T) {
	line := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 0, Charged: 0, Messages: 0,
		Advertised: []controller.SetAdvertisement{
			{Set: controller.SetKey{RepositoryID: 11, ScaleSetID: 4}, MaxCapacity: 0},
			{Set: controller.SetKey{RepositoryID: 12, ScaleSetID: 5}, MaxCapacity: 0},
		}})
	// admission=0 is the tell for the outage this line exists to reveal: every
	// set offered nothing, so GitHub had nothing to dispatch.
	for _, want := range []string{"ready=true", "admission=0", "charged=0", "polled=2", "11/4=0", "12/5=0", "messages=0"} {
		if !strings.Contains(line, want) {
			t.Fatalf("the cycle line is missing %q: %s", want, line)
		}
	}
	// An idle pool defers nothing, and a field that is always there is a field
	// nobody reads.
	if strings.Contains(line, "deferred") {
		t.Fatalf("a cycle with no deferred demand named it anyway: %s", line)
	}
}

// The polls= field is unconditional, like messages=, so it always has a fixed
// six-bucket shape (including refused, homelab#1097, and quarantined,
// homelab#1139) even on a cycle that never touched a real session.
func TestTheCycleLineNamesPollOutcomes(t *testing.T) {
	line := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 48,
		Polls: controller.PollOutcomes{Empty: 47, Message: 1, Refreshed: 0, Other: 0, Refused: 2, Quarantined: 1}})
	if !strings.Contains(line, "polls=202:47,200:1,401:0,other:0,refused:2,quarantined:1") {
		t.Fatalf("the cycle line is missing the poll outcome tally: %s", line)
	}
	// It sits between messages= and last_envelope_age=, so neither existing
	// field's relative order to the other moved.
	messages := strings.Index(line, "messages=")
	polls := strings.Index(line, "polls=")
	age := strings.Index(line, "last_envelope_age=")
	if !(messages < polls && polls < age) {
		t.Fatalf("polls= is not between messages= and last_envelope_age=: %s", line)
	}
	blank := formatCycleSummary(controller.CycleSummary{})
	if !strings.Contains(blank, "polls=202:0,200:0,401:0,other:0,refused:0,quarantined:0") {
		t.Fatalf("a cycle with no polls did not name the empty tally: %s", blank)
	}
}

// homelab#924: retained= is unconditional. A stuck teardown keeps its slot
// charged, and a field that appeared only when non-zero would make "nothing
// retained" and "this build does not report it" read the same. Zero prints as
// zero; a cycle that never reached Maintain prints unknown.
func TestTheCycleLineAlwaysNamesRetainedReservations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		summary controller.CycleSummary
		want    string
	}{
		{"measured, none retained", controller.CycleSummary{RetainedMeasured: true, Retained: 0}, " retained=0"},
		{"measured, one retained", controller.CycleSummary{RetainedMeasured: true, Retained: 1}, " retained=1"},
		{"cycle failed before Maintain", controller.CycleSummary{}, " retained=unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := formatCycleSummary(tc.summary)
			if !strings.Contains(line, tc.want) {
				t.Fatalf("the cycle line is missing %q: %s", tc.want, line)
			}
			// It follows last_envelope_age=, so no existing field moved.
			if strings.Index(line, "last_envelope_age=") > strings.Index(line, " retained=") {
				t.Fatalf("retained= was interleaved ahead of last_envelope_age=: %s", line)
			}
		})
	}
}

// A fired close-connections rung must announce itself: the operator watching
// the log is the audience for "we tried something non-destructive before
// restarting", not just the SessionsDeaf exit that follows a full window later.
func TestConnectionsClosedIsLoggedAsAnAction(t *testing.T) {
	var buf strings.Builder
	original := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(original)

	cycle := &fakeRuntimeCycle{calls: make(chan struct{}, 1), result: controller.CycleResult{
		Summary:           controller.CycleSummary{SessionDeafWindow: 30 * time.Minute},
		ConnectionsClosed: 3,
	}}
	runtime, err := newDaemonRuntime(cycle, &fakeRuntimeServer{started: make(chan struct{})}, time.Hour, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-cycle.calls:
	case <-time.After(time.Second):
		t.Fatal("the cycle never ran")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	for _, want := range []string{"quiet for 15m0s", "closing idle HTTP connections on 3 sessions"} {
		if !strings.Contains(got, want) {
			t.Fatalf("the close-connections action was not logged; missing %q: %s", want, got)
		}
	}
}

// Deferred demand is the difference between "no jobs exist" and "jobs are
// waiting for a slot". The pool served neither for 30 minutes and said nothing.
func TestTheCycleLineNamesDeferredDemandWhenThereIsSome(t *testing.T) {
	line := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 1, Charged: 1, Messages: 1, Deferred: 2,
		Advertised: []controller.SetAdvertisement{
			{Set: controller.SetKey{RepositoryID: 11, ScaleSetID: 4}, MaxCapacity: 1},
			{Set: controller.SetKey{RepositoryID: 12, ScaleSetID: 5}, MaxCapacity: 0},
		}})
	if !strings.Contains(line, " deferred=2") {
		t.Fatalf("the cycle line is missing deferred demand: %s", line)
	}
	if !strings.HasSuffix(line, " deferred=2") {
		t.Fatalf("deferred demand did not append to the existing fields: %s", line)
	}
}

func TestTheCycleLineIsEmittedForEveryCycle(t *testing.T) {
	var lines []string
	cycle := &fakeRuntimeCycle{calls: make(chan struct{}, 4)}
	runtime, err := newDaemonRuntime(cycle, &fakeRuntimeServer{started: make(chan struct{})}, time.Hour, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	emitted := make(chan struct{}, 4)
	runtime.summarize = func(summary controller.CycleSummary) {
		lines = append(lines, formatCycleSummary(summary))
		emitted <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-emitted:
	case <-time.After(time.Second):
		t.Fatal("a cycle ran without summarising itself")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(lines) == 0 {
		t.Fatal("no cycle line was recorded")
	}
}

func TestTheCycleLineNamesTheShortDimension(t *testing.T) {
	// `free=0` alone sent an operator to the thin pool for a shortage that was
	// memory committed to idle CronJob limits.
	line := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 0, Charged: 0,
		FreeSlots: 0, Binding: "memory"})
	for _, want := range []string{"free=0", "reason=memory"} {
		if !strings.Contains(line, want) {
			t.Fatalf("the cycle line is missing %q: %s", want, line)
		}
	}
	// An observation that never completed has measured no dimension at all, so
	// it must not read as "nothing is short".
	blind := formatCycleSummary(controller.CycleSummary{Ready: false, FreeSlots: -1})
	if !strings.Contains(blind, "reason=unknown") {
		t.Fatalf("an unmeasured host did not read as unknown: %s", blind)
	}
}

// A deferred sizing change is the one thing that explains a manifest saying
// capacity 2 and a pool behaving as capacity 1. The fields are appended, so the
// existing order every operator and log query reads is untouched, and they
// appear only while a change is actually pending.
func TestTheCycleLineNamesAPendingSizingChange(t *testing.T) {
	ordinary := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 1, Charged: 0})
	if strings.Contains(ordinary, "pending_") {
		t.Fatalf("a cycle with nothing pending advertised a pending change: %s", ordinary)
	}
	pending := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 1, Charged: 1,
		SizingPending: true, PendingCapacity: 2, PendingLegacySlots: 0})
	if !strings.HasPrefix(pending, ordinary[:strings.Index(ordinary, " charged=")]) {
		t.Fatalf("the pending fields were interleaved rather than appended: %s", pending)
	}
	for _, want := range []string{"pending_capacity=2", "pending_legacy_slots=0"} {
		if !strings.Contains(pending, want) {
			t.Fatalf("the cycle line is missing %q: %s", want, pending)
		}
	}
}

// The three appended groups arrived from two branches that never saw each
// other, so each one's own test passes with the other two absent -- and a line
// carrying all three is the one shape neither pinned. A log query reads fields
// by position, so the order is the contract: deferred, then pending_*, then
// draining.
func TestTheCycleLineOrdersEveryAppendedField(t *testing.T) {
	line := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 1, Charged: 1,
		Deferred: 3, SizingPending: true, PendingCapacity: 2, PendingLegacySlots: 1, Draining: true})
	want := " deferred=3 pending_capacity=2 pending_legacy_slots=1 draining=true"
	if !strings.HasSuffix(line, want) {
		t.Fatalf("the appended fields are out of order; want suffix %q: %s", want, line)
	}
	// Each group is still independent: dropping the middle one must close the
	// gap rather than leave a hole or reorder what remains.
	noSizing := formatCycleSummary(controller.CycleSummary{Ready: true, Admission: 1, Charged: 1,
		Deferred: 3, Draining: true})
	if !strings.HasSuffix(noSizing, " deferred=3 draining=true") {
		t.Fatalf("omitting the pending group disturbed the remaining order: %s", noSizing)
	}
	if strings.Contains(noSizing, "pending_") {
		t.Fatalf("a cycle with nothing pending advertised a pending change: %s", noSizing)
	}
}

// TestFormatLastEnvelopeAge pins the three states the field must keep apart.
// The zero CycleSummary is what a cycle that failed before summarize carries;
// rendering its LastEnvelopeAge of 0 as "0s" would claim an envelope just
// arrived on exactly the failing cycles an operator reads during an incident.
func TestFormatLastEnvelopeAge(t *testing.T) {
	for _, tc := range []struct {
		name    string
		summary controller.CycleSummary
		want    string
	}{
		{"cycle failed before measuring", controller.CycleSummary{}, "unknown"},
		{"measured, nothing yet this process", controller.CycleSummary{LastEnvelopeMeasured: true, LastEnvelopeAge: -1}, "none"},
		{"measured, just arrived", controller.CycleSummary{LastEnvelopeMeasured: true, LastEnvelopeAge: 0}, "0s"},
		{"measured, rounds to the second", controller.CycleSummary{LastEnvelopeMeasured: true, LastEnvelopeAge: 3*time.Minute + 12*time.Second + 400*time.Millisecond}, "3m12s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatLastEnvelopeAge(tc.summary); got != tc.want {
				t.Fatalf("formatLastEnvelopeAge(%+v) = %q, want %q", tc.summary, got, tc.want)
			}
		})
	}
	// The field must actually appear on the line, not just in the helper.
	line := formatCycleSummary(controller.CycleSummary{})
	if !strings.Contains(line, "last_envelope_age=unknown") {
		t.Fatalf("a failed cycle's line does not say the age is unknown: %q", line)
	}
}

// homelab#1216: a deaf exit from a process that heard nothing said "for 0s",
// because LastEnvelopeAge is -1 there. The phrase now says which case it is
// and carries the silence the verdict was judged on.
func TestTheDeafExitLineSaysHowLongItWasSilent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		summary controller.CycleSummary
		want    string
	}{
		{"nothing heard this process", controller.CycleSummary{LastEnvelopeMeasured: true, LastEnvelopeAge: -1, Silence: 10*time.Minute + 400*time.Millisecond, SilenceJudged: true},
			"since this process started (silence 10m0s)"},
		{"heard, then silent", controller.CycleSummary{LastEnvelopeMeasured: true, LastEnvelopeAge: 39*time.Minute + 22*time.Second, Silence: 30 * time.Minute, SilenceJudged: true},
			"for 39m22s (silence 30m0s)"},
		{"not measured", controller.CycleSummary{}, "(last envelope unknown; silence unmeasured)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatDeafSilence(tc.summary); got != tc.want {
				t.Fatalf("formatDeafSilence = %q, want %q", got, tc.want)
			}
		})
	}
}

// homelab#1216: a cycle that journaled a probe says so at the end of the line,
// and a cycle that did not keeps the line it always had.
func TestTheCycleLineNamesUnseenProbesOnlyWhenThereAreSome(t *testing.T) {
	if line := formatCycleSummary(controller.CycleSummary{UnseenProbes: 1}); !strings.HasSuffix(line, " unseen_probes=1") {
		t.Fatalf("a probing cycle's line does not end with unseen_probes=1: %q", line)
	}
	if line := formatCycleSummary(controller.CycleSummary{}); strings.Contains(line, "unseen_probes") {
		t.Fatalf("a quiet cycle's line names probes: %q", line)
	}
}

// homelab#1138: the deaf exit line names which sets rotated and which were
// kept, with counts, and a zero count is still printed so "nothing rotated"
// cannot read as "the field is missing".
func TestTheDeafExitLineNamesRotatedAndKeptSets(t *testing.T) {
	set := func(repo int64, id int) controller.SetKey {
		return controller.SetKey{RepositoryID: repo, ScaleSetID: id}
	}
	got := formatDeafRotation(controller.SessionRotation{
		Rotated: []controller.SetKey{set(1, 101), set(2, 102)},
		Kept:    []controller.SetKey{set(3, 103)},
		Failed:  []controller.SessionDegradation{{Set: set(4, 104), Err: errors.New("503")}},
		Skipped: []controller.SetKey{set(5, 105)},
	})
	want := " rotated=2 rotated_sets=1/101,2/102 kept=1 kept_sets=3/103 failed=1 failed_sets=4/104 skipped=1 skipped_sets=5/105"
	if got != want {
		t.Fatalf("deaf rotation fields:\n got %q\nwant %q", got, want)
	}
	if got := formatDeafRotation(controller.SessionRotation{}); got != " rotated=0 kept=0 failed=0 skipped=0" {
		t.Fatalf("an empty rotation printed %q", got)
	}
	blocked := formatDeafRotation(controller.SessionRotation{Blocked: "close-intent", CloseIntent: []controller.SetKey{set(4, 104)}})
	if blocked != " rotated=0 kept=0 failed=0 skipped=0 rotation_blocked=close-intent close_intent=1 close_intent_sets=4/104" {
		t.Fatalf("a blocked rotation printed %q", blocked)
	}
}

// Zero adopts the serial loop, so a manifest without the flag behaves as it
// did before homelab#1190 item 2; linux accepts the bounded fan-out.
func TestCreateConcurrencyDefaultsToSerialAndAcceptsTheLinuxFanOut(t *testing.T) {
	base := RuntimeConfig{
		Platform: "linux", InstallationRepo: "cameronsjo/homelab", AppIDFile: "/run/app/id", AppKeyFile: "/run/app/key",
		StateDir: "/var/lib/runner-pool", Capacity: -1, LegacySlots: -1, BootstrapURL: "https://runner-pool.example:8443",
		BootstrapAddress: "127.0.0.1:8443", BootstrapCertificateFile: "/run/tls/cert", BootstrapKeyFile: "/run/tls/key",
		CycleInterval: time.Second,
	}
	got, err := base.Validate()
	if err != nil || got.CreateConcurrency != 1 {
		t.Fatalf("Validate() = %d, %v; want the serial default 1", got.CreateConcurrency, err)
	}
	base.CreateConcurrency = 4
	if got, err := base.Validate(); err != nil || got.CreateConcurrency != 4 {
		t.Fatalf("Validate() = %d, %v; want 4 accepted on linux", got.CreateConcurrency, err)
	}
}

// homelab#1213: an open provisioning breaker held admission at zero for fifteen
// minutes while the line read `ready=true reason=slots`. breaker_until= is
// unconditional, and an open breaker is the reason.
func TestTheCycleLineNamesTheProvisioningBreaker(t *testing.T) {
	until := time.Date(2026, 10, 1, 15, 5, 31, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		summary    controller.CycleSummary
		wantField  string
		wantReason string
	}{
		{"open", controller.CycleSummary{Ready: true, Binding: "slots", BreakerMeasured: true, BreakerOpen: true, BreakerUntil: until},
			" breaker_until=2026-10-01T15:05:31Z", "reason=breaker"},
		{"closed", controller.CycleSummary{Ready: true, Binding: "slots", BreakerMeasured: true},
			" breaker_until=none", "reason=slots"},
		{"open with an unparseable deadline", controller.CycleSummary{Ready: true, Binding: "slots", BreakerMeasured: true, BreakerOpen: true},
			" breaker_until=invalid", "reason=breaker"},
		{"not read this cycle", controller.CycleSummary{Binding: "slots"},
			" breaker_until=unknown", "reason=slots"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := formatCycleSummary(tc.summary)
			if !strings.Contains(line, tc.wantField) || !strings.Contains(line, " "+tc.wantReason+" ") {
				t.Fatalf("want %q and %q: %s", tc.wantField, tc.wantReason, line)
			}
			// It follows pollfail=, so every field an operator reads by position
			// stays where it was.
			if strings.Index(line, " pollfail=") > strings.Index(line, " breaker_until=") {
				t.Fatalf("breaker_until= was interleaved ahead of pollfail=: %s", line)
			}
		})
	}
}
