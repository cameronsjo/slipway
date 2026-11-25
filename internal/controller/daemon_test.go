package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

type cycleRegistry struct {
	calls *[]string
	fail  string
}

func (r cycleRegistry) Refresh(context.Context) error {
	*r.calls = append(*r.calls, "refresh")
	if r.fail == "refresh" {
		return errors.New("refresh failed")
	}
	return nil
}
func (r cycleRegistry) Reconcile(context.Context) error {
	*r.calls = append(*r.calls, "manage")
	if r.fail == "manage" {
		return errors.New("management failed")
	}
	return nil
}

type cycleSessions struct {
	calls          *[]string
	fail           string
	polls          int
	deferred       int
	closeIdleCount int
	degraded       []SessionDegradation
	rotation       SessionRotation
}

func (s cycleSessions) Sync(context.Context) error {
	*s.calls = append(*s.calls, "sessions")
	if s.fail == "sessions" {
		return errors.New("session sync failed")
	}
	return nil
}
func (s cycleSessions) PollSweep(context.Context) ([]PollResult, int, error) {
	*s.calls = append(*s.calls, "poll")
	results := make([]PollResult, s.polls)
	if s.fail == "poll" {
		return results, s.deferred, errors.New("poll failed")
	}
	if s.fail == "set-poll" {
		return results, s.deferred, errors.Join(&SetPollError{Set: SetKey{RepositoryID: 7, ScaleSetID: 70}, Err: errors.New("503 from the queue")})
	}
	return results, s.deferred, nil
}
func (s cycleSessions) Degraded() []SessionDegradation { return s.degraded }
func (s cycleSessions) CloseIdleConnections() int {
	*s.calls = append(*s.calls, "close-idle")
	return s.closeIdleCount
}
func (s cycleSessions) RotateIdleSessions(context.Context) SessionRotation {
	*s.calls = append(*s.calls, "rotate")
	return s.rotation
}

type cycleLifecycle struct {
	calls      *[]string
	fail       string
	provisions int
	unbounded  bool
	retained   int
}

func (l *cycleLifecycle) ReconcileInventory(context.Context) error {
	*l.calls = append(*l.calls, "inventory")
	if l.fail == "inventory" {
		return errors.New("inventory failed")
	}
	return nil
}
func (l *cycleLifecycle) Maintain(context.Context) (int, error) {
	*l.calls = append(*l.calls, "maintain")
	if l.fail == "maintain" {
		return l.retained, errors.New("maintenance failed")
	}
	return l.retained, nil
}
func (l *cycleLifecycle) MaintainWorkers(context.Context) (int, error) {
	*l.calls = append(*l.calls, "maintain-workers")
	return 0, nil
}
func (l *cycleLifecycle) ProvisionNext(context.Context) (Lease, error) {
	*l.calls = append(*l.calls, "provision")
	l.provisions++
	if l.fail == "provision" {
		return Lease{}, errors.New("provision failed")
	}
	if !l.unbounded && l.provisions > 2 {
		return Lease{}, ErrNoDemand
	}
	return Lease{ID: "lease"}, nil
}

func TestDaemonCycleOrdersBoundariesAndStopsAtDemand(t *testing.T) {
	var calls []string
	registry := cycleRegistry{calls: &calls}
	sessions := cycleSessions{calls: &calls, polls: 2}
	lifecycle := &cycleLifecycle{calls: &calls}
	daemon, err := newDaemon(registry, sessions, lifecycle, 6)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"refresh", "manage", "inventory", "maintain", "sessions", "poll", "provision", "provision", "provision"}
	if !reflect.DeepEqual(calls, want) || result.Polled != 2 || len(result.Provisioned) != 2 {
		t.Fatalf("cycle result=%+v calls=%v", result, calls)
	}
}

func TestDaemonCycleFailsClosedAtEachBoundary(t *testing.T) {
	for _, failure := range []string{"refresh", "manage", "inventory", "maintain", "sessions", "poll", "provision"} {
		t.Run(failure, func(t *testing.T) {
			var calls []string
			registry := cycleRegistry{calls: &calls, fail: failure}
			sessions := cycleSessions{calls: &calls, fail: failure, polls: 2}
			lifecycle := &cycleLifecycle{calls: &calls, fail: failure}
			daemon, err := newDaemon(registry, sessions, lifecycle, 6)
			if err != nil {
				t.Fatal(err)
			}
			result, err := daemon.Cycle(ctx)
			if err == nil {
				t.Fatal("failed boundary was accepted")
			}
			if failure != "manage" && failure != "poll" && failure != "provision" && result.Polled != 0 {
				t.Fatalf("polling crossed failed %s boundary", failure)
			}
			if (failure == "manage" || failure == "poll" || failure == "provision") && result.Polled != 2 {
				t.Fatal("partial poll diagnostics were lost")
			}
			if failure != "manage" && failure != "provision" && lifecycle.provisions != 0 {
				t.Fatalf("acquisition crossed failed %s boundary", failure)
			}
			if failure == "manage" && lifecycle.provisions != 3 {
				t.Fatal("one repository management failure stopped healthy repositories")
			}
		})
	}
}

// homelab#924: Maintain's retained-reservation count reaches the cycle summary
// on every path that ran Maintain -- a completed cycle, and one that failed at
// a later boundary -- and is marked unmeasured on a path that never reached it.
func TestDaemonCycleCarriesTheRetainedReservationCount(t *testing.T) {
	for _, tc := range []struct {
		failure      string
		wantMeasured bool
	}{
		{"", true},
		{"sessions", true},
		{"poll", true},
		{"inventory", false},
		{"maintain", false},
	} {
		t.Run("fail="+tc.failure, func(t *testing.T) {
			var calls []string
			lifecycle := &cycleLifecycle{calls: &calls, fail: tc.failure, retained: 2}
			daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, fail: tc.failure, polls: 1}, lifecycle, 1)
			if err != nil {
				t.Fatal(err)
			}
			result, _ := daemon.Cycle(ctx)
			if result.Summary.RetainedMeasured != tc.wantMeasured {
				t.Fatalf("RetainedMeasured=%t, want %t", result.Summary.RetainedMeasured, tc.wantMeasured)
			}
			if tc.wantMeasured && result.Summary.Retained != 2 {
				t.Fatalf("the cycle summary lost Maintain's retained count: %d", result.Summary.Retained)
			}
		})
	}
}

func TestDaemonCycleNeverExceedsPlatformBound(t *testing.T) {
	var calls []string
	registry := cycleRegistry{calls: &calls}
	sessions := cycleSessions{calls: &calls}
	lifecycle := &cycleLifecycle{calls: &calls, unbounded: true}
	daemon, err := newDaemon(registry, sessions, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Provisioned) != 2 {
		t.Fatalf("cycle admitted %d workers", len(result.Provisioned))
	}
}

// The silence that cost a night. A cycle in which every scale set is offered
// zero capacity is a cycle in which GitHub can never dispatch a job, and it was
// indistinguishable from a healthy one: no error, no log, no journal row.
type advertisingSessions struct {
	calls    *[]string
	plan     []PollResult
	deferred int
}

func (s advertisingSessions) Sync(context.Context) error {
	*s.calls = append(*s.calls, "sessions")
	return nil
}

func (s advertisingSessions) PollSweep(context.Context) ([]PollResult, int, error) {
	*s.calls = append(*s.calls, "poll")
	return s.plan, s.deferred, nil
}

func (s advertisingSessions) Degraded() []SessionDegradation { return nil }
func (s advertisingSessions) CloseIdleConnections() int {
	*s.calls = append(*s.calls, "close-idle")
	return 0
}
func (s advertisingSessions) RotateIdleSessions(context.Context) SessionRotation {
	*s.calls = append(*s.calls, "rotate")
	return SessionRotation{}
}

func TestACycleReportsWhatItAdvertisedToEachScaleSet(t *testing.T) {
	var calls []string
	sessions := advertisingSessions{calls: &calls, deferred: 2, plan: []PollResult{
		{Set: SetKey{RepositoryID: 11, ScaleSetID: 4}, MaxCapacity: 0},
		{Set: SetKey{RepositoryID: 12, ScaleSetID: 5}, MaxCapacity: 1,
			Envelope: &Envelope{MessageID: 7}},
	}}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, sessions, &cycleLifecycle{calls: &calls}, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Summary.Advertised) != 2 {
		t.Fatalf("the summary lost a scale set: %+v", result.Summary.Advertised)
	}
	if result.Summary.Advertised[0].MaxCapacity != 0 || result.Summary.Advertised[1].MaxCapacity != 1 {
		t.Fatalf("the advertised capacities are not the polled ones: %+v", result.Summary.Advertised)
	}
	if result.Summary.Advertised[0].Set != (SetKey{RepositoryID: 11, ScaleSetID: 4}) {
		t.Fatalf("the summary lost which set got which capacity: %+v", result.Summary.Advertised)
	}
	if result.Summary.Messages != 1 {
		t.Fatalf("message count is %d, expected 1", result.Summary.Messages)
	}
	// Demand the sweep could not advertise. Without it the cycle line reads the
	// same whether jobs are waiting for a slot or none exist.
	if result.Summary.Deferred != 2 {
		t.Fatalf("deferred demand is %d, expected 2", result.Summary.Deferred)
	}
	// No store on this daemon, so the counts it cannot read say so rather than
	// reporting a confident zero -- a zero here would read exactly like the
	// outage it is meant to reveal.
	if result.Summary.Charged != -1 {
		t.Fatalf("an unreadable charged count was reported as %d", result.Summary.Charged)
	}
}

func TestTheSummarySurvivesAFailedCycle(t *testing.T) {
	var calls []string
	sessions := cycleSessions{calls: &calls, polls: 3, fail: "poll"}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, sessions, &cycleLifecycle{calls: &calls}, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(context.Background())
	if err == nil {
		t.Fatal("a failed poll sweep was accepted")
	}
	// A cycle that fails after PollSweep still advertised something, and that
	// is the number worth having beside the error.
	if len(result.Summary.Advertised) != 3 {
		t.Fatalf("the summary was dropped on the failure path: %+v", result.Summary)
	}
}

// TestCycleFiresTheCloseConnectionsRungOncePerSilentEpisode is the daemon-level
// wiring test for the non-destructive-first rung: half the deaf window with
// no active worker closes idle connections exactly once, a still-silent
// process does not repeat it, and a fresh envelope lets the next silent
// episode fire it again.
func TestCycleFiresTheCloseConnectionsRungOncePerSilentEpisode(t *testing.T) {
	store, _ := openTestStore(t, 2)
	var calls []string
	sessions := cycleSessions{calls: &calls, closeIdleCount: 3}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, sessions, &cycleLifecycle{calls: &calls}, 0)
	if err != nil {
		t.Fatal(err)
	}
	daemon.store = store
	daemon.deafAfter = 30 * time.Minute

	setLiveness := func(started, lastEnvelope time.Time) {
		store.mu.Lock()
		store.started = started
		store.lastEnvelope = lastEnvelope
		store.mu.Unlock()
	}

	// Half the window, no worker running: the rung fires.
	setLiveness(time.Now().Add(-15*time.Minute), time.Time{})
	calls = nil
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ConnectionsClosed != 3 || !slices.Contains(calls, "close-idle") {
		t.Fatalf("the rung did not close idle connections: closed=%d calls=%v", result.ConnectionsClosed, calls)
	}
	if result.SessionsDeaf {
		t.Fatalf("half the window was treated as the full deaf window: %+v", result.Summary)
	}

	// Still silent, same episode: must not fire again.
	calls = nil
	result, err = daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ConnectionsClosed != 0 || slices.Contains(calls, "close-idle") {
		t.Fatalf("the rung fired twice in the same silent episode: closed=%d calls=%v", result.ConnectionsClosed, calls)
	}

	// A fresh envelope ends the episode; the next silence starts a new one.
	setLiveness(time.Now().Add(-2*time.Hour), time.Now().Add(-15*time.Minute))
	calls = nil
	result, err = daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ConnectionsClosed != 3 || !slices.Contains(calls, "close-idle") {
		t.Fatalf("a fresh envelope did not let the rung fire again: closed=%d calls=%v", result.ConnectionsClosed, calls)
	}
}

// IMPORTANT 2: the unknown-vs-zero arm, entered rather than assumed. A fenced
// controller has measured no dimension at all, and that must not read as "the
// host is fine and nothing is short". `free=-1 reason=unknown` and
// `free=0 reason=memory` are different problems.
func TestAFencedControllerReportsUnknownRatherThanZeroFreeSlots(t *testing.T) {
	store, _ := openTestStore(t, 2)
	// A full but READABLE host first, so the summary has something real to lose.
	if err := store.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 0, Binding: "memory"}); err != nil {
		t.Fatal(err)
	}
	var calls []string
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls}, &cycleLifecycle{calls: &calls}, 2)
	if err != nil {
		t.Fatal(err)
	}
	daemon.store = store
	full, err := daemon.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if full.Summary.FreeSlots != 0 || full.Summary.Binding != "memory" {
		t.Fatalf("a full host did not report its shortage: free=%d reason=%q",
			full.Summary.FreeSlots, full.Summary.Binding)
	}

	// Now fence it: the adapter could not read the host at all.
	_ = store.ReconcileBackendInventory(ctx, BackendInventory{Complete: false})
	fenced, err := daemon.Cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fenced.Summary.FreeSlots != -1 {
		t.Fatalf("an unmeasured host reported %d free slots; -1 is the only honest answer",
			fenced.Summary.FreeSlots)
	}
	if fenced.Summary.Binding != "unknown" {
		t.Fatalf("an unmeasured host named a dimension it never measured: %q", fenced.Summary.Binding)
	}
}

// homelab#1096 (review D13): a discovery failure still runs the part of the
// cycle that needs no GitHub call -- the inventory and the worker half of
// Maintain -- so a finished worker is torn down during a GitHub blip. Nothing
// that calls GitHub runs, and the cycle still fails with the discovery error.
func TestADiscoveryFailureStillTearsDownWorkers(t *testing.T) {
	var calls []string
	lifecycle := &cycleLifecycle{calls: &calls}
	daemon, err := newDaemon(cycleRegistry{calls: &calls, fail: "refresh"}, cycleSessions{calls: &calls, polls: 1}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	if err == nil || err.Error() != "refresh failed" {
		t.Fatalf("the discovery failure was not reported as the cycle's error: %v", err)
	}
	if want := []string{"refresh", "inventory", "maintain-workers"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%v, want %v", calls, want)
	}
	if !result.Summary.RetainedMeasured || lifecycle.provisions != 0 || result.Polled != 0 {
		t.Fatalf("result=%+v provisions=%d", result, lifecycle.provisions)
	}
}

// A failed inventory in that path is reported beside the discovery error, and
// the worker half does not run on an untrusted inventory.
func TestADiscoveryFailureWithAFailedInventoryReportsBoth(t *testing.T) {
	var calls []string
	lifecycle := &cycleLifecycle{calls: &calls, fail: "inventory"}
	daemon, err := newDaemon(cycleRegistry{calls: &calls, fail: "refresh"}, cycleSessions{calls: &calls}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	if err == nil || !strings.Contains(err.Error(), "refresh failed") || !strings.Contains(err.Error(), "inventory failed") {
		t.Fatalf("both failures were not reported: %v", err)
	}
	if want := []string{"refresh", "inventory"}; !reflect.DeepEqual(calls, want) || result.Summary.RetainedMeasured {
		t.Fatalf("calls=%v measured=%t", calls, result.Summary.RetainedMeasured)
	}
}
