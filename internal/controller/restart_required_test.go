package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// homelab#1211: the two session-manager states only a new process can clear
// carry ErrRestartRequired, through every wrapper the cycle adds on the way to
// the daemon loop, and nothing ordinary does. The two real cases are produced
// by the manager itself, not built by hand, so a message that loses its %w
// goes red here rather than in production.
func TestSessionStateOnlyARestartClearsIsClassified(t *testing.T) {
	fencedManager := func(t *testing.T) *SessionManager {
		t.Helper()
		s, _ := openTestStore(t, 1)
		key := prepare(t, s, 1)[0]
		markManaged(t, s, []SetKey{key})
		manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: newSDKFixture(t, s, key)})
		if err := manager.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		// A live handle that no longer matches its durable record: the shape
		// the 2026-10-01 controller sat in for ten hours.
		manager.sessions[key].closed = true
		return manager
	}
	syncErr := func(t *testing.T) error { return fencedManager(t).Sync(ctx) }
	sweepErr := func(t *testing.T) error {
		_, _, err := fencedManager(t).PollSweep(ctx)
		return err
	}
	acquireErr := func(t *testing.T) error {
		_, err := fencedManager(t).AcquireNext(ctx)
		return err
	}
	tests := []struct {
		name    string
		err     func(*testing.T) error
		message string
		restart bool
	}{
		{"sync over a handle that differs from its durable identity", syncErr, "differs from durable identity", true},
		{"poll sweep over a fenced handle", sweepErr, "missing or fenced handle", true},
		{"acquisition over a fenced handle", acquireErr, "missing or fenced handle", true},
		{"joined beside a management error, as Cycle returns it", func(t *testing.T) error {
			return errors.Join(errors.New("management failed"), syncErr(t))
		}, "differs from durable identity", true},
		{"wrapped once more", func(t *testing.T) error {
			return fmt.Errorf("cycle: %w", sweepErr(t))
		}, "missing or fenced handle", true},
		{"a transient session sync failure", func(*testing.T) error { return errors.New("session sync failed") }, "session sync failed", false},
		{"a cycle deadline", func(*testing.T) error { return context.DeadlineExceeded }, "deadline", false},
		{"stale discovery", func(*testing.T) error { return ErrDiscoveryStale }, "discovery", false},
		{"one set's failed poll", func(*testing.T) error {
			return errors.Join(&SetPollError{Set: SetKey{RepositoryID: 7, ScaleSetID: 70}, Err: errors.New("503")})
		}, "poll failed", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.err(t)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error = %v, want one naming %q", err, tc.message)
			}
			if got := errors.Is(err, ErrRestartRequired); got != tc.restart {
				t.Fatalf("errors.Is(%v, ErrRestartRequired) = %t, want %t", err, got, tc.restart)
			}
		})
	}
}

// homelab#1211: a cycle that fails before summarize still carries the REST
// demand signal, which is what lets the daemon loop's backstop tell a stalled
// pool with queued jobs from an idle one. On 2026-10-01 every failed cycle's
// summary was the zero value, so demand read as unknown.
func TestACycleFailedBeforePollStillReportsRESTDemand(t *testing.T) {
	store, _ := openTestStore(t, 1)
	now := time.Now().UTC()
	store.RecordRESTDemand(map[int64]int{1: 3}, now, now.Add(10*time.Minute))
	var calls []string
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, fail: "sessions"}, &cycleLifecycle{calls: &calls}, 1)
	if err != nil {
		t.Fatal(err)
	}
	daemon.store = store
	result, err := daemon.Cycle(ctx)
	if err == nil {
		t.Fatal("the session sync failure was not returned")
	}
	if result.Polled != 0 || result.Summary.LastEnvelopeMeasured {
		t.Fatalf("the cycle reached the poll: %+v", result)
	}
	if !result.Summary.DemandKnown || result.Summary.QueuedDemand != 3 {
		t.Fatalf("demand_known=%t queued=%d, want true and 3", result.Summary.DemandKnown, result.Summary.QueuedDemand)
	}
}

// homelab#1211 review: the stalled-cycle backstop counts only a cycle that
// stopped in the session stage. Discovery, inventory and Maintain failures
// degrade on purpose (homelab#1096), and a pool whose every set is degraded
// sweeps zero sets without failing; a restart cures none of them.
func TestOnlyASessionStageFailureIsFlaggedForTheBackstop(t *testing.T) {
	degraded := []SessionDegradation{{Set: SetKey{RepositoryID: 1, ScaleSetID: 10}, Err: errors.New("create-intent waits on an operator")}}
	tests := []struct {
		name      string
		registry  string
		sessions  cycleSessions
		lifecycle string
		want      bool
	}{
		{"global session sync failure", "", cycleSessions{fail: "sessions"}, "", true},
		{"whole-sweep poll failure", "", cycleSessions{fail: "poll"}, "", true},
		{"discovery failure", "refresh", cycleSessions{}, "", false},
		{"inventory failure", "", cycleSessions{}, "inventory", false},
		{"maintenance failure", "", cycleSessions{}, "maintain", false},
		{"one set's failed poll", "", cycleSessions{fail: "set-poll", polls: 1}, "", false},
		{"every set degraded, zero sets polled", "", cycleSessions{degraded: degraded}, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			sessions := tc.sessions
			sessions.calls = &calls
			daemon, err := newDaemon(cycleRegistry{calls: &calls, fail: tc.registry}, sessions, &cycleLifecycle{calls: &calls, fail: tc.lifecycle}, 1)
			if err != nil {
				t.Fatal(err)
			}
			result, err := daemon.Cycle(ctx)
			if err == nil {
				t.Fatalf("the cycle returned no error; calls=%v", calls)
			}
			if result.SessionStageFailed != tc.want {
				t.Fatalf("SessionStageFailed = %t, want %t (err %v, polled %d)", result.SessionStageFailed, tc.want, err, result.Polled)
			}
		})
	}
}
