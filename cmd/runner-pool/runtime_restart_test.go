package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// clockedCycle moves a fake clock one cycle interval forward per call, so the
// stalled-cycle backstop can be driven through its window without waiting for
// it. It returns result and err on every call, unless script is set, which
// picks the outcome of call n (1-based). Run calls Cycle and the clock on one
// goroutine, and the test reads calls only after Run returns.
type clockedCycle struct {
	calls  int
	at     time.Time
	step   time.Duration
	result controller.CycleResult
	err    error
	script func(n int) (controller.CycleResult, error)
}

func (c *clockedCycle) Cycle(context.Context) (controller.CycleResult, error) {
	c.calls++
	c.at = c.at.Add(c.step)
	if c.script != nil {
		return c.script(c.calls)
	}
	return c.result, c.err
}

func (c *clockedCycle) now() time.Time { return c.at }

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logged bytes.Buffer
	previousFlags := log.Flags()
	log.SetOutput(&logged)
	log.SetFlags(0)
	// os.Stderr, not nil: the std logger is process-global, and a nil writer
	// panics the next test in this package that logs.
	t.Cleanup(func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(previousFlags)
	})
	return &logged
}

// runFor runs the runtime until it stops on its own or `within` passes, then
// cancels it. It reports Run's error and whether the runtime stopped on its own.
func runFor(t *testing.T, runtime *daemonRuntime, within time.Duration) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case err := <-done:
		return true, err
	case <-time.After(within):
	}
	cancel()
	select {
	case err := <-done:
		return false, err
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop after cancellation")
		return false, nil
	}
}

// homelab#1211: the controller sat on `process session handle differs from
// durable identity; restart and reconcile` for ten hours, logging it 599 times.
// A restart-required error now ends the process on the cycle that returns it,
// with the deaf exit's code, and an ordinary error still does not.
func TestARestartRequiredCycleErrorExitsAndATransientOneDoesNot(t *testing.T) {
	restart := errors.Join(errors.New("management failed"),
		fmt.Errorf("process session handle differs from durable identity; restart and reconcile: %w", controller.ErrRestartRequired))
	tests := []struct {
		name     string
		err      error
		wantExit bool
	}{
		{"restart required", restart, true},
		{"transient", errors.New("session sync failed"), false},
		{"cycle deadline", context.DeadlineExceeded, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logged := captureLog(t)
			cycle := &clockedCycle{at: time.Unix(0, 0), step: time.Minute, err: tc.err}
			server := &fakeRuntimeServer{started: make(chan struct{})}
			runtime, err := newDaemonRuntime(cycle, server, time.Millisecond, func(error) {})
			if err != nil {
				t.Fatal(err)
			}
			runtime.now = cycle.now
			// The backstop is on at its production default; a transient error
			// with no known demand must still not trip it.
			runtime.failedCycleRestartAfter = defaultFailedCycleRestartAfter
			stopped, runErr := runFor(t, runtime, 200*time.Millisecond)
			if stopped != tc.wantExit {
				t.Fatalf("stopped on its own = %t after %d cycles (err %v), want %t", stopped, cycle.calls, runErr, tc.wantExit)
			}
			if !tc.wantExit {
				if runErr != nil || cycle.calls < 3 {
					t.Fatalf("Run() = %v after %d cycles, want nil after several", runErr, cycle.calls)
				}
				return
			}
			if !errors.Is(runErr, errRestartRequired) || !exitsForRestart(runErr) {
				t.Fatalf("Run() = %v, want errRestartRequired, which main maps to exit %d", runErr, exitSessionsDeaf)
			}
			if cycle.calls != 1 {
				t.Fatalf("ran %d cycles, want exactly the one that returned the error", cycle.calls)
			}
			if line := logged.String(); !strings.Contains(line, "Session state needs a process restart") ||
				!strings.Contains(line, "differs from durable identity") {
				t.Fatalf("the exit line does not name the reason and the error: %q", line)
			}
		})
	}
}

// homelab#1211 backstop: cycles that keep failing in the session stage never
// reach the deaf check. After the window, with a fresh REST reading showing
// queued pool jobs, the process exits the same way. Without known demand, or
// for a failure a restart does not cure, it keeps running.
func TestStalledCyclesWithQueuedDemandExitAfterTheWindow(t *testing.T) {
	demand := controller.CycleSummary{DemandKnown: true, QueuedDemand: 2}
	stalled := controller.CycleResult{SessionStageFailed: true, Summary: demand}
	tests := []struct {
		name     string
		result   controller.CycleResult
		wantExit bool
	}{
		{"session stage failing with queued jobs", stalled, true},
		{"session stage failing with no fresh demand reading", controller.CycleResult{SessionStageFailed: true, Summary: controller.CycleSummary{QueuedDemand: 2}}, false},
		{"session stage failing on an idle pool", controller.CycleResult{SessionStageFailed: true, Summary: controller.CycleSummary{DemandKnown: true}}, false},
		// A degraded set is named in the error on every cycle while the rest
		// poll; that is not a stall.
		{"failing after polling some sets", controller.CycleResult{Polled: 3, Summary: demand}, false},
		// Every set degraded and waiting on an operator: the sweep runs over
		// zero sets, so nothing polls, but a restart cannot clear it.
		{"all sets degraded, nothing polled", controller.CycleResult{Polled: 0, Summary: demand}, false},
		// Discovery, inventory and Maintain failures degrade on purpose and keep
		// local teardown running (homelab#1096); the daemon does not flag them.
		{"discovery or inventory failing with queued jobs", controller.CycleResult{Summary: demand}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logged := captureLog(t)
			cycle := &clockedCycle{at: time.Unix(0, 0), step: time.Minute, result: tc.result, err: errors.New("session manager journal unreadable")}
			server := &fakeRuntimeServer{started: make(chan struct{})}
			runtime, err := newDaemonRuntime(cycle, server, time.Millisecond, func(error) {})
			if err != nil {
				t.Fatal(err)
			}
			runtime.now = cycle.now
			runtime.failedCycleRestartAfter = 10 * time.Minute
			stopped, runErr := runFor(t, runtime, 300*time.Millisecond)
			if stopped != tc.wantExit {
				t.Fatalf("stopped on its own = %t after %d cycles (err %v), want %t", stopped, cycle.calls, runErr, tc.wantExit)
			}
			if !tc.wantExit {
				if runErr != nil || cycle.calls <= 11 {
					t.Fatalf("Run() = %v after %d cycles, want nil after more than the window's 11", runErr, cycle.calls)
				}
				return
			}
			if !errors.Is(runErr, errCyclesStalled) || !exitsForRestart(runErr) {
				t.Fatalf("Run() = %v, want errCyclesStalled, which main maps to exit %d", runErr, exitSessionsDeaf)
			}
			// The run starts at the first failure; ten one-minute steps later
			// is the eleventh cycle.
			if cycle.calls != 11 {
				t.Fatalf("exited after %d cycles, want 11", cycle.calls)
			}
			line := logged.String()
			for _, want := range []string{"Cycles have failed in the session stage for 10m0s", "consecutive_failed_cycles=11", "queued_pool_jobs=2", "journal unreadable"} {
				if !strings.Contains(line, want) {
					t.Fatalf("the exit line lacks %q: %q", want, line)
				}
			}
		})
	}
}

// One cycle that gets through the session stage ends the run, so the window
// counts again from the next failure. Five failures, one good cycle, then a
// second stretch: the exit comes ten minutes into the second stretch, at call
// 17, not at call 11 as one unbroken run would give.
func TestTheStallWindowRestartsAfterAGoodCycle(t *testing.T) {
	logged := captureLog(t)
	failing := controller.CycleResult{SessionStageFailed: true, Summary: controller.CycleSummary{DemandKnown: true, QueuedDemand: 1}}
	cycle := &clockedCycle{at: time.Unix(0, 0), step: time.Minute, script: func(n int) (controller.CycleResult, error) {
		if n == 6 {
			return controller.CycleResult{Polled: 2}, nil
		}
		return failing, errors.New("session sync failed")
	}}
	server := &fakeRuntimeServer{started: make(chan struct{})}
	runtime, err := newDaemonRuntime(cycle, server, time.Millisecond, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	runtime.now = cycle.now
	runtime.failedCycleRestartAfter = 10 * time.Minute
	stopped, runErr := runFor(t, runtime, 2*time.Second)
	if !stopped || !errors.Is(runErr, errCyclesStalled) {
		t.Fatalf("stopped=%t Run()=%v after %d cycles, want errCyclesStalled", stopped, runErr, cycle.calls)
	}
	if cycle.calls != 17 {
		t.Fatalf("exited after %d cycles, want 17: the window must count from the second stretch", cycle.calls)
	}
	if line := logged.String(); !strings.Contains(line, "consecutive_failed_cycles=11") {
		t.Fatalf("the exit line does not count only the second stretch: %q", line)
	}
}

func TestExitsForRestartCoversEveryDeliberateRestart(t *testing.T) {
	shutdown := errors.New("bootstrap server shutdown: closed")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"deaf sessions", errSessionsDeaf, true},
		{"restart required", errRestartRequired, true},
		{"stalled cycles", errCyclesStalled, true},
		{"restart required beside a server shutdown error", errors.Join(errRestartRequired, shutdown), true},
		{"server failure", shutdown, false},
		{"the controller sentinel itself, never returned by Run", controller.ErrRestartRequired, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitsForRestart(tc.err); got != tc.want {
				t.Fatalf("exitsForRestart(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestFailedCycleRestartAfterDefaultsAndMustOutlastAnInterval(t *testing.T) {
	base := RuntimeConfig{
		Platform: "linux", InstallationRepo: "cameronsjo/homelab", AppIDFile: "/run/app/id", AppKeyFile: "/run/app/key",
		StateDir: "/var/lib/runner-pool", Capacity: -1, LegacySlots: -1,
		BootstrapURL: "https://runner-pool.example:8443", BootstrapAddress: "127.0.0.1:8443",
		BootstrapCertificateFile: "/run/tls/cert", BootstrapKeyFile: "/run/tls/key",
	}
	config, err := base.Validate()
	if err != nil {
		t.Fatal(err)
	}
	if config.FailedCycleRestartAfter != defaultFailedCycleRestartAfter {
		t.Fatalf("default = %s, want %s", config.FailedCycleRestartAfter, defaultFailedCycleRestartAfter)
	}
	for _, window := range []time.Duration{-time.Minute, time.Minute, 30 * time.Second} {
		edited := base
		edited.FailedCycleRestartAfter = window
		if _, err := edited.Validate(); err == nil || !strings.Contains(err.Error(), "failed-cycle-restart-after") {
			t.Fatalf("window %s: Validate() = %v, want a failed-cycle-restart-after refusal", window, err)
		}
	}
}
