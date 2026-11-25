package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// drainingCycle reports one drained sizing change and counts how many times it
// is asked. A second call means the runtime kept ticking through a restart it
// had already been told to make.
type drainingCycle struct {
	calls atomic.Int32
}

func (d *drainingCycle) Cycle(context.Context) (controller.CycleResult, error) {
	d.calls.Add(1)
	return controller.CycleResult{RestartForSizing: true, Summary: controller.CycleSummary{
		Ready: true, SizingPending: true, PendingCapacity: 2, PendingLegacySlots: 0, Draining: true,
	}}, nil
}

// stoppingServer records that its context was cancelled, which is the only
// evidence the bootstrap server was actually shut down rather than abandoned.
type stoppingServer struct {
	started chan struct{}
	stopped chan struct{}
}

func (s *stoppingServer) Serve(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	close(s.stopped)
	return nil
}

// The process-level half of the drain: the store latches and the daemon reports
// RestartForSizing, but adoption only happens in the next process's Open, so the
// runtime has to actually stop. Nothing else in this package exercises that --
// replacing `return result.RestartForSizing` with `return false` leaves every
// other test in cmd/runner-pool green.
func TestRunStopsCleanlyForADrainedSizingChange(t *testing.T) {
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

	cycle := &drainingCycle{}
	server := &stoppingServer{started: make(chan struct{}), stopped: make(chan struct{})}
	interval := 5 * time.Millisecond
	runtime, err := newDaemonRuntime(cycle, server, interval, func(err error) { t.Errorf("unexpected cycle error: %v", err) })
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	// A context that is never cancelled: the runtime must stop on its own.
	go func() { done <- runtime.Run(context.Background()) }()
	<-server.started
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() returned %v, want nil -- a nonzero exit can leave a supervisor refusing to restart", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop for a drained sizing change")
	}

	select {
	case <-server.stopped:
	case <-time.After(time.Second):
		t.Fatal("the bootstrap server was never shut down")
	}

	// The immediate first cycle is the one that reported the drain. Anything
	// past it is a tick the runtime should never have taken.
	time.Sleep(10 * interval)
	if calls := cycle.calls.Load(); calls != 1 {
		t.Fatalf("the runtime ran %d cycles, want exactly 1", calls)
	}

	for _, want := range []string{
		"sizing change pending and journal idle; restarting to adopt",
		"pending_capacity=2", "pending_legacy_slots=0",
		// The draining cycle's own summary line: polled=0 advertised=none is the
		// silent-outage signature, so it has to be marked.
		"draining=true",
	} {
		if !strings.Contains(logged.String(), want) {
			t.Fatalf("the runtime log is missing %q: %s", want, logged.String())
		}
	}
}
