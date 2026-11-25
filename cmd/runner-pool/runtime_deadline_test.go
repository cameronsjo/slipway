package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// hangingCycle blocks until its context ends, the way a Cycle stuck in a hung
// Kubernetes or witness call would, and records how each call ended.
type hangingCycle struct {
	ended chan error
}

func (h *hangingCycle) Cycle(ctx context.Context) (controller.CycleResult, error) {
	<-ctx.Done()
	h.ended <- ctx.Err()
	return controller.CycleResult{}, ctx.Err()
}

// Review R3 (D8): a hung cycle used to stop the pool silently, because Cycle had
// no deadline and the deaf check runs inside it. With a deadline the hung call
// ends with a deadline error, which is observed like any failed cycle, and the
// next tick starts a fresh one.
func TestAHungCycleEndsAtItsDeadlineAndTheNextCycleRuns(t *testing.T) {
	cycle := &hangingCycle{ended: make(chan error, 8)}
	server := &fakeRuntimeServer{started: make(chan struct{})}
	observed := make(chan error, 8)
	runtime, err := newDaemonRuntime(cycle, server, 5*time.Millisecond, func(err error) { observed <- err })
	if err != nil {
		t.Fatal(err)
	}
	runtime.deadline = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	for i := range 2 {
		select {
		case err := <-cycle.ended:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cycle %d ended with %v, want the deadline", i+1, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("cycle %d never reached its deadline: the pool would hang", i+1)
		}
		select {
		case err := <-observed:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("observed %v, want the deadline error", err)
			}
		case <-time.After(time.Second):
			t.Fatal("the deadline error was not observed")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() after cancellation error = %v", err)
	}
}

func TestTheCycleDeadlineIsHalfTheDeafWindowAndLongerThanAnInterval(t *testing.T) {
	if got := cycleDeadline(defaultSessionDeafAfter); got != 15*time.Minute {
		t.Fatalf("cycle deadline = %s, want 15m for the default 30m deaf window", got)
	}
	if cycleDeadline(defaultSessionDeafAfter) <= defaultDaemonInterval {
		t.Fatal("the default cycle deadline does not outlast one cycle interval")
	}
}

// Review of #1000, item 2: the cycle line prints the degraded and poll-failed
// counts on every line, zero included, and names the sets when there are any,
// bounded so a pool-wide failure cannot flood one line.
func TestTheCycleLineNamesDegradedAndPollFailedSets(t *testing.T) {
	quiet := formatCycleSummary(controller.CycleSummary{Ready: true})
	if !strings.Contains(quiet, " degraded=0 pollfail=0") || strings.Contains(quiet, "_sets=") {
		t.Fatalf("a quiet cycle line: %q", quiet)
	}
	var many []controller.SetKey
	for n := 1; n <= 7; n++ {
		many = append(many, controller.SetKey{RepositoryID: int64(n), ScaleSetID: 100 + n})
	}
	line := formatCycleSummary(controller.CycleSummary{Ready: true,
		DegradedSessions: 1, DegradedSets: []controller.SetKey{{RepositoryID: 9, ScaleSetID: 90}},
		PollFailures: len(many), PollFailedSets: many})
	for _, want := range []string{
		" degraded=1 pollfail=7",
		" degraded_sets=9/90",
		" pollfail_sets=1/101,2/102,3/103,4/104,5/105,+2",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("cycle line %q lacks %q", line, want)
		}
	}
}
