package controller

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

// completingSessions is cycleSessions whose sweep delivers one JobCompleted,
// the message that moves a lease to cleanup-pending with its worker present.
type completingSessions struct{ cycleSessions }

func (s completingSessions) PollSweep(ctx context.Context) ([]PollResult, int, error) {
	polls, deferred, err := s.cycleSessions.PollSweep(ctx)
	polls = append(polls, PollResult{Envelope: &Envelope{Events: []Event{
		{JobMessageBase: scaleset.JobMessageBase{JobMessageType: scaleset.JobMessageType{MessageType: scaleset.MessageTypeJobCompleted}}},
	}}})
	return polls, deferred, err
}

// passLifecycle answers each maintenance call in order, Maintain or
// MaintainWorkers: retainedByPass[i] on call i+1, and an error on call failOn
// (1-based; 0 never fails).
type passLifecycle struct {
	cycleLifecycle
	maintainCalls  int
	retainedByPass []int
	failOn         int
}

func (l *passLifecycle) Maintain(context.Context) (int, error) {
	return l.pass("maintain")
}

func (l *passLifecycle) MaintainWorkers(context.Context) (int, error) {
	return l.pass("maintain-workers")
}

func (l *passLifecycle) pass(name string) (int, error) {
	*l.calls = append(*l.calls, name)
	l.maintainCalls++
	retained := 0
	if l.maintainCalls <= len(l.retainedByPass) {
		retained = l.retainedByPass[l.maintainCalls-1]
	}
	if l.maintainCalls == l.failOn {
		return retained, errors.New("maintenance failed")
	}
	return retained, nil
}

// homelab#951: a completion read in this cycle's PollSweep is maintained in the
// same cycle, before any provisioning, and the two passes' retained counts add.
func TestACompletionSeenByPollIsMaintainedInTheSameCycle(t *testing.T) {
	var calls []string
	lifecycle := &passLifecycle{cycleLifecycle: cycleLifecycle{calls: &calls}, retainedByPass: []int{1, 2}}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, completingSessions{cycleSessions{calls: &calls}}, lifecycle, 6)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"refresh", "manage", "inventory", "maintain", "sessions", "poll", "maintain-workers", "provision", "provision", "provision"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%v, want %v", calls, want)
	}
	if !result.Summary.RetainedMeasured || result.Summary.Retained != 3 {
		t.Fatalf("retained=%d measured=%t, want 3 (1 + 2; the passes never cover one worker)",
			result.Summary.Retained, result.Summary.RetainedMeasured)
	}
}

// A quiet sweep carries no completion, so Maintain runs once: the second pass
// also retries stuck registration cleanups against GitHub, which must not
// double on every idle cycle.
func TestAQuietSweepDoesNotRunTheSecondPass(t *testing.T) {
	var calls []string
	lifecycle := &passLifecycle{cycleLifecycle: cycleLifecycle{calls: &calls}}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, polls: 3}, lifecycle, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	if lifecycle.maintainCalls != 1 {
		t.Fatalf("Maintain ran %d times on a sweep with no completion: %v", lifecycle.maintainCalls, calls)
	}
}

// The second pass fails closed exactly like the first: no provisioning after it.
func TestASecondPassMaintainErrorStopsProvisioning(t *testing.T) {
	var calls []string
	lifecycle := &passLifecycle{cycleLifecycle: cycleLifecycle{calls: &calls}, failOn: 2}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, completingSessions{cycleSessions{calls: &calls}}, lifecycle, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("a failed second maintenance pass was accepted")
	}
	if slices.Contains(calls, "provision") || lifecycle.provisions != 0 {
		t.Fatalf("provisioning crossed a failed second pass: %v", calls)
	}
}

// Deaf sessions outrank the second pass the same way they outrank provisioning.
func TestDeafSessionsSkipTheSecondPass(t *testing.T) {
	store, _ := openTestStore(t, 2)
	var calls []string
	lifecycle := &passLifecycle{cycleLifecycle: cycleLifecycle{calls: &calls}}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, completingSessions{cycleSessions{calls: &calls}}, lifecycle, 6)
	if err != nil {
		t.Fatal(err)
	}
	daemon.store = store
	daemon.deafAfter = 30 * time.Minute
	store.mu.Lock()
	store.started = time.Now().Add(-2 * time.Hour)
	store.lastEnvelope = time.Time{}
	store.mu.Unlock()
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !result.SessionsDeaf {
		t.Fatalf("the fixture is not deaf: %+v", result.Summary)
	}
	if lifecycle.maintainCalls != 1 || slices.Contains(calls, "provision") {
		t.Fatalf("a deaf cycle ran the second pass or provisioned: %v", calls)
	}
}
