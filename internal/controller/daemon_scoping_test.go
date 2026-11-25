package controller

import (
	"errors"
	"slices"
	"testing"
)

// Review R2 (D2): one set's failed poll no longer stops provisioning for every
// set. The failure is counted and returned; the sets that did poll are still
// provisioned in the same cycle.
func TestASetPollFailureStillProvisionsTheOtherSets(t *testing.T) {
	var calls []string
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, polls: 3, fail: "set-poll"}, &cycleLifecycle{calls: &calls}, 6)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	var setErr *SetPollError
	if !errors.As(err, &setErr) || setErr.Set != (SetKey{RepositoryID: 7, ScaleSetID: 70}) {
		t.Fatalf("the failed set is not named in the cycle error: %v", err)
	}
	if !slices.Contains(calls, "provision") {
		t.Fatalf("one set's failed poll stopped provisioning: %v", calls)
	}
	if result.Summary.PollFailures != 1 || result.Polled != 3 || !slices.Equal(result.Summary.PollFailedSets, []SetKey{{RepositoryID: 7, ScaleSetID: 70}}) {
		t.Fatalf("poll_failures=%d sets=%v polled=%d, want 1, [7/70] and 3", result.Summary.PollFailures, result.Summary.PollFailedSets, result.Polled)
	}
}

// A sweep that fails as a whole still stops the cycle before provisioning:
// nothing may act on demand the sweep could not read.
func TestAWholeSweepFailureStillStopsProvisioning(t *testing.T) {
	var calls []string
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, polls: 3, fail: "poll"}, &cycleLifecycle{calls: &calls}, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("a whole-sweep failure reported success")
	}
	if slices.Contains(calls, "provision") {
		t.Fatalf("a whole-sweep failure still provisioned: %v", calls)
	}
}

// Review R1 (D1): a set whose session could not be established is counted and
// named, and the cycle carries on through polling and provisioning.
func TestADegradedSessionIsCountedAndTheCycleCarriesOn(t *testing.T) {
	var calls []string
	degraded := []SessionDegradation{{Set: SetKey{RepositoryID: 9, ScaleSetID: 90}, Err: errors.New("resume 503")}}
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, polls: 2, degraded: degraded}, &cycleLifecycle{calls: &calls}, 6)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemon.Cycle(ctx)
	if err == nil {
		t.Fatal("a degraded set was not reported in the cycle error")
	}
	if result.Summary.DegradedSessions != 1 || !slices.Equal(result.Summary.DegradedSets, []SetKey{degraded[0].Set}) {
		t.Fatalf("degraded_sessions=%d sets=%v, want 1 and %v", result.Summary.DegradedSessions, result.Summary.DegradedSets, degraded[0].Set)
	}
	if !slices.Contains(calls, "poll") || !slices.Contains(calls, "provision") {
		t.Fatalf("a degraded set stopped the cycle: %v", calls)
	}
}

func TestOnlySetPollErrorsRecognisesItsShape(t *testing.T) {
	a := &SetPollError{Set: SetKey{RepositoryID: 1, ScaleSetID: 1}, Err: errors.New("x")}
	b := &SetPollError{Set: SetKey{RepositoryID: 2, ScaleSetID: 2}, Err: errors.New("y")}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"one", a, true},
		{"joined", errors.Join(a, b), true},
		{"mixed", errors.Join(a, errors.New("sweep")), false},
		{"plain", errors.New("sweep"), false},
	} {
		if got := onlySetPollErrors(c.err); got != c.want {
			t.Errorf("%s: onlySetPollErrors = %t, want %t", c.name, got, c.want)
		}
	}
}
