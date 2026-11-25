package discovery

import (
	"errors"
	"testing"
	"time"
)

const interval = time.Minute

var origin = time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

// B5: the case a naive age-over-three-intervals implementation gets wrong. With
// no completed cycle the age is -1, which compares below every threshold, so a
// process wedged before its first cycle would read healthy forever.
func TestCycleWithNoCompletedCycleGoesStaleAfterOneInterval(t *testing.T) {
	observer := NewCycleObserver()

	fresh := observer.Snapshot(origin, interval-time.Second, interval)
	if fresh.Stale {
		t.Fatal("a process inside its first interval with no cycle read stale")
	}
	if fresh.AgeSeconds != -1 {
		t.Fatalf("age with no completed cycle = %d, want -1", fresh.AgeSeconds)
	}
	if fresh.Status != Healthy {
		t.Fatalf("verdict inside the first interval = %q, want healthy", fresh.Status)
	}

	wedged := observer.Snapshot(origin, interval+time.Second, interval)
	if !wedged.Stale {
		t.Fatal("a process past one interval that has never completed a cycle read not-stale")
	}
	if wedged.Status != Degraded {
		t.Fatalf("verdict for a never-completed cycle past one interval = %q, want degraded", wedged.Status)
	}
	if wedged.AgeSeconds != -1 {
		t.Fatalf("age with no completed cycle = %d, want -1", wedged.AgeSeconds)
	}
}

func TestCycleFreshnessAndFailureCountAreSeparateFaults(t *testing.T) {
	for _, test := range []struct {
		name     string
		record   []error
		at       time.Time
		stale    bool
		failures int
		verdict  Verdict
	}{
		{"a completed cycle is fresh", []error{nil}, origin.Add(interval), false, 0, Healthy},
		{"three intervals is still fresh", []error{nil}, origin.Add(3 * interval), false, 0, Healthy},
		{"past three intervals is stale", []error{nil}, origin.Add(3*interval + time.Second), true, 0, Degraded},
		{"one failure is ordinary", []error{errors.New("x")}, origin.Add(interval), false, 1, Healthy},
		{"two failures are ordinary", []error{errors.New("x"), errors.New("x")}, origin.Add(interval), false, 2, Healthy},
		{"three failures in a row degrade", []error{errors.New("x"), errors.New("x"), errors.New("x")}, origin.Add(interval), false, 3, Degraded},
		{"a success resets the run", []error{errors.New("x"), errors.New("x"), errors.New("x"), nil}, origin.Add(interval), false, 0, Healthy},
	} {
		t.Run(test.name, func(t *testing.T) {
			observer := NewCycleObserver()
			for _, err := range test.record {
				observer.Completed(origin, err)
			}
			health := observer.Snapshot(test.at, test.at.Sub(origin)+interval, interval)
			if health.Stale != test.stale {
				t.Errorf("stale = %v, want %v", health.Stale, test.stale)
			}
			if health.ConsecutiveFailures != test.failures {
				t.Errorf("consecutive failures = %d, want %d", health.ConsecutiveFailures, test.failures)
			}
			if health.Status != test.verdict {
				t.Errorf("verdict = %q, want %q", health.Status, test.verdict)
			}
			if health.IntervalSeconds != int64(interval.Seconds()) {
				t.Errorf("interval was not published: %d", health.IntervalSeconds)
			}
		})
	}
}

// A cycle that failed is still a loop that is turning, so the diagnostic is
// published without the freshness field claiming the loop stopped.
func TestCycleDiagnosticIsRedactedAndClearedOnSuccess(t *testing.T) {
	observer := NewCycleObserver()
	observer.Completed(origin, errors.New("dial tcp 10.0.0.1:443: connect: connection refused"))
	health := observer.Snapshot(origin, 2*interval, interval)
	if health.Diagnostic != DiagnosticUnavailable {
		t.Fatalf("diagnostic = %q, want the redacted vocabulary word", health.Diagnostic)
	}
	observer.Completed(origin, nil)
	if got := observer.Snapshot(origin, 2*interval, interval).Diagnostic; got != "" {
		t.Fatalf("diagnostic after a successful cycle = %q, want empty", got)
	}
}

// A backwards clock is not evidence of freshness.
func TestCycleWithABackwardsClockIsStale(t *testing.T) {
	observer := NewCycleObserver()
	observer.Completed(origin, nil)
	health := observer.Snapshot(origin.Add(-time.Hour), 2*interval, interval)
	if !health.Stale || health.AgeSeconds != 0 {
		t.Fatalf("backwards clock gave stale=%v age=%d, want stale with a zero age", health.Stale, health.AgeSeconds)
	}
}
