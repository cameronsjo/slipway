package discovery

import (
	"sync"
	"time"
)

// CycleObserver is the producer behind CycleHealth. The maintenance loop lives
// in the daemon runtime and journals nothing about itself, so without this the
// section would publish state nothing produces — and the failure it exists to
// catch is exactly the one a status endpoint can manufacture: the HTTP server
// is its own goroutine, so a wedged or crash-looping cycle keeps answering 200
// with a report that quietly stops changing.
//
// It is process-local by construction. Its values reset on restart and are read
// against Report.UptimeSeconds, which is stated on CycleHealth where they are
// published.
type CycleObserver struct {
	mu                  sync.Mutex
	lastCompleted       time.Time
	consecutiveFailures int
	diagnostic          string
}

// NewCycleObserver returns an observer that has seen no cycle. That is the
// honest state of a process that has just started, and Snapshot treats it as
// wedged once one interval has passed without one.
func NewCycleObserver() *CycleObserver { return &CycleObserver{} }

// Completed records the end of one cycle, whether or not it returned an error.
// A cycle that ran and failed is still a loop that is turning, which is why
// freshness and the failure count are separate fields: they catch different
// faults and a caller must not have to choose which one to report.
func (o *CycleObserver) Completed(at time.Time, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lastCompleted = at.UTC()
	if err == nil {
		o.consecutiveFailures, o.diagnostic = 0, ""
		return
	}
	o.consecutiveFailures++
	o.diagnostic = Redact(err)
}

// Snapshot composes CycleHealth at one instant. uptime and interval come from
// the same report the result is placed in, so no two fields of one body can
// disagree about what time it is or what the configured interval was.
//
// Stale is true past three intervals, and also whenever no cycle has completed
// while uptime already exceeds one interval. That second term is the one a
// naive age-over-three-intervals implementation gets wrong: with no cycle
// behind it the age is -1, which compares below every threshold, so a process
// wedged before its first cycle would read healthy forever. Only the first
// interval of a fresh process reads not-stale with no cycle behind it.
func (o *CycleObserver) Snapshot(now time.Time, uptime, interval time.Duration) CycleHealth {
	o.mu.Lock()
	defer o.mu.Unlock()
	health := CycleHealth{
		AgeSeconds:          -1,
		IntervalSeconds:     int64(interval.Seconds()),
		ConsecutiveFailures: o.consecutiveFailures,
		Diagnostic:          o.diagnostic,
	}
	if o.lastCompleted.IsZero() {
		health.Stale = uptime > interval
	} else {
		health.LastCompleted = o.lastCompleted.Format(time.RFC3339Nano)
		age := now.UTC().Sub(o.lastCompleted)
		if age < 0 {
			// A backwards clock is not evidence of freshness. Treat it the way
			// the discovery fence treats a negative age: not current.
			health.AgeSeconds, health.Stale = 0, true
		} else {
			health.AgeSeconds = int64(age.Seconds())
			health.Stale = age > 3*interval
		}
	}
	health.Status = Healthy
	if health.Stale || health.ConsecutiveFailures >= cycleFailureThreshold {
		health.Status = Degraded
	}
	return health
}

// cycleFailureThreshold is the § What each section calls Healthy row for this
// section: a single failure is ordinary and is retried on the next tick, and a
// rising count is the controller failing to make progress.
const cycleFailureThreshold = 3
