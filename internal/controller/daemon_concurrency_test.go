package controller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fanOutLifecycle is a lifecycle whose ProvisionNext takes a few milliseconds,
// like a create waiting on GitHub and a Kata boot, and records how many ran at
// once. Its capacity check is atomic, as the Store's reservation is, so the
// daemon's job is only to stop starting creates, never to count slots.
// Each failure it returns stands for one provisioning_failures row, the unit
// the pool-wide breaker counts.
type fanOutLifecycle struct {
	cycleLifecycle
	mu         sync.Mutex
	demand     int
	charged    int
	capacity   int
	running    int
	maxRunning int
	calls      atomic.Int32
	failures   atomic.Int32
	failFirst  bool
	failFrom   int32 // every call numbered failFrom or later fails; 0 is never
	create     time.Duration
}

func (l *fanOutLifecycle) ProvisionNext(context.Context) (Lease, error) {
	call := l.calls.Add(1)
	l.mu.Lock()
	l.running++
	l.maxRunning = max(l.maxRunning, l.running)
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.running--
		l.mu.Unlock()
	}()
	time.Sleep(l.create)
	if (l.failFirst && call == 1) || (l.failFrom > 0 && call >= l.failFrom) {
		l.failures.Add(1)
		return Lease{}, errors.New("worker creation outcome uncertain")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.demand <= 0 {
		return Lease{}, ErrNoDemand
	}
	if l.charged >= l.capacity {
		return Lease{}, ErrCapacity
	}
	l.demand--
	l.charged++
	return Lease{ID: "lease"}, nil
}

func newFanOutDaemon(t *testing.T, lifecycle *fanOutLifecycle, limit, concurrency int) *Daemon {
	t.Helper()
	calls := new([]string)
	lifecycle.cycleLifecycle.calls = calls
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, limit)
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SetCreateConcurrency(concurrency); err != nil {
		t.Fatal(err)
	}
	return daemon
}

// A twelve-job burst fills twelve slots with at most the configured number of
// creates in flight: the bound is what keeps a burst from becoming twelve
// simultaneous GitHub acquires and Kata boots (homelab#1190 item 2).
func TestACycleCreatesConcurrentlyUpToTheBound(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 12, capacity: 12, create: 5 * time.Millisecond}
	daemon := newFanOutDaemon(t, lifecycle, 12, 4)
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Provisioned) != 12 {
		t.Fatalf("provisioned %d of a twelve-job burst, want 12", len(result.Provisioned))
	}
	if bound := min(4, maxFanOut); lifecycle.maxRunning > bound {
		t.Fatalf("%d creates ran at once, over the bound of %d", lifecycle.maxRunning, bound)
	}
	if lifecycle.maxRunning < 2 {
		t.Fatalf("creates ran %d at a time: the pass did not fan out", lifecycle.maxRunning)
	}
}

// The rollback: concurrency 1 is the serial loop, one create at a time.
func TestCreateConcurrencyOneIsSerial(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 6, capacity: 12, create: 2 * time.Millisecond}
	daemon := newFanOutDaemon(t, lifecycle, 12, 1)
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Provisioned) != 6 || lifecycle.maxRunning != 1 {
		t.Fatalf("provisioned=%d max at once=%d, want 6 created one at a time", len(result.Provisioned), lifecycle.maxRunning)
	}
}

// A fan-out stops at capacity: the pass ends at the first ErrCapacity and never
// charges past the platform bound, however many jobs wait.
func TestAConcurrentPassStopsAtCapacity(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 12, capacity: 5, create: 3 * time.Millisecond}
	daemon := newFanOutDaemon(t, lifecycle, 12, 4)
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Provisioned) != 5 || lifecycle.charged != 5 {
		t.Fatalf("provisioned=%d charged=%d against capacity 5", len(result.Provisioned), lifecycle.charged)
	}
	// Creates in flight when the first refusal came back may each draw one
	// more refusal, but none start after it.
	if calls := lifecycle.calls.Load(); calls > 5+4 {
		t.Fatalf("%d creates started for 5 slots: the pass kept starting after capacity refused", calls)
	}
}

// A broken backend costs one failure per pass, as the serial loop did: the
// first create runs alone, so it cannot take three siblings into the
// provisioning-failure breaker with it.
func TestTheFirstCreateOfAPassRunsAlone(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 12, capacity: 12, create: 2 * time.Millisecond, failFirst: true}
	daemon := newFanOutDaemon(t, lifecycle, 12, 4)
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the first create's failure")
	}
	if calls := lifecycle.calls.Load(); calls != 1 {
		t.Fatalf("%d creates started behind a failing first create, want 1", calls)
	}
	if daemon.refillAllowed.Load() {
		t.Fatal("a provisioning fault left the reaper allowed to refill")
	}
}

// homelab#1196 review I2. A fault that starts after the pass fanned out (the
// first create succeeded, then the image or JIT path broke) fails every
// create in flight at once. At concurrency 4 that was four rows in one pass,
// past the breaker's threshold of three. The fan-out is capped so a single
// pass stays under it, and nothing starts after the first failure.
func TestAFaultAfterFanOutCannotTripTheBreakerInOnePass(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 12, capacity: 12, create: 5 * time.Millisecond, failFrom: 2}
	daemon := newFanOutDaemon(t, lifecycle, 12, MaxCreateConcurrency)
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the fan-out's failures")
	}
	if rows := lifecycle.failures.Load(); rows >= provisionFailureThreshold {
		t.Fatalf("one pass wrote %d failure rows, want under the breaker threshold %d", rows, provisionFailureThreshold)
	}
	if calls := lifecycle.calls.Load(); calls > 1+maxFanOut {
		t.Fatalf("%d creates started; creates kept starting after the first failure", calls)
	}
}

// homelab#1196 review I3. Every create acquires a job and mints a JIT before
// the backend can refuse it for headroom, so a pass whose backend reported two
// free slots gains nothing from a second lane once the first create took one:
// it would only buy an acquire, a JIT and a cleanup.
func TestFanOutIsCappedAtTheBackendsFreeSlots(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 12, capacity: 2, create: 5 * time.Millisecond}
	daemon := newFanOutDaemon(t, lifecycle, 12, 4)
	daemon.freeSlots = func() (int, bool) { return 2, true }
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Provisioned) != 2 {
		t.Fatalf("provisioned %d into two free slots, want 2", len(result.Provisioned))
	}
	if lifecycle.maxRunning != 1 {
		t.Fatalf("%d creates ran at once with one free slot left after the first, want 1", lifecycle.maxRunning)
	}
	// Serial: two creates, then the one refusal the serial loop always paid.
	if calls := lifecycle.calls.Load(); calls != 3 {
		t.Fatalf("%d creates started for two free slots, want 3", calls)
	}
}

// An inventory that is not complete counts nothing, so it caps nothing: the
// backend's own headroom check still decides each create.
func TestAnIncompleteInventoryDoesNotCapFanOut(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 12, capacity: 12, create: 5 * time.Millisecond}
	daemon := newFanOutDaemon(t, lifecycle, 12, 4)
	daemon.freeSlots = func() (int, bool) { return 0, false }
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	if lifecycle.maxRunning != maxFanOut {
		t.Fatalf("%d creates ran at once, want the fan-out cap %d", lifecycle.maxRunning, maxFanOut)
	}
}

// The reaper fans out the same way and keeps its own per-pass cap.
func TestAReapPassCreatesConcurrentlyWithinItsCap(t *testing.T) {
	lifecycle := &fanOutLifecycle{demand: 0, capacity: 12, create: 5 * time.Millisecond}
	daemon := newFanOutDaemon(t, lifecycle, 12, 4)
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	lifecycle.mu.Lock()
	lifecycle.demand, lifecycle.maxRunning = 12, 0
	lifecycle.mu.Unlock()
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != reapRefillCap {
		t.Fatalf("one reap pass refilled %d, want the cap %d", len(reaped.Provisioned), reapRefillCap)
	}
	if lifecycle.maxRunning < 2 || lifecycle.maxRunning > 4 {
		t.Fatalf("a reap pass ran %d creates at once, want 2..4", lifecycle.maxRunning)
	}
}

func TestSetCreateConcurrencyRefusesOutOfRange(t *testing.T) {
	daemon := newFanOutDaemon(t, &fanOutLifecycle{}, 12, 1)
	for _, n := range []int{0, -1, MaxCreateConcurrency + 1} {
		if err := daemon.SetCreateConcurrency(n); err == nil {
			t.Fatalf("SetCreateConcurrency(%d) accepted", n)
		}
	}
}
