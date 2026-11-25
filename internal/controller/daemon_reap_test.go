package controller

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingSessions is a queue sweep that holds, the way the broker's empty
// long-poll holds for ~50 s, until the test releases it.
type blockingSessions struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSessions) Sync(context.Context) error { return nil }
func (s *blockingSessions) PollSweep(context.Context) ([]PollResult, int, error) {
	close(s.entered)
	<-s.release
	return nil, 0, nil
}
func (s *blockingSessions) Degraded() []SessionDegradation { return nil }
func (s *blockingSessions) CloseIdleConnections() int      { return 0 }
func (s *blockingSessions) RotateIdleSessions(context.Context) SessionRotation {
	return SessionRotation{}
}

// exclusiveLifecycle fails the test the moment two callers are inside the
// lifecycle at once, and counts what each path did.
type exclusiveLifecycle struct {
	t          *testing.T
	inside     atomic.Int32
	maintains  atomic.Int32
	workerOnly atomic.Int32
	provisions atomic.Int32
	charged    atomic.Int32 // slots currently charged by ProvisionNext
	capacity   int32
	demand     atomic.Int32 // jobs waiting for a slot
}

func (l *exclusiveLifecycle) enter() func() {
	if n := l.inside.Add(1); n != 1 {
		l.t.Errorf("%d callers inside the lifecycle at once: the reaper and the cycle overlapped", n)
	}
	time.Sleep(time.Millisecond) // widen the window a concurrent caller would hit
	return func() { l.inside.Add(-1) }
}
func (l *exclusiveLifecycle) ReconcileInventory(context.Context) error { defer l.enter()(); return nil }
func (l *exclusiveLifecycle) Maintain(context.Context) (int, error) {
	defer l.enter()()
	l.maintains.Add(1)
	return 0, nil
}
func (l *exclusiveLifecycle) MaintainWorkers(context.Context) (int, error) {
	defer l.enter()()
	l.workerOnly.Add(1)
	return 0, nil
}
func (l *exclusiveLifecycle) ProvisionNext(context.Context) (Lease, error) {
	defer l.enter()()
	if l.demand.Load() <= 0 {
		return Lease{}, ErrNoDemand
	}
	if l.charged.Load() >= l.capacity {
		return Lease{}, ErrCapacity
	}
	l.demand.Add(-1)
	if n := l.charged.Add(1); n > l.capacity {
		l.t.Errorf("slot over-admitted: %d charged against capacity %d", n, l.capacity)
	}
	l.provisions.Add(1)
	return Lease{ID: "lease"}, nil
}

// A worker that exits while the cycle is inside its long-poll is torn down and
// its slot refilled without waiting for the poll to return (homelab#1190).
func TestAWorkerExitingMidLongPollIsReapedAndRefilledBeforeThePollReturns(t *testing.T) {
	sessions := &blockingSessions{entered: make(chan struct{}), release: make(chan struct{})}
	lifecycle := &exclusiveLifecycle{t: t, capacity: 2}
	daemon, err := newDaemon(cycleRegistry{calls: new([]string)}, sessions, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	// A first cycle that reached provisioning makes refill legal for the reaper.
	first := &blockingSessions{entered: make(chan struct{}), release: make(chan struct{})}
	close(first.release)
	daemon.sessions = first
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	daemon.sessions = sessions
	lifecycle.charged.Store(2) // the pool is full

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := daemon.Cycle(ctx); err != nil {
			t.Error(err)
		}
	}()
	<-sessions.entered // the cycle is now parked in its long-poll

	// The runner exits and its lease is released; one job is waiting.
	lifecycle.charged.Store(1)
	lifecycle.demand.Store(1)
	before := lifecycle.workerOnly.Load() + lifecycle.maintains.Load()
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reaped.Ran {
		t.Fatal("the reaper did not run while the cycle was only waiting on the long-poll")
	}
	if got := lifecycle.workerOnly.Load() + lifecycle.maintains.Load(); got != before+1 {
		t.Fatalf("maintenance passes during the poll = %d, want 1", got-before)
	}
	if len(reaped.Provisioned) != 1 || lifecycle.charged.Load() != 2 {
		t.Fatalf("the freed slot was not refilled during the poll: provisioned=%d charged=%d", len(reaped.Provisioned), lifecycle.charged.Load())
	}
	close(sessions.release)
	<-done
}

// The reaper never runs lifecycle work while the cycle holds it, and the cycle
// never runs lifecycle work while the reaper holds it: one reaper at a time.
func TestTheReaperAndTheCycleNeverMaintainAtTheSameTime(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 4}
	lifecycle.demand.Store(1000)
	calls := new([]string)
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 4)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := daemon.Reap(ctx); err != nil {
					t.Error(err)
					return
				}
				lifecycle.charged.Store(0) // every provisioned worker "finished"
			}
		}()
	}
	for range 20 {
		*calls = nil
		if _, err := daemon.Cycle(ctx); err != nil {
			t.Fatal(err)
		}
		lifecycle.charged.Store(0)
	}
	close(stop)
	wg.Wait()
	if lifecycle.maintains.Load() < 20 {
		t.Fatalf("cycle maintenance passes = %d, want at least 20", lifecycle.maintains.Load())
	}
}

// The reaper refills only after a cycle has proven provisioning is allowed,
// and stops refilling the moment a cycle stops short of it (a sizing drain,
// deaf sessions, a failed boundary).
func TestTheReaperDoesNotRefillUntilACycleReachedProvisioning(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 2}
	lifecycle.demand.Store(5)
	calls := new([]string)
	daemon, err := newDaemon(cycleRegistry{calls: calls, fail: "refresh"}, cycleSessions{calls: calls}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 {
		t.Fatal("the reaper provisioned before any cycle reached provisioning")
	}
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the refresh failure")
	}
	reaped, err = daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 {
		t.Fatal("the reaper provisioned after a cycle failed before provisioning")
	}
}

// Registration cleanup is a GitHub read per lease; the reaper runs it at most
// once per registrationGap, so a fast reap interval does not multiply it.
func TestTheReaperRunsRegistrationCleanupAtMostOncePerGap(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 1}
	calls := new([]string)
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000_000, 0)
	daemon.now = func() time.Time { return now }
	daemon.discoveryOK.Store(true)
	for range 10 {
		if _, err := daemon.Reap(ctx); err != nil {
			t.Fatal(err)
		}
		now = now.Add(5 * time.Second)
	}
	// 50 s of 5 s ticks: one full pass at the start, the rest worker-only.
	if got := lifecycle.maintains.Load(); got != 1 {
		t.Fatalf("full Maintain passes in 50 s = %d, want 1", got)
	}
	if got := lifecycle.workerOnly.Load(); got != 9 {
		t.Fatalf("worker-only passes = %d, want 9", got)
	}
	now = now.Add(registrationGap)
	if _, err := daemon.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	if got := lifecycle.maintains.Load(); got != 2 {
		t.Fatalf("after the gap, full passes = %d, want 2", got)
	}
}

// A sweep that fails as a whole stops refill: the reaper must not keep
// provisioning on the strength of a poll that read nothing.
func TestAFailedSweepStopsTheReaperRefilling(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 2}
	lifecycle.demand.Store(5)
	calls := new([]string)
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls, fail: "poll"}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the poll failure")
	}
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 {
		t.Fatalf("the reaper provisioned %d after a failed sweep", len(reaped.Provisioned))
	}
}

// hangingLifecycle blocks MaintainWorkers until released, ignoring its
// context, the way a Kubernetes call with no timeout would.
type hangingLifecycle struct {
	cycleLifecycle
	entered chan struct{}
	release chan struct{}
}

func (l *hangingLifecycle) MaintainWorkers(context.Context) (int, error) {
	close(l.entered)
	<-l.release
	return 0, nil
}

// A reap pass that never returns must not hold Cycle past the cycle's own
// deadline: Cycle's wait for the lifecycle honours its context (review I1).
func TestAHungReapPassDoesNotHoldTheCyclePastItsDeadline(t *testing.T) {
	calls := new([]string)
	lifecycle := &hangingLifecycle{cycleLifecycle: cycleLifecycle{calls: calls}, entered: make(chan struct{}), release: make(chan struct{})}
	defer close(lifecycle.release)
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 1)
	if err != nil {
		t.Fatal(err)
	}
	daemon.lastRegistration = time.Now() // make the reaper take the worker-only path
	daemon.discoveryOK.Store(true)
	go func() { _, _ = daemon.Reap(context.Background()) }()
	<-lifecycle.entered
	cycleCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = daemon.Cycle(cycleCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cycle behind a hung reap returned %v, want its deadline error", err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("Cycle waited %s behind a hung reap, past its 100ms deadline", waited)
	}
}

// Every cycle clears refill at entry: after a cycle that provisioned, a cycle
// that fails at discovery must stop the reaper refilling (review I2).
func TestACycleThatFailsAfterAGoodOneStopsTheReaperRefilling(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 4}
	lifecycle.demand.Store(1)
	calls := new([]string)
	registry := &switchRegistry{calls: calls}
	daemon, err := newDaemon(registry, cycleSessions{calls: calls}, lifecycle, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	if !daemon.refillAllowed.Load() {
		t.Fatal("control: a clean cycle did not allow refill")
	}
	registry.fail = true
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the refresh failure")
	}
	lifecycle.demand.Store(3)
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 {
		t.Fatalf("the reaper provisioned %d after a cycle failed at discovery", len(reaped.Provisioned))
	}
}

// A provisioning error the cycle cannot explain stops the reaper refilling
// until a cycle provisions cleanly again (review I3).
func TestAProvisioningFaultStopsTheReaperRefilling(t *testing.T) {
	calls := new([]string)
	lifecycle := &cycleLifecycle{calls: calls, fail: "provision"}
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the provisioning failure")
	}
	before := lifecycle.provisions
	lifecycle.fail = ""
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 || lifecycle.provisions != before {
		t.Fatalf("the reaper provisioned after the cycle's provisioning failed: %d", len(reaped.Provisioned))
	}
}

// One reap pass refills at most reapRefillCap slots, however many are free.
func TestOneReapPassRefillsAtMostTheCap(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 12}
	calls := new([]string)
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 12)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	lifecycle.charged.Store(0)
	lifecycle.demand.Store(12)
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != reapRefillCap {
		t.Fatalf("one reap pass refilled %d, want the cap %d", len(reaped.Provisioned), reapRefillCap)
	}
}

// switchRegistry is cycleRegistry with a failure that can be turned on later.
type switchRegistry struct {
	calls *[]string
	fail  bool
}

func (r *switchRegistry) Refresh(context.Context) error {
	if r.fail {
		return errors.New("refresh failed")
	}
	return nil
}
func (r *switchRegistry) Reconcile(context.Context) error { return nil }

// During a discovery outage the reaper runs only the no-GitHub worker half of
// maintenance, as Cycle does (review nit).
func TestTheReaperSkipsRegistrationCleanupDuringADiscoveryOutage(t *testing.T) {
	lifecycle := &exclusiveLifecycle{t: t, capacity: 1}
	calls := new([]string)
	daemon, err := newDaemon(cycleRegistry{calls: calls, fail: "refresh"}, cycleSessions{calls: calls}, lifecycle, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.Cycle(ctx); err == nil {
		t.Fatal("expected the refresh failure")
	}
	before := lifecycle.maintains.Load()
	if _, err := daemon.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	if lifecycle.maintains.Load() != before || lifecycle.workerOnly.Load() == 0 {
		t.Fatalf("reap during a discovery outage ran full Maintain (%d) or no worker pass", lifecycle.maintains.Load()-before)
	}
}

// slowCreateLifecycle is a lifecycle whose worker Create takes longer than a
// reap pass's budget, and which records the failure row ProvisionNext writes
// when its context is cancelled mid-create.
type slowCreateLifecycle struct {
	cycleLifecycle
	create      time.Duration
	failureRows atomic.Int32
	provisioned atomic.Int32
	reapStarted atomic.Bool
}

func (l *slowCreateLifecycle) ProvisionNext(ctx context.Context) (Lease, error) {
	if !l.reapStarted.Load() {
		return Lease{}, ErrNoDemand // the priming cycle provisions nothing
	}
	if l.provisioned.Load() > 0 {
		return Lease{}, ErrNoDemand
	}
	select {
	case <-time.After(l.create):
		l.provisioned.Add(1)
		return Lease{ID: "lease"}, nil
	case <-ctx.Done():
		// ProvisionNext quarantines the lease and inserts a
		// provisioning_failures row when Create returns with an error.
		l.failureRows.Add(1)
		return Lease{}, ctx.Err()
	}
}

// A refill whose Create outlives the reap pass deadline finishes: the deadline
// bounds maintenance only, never a started Create, so it cannot write the
// failure rows that trip the pool-wide breaker (security review I1).
func TestACreateThatOutlivesTheReapDeadlineIsNotCancelled(t *testing.T) {
	calls := new([]string)
	lifecycle := &slowCreateLifecycle{cycleLifecycle: cycleLifecycle{calls: calls}, create: 150 * time.Millisecond}
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	daemon.passDeadline = 60 * time.Millisecond
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	lifecycle.reapStarted.Store(true)
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatalf("a slow Create failed the reap pass: %v", err)
	}
	if rows := lifecycle.failureRows.Load(); rows != 0 {
		t.Fatalf("the reap deadline cancelled a started Create: %d failure row(s)", rows)
	}
	if len(reaped.Provisioned) != 1 {
		t.Fatalf("the reaper refilled %d, want the one slow Create to finish", len(reaped.Provisioned))
	}
}

// A pass whose budget is already spent starts no refill at all, so the cycle,
// not an overrunning Create, fills the slot (security review I1).
func TestAReapPassWithNoBudgetLeftStartsNoRefill(t *testing.T) {
	calls := new([]string)
	lifecycle := &slowMaintainLifecycle{cycleLifecycle: cycleLifecycle{calls: calls, unbounded: true}, maintain: 50 * time.Millisecond}
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	daemon.passDeadline = 60 * time.Millisecond
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	daemon.lastRegistration = time.Now()
	before := lifecycle.provisions
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 || lifecycle.provisions != before {
		t.Fatalf("the reaper started %d refill(s) with under a third of its budget left", lifecycle.provisions-before)
	}
}

// slowMaintainLifecycle spends most of a pass's budget in MaintainWorkers.
type slowMaintainLifecycle struct {
	cycleLifecycle
	maintain time.Duration
}

func (l *slowMaintainLifecycle) MaintainWorkers(context.Context) (int, error) {
	time.Sleep(l.maintain)
	return 0, nil
}

// A refill fault in the reaper stops later reaper refills, the reaper-path
// twin of TestAProvisioningFaultStopsTheReaperRefilling (security review I2).
func TestAReaperProvisioningFaultStopsTheReaperRefilling(t *testing.T) {
	calls := new([]string)
	lifecycle := &cycleLifecycle{calls: calls, unbounded: true}
	daemon, err := newDaemon(cycleRegistry{calls: calls}, cycleSessions{calls: calls}, lifecycle, 2)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle.fail = ""
	if _, err := daemon.Cycle(ctx); err != nil {
		t.Fatal(err)
	}
	if !daemon.refillAllowed.Load() {
		t.Fatal("control: a clean cycle did not allow refill")
	}
	lifecycle.fail = "provision"
	if _, err := daemon.Reap(ctx); err == nil {
		t.Fatal("expected the reaper's provisioning failure")
	}
	lifecycle.fail = ""
	before := lifecycle.provisions
	reaped, err := daemon.Reap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reaped.Provisioned) != 0 || lifecycle.provisions != before {
		t.Fatalf("the reaper kept refilling after its own provisioning fault: %d", len(reaped.Provisioned))
	}
}
