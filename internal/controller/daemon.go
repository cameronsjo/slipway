package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
)

type registryCycle interface {
	Refresh(context.Context) error
	Reconcile(context.Context) error
}

type sessionCycle interface {
	Sync(context.Context) error
	// Degraded lists the sets the last Sync skipped because their session
	// could not be established; they are retried by the next Sync.
	Degraded() []SessionDegradation
	PollSweep(context.Context) ([]PollResult, int, error)
	// CloseIdleConnections closes idle HTTP connections on every managed
	// queue session's transport and returns how many sessions it touched.
	// Only called when sessionRecoveryRung judges the close-connections rung.
	CloseIdleConnections() int
	// RotateIdleSessions closes the session of every set with no assigned work
	// at a deaf exit, so the restart opens new ones instead of resuming the
	// same UUIDs (homelab#1138). Called only when the cycle is deaf.
	RotateIdleSessions(context.Context) SessionRotation
}

type lifecycleCycle interface {
	ReconcileInventory(context.Context) error
	// Maintain returns how many reservations it retained for a teardown that
	// lacked exact absence; see PoolLifecycle.Maintain.
	Maintain(context.Context) (int, error)
	// MaintainWorkers is Maintain without the GitHub registration cleanup;
	// see PoolLifecycle.MaintainWorkers.
	MaintainWorkers(context.Context) (int, error)
	ProvisionNext(context.Context) (Lease, error)
}

type CycleResult struct {
	Polled      int
	Provisioned []Lease
	Summary     CycleSummary
	// RestartForSizing is set once a sizing change deferred at Open becomes
	// adoptable: the journal is idle and this controller has latched itself out
	// of new work. The process must exit cleanly so its supervisor restarts it,
	// because the adoption happens in Open and nowhere else.
	RestartForSizing bool
	// SessionsDeaf is set when sessionsLookDeaf judged this process's queue
	// sessions dead: no envelope from any subscription within the configured
	// window since either the last envelope or process start. Unlike
	// RestartForSizing, this is a fault -- the caller exits non-zero so its
	// supervisor restarts it. A restart resumes every journaled active session
	// (same session IDs), which is why ten restarts on 2026-09-26 recovered
	// nothing (homelab#1074). So before this is returned, the cycle closes the
	// session of every set with no assigned work (Rotation); the restart opens
	// new sessions for those and resumes only the rest (homelab#1138).
	SessionsDeaf bool
	// Rotation is what the deaf exit's session rotation did. Empty unless
	// SessionsDeaf.
	Rotation SessionRotation
	// ConnectionsClosed is how many queue sessions had their idle HTTP
	// connections closed this cycle, because sessionRecoveryRung judged the
	// close-connections rung applies. Zero on every cycle that rung does not
	// fire on, which is nearly all of them.
	ConnectionsClosed int
	// SessionStageFailed is set when the cycle stopped in the session stage: Sync
	// failed globally, or PollSweep failed as a whole rather than per set. It is
	// the one failure shape the daemon loop's stalled-cycle backstop counts
	// (homelab#1211). A discovery, inventory or Maintain failure degrades on
	// purpose (homelab#1096) and a restart does not cure it; a sweep over zero
	// sets, because every set is degraded and waits on an operator, does not
	// fail at all.
	SessionStageFailed bool
}

// CycleSummary is what one successful cycle did. It exists because success was
// silent: the controller advertised capacity 0 to every scale set for hours,
// GitHub therefore offered it no job, and nothing anywhere said so. Every field
// here is a number the operator had to reconstruct from the journal and the
// host, and Admission -- the one that decides whether a job can ever arrive --
// was not reported by any surface at all.
//
// Admission is NOT `free slots` as the health report means it. The report
// computes free slots from the journal (platform slots minus charged leases);
// Admission is min(that, active+FreeSlots) from the last trusted BACKEND
// observation, so host headroom can hold it at zero while the report shows
// slots free. Reading one for the other is what made this invisible.
type CycleSummary struct {
	// Ready is the store's inventory fence. False means no session call was made.
	Ready bool
	// Admission is the total commitments a trusted inventory currently allows.
	// Zero means every advertisement in this cycle offered no capacity.
	Admission int
	// Charged is the lease count in any non-released phase.
	Charged int
	// ActiveWorkers is the lease count whose worker is plausibly running a
	// job (Store.ActiveWorkerLeases) -- the narrower count sessionsLookDeaf
	// and sessionRecoveryRung are fed, not Charged. See ActiveWorkerLeases
	// for why quarantined and the pre-provisioning phases are excluded.
	ActiveWorkers int
	// Advertised is one entry per polled scale set, in poll order.
	Advertised []SetAdvertisement
	// Messages is how many polls returned a durable envelope.
	Messages int
	// Deferred is assigned demand this cycle could not advertise because it
	// exceeded admission. Non-zero is the ordinary state of a busy shared pool;
	// it says jobs are waiting for a slot, not that anything is wrong.
	Deferred int
	// FreeSlots and Binding come from the last trusted backend observation.
	// `free=0 reason=memory` is the difference between "the pool is idle" and
	// "the pool cannot start anything and here is why"; without the reason, a
	// zero sent an operator to the thin pool for a memory problem.
	FreeSlots int
	Binding   string
	// BreakerOpen says the provisioning breaker (three provisioning failures
	// inside fifteen minutes) is holding admission at the active lease count,
	// and BreakerUntil is when it lets go -- zero if the stored deadline does
	// not parse. It is persisted in SQLite, survives a restart, and on
	// 2026-10-01 held admission at zero for fifteen minutes under a cycle line
	// that read ready=true reason=slots throughout (homelab#1213). Meaningful
	// only when BreakerMeasured: a failed read must not print as "closed".
	BreakerOpen     bool
	BreakerUntil    time.Time
	BreakerMeasured bool
	// SizingPending, PendingCapacity and PendingLegacySlots describe a sizing
	// change deferred at Open. Without them the operator sees a manifest saying
	// capacity 2, a pool behaving as capacity 1, and nothing anywhere joining
	// the two.
	SizingPending      bool
	PendingCapacity    int
	PendingLegacySlots int
	// Management is what this cycle's Reconcile decided about onboarding:
	// repositories entering scale-set creation, those the per-cycle budget held
	// back, and those MaxRepositories refused. With an empty allowlist the
	// served set tracks the account, so these are the only numbers that say
	// onboarding is in progress rather than finished.
	Management ManagementCycle
	// Draining marks the one cycle that stops early to adopt a sizing change.
	// It has to be unambiguous, because that cycle reports `polled=0
	// advertised=none` -- byte for byte the signature of the silent outage this
	// line exists to reveal, where the controller advertised nothing for hours.
	Draining bool
	// LastEnvelopeAge is how long it has been since this process last received
	// a queue envelope from any subscription, measured at summarize time.
	// Negative means none yet this process -- distinct from "just received
	// one", which is near zero. Meaningful only when LastEnvelopeMeasured.
	LastEnvelopeAge time.Duration
	// LastEnvelopeMeasured is false when summarize never ran -- a cycle that
	// failed before reaching it (registry refresh, inventory, session sync)
	// carries the zero CycleSummary, whose LastEnvelopeAge of 0 would otherwise
	// print as "an envelope just arrived" on exactly the failing cycles an
	// operator reads during an incident.
	LastEnvelopeMeasured bool
	// SessionsDeaf mirrors CycleResult.SessionsDeaf onto the line-printing
	// summary, computed by the same sessionsLookDeaf call that set it.
	SessionsDeaf bool
	// SessionDeafWindow is the window sessionsLookDeaf was evaluated against.
	// Carried here so the caller's exit-time log line can name it without a
	// second channel back to daemon configuration.
	SessionDeafWindow time.Duration
	// QueuedDemand and DemandKnown are the REST demand signal the deaf
	// check used this cycle (#1011); DemandKnown false means the idle
	// fallback applied.
	QueuedDemand int
	DemandKnown  bool
	// Rung is what sessionRecoveryRung decided this cycle. Meaningful only
	// when LastEnvelopeMeasured (the same guard as SessionsDeaf); the zero
	// value "" is not RungNone and must not be read as one.
	Rung Rung
	// Polls tallies this cycle's poll outcomes by what GitHub's queue
	// answered -- an empty long-poll, a delivered message, an SDK-silent 401
	// refresh, an envelope the journal itself refused, one refused often enough
	// to be acknowledged unapplied (quarantined), or anything else Poll
	// treated as a failure. Computed from the same []PollResult the cycle
	// line's `polled=`/`advertised=` fields are.
	Polls PollOutcomes
	// Retained is how many workers' reservations Maintain kept charged this
	// cycle because their teardown lacked exact absence (homelab#924). One is
	// a pod still terminating; the same one on every line is a stuck teardown
	// holding a slot until the cleanup-deadline quarantine. Meaningful only
	// when RetainedMeasured: a cycle that failed before Maintain finished
	// carries the zero value, which must not read as "nothing retained".
	Retained         int
	RetainedMeasured bool
	// DegradedSessions is how many managed sets this cycle's Sync skipped
	// because their queue session could not be established. Each one is
	// unpolled and unprovisioned this cycle while every other set runs; the
	// same set here on every line is a session that keeps failing.
	DegradedSessions int
	// DegradedSets names those sets, ordered by repository and set.
	DegradedSets []SetKey
	// PollFailures is how many sets' polls failed this cycle. Provisioning
	// still runs for the sets whose polls succeeded.
	PollFailures int
	// PollFailedSets names those sets, in sweep order.
	PollFailedSets []SetKey
	// UnseenProbes is how many probe rows this cycle journaled for reported
	// demand no envelope named (Store.ProbeUnseenDemand).
	UnseenProbes int
	// Silence is how long the deaf watchdog's clock had run when this cycle
	// judged it (silenceClock): from the latest of process start, the last
	// envelope and the demand appearing. Meaningful only when
	// LastEnvelopeMeasured and SilenceJudged.
	Silence       time.Duration
	SilenceJudged bool
}

// PollOutcomes tallies one cycle's polls by what GitHub's queue answered.
// Sums to len(Advertised) in the ordinary case; a PollResult whose Outcome
// was never set (the pre-#912-fix zero value) counts in Other rather than
// silently reading as an empty 202, so a caller that forgets to set it is
// visible instead of miscounted.
type PollOutcomes struct {
	Empty, Message, Refreshed, Refused, Quarantined, Other int
}

// SetAdvertisement is the capacity one scale set was offered in this cycle.
type SetAdvertisement struct {
	Set         SetKey
	MaxCapacity int
}

// Daemon joins the independently tested controller components into one bounded
// maintenance cycle. A platform command may schedule Cycle only after it has a
// real WorkerBackend and the live credential/protocol gates are accepted.
type Daemon struct {
	registry  registryCycle
	sessions  sessionCycle
	lifecycle lifecycleCycle
	limit     int
	// store is read only for the per-cycle summary. It is nil in the component
	// tests, which construct the daemon from interfaces; the summary degrades
	// to the poll results alone rather than refusing, and the deafness check
	// below is skipped along with it.
	store *Store
	// deafAfter is the sessionsLookDeaf window: how long this process may go
	// without receiving a queue envelope from any subscription before its
	// sessions are judged dead. Only consulted when store is non-nil.
	deafAfter time.Duration
	// demandDeafAfter is how long sessions may stay silent while REST shows
	// queued pool jobs before they count as deaf (#1011). deafAfter stays the
	// fallback when no REST reading is fresh.
	demandDeafAfter time.Duration
	// lastKnownEnvelope and rungFired track the close-connections rung's
	// "already ran for this silent episode" state across cycles. Both are
	// process-local and unguarded: Cycle runs sequentially from the one
	// caller (daemonRuntime.Run's ticker loop), never concurrently with
	// itself, so no lock is needed for state only Cycle touches.
	lastKnownEnvelope time.Time
	rungFired         bool

	// local serializes every lifecycle call between Cycle and Reap
	// (homelab#1190): one reaper at a time. Cycle holds it for everything but
	// its PollSweep, the ~50 s broker long-poll, which is exactly the window
	// Reap exists to use. It is a one-slot channel, not a sync.Mutex, so
	// Cycle's wait for it honours the cycle's context: a reap pass that hangs
	// cannot hold Cycle past cycleDeadline or the deaf watchdog.
	localOnce sync.Once
	local     chan struct{}
	// discoveryOK is whether the latest Cycle's discovery refresh succeeded.
	// During a discovery outage Cycle runs only the no-GitHub worker half of
	// maintenance, and Reap does the same.
	discoveryOK atomic.Bool
	// refillAllowed is true only while the latest Cycle reached its
	// provisioning loop. A cycle that stopped short of it (a failed boundary,
	// a sizing drain, deaf sessions) clears it, so Reap never provisions
	// anything the cycle itself would not have.
	refillAllowed atomic.Bool
	// lastRegistration is when a full Maintain (with its GitHub registration
	// cleanup) last ran, from either path; guarded by local.
	lastRegistration time.Time
	// now is time.Now, replaced in tests.
	now func() time.Time
	// passDeadline is reapPassDeadline, replaced in tests.
	passDeadline time.Duration
	// createConcurrency is the most worker creations one provisioning pass,
	// the cycle's or a reap pass's, runs at once (homelab#1190 item 2). One
	// is the serial loop this replaced. fanOutWidth may run fewer; see
	// provision.
	createConcurrency int
	// freeSlots replaces the Store's inventory reading in tests, which build
	// the daemon without a Store.
	freeSlots func() (int, bool)
}

// MaxCreateConcurrency bounds SetCreateConcurrency. Every create holds a
// GitHub acquire, a JIT registration and a Kata boot in flight, so the bound
// is a cap on burst load, not on throughput. While the breaker threshold is
// three, provision runs at most maxFanOut (two) at once whatever this is set
// to; see fanOutWidth.
const MaxCreateConcurrency = 8

// SetCreateConcurrency sets how many worker creations one provisioning pass
// runs at once. One keeps creation serial, which is also the rollback.
func (d *Daemon) SetCreateConcurrency(n int) error {
	if n < 1 || n > MaxCreateConcurrency {
		return fmt.Errorf("create concurrency must be 1..%d, got %d", MaxCreateConcurrency, n)
	}
	d.createConcurrency = n
	return nil
}

// registrationGap is the least time between two registration-cleanup passes
// the reaper runs: CleanupRegistration is a GitHub read per lease, so a fast
// reap interval must not multiply it (homelab#1190).
const registrationGap = time.Minute

// reapPassDeadline bounds the maintenance half of one Reap pass, well under
// the cycle deadline (half the 15-minute deaf window), so a stuck Kubernetes
// call in the reaper releases the lifecycle long before Cycle would time out
// waiting for it. It never cancels a refill already started: a worker Create
// cut short is quarantined and writes a provisioning_failures row, and three of
// those in 15 minutes trip the pool-wide breaker. A refill runs under the
// daemon's own context bounded by workerStartupTimeout, exactly as the cycle's
// does, and the deadline only decides whether the next refill STARTS.
const reapPassDeadline = 90 * time.Second

// reapRefillMinBudget is the least pass budget a refill needs to start. A
// worker starts in seconds when healthy; a pass with less than this left
// hands the rest to the cycle instead of starting a Create it would overrun.
const reapRefillMinBudget = 30 * time.Second

// reapRefillCap bounds how many slots one Reap pass refills. The cycle's own
// provisioning loop still fills the rest; the cap keeps one reap pass short
// and its hold on the lifecycle brief.
const reapRefillCap = 4

// ReapRefillCap is reapRefillCap, exported for the startup log line.
const ReapRefillCap = reapRefillCap

func (d *Daemon) lifecycleLock() chan struct{} {
	d.localOnce.Do(func() { d.local = make(chan struct{}, 1) })
	return d.local
}

// acquire waits for the lifecycle, or returns the context's error.
func (d *Daemon) acquire(ctx context.Context) error {
	select {
	case d.lifecycleLock() <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the lifecycle held by a reap pass: %w", ctx.Err())
	}
}

func (d *Daemon) tryAcquire() bool {
	select {
	case d.lifecycleLock() <- struct{}{}:
		return true
	default:
		return false
	}
}

func (d *Daemon) release() { <-d.lifecycleLock() }

// ReapResult is what one Reap did. Ran is false when a cycle held the
// lifecycle and the reaper stood aside.
type ReapResult struct {
	Ran         bool
	Retained    int
	Provisioned []Lease
}

// Reap is the worker half of a cycle, run on its own short timer so a finished
// worker is torn down, and its slot refilled from already-journaled demand,
// while the cycle is parked in its long-poll (homelab#1190). Its GitHub calls
// are a registration-cleanup pass at most once per registrationGap (none
// during a discovery outage) and, for each slot it refills, the same acquire
// and JIT registration the cycle's provisioning makes. It provisions only
// while refillAllowed, at most reapRefillCap slots, and one pass is bounded by
// reapPassDeadline. It never waits for the cycle: if the cycle holds the
// lifecycle, it returns Ran=false.
func (d *Daemon) Reap(ctx context.Context) (ReapResult, error) {
	var result ReapResult
	if !d.tryAcquire() {
		return result, nil
	}
	defer d.release()
	deadline := d.passDeadline
	if deadline <= 0 {
		deadline = reapPassDeadline
	}
	passCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	result.Ran = true
	if err := d.lifecycle.ReconcileInventory(passCtx); err != nil {
		return result, err
	}
	var err error
	if now := d.clock(); d.discoveryOK.Load() && now.Sub(d.lastRegistration) >= registrationGap {
		result.Retained, err = d.lifecycle.Maintain(passCtx)
		d.lastRegistration = now
	} else {
		result.Retained, err = d.lifecycle.MaintainWorkers(passCtx)
	}
	if err != nil || !d.refillAllowed.Load() {
		return result, err
	}
	budgetLeft := func() bool {
		end, _ := passCtx.Deadline()
		return time.Until(end) >= min(reapRefillMinBudget, deadline/3)
	}
	result.Provisioned, err = d.provision(min(d.limit, reapRefillCap), budgetLeft, func() (Lease, error) { return d.refill(ctx) })
	if err != nil {
		// The same rule as the cycle's loop: a fault neither path can
		// explain stops the reaper until a cycle provisions cleanly.
		d.refillAllowed.Store(false)
	}
	return result, err
}

// provision starts up to n worker creations through next and returns the
// leases that were created. It stops starting new ones at the first
// ErrNoDemand or ErrCapacity, which end a pass cleanly, at the first other
// error, which is returned, and whenever mayStart says no.
//
// Several run at once (fanOutWidth), because each create spends ~4 s on
// GitHub round trips and a Kata boot, and a serial loop made the last worker
// of a nine-job burst wait 53 s behind the first eight (homelab#1190).
//
// THE FIRST CREATE OF A PASS RUNS ALONE. A systemic fault (a bad image, a
// broken JIT path) then costs one failure per pass, as the serial loop did:
// provisionFailureThreshold failures in fifteen minutes trip the pool-wide
// breaker, and a fan-out into a broken backend would trip it in a single pass.
// Fan-out starts only after one create in the pass succeeded. A fault after
// that fails only the creates already in flight when it first shows, and none
// start after it; fanOutWidth keeps those under the threshold, so no single
// pass can trip the breaker on its own.
//
// Concurrency is safe to this layer because every create runs inside the
// lifecycle lock the caller holds, so no Maintain or inventory runs beside it,
// and the two shared limits are atomic below it: the journal's slot
// reservation holds the Store mutex (ReserveNextExcept), and the Kubernetes
// backend counts creations in flight against its headroom (Backend.Create).
func (d *Daemon) provision(n int, mayStart func() bool, next func() (Lease, error)) ([]Lease, error) {
	var (
		mu      sync.Mutex
		leases  []Lease
		errs    []error
		stopped bool
	)
	record := func(lease Lease, err error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err == nil:
			leases = append(leases, lease)
		case errors.Is(err, ErrNoDemand) || errors.Is(err, ErrCapacity):
			stopped = true
		default:
			stopped = true
			errs = append(errs, err)
		}
	}
	halted := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return stopped
	}
	if n < 1 || !mayStart() {
		return nil, nil
	}
	lease, err := next() // alone: see above
	record(lease, err)
	if halted() {
		return leases, errors.Join(errs...)
	}
	lanes := make(chan struct{}, d.fanOutWidth())
	var wg sync.WaitGroup
	for range n - 1 {
		lanes <- struct{}{} // a free lane, so at most fanOutWidth run
		if halted() || !mayStart() {
			<-lanes
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-lanes }()
			lease, err := next()
			record(lease, err)
		}()
	}
	wg.Wait()
	return leases, errors.Join(errs...)
}

// maxFanOut is the most creates one pass runs at once once it has fanned out.
// A fault shared by every create in flight fails all of them, so this many
// provisioning_failures rows is the most one pass can write, one short of the
// breaker. Raising createConcurrency does not raise it; raising the breaker
// threshold does.
const maxFanOut = provisionFailureThreshold - 1

// MaxFanOut is maxFanOut, exported for the startup log line.
const MaxFanOut = maxFanOut

// fanOutWidth is how many creates run at once after a pass's first create
// succeeded: createConcurrency, capped at maxFanOut, and at the backend's free
// slots from the inventory the pass began with, less the one the first create
// took. Each create acquires a job and mints a JIT registration before the
// backend can refuse it for headroom, so a lane with no slot behind it only
// buys a cleanup. A pass with no free slot left still runs one create at a
// time, as the serial loop did: the reading can be stale, and the backend's
// own check is what decides.
func (d *Daemon) fanOutWidth() int {
	width := min(max(1, d.createConcurrency), maxFanOut)
	if free, known := d.backendFreeSlots(); known {
		width = min(width, max(1, free-1))
	}
	return width
}

// backendFreeSlots is the free-slot count from the latest trusted inventory,
// and whether that inventory was complete enough to count.
func (d *Daemon) backendFreeSlots() (int, bool) {
	if d.freeSlots != nil {
		return d.freeSlots()
	}
	if d.store == nil {
		return 0, false
	}
	observation := d.store.Inventory()
	return observation.FreeSlots, observation.Complete
}

// refill starts one worker under the daemon's context, not the pass deadline,
// so shutdown still stops it but the reaper's own budget never does
// (reapPassDeadline says why).
func (d *Daemon) refill(ctx context.Context) (Lease, error) {
	ctx, cancel := context.WithTimeout(ctx, workerStartupTimeout)
	defer cancel()
	return d.lifecycle.ProvisionNext(ctx)
}

func (d *Daemon) clock() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

// defaultDemandDeafAfter is the #1011 window: queued pool jobs with no
// envelope for this long mean the sessions are deaf.
const defaultDemandDeafAfter = 10 * time.Minute

func NewDaemon(store *Store, registry *Registry, sessions *SessionManager, lifecycle *PoolLifecycle, deafAfter time.Duration) (*Daemon, error) {
	if store == nil || registry == nil || sessions == nil || lifecycle == nil {
		return nil, errors.New("daemon requires store, registry, sessions and lifecycle")
	}
	if deafAfter <= 0 {
		return nil, errors.New("daemon requires a positive session-deaf window")
	}
	daemon, err := newDaemon(registry, sessions, lifecycle, store.config.Capacity-store.config.LegacySlots)
	if err != nil {
		return nil, err
	}
	daemon.store = store
	daemon.deafAfter = deafAfter
	daemon.demandDeafAfter = defaultDemandDeafAfter
	return daemon, nil
}

// summarize never fails the cycle. A diagnostic that can turn a healthy cycle
// into a failed one is worse than the silence it replaces, so an unreadable
// count is reported as -1 rather than returned as an error.
func (d *Daemon) summarize(ctx context.Context, polls []PollResult, deferred int) CycleSummary {
	summary := CycleSummary{Charged: -1, ActiveWorkers: -1, Deferred: deferred, Advertised: make([]SetAdvertisement, 0, len(polls))}
	for _, poll := range polls {
		summary.Advertised = append(summary.Advertised, SetAdvertisement{Set: poll.Set, MaxCapacity: poll.MaxCapacity})
		if poll.Envelope != nil {
			summary.Messages++
		}
		switch poll.Outcome {
		case PollMessage:
			summary.Polls.Message++
		case PollRefreshed:
			summary.Polls.Refreshed++
		case PollEmpty:
			summary.Polls.Empty++
		case PollRefused:
			summary.Polls.Refused++
		case PollQuarantined:
			summary.Polls.Quarantined++
		default:
			summary.Polls.Other++
		}
	}
	if d.store == nil {
		return summary
	}
	summary.Ready, summary.Admission = d.store.Admission()
	summary.Management = d.store.ManagementCycle()
	observation := d.store.Inventory()
	summary.FreeSlots, summary.Binding = observation.FreeSlots, observation.Binding
	if !observation.Complete {
		// An incomplete observation has measured no dimension at all, so it
		// must not read as "nothing is short".
		summary.FreeSlots, summary.Binding = -1, "unknown"
	}
	if charged, err := d.store.ChargedSlots(ctx); err == nil {
		summary.Charged = charged
	}
	if active, err := d.store.ActiveWorkerLeases(ctx); err == nil {
		summary.ActiveWorkers = active
	}
	now := time.Now().UTC()
	if open, until, err := d.store.ProvisioningSuppression(ctx, now); err == nil {
		summary.BreakerMeasured, summary.BreakerOpen = true, open
		if open {
			// Zero when the stored deadline does not parse; ReconcileBackendInventory
			// fences on that, and the line says `invalid` rather than a time.
			summary.BreakerUntil, _ = time.Parse(time.RFC3339Nano, until)
		}
	}
	summary.PendingCapacity, summary.PendingLegacySlots, summary.SizingPending = d.store.PendingSizing()
	lastEnvelopeAt, processStarted := d.store.Liveness()
	summary.LastEnvelopeMeasured = true
	if lastEnvelopeAt.IsZero() {
		summary.LastEnvelopeAge = -1
	} else {
		summary.LastEnvelopeAge = now.Sub(lastEnvelopeAt)
	}
	if d.deafAfter > 0 {
		demand := d.store.RESTDemandSignal(now)
		demand.Window = d.demandDeafAfter
		summary.SessionsDeaf = sessionsLookDeaf(lastEnvelopeAt, processStarted, now, d.deafAfter, summary.ActiveWorkers, demand)
		if since, _, judged := silenceClock(lastEnvelopeAt, processStarted, d.deafAfter, demand); judged {
			summary.Silence, summary.SilenceJudged = now.Sub(since), true
		}
		summary.SessionDeafWindow = d.deafAfter
		if demand.Known && demand.Window > 0 {
			summary.SessionDeafWindow = demand.Window
		}
		summary.QueuedDemand, summary.DemandKnown = demand.Queued, demand.Known
		// A fresh envelope ends whatever silent episode the close-connections
		// rung was guarding against, so the latch clears here -- before the
		// rung below is judged -- and a process that goes silent again starts
		// a new episode free to fire the rung again.
		if lastEnvelopeAt.After(d.lastKnownEnvelope) {
			d.rungFired = false
		}
		d.lastKnownEnvelope = lastEnvelopeAt
		summary.Rung = sessionRecoveryRung(lastEnvelopeAt, processStarted, now, d.deafAfter, summary.ActiveWorkers, d.rungFired, demand)
	}
	return summary
}

// maintainLocally is the part of a cycle that needs no GitHub call: the
// trusted inventory, then the worker half of Maintain (deadlines, stranded
// leases, retirements, teardown and release of leases whose registration an
// earlier pass already proved absent).
func (d *Daemon) maintainLocally(ctx context.Context) (int, error) {
	if err := d.lifecycle.ReconcileInventory(ctx); err != nil {
		return 0, err
	}
	return d.lifecycle.MaintainWorkers(ctx)
}

func newDaemon(registry registryCycle, sessions sessionCycle, lifecycle lifecycleCycle, limit int) (*Daemon, error) {
	if registry == nil || sessions == nil || lifecycle == nil || limit < 0 {
		return nil, errors.New("daemon requires complete components and a nonnegative platform limit")
	}
	return &Daemon{registry: registry, sessions: sessions, lifecycle: lifecycle, limit: limit, createConcurrency: 1}, nil
}

// Cycle fails closed at each global boundary. A managed-set failure remains in
// the returned diagnostic while healthy, active sets continue; Registry has
// already fenced the failing repository in durable state. No session API call
// follows incomplete inventory and no acquisition follows a failed full poll.
//
// A cycle that fails before summarize still reports the REST demand signal, so
// the caller's failed-cycle backstop can tell a stalled pool with queued work
// from one with nothing to do (homelab#1211). That signal is read from the
// store and makes no GitHub call.
func (d *Daemon) Cycle(ctx context.Context) (CycleResult, error) {
	result, err := d.cycle(ctx)
	if err != nil && !result.Summary.LastEnvelopeMeasured && d.store != nil {
		demand := d.store.RESTDemandSignal(time.Now().UTC())
		result.Summary.QueuedDemand, result.Summary.DemandKnown = demand.Queued, demand.Known
	}
	return result, err
}

func (d *Daemon) cycle(ctx context.Context) (CycleResult, error) {
	var result CycleResult
	if err := d.acquire(ctx); err != nil {
		return result, err
	}
	held := true
	defer func() {
		if held {
			d.release()
		}
	}()
	// Cleared until this cycle reaches provisioning again; every early return
	// below leaves it false, so Reap stops refilling (homelab#1190).
	d.refillAllowed.Store(false)
	if err := d.registry.Refresh(ctx); err != nil {
		d.discoveryOK.Store(false)
		// DISCOVERY IS GITHUB; TEARDOWN IS NOT (homelab#1096, review D13). A
		// discovery blip used to return here, before the inventory and before
		// Maintain, so a finished worker held its slot, and every deadline
		// waited, for as long as GitHub was unreachable. The worker half of
		// Maintain makes no GitHub call, so it still runs; everything that does
		// -- management, registration cleanup, sessions, polls, provisioning --
		// still stops at this boundary, and the cycle still fails with the
		// discovery error.
		retained, localErr := d.maintainLocally(ctx)
		if localErr == nil {
			result.Summary.Retained, result.Summary.RetainedMeasured = retained, true
		}
		return result, errors.Join(err, localErr)
	}
	d.discoveryOK.Store(true)
	managementErr := d.registry.Reconcile(ctx)
	if err := d.lifecycle.ReconcileInventory(ctx); err != nil {
		return result, errors.Join(managementErr, err)
	}
	retained, err := d.lifecycle.Maintain(ctx)
	d.lastRegistration = d.clock()
	if err != nil {
		return result, errors.Join(managementErr, err)
	}
	// Set here and again after each summarize below, which builds a fresh
	// summary: a cycle that fails after Maintain still reports the count.
	result.Summary.Retained, result.Summary.RetainedMeasured = retained, true
	// After Maintain, which is what drives finished leases to 'released', and
	// before any session or provisioning work, so the drain takes effect without
	// one more job being claimed.
	if d.store != nil {
		drain, err := d.store.DrainForPendingSizing(ctx)
		if err != nil {
			return result, errors.Join(managementErr, err)
		}
		if drain {
			result.RestartForSizing = true
			// The drain returns before PollSweep, so no demand was read and
			// nothing could be deferred this cycle: the count is 0, not unknown.
			result.Summary = d.summarize(ctx, nil, 0)
			result.Summary.Retained, result.Summary.RetainedMeasured = retained, true
			result.Summary.Draining = true
			// A drain restarts the process either way, but a controller that is
			// ALSO deaf must say so: the caller picks the exit code and log line
			// from this flag, and dropping it here would report a deaf restart
			// as a routine sizing adoption with exit 0.
			result.SessionsDeaf = result.Summary.SessionsDeaf
			if result.SessionsDeaf {
				result.Rotation = d.sessions.RotateIdleSessions(ctx)
			}
			return result, managementErr
		}
	}
	// Sync fails the cycle only for a global fault. A set whose session cannot
	// be established is skipped and counted, and the rest are polled.
	if err := d.sessions.Sync(ctx); err != nil {
		result.SessionStageFailed = true
		return result, errors.Join(managementErr, err)
	}
	degraded := d.sessions.Degraded()
	for _, set := range degraded {
		managementErr = errors.Join(managementErr, fmt.Errorf("repository %d scale set %d skipped: %w", set.Set.RepositoryID, set.Set.ScaleSetID, set.Err))
	}
	// The long-poll is the one step Reap may run beside: it does no lifecycle
	// work, and a worker that exits while it waits should not wait with it.
	// Every boundary that stops provisioning (discovery, Maintain, a sizing
	// drain, session sync) has passed by here, so refill is legal for the
	// length of the poll. A deafness verdict after the poll clears it.
	d.refillAllowed.Store(true)
	held = false
	d.release()
	polls, deferred, err := d.sessions.PollSweep(ctx)
	if lockErr := d.acquire(ctx); lockErr != nil {
		d.refillAllowed.Store(false)
		return result, errors.Join(err, lockErr)
	}
	held = true
	// Closed again until the provisioning loop below re-proves it: any return
	// between here and there (a failed sweep, deafness) leaves it false, so the
	// deaf path needs no clear of its own.
	d.refillAllowed.Store(false)
	result.Polled = len(polls)
	result.Summary = d.summarize(ctx, polls, deferred)
	result.Summary.Retained, result.Summary.RetainedMeasured = retained, true
	result.Summary.DegradedSessions = len(degraded)
	for _, set := range degraded {
		result.Summary.DegradedSets = append(result.Summary.DegradedSets, set.Set)
	}
	result.SessionsDeaf = result.Summary.SessionsDeaf
	// ONE FAILED POLL NO LONGER STOPS PROVISIONING FOR EVERY SET. A per-set
	// failure (a 5xx, or an envelope the journal refuses on every redelivery)
	// used to return here, so one repository's trouble paused all of them, and
	// a poison envelope made that permanent. The failed set is named in the
	// joined error and counted; the sets that did poll carry on. A sweep that
	// failed as a whole (no complete session set, no advertisement plan) still
	// stops the cycle: nothing below may act on demand it could not read.
	if onlySetPollErrors(err) {
		result.Summary.PollFailedSets = setPollErrorKeys(err)
		result.Summary.PollFailures = len(result.Summary.PollFailedSets)
		managementErr = errors.Join(managementErr, err)
	} else if err != nil {
		result.SessionStageFailed = true
		return result, errors.Join(managementErr, err)
	}
	if result.Summary.Rung == RungCloseConnections {
		// The non-destructive first rung: rule out stale keep-alive
		// connections (cause B in the 2026-09-23 investigation) before the
		// destructive exit at the full window. Only reached once per silent
		// episode -- d.rungFired latches until an envelope arrives.
		result.ConnectionsClosed = d.sessions.CloseIdleConnections()
		d.rungFired = true
	}
	if result.SessionsDeaf {
		// Deaf sessions outrank provisioning: nothing after this point can
		// observe new demand reliably, so there is no reason to attempt it
		// before the caller restarts the process. The idle sets' sessions are
		// closed first, so that restart opens new ones (homelab#1138).
		result.Rotation = d.sessions.RotateIdleSessions(ctx)
		return result, managementErr
	}
	// A SECOND MAINTENANCE PASS, IN THE CYCLE THAT SAW THE JOB FINISH (#951).
	// JobCompleted is read in PollSweep, after the first Maintain, and moves
	// the lease to cleanup-pending with its worker still present. Without this
	// pass the worker's teardown waited a whole cycle to begin, one of the
	// minutes in the ~3.6 min gap between one job ending and the next worker
	// existing. The pass is MaintainWorkers: the worker half of Maintain, with
	// no registration cleanup, so it makes no GitHub call and the stuck-cleanup
	// backlog is still retried once per cycle, not twice. It is safe to repeat:
	// every transition is guarded on the phase it moves from, and
	// BeginWorkerDestroy selects only create-intent and present workers, so a
	// worker already in destroy-intent is not destroyed again by that path. The
	// redelete loop may re-send its delete in either pass, but each re-send
	// stamps the attempt first and waits a full redeleteBackoff before the next,
	// so one worker is re-sent at most once across the two. It runs only when
	// this sweep delivered a completion.
	//
	// The retained counts are summed, not replaced: a worker retained by the
	// first pass is either in destroy-intent inside its backoff, which neither
	// loop selects, or was just re-sent and stamped, so the second pass cannot
	// count it again.
	if sawCompletion(polls) {
		again, err := d.lifecycle.MaintainWorkers(ctx)
		retained += again
		result.Summary.Retained = retained
		if err != nil {
			return result, errors.Join(managementErr, err)
		}
	}
	// UNSEEN DEMAND, JUST BEFORE PROVISIONING (homelab#1216). A set whose
	// reported demand no envelope has named for unseenDemandAfter gets one probe
	// row, which the provisioning below then serves like any other job. Only
	// sets whose poll answered an empty 202 are candidates. A failure here is
	// reported and provisioning still runs: the probe is a fallback, and the
	// envelope path must not stop because of it.
	if d.store != nil {
		var quiet []SetKey
		for _, poll := range polls {
			if poll.Outcome == PollEmpty {
				quiet = append(quiet, poll.Set)
			}
		}
		probes, probeErr := d.store.ProbeUnseenDemand(ctx, quiet)
		result.Summary.UnseenProbes = len(probes)
		if probeErr != nil {
			managementErr = errors.Join(managementErr, fmt.Errorf("unseen-demand probe: %w", probeErr))
		}
	}
	d.refillAllowed.Store(true)
	provisioned, err := d.provision(d.limit, func() bool { return true }, func() (Lease, error) { return d.lifecycle.ProvisionNext(ctx) })
	result.Provisioned = provisioned
	if err != nil {
		// A provisioning fault the cycle cannot explain stops the reaper
		// too, until a cycle provisions cleanly again.
		d.refillAllowed.Store(false)
		return result, errors.Join(managementErr, err)
	}
	return result, managementErr
}

// setPollErrorKeys lists the sets named by an error onlySetPollErrors accepted.
func setPollErrorKeys(err error) []SetKey {
	if single, ok := err.(*SetPollError); ok {
		return []SetKey{single.Set}
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return nil
	}
	var keys []SetKey
	for _, member := range joined.Unwrap() {
		keys = append(keys, setPollErrorKeys(member)...)
	}
	return keys
}

// sawCompletion reports whether any poll this cycle delivered a JobCompleted
// event, the message that moves a lease to cleanup-pending with its worker
// still present.
func sawCompletion(polls []PollResult) bool {
	for _, poll := range polls {
		if poll.Envelope == nil {
			continue
		}
		for _, event := range poll.Envelope.Events {
			if event.MessageType == scaleset.MessageTypeJobCompleted {
				return true
			}
		}
	}
	return false
}
