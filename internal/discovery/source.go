package discovery

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

// StoreSource composes one report per request from the durable journal and the
// process-local observers that produce what the journal does not hold. It is
// read-only end to end: no GitHub call, no platform action, no journal write.
//
// The report is never cached. Its freshness is the controller's freshness for
// everything except the maintenance loop, which is why cycle is its own section
// with its own producer.
type StoreSource struct {
	store *controller.Store
	cycle *CycleObserver
	// bootstrap reports capability issuance. It is an interface so that the
	// composer depends on counts and not on the broker that holds payloads.
	bootstrap BootstrapCounterSource
	// listener reports whether the bootstrap HTTPS listener is accepting. It is
	// a function so that reading it adds no route, field or byte to that
	// listener; Track C supplies the read-only accessor behind it.
	listener func() bool
	// started is this process's start instant, the origin every cumulative
	// counter and every startup grace in the report is measured from.
	started time.Time
	// interval is the configured cycle interval, published so a check can be
	// audited against the threshold rather than trusting it.
	interval time.Duration
	// threshold is the demand stall threshold.
	threshold time.Duration
	// now is injectable so that a test can place a report at a chosen instant
	// without sleeping. Production leaves it nil and gets time.Now.
	now func() time.Time
}

// BootstrapCounterSource is the counts half of the JIT broker.
type BootstrapCounterSource interface {
	Counters() controller.BootstrapCounters
}

// SourceConfig composes a StoreSource. Every dependency is required: a report
// with a field nothing produces is the fault this whole track exists to remove,
// so the constructor refuses rather than publishing a zero value.
type SourceConfig struct {
	Store     *controller.Store
	Cycle     *CycleObserver
	Bootstrap BootstrapCounterSource
	// ListenerReady reports the bootstrap listener's accepting state.
	ListenerReady func() bool
	// Started is the process start instant.
	Started time.Time
	// Interval is the configured cycle interval.
	Interval time.Duration
	// StallThreshold defaults to DefaultStallThreshold when zero.
	StallThreshold time.Duration
	// Now defaults to time.Now.
	Now func() time.Time
}

func NewStoreSource(config SourceConfig) (*StoreSource, error) {
	if config.Store == nil || config.Cycle == nil || config.Bootstrap == nil || config.ListenerReady == nil {
		return nil, errors.New("discovery source requires a store, cycle observer, bootstrap counters and listener readiness")
	}
	if config.Started.IsZero() || config.Interval <= 0 {
		return nil, errors.New("discovery source requires a process start instant and a positive cycle interval")
	}
	threshold := config.StallThreshold
	if threshold <= 0 {
		threshold = DefaultStallThreshold
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &StoreSource{
		store: config.Store, cycle: config.Cycle, bootstrap: config.Bootstrap,
		listener: config.ListenerReady, started: config.Started.UTC(),
		interval: config.Interval, threshold: threshold, now: now,
	}, nil
}

// startupGrace is how long a fresh controller is legitimately admission-fenced
// and inventory-less before capacity and backend read degraded. Without it the
// top-level status row fires on every restart *before* the admission row that
// explains it, so a routine deploy pages with a message that names nothing — and
// an operator who is paged by every deploy stops reading the row that matters.
func (s *StoreSource) startupGrace() time.Duration { return 3 * s.interval }

// Report composes the whole document. It returns an error rather than a partial
// report; the serving layer turns that error into the 503 skeleton, so a body
// is either fully composed or it is not a 200.
func (s *StoreSource) Report(ctx context.Context) (Report, error) {
	now := s.now().UTC()
	uptime := now.Sub(s.started)
	if uptime < 0 {
		uptime = 0
	}

	report := Report{
		Platform:      s.store.Platform(),
		Generated:     now.Format(time.RFC3339Nano),
		UptimeSeconds: int64(uptime.Seconds()),
		Cycle:         s.cycle.Snapshot(now, uptime, s.interval),
	}

	discoveryHealth, err := s.discoverySection(ctx, now)
	if err != nil {
		return Report{}, err
	}
	report.Discovery = discoveryHealth

	capacity, legacy := s.store.PlatformSlots()
	charged, err := s.store.ChargedSlots(ctx)
	if err != nil {
		return Report{}, err
	}
	inventory := s.store.Inventory()
	ready, _ := s.store.Admission()
	stuck, err := s.store.StuckCharged(ctx, now)
	if err != nil {
		return Report{}, err
	}
	suppressed, until, err := s.store.ProvisioningSuppression(ctx, now)
	if err != nil {
		return Report{}, err
	}
	poolSlots := capacity - legacy
	free := poolSlots - charged
	if free < 0 {
		free = 0
	}
	report.Capacity = s.capacitySection(capacitySection{
		capacity: capacity, legacy: legacy, poolSlots: poolSlots, charged: charged, free: free,
		stuck: stuck, inventory: inventory, ready: ready, suppressed: suppressed,
		suppressedUntil: until, uptime: uptime,
	})

	phases, err := s.store.LeasePhaseCounts(ctx)
	if err != nil {
		return Report{}, err
	}
	report.Leases = phaseCounts(phases)

	demand, err := s.store.QueuedDemand(ctx)
	if err != nil {
		return Report{}, err
	}
	oldest, err := parseInstant(demand.OldestOffer)
	if err != nil {
		return Report{}, err
	}
	report.Demand = EvaluateStall(now, StallInputs{
		QueuedOffers: demand.Queued, FreeSlots: free,
		TrustedHeadroom: report.Capacity.TrustedHeadroom, OldestOffer: oldest, Threshold: s.threshold,
	})

	workers, err := s.store.WorkerCounts(ctx, now)
	if err != nil {
		return Report{}, err
	}
	report.Workers = workerSection(workers, inventory, s.store.RetiredUnused())
	report.Backend = s.backendSection(inventory, now, uptime)
	report.Bootstrap = bootstrapSection(s.bootstrap.Counters(), s.listener())

	repositories, err := s.repositorySection(ctx, demand)
	if err != nil {
		return Report{}, err
	}
	report.Repositories = repositories
	for _, repo := range repositories {
		report.RepositoriesTotal++
		if repo.Eligible {
			report.RepositoriesEligible++
		}
		if repo.Status == Degraded {
			report.RepositoriesDegraded++
		}
	}

	// PolicyDoc has no producer in this track. Track D loads the document and
	// fills this section; until then it reports the shipped posture — an
	// allowlist — and a degraded verdict, because a report that claimed a policy
	// was healthy before anything loaded one would be asserting a fact nothing
	// established. That is the rule this whole track exists to enforce.
	report.PolicyDoc = PolicyHealth{Status: Degraded, Degraded: true, Diagnostic: DiagnosticUnavailable}

	report.Status, report.Reasons = rollup(report)
	return report, nil
}

// rollup is B4: Status is the worst section verdict and Reasons names every
// section that is not healthy, so one Gatus condition on status catches anything
// and a reader learns which section pulled it down without reading the sections.
// Section verdicts are healthy or degraded only; unknown belongs to the 503
// skeleton and never appears in a composed body.
func rollup(report Report) (Verdict, []string) {
	sections := []struct {
		name    string
		verdict Verdict
	}{
		{"cycle", report.Cycle.Status},
		{"discovery", report.Discovery.Status},
		{"demand", report.Demand.Status},
		{"capacity", report.Capacity.Status},
		{"workers", report.Workers.Status},
		{"backend", report.Backend.Status},
		{"bootstrap", report.Bootstrap.Status},
		{"policy", report.PolicyDoc.Status},
	}
	status := Healthy
	var reasons []string
	for _, section := range sections {
		if section.verdict != Healthy {
			status = Degraded
			reasons = append(reasons, section.name)
		}
	}
	if report.RepositoriesDegraded > 0 {
		status = Degraded
		reasons = append(reasons, "repositories")
	}
	return status, reasons
}

func (s *StoreSource) discoverySection(ctx context.Context, now time.Time) (DiscoveryHealth, error) {
	status, err := s.store.DiscoveryStatus(ctx)
	if err != nil {
		return DiscoveryHealth{}, err
	}
	health := DiscoveryHealth{
		LastSuccess:      status.LastSuccess,
		LastAttempt:      status.LastAttempt,
		AgeSeconds:       -1,
		FreshnessSeconds: int64(controller.DiscoveryFreshness.Seconds()),
		Diagnostic:       RedactStored(status.Diagnostic),
	}
	// This is Store.discoveryCurrent's predicate, not a second opinion about the
	// same data: no parseable instant, a negative age, an age at or beyond the
	// freshness window, or any diagnostic. A monitoring section that disagreed
	// with the scheduler's own fence would be worse than no section.
	observed, parseErr := time.Parse(time.RFC3339Nano, status.LastSuccess)
	age := now.Sub(observed)
	health.Stale = parseErr != nil || age < 0 || age >= controller.DiscoveryFreshness || status.Diagnostic != ""
	if parseErr == nil && age >= 0 {
		health.AgeSeconds = int64(age.Seconds())
	}
	health.Status = Healthy
	if health.Stale {
		health.Status = Degraded
	}
	return health, nil
}

type capacitySection struct {
	capacity, legacy, poolSlots, charged, free, stuck int
	inventory                                         controller.InventoryObservation
	ready, suppressed                                 bool
	suppressedUntil                                   string
	uptime                                            time.Duration
}

func (s *StoreSource) capacitySection(in capacitySection) CapacityHealth {
	health := CapacityHealth{
		Capacity: in.capacity, LegacySlots: in.legacy, PoolSlots: in.poolSlots,
		Charged: in.charged, Free: in.free, StuckCharged: in.stuck,
		TrustedHeadroom: in.inventory.FreeSlots, Admission: "fenced",
		ProvisioningSuppressed: in.suppressed, SuppressedUntil: in.suppressedUntil,
	}
	if !in.inventory.Complete {
		health.TrustedHeadroom = 0
	}
	if in.ready {
		health.Admission = "open"
	} else {
		health.AdmissionReason = RedactInventory(in.inventory.Diagnostic)
		if health.AdmissionReason == "" {
			health.AdmissionReason = DiagnosticInventoryUnknown
		}
	}
	health.Status = Healthy
	fencedPastGrace := health.Admission != "open" && in.uptime > s.startupGrace()
	if in.stuck > 0 || in.suppressed || fencedPastGrace {
		health.Status = Degraded
	}
	return health
}

func (s *StoreSource) backendSection(inventory controller.InventoryObservation, now time.Time, uptime time.Duration) BackendHealth {
	health := BackendHealth{
		Complete: inventory.Complete, ObservedAt: inventory.ObservedAt, AgeSeconds: -1,
		FreshnessSeconds: int64(s.startupGrace().Seconds()), FreeSlots: inventory.FreeSlots,
		Diagnostic: RedactInventory(inventory.Diagnostic),
	}
	if !inventory.Complete {
		health.FreeSlots = 0
	}
	observed, parseErr := time.Parse(time.RFC3339Nano, inventory.ObservedAt)
	switch {
	case inventory.ObservedAt == "" || parseErr != nil:
		// No observation in this process. That is the ordinary first moment of a
		// fresh controller and is stale only once the startup grace has passed.
		health.Stale = uptime > s.startupGrace()
	default:
		age := now.Sub(observed)
		if age < 0 {
			age = 0
		}
		health.AgeSeconds = int64(age.Seconds())
		health.Stale = age > s.startupGrace()
	}
	health.Status = Healthy
	// The startup grace covers completeness too: a controller legitimately has
	// no inventory until its first cycle, and degrading from the first second
	// fires the top-level row before the row that explains it.
	if uptime > s.startupGrace() && (!health.Complete || health.Stale) {
		health.Status = Degraded
	}
	return health
}

func workerSection(counts controller.WorkerCounts, inventory controller.InventoryObservation, retired int) WorkerHealth {
	health := WorkerHealth{
		Tracked: counts.Tracked, Booting: counts.Booting, Running: counts.Running,
		Destroying: counts.Destroying, OverdueStartup: counts.OverdueStartup,
		OverdueCleanup: counts.OverdueCleanup, UntrackedSeen: inventory.UntrackedSeen,
		RetiredUnused: retired,
	}
	health.Status = Healthy
	if health.UntrackedSeen || health.OverdueStartup > 0 || health.OverdueCleanup > 0 {
		health.Status = Degraded
	}
	return health
}

func bootstrapSection(counters controller.BootstrapCounters, ready bool) BootstrapHealth {
	health := BootstrapHealth{
		ListenerReady: ready, Outstanding: counters.Outstanding,
		Delivered: counters.Delivered, Expired: counters.Expired, Failed: counters.Failed,
	}
	health.Status = Healthy
	if !ready {
		health.Status = Degraded
	}
	return health
}

func phaseCounts(counts map[controller.Phase]int) PhaseCounts {
	return PhaseCounts{
		Reserved:           counts[controller.Reserved],
		AcquisitionIntent:  counts[controller.AcquisitionIntent],
		AcquisitionUnknown: counts[controller.AcquisitionUnknown],
		Acquired:           counts[controller.Acquired],
		Provisioning:       counts[controller.Provisioning],
		Running:            counts[controller.Running],
		CleanupPending:     counts[controller.CleanupPending],
		Quarantined:        counts[controller.Quarantined],
	}
}

// degradedScaleSetPhases are the scale-set phases that count toward
// RepositoriesDegraded. The transient onboarding path — planned, create-intent,
// created — is the normal first minute of a newly granted repository and is
// excluded, so granting the App never pages anyone.
func degradedScaleSet(phase, diagnostic string) bool {
	return phase == "unverified" || diagnostic != ""
}

func (s *StoreSource) repositorySection(ctx context.Context, demand controller.Demand) ([]RepositoryHealth, error) {
	repos, err := s.store.Repositories(ctx)
	if err != nil {
		return nil, err
	}
	charged, err := s.store.ChargedByRepository(ctx)
	if err != nil {
		return nil, err
	}
	assigned, err := s.store.AssignedJobs(ctx)
	if err != nil {
		return nil, err
	}
	sessions, err := s.store.OpenSessionRepositories(ctx)
	if err != nil {
		return nil, err
	}
	label := "slipway-" + s.store.Platform()
	rows := make([]RepositoryHealth, 0, len(repos))
	for _, repo := range repos {
		id := repo.Repository.ID
		row := RepositoryHealth{
			Status:     Healthy,
			FullName:   repo.Repository.FullName,
			Eligible:   repo.Eligible,
			Discovered: repo.Discovered,
			ScaleSet: ScaleSetHealth{
				Phase: repo.Phase, Label: label, Observed: repo.Observed,
				Diagnostic: RedactStored(repo.Diagnostic),
			},
			// Policy is Track D's producer. Until it lands, the resolved value
			// is the shipped default — an allowlist, scheduling nothing — and it
			// says so through its sources rather than implying an entry exists.
			Policy: Resolved{
				Platforms: []string{s.store.Platform()}, EnabledSource: SourceDefault,
				MaxConcurrentSource: SourceDefault, PlatformsSource: SourceDefault,
			},
			ChargedSlots: charged[id],
			AssignedJobs: assigned[id],
			QueuedOffers: demand.ByRepo[id],
			SessionOpen:  sessions[id],
			Diagnostic:   RedactStored(repo.Diagnostic),
		}
		if degradedScaleSet(repo.Phase, repo.Diagnostic) {
			row.Status = Degraded
		}
		rows = append(rows, row)
	}
	// Ordered by full name so two consecutive reports diff cleanly.
	sort.Slice(rows, func(i, j int) bool { return rows[i].FullName < rows[j].FullName })
	return rows, nil
}

// parseInstant reads a durable RFC3339Nano field. An empty field is a legitimate
// absence — a pre-version-11 offer has none — and is not an error; an unparseable
// one is, because composing a report around a timestamp nobody can read would
// publish a wait the controller never measured.
func parseInstant(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}
