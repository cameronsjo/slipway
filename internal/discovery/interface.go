// Package discovery defines the pool's read-only self-description: one JSON
// document that says which repositories the App installation granted, what each
// one's scale set is doing, where every charged slot went, and whether the
// platform backend can be trusted right now.
//
// It exists so that no human maintains a list of repositories anywhere else.
// The App installation is the source of truth; Registry.Refresh commits a
// complete snapshot every minute; this package publishes that snapshot, plus
// the lease, worker, quarantine and backend state hanging off it, in the shape
// the estate's monitoring already reads. Granting the App on a new repository
// is the whole of onboarding: the report grows a row with no config edit, no
// manifest, and no deploy.
//
// This package also owns the status listener that serves the report. The
// listener lives here and not in internal/bootstrap, so that no edit to the
// status surface can reach the bootstrap one by accident.
//
// # Boundary
//
// Nothing here goes on the bootstrap listener. That listener is fixed by
// internal/bootstrap/CONTRACT.md to serve JITBroker and nothing else — "without
// a general controller mux, health route, debug route, static files, redirects,
// or proxy" — and it is the one endpoint a worker VM can reach. A status route
// added there would put pool-wide state behind a per-lease capability on the
// only surface untrusted workers can dial. The report is served by a separate
// listener, on a separate port, with its own read-only bearer credential, and
// the worker network policy must not reach it.
//
// That bearer is the only control in front of the report. The MS-A2 Traefik
// tier runs no forward-auth (repo CLAUDE.md § Ingress Security Model), so the
// route is reachable by every LAN device and every tailnet node and is as
// authenticated as this package makes it. The body is therefore kept to what
// Gatus needs: repository full names, states and counts, never repository IDs,
// visibility, credentials, or raw storage text.
//
// # No credential is ever a field
//
// Every type here is derived from durable journal state that the controller
// already treats as safe to log. Bootstrap capabilities, JIT payloads, session
// queue URLs, installation tokens and App keys are counted, never named.
// Diagnostics are drawn from a fixed vocabulary the composer owns and are
// redacted the way githubapp.SafeError redacts, so no upstream error text and
// no raw SQLite message reaches the body.
//
// # Shaped for Gatus, not for a human reader
//
// The estate has no Prometheus. Gatus asserts a status code and JSON body
// fields, and .claude/rules/gatus-checks.md records two hard limits it has paid
// for: Gatus cannot do timestamp arithmetic, and it cannot count, filter or
// safely index an array. So:
//
//   - Every alertable fact is a top-level scalar or a named struct field.
//     Repositories carries the per-repository detail for a human following a
//     runbook; a Gatus condition must never index into it.
//   - Ages are computed here and published both as a number of seconds and as
//     the boolean the controller already decided (Stale, Stalled, Overdue),
//     because the shim, not the check, is what knows the threshold. Each such
//     threshold is published beside its boolean so the check is auditable.
//   - A report the controller cannot fully compose is not emitted with gaps.
//     The serving layer answers 503 and the body carries Status Unknown with
//     the reason, matching the fail-closed rule the ci-freshness shim follows.
//
// # Unknown never appears in a 200 body
//
// Section verdicts in a 200 response are Healthy or Degraded only. Unknown is
// reserved for the 503 skeleton, where it means the composer could not
// establish the facts at all. This keeps one rule for a reader and one rule for
// a check: a 200 body is fully composed, and anything less is a 503.
//
// A 503 emits the complete section skeleton with every boolean at its
// fail-closed value — Stale and Stalled true, ListenerReady and Complete false,
// UntrackedSeen true, counts zero — so the seven diagnostic Gatus checks read
// the same verdict as the top-level one instead of each firing its own alert on
// a single outage.
//
// # Counters
//
// Delivered, Expired, Failed and RetiredUnused are cumulative since process
// start, not since a window. Read them against Report.UptimeSeconds: a restart
// resets them, and a rate is the difference between two reports.
package discovery

import "context"

// Verdict is the health word used at every level of the report. Unknown is not
// a milder Degraded: it means the composer could not establish the fact, and it
// appears only in a 503 body.
type Verdict string

const (
	// Healthy means every fact at this level was established and is within its
	// expected bound.
	Healthy Verdict = "healthy"
	// Degraded means a fact was established and is outside its bound.
	Degraded Verdict = "degraded"
	// Unknown means the facts could not be established. It accompanies a 503
	// and never appears in a 200 body.
	Unknown Verdict = "unknown"
)

// Policy resolution sources, published per field so a reader can tell an
// inherited value from an explicit one without reading the policy file.
const (
	SourceDefault    = "default"
	SourceRepository = "repository"
)

// Source produces one complete report. Implementations read the durable journal
// and the last trusted backend observation; they perform no GitHub API call and
// no platform mutation, so a monitoring probe can never change pool state or
// consume rate limit. A Source returns an error rather than a partial report,
// and the serving layer turns that error into the 503 skeleton.
type Source interface {
	Report(context.Context) (Report, error)
}

// Report is the whole document. Status is the worst verdict among the sections
// below, so a single Gatus condition on it catches anything, and Reasons says
// which section pulled it down without the check having to read the sections.
type Report struct {
	Status  Verdict  `json:"status"`
	Reasons []string `json:"reasons,omitempty"`

	// Platform is "linux" or "macos". One controller serves exactly one
	// platform, so this also identifies which report a probe reached.
	Platform string `json:"platform"`
	// Generated is RFC3339Nano, matching the journal's timestamp format.
	Generated string `json:"generated"`
	// UptimeSeconds is this process's age. Every cumulative counter in this
	// report is measured from the same instant, and several fences are
	// legitimately closed until the first cycle completes, so a check that
	// reads a counter or a fence reads this too.
	UptimeSeconds int64 `json:"uptime_seconds"`

	Cycle     CycleHealth     `json:"cycle"`
	Discovery DiscoveryHealth `json:"discovery"`
	Demand    DemandHealth    `json:"demand"`
	Capacity  CapacityHealth  `json:"capacity"`
	Leases    PhaseCounts     `json:"leases"`
	Workers   WorkerHealth    `json:"workers"`
	Backend   BackendHealth   `json:"backend"`
	Bootstrap BootstrapHealth `json:"bootstrap"`
	PolicyDoc PolicyHealth    `json:"policy"`

	// RepositoriesTotal, RepositoriesEligible and RepositoriesDegraded are the
	// rollups a Gatus condition uses. They exist because Gatus cannot count
	// Repositories itself (homelab#407: three repos on one healthy fleet split
	// green/green/red on array order alone).
	//
	// RepositoriesDegraded counts only repositories whose scale set is in a
	// terminal-bad state: an "unverified" phase, or any non-empty diagnostic.
	// The transient onboarding phases — "planned", "create-intent", "created" —
	// are the normal first minute of a newly granted repository and are
	// excluded, so granting the App does not page anyone.
	RepositoriesTotal    int `json:"repositories_total"`
	RepositoriesEligible int `json:"repositories_eligible"`
	RepositoriesDegraded int `json:"repositories_degraded"`

	// Repositories is the per-repository detail, ordered by full name so two
	// consecutive reports diff cleanly. It is for a human following a runbook.
	// It is not a Gatus condition target.
	Repositories []RepositoryHealth `json:"repositories"`
}

// CycleHealth reports whether the maintenance loop is still turning. It is the
// section that catches the failure this endpoint could otherwise create: the
// HTTP server is its own goroutine, so a wedged or crash-looping cycle keeps
// answering 200 with a report that quietly stops changing. Serving a value
// nobody checks the freshness of is the "green badge that lies" the Gatus rules
// warn about, so freshness is a published field and not an assumption.
//
// The producer is a process-local observer the daemon runtime updates after
// each cycle; nothing about the loop is journaled, so these values reset on
// restart and must be read against Report.UptimeSeconds.
type CycleHealth struct {
	Status Verdict `json:"status"`
	// LastCompleted is RFC3339Nano of the last cycle that finished, whether or
	// not it returned an error. Empty before the first one completes.
	LastCompleted string `json:"last_completed"`
	// AgeSeconds is the age of LastCompleted, -1 when there has been none.
	AgeSeconds int64 `json:"age_seconds"`
	// IntervalSeconds is the configured cycle interval, published so the check
	// can be audited against the threshold rather than trusting it.
	IntervalSeconds int64 `json:"interval_seconds"`
	// Stale is true when the last completed cycle is older than three
	// intervals, and also when AgeSeconds is -1 and UptimeSeconds already
	// exceeds one interval — a process that has never completed a cycle is
	// exactly the wedged case, not an exempt one. Only the first interval of a
	// fresh process reads not-stale with no cycle behind it.
	Stale bool `json:"stale"`
	// ConsecutiveFailures counts cycles that returned an error in a row. A
	// single failure is ordinary and is retried on the next tick; a rising
	// count is the controller failing to make progress.
	ConsecutiveFailures int `json:"consecutive_failures"`
	// Diagnostic is the last cycle's redacted failure string, empty when the
	// last cycle succeeded.
	Diagnostic string `json:"diagnostic,omitempty"`
}

// DiscoveryHealth describes the App-installation snapshot that every other
// section depends on. A stale snapshot fences new work, so this section going
// bad is the pool declining to schedule, not the pool being down.
type DiscoveryHealth struct {
	Status Verdict `json:"status"`
	// LastSuccess and LastAttempt are RFC3339Nano, empty when never observed.
	LastSuccess string `json:"last_success"`
	LastAttempt string `json:"last_attempt"`
	// AgeSeconds is the age of LastSuccess, -1 when there has been none.
	AgeSeconds int64 `json:"age_seconds"`
	// FreshnessSeconds is the window admission requires, published beside the
	// boolean it produces.
	FreshnessSeconds int64 `json:"freshness_seconds"`
	// Stale is exactly the condition Store.discoveryCurrent fences on: no
	// parseable LastSuccess, a negative age, an age at or beyond the freshness
	// window, or a non-empty diagnostic. It is the same predicate the scheduler
	// uses, not a second opinion about the same data.
	Stale bool `json:"stale"`
	// Diagnostic is the Registry's fixed failure string, empty when healthy.
	Diagnostic string `json:"diagnostic,omitempty"`
}

// DemandHealth separates the pool's two zero states, which look identical from
// outside and mean opposite things. Zero workers with zero queued demand is the
// ordinary idle state of an ephemeral pool and must never alert. Zero workers
// with queued demand and free capacity is an outage.
//
// Stalled is that distinction, computed here because Gatus can express neither
// the conjunction nor the elapsed time. Nothing else in this report can stand
// in for it: leases, workers and capacity all read the same in both states.
type DemandHealth struct {
	Status Verdict `json:"status"`
	// QueuedOffers counts offered job requests not yet acquired, across every
	// repository. Zero is the ordinary idle value.
	QueuedOffers int `json:"queued_offers"`
	// OldestOfferSeconds is how long the oldest unacquired offer has waited,
	// -1 when there are none. It is derived from the offer's own durable
	// timestamp, so it survives restart.
	OldestOfferSeconds int64 `json:"oldest_offer_seconds"`
	// StallThresholdSeconds is the wait past which an unserved offer counts as
	// stalled. Default 300. Published so the check is auditable and so the
	// threshold can move without rewriting the Gatus config.
	StallThresholdSeconds int64 `json:"stall_threshold_seconds"`
	// Stalled is true only when all four terms hold: there is queued demand,
	// the pool has a free slot, the backend reports trusted headroom to serve
	// it, and OldestOfferSeconds exceeds StallThresholdSeconds. With no free
	// slot the queue is working as designed, and without trusted headroom the
	// backend fence is the real fault and is reported there.
	Stalled bool `json:"stalled"`
	// StalledBlockedBy names the first term that was false, so a reader learns
	// why the pool is not considered stalled without re-deriving it:
	// "no-demand", "no-free-slot", "no-headroom", "within-threshold", or ""
	// when Stalled is true.
	StalledBlockedBy string `json:"stalled_blocked_by,omitempty"`
	// StalledSeconds is how far past the threshold the oldest offer has waited
	// (OldestOfferSeconds minus StallThresholdSeconds), 0 when not stalled. It
	// is derived from that durable timestamp, not from a process-local run
	// length, so a controller restart does not reset a real stall to zero.
	StalledSeconds int64 `json:"stalled_seconds"`
}

// CapacityHealth accounts for every slot: the ceiling in force, what the legacy
// fleet still reserves, what is charged, and what remains. Charged plus Free
// plus LegacySlots equals Capacity at all times; a slot held by an uncertain
// operation is charged, because the pool refuses to free a slot on a timeout.
type CapacityHealth struct {
	Status Verdict `json:"status"`
	// Capacity is the platform qualification ceiling currently configured
	// (Linux 16, macOS 2 at their ceilings).
	Capacity int `json:"capacity"`
	// LegacySlots are held back for the pre-pool fleet and never scheduled.
	LegacySlots int `json:"legacy_slots"`
	// PoolSlots is Capacity minus LegacySlots: what this controller may use.
	PoolSlots int `json:"pool_slots"`
	// Charged counts slots held by a lease in any non-released phase.
	Charged int `json:"charged"`
	// Free counts pool slots with no lease on them.
	Free int `json:"free"`
	// StuckCharged counts slots held by a lease that has sat in "quarantined"
	// or "acquire-unknown" for longer than the fifteen-minute provisioning
	// failure window. These never self-resolve — the pool refuses to free a
	// slot without independent evidence — so they are capacity permanently
	// lost until an operator reconciles them, and they are invisible in
	// Charged, which a busy healthy pool also raises.
	StuckCharged int `json:"stuck_charged"`
	// TrustedHeadroom is the backend's own answer for how many more workers fit
	// current CPU, memory and disk. It can be lower than Free, and the lower of
	// the two is what actually admits work.
	TrustedHeadroom int `json:"trusted_headroom"`
	// Admission is "open" or "fenced". Fenced is the normal state before the
	// first successful inventory of a process, so pair it with
	// Report.UptimeSeconds; the Gatus row for this field carries a threshold
	// high enough that a restart cannot page before the top-level status row.
	Admission string `json:"admission"`
	// AdmissionReason names the fence, empty when open.
	AdmissionReason string `json:"admission_reason,omitempty"`
	// ProvisioningSuppressed is true while repeated provisioning failures
	// suppress new reservations. SuppressedUntil is RFC3339Nano, empty when
	// not suppressed. This is the pool-wide backoff, distinct from an
	// individual lease's quarantined phase in PhaseCounts.
	ProvisioningSuppressed bool   `json:"provisioning_suppressed"`
	SuppressedUntil        string `json:"suppressed_until,omitempty"`
}

// PhaseCounts is the lease population by durable phase. It is a struct and not
// a map so that a phase that drops to zero still appears in the body: a Gatus
// condition on an absent JSON field cannot be told from a condition on a field
// that is zero.
//
// The JSON tags are the controller.Phase string values verbatim, so this
// section can be read against the journal with no translation table. Two of
// them contain a hyphen, which is why these are diagnostic fields and not Gatus
// condition targets — every alertable fact derived from them is published as a
// scalar elsewhere in the report.
type PhaseCounts struct {
	Reserved           int `json:"reserved"`
	AcquisitionIntent  int `json:"acquire-intent"`
	AcquisitionUnknown int `json:"acquire-unknown"`
	Acquired           int `json:"acquired"`
	Provisioning       int `json:"provisioning"`
	Running            int `json:"running"`
	CleanupPending     int `json:"cleanup-pending"`
	Quarantined        int `json:"quarantined"`
}

// WorkerHealth counts the worker resources the controller believes it owns and,
// separately, the ones that have outlived a durable deadline. Zero workers is
// the ordinary idle state of this pool and must never read as an outage — the
// mistake .claude/rules/gatus-checks.md records for the ephemeral fleet. What
// is alertable is a worker stuck past its deadline, or a resource nobody owns.
type WorkerHealth struct {
	Status Verdict `json:"status"`
	// Tracked is the number of durable worker records in any phase.
	Tracked int `json:"tracked"`
	// Booting is created but has emitted no job-start event yet.
	Booting int `json:"booting"`
	// Running has started a job.
	Running int `json:"running"`
	// Destroying has a delete intent recorded and no proof of absence yet.
	Destroying int `json:"destroying"`
	// OverdueStartup passed its ten-minute startup deadline. OverdueCleanup
	// passed its two-minute cleanup deadline. Both are computed here; Gatus
	// cannot compare the deadline to now.
	OverdueStartup int `json:"overdue_startup"`
	OverdueCleanup int `json:"overdue_cleanup"`
	// UntrackedSeen is true when the last inventory reconciliation refused
	// because it found a resource with no lease behind it. It is a boolean and
	// not a count on purpose: ReconcileBackendInventory returns on the first
	// such resource, so a count would be "one" for any number of them and would
	// invite a check that reads a magnitude the controller never measured. Any
	// true value fences admission for the whole controller and never
	// self-resolves.
	UntrackedSeen bool `json:"untracked_seen"`
	// RetiredUnused counts workers retired for emitting no job start within
	// five minutes, cumulative since process start (see § Counters). Sustained
	// growth means workers are booting and never picking up work.
	RetiredUnused int `json:"retired_unused"`
}

// BackendHealth reports the last trusted platform observation. Complete is the
// load-bearing field: an incomplete inventory is not a smaller inventory, it is
// no inventory, and the controller refuses to schedule on one.
//
// ReconcileBackendInventory persists none of this, so the producer is a
// process-local record of the last observation the composer reads. It resets on
// restart, which is correct: the admission fence resets on restart too.
type BackendHealth struct {
	Status Verdict `json:"status"`
	// Complete is the adapter's own claim that it listed its entire owned scope,
	// processes and writable storage both.
	Complete bool `json:"complete"`
	// ObservedAt is RFC3339Nano of the last inventory, empty when there has been
	// none in this process. AgeSeconds is -1 in that case.
	ObservedAt string `json:"observed_at"`
	AgeSeconds int64  `json:"age_seconds"`
	// FreshnessSeconds is the window past which an inventory is not worth
	// scheduling against, published beside the boolean it produces.
	FreshnessSeconds int64 `json:"freshness_seconds"`
	// Stale is true when AgeSeconds exceeds FreshnessSeconds, and also when
	// AgeSeconds is -1 and UptimeSeconds already exceeds one cycle interval.
	Stale bool `json:"stale"`
	// FreeSlots is the adapter's reported headroom (macOS: capped by -max-owned-vms).
	FreeSlots int `json:"free_slots"`
	// Diagnostic is a redacted controller string, empty when healthy.
	Diagnostic string `json:"diagnostic,omitempty"`
}

// BootstrapHealth reports capability issuance as counts only. It never names a
// lease, a capability, a runner name or a JIT payload: the whole point of the
// bootstrap contract is that those values do not leave the trusted process, and
// a monitoring body is outside it.
type BootstrapHealth struct {
	Status Verdict `json:"status"`
	// ListenerReady is whether the bootstrap HTTPS listener is accepting,
	// read through that server's own read-only accessor. A controller whose
	// listener is down can still discover and poll, so this is its own field
	// rather than a reason to call the whole report degraded.
	ListenerReady bool `json:"listener_ready"`
	// Outstanding capabilities are issued, unexpired and unconsumed.
	Outstanding int `json:"outstanding"`
	// Delivered, Expired and Failed are cumulative since process start (see
	// § Counters). Delivered means bytes were written, which is transport
	// progress and not proof that the runner executed.
	Delivered int `json:"delivered"`
	Expired   int `json:"expired"`
	Failed    int `json:"failed"`
}

// PolicyHealth describes the loaded policy document. Digest and LoadedAt make
// the posture auditable from the report: a policy that silently reverted to an
// older copy, or one that never reloaded after a deploy, is visible here and
// nowhere else.
type PolicyHealth struct {
	Status Verdict `json:"status"`
	// Digest is the hex SHA-256 of the exact bytes in force. Comparing it with
	// the digest of the committed file answers "is the running policy the one
	// in git" without exposing the contents.
	Digest string `json:"digest"`
	// LoadedAt is RFC3339Nano of the load that produced Digest.
	LoadedAt string `json:"loaded_at"`
	// Degraded is true when the last reload failed and the previous good
	// document is still in force. The controller refuses to start at all when
	// the policy is unreadable or unparseable on the first load, so this can
	// only describe a later reload.
	Degraded bool `json:"degraded"`
	// DefaultEnabled publishes the posture: false is an allowlist, true is
	// serve-everything-discovered. It is here so a reader never has to guess
	// which one is live.
	DefaultEnabled bool `json:"default_enabled"`
	// Entries is how many repositories carry an explicit entry. UnknownEntries
	// is how many of those name a repository the installation never granted,
	// which almost always means the App grant was forgotten.
	Entries        int `json:"entries"`
	UnknownEntries int `json:"unknown_entries"`
	// Diagnostic is a redacted string, empty when healthy. It never quotes the
	// policy document.
	Diagnostic string `json:"diagnostic,omitempty"`
}

// RepositoryHealth is one row of the discovered installation. Every field comes
// from the App snapshot or the controller's own ownership record; nothing here
// is configured by hand, which is what makes granting the App the whole of
// onboarding.
//
// The row carries no repository ID and no visibility flag. Both were in an
// earlier draft and neither is needed by any check, and this body sits behind a
// bearer token on an unauthenticated Traefik tier — so the row is the smallest
// thing that still lets an operator act on it.
type RepositoryHealth struct {
	Status Verdict `json:"status"`
	// FullName is owner/repository as GitHub reported it. It is the identity an
	// operator needs and the only one published.
	FullName string `json:"full_name"`
	// Eligible is false for an archived, disabled or since-removed repository.
	// An ineligible repository receives no new reservation but keeps its
	// existing leases and cleanup obligations, so it can be busy and ineligible
	// at the same time.
	Eligible bool `json:"eligible"`
	// Discovered is RFC3339Nano of the snapshot that first carried this row.
	Discovered string `json:"discovered"`

	ScaleSet ScaleSetHealth `json:"scale_set"`
	// Policy is the effective policy, with inheritance already applied, so a
	// reader never replays the resolution to know what is in force.
	Policy Resolved `json:"policy"`

	// ChargedSlots is this repository's share of CapacityHealth.Charged.
	ChargedSlots int `json:"charged_slots"`
	// AssignedJobs is the scale set's last reported assigned-job count. It is
	// GitHub's aggregate statistic, not a count of this pool's leases, and the
	// two legitimately disagree while work is in flight.
	AssignedJobs int `json:"assigned_jobs"`
	// QueuedOffers is this repository's share of DemandHealth.QueuedOffers.
	QueuedOffers int `json:"queued_offers"`
	// SessionOpen is whether a verified message session currently exists. A
	// repository with no session receives no offers.
	SessionOpen bool `json:"session_open"`
	// Diagnostic is the repository-scoped failure string, empty when healthy.
	// A failure here fences this repository only; the rest keep scheduling.
	Diagnostic string `json:"diagnostic,omitempty"`
}

// ScaleSetHealth is the controller's ownership record for one repository's
// scale set. Phase distinguishes an unverified set from an absent one, which
// matters because the registry refuses to adopt a set it did not create and
// refuses to repeat a POST whose outcome it does not know.
type ScaleSetHealth struct {
	// Phase is one of planned, create-intent, created, active, unverified. The
	// first three are the transient onboarding path and do not count toward
	// Report.RepositoriesDegraded; unverified does.
	Phase string `json:"phase"`
	// Label is the stable workflow selector a repository's jobs must request
	// (slipway-linux or slipway-macos). It is the only pool identifier a workflow file
	// names, and it does not vary per repository.
	Label string `json:"label"`
	// Observed is RFC3339Nano of the last ownership readback.
	Observed string `json:"observed"`
	// Diagnostic is a redacted controller string, empty when healthy.
	Diagnostic string `json:"diagnostic,omitempty"`
}

// Policy is the operator's overlay on discovery, and the only hand-maintained
// file in the design. Discovery decides which repositories exist; policy
// decides what the pool will do for them. Absence of a repository from the file
// is never a discovery question: an unlisted repository is still discovered,
// still reported and still has its scale set reconciled. Default decides only
// whether it is scheduled.
//
// Default.Enabled is false during the qualification pilots, which makes the
// file an allowlist and scheduling opt-in. That is deliberate and temporary —
// an allowlist is the hand-maintained list this whole design removes — and it
// flips to true after the acceptance audit in homelab#728. Nothing else in the
// type changes at that flip; the entries that existed only to enable a
// repository are deleted and the rest stay.
//
// The document is a git-tracked ConfigMap mounted at a path the runtime config
// names explicitly. It has no default path: a controller with no policy
// configured does not start. A policy that is unreadable or unparseable at
// startup is a startup failure, not a fallback to Default — failing closed here
// must not depend on which posture Default happens to carry, since after the
// flip an empty-policy fallback would schedule everything. A later reload that
// fails keeps the previous good document and sets PolicyHealth.Degraded.
type Policy struct {
	// Default applies to every discovered repository without its own entry.
	Default RepositoryPolicy `json:"default"`
	// Repositories is keyed by owner/repository. An entry naming a repository
	// the installation never granted is a configuration error and is counted in
	// PolicyHealth.UnknownEntries, because it usually means the App grant was
	// forgotten.
	Repositories map[string]RepositoryPolicy `json:"repositories,omitempty"`
}

// RepositoryPolicy is the per-repository settings, resolved against Policy's
// default. The pointer fields distinguish "not set, inherit" from "explicitly
// set to the zero value": MaxConcurrent of 0 is a real instruction to schedule
// nothing for this repository while leaving it discovered and monitored.
type RepositoryPolicy struct {
	// Enabled admits the repository to scheduling. A disabled repository is
	// still discovered, still reported, and still has its scale set reconciled,
	// so turning it back on needs no other change. While Default.Enabled is
	// false this is the allowlist entry.
	Enabled *bool `json:"enabled,omitempty"`
	// MaxConcurrent caps this repository's simultaneously charged slots. It
	// never raises the platform ceiling, which stays a hard upper bound, and it
	// is the control that stops one busy repository consuming the whole pool.
	MaxConcurrent *int `json:"max_concurrent,omitempty"`
	// Platforms lists the platforms permitted to serve this repository, as
	// "linux" and "macos". A controller ignores a repository whose platform
	// list excludes its own, which is how one policy file serves both.
	Platforms []string `json:"platforms,omitempty"`
}

// Resolved is the effective policy for one repository, with every inherited
// value already applied. Each value carries its own source, because a single
// source word for the whole struct cannot describe the ordinary case of a
// repository that overrides one field and inherits the others — and "which of
// these did someone actually write down" is the question an operator asks when
// a repository is not being scheduled.
type Resolved struct {
	Enabled       bool     `json:"enabled"`
	MaxConcurrent int      `json:"max_concurrent"`
	Platforms     []string `json:"platforms"`
	// Each source is SourceDefault or SourceRepository.
	EnabledSource       string `json:"enabled_source"`
	MaxConcurrentSource string `json:"max_concurrent_source"`
	PlatformsSource     string `json:"platforms_source"`
}
