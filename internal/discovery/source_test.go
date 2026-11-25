package discovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

var ctx = context.Background()

type fakeCounters struct{ counters controller.BootstrapCounters }

func (f fakeCounters) Counters() controller.BootstrapCounters { return f.counters }

// harness is one composed source over a real journal, with every injectable
// seam under the test's control: the clock, the process start instant, the
// cycle observer, the bootstrap counts and the listener's readiness.
type harness struct {
	store     *controller.Store
	cycle     *CycleObserver
	counters  *fakeCounters
	listener  bool
	now       time.Time
	started   time.Time
	source    *StoreSource
	setKey    controller.SetKey
	firstRepo int64
	dir       string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	// RepositoryAllowlist is not optional: controller.Open refuses an empty one
	// rather than defaulting to "onboard the whole App installation". It names
	// the same two repositories healthy() discovers, so a repository this
	// harness offers is one the pool would really plan a scale set for.
	store, err := controller.Open(dir, controller.Config{
		Platform: "linux", InstallationID: 42, Capacity: 2,
		RepositoryAllowlist: []string{"cameronsjo/homelab", "cameronsjo/cadence"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	// The journal stamps its own rows with the real clock, so the harness clock
	// has to start from the same place. A fixed instant would make every
	// discovery snapshot read as arriving from the future, which the freshness
	// fence correctly refuses — and every test would then pass or fail on that
	// rather than on what it is about.
	start := time.Now().UTC()
	h := &harness{
		dir:   dir,
		store: store, cycle: NewCycleObserver(), counters: &fakeCounters{}, listener: true,
		now: start, started: start.Add(-time.Hour),
		setKey: controller.SetKey{RepositoryID: 1, ScaleSetID: 101}, firstRepo: 1,
	}
	source, err := NewStoreSource(SourceConfig{
		Store: store, Cycle: h.cycle, Bootstrap: h.counters,
		ListenerReady: func() bool { return h.listener },
		Started:       h.started, Interval: interval,
		Now: func() time.Time { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.source = source
	return h
}

// healthy brings every section this track produces to its healthy value: a
// fresh App snapshot, a complete backend inventory, and a cycle that has just
// turned. It is the positive control — without it a test asserting "degraded"
// would pass against a journal that was never populated.
func (h *harness) healthy(t *testing.T) {
	t.Helper()
	// Private is load-bearing, not decoration. ADR-0003 forbids a self-hosted
	// runner on a public repository, and githubapp.Repository.Private is a plain
	// bool -- so a fixture that omits it decodes as public, is refused by
	// eligibleForPool, and this positive control would assert against a pool
	// that planned nothing.
	if err := h.store.Discover(ctx, []githubapp.Repository{
		{ID: 1, FullName: "cameronsjo/homelab", Private: true},
		{ID: 2, FullName: "cameronsjo/cadence", Private: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.store.BindSet(ctx, h.setKey); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ReconcileBackendInventory(ctx, controller.BackendInventory{Complete: true, FreeSlots: 2}); err != nil {
		t.Fatal(err)
	}
	// The journal stamped those rows with the real clock, which has moved on
	// since the harness captured its own. Re-sync, or every baseline reads as a
	// snapshot from the future and the freshness fence refuses it.
	h.now = time.Now().UTC()
	h.cycle.Completed(h.now, nil)
}

func (h *harness) offer(t *testing.T, requests int) {
	t.Helper()
	envelope := controller.Envelope{MessageID: 1,
		Statistics: scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: requests, TotalAssignedJobs: 7}}
	for n := 1; n <= requests; n++ {
		envelope.Events = append(envelope.Events, controller.Event{
			JobMessageBase: scaleset.JobMessageBase{
				JobMessageType: scaleset.JobMessageType{MessageType: scaleset.MessageTypeJobAvailable},
				// A distinct job ID per request, as GitHub sends: a shared literal
				// makes every offer resolve to the same durable job.
				RunnerRequestID: int64(n), OwnerName: "cameronsjo", RepositoryName: "homelab",
				JobID: fmt.Sprintf("workflow-job-%d", n),
			},
		})
	}
	if err := h.store.RecordEnvelope(ctx, h.setKey, uuid.New(), envelope); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) report(t *testing.T) Report {
	t.Helper()
	report, err := h.source.Report(ctx)
	if err != nil {
		t.Fatalf("report composition failed: %v", err)
	}
	return report
}

// B1/B4: one row per § What each section calls Healthy, each driving the
// top-level verdict on its own. The baseline names policy alone because Track D
// owns that producer; a section this track produces must add its own name.
func TestEachSectionIndependentlyDrivesTheTopLevelVerdict(t *testing.T) {
	for _, test := range []struct {
		name    string
		degrade func(t *testing.T, h *harness)
		section string
	}{
		{"a wedged cycle", func(t *testing.T, h *harness) {
			h.now = h.now.Add(4 * interval)
		}, "cycle"},
		{"a stale App snapshot", func(t *testing.T, h *harness) {
			h.now = h.now.Add(controller.DiscoveryFreshness)
			h.cycle.Completed(h.now, nil)
		}, "discovery"},
		{"stalled demand", func(t *testing.T, h *harness) {
			h.offer(t, 1)
			h.now = h.now.Add(DefaultStallThreshold + time.Minute)
			h.cycle.Completed(h.now, nil)
			if err := h.store.Discover(ctx, []githubapp.Repository{
				{ID: 1, FullName: "cameronsjo/homelab", Private: true},
				{ID: 2, FullName: "cameronsjo/cadence", Private: true}}); err != nil {
				t.Fatal(err)
			}
		}, "demand"},
		{"a fenced backend past the grace", func(t *testing.T, h *harness) {
			_ = h.store.ReconcileBackendInventory(ctx, controller.BackendInventory{Complete: false})
		}, "capacity"},
		{"an untracked worker", func(t *testing.T, h *harness) {
			_ = h.store.ReconcileBackendInventory(ctx, controller.BackendInventory{
				Complete: true, FreeSlots: 2,
				Workers: []controller.WorkerObservation{{LeaseID: "ghost", ResourceID: "ghost"}},
			})
		}, "workers"},
		{"a bootstrap listener that is down", func(t *testing.T, h *harness) {
			h.listener = false
		}, "bootstrap"},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t)
			h.healthy(t)
			baseline := h.report(t)
			if !slices.Equal(baseline.Reasons, []string{"policy"}) {
				t.Fatalf("baseline reasons = %v, want only the policy section Track D owns", baseline.Reasons)
			}

			test.degrade(t, h)
			degraded := h.report(t)
			if degraded.Status != Degraded {
				t.Fatalf("top-level status = %q, want degraded", degraded.Status)
			}
			if !slices.Contains(degraded.Reasons, test.section) {
				t.Fatalf("reasons = %v, want them to name %q", degraded.Reasons, test.section)
			}
		})
	}
}

// A repository whose scale set is unverified or carrying a diagnostic counts
// toward repositories_degraded; the transient onboarding phases do not, so
// granting the App never pages anyone.
func TestRepositoriesDegradedExcludesTheOnboardingPhases(t *testing.T) {
	for _, test := range []struct {
		phase    string
		degraded int
	}{
		{"planned", 0}, {"create-intent", 0}, {"created", 0}, {"active", 0}, {"unverified", 1},
	} {
		t.Run(test.phase, func(t *testing.T) {
			h := newHarness(t)
			h.healthy(t)
			h.setManagedPhase(t, h.firstRepo, test.phase, "")
			report := h.report(t)
			if report.RepositoriesDegraded != test.degraded {
				t.Fatalf("repositories_degraded for phase %q = %d, want %d", test.phase, report.RepositoriesDegraded, test.degraded)
			}
			if report.RepositoriesTotal != 2 || report.RepositoriesEligible != 2 {
				t.Fatalf("rollups = %d total / %d eligible, want 2 and 2",
					report.RepositoriesTotal, report.RepositoriesEligible)
			}
		})
	}
}

// The grace exists so a routine restart does not fire the top-level row before
// the admission row that would explain it.
func TestCapacityAndBackendReadHealthyInsideTheStartupGrace(t *testing.T) {
	h := newHarness(t)
	h.started = h.now.Add(-interval)
	source, err := NewStoreSource(SourceConfig{
		Store: h.store, Cycle: h.cycle, Bootstrap: h.counters,
		ListenerReady: func() bool { return h.listener },
		Started:       h.started, Interval: interval, Now: func() time.Time { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.source = source
	if err := h.store.Discover(ctx, []githubapp.Repository{{ID: 1, FullName: "cameronsjo/homelab"}}); err != nil {
		t.Fatal(err)
	}
	h.cycle.Completed(h.now, nil)

	inside := h.report(t)
	if inside.Capacity.Admission != "fenced" {
		t.Fatalf("a controller with no inventory reported admission %q", inside.Capacity.Admission)
	}
	if inside.Capacity.Status != Healthy || inside.Backend.Status != Healthy {
		t.Fatalf("inside the grace: capacity %q, backend %q, want both healthy",
			inside.Capacity.Status, inside.Backend.Status)
	}

	h.started = h.now.Add(-4 * interval)
	source, err = NewStoreSource(SourceConfig{
		Store: h.store, Cycle: h.cycle, Bootstrap: h.counters,
		ListenerReady: func() bool { return h.listener },
		Started:       h.started, Interval: interval, Now: func() time.Time { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.source = source
	outside := h.report(t)
	if outside.Capacity.Status != Degraded || outside.Backend.Status != Degraded {
		t.Fatalf("past the grace: capacity %q, backend %q, want both degraded",
			outside.Capacity.Status, outside.Backend.Status)
	}
}

// B4: unknown is reserved for the 503 skeleton. A composed body that carried it
// would give a reader two rules instead of one.
func TestAComposedBodyNeverCarriesUnknown(t *testing.T) {
	h := newHarness(t)
	h.healthy(t)
	h.offer(t, 2)
	h.listener = false
	h.cycle.Completed(h.now, context.DeadlineExceeded)
	h.setManagedPhase(t, h.firstRepo, "unverified", "scale set readback failed")

	body, err := json.Marshal(h.report(t))
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	// Assert on values, not on the raw bytes: "unknown_entries" is a field name,
	// and a substring check on the body would pass for the wrong reason forever.
	if path := findStringValue(decoded, string(Unknown), "$"); path != "" {
		t.Fatalf("a composed 200 body carried %q at %s: %s", Unknown, path, body)
	}
}

// findStringValue returns the path of the first string value equal to want, or
// an empty string when there is none.
func findStringValue(node any, want, path string) string {
	switch value := node.(type) {
	case string:
		if value == want {
			return path
		}
	case map[string]any:
		for key, child := range value {
			if found := findStringValue(child, want, path+"."+key); found != "" {
				return found
			}
		}
	case []any:
		for index, child := range value {
			if found := findStringValue(child, want, fmt.Sprintf("%s[%d]", path, index)); found != "" {
				return found
			}
		}
	}
	return ""
}

// B1/C6 precursor: the body is composed from the journal, and the journal holds
// queue URLs, runner names and controller sentences. None of them may reach it.
func TestTheBodyCarriesNoStoredDiagnosticTextAndNoCredentialShapedField(t *testing.T) {
	h := newHarness(t)
	h.healthy(t)
	const leak = "https://broker.actions.githubusercontent.com/message/secret-queue?token=abcd"
	h.setManagedPhase(t, h.firstRepo, "unverified", leak)

	body, err := json.Marshal(h.report(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{leak, "githubusercontent", "token=", "secret-queue", "sqlite", "SQL"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("the report body carried %q: %s", forbidden, body)
		}
	}
	var report Report
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Repositories[1].ScaleSet.Diagnostic != DiagnosticUnavailable {
		t.Fatalf("a stored diagnostic was published as %q rather than the redacted word",
			report.Repositories[1].ScaleSet.Diagnostic)
	}
}

// The per-repository rows carry the detail a runbook needs, ordered so two
// consecutive reports diff cleanly.
func TestRepositoryRowsCarryTheirOwnCountsInNameOrder(t *testing.T) {
	h := newHarness(t)
	h.healthy(t)
	h.offer(t, 3)
	report := h.report(t)

	if len(report.Repositories) != 2 {
		t.Fatalf("repository rows = %d, want 2", len(report.Repositories))
	}
	if report.Repositories[0].FullName != "cameronsjo/cadence" || report.Repositories[1].FullName != "cameronsjo/homelab" {
		t.Fatalf("rows are not in full-name order: %q then %q",
			report.Repositories[0].FullName, report.Repositories[1].FullName)
	}
	homelab := report.Repositories[1]
	if homelab.QueuedOffers != 3 {
		t.Fatalf("queued offers on the offered repository = %d, want 3", homelab.QueuedOffers)
	}
	if homelab.AssignedJobs != 7 {
		t.Fatalf("assigned jobs = %d, want GitHub's reported 7", homelab.AssignedJobs)
	}
	if homelab.ScaleSet.Label != "slipway-linux" {
		t.Fatalf("scale-set label = %q, want the stable platform selector", homelab.ScaleSet.Label)
	}
	if report.Demand.QueuedOffers != 3 {
		t.Fatalf("pool-wide queued offers = %d, want 3", report.Demand.QueuedOffers)
	}
}

// B8: capability issuance reaches the body as counts and nothing else.
func TestBootstrapSectionPublishesCountsOnly(t *testing.T) {
	h := newHarness(t)
	h.healthy(t)
	h.counters.counters = controller.BootstrapCounters{Outstanding: 1, Delivered: 4, Expired: 2, Failed: 3}
	report := h.report(t)
	if report.Bootstrap.Delivered != 4 || report.Bootstrap.Expired != 2 || report.Bootstrap.Failed != 3 || report.Bootstrap.Outstanding != 1 {
		t.Fatalf("bootstrap counts were not published: %+v", report.Bootstrap)
	}
	if report.Bootstrap.Status != Healthy {
		t.Fatalf("bootstrap verdict with a ready listener = %q, want healthy", report.Bootstrap.Status)
	}
}

// setManagedPhase writes the ownership record the registry would write. It goes
// at the journal directly, because minting a scale set needs the App API and a
// read-only composer test must not reach for one. Opening a second handle on
// the same file is safe here: the controller's exclusive lock is on pool.lock,
// and both handles use WAL with a busy timeout.
func (h *harness) setManagedPhase(t *testing.T, repo int64, phase, diagnostic string) {
	t.Helper()
	u := url.URL{Scheme: "file", Path: filepath.Join(h.dir, "pool.sqlite")}
	u.RawQuery = url.Values{"_pragma": {"journal_mode(WAL)", "busy_timeout(5000)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO managed_sets(repo_id,name,remote_id,phase,diagnostic,updated)
VALUES(?,?,?,?,?,?) ON CONFLICT(repo_id) DO UPDATE SET phase=excluded.phase,diagnostic=excluded.diagnostic`,
		repo, "slipway-linux-fixture", 101, phase, diagnostic, origin.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}
