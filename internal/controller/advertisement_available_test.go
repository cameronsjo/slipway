package controller

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

// observeStatistics writes one repository's scale-set statistics and the instant
// they were observed. It is the only writer these tests need: the live path
// (RecordEnvelope) writes exactly these two columns on every accepted envelope.
func observeStatistics(t *testing.T, s *Store, key SetKey, statistics scaleset.RunnerScaleSetStatistic, observed string) {
	t.Helper()
	data, err := json.Marshal(statistics)
	if err != nil {
		t.Fatal(err)
	}
	if err := changedOne(s.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(data), observed, key.RepositoryID, key.ScaleSetID)); err != nil {
		t.Fatal(err)
	}
}

// frozenObservation is an instant no running process can have produced, which
// is what statisticsFresh refuses.
const frozenObservation = "2000-01-01T00:00:00Z"

// capacityTotal is what the sweep commits across every repository. It must never
// exceed admission, whatever the demand.
func capacityTotal(plan AdvertisementPlan) int {
	total := 0
	for _, capacity := range plan.Capacity {
		total += capacity
	}
	return total
}

// TestAdvertisementServesARepositoryWithOnlyAvailableJobs is the 2026-09-23
// starvation. A repository this pool has never advertised capacity for has no
// journaled rows and no assigned jobs -- GitHub's broker assigns only to a scale
// set that advertised -- so its queued work exists solely as TotalAvailableJobs
// in the poll envelope. Planning from journaled rows plus TotalAssignedJobs read
// it as idle, and its only route to a slot was the single rotating exploratory
// slot: pi-extensions, the-estate and mcparr each held a job queued on slipway-linux
// for 45+ minutes while the pool logged admission=2 free=2 charged=0.
//
// The first sweep is the assertion. A repository with real available work must
// not have to wait for its turn in the exploratory rotation.
func TestAdvertisementServesARepositoryWithOnlyAvailableJobs(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)

	// Repository A: frozen everything. It sorts first, so it takes the sweep's
	// one exploratory slot and nothing else may reach B through that route.
	observeStatistics(t, s, keys[0],
		scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 2, TotalAvailableJobs: 2}, frozenObservation)
	// Repository B: a fresh envelope reporting one available job and no rows at
	// all, because the broker has never been asked to assign it anything.
	observeStatistics(t, s, keys[1],
		scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 1}, timestamp())

	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Capacity[keys[1]] != 1 {
		t.Fatalf("a repository with one available job was advertised %d on the first sweep: %+v",
			plan.Capacity[keys[1]], plan)
	}
	if plan.Capacity[keys[0]] > 1 {
		t.Fatalf("a frozen statistic bought more than the exploratory slot: %+v", plan)
	}
}

// TestAvailableJobsShareSlotsAcrossRepositories pins that available work enters
// the SAME fairness pass assigned work does: one slot per repository per pass in
// cursor order, so N repositories with available jobs share the admission rather
// than the first one taking all of it.
func TestAvailableJobsShareSlotsAcrossRepositories(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 3)
	activateTestSessions(t, s, keys)
	for _, key := range keys {
		observeStatistics(t, s, key, scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 1}, timestamp())
	}

	first, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if capacityTotal(first) != 2 || first.Deferred != 1 {
		t.Fatalf("three claimants against admission two: %+v", first)
	}
	unserved := keys[0]
	for _, key := range keys {
		if first.Capacity[key] == 0 {
			unserved = key
		}
	}
	if first.Capacity[unserved] != 0 {
		t.Fatalf("every repository was served on one sweep of admission two: %+v", first)
	}

	// The cursor moved to whoever was served last, so the deferred repository is
	// first in rotation next sweep. Nothing frees a slot in between: this is the
	// same admission, re-planned.
	second, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if second.Capacity[unserved] != 1 {
		t.Fatalf("the deferred repository was starved a second sweep: unserved=%v %+v", unserved, second)
	}
}

// TestAvailableJobsAreNeverChargedASlot is the containment claim. An available
// job is GitHub's, not ours: it may raise what a sweep ADVERTISES, and it may
// never become occupancy, because occupancy is what the journal charges against
// admission. A repository asking for more than the platform holds is bounded by
// admission and its overflow deferred.
func TestAvailableJobsAreNeverChargedASlot(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	activateTestSessions(t, s, keys)
	observeStatistics(t, s, keys[0], scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 3}, timestamp())

	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if capacityTotal(plan) != 2 || plan.Deferred != 1 {
		t.Fatalf("available jobs were not bounded by admission: %+v", plan)
	}

	var leases int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases != 0 {
		t.Fatalf("advertising for available jobs charged %d lease(s)", leases)
	}

	// Re-planning is the proof the first sweep charged nothing: an occupancy of
	// two against an admission of two would make the next sweep an invariant
	// violation rather than the same plan again.
	repeated, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatalf("a second sweep read advertised capacity as occupancy: %v", err)
	}
	if capacityTotal(repeated) != 2 {
		t.Fatalf("the repeated sweep changed what it committed: %+v", repeated)
	}
}

// TestFrozenAssignedStatisticsAreIgnoredAndLogged pins the count the freshness
// rules still guard. After a resume, github.com's refresh carries no statistics,
// so an ASSIGNED count is stuck at whatever the previous process last saw -- and
// a frozen non-zero one took the only slot every sweep while the repository with
// real queued jobs was advertised 0 for 100 minutes (2026-09-22).
//
// The AVAILABLE count is deliberately not this test's subject: it is GitHub's
// reading of a queue we have not advertised for, it cannot be re-observed until
// we do, and it buys one slot at any age (see the inherited and aged-out tests).
func TestFrozenAssignedStatisticsAreIgnoredAndLogged(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)
	observeStatistics(t, s, keys[0], scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 2}, frozenObservation)
	observeStatistics(t, s, keys[1], scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 1}, timestamp())

	logs := captureControllerLog(t)
	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	// One exploratory slot is all a frozen repository may have; the second slot
	// belongs to the repository with a fresh observation.
	if plan.Capacity[keys[0]] != 1 || plan.Capacity[keys[1]] != 1 {
		t.Fatalf("a frozen assigned count was planned as demand: %+v", plan)
	}
	line := logs.String()
	if !strings.Contains(line, "statistic_total_assigned_jobs=2") {
		t.Fatalf("the ignored assigned statistic was not reported: %q", line)
	}
	if !strings.Contains(line, "Ignored a scale-set demand statistic the planner cannot trust") {
		t.Fatalf("a frozen statistic was dropped silently: %q", line)
	}
	if !strings.Contains(line, "reason="+string(statisticsInherited)) {
		t.Fatalf("the log did not say WHY the statistic was refused: %q", line)
	}

	// The same number, observed by this process, is believed again: the rule
	// narrows what is trusted rather than discarding the statistic.
	observeStatistics(t, s, keys[0], scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 2}, timestamp())
	fresh, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Deferred == 0 {
		t.Fatalf("a re-observed available count was still ignored: %+v", fresh)
	}
}

// TestStatisticsAgeOutOfTheWindow pins both edges. The lower bound alone left a
// gap with the same shape as the outage: a poll that finds nothing returns
// HTTP 202 with no envelope, so an idle repository never re-observes, and a
// count that has since gone to zero stays believed for the life of the process.
// An `available=1` never re-observed kept claiming a turn on alternate sweeps
// forever.
func TestStatisticsAgeOutOfTheWindow(t *testing.T) {
	started := time.Date(2026, 9, 23, 2, 0, 0, 0, time.UTC)
	within := started.Add(time.Minute)

	for _, testCase := range []struct {
		name     string
		observed time.Time
		now      time.Time
		want     statisticsFreshness
	}{
		{"just-observed", within, within, statisticsCurrent},
		{"inside-the-window", within, within.Add(staleAssignmentWindow - time.Second), statisticsCurrent},
		{"exactly-at-the-window", within, within.Add(staleAssignmentWindow), statisticsCurrent},
		{"one-second-past-the-window", within, within.Add(staleAssignmentWindow + time.Second), statisticsAgedOut},
		{"long-past-the-window", within, within.Add(10 * staleAssignmentWindow), statisticsAgedOut},
		// The lower bound still wins: a pre-restart observation is inherited,
		// which is the more specific diagnosis even though it is also old.
		{"before-this-process", started.Add(-time.Minute), within, statisticsInherited},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := classifyStatistics(testCase.observed.Format(time.RFC3339Nano), started, testCase.now)
			if got != testCase.want {
				t.Fatalf("classifyStatistics = %q, want %q", got, testCase.want)
			}
		})
	}

	for _, observed := range []string{"", "not-an-instant", "2026-09-23"} {
		if got := classifyStatistics(observed, started, within); got != statisticsUnreadable {
			t.Fatalf("an unusable instant %q classified as %q", observed, got)
		}
	}
}

// TestAnAgedOutAvailableCountBuysOneSlotAndNoMore is the window's real shape
// after 2026-09-23: an aged-out AVAILABLE count is still believed, because an
// unserved queued job is precisely the thing that stops re-observing, but it is
// bounded to the single slot GitHub needs before it will assign anything.
//
// The probe is capacity and Deferred on ONE repository. Refused entirely, this
// is capacity 1 from the exploratory slot with nothing owed. Trusted in full, it
// is capacity 2 with 1 deferred. Bounded -- the rule -- is capacity 1 with
// nothing owed, which shares its numbers with the refusal, so the multi-
// repository test below is what separates those two.
func TestAnAgedOutAvailableCountBuysOneSlotAndNoMore(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	activateTestSessions(t, s, keys)

	// A controller that has been up an hour. Without this the fixture cannot
	// reach the aged-out arm at all: a store opened a moment ago makes every
	// old instant PRE-start, so it classifies as inherited and the lower bound
	// refuses it first. The window is only reachable above s.started.
	s.started = time.Now().UTC().Add(-time.Hour)

	// Three available jobs observed by THIS process, but longer ago than the
	// window. The queue may have emptied since; no envelope has said either way,
	// because a poll that finds nothing carries no statistics.
	aged := time.Now().UTC().Add(-2 * staleAssignmentWindow).Format(time.RFC3339Nano)
	observeStatistics(t, s, keys[0], scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 3}, aged)

	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	// One slot, and the other two available jobs are NOT owed: an aged-out count
	// buys a probe, never a backlog's worth of headroom.
	if plan.Capacity[keys[0]] != 1 || plan.Deferred != 0 {
		t.Fatalf("an aged-out available count was planned as more than one slot: %+v", plan)
	}

	// Re-observed inside the window by the same process, the same number is
	// demand again. This is what proves the window did the refusing above, and
	// not the lower bound or an empty statistic.
	observeStatistics(t, s, keys[0], scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 3}, timestamp())
	current, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if current.Capacity[keys[0]] != 2 || current.Deferred != 1 {
		t.Fatalf("a re-observed available count was not believed: %+v", current)
	}
}

// TestAnAgedOutAvailableCountCompetesInTheDemandPass is what separates bounded
// trust from refusal, and it is the outage this rule exists for. Refused, a
// repository with a queued job is reachable ONLY through the one exploratory
// slot a sweep, so with many subscriptions it waits its whole turn in the
// rotation -- 48 subscriptions at roughly a sweep a minute is the ~45-minute
// "stuck queue" measured on 2026-09-23.
//
// Eight subscriptions, two slots, and the two repositories holding queued work
// sit LAST in the rotation. Refused, they cannot both be served before sweep 7;
// believed, they are served in the demand pass beside the sweep's one probe.
func TestAnAgedOutAvailableCountCompetesInTheDemandPass(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 8)
	activateTestSessions(t, s, keys)
	s.started = time.Now().UTC().Add(-time.Hour)

	aged := time.Now().UTC().Add(-2 * staleAssignmentWindow).Format(time.RFC3339Nano)
	for i, key := range keys {
		statistics := scaleset.RunnerScaleSetStatistic{}
		observed := timestamp()
		if i >= 6 {
			statistics.TotalAvailableJobs = 1
			observed = aged
		}
		observeStatistics(t, s, key, statistics, observed)
	}

	waiting := map[int64]bool{keys[6].RepositoryID: true, keys[7].RepositoryID: true}
	for sweep := 1; sweep <= 3; sweep++ {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if plan.Capacity[key] > 0 {
				delete(waiting, key.RepositoryID)
			}
		}
	}
	if len(waiting) != 0 {
		t.Fatalf("queued work waited on the exploratory rotation instead of the demand pass: %v", waiting)
	}
}

// TestExploratoryRotationSurvivesAvailableDemand is the negative control for the
// change. Available jobs entering the demand pass must not consume the probe a
// genuinely idle repository needs: GitHub assigns nothing to a scale set that
// never advertised, so an idle repository with headroom beside it must still get
// its turn, and the turn must still rotate.
func TestExploratoryRotationSurvivesAvailableDemand(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 3)
	activateTestSessions(t, s, keys)
	for _, key := range keys {
		observeStatistics(t, s, key, scaleset.RunnerScaleSetStatistic{}, timestamp())
	}

	served := map[int64]int{}
	for sweep := 1; sweep <= 3; sweep++ {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		if capacityTotal(plan) != 1 {
			t.Fatalf("sweep %d offered %d exploratory slots, want exactly one: %+v",
				sweep, capacityTotal(plan), plan)
		}
		for _, key := range keys {
			served[key.RepositoryID] += plan.Capacity[key]
		}
	}
	for _, key := range keys {
		if served[key.RepositoryID] != 1 {
			t.Fatalf("exploratory turns did not rotate over three sweeps: %v", served)
		}
	}
}

// TestAnInheritedAvailableCountStillBuysOneSlot is the restart door. A resume
// does not rewrite scale_sets.statistics, so after every restart each
// repository's last reading is INHERITED -- and a repository whose queue GitHub
// will not describe until we advertise has no way to produce a newer one. It
// must still get its one slot, or the pool re-enters the starvation on every
// restart and heals only through the exploratory rotation.
func TestAnInheritedAvailableCountStillBuysOneSlot(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 8)
	activateTestSessions(t, s, keys)

	// Observed by the PREVIOUS process: every instant predates s.started.
	inherited := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	for i, key := range keys {
		statistics := scaleset.RunnerScaleSetStatistic{}
		if i >= 6 {
			statistics.TotalAvailableJobs = 1
		}
		observeStatistics(t, s, key, statistics, inherited)
	}

	waiting := map[int64]bool{keys[6].RepositoryID: true, keys[7].RepositoryID: true}
	for sweep := 1; sweep <= 3; sweep++ {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if plan.Capacity[key] > 0 {
				delete(waiting, key.RepositoryID)
			}
		}
	}
	if len(waiting) != 0 {
		t.Fatalf("an inherited available count was refused across a restart: %v", waiting)
	}
}

// TestAnUnreadableObservationIsStillRefused keeps the fail-closed arm honest:
// widening the available count to every AGE must not widen it to an instant the
// planner could not parse at all.
func TestAnUnreadableObservationIsStillRefused(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)
	observeStatistics(t, s, keys[0], scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 3}, "not-an-instant")
	observeStatistics(t, s, keys[1], scaleset.RunnerScaleSetStatistic{}, timestamp())

	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	// Refused as demand, so the sweep spends its one exploratory slot and
	// nothing more. Believed, this would be capacity on both repositories.
	if capacityTotal(plan) != 1 || plan.Deferred != 0 {
		t.Fatalf("an unparsable observation was planned as demand: %+v", plan)
	}
}
