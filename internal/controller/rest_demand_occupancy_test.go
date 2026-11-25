package controller

import (
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// restDemandOccupancyFixture builds the shape measured live on 2026-09-25
// 23:07-23:19Z: the first repository already runs one job (one charged lease),
// its latest poll envelope reports `available` jobs (zero live), and a REST
// read counts `queued` more pool jobs for it. freshFor is how long that REST
// reading stays trusted; a negative value makes it stale.
func restDemandOccupancyFixture(t *testing.T, admission, repositories, available, queued int, freshFor time.Duration) (*Store, []SetKey) {
	t.Helper()
	s, _ := openTestStore(t, admission)
	keys := prepare(t, s, repositories)
	activateTestSessions(t, s, keys)
	key := keys[0]
	offer(t, s, key, 1)
	mustReserve(t, s)
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2,
		Statistics: scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: available}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: admission}); err != nil {
		t.Fatal(err)
	}
	// The read returns after the running job's lease exists, as it did live:
	// that lease is occupancy, not a job the reading counted as queued.
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{key.RepositoryID: queued}, now, now.Add(freshFor))
	if occupied := chargedLeases(t, s, key.RepositoryID); occupied != 1 {
		t.Fatalf("fixture occupancy is %d, want 1", occupied)
	}
	return s, keys
}

func chargedLeases(t *testing.T, s *Store, repoID int64) int {
	t.Helper()
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE repo_id=? AND phase!='released'`, repoID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func planOnce(t *testing.T, s *Store, keys []SetKey) AdvertisementPlan {
	t.Helper()
	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// TestRESTDemandAddsQueuedJobsOnTopOfOccupancy is the live 2026-09-25 case:
// one job running, four more queued (REST only), three slots free. The fix
// must advertise the free slots, not stop at the one already occupied. Before
// it, this read 1 (confirmed on the pre-fix code).
func TestRESTDemandAddsQueuedJobsOnTopOfOccupancy(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 4, time.Hour)
	if got := planOnce(t, s, keys).Capacity[keys[0]]; got != 4 {
		t.Fatalf("one running and four queued with three slots free advertised %d, want 4 (occupied 1 + 3 free)", got)
	}
}

// The queued count is added to the running work, not compared with it: one
// running and two queued is three, where max(running, queued) would say two
// and leave a free slot idle.
func TestRESTDemandIsAddedToRunningWorkNotMaxedWithIt(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 2, time.Hour)
	if got := planOnce(t, s, keys).Capacity[keys[0]]; got != 3 {
		t.Fatalf("one running and two queued advertised %d, want 3", got)
	}
}

func TestRESTDemandOfZeroAddsNothing(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 0, time.Hour)
	if got := planOnce(t, s, keys).Capacity[keys[0]]; got != 1 {
		t.Fatalf("a zero REST count advertised %d, want only the occupied 1", got)
	}
}

func TestAStaleRESTReadingAddsNothing(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 4, -time.Second)
	if got := planOnce(t, s, keys).Capacity[keys[0]]; got != 1 {
		t.Fatalf("a stale REST reading advertised %d, want only the occupied 1", got)
	}
}

// Envelope demand alone plans exactly as before this change: a current
// statistic of two available jobs is the repository's total demand.
func TestEnvelopeDemandWithoutRESTIsUnchanged(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 2, 0, time.Hour)
	if got := planOnce(t, s, keys).Capacity[keys[0]]; got != 2 {
		t.Fatalf("a current statistic of two available advertised %d, want 2", got)
	}
}

// A large REST count cannot starve another repository: the first pass gives
// every repository with demand one slot before any repository gets a second.
func TestALargeRESTCountDoesNotStarveAnotherRepository(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 3, 2, 0, 9, time.Hour)
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{keys[1].RepositoryID: 1}, now, now.Add(time.Hour))
	plan := planOnce(t, s, keys)
	if plan.Capacity[keys[1]] != 1 {
		t.Fatalf("the second repository got %d slots behind a REST count of nine, want 1: %+v", plan.Capacity[keys[1]], plan.Capacity)
	}
	if plan.Capacity[keys[0]] != 2 {
		t.Fatalf("the busy repository got %d, want 2 (occupied 1 + the one slot left): %+v", plan.Capacity[keys[0]], plan.Capacity)
	}
}

// A job queued at the read and leased since is occupancy now; the stale count
// must not also claim it. One job runs, the read counts 2 queued, then both
// are leased: occupancy is 3 and nothing is queued. With one slot left the
// plan must advertise 3; the uncorrected sum (3 + 2) takes the free slot too.
func TestLeasesCreatedAfterTheReadAreNotCountedTwice(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 2, time.Hour)
	key := keys[0]
	for n := int64(10); n < 12; n++ {
		time.Sleep(2 * time.Millisecond)
		e := Envelope{MessageID: int(n), Statistics: scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 1},
			Events: []Event{event(key, n, scaleset.MessageTypeJobAvailable)}}
		if err := s.RecordEnvelope(ctx, key, uuid.New(), e); err != nil {
			t.Fatal(err)
		}
		mustReserve(t, s)
	}
	if occupied := chargedLeases(t, s, key.RepositoryID); occupied != 3 {
		t.Fatalf("occupancy is %d, want 3", occupied)
	}
	if got := planOnce(t, s, keys).Capacity[key]; got != 3 {
		t.Fatalf("a read of 2 whose jobs were both leased since advertised %d, want occupancy 3", got)
	}
}

// Only leases created AFTER the read come off its count. The fixture's
// running lease predates the read, so the full count of 4 still applies.
func TestALeaseCreatedBeforeTheReadIsNotSubtracted(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 2, time.Hour)
	if got := planOnce(t, s, keys).Capacity[keys[0]]; got != 3 {
		t.Fatalf("a lease older than the read was subtracted: advertised %d, want 1 running + 2 queued = 3", got)
	}
}

// A set whose poll failed keeps its occupancy but gets no REST demand: nothing
// can be reserved for it this cycle, so advertising more only strands demand.
func TestAPollFailedSetGetsNoRESTDemand(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 1, 0, 4, time.Hour)
	plan, err := s.PlanAdvertisementsWith(ctx, keys, nil, []SetKey{keys[0]})
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Capacity[keys[0]]; got != 1 {
		t.Fatalf("a poll-failed set advertised %d, want only its occupancy 1", got)
	}
}

// A degraded set is left out of the plan entirely, REST reading or not.
func TestADegradedSetGetsNoRESTDemand(t *testing.T) {
	s, keys := restDemandOccupancyFixture(t, 4, 2, 0, 4, time.Hour)
	plan, err := s.PlanAdvertisementsExcept(ctx, keys[1:], keys[:1])
	if err != nil {
		t.Fatal(err)
	}
	if got, planned := plan.Capacity[keys[0]]; planned {
		t.Fatalf("a degraded set with a REST reading of 4 was planned at %d, want absent", got)
	}
}
