package controller

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

func activateTestSessions(t *testing.T, s *Store, keys []SetKey) {
	t.Helper()
	statistics, err := json.Marshal(scaleset.RunnerScaleSetStatistic{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		fingerprint := make([]byte, 32)
		fingerprint[0] = byte(key.RepositoryID)
		if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,session_id,queue_sha256,phase,created,updated)
VALUES(?,?,?,?,?,'active',?,?)`, "owner-"+uuid.NewString(), key.RepositoryID, key.ScaleSetID, uuid.NewString(), fingerprint, timestamp(), timestamp()); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
			string(statistics), timestamp(), key.RepositoryID, key.ScaleSetID); err != nil {
			t.Fatal(err)
		}
	}
}

// assignJobs is GitHub's demand for one repository: the assigned-job count the
// controller does not control and must plan around.
func assignJobs(t *testing.T, s *Store, key SetKey, assigned int) {
	t.Helper()
	statistics, err := json.Marshal(scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: assigned})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(statistics), timestamp(), key.RepositoryID, key.ScaleSetID); err != nil {
		t.Fatal(err)
	}
}

func TestAdvertisementRotationPersistsAndIncludesZeroAllocation(t *testing.T) {
	s, dir := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)
	first, err := s.PlanAdvertisements(ctx, []SetKey{keys[1], keys[0]})
	if err != nil || first.Capacity[keys[0]] != 1 || first.Capacity[keys[1]] != 0 {
		t.Fatalf("first advertisement: %+v %v", first, err)
	}
	second, err := s.PlanAdvertisements(ctx, keys)
	if err != nil || second.Capacity[keys[0]] != 0 || second.Capacity[keys[1]] != 1 {
		t.Fatalf("second advertisement: %+v %v", second, err)
	}
	if _, err := s.PlanAdvertisements(ctx, keys[:1]); err == nil {
		t.Fatal("incomplete sweep could starve an active zero-allocation repository")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.PlanAdvertisements(ctx, keys); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("restart used stale inventory: %v", err)
	}
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	third, err := restarted.PlanAdvertisements(ctx, keys)
	if err != nil || third.Capacity[keys[0]] != 1 || third.Capacity[keys[1]] != 0 {
		t.Fatalf("persisted rotation: %+v %v", third, err)
	}
}

// Each repository is charged one of the platform's two slots, so every expected
// value is strictly below the ceiling and no plan that hands the whole platform
// to one repository can pass. The second repository's 1 comes from its own
// charged lease -- preservation -- not from dividing the ceiling in half.
func TestAdvertisementPreservesChargedAndAssignedCapacity(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)
	for _, key := range keys {
		offer(t, s, key, 2)
	}
	charged := map[SetKey]bool{}
	for range 2 {
		charged[mustReserve(t, s).Set] = true
	}
	if len(charged) != 2 {
		t.Fatalf("fixture did not charge one slot to each repository: %+v", charged)
	}
	// Both repositories get a statistic, because demand now reads available jobs
	// as well as assigned ones and `offer` above left an available count of two
	// per repository. GitHub drops an available job from that count once a runner
	// takes it, so a fixture that refreshes only one repository is asserting
	// against an observation the live pool would never hold.
	stats := scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1}
	data, _ := json.Marshal(stats)
	for _, key := range keys {
		if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=? WHERE repo_id=?`, string(data), key.RepositoryID); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := s.PlanAdvertisements(ctx, keys)
	if err != nil || plan.Capacity[keys[0]] != 1 || plan.Capacity[keys[1]] != 1 {
		t.Fatalf("charged and assigned capacity was not preserved: %+v %v", plan, err)
	}
	// The platform is exactly committed, so an assignment matching what is
	// already charged changes nothing and must still be accepted.
	if _, err := s.PlanAdvertisements(ctx, keys); err != nil {
		t.Fatalf("exact platform obligation rejected: %v", err)
	}
	stats.TotalAssignedJobs = 2
	data, _ = json.Marshal(stats)
	if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=? WHERE repo_id=?`, string(data), keys[0].RepositoryID); err != nil {
		t.Fatal(err)
	}
	// Demand now exceeds the fully committed platform. The overflow is deferred
	// to a later sweep; it must never fail the cycle, because failing here also
	// skipped the polling and provisioning that free the slots it is waiting on.
	beyond, err := s.PlanAdvertisements(ctx, keys)
	if err != nil {
		t.Fatalf("demand beyond platform capacity failed the sweep: %v", err)
	}
	if beyond.Capacity[keys[0]] != 1 || beyond.Capacity[keys[1]] != 1 || beyond.Deferred != 1 {
		t.Fatalf("demand beyond platform capacity was not deferred: %+v", beyond)
	}
}

// The live defect: two allowlisted repositories each had a job assigned by
// GitHub's broker against an admission of one, every cycle failed before
// polling or provisioning, and both jobs sat queued for 30+ minutes with the
// pool idle. Demand is GitHub's view, not the controller's, so demand above
// admission is the ordinary state of a shared pool and is planned around.
func TestAdvertisementServesWhatFitsAndDefersTheRest(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		capacity  int
		repos     int
		charge    []int
		assigned  []int
		admission int
		want      []int
		deferred  int
	}{
		{
			name:     "one slot two claimants serves the first and defers the second",
			capacity: 1, repos: 2, charge: []int{0, 0}, assigned: []int{1, 1},
			admission: 1, want: []int{1, 0}, deferred: 1,
		},
		{
			name:     "a charged repository keeps its slot and the free one serves the other",
			capacity: 2, repos: 2, charge: []int{1, 0}, assigned: []int{0, 1},
			admission: 2, want: []int{1, 1}, deferred: 0,
		},
		{
			name:     "no admission plans nothing and defers every assignment",
			capacity: 2, repos: 2, charge: []int{0, 0}, assigned: []int{1, 1},
			admission: 0, want: []int{0, 0}, deferred: 2,
		},
		{
			name:     "two slots and three claimants serve two and defer one",
			capacity: 2, repos: 3, charge: []int{0, 0, 0}, assigned: []int{1, 1, 1},
			admission: 2, want: []int{1, 1, 0}, deferred: 1,
		},
		{
			// An IDLE NEIGHBOUR COSTS THE BUSY REPOSITORY ONE SLOT, and that is
			// the price of the exploratory slot rotating. This case used to want
			// [2 0]: demand consumed all the headroom and the idle repository was
			// advertised nothing, which on a one-slot pool meant it could never be
			// offered work for the life of the neighbour's backlog (GitHub assigns
			// only to a scale set that asked for capacity). The busy repository
			// still grows into everything left, and the case below shows it takes
			// the whole admission whenever no idle neighbour needs a probe.
			name:     "a busy repository yields exactly one slot to an idle neighbour's probe",
			capacity: 2, repos: 2, charge: []int{0, 0}, assigned: []int{3, 0},
			admission: 2, want: []int{1, 1}, deferred: 2,
		},
		{
			name:     "one repository may grow into the whole free admission",
			capacity: 2, repos: 1, charge: []int{0}, assigned: []int{3},
			admission: 2, want: []int{2}, deferred: 1,
		},
		{
			// Every repository has demand, so nothing is owed a probe and the
			// exploratory slot is never spent.
			name:     "demand everywhere leaves no slot for exploration",
			capacity: 2, repos: 2, charge: []int{0, 0}, assigned: []int{2, 2},
			admission: 2, want: []int{1, 1}, deferred: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			s, _ := openTestStore(t, testCase.capacity)
			keys := prepare(t, s, testCase.repos)
			activateTestSessions(t, s, keys)
			charged := 0
			for i, count := range testCase.charge {
				if count == 0 {
					continue
				}
				// Only this repository has an offered job while it reserves, so
				// each lease lands on the repository the case names.
				offer(t, s, keys[i], count)
				for range count {
					mustReserve(t, s)
					charged++
				}
			}
			for i, count := range testCase.assigned {
				assignJobs(t, s, keys[i], count)
			}
			if err := s.ReconcileBackendInventory(ctx, BackendInventory{
				Complete: true, FreeSlots: testCase.admission - charged}); err != nil {
				t.Fatal(err)
			}
			if _, admission := s.Admission(); admission != testCase.admission {
				t.Fatalf("fixture admission is %d, not the %d this case tests", admission, testCase.admission)
			}
			plan, err := s.PlanAdvertisements(ctx, keys)
			if err != nil {
				t.Fatalf("demand above admission failed the sweep: %v", err)
			}
			committed := 0
			for i, key := range keys {
				if plan.Capacity[key] != testCase.want[i] {
					t.Fatalf("planned %+v, want %v", plan.Capacity, testCase.want)
				}
				committed += plan.Capacity[key]
			}
			if committed > testCase.admission {
				t.Fatalf("committed %d against an admission of %d", committed, testCase.admission)
			}
			if plan.Deferred != testCase.deferred {
				t.Fatalf("deferred %d, want %d", plan.Deferred, testCase.deferred)
			}
		})
	}
}

// Fairness is the whole reason deferral is safe: a repository that loses a
// sweep must win the next one, or a busy neighbour starves it forever.
func TestAdvertisementCursorServesTheRepositoryThatWaited(t *testing.T) {
	s, _ := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	activateTestSessions(t, s, keys)
	for _, key := range keys {
		assignJobs(t, s, key, 1)
	}
	first, err := s.PlanAdvertisements(ctx, keys)
	if err != nil || first.Capacity[keys[0]] != 1 || first.Capacity[keys[1]] != 0 || first.Deferred != 1 {
		t.Fatalf("first sweep: %+v %v", first, err)
	}
	second, err := s.PlanAdvertisements(ctx, keys)
	if err != nil || second.Capacity[keys[0]] != 0 || second.Capacity[keys[1]] != 1 || second.Deferred != 1 {
		t.Fatalf("the deferred repository did not receive the next slot: %+v %v", second, err)
	}
}

// The exploratory slot must skip the repository that just took a demand slot.
// Without that, repository 11 takes both slots of an admission of two every
// sweep and repository 12 is never advertised anything at all.
func TestAdvertisementExploresTheNextRepositoryAfterServingDemand(t *testing.T) {
	keys := []SetKey{{RepositoryID: 11, ScaleSetID: 1}, {RepositoryID: 12, ScaleSetID: 2}}
	plan, chosen, _ := planCapacity(keys, map[int64]int{}, map[int64]int{11: 1}, 2, 0)
	if plan.Capacity[keys[0]] != 1 || plan.Capacity[keys[1]] != 1 || chosen != 12 {
		t.Fatalf("the served repository took the exploratory slot too: %+v chosen=%d", plan.Capacity, chosen)
	}
}

// The live defect of 2026-09-24: with headroom 1 the exploratory slot walked
// 48 repositories one per sweep and won almost every time, so the repository
// holding ~36 queued jobs got one slot about every 48 minutes. Each case runs
// successive sweeps, feeding the returned cursor back in the way
// PlanAdvertisements persists it, and pins the whole plan (every repository not
// listed in capacity must be advertised zero), the cursor, and the deferral.
func TestPlanCapacityServesDemandBeforeExploration(t *testing.T) {
	type sweep struct {
		capacity map[int64]int
		chosen   int64
		deferred int
	}
	for _, testCase := range []struct {
		name     string
		repos    int
		demand   map[int64]int
		headroom int
		cursor   int64
		sweeps   []sweep
	}{
		{
			name:  "headroom 1 serves the one repository with demand however far it sits from the cursor",
			repos: 48, demand: map[int64]int{40: 36}, headroom: 1, cursor: 0,
			sweeps: []sweep{
				{capacity: map[int64]int{40: 1}, chosen: 40, deferred: 35},
				{capacity: map[int64]int{40: 1}, chosen: 40, deferred: 35},
			},
		},
		{
			name:  "headroom 2 serves demand once and spends the rest on one exploratory probe",
			repos: 5, demand: map[int64]int{4: 3}, headroom: 2, cursor: 0,
			sweeps: []sweep{
				{capacity: map[int64]int{1: 1, 4: 1}, chosen: 1, deferred: 2},
			},
		},
		{
			name:  "headroom 1 rotates among the repositories with demand and never probes",
			repos: 10, demand: map[int64]int{3: 1, 6: 1, 9: 1}, headroom: 1, cursor: 0,
			sweeps: []sweep{
				{capacity: map[int64]int{3: 1}, chosen: 3, deferred: 2},
				{capacity: map[int64]int{6: 1}, chosen: 6, deferred: 2},
				{capacity: map[int64]int{9: 1}, chosen: 9, deferred: 2},
				{capacity: map[int64]int{3: 1}, chosen: 3, deferred: 2},
			},
		},
		{
			name:  "no demand anywhere still rotates the exploratory probe through every repository",
			repos: 4, demand: map[int64]int{}, headroom: 1, cursor: 0,
			sweeps: []sweep{
				{capacity: map[int64]int{1: 1}, chosen: 1},
				{capacity: map[int64]int{2: 1}, chosen: 2},
				{capacity: map[int64]int{3: 1}, chosen: 3},
				{capacity: map[int64]int{4: 1}, chosen: 4},
				{capacity: map[int64]int{1: 1}, chosen: 1},
			},
		},
		{
			// A later demand pass must not pull the cursor back to the busy
			// repository, or every sweep probes the same idle neighbour and the
			// rest are never advertised at all.
			name:  "spare headroom beyond demand walks the probe through the idle repositories",
			repos: 5, demand: map[int64]int{1: 5}, headroom: 3, cursor: 0,
			sweeps: []sweep{
				{capacity: map[int64]int{1: 2, 2: 1}, chosen: 2, deferred: 3},
				{capacity: map[int64]int{1: 2, 3: 1}, chosen: 3, deferred: 3},
				{capacity: map[int64]int{1: 2, 4: 1}, chosen: 4, deferred: 3},
				{capacity: map[int64]int{1: 2, 5: 1}, chosen: 5, deferred: 3},
				{capacity: map[int64]int{1: 2, 2: 1}, chosen: 2, deferred: 3},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			keys := make([]SetKey, testCase.repos)
			for i := range keys {
				keys[i] = SetKey{RepositoryID: int64(i + 1), ScaleSetID: 100 + i}
			}
			cursor := testCase.cursor
			for i, want := range testCase.sweeps {
				plan, chosen, granted := planCapacity(keys, map[int64]int{}, testCase.demand, testCase.headroom, cursor)
				for _, key := range keys {
					if plan.Capacity[key] != want.capacity[key.RepositoryID] {
						t.Fatalf("sweep %d from cursor %d: repository %d planned %d, want %d (capacity %v)",
							i, cursor, key.RepositoryID, plan.Capacity[key], want.capacity[key.RepositoryID], want.capacity)
					}
				}
				if !granted || chosen != want.chosen {
					t.Fatalf("sweep %d from cursor %d: chosen %d granted %v, want %d", i, cursor, chosen, granted, want.chosen)
				}
				if plan.Deferred != want.deferred {
					t.Fatalf("sweep %d from cursor %d: deferred %d, want %d", i, cursor, plan.Deferred, want.deferred)
				}
				cursor = chosen
			}
		})
	}
}

// Occupancy above admission stays an error. It is not demand: the journal
// charges workers the platform no longer admits, which means admission shrank
// underneath running workers and no plan can describe that.
func TestAdvertisementRefusesOccupancyAboveAdmission(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	activateTestSessions(t, s, keys)
	offer(t, s, keys[0], 1)
	mustReserve(t, s)
	assignJobs(t, s, keys[0], 0)
	// The shrink a quarantine or a smaller backend observation performs, applied
	// directly because no public call can drop admission below a charged lease.
	s.mu.Lock()
	s.admission = 0
	s.mu.Unlock()
	if _, err := s.PlanAdvertisements(ctx, keys); err == nil {
		t.Fatal("occupancy above admission was planned rather than refused")
	}
}

func TestSDKAdvertisementPlanDrivesEverySessionPoll(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	firstFixture := newSDKFixture(t, s, keys[0])
	firstSession := firstFixture.open()
	secondFixture := newSDKFixture(t, s, keys[1])
	secondSession := secondFixture.open()
	sessions := map[SetKey]*QueueSession{keys[0]: firstSession, keys[1]: secondSession}
	for range 2 {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if envelope, err := sessions[key].Poll(ctx, plan.Capacity[key]); err != nil || envelope != nil {
				t.Fatalf("poll %v at %d: %+v %v", key, plan.Capacity[key], envelope, err)
			}
		}
	}
	firstFixture.configure(func() {
		if len(firstFixture.caps) != 2 || firstFixture.caps[0] != "1" || firstFixture.caps[1] != "0" {
			t.Errorf("first repository did not rotate through capacity and zero: %v", firstFixture.caps)
		}
	})
	secondFixture.configure(func() {
		if len(secondFixture.caps) != 2 || secondFixture.caps[0] != "0" || secondFixture.caps[1] != "1" {
			t.Errorf("second repository did not rotate through zero and capacity: %v", secondFixture.caps)
		}
	})
}

// TestAdvertisementRESTDemandRecoversRepositoriesTheSessionsNeverSaw
// reproduces the outage homelab#913 measured 2026-09-23: eight subscriptions,
// admission two, and two repositories holding real queued work that no
// session ever observed -- exactly the shape a job queued while the queue
// sessions were deaf, or during a restart, leaves behind. Without a REST
// reading, both are invisible to journaledDemand (no assigned-job row) and to
// the scale-set statistic (never observed), so only the one-per-sweep
// exploratory slot can ever reach them -- eight sweeps to cycle through eight
// repositories. A fresh REST reading must recover both within the first two
// sweeps instead.
func TestAdvertisementRESTDemandRecoversRepositoriesTheSessionsNeverSaw(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 8)
	activateTestSessions(t, s, keys)
	// The two repositories GitHub actually has queued jobs for. Neither
	// receives a journaled row nor a statistics update: today's planner has no
	// way to see them at all.
	held := []SetKey{keys[6], keys[7]}
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{held[0].RepositoryID: 1, held[1].RepositoryID: 1}, now, now.Add(time.Hour))
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 2}); err != nil {
		t.Fatal(err)
	}
	if _, admission := s.Admission(); admission != 2 {
		t.Fatalf("fixture admission is %d, want 2", admission)
	}
	advertisedEver := map[SetKey]bool{}
	for sweep := range 2 {
		plan, err := s.PlanAdvertisements(ctx, keys)
		if err != nil {
			t.Fatalf("sweep %d: %v", sweep, err)
		}
		for key, capacity := range plan.Capacity {
			if capacity > 0 {
				advertisedEver[key] = true
			}
		}
	}
	for _, key := range held {
		if !advertisedEver[key] {
			t.Fatalf("repository %d never received advertised capacity within two sweeps: %+v", key.RepositoryID, advertisedEver)
		}
	}
}
