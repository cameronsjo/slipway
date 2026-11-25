package controller

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// The arithmetic behind "unseen": a statistic counts jobs already running on
// this pool's workers, the REST count does not, and probes are never counted.
func TestUnseenDemandComparesEachSignalWithWhatItCounts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		demand unseenDemand
		want   int
	}{
		{"nothing reported", unseenDemand{}, 0},
		{"two queued, nothing held: the 2026-10-01 shape", unseenDemand{Queued: 2}, 2},
		{"two queued, one row offered", unseenDemand{Queued: 2, Reservable: 1}, 1},
		{"two queued, two workers starting", unseenDemand{Queued: 2, Starting: 2}, 0},
		{"a running job does not hide a queued one", unseenDemand{Queued: 1, Running: 1}, 1},
		{"assigned counts running jobs", unseenDemand{Assigned: 2, Running: 2}, 0},
		{"assigned beyond everything held", unseenDemand{Assigned: 3, Running: 1, Starting: 1}, 1},
		{"held beyond reported never goes negative", unseenDemand{Queued: 1, Reservable: 3}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.demand.unseen(); got != tc.want {
				t.Fatalf("unseen() of %+v = %d, want %d", tc.demand, got, tc.want)
			}
		})
	}
}

func TestDecideUnseenProbe(t *testing.T) {
	now := time.Date(2026, 10, 1, 21, 30, 0, 0, time.UTC)
	started := now.Add(-time.Hour)
	queued := unseenDemand{Queued: 2}
	for _, tc := range []struct {
		name      string
		demand    unseenDemand
		clock     unseenClock
		started   time.Time
		wantSince time.Time
		wantProbe unseenVerdict
	}{
		{"nothing unseen clears the clock", unseenDemand{}, unseenClock{Since: now.Add(-time.Hour)}, started, time.Time{}, unseenWait},
		{"demand first seen now starts the clock", queued, unseenClock{}, started, now, unseenWait},
		{"one second short of the window", queued, unseenClock{Since: now.Add(-unseenDemandAfter + time.Second)}, started, now.Add(-unseenDemandAfter + time.Second), unseenWait},
		{"unseen for the whole window probes", queued, unseenClock{Since: now.Add(-unseenDemandAfter)}, started, now.Add(-unseenDemandAfter), unseenProbe},
		{"a recent envelope from the set holds it off", queued, unseenClock{Since: now.Add(-time.Hour), LastEnvelope: now.Add(-time.Minute)}, started, now.Add(-time.Hour), unseenWait},
		{"a process that just started holds it off", queued, unseenClock{Since: now.Add(-time.Hour)}, now.Add(-time.Minute), now.Add(-time.Hour), unseenWait},
		{"a live probe holds it off", unseenDemand{Queued: 2, ProbeLive: true}, unseenClock{Since: now.Add(-time.Hour)}, started, now.Add(-time.Hour), unseenWait},
		{"a probe inside the retry window holds it off", queued, unseenClock{Since: now.Add(-time.Hour), LastProbe: now.Add(-unseenProbeRetry + time.Second)}, started, now.Add(-time.Hour), unseenWait},
		{"a probe past the retry window allows another", queued, unseenClock{Since: now.Add(-time.Hour), LastProbe: now.Add(-unseenProbeRetry)}, started, now.Add(-time.Hour), unseenProbe},
		{"a clock from the future restarts at now", queued, unseenClock{Since: now.Add(time.Hour)}, started, now, unseenWait},
		{"one probe short of the limit still probes", queued, unseenClock{Since: now.Add(-time.Hour), Probes: unseenProbeLimit - 1}, started, now.Add(-time.Hour), unseenProbe},
		{"the limit stops it", queued, unseenClock{Since: now.Add(-time.Hour), Probes: unseenProbeLimit}, started, now.Add(-time.Hour), unseenExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			since, probe := decideUnseenProbe(tc.demand, tc.clock, tc.started, now)
			if !since.Equal(tc.wantSince) || probe != tc.wantProbe {
				t.Fatalf("decideUnseenProbe = (%s, %d), want (%s, %d)", since, probe, tc.wantSince, tc.wantProbe)
			}
		})
	}
}

// unseenFixture is the 2026-10-01 shape: an active session, nothing journaled,
// no lease, and a fresh REST read counting `queued` pool jobs. The process and
// the demand are both older than unseenDemandAfter, so only what each test
// adds decides whether a probe is journaled.
func unseenFixture(t *testing.T, queued int) (*Store, string, SetKey) {
	t.Helper()
	s, dir := openTestStore(t, 4)
	keys := prepare(t, s, 1)
	activateTestSessions(t, s, keys)
	if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 4}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{keys[0].RepositoryID: queued}, now, now.Add(time.Hour))
	s.mu.Lock()
	s.started = now.Add(-time.Hour)
	s.unseenSince = map[SetKey]time.Time{keys[0]: now.Add(-unseenDemandAfter - time.Second)}
	s.mu.Unlock()
	return s, dir, keys[0]
}

func mustProbe(t *testing.T, s *Store, key SetKey) []UnseenProbe {
	t.Helper()
	probes, err := s.ProbeUnseenDemand(ctx, []SetKey{key})
	if err != nil {
		t.Fatal(err)
	}
	return probes
}

// The incident, end to end inside the journal: queued jobs no envelope named
// become one probe row, the ordinary reservation takes it, and its acquisition
// settles under its own source with no GitHub call.
func TestQueuedJobsWithNoEnvelopeGetOneProbeWorker(t *testing.T) {
	s, _, key := unseenFixture(t, 2)
	probes := mustProbe(t, s, key)
	if len(probes) != 1 || probes[0].Set != key || !isUnseenJobKey(probes[0].JobKey) || probes[0].Demand != 2 {
		t.Fatalf("probes = %+v, want one probe for %v carrying demand 2", probes, key)
	}
	if again := mustProbe(t, s, key); len(again) != 0 {
		t.Fatalf("a second sweep journaled %+v while the first probe was still offered", again)
	}
	lease := mustReserve(t, s)
	if lease.JobKey != probes[0].JobKey || lease.Set != key || lease.RequestID != 0 {
		t.Fatalf("reserved %+v, want the probe row %q", lease, probes[0].JobKey)
	}
	if err := s.AcquireReserved(ctx, lease.ID, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := s.AcquisitionEvidence(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Source != AcquisitionSourceUnseenDemand || evidence.Outcome != AcquisitionAccepted {
		t.Fatalf("probe acquisition evidence = %+v, want accepted under %q", evidence, AcquisitionSourceUnseenDemand)
	}
	if again := mustProbe(t, s, key); len(again) != 0 {
		t.Fatalf("a sweep journaled %+v while the probe's lease was live", again)
	}
}

// A set GitHub is still talking to is served by the envelope path.
func TestARecentEnvelopeFromTheSetHoldsTheProbeOff(t *testing.T) {
	s, _, key := unseenFixture(t, 2)
	statistics := scaleset.RunnerScaleSetStatistic{}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 7, Statistics: statistics}); err != nil {
		t.Fatal(err)
	}
	if probes := mustProbe(t, s, key); len(probes) != 0 {
		t.Fatalf("journaled %+v one moment after the set delivered an envelope", probes)
	}
}

// Work the journal already holds is not unseen: two offered rows cover two
// queued jobs.
func TestJournaledRowsCoverTheReportedDemand(t *testing.T) {
	s, _, key := unseenFixture(t, 2)
	s.mu.Lock()
	since := s.unseenSince[key]
	s.mu.Unlock()
	offer(t, s, key, 2)
	// offer delivered an envelope; take that instant away so only the rows decide.
	s.mu.Lock()
	delete(s.setEnvelope, key)
	s.unseenSince[key] = since
	s.mu.Unlock()
	if probes := mustProbe(t, s, key); len(probes) != 0 {
		t.Fatalf("journaled %+v for demand two offered rows already cover", probes)
	}
}

// A current statistic is demand too -- upstream's whole signal -- and it counts
// jobs already running, so a running lease covers one.
func TestACurrentAssignedStatisticIsDemand(t *testing.T) {
	s, _, key := unseenFixture(t, 0)
	statistics, err := json.Marshal(scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`,
		string(statistics), timestamp(), key.RepositoryID, key.ScaleSetID); err != nil {
		t.Fatal(err)
	}
	if probes := mustProbe(t, s, key); len(probes) != 1 {
		t.Fatalf("a current TotalAssignedJobs=1 with nothing held journaled %d probes, want 1", len(probes))
	}
}

// Without any reported demand nothing is journaled, however long it is quiet.
func TestNoReportedDemandNoProbe(t *testing.T) {
	s, _, key := unseenFixture(t, 0)
	if probes := mustProbe(t, s, key); len(probes) != 0 {
		t.Fatalf("journaled %+v with nothing reported", probes)
	}
	s.mu.Lock()
	_, kept := s.unseenSince[key]
	s.mu.Unlock()
	if kept {
		t.Fatal("the unseen clock kept running with nothing unseen")
	}
}

// A probe is this process's measurement. The next process must not inherit it
// as demand to serve.
func TestAProbeRowIsNotTrustedAcrossARestart(t *testing.T) {
	s, dir, key := unseenFixture(t, 2)
	probes := mustProbe(t, s, key)
	if len(probes) != 1 {
		t.Fatalf("fixture journaled %d probes, want 1", len(probes))
	}
	reopened := reopenTestStore(t, s, dir, 4)
	var state string
	if err := reopened.db.QueryRow(`SELECT state FROM jobs WHERE job_key=?`, probes[0].JobKey).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "unavailable" {
		t.Fatalf("an unreserved probe row came back as %q after a restart, want unavailable", state)
	}
}

// The two inferred sources never stand in for each other.
func TestProbeAndBrokerSettlementsAreNotInterchangeable(t *testing.T) {
	probe := Lease{ID: "l1", JobKey: unseenJobKeyPrefix + "x", Phase: AcquisitionIntent}
	broker := Lease{ID: "l2", JobKey: jobKeyStringPrefix + "x", Phase: AcquisitionIntent}
	for _, tc := range []struct {
		name   string
		lease  Lease
		source string
		ok     bool
	}{
		{"probe as unseen demand", probe, AcquisitionSourceUnseenDemand, true},
		{"probe as broker assignment", probe, AcquisitionSourceBrokerAssignment, false},
		{"broker job as unseen demand", broker, AcquisitionSourceUnseenDemand, false},
		{"broker job as broker assignment", broker, AcquisitionSourceBrokerAssignment, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAcquisitionSettlement(tc.lease, AcquisitionSettlement{JobKey: tc.lease.JobKey, Outcome: AcquisitionAccepted, Source: tc.source}, nil, 0, false)
			if (err == nil) != tc.ok {
				t.Fatalf("validate %s = %v, want ok=%t", tc.name, err, tc.ok)
			}
		})
	}
	err := validateAcquisitionSettlement(probe, AcquisitionSettlement{JobKey: probe.JobKey, Outcome: AcquisitionCompleted, Source: AcquisitionSourceUnseenDemand}, nil, 0, false)
	if err == nil || !strings.Contains(err.Error(), "only record acceptance") {
		t.Fatalf("a probe completion was accepted: %v", err)
	}
}

// quietSessions answers one sweep for one set with the given outcome.
type quietSessions struct {
	cycleSessions
	set     SetKey
	outcome PollOutcome
}

func (s quietSessions) PollSweep(ctx context.Context) ([]PollResult, int, error) {
	*s.calls = append(*s.calls, "poll")
	return []PollResult{{Set: s.set, MaxCapacity: 2, Outcome: s.outcome}}, 0, nil
}

func probeRows(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_key LIKE ?`, unseenJobKeyPrefix+"%").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// The cycle journals the probe after a quiet sweep and before provisioning, so
// the same cycle serves it. Only a set whose poll answered an empty 202 is a
// candidate: one that delivered a message is being heard.
func TestTheCycleProbesOnlySetsThatHeardNothing(t *testing.T) {
	for _, tc := range []struct {
		outcome PollOutcome
		probes  int
	}{{PollEmpty, 1}, {PollMessage, 0}, {PollOther, 0}} {
		t.Run(string(tc.outcome), func(t *testing.T) {
			s, _, key := unseenFixture(t, 2)
			var calls []string
			daemon, err := newDaemon(cycleRegistry{calls: &calls}, quietSessions{cycleSessions{calls: &calls}, key, tc.outcome}, &cycleLifecycle{calls: &calls}, 4)
			if err != nil {
				t.Fatal(err)
			}
			daemon.store = s
			result, err := daemon.Cycle(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if result.Summary.UnseenProbes != tc.probes || probeRows(t, s) != tc.probes {
				t.Fatalf("outcome %s: summary probes=%d rows=%d, want %d", tc.outcome, result.Summary.UnseenProbes, probeRows(t, s), tc.probes)
			}
			poll, provision := slices.Index(calls, "poll"), slices.Index(calls, "provision")
			if poll < 0 || provision < poll {
				t.Fatalf("provisioning did not follow the sweep: %v", calls)
			}
		})
	}
}

// reserveProbe takes the offered probe through the ordinary reservation and
// acquisition, as the cycle's provisioning does.
func reserveProbe(t *testing.T, s *Store, jobKey string) Lease {
	t.Helper()
	lease := mustReserve(t, s)
	if lease.JobKey != jobKey {
		t.Fatalf("reserved %q, want the probe %q", lease.JobKey, jobKey)
	}
	if err := s.AcquireReserved(ctx, lease.ID, nil); err != nil {
		t.Fatal(err)
	}
	return lease
}

// A probe is not demand, as a row or as a lease. Counting either made the
// reported demand look served while the probe stood, which cleared the unseen
// clock and held the next probe off for a further unseenDemandAfter. The
// probe's lease is created after the REST read, which is exactly the lease
// leasesCreatedSince takes off the count.
func TestAProbeDoesNotRestartTheUnseenClock(t *testing.T) {
	s, _, key := unseenFixture(t, 1)
	s.mu.Lock()
	since := s.unseenSince[key]
	s.mu.Unlock()
	first := mustProbe(t, s, key)
	if len(first) != 1 {
		t.Fatalf("fixture journaled %d probes, want 1", len(first))
	}
	if again := mustProbe(t, s, key); len(again) != 0 {
		t.Fatalf("journaled %+v while the first probe was offered", again)
	}
	lease := reserveProbe(t, s, first[0].JobKey)
	if again := mustProbe(t, s, key); len(again) != 0 {
		t.Fatalf("journaled %+v while the first probe's lease was live", again)
	}
	s.mu.Lock()
	kept := s.unseenSince[key]
	s.mu.Unlock()
	if !kept.Equal(since) {
		t.Fatalf("the unseen clock moved from %s to %s under its own probe", since, kept)
	}
	// The probe found nothing and is released; the retry window has passed.
	if _, err := s.db.Exec(`UPDATE leases SET phase='released' WHERE id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.unseenProbed[key] = time.Now().UTC().Add(-unseenProbeRetry)
	s.mu.Unlock()
	if next := mustProbe(t, s, key); len(next) != 1 {
		t.Fatalf("the demand still unseen since before the first probe got %d probes once the retry window passed, want 1", len(next))
	}
}

// expireProbe releases a probe's lease or retires its offered row, and moves
// the retry window past, so the next sweep is decided by the probe count alone.
func expireProbe(t *testing.T, s *Store, key SetKey, jobKey string) {
	t.Helper()
	if _, err := s.db.Exec(`UPDATE jobs SET state='unavailable' WHERE job_key=?`, jobKey); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.unseenProbed[key] = time.Now().UTC().Add(-unseenProbeRetry)
	s.mu.Unlock()
}

// homelab#1216 review: demand this pool can never serve kept a probe going every
// retry window forever. After unseenProbeLimit probes with no envelope between
// them the set stops, says so once, and starts again on its next envelope.
func TestProbingStopsAfterTheLimitUntilTheSetIsHeard(t *testing.T) {
	logs := captureLogs(t)
	s, _, key := unseenFixture(t, 1)
	for n := 1; n <= unseenProbeLimit; n++ {
		probes := mustProbe(t, s, key)
		if len(probes) != 1 {
			t.Fatalf("probe %d: journaled %d, want 1", n, len(probes))
		}
		expireProbe(t, s, key, probes[0].JobKey)
	}
	for range 2 {
		if probes := mustProbe(t, s, key); len(probes) != 0 {
			t.Fatalf("journaled %+v past the limit", probes)
		}
	}
	if got := strings.Count(logs.String(), "Stopped probing a scale set"); got != 1 {
		t.Fatalf("the stop was logged %d times, want once: %s", got, logs.String())
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 9}); err != nil {
		t.Fatal(err)
	}
	// Heard: the count resets. The envelope also restarts the quiet period, so
	// age it past unseenDemandAfter to isolate the count.
	s.mu.Lock()
	s.setEnvelope[key] = time.Now().UTC().Add(-unseenDemandAfter)
	s.mu.Unlock()
	if probes := mustProbe(t, s, key); len(probes) != 1 {
		t.Fatalf("after an envelope the set got %d probes, want 1", len(probes))
	}
}

// The probe's purpose, end to end in the journal: GitHub hands the probe's
// runner a job no envelope had named, and that job's JobStarted and
// JobCompleted advance the probe's own lease by runner identity.
func TestARealJobOnTheProbeRunnerAdvancesTheProbeLease(t *testing.T) {
	s, _, key := unseenFixture(t, 1)
	probes := mustProbe(t, s, key)
	if len(probes) != 1 {
		t.Fatalf("fixture journaled %d probes, want 1", len(probes))
	}
	lease := reserveProbe(t, s, probes[0].JobKey)
	if err := s.Transition(ctx, lease.ID, Acquired, Provisioning); err != nil {
		t.Fatal(err)
	}
	now := timestamp()
	if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?,?,?,?,?)`, lease.ID, []byte("test"), "stream-written", 501, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
VALUES(?,?,?,?,?,?)`, lease.ID, lease.RunnerName, "present", now, now, now); err != nil {
		t.Fatal(err)
	}
	brokerEvent := func(kind scaleset.MessageType) Event {
		e := Event{JobMessageBase: scaleset.JobMessageBase{JobMessageType: scaleset.JobMessageType{MessageType: kind},
			OwnerName: "owner", RepositoryName: "repo1", JobID: "unannounced-job"}}
		e.RunnerID, e.RunnerName = 501, lease.RunnerName
		return e
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 20, Events: []Event{brokerEvent(scaleset.MessageTypeJobStarted)}}); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Running {
		t.Fatalf("the real job's start left the probe lease at %+v %v, want running", stored, err)
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 21, Events: []Event{brokerEvent(scaleset.MessageTypeJobCompleted)}}); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != CleanupPending {
		t.Fatalf("the real job's completion left the probe lease at %+v %v, want cleanup-pending", stored, err)
	}
}

// A probe row the stranded-lease backstop demoted to assigned-stale is retired
// at Open too, so it cannot count as advertised demand in the next process.
func TestAStaleProbeRowIsRetiredAtRestart(t *testing.T) {
	s, dir, key := unseenFixture(t, 2)
	probes := mustProbe(t, s, key)
	if len(probes) != 1 {
		t.Fatalf("fixture journaled %d probes, want 1", len(probes))
	}
	if _, err := s.db.Exec(`UPDATE jobs SET state=? WHERE job_key=?`, jobStateAssignedStale, probes[0].JobKey); err != nil {
		t.Fatal(err)
	}
	reopened := reopenTestStore(t, s, dir, 4)
	var state string
	if err := reopened.db.QueryRow(`SELECT state FROM jobs WHERE job_key=?`, probes[0].JobKey).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "unavailable" {
		t.Fatalf("a stale probe row came back as %q after a restart, want unavailable", state)
	}
}

// homelab#1219: the deaf watchdog restarts a process with unseen demand about
// every ten minutes, and each process probed once, so a per-process count never
// reached the limit. The count is read from the journal and survives restarts.
func TestTheProbeLimitSurvivesRestarts(t *testing.T) {
	s, dir, key := unseenFixture(t, 1)
	for n := 1; n <= unseenProbeLimit+1; n++ {
		probes := mustProbe(t, s, key)
		want := 1
		if n > unseenProbeLimit {
			want = 0
		}
		if len(probes) != want {
			t.Fatalf("process %d journaled %d probes, want %d", n, len(probes), want)
		}
		if want == 1 {
			expireProbe(t, s, key, probes[0].JobKey)
		}
		// The deaf exit and the restart: a new process, its own clocks, the
		// same journal and REST demand.
		s = reopenTestStore(t, s, dir, 4)
		if err := s.ReconcileBackendInventory(ctx, BackendInventory{Complete: true, FreeSlots: 4}); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		s.RecordRESTDemand(map[int64]int{key.RepositoryID: 1}, now, now.Add(time.Hour))
		s.mu.Lock()
		s.started = now.Add(-time.Hour)
		s.unseenSince = map[SetKey]time.Time{key: now.Add(-unseenDemandAfter - time.Second)}
		s.mu.Unlock()
	}
}
