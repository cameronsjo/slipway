package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// The job ID in the committed fixture. Synthetic, like every value there.
const fixtureJobID = "00000000-0000-4000-8000-000000000001"

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// assigned builds the event shape GitHub's broker actually sends: no runner
// request ID at all, identified only by the string job ID.
func assigned(key SetKey, jobID string, kind scaleset.MessageType) Event {
	e := event(key, 0, kind)
	e.RunnerRequestID = 0
	e.JobID = jobID
	return e
}

func TestJobKeyPrefersRequestIDAndFallsBackToJobID(t *testing.T) {
	long := strings.Repeat("a", maxJobIDLength+1)
	for _, tc := range []struct {
		name    string
		request int64
		jobID   string
		want    string
		refused bool
	}{
		{name: "request id wins", request: 42, jobID: "ignored-while-a-request-exists", want: "42"},
		{name: "request id alone", request: 7, want: "7"},
		{name: "job id when request is zero", jobID: fixtureJobID, want: "job:" + fixtureJobID},
		{name: "namespaces cannot collide", jobID: "42", want: "job:42"},
		{name: "neither identity", refused: true},
		{name: "negative request id", request: -1, jobID: fixtureJobID, refused: true},
		{name: "oversized job id", jobID: long, refused: true},
		{name: "job id with a space", jobID: "two words", refused: true},
		{name: "job id with a control byte", jobID: "a\x00b", refused: true},
		{name: "job id with a non-ASCII byte", jobID: "café", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := jobKey(Event{JobMessageBase: scaleset.JobMessageBase{
				RunnerRequestID: tc.request, JobID: tc.jobID}})
			if tc.refused {
				if err == nil {
					t.Fatalf("job key %q was accepted", got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("job key = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
	// The collision the prefix exists to prevent: request ID 42 and job ID "42"
	// are different jobs and must never share a key.
	withRequest, _ := jobKey(Event{JobMessageBase: scaleset.JobMessageBase{RunnerRequestID: 42}})
	withJobID, _ := jobKey(Event{JobMessageBase: scaleset.JobMessageBase{JobID: "42"}})
	if withRequest == withJobID {
		t.Fatalf("request-keyed and job-keyed identities collided on %q", withRequest)
	}
}

// The real wire shape, parsed by the real parser. Every controller test before
// this one built its envelope by hand, which is why the live payload was
// refused for a whole job's lifetime before anyone could see why.
func TestLiveJobAssignedFixtureParsesAsOneBrokerAssignedJob(t *testing.T) {
	envelope, err := ParseEnvelope(readFixture(t, "live-jobassigned-2026-09-20.json"))
	if err != nil {
		t.Fatalf("the measured live envelope was refused: %v", err)
	}
	if len(envelope.Events) != 1 {
		t.Fatalf("events = %d, want the single JobAssigned GitHub actually sent", len(envelope.Events))
	}
	e := envelope.Events[0]
	if e.MessageType != scaleset.MessageTypeJobAssigned {
		t.Fatalf("message type = %q, want JobAssigned", e.MessageType)
	}
	if e.RunnerRequestID != 0 {
		t.Fatalf("runnerRequestId = %d; the fixture must keep the measured zero", e.RunnerRequestID)
	}
	key, err := jobKey(e)
	if err != nil || key != "job:"+fixtureJobID {
		t.Fatalf("job key = %q, %v; want %q", key, err, "job:"+fixtureJobID)
	}
	if stateForJobEvent(e) != jobStateAssigned {
		t.Fatalf("state = %q, want assigned", stateForJobEvent(e))
	}
}

func TestEnvelopeRefusesAnEventWithNeitherIdentityAndNamesNoValue(t *testing.T) {
	bare := event(SetKey{RepositoryID: 1}, 0, scaleset.MessageTypeJobAssigned)
	bare.RunnerRequestID, bare.JobID = 0, ""
	// Distinctive values, so the leak assertion below cannot be satisfied by a
	// field name that happens to contain the fixture's value.
	bare.OwnerName, bare.RepositoryName = "leak-canary-owner", "leak-canary-repository"
	err := (Envelope{MessageID: 1, Events: []Event{bare}}).validate()
	if err == nil {
		t.Fatal("an event with no job identity at all was accepted")
	}
	for _, want := range []string{"request_id_present=false", "job_id_present=false", "job_id_acceptable=false"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not report %q", err, want)
		}
	}
	// Presence and booleans only: no owner, repository or job ID value may
	// appear in a message that is logged.
	for _, secret := range []string{bare.OwnerName, bare.RepositoryName} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatalf("refusal %q leaked the value %q", err, secret)
		}
	}
	// The positive control: the same event with only a job ID added is accepted,
	// so the refusal above is about identity and not about something else.
	bare.JobID = fixtureJobID
	if err := (Envelope{MessageID: 1, Events: []Event{bare}}).validate(); err != nil {
		t.Fatalf("a broker-assigned event with a job ID was refused: %v", err)
	}
}

// The whole point of the change: a job GitHub assigned directly runs end to end
// and the slot it held comes back. This fixture holds runnerRequestId at 0 for
// all three events; the split cases below cover the shapes where it does not. Before the job-key re-key the completion could not
// be found, PendingFinishedCleanup never matched, and the slot leaked forever.
func TestBrokerAssignedJobCompletesAndReleasesItsSlot(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	jobID := fixtureJobID

	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1,
		Events: []Event{assigned(key, jobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatalf("the broker-assigned envelope was refused: %v", err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`,
		key.RepositoryID, key.ScaleSetID, "job:"+jobID).Scan(&state); err != nil || state != jobStateAssigned {
		t.Fatalf("broker-assigned job state = %q, %v; want assigned", state, err)
	}

	lease := mustReserve(t, s)
	if lease.JobKey != "job:"+jobID || lease.RequestID != 0 {
		t.Fatalf("reserved lease = %q/%d; want the job key and no request ID", lease.JobKey, lease.RequestID)
	}

	if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(context.Context, []int64) ([]int64, error) {
		t.Error("AcquireJobs was called for a job with no runner request ID")
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Acquired {
		t.Fatalf("broker-assigned lease did not reach acquired: %+v %v", stored, err)
	}
	evidence, err := s.AcquisitionEvidence(ctx, lease.ID)
	if err != nil || evidence.Source != AcquisitionSourceBrokerAssignment || evidence.Outcome != AcquisitionAccepted {
		t.Fatalf("acquisition evidence = %+v, %v; want an accepted broker assignment", evidence, err)
	}
	if evidence.Source == AcquisitionSourceAPI {
		t.Fatal("an inference was recorded as GitHub's acquire response")
	}

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

	started := assigned(key, jobID, scaleset.MessageTypeJobStarted)
	started.RunnerID, started.RunnerName = 501, lease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2, Events: []Event{started}}); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Running {
		t.Fatalf("a runnerRequestId-0 start did not advance the lease: %+v %v", stored, err)
	}

	completed := assigned(key, jobID, scaleset.MessageTypeJobCompleted)
	completed.RunnerID, completed.RunnerName, completed.Result = 501, lease.RunnerName, "success"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 3, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	settled, err := s.AcquisitionEvidence(ctx, lease.ID)
	if err != nil || settled.Outcome != AcquisitionCompleted || settled.RunnerLeaseID != lease.ID {
		t.Fatalf("a runnerRequestId-0 completion did not settle the lease: %+v %v", settled, err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != CleanupPending {
		t.Fatalf("completion did not schedule cleanup: %+v %v", stored, err)
	}

	if _, err := s.BeginWorkerDestroy(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: lease.ID,
		ResourceID: lease.RunnerName, ProcessAbsent: true, StorageAbsent: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, lease.ID); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingFinishedCleanup(ctx)
	if err != nil || len(pending) != 1 || pending[0] != lease.ID {
		t.Fatalf("finished cleanup = %+v, %v; the broker-assigned lease never matched", pending, err)
	}
	if err := s.FinishWorkerCleanup(ctx, lease.ID, true, true); err != nil {
		t.Fatalf("the slot was not released: %v", err)
	}
	active, err := s.ActiveLeases(ctx)
	if err != nil || len(active) != 0 {
		t.Fatalf("active leases = %+v, %v; the slot leaked", active, err)
	}
}

// Two broker-assigned jobs in flight at once. Both carry runnerRequestId 0, so
// a lookup keyed on the request ID matches either one -- which is exactly why
// the lookup must key on the job key. A single-job test cannot see this: one
// zero matches one row by luck.
func TestConcurrentBrokerAssignedJobsSettleTheRightLease(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	leases := map[string]Lease{}
	for n, jobID := range []string{"job-alpha", "job-beta"} {
		if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1 + n,
			Events: []Event{assigned(key, jobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
			t.Fatal(err)
		}
		lease := mustReserve(t, s)
		if lease.RequestID != 0 {
			t.Fatalf("lease for %s carries request ID %d; the fixture is not broker-assigned", jobID, lease.RequestID)
		}
		if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(context.Context, []int64) ([]int64, error) {
			t.Error("AcquireJobs was called for a broker-assigned job")
			return nil, nil
		})); err != nil {
			t.Fatal(err)
		}
		if err := s.Transition(ctx, lease.ID, Acquired, Provisioning); err != nil {
			t.Fatal(err)
		}
		now := timestamp()
		if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?,?,?,?,?)`, lease.ID, []byte("test"), "stream-written", 600+n, now, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
VALUES(?,?,?,?,?,?)`, lease.ID, lease.RunnerName, "present", now, now, now); err != nil {
			t.Fatal(err)
		}
		leases[jobID] = lease
	}

	// Only the second job completes.
	beta := leases["job-beta"]
	completed := assigned(key, "job-beta", scaleset.MessageTypeJobCompleted)
	completed.RunnerID, completed.RunnerName, completed.Result = 601, beta.RunnerName, "success"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 10, Events: []Event{completed}}); err != nil {
		t.Fatalf("the completion was refused or settled the wrong lease: %v", err)
	}

	settled, err := s.AcquisitionEvidence(ctx, beta.ID)
	if err != nil || settled.Outcome != AcquisitionCompleted || settled.JobKey != beta.JobKey {
		t.Fatalf("beta evidence = %+v, %v; want a completion against its own job key", settled, err)
	}
	if stored, err := s.Lease(ctx, beta.ID); err != nil || stored.Phase != CleanupPending {
		t.Fatalf("beta did not enter cleanup: %+v %v", stored, err)
	}

	alpha := leases["job-alpha"]
	untouched, err := s.AcquisitionEvidence(ctx, alpha.ID)
	if err != nil || untouched.Outcome != AcquisitionAccepted || untouched.Source != AcquisitionSourceBrokerAssignment {
		t.Fatalf("alpha evidence = %+v, %v; beta's completion reached the wrong lease", untouched, err)
	}
	if stored, err := s.Lease(ctx, alpha.ID); err != nil || stored.Phase != Provisioning {
		t.Fatalf("alpha phase = %+v, %v; want the still-running job left alone", stored, err)
	}
}

// GitHub has already bound an assigned job to this scale set and times the
// assignment out if nothing picks it up, so it outranks every queued job -- one
// of which is deliberately older here.
func TestAssignedJobIsReservedAheadOfAnOlderQueuedJob(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1) // request 1: queued, and the oldest row in the table.
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2,
		Events: []Event{assigned(key, fixtureJobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}
	first := mustReserve(t, s)
	if first.JobKey != "job:"+fixtureJobID {
		t.Fatalf("first reservation = %q; the older queued job took the assigned job's turn", first.JobKey)
	}
	second := mustReserve(t, s)
	if second.JobKey != "1" {
		t.Fatalf("second reservation = %q, want the queued job", second.JobKey)
	}
}

// FIFO still holds inside each class: two assigned jobs are served in arrival
// order. Without this the rule above would be satisfied by any order at all.
func TestAssignedJobsAreReservedInArrivalOrder(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	for n, jobID := range []string{"job-one", "job-two"} {
		if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 10 + n,
			Events: []Event{assigned(key, jobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
			t.Fatal(err)
		}
	}
	if first := mustReserve(t, s); first.JobKey != "job:job-one" {
		t.Fatalf("first reservation = %q, want the earlier assignment", first.JobKey)
	}
	if second := mustReserve(t, s); second.JobKey != "job:job-two" {
		t.Fatalf("second reservation = %q, want the later assignment", second.JobKey)
	}
}

// The advertised-slot hazard: an advertisement never promises more than the
// free admission, and the one free slot it does promise cannot be taken by an
// older queued job ahead of the assigned job it was advertised for.
func TestAdvertisementsStayWithinAdmissionAndCannotStarveAnAssignedJob(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2,
		Statistics: scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1},
		Events:     []Event{assigned(key, fixtureJobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,session_id,phase,created,updated)
VALUES('owner-1',?,?,'11111111-1111-1111-1111-111111111111','active',?,?)`,
		key.RepositoryID, key.ScaleSetID, timestamp(), timestamp()); err != nil {
		t.Fatal(err)
	}
	plan, err := s.PlanAdvertisements(ctx, []SetKey{key})
	if err != nil {
		t.Fatal(err)
	}
	_, admission := s.Admission()
	total := 0
	for _, value := range plan.Capacity {
		total += value
	}
	if total > admission {
		t.Fatalf("advertised %d against an admission of %d", total, admission)
	}
	if total == 0 {
		t.Fatal("nothing was advertised; the starvation assertion below would pass vacuously")
	}
	// Every advertised slot is now consumed in order. The assigned job must take
	// the first one, not the older queued job.
	if lease := mustReserve(t, s); lease.JobKey != "job:"+fixtureJobID {
		t.Fatalf("the advertised slot went to %q, starving the assigned job", lease.JobKey)
	}
}

// GitHub retries an assignment nobody picked up: JobCompleted result=canceled,
// then a fresh JobAssigned for the same job, up to three times. Dropping the
// second shape by rank loses the retry silently.
func TestCanceledAssignmentIsReofferedByAFreshJobAssigned(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	canceled := assigned(key, fixtureJobID, scaleset.MessageTypeJobCompleted)
	canceled.Result = "canceled"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1, Events: []Event{canceled}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2,
		Events: []Event{assigned(key, fixtureJobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`,
		key.RepositoryID, key.ScaleSetID, "job:"+fixtureJobID).Scan(&state); err != nil || state != jobStateAssigned {
		t.Fatalf("state after the retry = %q, %v; want assigned", state, err)
	}
}

// The other half of the same rule, and the one a careless widening breaks: work
// that finished successfully stays terminal, however it is re-announced.
func TestSuccessfullyCompletedJobStaysTerminalAgainstEitherReoffer(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	for n, kind := range []scaleset.MessageType{scaleset.MessageTypeJobAvailable, scaleset.MessageTypeJobAssigned} {
		jobID := "finished-" + string(rune('a'+n))
		done := assigned(key, jobID, scaleset.MessageTypeJobCompleted)
		done.Result = "success"
		if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 20 + n, Events: []Event{done}}); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 30 + n,
			Events: []Event{assigned(key, jobID, kind)}}); err != nil {
			t.Fatal(err)
		}
		var state string
		if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`,
			key.RepositoryID, key.ScaleSetID, "job:"+jobID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != jobStateTerminal {
			t.Fatalf("a %s reoffered successful work: state = %q", kind, state)
		}
	}
}

// The new evidence source is narrow on purpose: it is the controller's own
// inference, not GitHub's answer, so it may only record acceptance and only for
// a job that genuinely has no request to acquire.
func TestBrokerAssignmentEvidenceIsNarrowerThanAnAcquireResponse(t *testing.T) {
	requestKeyed := Lease{ID: "l1", JobKey: "42", RequestID: 42, Phase: AcquisitionIntent}
	brokerKeyed := Lease{ID: "l2", JobKey: "job:" + fixtureJobID, Phase: AcquisitionIntent}
	for _, tc := range []struct {
		name       string
		lease      Lease
		settlement AcquisitionSettlement
		refused    bool
	}{
		{name: "accepted broker assignment", lease: brokerKeyed, settlement: AcquisitionSettlement{
			JobKey: brokerKeyed.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceBrokerAssignment}},
		{name: "declined broker assignment", lease: brokerKeyed, refused: true, settlement: AcquisitionSettlement{
			JobKey: brokerKeyed.JobKey, Outcome: AcquisitionDeclined, Source: AcquisitionSourceBrokerAssignment}},
		{name: "completed broker assignment", lease: brokerKeyed, refused: true, settlement: AcquisitionSettlement{
			JobKey: brokerKeyed.JobKey, Outcome: AcquisitionCompleted, Source: AcquisitionSourceBrokerAssignment}},
		{name: "broker assignment carrying runner identity", lease: brokerKeyed, refused: true, settlement: AcquisitionSettlement{
			JobKey: brokerKeyed.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceBrokerAssignment,
			RunnerID: 1, RunnerName: "n", RunnerLeaseID: "l2"}},
		{name: "broker assignment for a request-keyed job", lease: requestKeyed, refused: true, settlement: AcquisitionSettlement{
			JobKey: requestKeyed.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceBrokerAssignment}},
		{name: "wrong job key", lease: brokerKeyed, refused: true, settlement: AcquisitionSettlement{
			JobKey: "job:other", Outcome: AcquisitionAccepted, Source: AcquisitionSourceBrokerAssignment}},
		{name: "acquire response still works", lease: requestKeyed, settlement: AcquisitionSettlement{
			JobKey: requestKeyed.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAcquisitionSettlement(tc.lease, tc.settlement, nil, 0, false)
			if tc.refused != (err != nil) {
				t.Fatalf("validate = %v, refused = %t", err, tc.refused)
			}
		})
	}
}

// The refresh half of the same live job, driven by the response body GitHub
// actually sends. The fixture has no statistics field at all.
func TestLiveSessionRefreshFixtureIsAcceptedWithoutStatistics(t *testing.T) {
	body := readFixture(t, "live-session-refresh-2026-09-20.json")
	var fixture scaleset.RunnerScaleSetSession
	if err := json.Unmarshal(body, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Statistics != nil {
		t.Fatal("the refresh fixture carries statistics; it no longer reproduces the measured shape")
	}
	q := &QueueSession{
		key:          SetKey{RepositoryID: 1, ScaleSetID: fixture.RunnerScaleSet.ID},
		owner:        fixture.OwnerName,
		queueOrigins: githubQueueOrigins,
		identity:     sessionIdentity{id: fixture.SessionID, queue: fixture.MessageQueueURL},
		queueSHA256:  sha256.Sum256([]byte(fixture.MessageQueueURL)),
	}
	if _, err := q.validateSession(body, true); err != nil {
		t.Fatalf("the measured refresh response was refused: %v", err)
	}
	// Creation is unchanged: a create response with no statistics is still a
	// refusal, so the widening above is scoped to the refresh alone.
	_, err := q.validateSession(body, false)
	if err == nil || !strings.Contains(err.Error(), "carries no statistics") {
		t.Fatalf("session creation without statistics = %v; want a refusal naming statistics", err)
	}
}

// The controller-level half: a refresh answering without statistics is accepted
// end to end, commits no statistics of its own, and leaves the creation
// observation in place rather than zeroing it.
func TestSDKRefreshWithoutStatisticsIsAcceptedAndCommitsNone(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "POST" && r.URL.Path == f.sessionPath() {
				session := f.session()
				session.Statistics.TotalAssignedJobs = 5
				json.NewEncoder(w).Encode(session)
				return true
			}
			return false
		}
	})
	q := f.open()
	if e, err := q.Poll(ctx, 0); err != nil || e != nil {
		t.Fatalf("idle poll: %+v %v", e, err)
	}
	if stats, err := s.Statistics(ctx, key); err != nil || stats == nil || stats.TotalAssignedJobs != 5 {
		t.Fatalf("creation statistics lost: %+v %v", stats, err)
	}
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" && r.URL.Path == "/queue" && f.counts["GET /queue"] == 2 {
				w.WriteHeader(401)
				return true
			}
			if r.Method == "PATCH" {
				session := f.session()
				session.Statistics = nil // The measured shape: no statistics on refresh.
				json.NewEncoder(w).Encode(session)
				return true
			}
			return false
		}
	})
	if e, err := q.Poll(ctx, 0); err != nil || e != nil {
		t.Fatalf("a refresh without statistics fenced the session: %+v %v", e, err)
	}
	if f.count("PATCH", f.sessionPath()+"/"+f.id.String()) != 1 {
		t.Fatal("the refresh under test never happened")
	}
	stats, err := s.Statistics(ctx, key)
	if err != nil || stats == nil || stats.TotalAssignedJobs != 5 {
		t.Fatalf("statistics after a statistics-free refresh = %+v, %v; want the creation observation intact", stats, err)
	}
}

// provisionBrokerAssigned drives one broker-assigned job to a provisioning
// lease with a live JIT runner and a present worker, which is the state the
// identity-split scenarios below start from.
func provisionBrokerAssigned(t *testing.T, s *Store, key SetKey, jobID string, runnerID int) Lease {
	t.Helper()
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1,
		Events: []Event{assigned(key, jobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}
	lease := mustReserve(t, s)
	if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(context.Context, []int64) ([]int64, error) {
		t.Error("AcquireJobs was called for a broker-assigned job")
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, Acquired, Provisioning); err != nil {
		t.Fatal(err)
	}
	now := timestamp()
	if _, err := s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,runner_id,created,expires,updated)
VALUES(?,?,?,?,?,?,?)`, lease.ID, []byte("test"), "stream-written", runnerID, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
VALUES(?,?,?,?,?,?)`, lease.ID, lease.RunnerName, "present", now, now, now); err != nil {
		t.Fatal(err)
	}
	return lease
}

func jobRowCount(t *testing.T, s *Store, key SetKey) int {
	t.Helper()
	var rows int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE repo_id=? AND set_id=?`,
		key.RepositoryID, key.ScaleSetID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

// A job's identity can change shape between events, in either direction: only
// the JobAssigned shape has ever been measured live. Keying on the event alone
// opens a second jobs row, the completion settles nothing, and the lease sits in
// provisioning until RetireUnusedWorkers destroys the worker mid-job. Identity
// must therefore be resolved against the journal.
func TestIdentitySplitAcrossEventsResolvesToOneJob(t *testing.T) {
	for _, tc := range []struct {
		name         string
		firstRequest int64
		laterRequest int64
	}{
		{name: "broker assignment then a positive request id", laterRequest: 99999},
		{name: "positive request id then a broker shape", firstRequest: 4242},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			jobID := fixtureJobID

			first := assigned(key, jobID, scaleset.MessageTypeJobAssigned)
			first.RunnerRequestID = tc.firstRequest
			if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1, Events: []Event{first}}); err != nil {
				t.Fatal(err)
			}
			lease := mustReserve(t, s)
			if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(_ context.Context, ids []int64) ([]int64, error) {
				return ids, nil
			})); err != nil {
				t.Fatal(err)
			}
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

			started := assigned(key, jobID, scaleset.MessageTypeJobStarted)
			started.RunnerRequestID = tc.laterRequest
			started.RunnerID, started.RunnerName = 501, lease.RunnerName
			if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2, Events: []Event{started}}); err != nil {
				t.Fatal(err)
			}
			if rows := jobRowCount(t, s, key); rows != 1 {
				t.Fatalf("jobs rows = %d, want 1; the changed shape opened a second durable job", rows)
			}
			if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Running {
				t.Fatalf("the started event did not reach the lease: %+v %v", stored, err)
			}

			completed := assigned(key, jobID, scaleset.MessageTypeJobCompleted)
			completed.RunnerRequestID = tc.laterRequest
			completed.RunnerID, completed.RunnerName, completed.Result = 501, lease.RunnerName, "success"
			if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 3, Events: []Event{completed}}); err != nil {
				t.Fatal(err)
			}
			settled, err := s.AcquisitionEvidence(ctx, lease.ID)
			if err != nil || settled.Outcome != AcquisitionCompleted {
				t.Fatalf("completion evidence = %+v, %v; the completion was dropped", settled, err)
			}
			if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != CleanupPending {
				t.Fatalf("lease phase = %+v, %v; want cleanup-pending", stored, err)
			}

			if _, err := s.BeginWorkerDestroy(ctx, lease.ID); err != nil {
				t.Fatal(err)
			}
			if err := s.ConfirmWorkerAbsent(ctx, WorkerAbsence{LeaseID: lease.ID,
				ResourceID: lease.RunnerName, ProcessAbsent: true, StorageAbsent: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, lease.ID); err != nil {
				t.Fatal(err)
			}
			pending, err := s.PendingFinishedCleanup(ctx)
			if err != nil || len(pending) != 1 || pending[0] != lease.ID {
				t.Fatalf("finished cleanup = %+v, %v; the split lease never matched", pending, err)
			}
			if err := s.FinishWorkerCleanup(ctx, lease.ID, true, true); err != nil {
				t.Fatalf("the slot was not released: %v", err)
			}
		})
	}
}

// The consequence that makes the split Critical rather than cosmetic: a lease
// stranded in provisioning is retired as unused five minutes later, destroying
// the worker while the job it is running is still running.
func TestRunningWorkerIsNotRetiredWhenItsRequestIDAppearsLate(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	lease := provisionBrokerAssigned(t, s, key, fixtureJobID, 501)

	started := assigned(key, fixtureJobID, scaleset.MessageTypeJobStarted)
	started.RunnerRequestID = 99999
	started.RunnerID, started.RunnerName = 501, lease.RunnerName
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2, Events: []Event{started}}); err != nil {
		t.Fatal(err)
	}
	// Well past the five-minute unused-worker deadline.
	retired, err := s.RetireUnusedWorkers(ctx, time.Now().UTC().Add(unusedWorkerTimeout+time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range retired {
		if id == lease.ID {
			t.Fatal("the worker running this job was retired as unused")
		}
	}
	// The negative control lives in TestUnusedWorkerIsStillRetiredWhenNoJobEverStarts:
	// without it this assertion would also pass against a reconciler that retires
	// nothing at all.
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Running {
		t.Fatalf("lease phase = %+v, %v; want running", stored, err)
	}
}

// RetireUnusedWorkers still does its job. Without this the test above could
// pass against a reconciler that retires nothing at all.
func TestUnusedWorkerIsStillRetiredWhenNoJobEverStarts(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	lease := provisionBrokerAssigned(t, s, key, fixtureJobID, 501)
	retired, err := s.RetireUnusedWorkers(ctx, time.Now().UTC().Add(unusedWorkerTimeout+time.Minute))
	if err != nil || len(retired) != 1 || retired[0] != lease.ID {
		t.Fatalf("retired = %+v, %v; a worker that never started was not retired", retired, err)
	}
}

// Two durable jobs, one event naming both. Merging them would attribute one
// job's completion to the other's lease, so the envelope is refused and stays
// unacknowledged for redelivery.
func TestEventMatchingTwoDurableJobsRefusesTheEnvelope(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1) // request-keyed job "1"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2,
		Events: []Event{assigned(key, fixtureJobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}
	if rows := jobRowCount(t, s, key); rows != 2 {
		t.Fatalf("jobs rows = %d, want the two distinct jobs the conflict needs", rows)
	}
	conflict := assigned(key, fixtureJobID, scaleset.MessageTypeJobStarted)
	conflict.RunnerRequestID = 1
	err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 3, Events: []Event{conflict}})
	if err == nil {
		t.Fatal("an event naming two different durable jobs was accepted")
	}
	for _, want := range []string{"two different durable jobs", "request_id_present=true", "job_id_present=true"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not report %q", err, want)
		}
	}
	if rows := jobRowCount(t, s, key); rows != 2 {
		t.Fatalf("jobs rows = %d after the refusal; it must change nothing", rows)
	}
	var pending int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE message_id=3`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatal("the refused envelope was journaled; it must not be acknowledged")
	}
}

// A jobId spelled with different case is the same job. Two rows would split it
// exactly like a shape change does.
func TestCaseVariantJobIDResolvesToOneJob(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	lower := "abcdef01-2345-4678-9abc-def012345678"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1,
		Events: []Event{assigned(key, lower, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2,
		Events: []Event{assigned(key, strings.ToUpper(lower), scaleset.MessageTypeJobStarted)}}); err != nil {
		t.Fatal(err)
	}
	if rows := jobRowCount(t, s, key); rows != 1 {
		t.Fatalf("jobs rows = %d, want 1; a case variant opened a second job", rows)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`,
		key.RepositoryID, key.ScaleSetID, "job:"+lower).Scan(&state); err != nil || state != jobStateRunning {
		t.Fatalf("state = %q, %v; the uppercase event did not reach the lowercase row", state, err)
	}
}

// The wire-shape diagnostic exists to measure what GitHub sends next. It must
// publish booleans and fixed vocabulary only -- never a value, never the key.
func TestEventShapeDiagnosticIsOptInAndPublishesNoValue(t *testing.T) {
	// The opt-in decision, pinned without an ambient environment: only "1" turns
	// it on, so a stray truthy-looking value leaves the pilot silent.
	for value, want := range map[string]bool{"1": true, "": false, "0": false, "true": false, "yes": false, " 1": false} {
		if got := eventShapeLogEnabled(value); got != want {
			t.Fatalf("eventShapeLogEnabled(%q) = %t, want %t", value, got, want)
		}
	}

	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	// Off: recording an envelope emits nothing. Set explicitly rather than read
	// from Open, so a developer running with the variable set still tests this.
	s.eventShape = nil
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 1,
		Events: []Event{assigned(key, fixtureJobID, scaleset.MessageTypeJobAssigned)}}); err != nil {
		t.Fatal(err)
	}

	var captured bytes.Buffer
	s.eventShape = slog.New(slog.NewTextHandler(&captured, nil))
	canaryJobID := "leak-canary-jobid-0001"
	started := assigned(key, canaryJobID, scaleset.MessageTypeJobStarted)
	started.RunnerName, started.RunnerID, started.Result = "leak-canary-runner", 9, "leak-canary-result"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 2, Events: []Event{started}}); err != nil {
		t.Fatal(err)
	}
	line := captured.String()
	if line == "" {
		t.Fatal("an enabled diagnostic wrote nothing")
	}
	for _, want := range []string{"message_type=JobStarted", "request_id_present=false", "job_id_present=true",
		"runner_name_present=true", "result_present=true", "key_namespace=job", "resolution=created"} {
		if !strings.Contains(line, want) {
			t.Fatalf("diagnostic %q does not report %q", line, want)
		}
	}
	for _, secret := range []string{canaryJobID, "leak-canary-runner", "leak-canary-result",
		started.OwnerName, started.RepositoryName, "job:"} {
		if strings.Contains(line, secret) {
			t.Fatalf("diagnostic %q leaked the value %q", line, secret)
		}
	}
}
