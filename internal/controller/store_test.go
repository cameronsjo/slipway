package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/google/uuid"
)

var ctx = context.Background()

// testAllowlist covers every repository name the controller tests discover.
// Eligibility now requires allowlist membership as well as Private, so a
// fixture that forgets either one is refused -- which is the point of the gate
// and is asserted directly in TestEligibilityRequiresPrivateAndAllowlist.
var testAllowlist = func() []string {
	entries := []string{
		"owner/repo1", "owner/private", "owner/archive", "owner/new-canary",
		"owner/renamed", "owner/second", "owner/new-grant", "owner/public",
	}
	for n := 1; n <= 16; n++ {
		entries = append(entries, fmt.Sprintf("owner/repo%d", n))
	}
	return entries
}()

// testRepository builds a discovered repository the allowlist admits. Private
// is true because every fixture repository stands in for a private one; a
// public fixture is written explicitly where a test needs the refusal.
func testRepository(id int64, fullName string) githubapp.Repository {
	return githubapp.Repository{ID: id, FullName: fullName, Private: true}
}

func openTestStore(t *testing.T, capacity int) (*Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir, Config{Platform: "linux", InstallationID: 42, Capacity: capacity, RepositoryAllowlist: testAllowlist})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func prepare(t *testing.T, s *Store, repositories int) []SetKey {
	t.Helper()
	var repos []githubapp.Repository
	var keys []SetKey
	for n := 1; n <= repositories; n++ {
		repos = append(repos, testRepository(int64(n), fmt.Sprintf("owner/repo%d", n)))
		keys = append(keys, SetKey{RepositoryID: int64(n), ScaleSetID: 100 + n})
	}
	if err := s.Discover(ctx, repos); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := s.BindSet(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	return keys
}

func offer(t *testing.T, s *Store, key SetKey, count int) {
	t.Helper()
	e := Envelope{MessageID: 1, Statistics: scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: count}}
	for n := 1; n <= count; n++ {
		e.Events = append(e.Events, event(key, int64(n), scaleset.MessageTypeJobAvailable))
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), e); err != nil {
		t.Fatal(err)
	}
}

// event builds one request-keyed wire event. The job ID is derived from the
// request ID because GitHub's are unique per job: a shared literal would make
// every fixture job resolve to the same durable row, which is the identity
// conflict the journal-backed resolver refuses.
func event(key SetKey, request int64, kind scaleset.MessageType) Event {
	return Event{JobMessageBase: scaleset.JobMessageBase{JobMessageType: scaleset.JobMessageType{MessageType: kind},
		RunnerRequestID: request, OwnerName: "owner", RepositoryName: fmt.Sprintf("repo%d", key.RepositoryID),
		JobID: fmt.Sprintf("workflow-job-%d", request)}}
}

// mustJobKey resolves an event's durable job key the same way the journal does.
func mustJobKey(t *testing.T, e Event) string {
	t.Helper()
	key, err := jobKey(e)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustReserve(t *testing.T, s *Store) Lease {
	t.Helper()
	lease, err := s.ReserveNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func finish(t *testing.T, s *Store, lease Lease) {
	t.Helper()
	current, err := s.Lease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Phase == Reserved {
		if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
			t.Fatal(err)
		}
		if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
			JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI,
		}); err != nil {
			t.Fatal(err)
		}
		current.Phase = Acquired
	}
	if err := s.Transition(ctx, lease.ID, current.Phase, CleanupPending); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishCleanup(ctx, lease.ID, CleanupEvidence{true, true, true}); err != nil {
		t.Fatal(err)
	}
}

// S9, homelab#926 and homelab#960: the Linux ceiling is sixteen concurrent
// workers. Seventeen is refused by name so that raising the ceiling past the
// slot set turns this test red. Capacity sixteen is the positive control:
// without it a journal that never opens would also pass, and it goes red if the
// ceiling falls back to four.
func TestLinuxCapacityOfSeventeenIsRefusedByOpen(t *testing.T) {
	accepted, err := Open(filepath.Join(t.TempDir(), "at-ceiling"),
		Config{Platform: "linux", InstallationID: 42, Capacity: 16, RepositoryAllowlist: testAllowlist})
	if err != nil {
		t.Fatalf("the Linux ceiling of sixteen was refused: %v", err)
	}
	accepted.Close()

	refused, err := Open(filepath.Join(t.TempDir(), "above-ceiling"),
		Config{Platform: "linux", InstallationID: 42, Capacity: 17, RepositoryAllowlist: testAllowlist})
	if err == nil {
		refused.Close()
		t.Fatal("controller.Open accepted a Linux capacity of 17")
	}
	if !strings.Contains(err.Error(), "qualification capacity") {
		t.Fatalf("capacity 17 was refused for the wrong reason: %v", err)
	}
}

func TestRoundRobinAndLoneRepositoryCapacity(t *testing.T) {
	t.Run("round robin", func(t *testing.T) {
		s, _ := openTestStore(t, 2)
		keys := prepare(t, s, 2)
		for _, key := range keys {
			offer(t, s, key, 20)
		}
		for n := 0; n < 2; n++ {
			lease := mustReserve(t, s)
			if lease.Set.RepositoryID != int64(n%2+1) || lease.Slot != n {
				t.Fatalf("unfair or overlapping allocation: %+v", lease)
			}
		}
		if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
			t.Fatalf("overcommitted: %v", err)
		}
	})
	t.Run("lone repository", func(t *testing.T) {
		s, _ := openTestStore(t, 2)
		key := prepare(t, s, 1)[0]
		offer(t, s, key, 20)
		for n := 0; n < 2; n++ {
			if lease := mustReserve(t, s); lease.Set != key {
				t.Fatal(lease)
			}
		}
	})
}

func TestConcurrentAllocationsCannotExceedCap(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 50)
	var wg sync.WaitGroup
	results := make(chan error, 50)
	for range 50 {
		wg.Go(func() { _, err := s.ReserveNext(ctx); results <- err })
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrCapacity) {
			t.Fatal(err)
		}
	}
	if accepted != 2 {
		t.Fatalf("accepted %d allocations", accepted)
	}
}

func TestWaitingRepositoryGetsNextFreedSlotAcrossRestart(t *testing.T) {
	s, dir := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	offer(t, s, keys[0], 20)
	first := mustReserve(t, s)
	offer(t, s, keys[1], 20)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("restart trusted old inventory: %v", err)
	}
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("missing worker freed reservation: %v", err)
	}
	finish(t, restarted, first)
	if next := mustReserve(t, restarted); next.Set != keys[1] {
		t.Fatalf("persisted fairness cursor starved waiting repository: %+v", next)
	}
}

type acquireFunc func(context.Context, []int64) ([]int64, error)

func (f acquireFunc) AcquireJobs(ctx context.Context, ids []int64) ([]int64, error) {
	return f(ctx, ids)
}

func TestAcquireIntentIsDurableAndUnknownIsNeverReplayed(t *testing.T) {
	for _, failure := range []string{"lost-response", "wrong-id", "duplicate-id", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			offer(t, s, key, 2)
			lease := mustReserve(t, s)
			calls := 0
			apiCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			api := acquireFunc(func(_ context.Context, ids []int64) ([]int64, error) {
				calls++
				stored, err := s.Lease(ctx, lease.ID)
				if err != nil || stored.Phase != AcquisitionIntent || len(ids) != 1 || ids[0] != lease.RequestID {
					t.Fatalf("API ran without exact durable intent: %+v %v", stored, err)
				}
				switch failure {
				case "wrong-id":
					return []int64{ids[0] + 1}, nil
				case "duplicate-id":
					return []int64{ids[0], ids[0]}, nil
				case "cancelled":
					cancel()
					return nil, context.Canceled
				default:
					return nil, errors.New("server acted, response lost")
				}
			})
			if err := s.AcquireReserved(apiCtx, lease.ID, api); err == nil {
				t.Fatal("uncertain operation passed")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := Open(dir, s.config)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			stored, err := restarted.Lease(ctx, lease.ID)
			if err != nil || stored.Phase != AcquisitionUnknown {
				t.Fatalf("uncertainty lost: %+v %v", stored, err)
			}
			if err := restarted.AcquireReserved(ctx, lease.ID, api); err == nil || calls != 1 {
				t.Fatalf("replayed uncertain mutation: calls=%d error=%v", calls, err)
			}
			if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
				t.Fatal(err)
			}
			if err := restarted.AcquireReserved(ctx, lease.ID, api); err == nil || calls != 1 {
				t.Fatalf("replayed uncertain mutation after inventory reconciliation: calls=%d error=%v", calls, err)
			}
			if _, err := restarted.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
				t.Fatalf("replaced uncertain operation: %v", err)
			}
		})
	}
}

func TestEveryLifecyclePhaseAndCleanupFailureRetainCapacity(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 2)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.SettleAcquisition(ctx, lease.ID, AcquisitionSettlement{
		JobKey: lease.JobKey, Outcome: AcquisitionAccepted, Source: AcquisitionSourceAPI,
	}); err != nil {
		t.Fatal(err)
	}
	lease.Phase = Acquired
	for _, phase := range []Phase{Provisioning, Running, CleanupPending, Quarantined, CleanupPending} {
		if err := s.Transition(ctx, lease.ID, lease.Phase, phase); err != nil {
			t.Fatal(err)
		}
		lease.Phase = phase
		if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
			t.Fatalf("phase %s released capacity: %v", phase, err)
		}
	}
	for _, evidence := range []CleanupEvidence{{false, true, true}, {true, false, true}, {true, true, false}} {
		if err := s.FinishCleanup(ctx, lease.ID, evidence); err == nil {
			t.Fatalf("incomplete cleanup released slot: %+v", evidence)
		}
	}
	if err := s.FinishCleanup(ctx, lease.ID, CleanupEvidence{true, true, true}); err != nil {
		t.Fatal(err)
	}
	if next := mustReserve(t, s); next.RequestID == lease.RequestID {
		t.Fatal("released lease replayed an obsolete acquisition offer")
	}
}

func TestAcquireSuccessAndDefiniteDeclineRemainBounded(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			offer(t, s, key, 2)
			lease := mustReserve(t, s)
			api := acquireFunc(func(_ context.Context, ids []int64) ([]int64, error) {
				if accepted {
					return ids, nil
				}
				return nil, nil
			})
			if err := s.AcquireReserved(ctx, lease.ID, api); err != nil {
				t.Fatal(err)
			}
			stored, err := s.Lease(ctx, lease.ID)
			if err != nil || (accepted && stored.Phase != Acquired) || (!accepted && stored.Phase != CleanupPending) {
				t.Fatalf("wrong acquisition result: %+v %v", stored, err)
			}
			if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
				t.Fatal("acquisition result bypassed cleanup")
			}
		})
	}
}

func TestDiscoveryRevocationAndUnknownWorkersFenceAllocation(t *testing.T) {
	s, _ := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 3)
	lease := mustReserve(t, s)
	if err := s.Discover(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("revoked repository allocated new worker: %v", err)
	}
	if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(context.Context, []int64) ([]int64, error) {
		t.Fatal("acquired after known repository revocation")
		return nil, nil
	})); err == nil {
		t.Fatal("known revocation allowed new acquisition")
	}
	active, err := s.ActiveLeases(ctx)
	if err != nil || len(active) != 1 || active[0].ID != lease.ID {
		t.Fatalf("revocation discarded live work: %+v %v", active, err)
	}
	for _, inventory := range [][]string{{"orphan"}, {lease.ID, lease.ID}} {
		if err := s.ReconcileInventory(ctx, true, inventory); err == nil {
			t.Fatal("invalid inventory accepted")
		}
		if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrInventoryUnknown) {
			t.Fatalf("unknown inventory supplied free capacity: %v", err)
		}
	}
}

func TestLegacyReservationLockAndConfigIdentity(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	config := Config{Platform: "macos", InstallationID: 42, Capacity: 2, LegacySlots: 1, RepositoryAllowlist: testAllowlist}
	s, err := Open(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 3)
	mustReserve(t, s)
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatal("legacy guest not charged")
	}
	if other, err := Open(dir, config); err == nil {
		other.Close()
		t.Fatal("second controller obtained allocation lock")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The lease reserved above is still live, so dropping the legacy reservation
	// is DEFERRED, not refused: refusing to open would strand the pool, since
	// nothing but a live controller can release that lease. What this pins is
	// that the restart does not silently take the reservation away underneath a
	// charged slot -- the process keeps running on the journal's 2/1.
	// TestBusyJournalDefersNewSizing covers the deferral itself.
	config.LegacySlots = 0
	other, err := Open(dir, config)
	if err != nil {
		t.Fatalf("a busy journal refused to open, which strands the running lease: %v", err)
	}
	defer other.Close()
	if capacity, legacy := other.PlatformSlots(); capacity != 2 || legacy != 1 {
		t.Fatalf("restart silently removed legacy reservation: effective sizing is %d/%d", capacity, legacy)
	}
}

type ackFunc func(context.Context, int) error

func (f ackFunc) DeleteMessage(ctx context.Context, id int) error { return f(ctx, id) }

func wire(t *testing.T, id int, events any, assigned int) []byte {
	t.Helper()
	body, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"messageId": id, "messageType": "RunnerScaleSetJobMessages", "body": string(body),
		"statistics": scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: assigned}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReceivePersistsBeforeAckAndLostAckSurvivesRestart(t *testing.T) {
	s, dir := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	session := uuid.New()
	data := wire(t, 7, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}, 100)
	calls := 0
	api := ackFunc(func(_ context.Context, id int) error {
		calls++
		pending, err := s.PendingMessages(ctx)
		if err != nil || len(pending) != 1 || pending[0].MessageID != id {
			t.Fatalf("ack without durable envelope: %+v %v", pending, err)
		}
		return errors.New("DELETE response lost")
	})
	if err := s.Receive(ctx, key, session, data, api); err == nil || calls != 1 {
		t.Fatal("failed ACK not reported")
	}
	if cursor, err := s.Cursor(ctx, key, session); err != nil || cursor != 0 {
		t.Fatalf("cursor advanced over failed ACK: %d %v", cursor, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Receive(ctx, key, session, data, ackFunc(func(context.Context, int) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	mustReserve(t, restarted)
	if _, err := restarted.ReserveNext(ctx); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("redelivery duplicated an acquisition offer: %v", err)
	}
	statistics, err := restarted.Statistics(ctx, key)
	if err != nil || statistics.TotalAssignedJobs != 100 {
		t.Fatalf("truncated event batch replaced aggregate demand: %+v %v", statistics, err)
	}
	if cursor, err := restarted.Cursor(ctx, key, uuid.New()); err != nil || cursor != 0 {
		t.Fatal("cursor copied into replacement session")
	}
}

func TestWireOrderUnknownEventsAndCredentialOmission(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	available := event(key, 1, scaleset.MessageTypeJobAvailable)
	completed := event(key, 1, scaleset.MessageTypeJobCompleted)
	completed.Result = "canceled"
	// The same workflow job/request can be offered after cancellation. Preserve
	// actual order rather than SDK's Available/Assigned/Started/Completed slices.
	data := wire(t, 1, []Event{completed, available}, 1)
	if err := s.Receive(ctx, key, uuid.New(), data, ackFunc(func(context.Context, int) error { return nil })); err != nil {
		t.Fatal(err)
	}
	mustReserve(t, s)
	unknown := available
	unknown.MessageType = "FutureUnrecognizedEvent"
	if err := s.Receive(ctx, key, uuid.New(), wire(t, 2, []Event{unknown}, 1), ackFunc(func(context.Context, int) error {
		t.Fatal("unknown event acknowledged")
		return nil
	})); err == nil {
		t.Fatal("unknown event silently dropped")
	}
	secret := "must-not-persist-queue-url-token"
	data = wire(t, 3, []map[string]any{{"messageType": "JobAvailable", "ownerName": "owner", "repositoryName": "repo1", "runnerRequestId": 2,
		"acquireJobUrl": "https://example.invalid/?token=" + secret}}, 1)
	if err := s.Receive(ctx, key, uuid.New(), data, ackFunc(func(context.Context, int) error { return nil })); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil || bytes.Contains(data, []byte(secret)) {
			t.Fatalf("credential URL reached journal or inspection failed: %s %v", entry.Name(), err)
		}
	}
}

func TestFailedPersistenceCannotAcknowledgeOrAcquire(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 2)
	lease := mustReserve(t, s)
	// An actual SQLite write rejection, not a mock that merely reports failure.
	if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
		t.Fatal(err)
	}
	if err := s.Receive(ctx, key, uuid.New(), wire(t, 2, []Event{event(key, 3, scaleset.MessageTypeJobAvailable)}, 3),
		ackFunc(func(context.Context, int) error { t.Fatal("ACK after failed persistence"); return nil })); err == nil {
		t.Fatal("read-only inbox write passed")
	}
	if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(context.Context, []int64) ([]int64, error) {
		t.Fatal("API acquisition after failed intent persistence")
		return nil, nil
	})); err == nil {
		t.Fatal("read-only intent write passed")
	}
}

func TestChangedRedeliveryAndWrongRepositoryCannotAcknowledge(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	session := uuid.New()
	first := Envelope{MessageID: 1, Events: []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}}
	if err := s.RecordEnvelope(ctx, key, session, first); err != nil {
		t.Fatal(err)
	}
	for _, value := range []Event{event(key, 2, scaleset.MessageTypeJobAvailable), event(SetKey{RepositoryID: 2}, 1, scaleset.MessageTypeJobAvailable)} {
		if err := s.Receive(ctx, key, session, wire(t, 1, []Event{value}, 1), ackFunc(func(context.Context, int) error {
			t.Fatal("changed delivery acknowledged")
			return nil
		})); err == nil {
			t.Fatal("changed delivery passed")
		}
	}
}

func TestDuplicateEnvelopeIdentityIsValidatedBeforeStatisticsChange(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	session := uuid.New()
	event := event(key, 1, scaleset.MessageTypeJobAvailable)
	first := Envelope{MessageID: 9, Statistics: scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1}, Events: []Event{event}}
	if err := s.RecordEnvelope(ctx, key, session, first); err != nil {
		t.Fatal(err)
	}
	conflict := first
	conflict.Statistics.TotalAssignedJobs = 99
	if err := s.RecordEnvelope(ctx, key, session, conflict); err == nil {
		t.Fatal("changed duplicate statistics were accepted")
	}
	statistics, err := s.Statistics(ctx, key)
	if err != nil || statistics.TotalAssignedJobs != 1 {
		t.Fatalf("changed duplicate mutated statistics: %+v %v", statistics, err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`, key.RepositoryID, key.ScaleSetID, mustJobKey(t, event)).Scan(&state); err != nil || state != jobStateQueued {
		t.Fatalf("changed duplicate mutated job state: %q %v", state, err)
	}
}

func TestStaleAvailabilityAndAssignmentPreserveDurableJobStates(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	protected := []string{string(Reserved), string(Acquired), string(Running), string(CleanupPending), jobStateTerminal}
	for i, state := range protected {
		requestID := int64(100 + i)
		previous := event(key, requestID, scaleset.MessageTypeJobAvailable)
		if state == jobStateTerminal {
			previous = event(key, requestID, scaleset.MessageTypeJobCompleted)
			previous.Result = "success"
		}
		payload, err := json.Marshal(previous)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO jobs(repo_id,set_id,job_key,runner_request_id,state,payload) VALUES(?,?,?,?,?,?)`,
			key.RepositoryID, key.ScaleSetID, mustJobKey(t, previous), requestID, state, string(payload)); err != nil {
			t.Fatal(err)
		}
		for n, kind := range []scaleset.MessageType{scaleset.MessageTypeJobAvailable, scaleset.MessageTypeJobAssigned} {
			incoming := event(key, requestID, kind)
			if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 20 + i*2 + n, Events: []Event{incoming}}); err != nil {
				t.Fatal(err)
			}
			var actual string
			if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`, key.RepositoryID, key.ScaleSetID, mustJobKey(t, incoming)).Scan(&actual); err != nil {
				t.Fatal(err)
			}
			if actual != state {
				t.Fatalf("stale %s changed %s to %s", kind, state, actual)
			}
		}
	}
}

func TestCanceledTerminalJobCanBeReofferedInLaterEnvelope(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	requestID := int64(200)
	completed := event(key, requestID, scaleset.MessageTypeJobCompleted)
	completed.Result = "canceled"
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 30, Events: []Event{completed}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), Envelope{MessageID: 31, Events: []Event{event(key, requestID, scaleset.MessageTypeJobAvailable)}}); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`, key.RepositoryID, key.ScaleSetID, mustJobKey(t, completed)).Scan(&state); err != nil || state != jobStateQueued {
		t.Fatalf("canceled job was not reoffered: %q %v", state, err)
	}
}

// TestRefusalCountSurvivesARestart pins homelab#1139: the quarantine count is
// journaled, so a controller that restarts between redeliveries still reaches
// the threshold instead of starting over and looping forever. A change of
// reason restarts the count, because quarantine is for one kind of refusal.
func TestRefusalCountSurvivesARestart(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	session := uuid.New()
	for want := 1; want <= 2; want++ {
		if got, err := s.RecordRefusal(ctx, key, session, 9, refusalForeignRepository, refusedShape{}); err != nil || got != want {
			t.Fatalf("refusal %d counted as %d: %v", want, got, err)
		}
	}
	restarted := reopenTestStore(t, s, dir, 1)
	if got, err := restarted.RecordRefusal(ctx, key, session, 9, refusalForeignRepository, refusedShape{}); err != nil || got != refusalQuarantineThreshold {
		t.Fatalf("the third refusal after a restart counted as %d: %v", got, err)
	}
	if got, err := restarted.RecordRefusal(ctx, key, session, 9, refusalInvalidEnvelope, refusedShape{}); err != nil || got != 1 {
		t.Fatalf("a different refusal reason continued the count at %d: %v", got, err)
	}
}

// TestLivenessOnlyRecordsCommittedEnvelope pins homelab#1097 half A: a refused
// envelope must not count as heard, and only a committed one -- including a
// redelivery already applied -- may move Liveness forward.
func TestLivenessOnlyRecordsCommittedEnvelope(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	if last, _ := s.Liveness(); !last.IsZero() {
		t.Fatalf("liveness set before any envelope: %v", last)
	}
	wrongRepo := Envelope{MessageID: 1, Events: []Event{event(SetKey{RepositoryID: 2}, 1, scaleset.MessageTypeJobAvailable)}}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), wrongRepo); err == nil {
		t.Fatal("wrong-repository envelope was accepted")
	}
	if last, _ := s.Liveness(); !last.IsZero() {
		t.Fatalf("liveness moved on a refused envelope: %v", last)
	}
	valid := Envelope{MessageID: 1, Events: []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}}
	session := uuid.New()
	if err := s.RecordEnvelope(ctx, key, session, valid); err != nil {
		t.Fatal(err)
	}
	if last, _ := s.Liveness(); last.IsZero() {
		t.Fatal("liveness did not move on a committed envelope")
	}

	t.Run("redelivery already applied still counts as heard", func(t *testing.T) {
		s.mu.Lock()
		s.lastEnvelope = time.Time{}
		s.mu.Unlock()
		if err := s.RecordEnvelope(ctx, key, session, valid); err != nil {
			t.Fatal(err)
		}
		if last, _ := s.Liveness(); last.IsZero() {
			t.Fatal("liveness did not move on a redelivered envelope already applied")
		}
	})
}

func TestCursorCannotSkipPendingEarlierMessage(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	session := uuid.New()
	for _, id := range []int{10, 20} {
		if err := s.RecordEnvelope(ctx, key, session, Envelope{MessageID: id}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkAcknowledged(ctx, key, session, 20); err != nil {
		t.Fatal(err)
	}
	if cursor, err := s.Cursor(ctx, key, session); err != nil || cursor != 0 {
		t.Fatalf("cursor skipped pending message: %d %v", cursor, err)
	}
	if err := s.MarkAcknowledged(ctx, key, session, 10); err != nil {
		t.Fatal(err)
	}
	if cursor, err := s.Cursor(ctx, key, session); err != nil || cursor != 20 {
		t.Fatalf("cursor did not advance after ACKs: %d %v", cursor, err)
	}
}
