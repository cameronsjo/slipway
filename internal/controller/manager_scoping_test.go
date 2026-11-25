package controller

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// offerRequest journals one queued job for key with a request ID unique across
// the test, so two sets' jobs never resolve to the same durable row.
func offerRequest(t *testing.T, s *Store, key SetKey, request int64) {
	t.Helper()
	e := Envelope{MessageID: 1, Statistics: scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 1},
		Events: []Event{event(key, request, scaleset.MessageTypeJobAvailable)}}
	if err := s.RecordEnvelope(ctx, key, uuid.New(), e); err != nil {
		t.Fatal(err)
	}
}

// Review of #1000, item 1: a set whose poll failed this cycle is left out of
// reservation, as a degraded set is, and another set's job is still reserved.
func TestAPollFailedSetIsNotReservedThatCycle(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{keys[0]: newSDKFixture(t, s, keys[0]), keys[1]: newSDKFixture(t, s, keys[1])}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	fixtures[keys[0]].configure(func() {
		fixtures[keys[0]].override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodGet && r.URL.Path == "/queue" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			return false
		}
	})
	if _, _, err := manager.PollSweep(ctx); !onlySetPollErrors(err) {
		t.Fatalf("sweep error = %v, want only a per-set poll failure", err)
	}
	offerRequest(t, s, keys[0], 1)
	offerRequest(t, s, keys[1], 2)
	lease, err := manager.AcquireNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Set != keys[1] {
		t.Fatalf("reserved %v, want the set whose poll succeeded (%v)", lease.Set, keys[1])
	}
	if lease, err := manager.AcquireNext(ctx); !errors.Is(err, ErrNoDemand) || lease.ID != "" {
		t.Fatalf("the poll-failed set was reserved: lease=%+v err=%v", lease, err)
	}
}

// Review of #1000, item 3: the cycle context ending while a session create
// POST is in flight must not strand a create-intent. The open runs detached
// with its own bound, so the create completes and commits.
func TestACycleEndingDuringASessionCreateDoesNotStrandItsIntent(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	cycleCtx, endCycle := context.WithTimeout(ctx, time.Hour)
	defer endCycle()
	fixture.configure(func() {
		fixture.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPost && r.URL.Path == fixture.sessionPath() {
				// The cycle ends while GitHub is still answering the create.
				endCycle()
			}
			return false
		}
	})
	if err := manager.Sync(cycleCtx); err != nil {
		t.Fatal(err)
	}
	if cycleCtx.Err() == nil {
		t.Fatal("the fixture never ended the cycle during the create")
	}
	if degraded := manager.Degraded(); len(degraded) != 0 {
		t.Fatalf("the create was cut by the cycle: %+v", degraded)
	}
	records, err := s.PendingSessions(ctx)
	if err != nil || len(records) != 1 || records[0].Phase != "active" {
		t.Fatalf("session after the cycle ended mid-create: %+v %v, want one active", records, err)
	}
}

// Review of #1000, item 3: with less than the open bound left before the
// cycle deadline, Sync starts no new create. The set waits for the next cycle
// with no POST and no create-intent row.
func TestANewSessionIsNotStartedTooCloseToTheCycleDeadline(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	cycleCtx, cancel := context.WithTimeout(ctx, sessionOpenTimeout/2)
	defer cancel()
	if err := manager.Sync(cycleCtx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 1 || degraded[0].Set != key {
		t.Fatalf("degraded = %+v, want the deferred set", degraded)
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 0 {
		t.Fatalf("a create started with too little time left: %d POSTs", got)
	}
	if records, err := s.PendingSessions(ctx); err != nil || len(records) != 0 {
		t.Fatalf("a deferred create left journal rows: %+v %v", records, err)
	}
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 0 {
		t.Fatalf("the next cycle did not create the session: %+v", degraded)
	}
}
