package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
)

func markManaged(t *testing.T, s *Store, keys []SetKey) {
	t.Helper()
	for _, key := range keys {
		if _, err := s.db.Exec(`INSERT INTO managed_sets(repo_id,name,remote_id,phase,updated) VALUES(?,?,?,'active',?)`,
			key.RepositoryID, fmt.Sprintf("owned-%d", key.RepositoryID), key.ScaleSetID, timestamp()); err != nil {
			t.Fatal(err)
		}
	}
}

func fixtureManager(t *testing.T, s *Store, fixtures map[SetKey]*sdkFixture) *SessionManager {
	t.Helper()
	manager, err := newSessionManager(s,
		func(ctx context.Context, key SetKey) (*QueueSession, error) {
			fixture := fixtures[key]
			if fixture == nil {
				return nil, errors.New("unexpected open key")
			}
			return s.openQueueSession(ctx, key, fixture.client, fixture.server.URL)
		},
		func(ctx context.Context, key SetKey) (*QueueSession, error) {
			fixture := fixtures[key]
			if fixture == nil {
				return nil, errors.New("unexpected resume key")
			}
			return s.resumeQueueSession(ctx, key, fixture.client, fixture.server.URL)
		},
		func(ctx context.Context, key SetKey) error {
			fixture := fixtures[key]
			if fixture == nil {
				return errors.New("unexpected close-retry key")
			}
			return s.retryCloseIntent(ctx, key, fixture.client)
		})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestSessionManagerCompleteFairSweepAndAcquisition(t *testing.T) {
	s, _ := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{}
	for _, key := range keys {
		fixtures[key] = newSDKFixture(t, s, key)
	}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if got := fixtures[key].count(http.MethodPost, fixtures[key].sessionPath()); got != 1 {
			t.Fatalf("session %v opened %d times", key, got)
		}
	}
	fixtures[keys[0]].configure(func() {
		fixtures[keys[0]].wire = wireEnvelope(t, []Event{event(keys[0], 71, scaleset.MessageTypeJobAvailable)})
	})
	first, deferred, err := manager.PollSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deferred != 0 {
		t.Fatalf("an idle sweep deferred %d", deferred)
	}
	if len(first) != 2 || first[0].MaxCapacity != 1 || first[1].MaxCapacity != 0 || first[0].Envelope == nil {
		t.Fatalf("first complete sweep: %+v", first)
	}
	fixtures[keys[0]].configure(func() { fixtures[keys[0]].wire = nil })
	second, _, err := manager.PollSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The first sweep journaled a queued job for keys[0], so it keeps the one
	// slot. Demand is served before the exploratory probe: keys[1] is probed
	// only when headroom exceeds the repositories with unmet demand, and a job
	// queued there reaches the demand pass through the REST demand read instead
	// (see planCapacity). The journaled row stays reservable either way, because
	// ReserveNext never consults the advertisement plan.
	if len(second) != 2 || second[0].MaxCapacity != 1 || second[1].MaxCapacity != 0 {
		t.Fatalf("second complete sweep: %+v", second)
	}
	lease, err := manager.AcquireNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Set != keys[0] {
		t.Fatalf("acquired through wrong session: %+v", lease)
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Acquired {
		t.Fatalf("acquisition was not committed: %+v %v", stored, err)
	}
}

func TestSessionManagerRestartResumesExactSessionBeforePolling(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	records, err := s.PendingSessions(ctx)
	if err != nil || len(records) != 1 || records[0].Phase != "active" {
		t.Fatalf("initial session: %+v %v", records, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	fixture.configure(func() { fixture.s = restarted })
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	recovered := fixtureManager(t, restarted, map[SetKey]*sdkFixture{key: fixture})
	if err := recovered.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if fixture.count(http.MethodPost, fixture.sessionPath()) != 1 ||
		fixture.count(http.MethodPatch, fixture.sessionPath()+"/"+fixture.id.String()) != 1 {
		t.Fatal("restart created a replacement instead of resuming the exact session")
	}
	if results, _, err := recovered.PollSweep(ctx); err != nil || len(results) != 1 {
		t.Fatalf("recovered sweep: %+v %v", results, err)
	}
}

func TestSessionManagerPollFailureDoesNotOmitAnotherRepository(t *testing.T) {
	s, _ := openTestStore(t, 1)
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
	results, _, err := manager.PollSweep(ctx)
	if err == nil || len(results) != 2 {
		t.Fatalf("failed complete sweep: %+v %v", results, err)
	}
	for _, key := range keys {
		if fixtures[key].count(http.MethodGet, "/queue") != 1 {
			t.Fatalf("repository %v was omitted after peer failure", key)
		}
	}
}

func TestSessionManagerRevokedRepositoryDoesNotStarveHealthyRepository(t *testing.T) {
	s, _ := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{keys[0]: newSDKFixture(t, s, keys[0]), keys[1]: newSDKFixture(t, s, keys[1])}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE repositories SET eligible=0 WHERE id=?`, keys[0].RepositoryID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	results, _, err := manager.PollSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Set != keys[1] {
		t.Fatalf("revoked repository altered healthy sweep: %+v", results)
	}
}

// homelab#924: a session whose repository leaves the desired set stays durable,
// but its live handle -- and the Administration:write client inside it -- is
// dropped from memory. Re-entry resumes the exact durable session through the
// resume path, which builds a fresh client, rather than reusing the old one.
func TestSessionManagerDropsTheHandleOfARevokedRepositoryAndResumesOnReentry(t *testing.T) {
	s, _ := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{keys[0]: newSDKFixture(t, s, keys[0]), keys[1]: newSDKFixture(t, s, keys[1])}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	revoked := keys[0]
	if _, err := s.db.Exec(`UPDATE repositories SET eligible=0 WHERE id=?`, revoked.RepositoryID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	_, held := manager.sessions[revoked]
	_, healthy := manager.sessions[keys[1]]
	manager.mu.Unlock()
	if held {
		t.Fatal("a revoked repository's session handle, and its Administration:write client, stayed in memory")
	}
	if !healthy {
		t.Fatal("dropping the revoked handle also dropped a healthy one")
	}
	records, err := s.PendingSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	durable := false
	for _, record := range records {
		if record.Set == revoked && record.Phase == "active" {
			durable = true
		}
	}
	if !durable {
		t.Fatalf("the durable session record did not survive the revocation: %+v", records)
	}

	if _, err := s.db.Exec(`UPDATE repositories SET eligible=1 WHERE id=?`, revoked.RepositoryID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Sync(ctx); err != nil {
		t.Fatalf("re-entry did not rebuild the session: %v", err)
	}
	fixture := fixtures[revoked]
	if fixture.count(http.MethodPost, fixture.sessionPath()) != 1 ||
		fixture.count(http.MethodPatch, fixture.sessionPath()+"/"+fixture.id.String()) != 1 {
		t.Fatal("re-entry created a replacement session instead of resuming the durable one")
	}
	if results, _, err := manager.PollSweep(ctx); err != nil || len(results) != 2 {
		t.Fatalf("re-entered repository is not polled: %+v %v", results, err)
	}
}

func TestSessionManagerPollsRepositoriesConcurrently(t *testing.T) {
	s, _ := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{keys[0]: newSDKFixture(t, s, keys[0]), keys[1]: newSDKFixture(t, s, keys[1])}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	defer func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	}()
	fixtures[keys[0]].configure(func() {
		fixtures[keys[0]].override = func(w http.ResponseWriter, rand *http.Request) bool {
			if rand.Method != http.MethodGet || rand.URL.Path != "/queue" {
				return false
			}
			close(firstStarted)
			<-releaseFirst
			w.WriteHeader(http.StatusAccepted)
			return true
		}
	})
	fixtures[keys[1]].configure(func() {
		fixtures[keys[1]].override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodGet || r.URL.Path != "/queue" {
				return false
			}
			close(secondStarted)
			w.WriteHeader(http.StatusAccepted)
			return true
		}
	})
	done := make(chan error, 1)
	go func() { _, _, err := manager.PollSweep(ctx); done <- err }()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first repository poll did not start")
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second repository waited for the first long poll")
	}
	close(releaseFirst)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSessionManagerRequiresCompleteInventoryBeforeAPI(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	if err := s.ReconcileInventory(ctx, false, nil); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("incomplete inventory result: %v", err)
	}
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); !errors.Is(err, ErrInventoryUnknown) {
		t.Fatalf("session API was not fenced by inventory: %v", err)
	}
	if fixture.count(http.MethodPost, fixture.sessionPath()) != 0 {
		t.Fatal("inventory fence allowed session creation")
	}
}

// An uncertain create-intent for one set is scoped to that set (review R1,
// D1). It is never retried -- no session POST reaches the API for it -- and it
// is listed in Degraded on every Sync. Every other set is still synced and
// polled; before this, the one uncertain set stopped all of them.
func TestSessionManagerScopesAnUncertainSessionToItsOwnSet(t *testing.T) {
	s, _ := openTestStore(t, 1)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{keys[0]: newSDKFixture(t, s, keys[0]), keys[1]: newSDKFixture(t, s, keys[1])}
	if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,phase,created,updated) VALUES(?,?,?,'create-intent',?,?)`,
		"uncertain", keys[0].RepositoryID, keys[0].ScaleSetID, timestamp(), timestamp()); err != nil {
		t.Fatal(err)
	}
	manager := fixtureManager(t, s, fixtures)
	for cycle := 1; cycle <= 2; cycle++ {
		if err := manager.Sync(ctx); err != nil {
			t.Fatalf("cycle %d: one uncertain set failed the whole sync: %v", cycle, err)
		}
		degraded := manager.Degraded()
		if len(degraded) != 1 || degraded[0].Set != keys[0] || degraded[0].Err == nil {
			t.Fatalf("cycle %d: degraded = %+v, want only %v", cycle, degraded, keys[0])
		}
		if fixtures[keys[0]].count(http.MethodPost, fixtures[keys[0]].sessionPath()) != 0 {
			t.Fatalf("cycle %d: an uncertain create was replayed", cycle)
		}
		polls, _, err := manager.PollSweep(ctx)
		if err != nil {
			t.Fatalf("cycle %d: poll sweep over the healthy set failed: %v", cycle, err)
		}
		if len(polls) != 1 || polls[0].Set != keys[1] {
			t.Fatalf("cycle %d: polled %+v, want only the healthy set %v", cycle, polls, keys[1])
		}
	}
	if fixtures[keys[1]].count(http.MethodPost, fixtures[keys[1]].sessionPath()) != 1 {
		t.Fatal("the healthy set's session was not created exactly once")
	}
}

// Scenario 1 at the session boundary: cancelling an in-flight acquisition must
// leave the physical slot charged and the request unsettled, never retried.
func TestSessionManagerCanceledAcquisitionRetainsChargedReservation(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	offer(t, s, key, 1)
	acquireCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fixture.configure(func() {
		fixture.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/acquirejobs") {
				return false
			}
			// The caller's context is canceled while the acquisition is in
			// flight and its response never arrives.
			cancel()
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("fixture cannot drop an in-flight acquisition")
				return true
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return true
			}
			conn.Close()
			return true
		}
	})
	lease, err := manager.AcquireNext(acquireCtx)
	if err == nil {
		t.Fatal("canceled acquisition reported success")
	}
	if lease.ID == "" {
		t.Fatal("cancellation discarded the durable reservation")
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != AcquisitionUnknown {
		t.Fatalf("canceled acquisition left phase %+v %v", stored, err)
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("canceled acquisition released the slot: %v", err)
	}
	acquirePath := strings.TrimSuffix(fixture.sessionPath(), "/sessions") + "/acquirejobs"
	if got := fixture.count(http.MethodPost, acquirePath); got != 1 {
		t.Fatalf("canceled acquisition was retried: %d", got)
	}
}

// Review R1 (D1): a resume failure for one set skips only that set, and the
// next Sync retries it. While it is down, AcquireNext never reserves a job for
// it (a reservation there would be stranded, charged, with no session to
// acquire through).
func TestAResumeFailureIsScopedToItsSetAndRetriedNextSync(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	fixtures := map[SetKey]*sdkFixture{keys[0]: newSDKFixture(t, s, keys[0]), keys[1]: newSDKFixture(t, s, keys[1])}
	if err := fixtureManager(t, s, fixtures).Sync(ctx); err != nil {
		t.Fatal(err)
	}
	failResume := true
	restarted, err := newSessionManager(s,
		func(ctx context.Context, key SetKey) (*QueueSession, error) {
			return s.openQueueSession(ctx, key, fixtures[key].client, fixtures[key].server.URL)
		},
		func(ctx context.Context, key SetKey) (*QueueSession, error) {
			if key == keys[0] && failResume {
				return nil, errors.New("resume answered 503")
			}
			return s.resumeQueueSession(ctx, key, fixtures[key].client, fixtures[key].server.URL)
		},
		func(ctx context.Context, key SetKey) error {
			return s.retryCloseIntent(ctx, key, fixtures[key].client)
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Sync(ctx); err != nil {
		t.Fatalf("one failed resume failed the whole sync: %v", err)
	}
	if degraded := restarted.Degraded(); len(degraded) != 1 || degraded[0].Set != keys[0] {
		t.Fatalf("degraded = %+v, want only %v", degraded, keys[0])
	}
	polls, _, err := restarted.PollSweep(ctx)
	if err != nil || len(polls) != 1 || polls[0].Set != keys[1] {
		t.Fatalf("polled %+v (%v), want only the healthy set", polls, err)
	}
	offerRequest(t, s, keys[0], 1)
	offerRequest(t, s, keys[1], 2)
	lease, err := restarted.AcquireNext(ctx)
	if err != nil || lease.Set != keys[1] {
		t.Fatalf("the healthy set's job was not reserved: lease=%+v err=%v", lease, err)
	}
	if lease, err := restarted.AcquireNext(ctx); !errors.Is(err, ErrNoDemand) || lease.ID != "" {
		t.Fatalf("a job on the degraded set was reserved: lease=%+v err=%v", lease, err)
	}
	failResume = false
	if err := restarted.Sync(ctx); err != nil {
		t.Fatalf("retry sync: %v", err)
	}
	if degraded := restarted.Degraded(); len(degraded) != 0 {
		t.Fatalf("the set was not retried and recovered: %+v", degraded)
	}
	if polls, _, err := restarted.PollSweep(ctx); err != nil || len(polls) != 2 {
		t.Fatalf("after recovery polled %d sets (%v), want 2", len(polls), err)
	}
	// Reserve through the same exclusion AcquireNext uses, without a second
	// acquisition: the SDK fixture expects one in-flight lease at a time.
	restarted.mu.Lock()
	skip := restarted.unreservableKeys()
	restarted.mu.Unlock()
	if lease, err := s.ReserveNextExcept(ctx, skip); err != nil || lease.Set != keys[0] {
		t.Fatalf("the recovered set's job was not reservable: skip=%v lease=%+v err=%v", skip, lease, err)
	}
}
