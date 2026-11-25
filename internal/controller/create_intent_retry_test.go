package controller

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// homelab#1235: a create-intent left by an uncertain session POST used to
// degrade its set forever. Once it has been idle for createIntentRetryAfter it
// now gets exactly one fresh create, and the answer to that create decides.

// strandIntent writes the row an uncertain create leaves behind, last touched
// idle ago.
func strandIntent(t *testing.T, s *Store, key SetKey, owner string, idle time.Duration) {
	t.Helper()
	stamp := time.Now().UTC().Add(-idle).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,phase,created,updated) VALUES(?,?,?,'create-intent',?,?)`,
		owner, key.RepositoryID, key.ScaleSetID, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

func intentUpdated(t *testing.T, s *Store, owner string) time.Time {
	t.Helper()
	var updated string
	if err := s.db.QueryRow(`SELECT updated FROM sessions WHERE owner=? AND phase='create-intent'`, owner).Scan(&updated); err != nil {
		t.Fatalf("create-intent %s is gone: %v", owner, err)
	}
	stamp, err := time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		t.Fatal(err)
	}
	return stamp
}

func TestCreateIntentRetryDue(t *testing.T) {
	now := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		updated time.Time
		want    bool
	}{
		"zero stamp":         {time.Time{}, false},
		"just written":       {now, false},
		"one second short":   {now.Add(-createIntentRetryAfter + time.Second), false},
		"exactly the window": {now.Add(-createIntentRetryAfter), true},
		"two days stuck":     {now.Add(-48 * time.Hour), true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := createIntentRetryDue(tc.updated, now); got != tc.want {
				t.Fatalf("createIntentRetryDue(%v) = %v, want %v", tc.updated, got, tc.want)
			}
		})
	}
}

// A 201 on the retry makes the stranded intent the set's active session, under
// the same owner, and the set leaves Degraded.
func TestStaleCreateIntentResolvesOnCreated(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	strandIntent(t, s, key, "stranded-owner", 2*createIntentRetryAfter)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 0 {
		t.Fatalf("degraded after a 201 retry: %+v", degraded)
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 1 {
		t.Fatalf("session POSTs = %d, want exactly one retry", got)
	}
	if got := activeSessionID(t, s, key); got != fixture.id.String() {
		t.Fatalf("active session = %s, want %s", got, fixture.id)
	}
	var owner string
	if err := s.db.QueryRow(`SELECT owner FROM sessions WHERE phase='active'`).Scan(&owner); err != nil || owner != "stranded-owner" {
		t.Fatalf("active owner = %q (%v), want the stranded intent's owner", owner, err)
	}
	if phases := sessionPhases(t, s, key); len(phases) != 1 || phases[0] != "active" {
		t.Fatalf("session phases = %v, want only [active]", phases)
	}
}

// ageIntent moves an intent's stamp back, standing in for a window passing.
func ageIntent(t *testing.T, s *Store, owner string, idle time.Duration) {
	t.Helper()
	stamp := time.Now().UTC().Add(-idle).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE sessions SET updated=? WHERE owner=? AND phase='create-intent'`, stamp, owner); err != nil {
		t.Fatal(err)
	}
}

// GitHub can hold the orphaned session indefinitely, answering every POST
// with 409. The set must then cost at most one POST per window: the intent is
// kept and restamped, never dropped into the ordinary every-cycle create.
func TestStaleCreateIntentHeldOnPersistentConflict(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() { server.conflicts = 1000 })
	strandIntent(t, s, key, "stranded-owner", 2*createIntentRetryAfter)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	const windows, cyclesPerWindow = 3, 5
	for window := 1; window <= windows; window++ {
		for cycle := 1; cycle <= cyclesPerWindow; cycle++ {
			if err := manager.Sync(ctx); err != nil {
				t.Fatalf("window %d cycle %d: %v", window, cycle, err)
			}
			degraded := manager.Degraded()
			if len(degraded) != 1 || degraded[0].Set != key {
				t.Fatalf("window %d cycle %d: degraded = %+v, want the set", window, cycle, degraded)
			}
			if cycle == 1 && !strings.Contains(degraded[0].Err.Error(), "409") {
				t.Fatalf("window %d: degraded reason %q does not name the 409", window, degraded[0].Err)
			}
			if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != window {
				t.Fatalf("window %d cycle %d: session POSTs = %d, want one per window (%d)", window, cycle, got, window)
			}
			if phases := sessionPhases(t, s, key); len(phases) != 1 || phases[0] != "create-intent" {
				t.Fatalf("window %d cycle %d: session phases = %v, want the intent kept", window, cycle, phases)
			}
		}
		ageIntent(t, s, "stranded-owner", createIntentRetryAfter)
	}
}

// A 409 on one retry and a 201 on the next window's retry resolves the set,
// under the stranded intent's owner.
func TestStaleCreateIntentResolvesAfterConflict(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() { server.conflicts = 1 })
	strandIntent(t, s, key, "stranded-owner", 2*createIntentRetryAfter)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 1 {
		t.Fatalf("degraded after the 409 = %+v, want the set", degraded)
	}
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 1 {
		t.Fatalf("session POSTs inside the window after a 409 = %d, want 1", got)
	}
	ageIntent(t, s, "stranded-owner", createIntentRetryAfter)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 0 {
		t.Fatalf("degraded after the 201 retry: %+v", degraded)
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 2 {
		t.Fatalf("session POSTs = %d, want 2 (409, then 201 a window later)", got)
	}
	activeSessionID(t, s, key)
	var owner string
	if err := s.db.QueryRow(`SELECT owner FROM sessions WHERE phase='active'`).Scan(&owner); err != nil || owner != "stranded-owner" {
		t.Fatalf("active owner = %q (%v), want the stranded intent's owner", owner, err)
	}
}

// Any other answer keeps the intent, restamps it, and waits a full window
// before the next attempt: a 5xx never turns into a POST every cycle.
func TestStaleCreateIntentBacksOffOnServerError(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	fixture.configure(func() {
		fixture.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPost && r.URL.Path == fixture.sessionPath() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			return false
		}
	})
	strandIntent(t, s, key, "stranded-owner", 2*createIntentRetryAfter)
	before := time.Now().UTC()
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	for cycle := 1; cycle <= 3; cycle++ {
		if err := manager.Sync(ctx); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		if degraded := manager.Degraded(); len(degraded) != 1 || degraded[0].Set != key {
			t.Fatalf("cycle %d: degraded = %+v, want the set", cycle, degraded)
		}
		if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 1 {
			t.Fatalf("cycle %d: session POSTs = %d, want the one retry only", cycle, got)
		}
	}
	if phases := sessionPhases(t, s, key); len(phases) != 1 || phases[0] != "create-intent" {
		t.Fatalf("session phases = %v, want the intent kept", phases)
	}
	if stamp := intentUpdated(t, s, "stranded-owner"); stamp.Before(before) {
		t.Fatalf("intent stamp %v was not moved forward by the attempt (before %v)", stamp, before)
	}
}

// An intent younger than the window is never retried: no POST, set degraded.
func TestFreshCreateIntentIsNotRetried(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	strandIntent(t, s, key, "stranded-owner", createIntentRetryAfter-time.Minute)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	for cycle := 1; cycle <= 2; cycle++ {
		if err := manager.Sync(ctx); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		degraded := manager.Degraded()
		if len(degraded) != 1 || !strings.Contains(degraded[0].Err.Error(), "retried with one fresh create after") {
			t.Fatalf("cycle %d: degraded = %+v, want the not-yet-due intent", cycle, degraded)
		}
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 0 {
		t.Fatalf("session POSTs = %d, want none before the window", got)
	}
	// The store refuses an early retry on its own, not only through Sync.
	if _, err := s.openQueueSession(ctx, key, fixture.client, fixture.server.URL); err == nil {
		t.Fatal("openQueueSession retried a create-intent before its window")
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 0 {
		t.Fatalf("session POSTs = %d after a direct open, want none", got)
	}
}
