package controller

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// homelab#1167: a close-intent left by a session DELETE whose outcome was lost
// used to degrade its set forever. Once it has been idle for
// closeIntentRetryAfter it now gets exactly one fresh DELETE per window.

// strandCloseIntent writes the row an uncertain DELETE leaves behind for the
// fixture's current session UUID, last touched idle ago.
func strandCloseIntent(t *testing.T, s *Store, f *sdkFixture, owner string, idle time.Duration) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	f.configure(func() { id = f.id })
	stamp := time.Now().UTC().Add(-idle).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,session_id,phase,created,updated) VALUES(?,?,?,?,'close-intent',?,?)`,
		owner, f.key.RepositoryID, f.key.ScaleSetID, id.String(), stamp, stamp); err != nil {
		t.Fatal(err)
	}
	return id
}

func ageCloseIntent(t *testing.T, s *Store, owner string, idle time.Duration) {
	t.Helper()
	stamp := time.Now().UTC().Add(-idle).Format(time.RFC3339Nano)
	if _, err := s.db.Exec(`UPDATE sessions SET updated=? WHERE owner=? AND phase='close-intent'`, stamp, owner); err != nil {
		t.Fatal(err)
	}
}

func TestCloseIntentRetryDue(t *testing.T) {
	now := time.Date(2026, 10, 4, 14, 0, 0, 0, time.UTC)
	for name, tc := range map[string]struct {
		updated time.Time
		want    bool
	}{
		"zero stamp":         {time.Time{}, false},
		"just written":       {now, false},
		"one second short":   {now.Add(-closeIntentRetryAfter + time.Second), false},
		"exactly the window": {now.Add(-closeIntentRetryAfter), true},
		"days stuck":         {now.Add(-72 * time.Hour), true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := closeIntentRetryDue(tc.updated, now); got != tc.want {
				t.Fatalf("closeIntentRetryDue(%v) = %v, want %v", tc.updated, got, tc.want)
			}
		})
	}
}

// A 204 or the DELETE's own 404 records the intent closed, and the same Sync
// creates a new session for the set.
func TestStaleCloseIntentResolves(t *testing.T) {
	for name, status := range map[string]int{"deleted": http.StatusNoContent, "already gone": http.StatusNotFound} {
		t.Run(name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			markManaged(t, s, []SetKey{key})
			fixture := newSDKFixture(t, s, key)
			server := rotateOnDelete(t, fixture)
			fixture.configure(func() { server.deleteStatus = status })
			old := strandCloseIntent(t, s, fixture, "stranded-owner", 2*closeIntentRetryAfter)
			manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
			if err := manager.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if degraded := manager.Degraded(); len(degraded) != 0 {
				t.Fatalf("degraded after the close retry: %+v", degraded)
			}
			if got := deleteCount(fixture, old.String()); got != 1 {
				t.Fatalf("DELETEs for the stranded session = %d, want 1", got)
			}
			if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 1 {
				t.Fatalf("session POSTs = %d, want one new session", got)
			}
			if phases := sessionPhases(t, s, key); len(phases) != 2 || phases[0] != "closed" || phases[1] != "active" {
				t.Fatalf("session phases = %v, want [closed active]", phases)
			}
			if got := activeSessionID(t, s, key); got == old.String() {
				t.Fatal("the new active session reuses the closed UUID")
			}
		})
	}
}

// A DELETE that keeps failing costs at most one request per window: the intent
// is kept, restamped, and nothing creates a replacement.
func TestStaleCloseIntentHeldOnPersistentFailure(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() { server.deleteStatus = http.StatusServiceUnavailable })
	old := strandCloseIntent(t, s, fixture, "stranded-owner", 2*closeIntentRetryAfter)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	const windows, cyclesPerWindow = 3, 5
	for window := 1; window <= windows; window++ {
		for cycle := 1; cycle <= cyclesPerWindow; cycle++ {
			if err := manager.Sync(ctx); err != nil {
				t.Fatalf("window %d cycle %d: %v", window, cycle, err)
			}
			degraded := manager.Degraded()
			if len(degraded) != 1 || degraded[0].Set != key || !strings.Contains(degraded[0].Err.Error(), "session close outcome is uncertain") {
				t.Fatalf("window %d cycle %d: degraded = %+v, want the close-intent set", window, cycle, degraded)
			}
			if got := deleteCount(fixture, old.String()); got != window {
				t.Fatalf("window %d cycle %d: DELETEs = %d, want one per window (%d)", window, cycle, got, window)
			}
			if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 0 {
				t.Fatalf("window %d cycle %d: session POSTs = %d, want no replacement", window, cycle, got)
			}
			if phases := sessionPhases(t, s, key); len(phases) != 1 || phases[0] != "close-intent" {
				t.Fatalf("window %d cycle %d: session phases = %v, want the intent kept", window, cycle, phases)
			}
		}
		ageCloseIntent(t, s, "stranded-owner", closeIntentRetryAfter)
	}
}

// An intent younger than the window is never retried, by Sync or directly.
func TestFreshCloseIntentIsNotRetried(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	rotateOnDelete(t, fixture)
	old := strandCloseIntent(t, s, fixture, "stranded-owner", closeIntentRetryAfter-time.Minute)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	for cycle := 1; cycle <= 2; cycle++ {
		if err := manager.Sync(ctx); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		degraded := manager.Degraded()
		if len(degraded) != 1 || !strings.Contains(degraded[0].Err.Error(), "retried with one fresh DELETE after") {
			t.Fatalf("cycle %d: degraded = %+v, want the not-yet-due close-intent", cycle, degraded)
		}
	}
	if err := s.retryCloseIntent(ctx, key, fixture.client); err == nil {
		t.Fatal("retryCloseIntent retried a close-intent before its window")
	}
	if got := deleteCount(fixture, old.String()); got != 0 {
		t.Fatalf("DELETEs = %d, want none before the window", got)
	}
}

// A 404 on the admin-token refresh means the DELETE was never sent. The retry
// must not read it as "already gone": the intent stays, and no new session is
// created over a session GitHub may still hold.
func TestStaleCloseIntentKeptOnTokenRefresh404(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() {
		fixture.adminTTL = 30 * time.Second
		server.tokenStatus = http.StatusNotFound
	})
	old := strandCloseIntent(t, s, fixture, "stranded-owner", 2*closeIntentRetryAfter)
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := deleteCount(fixture, old.String()); got != 0 {
		t.Fatalf("the fixture is not the shape under test: %d session DELETEs were sent", got)
	}
	if degraded := manager.Degraded(); len(degraded) != 1 || degraded[0].Set != key {
		t.Fatalf("degraded = %+v, want the set", degraded)
	}
	if phases := sessionPhases(t, s, key); len(phases) != 1 || phases[0] != "close-intent" {
		t.Fatalf("a token-refresh 404 was recorded as a close: %v", phases)
	}
	if got := fixture.count(http.MethodPost, fixture.sessionPath()); got != 0 {
		t.Fatalf("session POSTs = %d, want no replacement", got)
	}
}

func readCloseIntent(t *testing.T, s *Store, key SetKey) closeIntentRow {
	t.Helper()
	var row closeIntentRow
	if err := s.db.QueryRow(`SELECT owner,session_id,updated FROM sessions WHERE repo_id=? AND set_id=? AND phase='close-intent'`,
		key.RepositoryID, key.ScaleSetID).Scan(&row.owner, &row.sessionID, &row.updated); err != nil {
		t.Fatal(err)
	}
	return row
}

// Two callers that both read the intent before either claims it must send one
// DELETE between them. The first DELETE answers 503, so the intent is still a
// close-intent when the second racer, holding the stale read, tries to claim
// it: only the compare-and-swap on updated refuses that second claim.
func TestRacingCloseIntentClaimsSendOneDelete(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() { server.deleteStatus = http.StatusServiceUnavailable })
	old := strandCloseIntent(t, s, fixture, "stranded-owner", 2*closeIntentRetryAfter)
	stale := readCloseIntent(t, s, key)
	if err := s.retryReadCloseIntent(ctx, key, fixture.client, stale); err == nil {
		t.Fatal("first racer: a 503 DELETE was accepted")
	}
	err := s.retryReadCloseIntent(ctx, key, fixture.client, stale)
	if err == nil || !strings.Contains(err.Error(), "fenced by a concurrent claim") {
		t.Fatalf("second racer: err = %v, want the concurrent-claim refusal", err)
	}
	if got := deleteCount(fixture, old.String()); got != 1 {
		t.Fatalf("DELETEs = %d, want exactly one between the two racers", got)
	}
}

// The DELETE succeeds but recording the close fails. The intent must stay
// (restamped), nothing replaces the session that window, and the next
// window's DELETE, now answered 404, records it closed.
func TestCloseIntentKeptWhenTheCloseCommitFails(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	var deletes int
	fixture.configure(func() {
		fixture.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != http.MethodDelete || !strings.HasPrefix(r.URL.Path, fixture.sessionPath()+"/") {
				return false
			}
			deletes++
			if deletes == 1 {
				// The DELETE lands at GitHub; the journal write after it fails.
				if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
					t.Errorf("write fault: %v", err)
				}
				w.WriteHeader(http.StatusNoContent)
				return true
			}
			fixture.id = uuid.New() // The next create gets a fresh session.
			w.WriteHeader(http.StatusNotFound)
			return true
		}
	})
	old := strandCloseIntent(t, s, fixture, "stranded-owner", 2*closeIntentRetryAfter)
	err := s.retryCloseIntent(ctx, key, fixture.client)
	if err == nil || !strings.Contains(err.Error(), "close could not be recorded") {
		t.Fatalf("err = %v, want the failed close commit", err)
	}
	if _, err := s.db.Exec(`PRAGMA query_only=OFF`); err != nil {
		t.Fatal(err)
	}
	if phases := sessionPhases(t, s, key); len(phases) != 1 || phases[0] != "close-intent" {
		t.Fatalf("session phases = %v, want the intent kept", phases)
	}
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 1 {
		t.Fatalf("degraded inside the window = %+v, want the set", degraded)
	}
	if got := deleteCount(fixture, old.String()); got != 1 {
		t.Fatalf("DELETEs inside the window = %d, want 1", got)
	}
	ageCloseIntent(t, s, "stranded-owner", closeIntentRetryAfter)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := manager.Degraded(); len(degraded) != 0 {
		t.Fatalf("degraded after the 404 retry: %+v", degraded)
	}
	if got := deleteCount(fixture, old.String()); got != 2 {
		t.Fatalf("DELETEs = %d, want 2 (204 with a failed commit, then 404)", got)
	}
	if phases := sessionPhases(t, s, key); len(phases) != 2 || phases[0] != "closed" || phases[1] != "active" {
		t.Fatalf("session phases = %v, want [closed active]", phases)
	}
}
