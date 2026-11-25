package controller

import (
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// rotatingServer makes an sdkFixture answer the session API the way GitHub
// does once a session is deleted: the old UUID is gone (PATCH answers 404) and
// the next POST issues a new UUID. Every field is read and written under the
// fixture's lock, through configure or inside the handler.
type rotatingServer struct {
	deleteStatus int  // Zero means 204.
	hang         bool // Hold the DELETE until the client gives up.
	conflicts    int  // POSTs to answer 409 before creating normally.
	tokenStatus  int  // Non-zero: the registration-token call answers this.
	deleted      map[uuid.UUID]bool
}

func rotateOnDelete(t *testing.T, f *sdkFixture) *rotatingServer {
	t.Helper()
	server := &rotatingServer{deleted: map[uuid.UUID]bool{}}
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			sessionPrefix := f.sessionPath() + "/"
			switch {
			case strings.HasSuffix(r.URL.Path, "/actions/runners/registration-token") && server.tokenStatus != 0:
				w.WriteHeader(server.tokenStatus)
				return true
			case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, sessionPrefix):
				id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, sessionPrefix))
				if err != nil {
					t.Errorf("session DELETE names no UUID: %s", r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return true
				}
				if server.hang {
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
						t.Error("the client never gave up on a hung session DELETE")
					}
					return true
				}
				status := server.deleteStatus
				if status == 0 {
					status = http.StatusNoContent
				}
				if status == http.StatusNoContent || status == http.StatusNotFound {
					server.deleted[id] = true
					if id == f.id {
						f.id = uuid.New()
					}
				}
				w.WriteHeader(status)
				return true
			case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, sessionPrefix):
				id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, sessionPrefix))
				if err == nil && server.deleted[id] {
					w.WriteHeader(http.StatusNotFound)
					return true
				}
			case r.Method == http.MethodPost && r.URL.Path == f.sessionPath() && server.conflicts > 0:
				server.conflicts--
				w.WriteHeader(http.StatusConflict)
				return true
			}
			return false
		}
	})
	return server
}

func activeSessionID(t *testing.T, s *Store, key SetKey) string {
	t.Helper()
	var id string
	if err := s.db.QueryRow(`SELECT session_id FROM sessions WHERE repo_id=? AND set_id=? AND phase='active'`,
		key.RepositoryID, key.ScaleSetID).Scan(&id); err != nil {
		t.Fatalf("no active session for %v: %v", key, err)
	}
	return id
}

func sessionPhases(t *testing.T, s *Store, key SetKey) []string {
	t.Helper()
	rows, err := s.db.Query(`SELECT phase FROM sessions WHERE repo_id=? AND set_id=? ORDER BY rowid`, key.RepositoryID, key.ScaleSetID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var phases []string
	for rows.Next() {
		var phase string
		if err := rows.Scan(&phase); err != nil {
			t.Fatal(err)
		}
		phases = append(phases, phase)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return phases
}

// restartManager closes the store and reopens it as a new process would, then
// builds a fresh manager over the same fixtures.
func restartManager(t *testing.T, s *Store, dir string, fixtures map[SetKey]*sdkFixture) (*Store, *SessionManager) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	for _, fixture := range fixtures {
		fixture.configure(func() { fixture.s = restarted })
	}
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	return restarted, fixtureManager(t, restarted, fixtures)
}

func deleteCount(f *sdkFixture, id string) int {
	return f.count(http.MethodDelete, f.sessionPath()+"/"+id)
}

// homelab#1138, the core claim: a deaf exit closes every set with no assigned
// work, so the next process opens a NEW session for it instead of resuming the
// UUID GitHub stopped delivering to, and that new session hears a JobAssigned.
// A set with assigned work is left alone and resumed.
func TestDeafRotationReplacesAnIdleSetsSessionAndKeepsABusyOne(t *testing.T) {
	s, dir := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	idle, busy := keys[0], keys[1]
	fixtures := map[SetKey]*sdkFixture{idle: newSDKFixture(t, s, idle), busy: newSDKFixture(t, s, busy)}
	for _, fixture := range fixtures {
		rotateOnDelete(t, fixture)
	}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	idleBefore, busyBefore := activeSessionID(t, s, idle), activeSessionID(t, s, busy)
	if err := s.recordStatistics(ctx, busy, scaleset.RunnerScaleSetStatistic{TotalAssignedJobs: 1}); err != nil {
		t.Fatal(err)
	}

	rotation := manager.RotateIdleSessions(ctx)
	if !reflect.DeepEqual(rotation.Rotated, []SetKey{idle}) || !reflect.DeepEqual(rotation.Kept, []SetKey{busy}) ||
		len(rotation.Failed) != 0 || len(rotation.Skipped) != 0 {
		t.Fatalf("rotation = %+v, want only %v rotated and %v kept", rotation, idle, busy)
	}
	if deleteCount(fixtures[busy], busyBefore) != 0 {
		t.Fatal("a set with assigned work had its session deleted")
	}
	// Bounded: a second call in the same exit sends nothing more.
	if again := manager.RotateIdleSessions(ctx); len(again.Rotated) != 0 || deleteCount(fixtures[idle], idleBefore) != 1 {
		t.Fatalf("rotation repeated: %+v, DELETEs=%d", again, deleteCount(fixtures[idle], idleBefore))
	}

	restarted, recovered := restartManager(t, s, dir, fixtures)
	if err := recovered.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := recovered.Degraded(); len(degraded) != 0 {
		t.Fatalf("a set was left degraded after rotation: %+v", degraded)
	}
	if idleAfter := activeSessionID(t, restarted, idle); idleAfter == idleBefore {
		t.Fatalf("the idle set resumed the same session %s instead of opening a new one", idleAfter)
	}
	if busyAfter := activeSessionID(t, restarted, busy); busyAfter != busyBefore {
		t.Fatalf("the busy set's session changed from %s to %s", busyBefore, busyAfter)
	}
	if got := fixtures[idle].count(http.MethodPost, fixtures[idle].sessionPath()); got != 2 {
		t.Fatalf("the idle set opened %d sessions, want 2 (original and replacement)", got)
	}
	if got := fixtures[busy].count(http.MethodPost, fixtures[busy].sessionPath()); got != 1 {
		t.Fatalf("the busy set opened %d sessions, want 1", got)
	}

	fixtures[idle].configure(func() {
		fixtures[idle].wire = wireEnvelope(t, []Event{event(idle, 71, scaleset.MessageTypeJobAssigned)})
	})
	results, _, err := recovered.PollSweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	heard := false
	for _, result := range results {
		if result.Set != idle || result.Envelope == nil {
			continue
		}
		for _, e := range result.Envelope.Events {
			heard = heard || e.MessageType == scaleset.MessageTypeJobAssigned && e.RunnerRequestID == 71
		}
	}
	if !heard {
		t.Fatalf("the replacement session did not deliver the JobAssigned: %+v", results)
	}
}

// An unreleased lease is the other half of "assigned work": the set is kept
// even when its statistics read idle. The first set is the control -- the same
// offer and acknowledgment, no lease -- and it rotates.
func TestDeafRotationNeverClosesASetWithAnUnreleasedLease(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	control, leased := keys[0], keys[1]
	fixtures := map[SetKey]*sdkFixture{control: newSDKFixture(t, s, control), leased: newSDKFixture(t, s, leased)}
	for _, fixture := range fixtures {
		rotateOnDelete(t, fixture)
	}
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	leasedID := activeSessionID(t, s, leased)
	offer(t, s, control, 1)
	offer(t, s, leased, 1)
	// Acknowledged, so only the lease can refuse the close.
	if _, err := s.db.Exec(`UPDATE messages SET acked=1`); err != nil {
		t.Fatal(err)
	}
	lease, err := s.ReserveNextExcept(ctx, []SetKey{control})
	if err != nil || lease.Set != leased {
		t.Fatalf("reserve on the leased set: %+v %v", lease, err)
	}

	rotation := manager.RotateIdleSessions(ctx)
	if !reflect.DeepEqual(rotation.Rotated, []SetKey{control}) || !reflect.DeepEqual(rotation.Kept, []SetKey{leased}) {
		t.Fatalf("rotation = %+v, want %v rotated and %v kept", rotation, control, leased)
	}
	if deleteCount(fixtures[leased], leasedID) != 0 {
		t.Fatal("a set with an unreleased lease had its session deleted")
	}
	if phases := sessionPhases(t, s, leased); !reflect.DeepEqual(phases, []string{"active"}) {
		t.Fatalf("the leased set's session left active: %v", phases)
	}
}

// A 404 on the DELETE means the server session is already gone: the set is
// recorded closed and counts as rotated, so the next process opens a new one.
func TestDeafRotationTreatsA404AsAlreadyClosed(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() { server.deleteStatus = http.StatusNotFound })
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	before := activeSessionID(t, s, key)

	rotation := manager.RotateIdleSessions(ctx)
	if !reflect.DeepEqual(rotation.Rotated, []SetKey{key}) || len(rotation.Failed) != 0 {
		t.Fatalf("rotation = %+v, want %v rotated", rotation, key)
	}
	if phases := sessionPhases(t, s, key); !reflect.DeepEqual(phases, []string{"closed"}) {
		t.Fatalf("a 404 did not record the session closed: %v", phases)
	}
	restarted, recovered := restartManager(t, s, dir, map[SetKey]*sdkFixture{key: fixture})
	if err := recovered.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if after := activeSessionID(t, restarted, key); after == before {
		t.Fatal("the set resumed the session GitHub no longer has")
	}
}

// Any other DELETE outcome is uncertain. The close-intent stays, the set is
// named, and it is never replaced: the next process skips it until the
// close-intent is resolved. Rotation stops at the first such failure, so the
// sets after it keep their sessions and resume as before.
func TestDeafRotationRetainsCloseIntentOnAFailedDelete(t *testing.T) {
	for _, scenario := range []string{"503", "401", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			s, dir := openTestStore(t, 2)
			keys := prepare(t, s, 2)
			markManaged(t, s, keys)
			failing, later := keys[0], keys[1]
			fixtures := map[SetKey]*sdkFixture{failing: newSDKFixture(t, s, failing), later: newSDKFixture(t, s, later)}
			server := rotateOnDelete(t, fixtures[failing])
			rotateOnDelete(t, fixtures[later])
			fixtures[failing].configure(func() {
				switch scenario {
				case "503":
					server.deleteStatus = http.StatusServiceUnavailable
				case "401":
					server.deleteStatus = http.StatusUnauthorized
				case "timeout":
					server.hang = true
				}
			})
			manager := fixtureManager(t, s, fixtures)
			manager.closeBound = 200 * time.Millisecond
			if err := manager.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			failingID, laterID := activeSessionID(t, s, failing), activeSessionID(t, s, later)

			rotation := manager.RotateIdleSessions(ctx)
			if len(rotation.Rotated) != 0 || len(rotation.Failed) != 1 || rotation.Failed[0].Set != failing ||
				rotation.Failed[0].Err == nil || !reflect.DeepEqual(rotation.Skipped, []SetKey{later}) {
				t.Fatalf("rotation = %+v, want %v failed and %v skipped", rotation, failing, later)
			}
			if phases := sessionPhases(t, s, failing); !reflect.DeepEqual(phases, []string{"close-intent"}) {
				t.Fatalf("an uncertain DELETE did not retain close-intent: %v", phases)
			}
			if deleteCount(fixtures[later], laterID) != 0 {
				t.Fatal("rotation continued past an uncertain DELETE")
			}

			_, recovered := restartManager(t, s, dir, fixtures)
			if err := recovered.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if degraded := recovered.Degraded(); len(degraded) != 1 || degraded[0].Set != failing {
				t.Fatalf("degraded = %+v, want only %v", degraded, failing)
			}
			if got := fixtures[failing].count(http.MethodPost, fixtures[failing].sessionPath()); got != 1 {
				t.Fatalf("a set with an uncertain close was replaced: %d POSTs", got)
			}
			if got := deleteCount(fixtures[failing], failingID); got != 1 {
				t.Fatalf("the uncertain DELETE was replayed: %d DELETEs", got)
			}
			if got := fixtures[later].count(http.MethodPatch, fixtures[later].sessionPath()+"/"+laterID); got != 1 {
				t.Fatalf("the skipped set did not resume its session: %d PATCHes", got)
			}
		})
	}
}

// A 409 on the replacement create (GitHub still holds a session for the set)
// is a definite refusal: nothing was created. The set is degraded for that
// cycle only, the other set is unaffected, and the next Sync creates it.
func TestAConflictOnTheReplacementCreateIsRetriedNextCycle(t *testing.T) {
	s, dir := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	conflicted, other := keys[0], keys[1]
	fixtures := map[SetKey]*sdkFixture{conflicted: newSDKFixture(t, s, conflicted), other: newSDKFixture(t, s, other)}
	server := rotateOnDelete(t, fixtures[conflicted])
	rotateOnDelete(t, fixtures[other])
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	before := activeSessionID(t, s, conflicted)
	if rotation := manager.RotateIdleSessions(ctx); len(rotation.Rotated) != 2 {
		t.Fatalf("rotation = %+v, want both sets rotated", rotation)
	}
	fixtures[conflicted].configure(func() { server.conflicts = 1 })

	restarted, recovered := restartManager(t, s, dir, fixtures)
	if err := recovered.Sync(ctx); err != nil {
		t.Fatalf("a 409 failed the whole sync: %v", err)
	}
	if degraded := recovered.Degraded(); len(degraded) != 1 || degraded[0].Set != conflicted {
		t.Fatalf("degraded = %+v, want only %v", degraded, conflicted)
	}
	if phases := sessionPhases(t, restarted, conflicted); !reflect.DeepEqual(phases, []string{"closed"}) {
		t.Fatalf("a refused create left a journal row behind: %v", phases)
	}
	if polls, _, err := recovered.PollSweep(ctx); err != nil || len(polls) != 1 || polls[0].Set != other {
		t.Fatalf("the other set was not polled: %+v %v", polls, err)
	}

	if err := recovered.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if degraded := recovered.Degraded(); len(degraded) != 0 {
		t.Fatalf("the conflicted set was not retried: %+v", degraded)
	}
	if after := activeSessionID(t, restarted, conflicted); after == before {
		t.Fatal("the retried set resumed the old session")
	}
	if got := fixtures[conflicted].count(http.MethodPost, fixtures[conflicted].sessionPath()); got != 3 {
		t.Fatalf("POSTs = %d, want 3 (original, 409, retry)", got)
	}
}

// The daemon wiring: a deaf cycle rotates before it returns and carries what
// the rotation did to the caller's exit line; a cycle that is not deaf never
// rotates.
func TestADeafCycleRotatesIdleSessionsBeforeItReturns(t *testing.T) {
	want := SessionRotation{Rotated: []SetKey{{RepositoryID: 1, ScaleSetID: 101}}, Kept: []SetKey{{RepositoryID: 2, ScaleSetID: 102}}}
	for _, deaf := range []bool{true, false} {
		t.Run(fmt.Sprintf("deaf=%t", deaf), func(t *testing.T) {
			store, _ := openTestStore(t, 2)
			var calls []string
			sessions := cycleSessions{calls: &calls, rotation: want}
			daemon, err := newDaemon(cycleRegistry{calls: &calls}, sessions, &cycleLifecycle{calls: &calls}, 0)
			if err != nil {
				t.Fatal(err)
			}
			daemon.store = store
			daemon.deafAfter = 30 * time.Minute
			store.mu.Lock()
			store.started = time.Now().Add(-2 * time.Hour)
			store.lastEnvelope = time.Time{}
			if !deaf {
				store.lastEnvelope = time.Now()
			}
			store.mu.Unlock()
			result, err := daemon.Cycle(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if result.SessionsDeaf != deaf {
				t.Fatalf("the fixture is not the shape under test: deaf=%t %+v", result.SessionsDeaf, result.Summary)
			}
			rotated := slices.Contains(calls, "rotate")
			if rotated != deaf {
				t.Fatalf("rotate called=%t on a deaf=%t cycle: %v", rotated, deaf, calls)
			}
			if deaf && !reflect.DeepEqual(result.Rotation, want) {
				t.Fatalf("the cycle dropped the rotation: %+v", result.Rotation)
			}
			if deaf && calls[len(calls)-1] != "rotate" {
				t.Fatalf("rotation is not the last step of a deaf cycle: %v", calls)
			}
		})
	}
}

// Rotation exists to recover a backlog of queued jobs, so a job the set had
// journaled before its session was closed must be servable again when GitHub
// offers it on the replacement session. On github.com that offer is a lone
// JobAssigned with request ID 0 (docs/github-scale-set-api-behavior.md); under
// the acquire model it is a JobAvailable with a positive request ID. In both
// shapes the re-offer repeats the event the journal already holds, so it does
// not outrank the job's previous state. Each case delivers the job on the old
// session, restarts once (which is what demotes an inherited broker assignment
// to 'assigned-stale'), rotates, restarts, re-offers the same event on the new
// session, and then acquires and leases it through that session.
func TestARotatedSetServesItsQueuedJobOnTheNewSession(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event func(SetKey) Event
	}{
		{"broker JobAssigned with request ID 0", func(key SetKey) Event {
			return assigned(key, "workflow-job-broker", scaleset.MessageTypeJobAssigned)
		}},
		{"JobAvailable with a request ID", func(key SetKey) Event {
			return event(key, 71, scaleset.MessageTypeJobAvailable)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			markManaged(t, s, []SetKey{key})
			fixture := newSDKFixture(t, s, key)
			rotateOnDelete(t, fixture)
			fixtures := map[SetKey]*sdkFixture{key: fixture}
			manager := fixtureManager(t, s, fixtures)
			if err := manager.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			offered := tc.event(key)
			deliver := func(m *SessionManager) {
				t.Helper()
				fixture.configure(func() { fixture.wire = wireEnvelope(t, []Event{offered}) })
				if _, _, err := m.PollSweep(ctx); err != nil {
					t.Fatal(err)
				}
				fixture.configure(func() { fixture.wire = nil })
			}
			deliver(manager)

			s, manager = restartManager(t, s, dir, fixtures)
			if err := manager.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			before := activeSessionID(t, s, key)
			if rotation := manager.RotateIdleSessions(ctx); !reflect.DeepEqual(rotation.Rotated, []SetKey{key}) {
				t.Fatalf("rotation = %+v, want %v rotated", rotation, key)
			}

			s, manager = restartManager(t, s, dir, fixtures)
			if err := manager.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if after := activeSessionID(t, s, key); after == before {
				t.Fatal("the set resumed its old session instead of opening a new one")
			}
			deliver(manager)
			lease, err := manager.AcquireNext(ctx)
			if err != nil {
				t.Fatalf("the re-offered job was not acquired through the new session: lease=%+v err=%v", lease, err)
			}
			if want := mustJobKey(t, offered); lease.Set != key || lease.JobKey != want {
				t.Fatalf("leased %+v, want job %s on %v", lease, want, key)
			}
			stored, err := s.Lease(ctx, lease.ID)
			if err != nil || stored.Phase != Acquired {
				t.Fatalf("the lease was not acquired: %+v %v", stored, err)
			}
		})
	}
}

// Security review I1 on #1166: the admin token refresh runs before the DELETE,
// through the parent client, and a 404 there (the App lost the repository)
// also matches scaleset.NotFoundError. The DELETE was never sent, so the
// session must keep its close-intent and count as failed, not rotated. The
// fixture's admin tokens are shorter-lived than the SDK's 60 s refresh margin,
// so every administrative request refreshes first.
func TestDeafRotationDoesNotCountATokenRefresh404AsClosed(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	markManaged(t, s, []SetKey{key})
	fixture := newSDKFixture(t, s, key)
	server := rotateOnDelete(t, fixture)
	fixture.configure(func() { fixture.adminTTL = 30 * time.Second })
	manager := fixtureManager(t, s, map[SetKey]*sdkFixture{key: fixture})
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	id := activeSessionID(t, s, key)
	fixture.configure(func() { server.tokenStatus = http.StatusNotFound })

	rotation := manager.RotateIdleSessions(ctx)
	if len(rotation.Rotated) != 0 || len(rotation.Failed) != 1 || rotation.Failed[0].Set != key {
		t.Fatalf("rotation = %+v, want %v failed, nothing rotated", rotation, key)
	}
	if got := deleteCount(fixture, id); got != 0 {
		t.Fatalf("the fixture is not the shape under test: %d session DELETEs were sent", got)
	}
	if phases := sessionPhases(t, s, key); !reflect.DeepEqual(phases, []string{"close-intent"}) {
		t.Fatalf("a token-refresh 404 was recorded as a close: %v", phases)
	}
}

// Security review I2 on #1166: a close-intent is resolved only by the hourly
// retry (homelab#1167), so while any set holds one, a later deaf exit rotates
// nothing. Otherwise each deaf exit could strand one more set.
func TestAHeldCloseIntentBlocksTheNextRotation(t *testing.T) {
	s, dir := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	markManaged(t, s, keys)
	stuck, other := keys[0], keys[1]
	fixtures := map[SetKey]*sdkFixture{stuck: newSDKFixture(t, s, stuck), other: newSDKFixture(t, s, other)}
	server := rotateOnDelete(t, fixtures[stuck])
	rotateOnDelete(t, fixtures[other])
	fixtures[stuck].configure(func() { server.deleteStatus = http.StatusServiceUnavailable })
	manager := fixtureManager(t, s, fixtures)
	if err := manager.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	otherID := activeSessionID(t, s, other)
	if first := manager.RotateIdleSessions(ctx); len(first.Failed) != 1 || first.Failed[0].Set != stuck {
		t.Fatalf("first rotation = %+v, want %v failed", first, stuck)
	}

	restarted, recovered := restartManager(t, s, dir, fixtures)
	if err := recovered.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	second := recovered.RotateIdleSessions(ctx)
	if len(second.Rotated) != 0 || len(second.Failed) != 0 || second.Blocked != "close-intent" ||
		!reflect.DeepEqual(second.CloseIntent, []SetKey{stuck}) {
		t.Fatalf("second rotation = %+v, want nothing rotated and blocked by %v", second, stuck)
	}
	if got := deleteCount(fixtures[other], otherID); got != 0 {
		t.Fatalf("a rotation ran while a close-intent was held: %d DELETEs on the other set", got)
	}
	if after := activeSessionID(t, restarted, other); after != otherID {
		t.Fatalf("the other set's session changed from %s to %s", otherID, after)
	}
}
