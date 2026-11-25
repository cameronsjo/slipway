package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
)

func TestSDKSessionIntentSurvivesUncertainCreation(t *testing.T) {
	for _, scenario := range []string{"503", "redirect", "truncated", "wrong owner", "foreign queue origin", "commit failure"} {
		t.Run(scenario, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			f.configure(func() {
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.URL.Path != f.sessionPath() || r.Method != "POST" {
						return false
					}
					switch scenario {
					case "503":
						w.WriteHeader(503)
					case "redirect":
						w.Header().Set("Location", "/recreated")
						w.WriteHeader(307)
					case "truncated":
						w.Header().Set("Content-Length", "1000")
						fmt.Fprint(w, "{")
					case "wrong owner":
						session := f.session()
						session.OwnerName = "someone-else"
						json.NewEncoder(w).Encode(session)
					case "foreign queue origin":
						session := f.session()
						session.MessageQueueURL = "https://broker.example.invalid/queue"
						json.NewEncoder(w).Encode(session)
					case "commit failure":
						if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
							t.Errorf("write fault: %v", err)
						}
						json.NewEncoder(w).Encode(f.session())
					}
					return true
				}
			})
			if _, err := s.openQueueSession(ctx, key, f.client, f.server.URL); err == nil {
				t.Fatal("uncertain session accepted")
			}
			pending, err := s.PendingSessions(ctx)
			if err != nil || len(pending) != 1 || pending[0].Phase != "create-intent" || pending[0].Owner == "" {
				t.Fatalf("intent lost: %+v %v", pending, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := Open(dir, s.config)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if _, err := restarted.openQueueSession(ctx, key, f.client, f.server.URL); err == nil {
				t.Fatal("restart repeated uncertain create")
			}
			if f.count("POST", f.sessionPath()) != 1 || f.count("POST", "/recreated") != 0 {
				t.Fatal("session POST replayed or redirected")
			}
		})
	}
}

func TestSDKSessionFencesBeforeCreation(t *testing.T) {
	for _, scenario := range []string{"write failure", "revoked", "unsettled lease", "active session"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			wantCreates := 0
			switch scenario {
			case "write failure":
				if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := s.Discover(ctx, nil); err != nil {
					t.Fatal(err)
				}
			case "unsettled lease":
				offer(t, s, key, 1)
				mustReserve(t, s)
			case "active session":
				f.open()
				wantCreates = 1
			}
			if _, err := s.openQueueSession(ctx, key, f.client, f.server.URL); err == nil {
				t.Fatal("session fence bypassed")
			}
			if f.count("POST", f.sessionPath()) != wantCreates {
				t.Fatal("fenced session reached API")
			}
		})
	}
}

func TestSDKInitialAndRefreshedStatisticsSurviveIdleRestart(t *testing.T) {
	s, dir := openTestStore(t, 1)
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
		t.Fatalf("initial statistics lost: %+v %v", stats, err)
	}
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" && r.URL.Path == "/queue" && f.counts["GET /queue"] == 2 {
				w.WriteHeader(401)
				return true
			}
			if r.Method == "PATCH" {
				session := f.session()
				session.Statistics.TotalAssignedJobs = 8
				json.NewEncoder(w).Encode(session)
				return true
			}
			return false
		}
	})
	if e, err := q.Poll(ctx, 0); err != nil || e != nil {
		t.Fatalf("refreshed idle poll: %+v %v", e, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if stats, err := restarted.Statistics(ctx, key); err != nil || stats == nil || stats.TotalAssignedJobs != 8 {
		t.Fatalf("refreshed statistics lost: %+v %v", stats, err)
	}
	if _, err := restarted.openQueueSession(ctx, key, f.client, f.server.URL); err == nil || f.count("POST", f.sessionPath()) != 1 {
		t.Fatal("active session silently replaced on restart")
	}
	if leases, err := restarted.ActiveLeases(ctx); err != nil || len(leases) != 0 {
		t.Fatalf("aggregate synthesized reservations: %+v %v", leases, err)
	}
}

func TestSDKUncertainCloseRetainsFence(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	f.configure(func() { f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}) })
	if _, err := q.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "DELETE" && r.URL.Path == f.sessionPath()+"/"+f.id.String() {
				pending, err := s.PendingSessions(ctx)
				if err != nil || len(pending) != 1 || pending[0].Phase != "close-intent" {
					t.Errorf("close before intent: %+v %v", pending, err)
				}
				w.WriteHeader(503)
				return true
			}
			return false
		}
	})
	if err := q.Close(ctx); err == nil {
		t.Fatal("uncertain close accepted")
	}
	if err := q.Close(ctx); err == nil {
		t.Fatal("uncertain close retried")
	}
	if _, err := q.Poll(ctx, 1); err == nil || f.count("GET", "/queue") != 1 {
		t.Fatal("closing session polled")
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("closing session still offers work: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.openQueueSession(ctx, key, f.client, f.server.URL); err == nil {
		t.Fatal("unknown close permitted replacement")
	}
	if f.count("DELETE", f.sessionPath()+"/"+f.id.String()) != 1 {
		t.Fatal("close DELETE replayed")
	}
}

func TestSDKCloseRetainsUnacknowledgedAndAssignedWork(t *testing.T) {
	for _, scenario := range []string{"unacknowledged", "acquired", "assigned", "running"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			stats := scaleset.RunnerScaleSetStatistic{}
			switch scenario {
			case "unacknowledged":
				offer(t, s, key, 1)
			case "acquired":
				stats.TotalAcquiredJobs = 1
			case "assigned":
				stats.TotalAssignedJobs = 1
			case "running":
				stats.TotalRunningJobs = 1
			}
			if err := s.recordStatistics(ctx, key, stats); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(ctx); err == nil {
				t.Fatal("unsettled broker work discarded")
			}
			if f.count("DELETE", f.sessionPath()+"/"+f.id.String()) != 0 {
				t.Fatal("close guard ran after DELETE")
			}
		})
	}
}

func TestSDKMissingWireHookCannotAcknowledgeTypedResult(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	f.configure(func() { f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}) })
	// Deliberate negative control: the actual SDK can decode this result, but a
	// typed success without the wire hook must not confer ACK eligibility.
	q.http.ResponseLogHook = nil
	if _, err := q.Poll(ctx, 1); err == nil {
		t.Fatal("typed-only SDK result accepted")
	}
	if f.count("GET", "/queue") != 1 || f.count("DELETE", "/queue/7") != 0 {
		t.Fatal("uncommitted result acknowledged")
	}
}

func TestPublicSessionConstructorRejectsWrongInstallation(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	if _, err := s.OpenQueueSession(ctx, key, &githubapp.App{}, githubapp.Installation{ID: 43}); err == nil {
		t.Fatal("wrong installation accepted")
	}
	if records, err := s.PendingSessions(ctx); err != nil || len(records) != 0 {
		t.Fatalf("wrong installation wrote intent: %+v %v", records, err)
	}
}

func restartActiveSession(t *testing.T, s *Store, dir string, f *sdkFixture, q *QueueSession) *Store {
	t.Helper()
	q.http.HTTPClient.CloseIdleConnections()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	f.configure(func() { f.s = restarted })
	return restarted
}

func TestSDKResumeRequiresInventoryAndDurableRoutingIdentity(t *testing.T) {
	for _, scenario := range []string{"inventory unknown", "missing fingerprint"} {
		t.Run(scenario, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			restarted := restartActiveSession(t, s, dir, f, q)
			if scenario == "missing fingerprint" {
				if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
					t.Fatal(err)
				}
				if _, err := restarted.db.Exec(`UPDATE sessions SET queue_sha256=X''`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := restarted.resumeQueueSession(ctx, key, f.client, f.server.URL); err == nil {
				t.Fatal("recovery fence was bypassed")
			}
			if f.count("PATCH", f.sessionPath()+"/"+f.id.String()) != 0 || f.count("POST", f.sessionPath()) != 1 {
				t.Fatal("fenced recovery reached or replaced the remote session")
			}
		})
	}
}

func TestSDKResumePreservesCursorAndReplaysPendingAcknowledgment(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	f.configure(func() {
		f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)})
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodDelete && r.URL.Path == "/queue/7" && f.counts["DELETE /queue/7"] == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return true
			}
			return false
		}
	})
	if _, err := q.Poll(ctx, 1); err == nil {
		t.Fatal("lost acknowledgment accepted")
	}
	restarted := restartActiveSession(t, s, dir, f, q)
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	f.configure(func() { f.wire = nil })
	resumed, err := restarted.resumeQueueSession(ctx, key, f.client, f.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if cursor, err := restarted.Cursor(ctx, key, f.id); err != nil || cursor != 7 {
		t.Fatalf("resumed cursor: %d %v", cursor, err)
	}
	if f.count("DELETE", "/queue/7") != 2 || f.count("PATCH", f.sessionPath()+"/"+f.id.String()) != 1 || f.count("POST", f.sessionPath()) != 1 {
		t.Fatal("resume did not reconcile the exact session inbox")
	}
	if envelope, err := resumed.Poll(ctx, 0); err != nil || envelope != nil {
		t.Fatalf("resumed poll failed: %+v %v", envelope, err)
	}
	f.configure(func() {
		if len(f.queries) == 0 || f.queries[len(f.queries)-1] != "7" {
			t.Errorf("resumed poll lost cursor: %v", f.queries)
		}
	})
}

func TestSDKUncertainResumeRetainsExactJournalIdentity(t *testing.T) {
	for _, scenario := range []string{"503", "redirect", "truncated", "lost response", "foreign queue"} {
		t.Run(scenario, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			before, err := s.PendingSessions(ctx)
			if err != nil || len(before) != 1 {
				t.Fatalf("active identity: %+v %v", before, err)
			}
			restarted := restartActiveSession(t, s, dir, f, q)
			if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
				t.Fatal(err)
			}
			f.configure(func() {
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method != http.MethodPatch {
						if r.URL.Path == "/redirected" {
							t.Error("resume followed redirect")
						}
						return false
					}
					switch scenario {
					case "503":
						w.WriteHeader(http.StatusServiceUnavailable)
					case "redirect":
						w.Header().Set("Location", "/redirected")
						w.WriteHeader(http.StatusTemporaryRedirect)
					case "truncated":
						w.Header().Set("Content-Length", "1000")
						fmt.Fprint(w, "{")
					case "lost response":
						panic(http.ErrAbortHandler)
					case "foreign queue":
						session := f.session()
						session.MessageQueueURL += "/foreign"
						json.NewEncoder(w).Encode(session)
					}
					return true
				}
			})
			if resumed, err := restarted.resumeQueueSession(ctx, key, f.client, f.server.URL); err == nil || resumed != nil {
				t.Fatal("uncertain recovery published a handle")
			}
			after, err := restarted.PendingSessions(ctx)
			if err != nil || len(after) != 1 || after[0] != before[0] {
				t.Fatalf("recovery changed journal identity: before=%+v after=%+v err=%v", before, after, err)
			}
			if f.count("PATCH", f.sessionPath()+"/"+f.id.String()) != 1 || f.count("POST", f.sessionPath()) != 1 || f.count("GET", "/queue") != 0 {
				t.Fatal("uncertain recovery retried, replaced, or polled")
			}
		})
	}
}
