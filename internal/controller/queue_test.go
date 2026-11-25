package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"
)

// All requests go through the pinned SDK, its real auth flow and TLS transport.
// The fixture's only credentials are generated test keys and sentinel tokens.
type sdkFixture struct {
	t        *testing.T
	s        *Store
	key      SetKey
	server   *httptest.Server
	client   *scaleset.Client
	id       uuid.UUID
	owner    string
	mu       sync.Mutex
	counts   map[string]int
	queries  []string
	caps     []string
	wire     []byte
	override func(http.ResponseWriter, *http.Request) bool
	// adminTTL, when set, makes each runner-registration answer carry a fresh
	// admin token that expires this soon. Under the SDK's 60 s refresh margin
	// that forces a token refresh before every administrative request.
	adminTTL time.Duration
	private  *rsa.PrivateKey
}

func newSDKFixture(t *testing.T, s *Store, key SetKey) *sdkFixture {
	t.Helper()
	f := &sdkFixture{t: t, s: s, key: key, id: uuid.New(), counts: map[string]int{}}
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.private = private
	admin, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"exp": time.Now().Add(time.Hour).Unix()}).SignedString(private)
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.counts[r.Method+" "+r.URL.Path]++
		if r.Method == "GET" && r.URL.Path == "/queue" {
			f.queries = append(f.queries, r.URL.Query().Get("lastMessageId"))
			f.caps = append(f.caps, r.Header.Get(scaleset.HeaderScaleSetMaxCapacity))
		}
		if r.Method == "POST" && r.URL.Path == f.sessionPath() {
			var requested scaleset.RunnerScaleSetSession
			if err := json.NewDecoder(r.Body).Decode(&requested); err != nil {
				t.Errorf("session request: %v", err)
			}
			f.owner = requested.OwnerName
			pending, err := f.s.PendingSessions(ctx)
			found := false
			for _, record := range pending {
				found = found || record.Owner == f.owner && record.Set == key && record.Phase == "create-intent"
			}
			if err != nil || !found {
				t.Errorf("POST preceded creation intent: %+v %v", pending, err)
			}
		}
		if f.override != nil && f.override(w, r) {
			return
		}
		switch {
		case r.URL.Path == "/api/v3/app/installations/42/access_tokens":
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{"token": "installation-sentinel", "expires_at": time.Now().Add(time.Hour).Format(time.RFC3339)})
		case strings.HasSuffix(r.URL.Path, "/actions/runners/registration-token"):
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"token":"registration-sentinel"}`)
		case r.URL.Path == "/api/v3/actions/runner-registration":
			token := admin
			if f.adminTTL > 0 {
				short, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"exp": time.Now().Add(f.adminTTL).Unix()}).SignedString(f.private)
				if err != nil {
					t.Errorf("short-lived admin token: %v", err)
				}
				token = short
			}
			json.NewEncoder(w).Encode(map[string]string{"url": f.server.URL + "/tenant", "token": token})
		case r.URL.Path == f.sessionPath() && r.Method == "POST":
			json.NewEncoder(w).Encode(f.session())
		case r.URL.Path == f.sessionPath()+"/"+f.id.String() && r.Method == "PATCH":
			json.NewEncoder(w).Encode(f.session())
		case r.URL.Path == f.sessionPath()+"/"+f.id.String() && r.Method == "DELETE":
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/queue" && r.Method == "GET":
			if f.wire == nil {
				w.WriteHeader(http.StatusAccepted)
			} else {
				w.Write(f.wire)
			}
		case strings.HasPrefix(r.URL.Path, "/queue/") && r.Method == "DELETE":
			pending, err := f.s.PendingMessages(ctx)
			committed := err == nil && len(pending) == 1 && pending[0].Session == f.id && pending[0].Set == key
			// The one other ACK allowed: a quarantine, which has no committed
			// envelope but must have a durable refusal count at the threshold.
			if !committed && !f.quarantineDue(strings.TrimPrefix(r.URL.Path, "/queue/")) {
				t.Errorf("ACK lacks committed scope/identity or a due quarantine: %+v %v", pending, err)
			}
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/acquirejobs") && r.Method == "POST":
			leases, err := f.s.ActiveLeases(ctx)
			if err != nil || len(leases) != 1 || leases[0].Phase != AcquisitionIntent {
				t.Errorf("acquisition lacks durable intent: %+v %v", leases, err)
			}
			var ids []int64
			if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
				t.Errorf("acquire body: %v", err)
			}
			json.NewEncoder(w).Encode(map[string]any{"count": len(ids), "value": ids})
		default:
			t.Errorf("unexpected fixture request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.server.Close)
	pool := x509.NewCertPool()
	pool.AddCert(f.server.Certificate())
	httpClient := retryablehttp.NewClient()
	httpClient.RetryMax = 0
	httpClient.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("test redirect refused") }
	f.client, err = scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
		GitHubConfigURL: fmt.Sprintf("%s/owner/repo%d", f.server.URL, key.RepositoryID),
		GitHubAppAuth: scaleset.GitHubAppAuth{ClientID: "test-app", InstallationID: 42,
			PrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(private)}))},
	}, scaleset.WithRetryableHTTPClint(httpClient), scaleset.WithRootCAs(pool))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *sdkFixture) sessionPath() string {
	return fmt.Sprintf("/tenant/_apis/runtime/runnerscalesets/%d/sessions", f.key.ScaleSetID)
}

func (f *sdkFixture) session() scaleset.RunnerScaleSetSession {
	return scaleset.RunnerScaleSetSession{SessionID: f.id, OwnerName: f.owner,
		RunnerScaleSet: &scaleset.RunnerScaleSet{ID: f.key.ScaleSetID}, MessageQueueURL: f.server.URL + "/queue",
		MessageQueueAccessToken: "queue-credential-sentinel", Statistics: &scaleset.RunnerScaleSetStatistic{}}
}

// quarantineDue reports whether a DELETE is the one quarantine this session
// owes: exactly one refused message at the threshold and not yet recorded as
// acknowledged. The DELETE path must name that message; a DELETE for any other
// ID fails the test outright rather than reading as "not due".
func (f *sdkFixture) quarantineDue(pathID string) bool {
	rows, err := f.s.db.Query(`SELECT message_id FROM refused_envelopes WHERE repo_id=? AND set_id=? AND session=? AND acked='' AND refusals>=?`,
		f.key.RepositoryID, f.key.ScaleSetID, f.id.String(), refusalQuarantineThreshold)
	if err != nil {
		f.t.Errorf("read the due quarantine: %v", err)
		return false
	}
	defer rows.Close()
	var due []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			f.t.Errorf("read the due quarantine: %v", err)
			return false
		}
		due = append(due, id)
	}
	if err := rows.Err(); err != nil || len(due) != 1 {
		return false
	}
	if pathID != strconv.Itoa(due[0]) {
		f.t.Errorf("quarantine DELETE names message %q, but the refused message is %d", pathID, due[0])
		return false
	}
	return true
}

// commitWithLostACK commits message 7 in this fixture's session and loses its
// ACK, leaving a durable messages row with acked=0. That is the only way a
// messages row and a later refusal of the same ID can coexist; the refusal
// itself can be of any kind.
func commitWithLostACK(t *testing.T, f *sdkFixture, q *QueueSession, key SetKey) {
	t.Helper()
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
		t.Fatal("the lost ACK was accepted")
	}
	f.configure(func() { f.override = nil })
	if acked := messageAcked(t, f, key); acked != 0 {
		t.Fatalf("the first delivery is not left unacknowledged: acked=%d", acked)
	}
}

// messageAcked reads the acked flag of this session's messages row for 7.
func messageAcked(t *testing.T, f *sdkFixture, key SetKey) int {
	t.Helper()
	var acked int
	if err := f.s.db.QueryRow(`SELECT acked FROM messages WHERE repo_id=? AND set_id=? AND session=? AND message_id=7`,
		key.RepositoryID, key.ScaleSetID, f.id.String()).Scan(&acked); err != nil {
		t.Fatal(err)
	}
	return acked
}

func (f *sdkFixture) configure(fn func()) { f.mu.Lock(); defer f.mu.Unlock(); fn() }
func (f *sdkFixture) count(method, path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[method+" "+path]
}

func (f *sdkFixture) open() *QueueSession {
	f.t.Helper()
	q, err := f.s.openQueueSession(ctx, f.key, f.client, f.server.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	return q
}

func wireEnvelope(t *testing.T, events []Event) []byte {
	t.Helper()
	body, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"messageId": 7, "messageType": "RunnerScaleSetJobMessages", "body": string(body),
		"statistics": scaleset.RunnerScaleSetStatistic{TotalAvailableJobs: 100}})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestSDKOrderedCommitAndACK(t *testing.T) {
	s, dir := openTestStore(t, 2)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	events := []Event{event(key, 1, scaleset.MessageTypeJobAvailable), event(key, 1, scaleset.MessageTypeJobCompleted), event(key, 1, scaleset.MessageTypeJobAvailable)}
	f.configure(func() { f.wire = wireEnvelope(t, events) })
	e, err := q.Poll(ctx, 2)
	if err != nil || e == nil || len(e.Events) != 3 || e.Events[1].MessageType != scaleset.MessageTypeJobCompleted || e.Statistics.TotalAvailableJobs != 100 {
		t.Fatalf("wire order/statistics lost: %+v %v", e, err)
	}
	lease := mustReserve(t, s)
	if err := q.Acquire(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	current, err := s.Lease(ctx, lease.ID)
	if err != nil || current.Phase != Acquired {
		t.Fatalf("acquisition not recorded: %+v %v", current, err)
	}
	if err := q.Close(ctx); err == nil || f.count("DELETE", f.sessionPath()+"/"+f.id.String()) != 0 {
		t.Fatal("closed a session with active work")
	}
	f.configure(func() { f.wire = nil })
	if e, err := q.Poll(ctx, 0); err != nil || e != nil {
		t.Fatalf("202 idle poll: %+v %v", e, err)
	}
	f.configure(func() {
		if fmt.Sprint(f.queries) != "[ 7]" || fmt.Sprint(f.caps) != "[2 0]" {
			t.Errorf("cursor/capacity header: %v %v", f.queries, f.caps)
		}
	})
	finish(t, s, lease)
	if err := q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if records, err := s.PendingSessions(ctx); err != nil || len(records) != 0 {
		t.Fatalf("settled session remains pending: %+v %v", records, err)
	}
	for _, name := range []string{"pool.sqlite", "pool.sqlite-wal"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"queue-credential-sentinel", "registration-sentinel", "installation-sentinel", f.server.URL} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("transient secret/routing value entered %s", name)
			}
		}
	}
}

// TestPollTracksLastOutcome pins the wire-level classification LastPollOutcome
// exposes to the daemon's per-cycle polls= tally: a 202 with no wire fetch
// reads PollEmpty, and a delivered message reads PollMessage.
func TestPollTracksLastOutcome(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()

	f.configure(func() { f.wire = nil })
	if e, err := q.Poll(ctx, 0); err != nil || e != nil {
		t.Fatalf("202 idle poll: %+v %v", e, err)
	}
	if got := q.LastPollOutcome(); got != PollEmpty {
		t.Fatalf("LastPollOutcome() after a 202 = %q, want %q", got, PollEmpty)
	}

	events := []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
	f.configure(func() { f.wire = wireEnvelope(t, events) })
	e, err := q.Poll(ctx, 1)
	if err != nil || e == nil {
		t.Fatalf("delivered poll: %+v %v", e, err)
	}
	if got := q.LastPollOutcome(); got != PollMessage {
		t.Fatalf("LastPollOutcome() after a delivered message = %q, want %q", got, PollMessage)
	}
}

// classifyPollOutcome is the pure decision LastPollOutcome is built on. Table
// it directly so the priority order (error beats envelope beats a mid-poll
// 401 beats an ordinary empty 202) is pinned independent of any wire fixture.
func TestClassifyPollOutcome(t *testing.T) {
	someErr := errors.New("boom")
	tests := []struct {
		name    string
		capture *pollCapture
		env     *Envelope
		err     error
		want    PollOutcome
	}{
		{"nil capture, no error, no envelope", nil, nil, nil, PollEmpty},
		{"error always wins, even with an envelope", &pollCapture{}, &Envelope{}, someErr, PollOther},
		{"error wins over an observed 401", &pollCapture{unauthorized: true}, nil, someErr, PollOther},
		{"a delivered envelope wins over an observed 401", &pollCapture{unauthorized: true}, &Envelope{}, nil, PollMessage},
		{"an observed 401 with no envelope and no error", &pollCapture{unauthorized: true}, nil, nil, PollRefreshed},
		{"an ordinary empty 202", &pollCapture{}, nil, nil, PollEmpty},
		{"a wrapped refusal is refused, not other", &pollCapture{}, nil, fmt.Errorf("%w: %w", ErrEnvelopeRefused, someErr), PollRefused},
		{"a plain error is other, not refused", &pollCapture{}, nil, someErr, PollOther},
		{"a quarantine is quarantined, not refused", &pollCapture{}, nil, fmt.Errorf("%w: reason=x", ErrEnvelopeQuarantined), PollQuarantined},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyPollOutcome(tt.capture, tt.env, tt.err); got != tt.want {
				t.Errorf("classifyPollOutcome(%+v, %v, %v) = %q, want %q", tt.capture, tt.env, tt.err, got, tt.want)
			}
		})
	}
}

func TestSDKPollRejectsBeforeACK(t *testing.T) {
	for _, scenario := range []string{"unknown event", "foreign repository", "write failure", "oversize", "truncated", "redirect", "503", "missing statistics"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			e := event(key, 1, scaleset.MessageTypeJobAvailable)
			if scenario == "unknown event" {
				e.MessageType = "FutureEvent"
			}
			if scenario == "foreign repository" {
				e.RepositoryName = "elsewhere"
			}
			f.configure(func() {
				f.wire = wireEnvelope(t, []Event{e})
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method != "GET" || r.URL.Path != "/queue" {
						return false
					}
					switch scenario {
					case "oversize":
						w.Write(bytes.Repeat([]byte("x"), maxQueueBytes+1))
					case "truncated":
						w.Header().Set("Content-Length", "100")
						fmt.Fprint(w, "{")
					case "redirect":
						w.Header().Set("Location", "/redirected")
						w.WriteHeader(307)
					case "503":
						w.WriteHeader(503)
						fmt.Fprint(w, "private-response-sentinel")
					case "missing statistics":
						fmt.Fprint(w, `{"messageId":7,"messageType":"RunnerScaleSetJobMessages"}`)
					default:
						return false
					}
					return true
				}
			})
			if scenario == "write failure" {
				if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
					t.Fatal(err)
				}
			}
			_, err := q.Poll(ctx, 1)
			if err == nil || strings.Contains(err.Error(), "private-response-sentinel") {
				t.Fatalf("poll failure hidden/leaked: %v", err)
			}
			if f.count("GET", "/queue") != 1 || f.count("GET", "/redirected") != 0 || f.count("DELETE", "/queue/7") != 0 {
				t.Fatal("failed poll retried, redirected, or acknowledged")
			}
			if cursor, err := s.Cursor(ctx, key, f.id); err != nil || cursor != 0 {
				t.Fatalf("cursor advanced: %d %v", cursor, err)
			}
		})
	}
}

// TestSDKQuarantinesAfterThreeRefusals pins homelab#1139: an envelope refused
// the same deterministic way on every redelivery is acknowledged on the third
// refusal instead of blocking its session's queue forever. All four refusal
// kinds run end to end through Poll. The journal keeps identity, a reason
// word, the event count and the type names only, never the payload.
func TestSDKQuarantinesAfterThreeRefusals(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reason     string
		events     int
		eventTypes string
		cursor     int
		// durable marks a case whose message 7 has a committed messages row.
		durable bool
		// arrange prepares the journal and returns the events of the envelope
		// that is refused on every poll.
		arrange func(t *testing.T, s *Store, f *sdkFixture, q *QueueSession, key SetKey) []Event
	}{
		{"foreign repository", refusalForeignRepository, 1, "JobAvailable", 0, false,
			func(t *testing.T, s *Store, f *sdkFixture, q *QueueSession, key SetKey) []Event {
				e := event(key, 1, scaleset.MessageTypeJobAvailable)
				e.JobID, e.RepositoryName = "payload-sentinel-job", "payload-sentinel-repository"
				return []Event{e}
			}},
		// A body that fails to parse has no trusted events: 0 and empty.
		{"unknown event type", refusalInvalidEnvelope, 0, "", 0, false,
			func(t *testing.T, s *Store, f *sdkFixture, q *QueueSession, key SetKey) []Event {
				e := event(key, 1, scaleset.MessageTypeJobAvailable)
				e.JobID, e.MessageType = "payload-sentinel-job", "PayloadSentinelEvent"
				return []Event{e}
			}},
		// Message 7 was committed and its ACK lost, then GitHub redelivers 7
		// with different events. The quarantine must acknowledge the durable
		// row too, or the cursor stays pinned behind it for good.
		{"redelivery changed", refusalRedeliveryChanged, 1, "JobCompleted", 7, true,
			func(t *testing.T, s *Store, f *sdkFixture, q *QueueSession, key SetKey) []Event {
				commitWithLostACK(t, f, q, key)
				e := event(key, 1, scaleset.MessageTypeJobCompleted)
				e.RunnerName = "payload-sentinel-runner"
				return []Event{e}
			}},
		// Message 7 was committed and its ACK lost, then the repository was
		// renamed, so the unchanged redelivery is refused as foreign before the
		// earlier-delivery lookup runs. The durable row must be acknowledged
		// whatever the refusal reason, or the cursor stays pinned behind it.
		{"foreign repository with a durable row", refusalForeignRepository, 1, "JobAvailable", 7, true,
			func(t *testing.T, s *Store, f *sdkFixture, q *QueueSession, key SetKey) []Event {
				commitWithLostACK(t, f, q, key)
				mustExec(t, s, `UPDATE repositories SET full_name='renamed/elsewhere' WHERE id=?`, key.RepositoryID)
				return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
			}},
		// Request 1 and job "conflict-job" are already two different durable
		// jobs; an event naming both cannot be merged into either.
		{"job identity conflict", refusalJobIdentityConflict, 1, "JobAssigned", 0, false,
			func(t *testing.T, s *Store, f *sdkFixture, q *QueueSession, key SetKey) []Event {
				offer(t, s, key, 1)
				brokerAssign(t, s, key, "conflict-job", 2)
				e := event(key, 1, scaleset.MessageTypeJobAssigned)
				e.JobID, e.RunnerName = "conflict-job", "payload-sentinel-runner"
				return []Event{e}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			events := tc.arrange(t, s, f, q, key)
			heard, _ := s.Liveness()
			deletes := f.count("DELETE", "/queue/7")
			f.configure(func() { f.wire = wireEnvelope(t, events) })
			for poll := 1; poll <= refusalQuarantineThreshold; poll++ {
				_, err := q.Poll(ctx, 1)
				want, wantDeletes := PollRefused, deletes
				if poll == refusalQuarantineThreshold {
					want, wantDeletes = PollQuarantined, deletes+1
				}
				if err == nil || q.LastPollOutcome() != want || f.count("DELETE", "/queue/7") != wantDeletes {
					t.Fatalf("poll %d: outcome=%q deletes=%d err=%v; want %q with %d deletes",
						poll, q.LastPollOutcome(), f.count("DELETE", "/queue/7"), err, want, wantDeletes)
				}
				if poll == refusalQuarantineThreshold && !errors.Is(err, ErrEnvelopeQuarantined) {
					t.Fatalf("the quarantining poll did not return ErrEnvelopeQuarantined: %v", err)
				}
			}
			var reason, eventTypes, acked string
			var refusals, eventCount int
			if err := s.db.QueryRow(`SELECT reason,refusals,events,event_types,acked FROM refused_envelopes WHERE repo_id=? AND set_id=? AND session=? AND message_id=7`,
				key.RepositoryID, key.ScaleSetID, f.id.String()).Scan(&reason, &refusals, &eventCount, &eventTypes, &acked); err != nil {
				t.Fatal(err)
			}
			if reason != tc.reason || refusals != refusalQuarantineThreshold || acked == "" || eventCount != tc.events || eventTypes != tc.eventTypes {
				t.Fatalf("refused_envelopes row: reason=%q refusals=%d events=%d event_types=%q acked=%q",
					reason, refusals, eventCount, eventTypes, acked)
			}
			if tc.durable {
				if got := messageAcked(t, f, key); got != 1 {
					t.Fatalf("the quarantined redelivery left its messages row acked=%d", got)
				}
			}
			if cursor, err := s.Cursor(ctx, key, f.id); err != nil || cursor != tc.cursor {
				t.Fatalf("cursor after quarantine = %d, want %d: %v", cursor, tc.cursor, err)
			}
			if last, _ := s.Liveness(); !last.Equal(heard) {
				t.Fatalf("a quarantine counted as a heard envelope: %v -> %v", heard, last)
			}
			// GitHub no longer owes the message, so the next poll is an ordinary
			// 202, and it asks from past the quarantined ID when one was journaled.
			f.configure(func() { f.wire = nil })
			if env, err := q.Poll(ctx, 1); err != nil || env != nil || q.LastPollOutcome() != PollEmpty {
				t.Fatalf("poll after quarantine: %+v %v %q", env, err, q.LastPollOutcome())
			}
			f.configure(func() {
				// The SDK omits lastMessageId at cursor 0, which the fixture records as "".
				want := ""
				if tc.cursor > 0 {
					want = strconv.Itoa(tc.cursor)
				}
				if got := f.queries[len(f.queries)-1]; got != want {
					t.Errorf("the poll after quarantine asked from %q, want %q", got, want)
				}
			})
			for _, name := range []string{"pool.sqlite", "pool.sqlite-wal"} {
				data, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(data, []byte("payload-sentinel")) || bytes.Contains(data, []byte("PayloadSentinel")) {
					t.Fatalf("refused payload entered %s", name)
				}
			}
		})
	}
}

// TestQuarantineCrashAfterDeleteDoesNotPinTheCursor pins the crash window
// between the quarantine DELETE and MarkQuarantined. For redelivery-changed a
// durable messages row exists; if it were acknowledged only after the DELETE,
// a crash there would leave it acked=0 for good and the cursor pinned behind
// it. The trigger makes MarkQuarantined fail after the DELETE succeeded.
func TestQuarantineCrashAfterDeleteDoesNotPinTheCursor(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	commitWithLostACK(t, f, q, key)
	if _, err := s.db.Exec(`CREATE TRIGGER lost_quarantine_stamp BEFORE UPDATE OF acked ON refused_envelopes BEGIN SELECT RAISE(ABORT,'crash'); END`); err != nil {
		t.Fatal(err)
	}
	deletes := f.count("DELETE", "/queue/7")
	f.configure(func() { f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobCompleted)}) })
	var err error
	for poll := 1; poll <= refusalQuarantineThreshold; poll++ {
		_, err = q.Poll(ctx, 1)
	}
	// Proves the simulated crash happened: the DELETE went out and the stamp
	// after it failed. Without this the assertions below could pass vacuously.
	if !errors.Is(err, ErrEnvelopeQuarantined) || !strings.Contains(err.Error(), "could not be recorded") || f.count("DELETE", "/queue/7") != deletes+1 {
		t.Fatalf("the DELETE-then-stamp-failure window was not reached: deletes=%d err=%v", f.count("DELETE", "/queue/7")-deletes, err)
	}
	var stamp string
	if err := s.db.QueryRow(`SELECT acked FROM refused_envelopes WHERE message_id=7`).Scan(&stamp); err != nil || stamp != "" {
		t.Fatalf("the quarantine stamp was recorded despite the trigger: %q %v", stamp, err)
	}
	if got := messageAcked(t, f, key); got != 1 {
		t.Fatalf("the redelivered message stayed acked=%d after its quarantine DELETE", got)
	}
	if cursor, err := s.Cursor(ctx, key, f.id); err != nil || cursor != 7 {
		t.Fatalf("the cursor is pinned at %d after the crash window: %v", cursor, err)
	}
}

// TestQuarantineFailureLeavesTheDurableRowUnacked pins the two failure branches
// around a quarantine of a message with a durable messages row (committed, ACK
// lost, then refused as foreign-repository). Either way the quarantine returns
// the refusal, the row ends at acked=0 and the cursor does not pass a message
// GitHub still holds.
func TestQuarantineFailureLeavesTheDurableRowUnacked(t *testing.T) {
	for _, tc := range []struct {
		name string
		// inject arranges the failure after the durable row exists.
		inject     func(t *testing.T, s *Store, f *sdkFixture)
		deleteSent bool
	}{
		// The pre-DELETE ack fails: no DELETE may go out.
		{"ack fails", func(t *testing.T, s *Store, f *sdkFixture) {
			mustExec(t, s, `CREATE TRIGGER lost_ack BEFORE UPDATE OF acked ON messages BEGIN SELECT RAISE(ABORT,'ack-io'); END`)
		}, false},
		// The ack succeeds and the DELETE fails: the ack must be reverted.
		{"DELETE fails after the ack", func(t *testing.T, s *Store, f *sdkFixture) {
			f.configure(func() {
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == http.MethodDelete && r.URL.Path == "/queue/7" {
						w.WriteHeader(http.StatusInternalServerError)
						return true
					}
					return false
				}
			})
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			commitWithLostACK(t, f, q, key)
			mustExec(t, s, `UPDATE repositories SET full_name='renamed/elsewhere' WHERE id=?`, key.RepositoryID)
			tc.inject(t, s, f)
			deletes := f.count("DELETE", "/queue/7")
			f.configure(func() { f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}) })
			var err error
			for poll := 1; poll <= refusalQuarantineThreshold; poll++ {
				_, err = q.Poll(ctx, 1)
			}
			wantDeletes := deletes
			if tc.deleteSent {
				wantDeletes++
			}
			if !errors.Is(err, ErrEnvelopeRefused) || errors.Is(err, ErrEnvelopeQuarantined) || q.LastPollOutcome() != PollRefused {
				t.Fatalf("the failed quarantine did not return the refusal: outcome=%q err=%v", q.LastPollOutcome(), err)
			}
			if got := f.count("DELETE", "/queue/7"); got != wantDeletes {
				t.Fatalf("quarantine DELETEs = %d, want %d", got-deletes, wantDeletes-deletes)
			}
			if got := messageAcked(t, f, key); got != 0 {
				t.Fatalf("the durable row is acked=%d after a failed quarantine", got)
			}
			var stamp string
			if err := s.db.QueryRow(`SELECT acked FROM refused_envelopes WHERE message_id=7`).Scan(&stamp); err != nil || stamp != "" {
				t.Fatalf("a failed quarantine was stamped: %q %v", stamp, err)
			}
			if cursor, err := s.Cursor(ctx, key, f.id); err != nil || cursor != 0 {
				t.Fatalf("the cursor advanced to %d past a message GitHub still holds: %v", cursor, err)
			}
			f.configure(func() { f.wire, f.override = nil, nil })
			if _, err := q.Poll(ctx, 1); err != nil {
				t.Fatalf("poll after the failed quarantine: %v", err)
			}
			f.configure(func() {
				if got := f.queries[len(f.queries)-1]; got != "" {
					t.Errorf("the poll after a failed quarantine asked from %q; want lastMessageId omitted", got)
				}
			})
		})
	}
}

// TestTransientRecordFailuresNeverCountTowardQuarantine: a storage fault is
// not a property of the envelope, so however often it repeats, the message is
// never acknowledged unapplied. One case per unwrapped error return in
// RecordEnvelope and resolveJobKeyTx that a poll can reach; refused_envelopes
// stays writable in every case, so a wrong classification would show. Each
// case also asserts the error it expects, so a case that stopped reaching its
// site fails instead of passing on some other error.
//
// Not covered, because a poll cannot reach them without a fault this fixture
// cannot inject: the nil-session check, the encode failure, BeginTx, the
// earlier-delivery lookup's non-ErrNoRows error, settleRunnerEventTx, and Commit.
func TestTransientRecordFailuresNeverCountTowardQuarantine(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange injects the fault and returns the envelope's events.
		arrange func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event
		reached func(error) bool
	}{
		{"scale-set row missing (full_name lookup)", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			mustExec(t, s, `DELETE FROM scale_sets WHERE repo_id=?`, key.RepositoryID)
			return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
		}, func(err error) bool { return errors.Is(err, sql.ErrNoRows) }},
		{"earlier delivery unreadable", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			mustExec(t, s, `INSERT INTO messages(repo_id,set_id,session,message_id,payload,received) VALUES(?,?,?,7,'not json','')`,
				key.RepositoryID, key.ScaleSetID, f.id.String())
			return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
		}, func(err error) bool { return strings.Contains(err.Error(), "invalid character") }},
		{"scale_sets UPDATE fails", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			mustExec(t, s, `CREATE TRIGGER transient_io BEFORE UPDATE ON scale_sets BEGIN SELECT RAISE(ABORT,'scale-set-io'); END`)
			return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
		}, func(err error) bool { return strings.Contains(err.Error(), "scale-set-io") }},
		{"messages INSERT fails", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			mustExec(t, s, `CREATE TRIGGER transient_io BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'messages-io'); END`)
			return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
		}, func(err error) bool { return strings.Contains(err.Error(), "messages-io") }},
		{"jobs unreadable (resolveJobKeyTx)", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			mustExec(t, s, `ALTER TABLE jobs RENAME TO jobs_unreadable`)
			return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
		}, func(err error) bool { return strings.Contains(err.Error(), "no such table: jobs") }},
		{"jobs INSERT fails", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			mustExec(t, s, `CREATE TRIGGER transient_io BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT,'jobs-insert-io'); END`)
			return []Event{event(key, 1, scaleset.MessageTypeJobAvailable)}
		}, func(err error) bool { return strings.Contains(err.Error(), "jobs-insert-io") }},
		{"jobs UPDATE fails", func(t *testing.T, s *Store, f *sdkFixture, key SetKey) []Event {
			offer(t, s, key, 1)
			mustExec(t, s, `CREATE TRIGGER transient_io BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT,'jobs-update-io'); END`)
			return []Event{event(key, 1, scaleset.MessageTypeJobAssigned)}
		}, func(err error) bool { return strings.Contains(err.Error(), "jobs-update-io") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			events := tc.arrange(t, s, f, key)
			f.configure(func() { f.wire = wireEnvelope(t, events) })
			for poll := 1; poll <= refusalQuarantineThreshold+1; poll++ {
				_, err := q.Poll(ctx, 1)
				if err == nil || !tc.reached(err) {
					t.Fatalf("poll %d did not fail at the injected fault: %v", poll, err)
				}
			}
			if deletes := f.count("DELETE", "/queue/7"); deletes != 0 {
				t.Fatalf("a transient failure was acknowledged %d times", deletes)
			}
			var rows int
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM refused_envelopes`).Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("a transient failure was counted as a refusal: rows=%d %v", rows, err)
			}
		})
	}
}

func mustExec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

// TestRefusalReasonOf pins which errors count toward quarantine: the four
// deterministic refusal kinds, and nothing that may be transient.
func TestRefusalReasonOf(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason string
		ok     bool
	}{
		{"invalid envelope", refuse(refusalInvalidEnvelope, errors.New("x")), refusalInvalidEnvelope, true},
		{"foreign repository", refuse(refusalForeignRepository, errors.New("x")), refusalForeignRepository, true},
		{"redelivery changed", refuse(refusalRedeliveryChanged, errors.New("x")), refusalRedeliveryChanged, true},
		{"job identity conflict", refuse(refusalJobIdentityConflict, errors.New("x")), refusalJobIdentityConflict, true},
		{"wrapped under ErrEnvelopeRefused", fmt.Errorf("%w: %w", ErrEnvelopeRefused, refuse(refusalForeignRepository, errors.New("x"))), refusalForeignRepository, true},
		{"missing row", sql.ErrNoRows, "", false},
		{"canceled", context.Canceled, "", false},
		{"plain refusal sentinel", ErrEnvelopeRefused, "", false},
		{"nil", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if reason, ok := refusalReasonOf(tc.err); reason != tc.reason || ok != tc.ok {
				t.Fatalf("refusalReasonOf(%v) = %q, %t; want %q, %t", tc.err, reason, ok, tc.reason, tc.ok)
			}
		})
	}
}

func TestSDKLostACKRedelivery(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	f.configure(func() {
		f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)})
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "DELETE" && r.URL.Path == "/queue/7" && f.counts["DELETE /queue/7"] == 1 {
				// Commit observed even when the ACK response gets lost.
				pending, err := s.PendingMessages(ctx)
				if err != nil || len(pending) != 1 {
					t.Errorf("ACK before commit: %+v %v", pending, err)
				}
				w.WriteHeader(503)
				return true
			}
			return false
		}
	})
	if _, err := q.Poll(ctx, 1); err == nil {
		t.Fatal("lost ACK accepted")
	}
	lease := mustReserve(t, s)
	if _, err := q.Poll(ctx, 1); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM jobs WHERE job_key='1'`).Scan(&state); err != nil || state != "reserved" {
		t.Fatalf("redelivery reapplied offer: %s %v", state, err)
	}
	if cursor, err := s.Cursor(ctx, key, f.id); err != nil || cursor != 7 {
		t.Fatalf("ACK cursor: %d %v", cursor, err)
	}
	if f.count("DELETE", "/queue/7") != 2 {
		t.Fatal("unexpected ACK retry count")
	}
	finish(t, s, lease)
}

func TestSDKRefreshIdentityFence(t *testing.T) {
	for _, operation := range []string{"poll", "acquire", "ack"} {
		for _, drift := range []string{"none", "uuid", "owner", "set", "queue", "foreign origin"} {
			t.Run(operation+"/"+drift, func(t *testing.T) {
				s, _ := openTestStore(t, 1)
				key := prepare(t, s, 1)[0]
				f := newSDKFixture(t, s, key)
				q := f.open()
				method, path := "GET", "/queue"
				if operation == "acquire" {
					method, path = "POST", strings.TrimSuffix(f.sessionPath(), "/sessions")+"/acquirejobs"
				}
				if operation == "ack" {
					method, path = "DELETE", "/queue/7"
				}
				f.configure(func() {
					f.wire = wireEnvelope(t, []Event{event(key, 1, scaleset.MessageTypeJobAvailable)})
					f.override = func(w http.ResponseWriter, r *http.Request) bool {
						if r.Method == method && r.URL.Path == path && f.counts[method+" "+path] == 1 {
							w.WriteHeader(401)
							return true
						}
						if r.Method == "PATCH" {
							session := f.session()
							session.MessageQueueAccessToken = "refreshed-secret"
							switch drift {
							case "uuid":
								session.SessionID = uuid.New()
							case "owner":
								session.OwnerName = "foreign"
							case "set":
								session.RunnerScaleSet.ID++
							case "queue":
								session.MessageQueueURL += "/elsewhere"
							case "foreign origin":
								session.MessageQueueURL = "https://broker.example.invalid/queue"
							}
							json.NewEncoder(w).Encode(session)
							return true
						}
						return false
					}
				})
				var err error
				if operation == "acquire" {
					offer(t, s, key, 1)
					lease := mustReserve(t, s)
					err = q.Acquire(ctx, lease.ID)
					stored, readErr := s.Lease(ctx, lease.ID)
					want := Acquired
					if drift != "none" {
						want = AcquisitionUnknown
					}
					if readErr != nil || stored.Phase != want {
						t.Fatalf("lease after refresh: %+v %v", stored, readErr)
					}
				} else {
					_, err = q.Poll(ctx, 1)
				}
				wantCalls := 2
				if drift != "none" {
					wantCalls = 1
				}
				if (err == nil) != (drift == "none") || f.count(method, path) != wantCalls {
					t.Fatalf("refresh fence: err=%v calls=%d want=%d", err, f.count(method, path), wantCalls)
				}
				if drift != "none" && q.sdk.Session().MessageQueueAccessToken == "refreshed-secret" {
					t.Fatal("foreign session reached SDK")
				}
			})
		}
	}
}

func TestSDKResumedSessionRetainsRefreshIdentityFence(t *testing.T) {
	for _, drift := range []string{"none", "queue", "foreign origin"} {
		t.Run(drift, func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			created := f.open()
			restarted := restartActiveSession(t, s, dir, f, created)
			if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
				t.Fatal(err)
			}
			q, err := restarted.resumeQueueSession(ctx, key, f.client, f.server.URL)
			if err != nil {
				t.Fatal(err)
			}
			f.configure(func() {
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == http.MethodGet && r.URL.Path == "/queue" && f.counts["GET /queue"] == 1 {
						w.WriteHeader(http.StatusUnauthorized)
						return true
					}
					if r.Method == http.MethodPatch {
						session := f.session()
						session.MessageQueueAccessToken = "refreshed-secret"
						if drift == "queue" {
							session.MessageQueueURL += "/foreign"
						}
						if drift == "foreign origin" {
							session.MessageQueueURL = "https://broker.example.invalid/queue"
						}
						json.NewEncoder(w).Encode(session)
						return true
					}
					return false
				}
			})
			_, err = q.Poll(ctx, 0)
			wantGets := 2
			if drift != "none" {
				wantGets = 1
			}
			if (err == nil) != (drift == "none") || f.count("GET", "/queue") != wantGets || f.count("PATCH", f.sessionPath()+"/"+f.id.String()) != 2 {
				t.Fatalf("resumed refresh fence: err=%v gets=%d", err, f.count("GET", "/queue"))
			}
			if drift != "none" && q.sdk.Session().MessageQueueAccessToken == "refreshed-secret" {
				t.Fatal("foreign refresh reached resumed SDK handle")
			}
			// The polls= cycle field is only worth reading if a REAL wire 401
			// that the SDK refreshed around shows up as 401:. A synthetic
			// pollCapture flag cannot prove the capture hook sees the response
			// the SDK retries past; this path puts an actual 401 on the wire.
			if drift == "none" {
				if got := q.LastPollOutcome(); got != PollRefreshed {
					t.Fatalf("a wire 401 the SDK refreshed around was recorded as %q, want %q", got, PollRefreshed)
				}
			}
		})
	}
}

func TestSDKUncertainRefreshPermanentlyFencesHandle(t *testing.T) {
	for _, scenario := range []string{"503", "truncated", "lost response", "statistics write"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			f.configure(func() {
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == http.MethodGet && r.URL.Path == "/queue" {
						w.WriteHeader(http.StatusUnauthorized)
						return true
					}
					if r.Method != http.MethodPatch {
						return false
					}
					switch scenario {
					case "503":
						w.WriteHeader(http.StatusServiceUnavailable)
					case "truncated":
						w.Header().Set("Content-Length", "1000")
						fmt.Fprint(w, "{")
					case "lost response":
						panic(http.ErrAbortHandler)
					case "statistics write":
						if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
							t.Error(err)
						}
						json.NewEncoder(w).Encode(f.session())
					}
					return true
				}
			})
			if _, err := q.Poll(ctx, 0); err == nil {
				t.Fatal("uncertain refresh accepted")
			}
			if _, err := q.Poll(ctx, 0); err == nil {
				t.Fatal("uncertain refresh handle remained active")
			}
			if f.count("PATCH", f.sessionPath()+"/"+f.id.String()) != 1 || f.count("GET", "/queue") != 1 {
				t.Fatal("uncertain refresh was replayed")
			}
		})
	}
}

func TestNativeResumedHandleNeedsControllerRefreshFence(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	created := f.open()
	created.http.HTTPClient.CloseIdleConnections()
	records, err := s.PendingSessions(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("active identity: %+v %v", records, err)
	}
	sessionHTTP := retryablehttp.NewClient()
	sessionHTTP.RetryMax = 0
	sessionHTTP.CheckRetry = func(_ context.Context, _ *http.Response, err error) (bool, error) { return false, err }
	sessionHTTP.HTTPClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("test redirect refused") }
	native, err := f.client.ResumeMessageSessionClient(ctx, scaleset.SessionResumeIdentity{
		ScaleSetID:  key.ScaleSetID,
		Owner:       records[0].Owner,
		SessionID:   f.id,
		QueueSHA256: sha256.Sum256([]byte(f.server.URL + "/queue")),
	}, scaleset.WithRetryableHTTPClint(sessionHTTP))
	if err != nil {
		t.Fatal(err)
	}
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodGet && r.URL.Path == "/queue" {
				w.WriteHeader(http.StatusUnauthorized)
				return true
			}
			if r.Method == http.MethodPatch {
				session := f.session()
				session.MessageQueueURL = f.server.URL + "/foreign"
				session.MessageQueueAccessToken = "foreign-token"
				json.NewEncoder(w).Encode(session)
				return true
			}
			if r.Method == http.MethodGet && r.URL.Path == "/foreign" {
				if r.Header.Get("Authorization") != "Bearer foreign-token" {
					t.Error("foreign token was not used by unguarded native refresh")
				}
				w.WriteHeader(http.StatusAccepted)
				return true
			}
			return false
		}
	})
	if message, err := native.GetMessage(ctx, 0, 0); err != nil || message != nil {
		t.Fatalf("negative control did not follow native foreign refresh: %+v %v", message, err)
	}
	if f.count("GET", "/foreign") != 1 {
		t.Fatal("negative control did not demonstrate the missing native refresh fence")
	}
}

func TestSDKUncertainAcquisitionNeverRetries(t *testing.T) {
	for _, scenario := range []string{"503", "redirect", "truncated", "bad count"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			f := newSDKFixture(t, s, key)
			q := f.open()
			offer(t, s, key, 1)
			lease := mustReserve(t, s)
			path := strings.TrimSuffix(f.sessionPath(), "/sessions") + "/acquirejobs"
			f.configure(func() {
				f.override = func(w http.ResponseWriter, r *http.Request) bool {
					if r.URL.Path != path {
						return false
					}
					stored, err := s.Lease(ctx, lease.ID)
					if err != nil || stored.Phase != AcquisitionIntent {
						t.Errorf("no acquisition intent: %+v %v", stored, err)
					}
					switch scenario {
					case "503":
						w.WriteHeader(503)
					case "redirect":
						w.Header().Set("Location", "/replayed")
						w.WriteHeader(307)
					case "truncated":
						w.Header().Set("Content-Length", "100")
						fmt.Fprint(w, "{")
					case "bad count":
						fmt.Fprint(w, `{"count":1,"value":[]}`)
					}
					return true
				}
			})
			if err := q.Acquire(ctx, lease.ID); err == nil {
				t.Fatal("uncertain acquisition accepted")
			}
			if err := q.Acquire(ctx, lease.ID); err == nil {
				t.Fatal("uncertain operation replayed")
			}
			stored, err := s.Lease(ctx, lease.ID)
			if err != nil || stored.Phase != AcquisitionUnknown || f.count("POST", path) != 1 || f.count("POST", "/replayed") != 0 {
				t.Fatalf("uncertainty escaped: %+v %v", stored, err)
			}
		})
	}
}

func TestSDKScopeAndAdvertisedCapacity(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	f := newSDKFixture(t, s, keys[0])
	q := f.open()
	offer(t, s, keys[1], 1)
	lease := mustReserve(t, s)
	if err := q.Acquire(ctx, lease.ID); err == nil {
		t.Fatal("foreign lease accepted")
	}
	stored, err := s.Lease(ctx, lease.ID)
	if err != nil || stored.Phase != Reserved {
		t.Fatalf("foreign lease mutated: %+v %v", stored, err)
	}
	for _, cap := range []int{-1, 3} {
		if _, err := q.Poll(ctx, cap); err == nil {
			t.Fatal("invalid advertisement accepted")
		}
	}
	if f.count("GET", "/queue") != 0 {
		t.Fatal("invalid advertisement sent")
	}
}

type brokenBody struct {
	closeFailure bool
	closed       bool
}

func (b *brokenBody) Read([]byte) (int, error) {
	if b.closeFailure {
		return 0, io.EOF
	}
	return 0, errors.New("read sentinel")
}
func (b *brokenBody) Close() error {
	b.closed = true
	if b.closeFailure {
		return errors.New("close sentinel")
	}
	return nil
}

func TestResponseReplayPreservesReadAndCloseFailures(t *testing.T) {
	for _, closeFailure := range []bool{false, true} {
		original := &brokenBody{closeFailure: closeFailure}
		resp := &http.Response{Body: original}
		if _, err := readResponse(resp); err == nil || !original.closed {
			t.Fatal("original body failure hidden or leaked")
		}
		if _, err := io.ReadAll(resp.Body); err == nil {
			t.Fatal("SDK would see a successful replay")
		}
	}
}

func TestSDKAcquisitionDoesNotWaitForLongPoll(t *testing.T) {
	s, _ := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	f := newSDKFixture(t, s, key)
	q := f.open()
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	f.configure(func() {
		f.override = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method != "GET" || r.URL.Path != "/queue" {
				return false
			}
			// The normal fixture serializes its mutable response controls. Release
			// that fixture mutex here so an acquisition can reach the same server.
			f.mu.Unlock()
			defer f.mu.Lock()
			close(entered)
			select {
			case <-release:
				w.WriteHeader(http.StatusAccepted)
			case <-r.Context().Done():
			}
			return true
		}
	})
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	polled := make(chan error, 1)
	go func() { _, err := q.Poll(pollCtx, 1); polled <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not reach server")
	}
	acquired := make(chan error, 1)
	go func() { acquired <- q.Acquire(ctx, lease.ID) }()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition waited for long poll")
	}
	cancel()
	select {
	case err := <-polled:
		if err == nil {
			t.Fatal("canceled long poll succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("poll cancellation did not finish")
	}
}
