package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/bootstrap"
)

var testJIT = base64.StdEncoding.EncodeToString([]byte(`{".credentials":"credential-sentinel"}`))

type fakeJIT struct {
	s                                                *Store
	lease                                            Lease
	runner                                           *scaleset.RunnerReference
	lookups, creates, removes                        int
	lostCreate, lateCreate, lostRemove, retainRemove bool
	generate                                         func() (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	lookup                                           func() (*scaleset.RunnerReference, error)
}

func (f *fakeJIT) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	f.lookups++
	if name != f.lease.RunnerName {
		return nil, errors.New("wrong fixture name")
	}
	if f.lookup != nil {
		return f.lookup()
	}
	return f.runner, nil
}
func (f *fakeJIT) GenerateJitRunnerConfig(_ context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, set int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.creates++
	record, err := f.s.JIT(ctx, f.lease.ID)
	if err != nil || record.Phase != "jit-intent" || setting.Name != f.lease.RunnerName || setting.WorkFolder != "_work" || set != f.lease.Set.ScaleSetID {
		return nil, errors.New("POST preceded scoped intent")
	}
	if f.generate != nil {
		return f.generate()
	}
	if !f.lateCreate {
		f.runner = &scaleset.RunnerReference{ID: 77, Name: setting.Name, RunnerScaleSetID: set}
	}
	if f.lostCreate {
		return nil, errors.New("upstream error with credential-sentinel")
	}
	return &scaleset.RunnerScaleSetJitRunnerConfig{Runner: f.runner, EncodedJITConfig: testJIT}, nil
}
func (f *fakeJIT) RemoveRunner(_ context.Context, id int64) error {
	f.removes++
	r, err := f.s.JIT(ctx, f.lease.ID)
	if err != nil || r.Removal != "remove-intent" || r.RunnerID != int(id) || !r.Revoked {
		return errors.New("DELETE preceded scoped intent")
	}
	if !f.retainRemove {
		f.runner = nil
	}
	if f.lostRemove {
		return errors.New("lost DELETE")
	}
	return nil
}

func jitFixture(t *testing.T) (*Store, string, Lease, *JITBroker, *fakeJIT) {
	t.Helper()
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 2)
	lease := mustReserve(t, s)
	if err := s.Transition(ctx, lease.ID, Reserved, AcquisitionIntent); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, AcquisitionIntent, Acquired); err != nil {
		t.Fatal(err)
	}
	f := &fakeJIT{s: s, lease: lease}
	b := newJITBroker(s, func(_ context.Context, l Lease) (jitAPI, error) {
		if l.Set != key {
			return nil, errors.New("wrong repository client")
		}
		return f, nil
	})
	return s, dir, lease, b, f
}

func jitRequest(t *testing.T, id string, token bootstrap.Token) *http.Request {
	t.Helper()
	path, err := bootstrap.Path(id)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "https://bootstrap.example"+path, nil)
	r.TLS = &tls.ConnectionState{}
	r.Header.Set("Authorization", "Bearer "+token.Encoded())
	return r
}

func TestJITOneUseTLSDeliveryAndJournalOmission(t *testing.T) {
	s, dir, lease, b, f := jitFixture(t)
	token, err := b.Prepare(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(b)
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	payload, err := bootstrap.Fetch(ctx, server.URL, lease.ID, token, roots)
	if err != nil || string(payload) != testJIT {
		t.Fatalf("TLS handoff failed: %v", err)
	}
	if _, err := bootstrap.Fetch(ctx, server.URL, lease.ID, token, roots); err == nil {
		t.Fatal("replayed capability delivered JIT")
	}
	if _, err := b.Prepare(ctx, lease.ID); err == nil || f.creates != 1 {
		t.Fatal("JIT POST repeated")
	}
	record, err := s.JIT(ctx, lease.ID)
	if err != nil || record.Phase != "stream-written" || record.RunnerID != 77 {
		t.Fatalf("wrong delivery journal: %+v %v", record, err)
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatal("delivery released reservation")
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		body, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{token.Encoded(), testJIT, "credential-sentinel"} {
			if bytes.Contains(body, []byte(secret)) {
				t.Fatal("secret persisted in controller state")
			}
		}
	}
}

func TestJITWrongScopeAndConcurrentReplay(t *testing.T) {
	s, _, lease, b, _ := jitFixture(t)
	token, err := b.Prepare(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	wrong, _ := bootstrap.NewToken()
	for _, mode := range []string{"wrong token", "http", "method", "query", "body", "transfer encoding", "duplicate auth", "escaped path"} {
		r := jitRequest(t, lease.ID, token)
		switch mode {
		case "wrong token":
			r.Header.Set("Authorization", "Bearer "+wrong.Encoded())
		case "http":
			r.TLS = nil
		case "method":
			r.Method = "POST"
		case "query":
			r.URL.RawQuery = "token=x"
		case "body":
			r.Body = io.NopCloser(strings.NewReader("x"))
			r.ContentLength = 1
		case "transfer encoding":
			r.TransferEncoding = []string{"chunked"}
		case "duplicate auth":
			r.Header.Add("Authorization", "Bearer "+token.Encoded())
		case "escaped path":
			r.URL.RawPath = strings.Replace(r.URL.Path, "jit", "%6ait", 1)
		}
		w := httptest.NewRecorder()
		b.ServeHTTP(w, r)
		if w.Code != 403 || strings.Contains(w.Body.String(), testJIT) {
			t.Fatalf("%s bypassed authentication", mode)
		}
	}
	record, _ := s.JIT(ctx, lease.ID)
	if record.Phase != "ready" {
		t.Fatal("invalid token consumed another worker's capability")
	}
	var wg sync.WaitGroup
	codes := make(chan int, 20)
	for range 20 {
		wg.Go(func() { w := httptest.NewRecorder(); b.ServeHTTP(w, jitRequest(t, lease.ID, token)); codes <- w.Code })
	}
	wg.Wait()
	close(codes)
	accepted := 0
	for code := range codes {
		if code == 200 {
			accepted++
		} else if code != 403 {
			t.Fatal("unexpected status")
		}
	}
	if accepted != 1 {
		t.Fatalf("concurrent deliveries=%d", accepted)
	}
}

type failingJITWriter struct {
	header http.Header
	writes int
	check  func()
}

func (w *failingJITWriter) Header() http.Header { return w.header }
func (*failingJITWriter) WriteHeader(int)       {}
func (w *failingJITWriter) Write(b []byte) (int, error) {
	w.check()
	w.writes++
	return len(b) / 2, errors.New("connection lost")
}

func TestJITFailureBeforeAndAfterStreamCommit(t *testing.T) {
	for _, mode := range []string{"write failure", "database failure", "canceled", "expired", "cleanup phase"} {
		t.Run(mode, func(t *testing.T) {
			s, _, lease, b, _ := jitFixture(t)
			token, err := b.Prepare(ctx, lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			r := jitRequest(t, lease.ID, token)
			switch mode {
			case "database failure":
				_, err = s.db.Exec(`CREATE TRIGGER reject_stream BEFORE UPDATE OF phase ON jit_transfers WHEN NEW.phase='stream-intent' BEGIN SELECT RAISE(FAIL,'fixture refusal'); END`)
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				r = r.WithContext(c)
			case "expired":
				b.secrets[lease.ID].expires = time.Now().Add(-time.Second)
			case "cleanup phase":
				err = s.Transition(ctx, lease.ID, Provisioning, CleanupPending)
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "write failure" {
				w := &failingJITWriter{header: http.Header{}, check: func() {
					rec, e := s.JIT(ctx, lease.ID)
					if e != nil || rec.Phase != "stream-intent" {
						t.Error("bytes preceded durable consumption")
					}
				}}
				b.ServeHTTP(w, r)
				if w.writes != 1 {
					t.Fatal("write failure control did not run")
				}
			} else {
				w := httptest.NewRecorder()
				b.ServeHTTP(w, r)
				if w.Code != 403 {
					t.Fatal("invalid handoff wrote payload")
				}
			}
			w := httptest.NewRecorder()
			b.ServeHTTP(w, jitRequest(t, lease.ID, token))
			if w.Code != 403 {
				t.Fatal("failed handoff was replayed")
			}
		})
	}
}

func TestJITMintFaultsAndRestart(t *testing.T) {
	for _, mode := range []string{"lost response", "late create", "invalid identity", "null response", "invalid payload", "failed intent", "failed identity save", "revoked", "inventory unknown"} {
		t.Run(mode, func(t *testing.T) {
			s, dir, lease, b, f := jitFixture(t)
			switch mode {
			case "lost response":
				f.lostCreate = true
			case "late create":
				f.lostCreate = true
				f.lateCreate = true
			case "invalid identity":
				f.generate = func() (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
					return &scaleset.RunnerScaleSetJitRunnerConfig{Runner: &scaleset.RunnerReference{ID: 77, Name: "foreign", RunnerScaleSetID: lease.Set.ScaleSetID}, EncodedJITConfig: testJIT}, nil
				}
			case "null response":
				f.generate = func() (*scaleset.RunnerScaleSetJitRunnerConfig, error) { return nil, nil }
			case "invalid payload":
				f.generate = func() (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
					return &scaleset.RunnerScaleSetJitRunnerConfig{Runner: &scaleset.RunnerReference{ID: 77, Name: lease.RunnerName, RunnerScaleSetID: lease.Set.ScaleSetID}, EncodedJITConfig: "bad"}, nil
				}
			case "failed intent":
				if _, err := s.db.Exec(`CREATE TRIGGER reject_jit BEFORE UPDATE OF phase ON jit_transfers WHEN NEW.phase='jit-intent' BEGIN SELECT RAISE(FAIL,'fixture refusal'); END`); err != nil {
					t.Fatal(err)
				}
			case "failed identity save":
				if _, err := s.db.Exec(`CREATE TRIGGER reject_identity BEFORE UPDATE OF runner_id ON jit_transfers BEGIN SELECT RAISE(FAIL,'fixture refusal'); END`); err != nil {
					t.Fatal(err)
				}
			case "revoked":
				if err := s.Discover(ctx, nil); err != nil {
					t.Fatal(err)
				}
			case "inventory unknown":
				if err := s.ReconcileInventory(ctx, false, nil); !errors.Is(err, ErrInventoryUnknown) {
					t.Fatal(err)
				}
			}
			_, err := b.Prepare(ctx, lease.ID)
			if err == nil || strings.Contains(err.Error(), "credential-sentinel") {
				t.Fatal("uncertain mint accepted or exposed credentials")
			}
			if (mode == "failed intent" || mode == "revoked" || mode == "inventory unknown") && f.creates != 0 {
				t.Fatal("POST ran before valid durable intent")
			}
			creates := f.creates
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir, s.config)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			f.s = reopened
			b = newJITBroker(reopened, func(context.Context, Lease) (jitAPI, error) { return f, nil })
			if mode != "revoked" && mode != "inventory unknown" {
				if err := reopened.ReconcileInventory(ctx, true, nil); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := b.Prepare(ctx, lease.ID); err == nil || f.creates != creates {
				t.Fatal("restart replayed mint")
			}
			active, err := reopened.ActiveLeases(ctx)
			if err != nil || len(active) != 1 {
				t.Fatal("restart lost reservation")
			}
		})
	}
}

func TestJITRestartForgetsReadyPayload(t *testing.T) {
	s, dir, lease, b, f := jitFixture(t)
	// Clear this disposable fixture's transfers so the restart below starts the
	// JIT lifecycle from scratch against a lease that is already acquired.
	if _, err := s.db.Exec(`DROP TABLE jit_transfers`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	f.s = reopened
	if err := reopened.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	b = newJITBroker(reopened, func(context.Context, Lease) (jitAPI, error) { return f, nil })
	token, err := b.Prepare(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	f.s = restarted
	b = newJITBroker(restarted, func(context.Context, Lease) (jitAPI, error) { return f, nil })
	w := httptest.NewRecorder()
	b.ServeHTTP(w, jitRequest(t, lease.ID, token))
	if w.Code != 403 {
		t.Fatal("restart redelivered payload")
	}
	if _, err := b.Prepare(ctx, lease.ID); err == nil || f.creates != 1 {
		t.Fatal("restart regenerated JIT after inventory reconciliation")
	}
	if err := restarted.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	if absent, err := b.CleanupRegistration(ctx, lease.ID, true); err != nil || !absent {
		t.Fatal("restart could not clean known registration")
	}
}

func TestJITCleanupRequiresPositiveIdentityAndPhysicalAbsence(t *testing.T) {
	for _, mode := range []string{"normal", "lost mint", "late mint", "lost delete gone", "lost delete present", "foreign", "worker present", "preexisting collision"} {
		t.Run(mode, func(t *testing.T) {
			s, _, lease, b, f := jitFixture(t)
			if mode == "lost mint" || mode == "late mint" {
				f.lostCreate = true
			}
			if mode == "late mint" {
				f.lateCreate = true
			}
			if mode == "preexisting collision" {
				f.runner = &scaleset.RunnerReference{ID: 55, Name: lease.RunnerName, RunnerScaleSetID: lease.Set.ScaleSetID}
			}
			token, _ := b.Prepare(ctx, lease.ID)
			if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
				t.Fatal(err)
			}
			if mode == "foreign" {
				f.runner = &scaleset.RunnerReference{ID: 88, Name: lease.RunnerName, RunnerScaleSetID: 999}
			}
			if strings.HasPrefix(mode, "lost delete") {
				f.lostRemove = true
				f.retainRemove = mode == "lost delete present"
			}
			absent, err := b.CleanupRegistration(ctx, lease.ID, mode != "worker present")
			want := mode == "normal" || mode == "lost mint" || mode == "lost delete gone"
			if absent != want || (err == nil) != want {
				t.Fatalf("cleanup verdict: absent=%v err=%v", absent, err)
			}
			if mode == "foreign" || mode == "worker present" || mode == "preexisting collision" || mode == "late mint" {
				if f.removes != 0 {
					t.Fatal("unsafe removal")
				}
			}
			w := httptest.NewRecorder()
			b.ServeHTTP(w, jitRequest(t, lease.ID, token))
			if w.Code != 403 {
				t.Fatal("cleanup left bootstrap usable")
			}
			if mode == "late mint" {
				f.runner = &scaleset.RunnerReference{ID: 77, Name: lease.RunnerName, RunnerScaleSetID: lease.Set.ScaleSetID}
				if absent, err := b.CleanupRegistration(ctx, lease.ID, true); err != nil || !absent || f.creates != 1 {
					t.Fatal("late positive identity did not reconcile original mint")
				}
			}
			if mode == "lost delete present" {
				_, _ = b.CleanupRegistration(ctx, lease.ID, true)
				if f.removes != 1 {
					t.Fatal("uncertain DELETE replayed")
				}
			}
			current, err := s.Lease(ctx, lease.ID)
			if err != nil || current.Phase != CleanupPending {
				t.Fatal("runner cleanup released capacity without acquisition/VM evidence")
			}
		})
	}
}

func TestJITNativeSDKGenerationAndRemoval(t *testing.T) {
	s, _, lease, _, _ := jitFixture(t)
	f := newSDKFixture(t, s, lease.Set)
	var runner *scaleset.RunnerReference
	f.override = func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case strings.HasSuffix(r.URL.Path, "/agents"):
			if r.URL.Query().Get("agentName") != lease.RunnerName {
				t.Error("native lookup scope mismatch")
			}
			value := []scaleset.RunnerReference{}
			if runner != nil {
				value = append(value, *runner)
			}
			json.NewEncoder(w).Encode(map[string]any{"count": len(value), "value": value})
			return true
		case strings.HasSuffix(r.URL.Path, "/generatejitconfig"):
			record, err := s.JIT(ctx, lease.ID)
			if err != nil || record.Phase != "jit-intent" {
				t.Error("native POST lacks intent")
			}
			var setting scaleset.RunnerScaleSetJitRunnerSetting
			if err := json.NewDecoder(r.Body).Decode(&setting); err != nil || setting.Name != lease.RunnerName || setting.WorkFolder != "_work" {
				t.Error("native JIT setting mismatch")
			}
			runner = &scaleset.RunnerReference{ID: 77, Name: lease.RunnerName, RunnerScaleSetID: lease.Set.ScaleSetID}
			json.NewEncoder(w).Encode(scaleset.RunnerScaleSetJitRunnerConfig{Runner: runner, EncodedJITConfig: testJIT})
			return true
		case strings.HasSuffix(r.URL.Path, "/agents/77") && r.Method == "DELETE":
			record, err := s.JIT(ctx, lease.ID)
			if err != nil || record.Removal != "remove-intent" {
				t.Error("native DELETE lacks intent")
			}
			runner = nil
			w.WriteHeader(204)
			return true
		}
		return false
	}
	b := newJITBroker(s, func(context.Context, Lease) (jitAPI, error) { return f.client, nil })
	if _, err := b.Prepare(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, lease.ID, Provisioning, CleanupPending); err != nil {
		t.Fatal(err)
	}
	if absent, err := b.CleanupRegistration(ctx, lease.ID, true); err != nil || !absent {
		t.Fatalf("native SDK cleanup failed: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts[fmt.Sprintf("POST /tenant/_apis/runtime/runnerscalesets/%d/generatejitconfig", lease.Set.ScaleSetID)] != 1 || f.counts["DELETE /tenant/_apis/distributedtask/pools/0/agents/77"] != 1 {
		t.Fatal("wrong native mutation count")
	}
}

// The bootstrap contract refuses an expired or revoked capability with the same
// credential-free response as an unknown one. Track B acceptance: the refusal
// must neither deliver JIT bytes nor repeat the capability back to the caller.
func TestJITRefusesExpiredAndRevokedCapability(t *testing.T) {
	for _, mode := range []string{"expired", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			s, _, lease, b, _ := jitFixture(t)
			token, err := b.Prepare(ctx, lease.ID)
			if err != nil {
				t.Fatal(err)
			}
			// Positive control: this capability is deliverable until it is not.
			ready, err := s.JIT(ctx, lease.ID)
			if err != nil || ready.Phase != "ready" {
				t.Fatalf("capability was not deliverable before the test: %+v %v", ready, err)
			}
			switch mode {
			case "expired":
				b.mu.Lock()
				b.secrets[lease.ID].expires = time.Now().Add(-time.Second)
				b.mu.Unlock()
			case "revoked":
				if err := changedOne(s.db.ExecContext(ctx, `UPDATE jit_transfers SET revoked=1,updated=? WHERE lease_id=?`, timestamp(), lease.ID)); err != nil {
					t.Fatal(err)
				}
			}
			w := httptest.NewRecorder()
			b.ServeHTTP(w, jitRequest(t, lease.ID, token))
			if w.Code != 403 {
				t.Fatalf("%s capability returned %d", mode, w.Code)
			}
			dump := w.Body.String() + fmt.Sprint(w.Result().Header)
			for _, secret := range []string{token.Encoded(), testJIT, "credential-sentinel"} {
				if strings.Contains(dump, secret) {
					t.Fatalf("%s refusal echoed secret material", mode)
				}
			}
			// The refusal must be indistinguishable from an unknown capability's.
			unknown, err := bootstrap.NewToken()
			if err != nil {
				t.Fatal(err)
			}
			other := httptest.NewRecorder()
			b.ServeHTTP(other, jitRequest(t, lease.ID, unknown))
			if other.Code != w.Code || other.Body.String() != w.Body.String() || fmt.Sprint(other.Result().Header) != fmt.Sprint(w.Result().Header) {
				t.Fatalf("%s refusal differs from an unknown capability's refusal", mode)
			}
			record, err := s.JIT(ctx, lease.ID)
			if err != nil || record.Phase == "stream-intent" || record.Phase == "stream-written" {
				t.Fatalf("%s capability was consumed as a delivery: %+v %v", mode, record, err)
			}
		})
	}
}
