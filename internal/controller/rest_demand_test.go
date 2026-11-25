package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

func TestClassifyRESTDemand(t *testing.T) {
	now := time.Date(2026, 9, 23, 16, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name    string
		reading restDemandReading
		want    int
	}{
		{
			name:    "a fresh positive reading contributes one slot",
			reading: restDemandReading{count: 1, observedAt: now.Add(-time.Minute), freshUntil: now.Add(9 * time.Minute)},
			want:    1,
		},
		{
			name:    "a reading above one contributes its whole count",
			reading: restDemandReading{count: 7, observedAt: now.Add(-time.Minute), freshUntil: now.Add(9 * time.Minute)},
			want:    7,
		},
		{
			name:    "a zero count contributes nothing even while fresh",
			reading: restDemandReading{count: 0, observedAt: now.Add(-time.Minute), freshUntil: now.Add(9 * time.Minute)},
			want:    0,
		},
		{
			name:    "a negative count contributes nothing",
			reading: restDemandReading{count: -1, observedAt: now.Add(-time.Minute), freshUntil: now.Add(9 * time.Minute)},
			want:    0,
		},
		{
			name:    "a reading past its freshUntil instant contributes nothing",
			reading: restDemandReading{count: 3, observedAt: now.Add(-11 * time.Minute), freshUntil: now.Add(-time.Second)},
			want:    0,
		},
		{
			name:    "a reading exactly at its freshUntil instant is still trusted",
			reading: restDemandReading{count: 1, observedAt: now.Add(-10 * time.Minute), freshUntil: now},
			want:    1,
		},
		{
			name:    "a never-observed repository (the zero value) contributes nothing",
			reading: restDemandReading{},
			want:    0,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := classifyRESTDemand(testCase.reading, now); got != testCase.want {
				t.Fatalf("classifyRESTDemand(%+v, %v) = %d, want %d", testCase.reading, now, got, testCase.want)
			}
		})
	}
}

// TestRecordRESTDemandStoresZerosSoADrainedQueueClearsImmediately pins the
// reason RecordRESTDemand writes every repository in a successful read's
// result, not only the positive ones: a repository whose queue drained must
// stop claiming a slot the moment the next read confirms it, rather than
// waiting up to two intervals for the old positive reading to age out.
func TestRecordRESTDemandStoresZerosSoADrainedQueueClearsImmediately(t *testing.T) {
	s, _ := openTestStore(t, 2)
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{1: 1}, now, now.Add(time.Hour))
	s.mu.Lock()
	got := s.restDemandWant(1, now)
	s.mu.Unlock()
	if got != 1 {
		t.Fatalf("restDemandWant after a positive read = %d, want 1", got)
	}
	// The next read finds nothing queued. RecordRESTDemand must overwrite the
	// old positive reading with a fresh zero, not leave it to expire on its own.
	s.RecordRESTDemand(map[int64]int{1: 0}, now, now.Add(time.Hour))
	s.mu.Lock()
	got = s.restDemandWant(1, now)
	s.mu.Unlock()
	if got != 0 {
		t.Fatalf("restDemandWant after the queue drained = %d, want 0", got)
	}
}

// TestRESTDemandTrackerRefreshFailureKeepsThePreviousReading pins the
// fail-open direction the design requires: a failed REST read must never
// clear what the last successful one observed. The previous reading is left
// to age out on the freshness window Refresh already set, never zeroed early
// by the failure itself.
func TestRESTDemandTrackerRefreshFailureKeepsThePreviousReading(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	repoID := keys[0].RepositoryID
	tracker := NewRESTDemandTracker(s, failingRESTDemandSource{}, "linux", time.Minute)
	// Seed a positive reading directly, the way a prior successful Refresh would.
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{repoID: 1}, now, now.Add(time.Hour))
	tracker.Refresh(ctx)
	s.mu.Lock()
	got := s.restDemandWant(repoID, now)
	s.mu.Unlock()
	if got != 1 {
		t.Fatalf("a failed REST read cleared the previous reading: restDemandWant = %d, want 1", got)
	}
}

var errRESTDemandSourceFailed = errors.New("simulated REST demand source failure")

type failingRESTDemandSource struct{}

func (failingRESTDemandSource) QueuedPoolJobs(context.Context, []RepoRef, string) (RESTDemandResult, error) {
	return RESTDemandResult{}, errRESTDemandSourceFailed
}

// scriptedRESTDemandSource returns one scripted result per call and counts
// the calls, so a test can tell a skipped read from a performed one.
type scriptedRESTDemandSource struct {
	results []RESTDemandResult
	calls   int
}

func (s *scriptedRESTDemandSource) QueuedPoolJobs(context.Context, []RepoRef, string) (RESTDemandResult, error) {
	s.calls++
	if len(s.results) == 0 {
		return RESTDemandResult{Counts: map[int64]int{}}, nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

func captureTrackerLog(tracker *RESTDemandTracker) *bytes.Buffer {
	var buf bytes.Buffer
	tracker.log = slog.New(slog.NewTextHandler(&buf, nil))
	return &buf
}

func restDemandWantAt(s *Store, repoID int64, at time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restDemandWant(repoID, at)
}

// One repository's failure must not cost every other repository its
// reading: the failed one keeps what the last read saw, the rest are
// recorded, and the read's log line says how many failed.
func TestRESTDemandTrackerRecordsTheRestWhenOneRepositoryFails(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 3)
	failed, drained, queued := keys[0].RepositoryID, keys[1].RepositoryID, keys[2].RepositoryID
	now := time.Now().UTC()
	s.RecordRESTDemand(map[int64]int{failed: 1, drained: 1}, now, now.Add(time.Hour))
	source := &scriptedRESTDemandSource{results: []RESTDemandResult{{
		Counts: map[int64]int{drained: 0, queued: 2}, Failed: 1, FailedFirst: "owner/repo1: GitHub API HTTP 502",
	}}}
	tracker := NewRESTDemandTracker(s, source, "linux", time.Minute)
	logged := captureTrackerLog(tracker)
	tracker.Refresh(ctx)
	if got := restDemandWantAt(s, failed, now); got != 1 {
		t.Fatalf("the failed repository lost its previous reading: want 1, got %d", got)
	}
	if got := restDemandWantAt(s, drained, now); got != 0 {
		t.Fatalf("the drained repository kept its old reading: want 0, got %d", got)
	}
	if got := restDemandWantAt(s, queued, now); got != 2 {
		t.Fatalf("the healthy repository was not recorded: want its count 2, got %d", got)
	}
	if !strings.Contains(logged.String(), "failed=1") || !strings.Contains(logged.String(), "repos_read=2") {
		t.Fatalf("read log line does not carry failed= and repos_read=: %s", logged)
	}
}

// A rate-limited read stops the reader until the reported reset: no read is
// attempted before it, and one is attempted after it.
func TestRESTDemandTrackerBacksOffUntilTheReportedReset(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 2)
	clock := time.Now().UTC()
	reset := clock.Add(20 * time.Minute)
	source := &scriptedRESTDemandSource{results: []RESTDemandResult{{
		Counts: map[int64]int{keys[0].RepositoryID: 1}, RateLimited: true, RetryAfter: reset,
	}}}
	tracker := NewRESTDemandTracker(s, source, "linux", 5*time.Minute)
	logged := captureTrackerLog(tracker)
	tracker.now = func() time.Time { return clock }
	tracker.Refresh(ctx)
	if source.calls != 1 || restDemandWantAt(s, keys[0].RepositoryID, clock) != 1 {
		t.Fatalf("the rate-limited read's completed repository was not recorded: calls=%d", source.calls)
	}
	if !strings.Contains(logged.String(), "rate_limited_until=") {
		t.Fatalf("read log line does not say it was rate limited: %s", logged)
	}
	if delay := tracker.nextDelay(); delay != 20*time.Minute {
		t.Fatalf("next read scheduled in %v, want the 20m until the reset", delay)
	}
	clock = reset.Add(-time.Second)
	tracker.Refresh(ctx)
	if source.calls != 1 {
		t.Fatalf("read the API %d times before the reported reset, want 1", source.calls)
	}
	clock = reset
	tracker.Refresh(ctx)
	if source.calls != 2 {
		t.Fatalf("did not resume reading at the reported reset: calls=%d", source.calls)
	}
}

// Refusals are logged once per read, and a refusal caused only by the first
// discovery not having finished yet schedules a quick retry rather than a
// whole interval of blindness at startup.
func TestRESTDemandTrackerRetriesSoonWhileDiscoveryIsPending(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 2)
	source := &scriptedRESTDemandSource{results: []RESTDemandResult{
		{Counts: map[int64]int{}, Refused: []string{"owner/repo1", "owner/repo2"}, DiscoveryPending: true},
		{Counts: map[int64]int{2: 0}, Refused: []string{"owner/repo1"}},
	}}
	tracker := NewRESTDemandTracker(s, source, "linux", 5*time.Minute)
	logged := captureTrackerLog(tracker)
	tracker.Refresh(ctx)
	if delay := tracker.nextDelay(); delay != restDemandDiscoveryRetry {
		t.Fatalf("next read in %v while discovery is pending, want %v", delay, restDemandDiscoveryRetry)
	}
	tracker.Refresh(ctx)
	if delay := tracker.nextDelay(); delay != 5*time.Minute {
		t.Fatalf("next read in %v after discovery, want the interval", delay)
	}
	if strings.Count(logged.String(), "refused repositories") != 1 || !strings.Contains(logged.String(), "refused=owner/repo1") {
		t.Fatalf("want one refusal line naming owner/repo1: %s", logged)
	}
}

func testGitHubApp(t *testing.T) *githubapp.App {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	idFile, keyFile := filepath.Join(dir, "id"), filepath.Join(dir, "key")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(idFile, []byte("123"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	app, err := githubapp.New(idFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	return app
}

// REST demand is optional. An installation without actions:read disables
// the reader with one error naming the permission and the consequence, and
// never refuses to build the controller; one with it builds a live reader.
func TestGitHubRESTDemandTrackerChecksTheActionsPermissionUpFront(t *testing.T) {
	s, _ := openTestStore(t, 2)
	app := testGitHubApp(t)
	for _, tc := range []struct {
		name        string
		permissions map[string]string
		enabled     bool
	}{
		{"missing", map[string]string{"administration": "write"}, false},
		{"read", map[string]string{"administration": "write", "actions": "read"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installation := githubapp.Installation{ID: 42, Selection: "all", Permissions: tc.permissions}
			var logged bytes.Buffer
			tracker, err := NewGitHubRESTDemandTracker(s, app, installation, "linux", time.Minute, slog.New(slog.NewTextHandler(&logged, nil)))
			if err != nil || tracker == nil {
				t.Fatalf("tracker construction refused: %v", err)
			}
			const disabled = "REST demand disabled: the GitHub App installation lacks actions:read; queued jobs will be discovered only by session envelopes."
			if tc.enabled {
				if tracker.source == nil || strings.Contains(logged.String(), "REST demand disabled") {
					t.Fatalf("a granted installation was disabled: %s", logged.String())
				}
				return
			}
			if tracker.source != nil || strings.Count(logged.String(), disabled) != 1 || !strings.Contains(logged.String(), "level=ERROR") {
				t.Fatalf("want one error line and a disabled tracker, got source=%v log=%s", tracker.source, logged.String())
			}
			done := make(chan struct{})
			go func() {
				tracker.Run(context.Background())
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("a disabled tracker's Run did not return")
			}
		})
	}
}
