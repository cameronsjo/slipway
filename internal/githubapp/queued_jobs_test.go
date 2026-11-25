package githubapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// queuedJobsServer is a fake GitHub for QueuedPoolJobs. Discovery reports
// `discovered`; the actions:read mint records its body; run and job listings
// are answered by the per-path handlers, and anything unrouted is an empty
// list.
type queuedJobsServer struct {
	t          *testing.T
	discovered []Repository

	mu      sync.Mutex
	mints   [][]byte
	paths   []string
	revokes []string          // Authorization header of each DELETE /installation/token
	runs    map[string]string // "owner/repo status" -> workflow_runs JSON body
	jobs    map[string]string // "owner/repo runID" -> jobs JSON body
	respond map[string]func(http.ResponseWriter) bool
	// mintResponse, when set, replaces the default mint response, which echoes
	// exactly the requested repository IDs and {"actions":"read"}.
	mintResponse func(requestedIDs []int64) string
	// revokeStatus is the revoke's HTTP status; zero means 204.
	revokeStatus int
}

// mintExpiresAt is the expires_at every fixture mint reports.
const mintExpiresAt = "2026-09-23T13:00:00Z"

func echoedGrant(ids []int64) string {
	repos := make([]string, len(ids))
	for i, id := range ids {
		repos[i] = fmt.Sprintf(`{"id":%d}`, id)
	}
	return `{"token":"actions-read-token","expires_at":"` + mintExpiresAt +
		`","permissions":{"actions":"read"},"repositories":[` + strings.Join(repos, ",") + `]}`
}

func (s *queuedJobsServer) handler(w http.ResponseWriter, r *http.Request) {
	s.t.Helper()
	switch {
	case r.URL.Path == "/installation/repositories":
		body, err := json.Marshal(map[string]any{"total_count": len(s.discovered), "repositories": s.discovered})
		if err != nil {
			s.t.Error(err)
		}
		w.Write(body)
		return
	case r.URL.Path == "/app/installations/42/access_tokens":
		var raw map[string]json.RawMessage
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			s.t.Error(err)
		}
		if err := json.Unmarshal(payload, &raw); err != nil {
			s.t.Error(err)
		}
		if _, scoped := raw["repository_ids"]; !scoped && strings.Contains(string(raw["permissions"]), "metadata") {
			fmt.Fprint(w, `{"token":"discovery-token"}`)
			return
		}
		var ids []int64
		if len(raw["repository_ids"]) > 0 {
			if err := json.Unmarshal(raw["repository_ids"], &ids); err != nil {
				s.t.Error(err)
			}
		}
		s.mu.Lock()
		s.mints = append(s.mints, payload)
		respond := s.mintResponse
		s.mu.Unlock()
		if respond == nil {
			respond = echoedGrant
		}
		fmt.Fprint(w, respond(ids))
		return
	case r.URL.Path == "/installation/token":
		if r.Method != http.MethodDelete {
			s.t.Errorf("unexpected %s on /installation/token", r.Method)
		}
		s.mu.Lock()
		s.revokes = append(s.revokes, r.Header.Get("Authorization"))
		status := s.revokeStatus
		s.mu.Unlock()
		if status == 0 {
			status = http.StatusNoContent
		}
		w.WriteHeader(status)
		return
	}
	if r.Header.Get("Authorization") != "Bearer actions-read-token" {
		s.t.Errorf("read used the wrong credential: %q on %s", r.Header.Get("Authorization"), r.URL.Path)
	}
	s.mu.Lock()
	s.paths = append(s.paths, r.URL.RequestURI())
	respond := s.respond[r.URL.Path]
	s.mu.Unlock()
	if respond != nil && respond(w) {
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/repos/"), "/")
	fullName := parts[0] + "/" + parts[1]
	switch {
	case len(parts) == 4 && parts[3] == "runs":
		if body, ok := s.runs[fullName+" "+r.URL.Query().Get("status")]; ok {
			fmt.Fprint(w, body)
			return
		}
		fmt.Fprint(w, `{"workflow_runs":[]}`)
	case len(parts) == 6 && parts[5] == "jobs":
		if body, ok := s.jobs[fullName+" "+parts[4]]; ok {
			fmt.Fprint(w, body)
			return
		}
		fmt.Fprint(w, `{"jobs":[]}`)
	default:
		s.t.Errorf("unexpected path %s", r.URL.Path)
		w.WriteHeader(404)
	}
}

func (s *queuedJobsServer) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.paths)
}

func (s *queuedJobsServer) mintBodies() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.mints)
}

func (s *queuedJobsServer) revoked() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.revokes)
}

// assertRevokedOnce requires exactly one revoke, made with the minted token.
func assertRevokedOnce(t *testing.T, server *queuedJobsServer) {
	t.Helper()
	if revokes := server.revoked(); len(revokes) != 1 || revokes[0] != "Bearer actions-read-token" {
		t.Fatalf("revokes = %q, want exactly one DELETE /installation/token with the minted token", revokes)
	}
}

func privateRepo(id int64, name string) Repository {
	return Repository{ID: id, FullName: "owner/" + name, Private: true}
}

// newQueuedJobsFixture returns an App that has completed one discovery of
// `discovered`, and an installation granting actions:read.
func newQueuedJobsFixture(t *testing.T, discovered []Repository) (*App, Installation, *queuedJobsServer) {
	t.Helper()
	server := &queuedJobsServer{t: t, discovered: discovered, runs: map[string]string{}, jobs: map[string]string{},
		respond: map[string]func(http.ResponseWriter) bool{}}
	app, installation := fixture(t, server.handler)
	installation.Selection = "all"
	installation.Permissions = map[string]string{"administration": "write", "actions": "read"}
	if len(discovered) > 0 {
		if _, err := app.Discover(context.Background(), installation); err != nil {
			t.Fatalf("discovery setup failed: %v", err)
		}
	}
	return app, installation, server
}

// assertActionsReadTokenScope pins the actions:read mint body to EXACTLY
// {"repository_ids":[want...],"permissions":{"actions":"read"}}: no other
// field, no other permission, repository IDs rather than names (a name can be
// reused by a different repository between discoveries), and never an omitted
// or empty repository list -- GitHub reads either as every repository the
// installation can reach, and this App is installed on all of them.
func assertActionsReadTokenScope(t *testing.T, body []byte, want []int64) {
	t.Helper()
	ids := make([]string, len(want))
	for i, id := range want {
		ids[i] = strconv.FormatInt(id, 10)
	}
	expected := `{"repository_ids":[` + strings.Join(ids, ",") + `],"permissions":{"actions":"read"}}`
	if len(want) == 0 || string(body) != expected {
		t.Fatalf("actions:read token request body\n got %s\nwant %s", body, expected)
	}
}

func TestQueuedPoolJobsMintsExactlyActionsReadForTheQueriedRepositories(t *testing.T) {
	a, b := privateRepo(1, "alpha"), privateRepo(2, "beta")
	app, installation, server := newQueuedJobsFixture(t, []Repository{a, b})
	read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{a, b}, "slipway-linux")
	mints := server.mintBodies()
	if len(mints) != 1 {
		t.Fatalf("minted %d actions:read tokens, want exactly 1", len(mints))
	}
	assertActionsReadTokenScope(t, mints[0], []int64{1, 2})
	if err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	if len(read.Counts) != 2 || read.Counts[1] != 0 || read.Counts[2] != 0 {
		t.Fatalf("counts = %v, want a zero for each queried repository", read.Counts)
	}
	assertRevokedOnce(t, server)
}

// The mint refuses an empty repository list itself, before any HTTP call:
// GitHub reads an empty list as every repository the installation reaches.
func TestActionsReadTokenRefusesAnEmptyRepositoryListWithoutCallingGitHub(t *testing.T) {
	var calls atomic.Int32
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Errorf("empty repository list reached GitHub: %s %s", r.Method, r.URL.Path)
		fmt.Fprint(w, echoedGrant(nil))
	})
	installation.Permissions = map[string]string{"actions": "read"}
	for _, repos := range [][]Repository{nil, {}} {
		token, err := app.actionsReadToken(context.Background(), installation, repos)
		if err == nil || token != "" {
			t.Fatalf("token=%q err=%v, want a refusal", token, err)
		}
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("made %d HTTP calls, want none", n)
	}
}

// One log line per mint records what the token covered, and never the token.
func TestActionsReadMintLogsWhatTheTokenCoveredButNotTheToken(t *testing.T) {
	a, b := privateRepo(1, "alpha"), privateRepo(2, "beta")
	app, installation, _ := newQueuedJobsFixture(t, []Repository{a, b})
	var logged strings.Builder
	app.log = slog.New(slog.NewTextHandler(&logged, nil))
	if _, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{a, b}, "slipway-linux"); err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	var mintLines []string
	for _, line := range strings.Split(strings.TrimSpace(logged.String()), "\n") {
		if strings.Contains(line, "Minted a scoped actions:read installation token") {
			mintLines = append(mintLines, line)
		}
	}
	if len(mintLines) != 1 {
		t.Fatalf("mint log lines = %d, want exactly 1; log:\n%s", len(mintLines), logged.String())
	}
	line := mintLines[0]
	for _, want := range []string{"installation=42", "permissions=actions:read", "repositories=owner/alpha,owner/beta",
		"repository_ids=\"[1 2]\"", "expires_at=" + mintExpiresAt} {
		if !strings.Contains(line, want) {
			t.Fatalf("mint log line lacks %q: %s", want, line)
		}
	}
	if strings.Contains(logged.String(), "actions-read-token") {
		t.Fatalf("the log carries the token value:\n%s", logged.String())
	}
}

// A mint response granting anything other than exactly what was requested is
// never used, and the token it carried is revoked.
func TestQueuedPoolJobsRejectsAndRevokesAWiderGrant(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response func([]int64) string
	}{
		{"an extra permission", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"read","contents":"write"},"repositories":[{"id":1},{"id":2}]}`
		}},
		{"metadata write beside actions read", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"read","metadata":"write"},"repositories":[{"id":1},{"id":2}]}`
		}},
		{"an extra permission with an empty level", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"read","contents":""},"repositories":[{"id":1},{"id":2}]}`
		}},
		{"implicit metadata read alone", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"metadata":"read"},"repositories":[{"id":1},{"id":2}]}`
		}},
		{"actions write", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"write"},"repositories":[{"id":1},{"id":2}]}`
		}},
		{"an extra repository", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"read"},"repositories":[{"id":1},{"id":2},{"id":3}]}`
		}},
		{"a different repository", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"read"},"repositories":[{"id":1},{"id":3}]}`
		}},
		{"no repository list (an unscoped token)", func([]int64) string {
			return `{"token":"actions-read-token","permissions":{"actions":"read"}}`
		}},
		{"no permission set", func([]int64) string {
			return `{"token":"actions-read-token","repositories":[{"id":1},{"id":2}]}`
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := privateRepo(1, "alpha"), privateRepo(2, "beta")
			app, installation, server := newQueuedJobsFixture(t, []Repository{a, b})
			server.mintResponse = tc.response
			read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{a, b}, "slipway-linux")
			if err == nil {
				t.Fatalf("a wider grant was accepted: %+v", read)
			}
			if paths := server.requested(); len(paths) != 0 {
				t.Fatalf("the rejected token was used: %q", paths)
			}
			assertRevokedOnce(t, server)
		})
	}
}

// GitHub adds metadata:read to any repository-scoped token, so a mint response
// echoing it beside actions:read is the grant that was asked for, and is used.
func TestQueuedPoolJobsAcceptsTheImplicitMetadataReadEcho(t *testing.T) {
	a, b := privateRepo(1, "alpha"), privateRepo(2, "beta")
	app, installation, server := newQueuedJobsFixture(t, []Repository{a, b})
	server.mintResponse = func([]int64) string {
		return `{"token":"actions-read-token","permissions":{"actions":"read","metadata":"read"},"repositories":[{"id":1},{"id":2}]}`
	}
	read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{a, b}, "slipway-linux")
	if err != nil {
		t.Fatalf("the implicit metadata:read echo was refused: %v", err)
	}
	if len(read.Counts) != 2 {
		t.Fatalf("counts = %v, want one per queried repository", read.Counts)
	}
	if len(server.requested()) == 0 {
		t.Fatal("the accepted token was never used")
	}
	assertRevokedOnce(t, server)
}

// The token is revoked after every read, including one that stopped early,
// and a failed revoke never fails the read.
func TestQueuedPoolJobsRevokesTheTokenAfterEveryRead(t *testing.T) {
	alpha, beta, gamma := privateRepo(1, "alpha"), privateRepo(2, "beta"), privateRepo(3, "gamma")
	all := []Repository{alpha, beta, gamma}
	t.Run("after a complete read", func(t *testing.T) {
		app, installation, server := newQueuedJobsFixture(t, all)
		if _, err := app.QueuedPoolJobs(context.Background(), installation, all, "slipway-linux"); err != nil {
			t.Fatalf("QueuedPoolJobs: %v", err)
		}
		assertRevokedOnce(t, server)
	})
	t.Run("after a rate-limit stop", func(t *testing.T) {
		app, installation, server := newQueuedJobsFixture(t, all)
		server.respond["/repos/owner/beta/actions/runs"] = func(w http.ResponseWriter) bool {
			w.WriteHeader(http.StatusTooManyRequests)
			return true
		}
		read, err := app.QueuedPoolJobs(context.Background(), installation, all, "slipway-linux")
		if err != nil || !read.RateLimited {
			t.Fatalf("err=%v rate_limited=%v, want a rate-limit stop", err, read.RateLimited)
		}
		assertRevokedOnce(t, server)
	})
	t.Run("after a partial read ended by cancellation", func(t *testing.T) {
		app, installation, server := newQueuedJobsFixture(t, all)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		server.respond["/repos/owner/beta/actions/runs"] = func(w http.ResponseWriter) bool {
			cancel()
			w.WriteHeader(http.StatusBadGateway)
			return true
		}
		read, err := app.QueuedPoolJobs(ctx, installation, all, "slipway-linux")
		if !errors.Is(err, context.Canceled) || read.Counts[1] != 0 || len(read.Counts) != 1 {
			t.Fatalf("err=%v counts=%v, want a cancelled read holding only alpha", err, read.Counts)
		}
		assertRevokedOnce(t, server)
	})
	t.Run("a failed revoke is logged and does not fail the read", func(t *testing.T) {
		app, installation, server := newQueuedJobsFixture(t, all)
		server.revokeStatus = http.StatusInternalServerError
		var logged strings.Builder
		app.log = slog.New(slog.NewTextHandler(&logged, nil))
		read, err := app.QueuedPoolJobs(context.Background(), installation, all, "slipway-linux")
		if err != nil || len(read.Counts) != 3 {
			t.Fatalf("err=%v counts=%v, want a complete read despite the failed revoke", err, read.Counts)
		}
		assertRevokedOnce(t, server)
		if !strings.Contains(logged.String(), "level=WARN") || !strings.Contains(logged.String(), "revoking the actions:read installation token") {
			t.Fatalf("the failed revoke was not logged:\n%s", logged.String())
		}
		if strings.Contains(logged.String(), "actions-read-token") {
			t.Fatalf("the log carries the token value:\n%s", logged.String())
		}
	})
}

// The mint re-applies ScaleSetClient's repository rule itself. A public,
// archived, never-discovered, or foreign repository handed in by a caller
// must be left out of the token request entirely, and visibility is judged
// from discovery, not from the caller's copy of the flags.
func TestQueuedPoolJobsNeverMintsForARepositoryTheChokePointWouldRefuse(t *testing.T) {
	alpha, beta := privateRepo(1, "alpha"), privateRepo(2, "beta")
	public := Repository{ID: 3, FullName: "owner/public"}
	archived := Repository{ID: 4, FullName: "owner/archived", Private: true, Archived: true}
	disabled := Repository{ID: 5, FullName: "owner/disabled", Private: true, Disabled: true}
	app, installation, server := newQueuedJobsFixture(t, []Repository{alpha, beta, public, archived, disabled})
	claimsPrivate := public
	claimsPrivate.Private = true // the caller's copy lies; discovery says public
	asked := []Repository{
		alpha, public, claimsPrivate, archived, disabled,
		{ID: 9, FullName: "owner/never-discovered"},
		{ID: 1, FullName: "another/alpha"},
		{ID: 1, FullName: "owner/renamed"},
		beta,
	}
	read, err := app.QueuedPoolJobs(context.Background(), installation, asked, "slipway-linux")
	if err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	mints := server.mintBodies()
	if len(mints) != 1 {
		t.Fatalf("minted %d actions:read tokens, want exactly 1", len(mints))
	}
	assertActionsReadTokenScope(t, mints[0], []int64{1, 2})
	if len(read.Refused) != 7 || read.DiscoveryPending {
		t.Fatalf("refused = %q pending=%v, want the seven ineligible repositories", read.Refused, read.DiscoveryPending)
	}
	for _, path := range server.requested() {
		if !strings.HasPrefix(path, "/repos/owner/alpha/") && !strings.HasPrefix(path, "/repos/owner/beta/") {
			t.Fatalf("read a refused repository: %s", path)
		}
	}
	if len(read.Counts) != 2 {
		t.Fatalf("counts = %v, want only the two accepted repositories", read.Counts)
	}
}

func TestQueuedPoolJobsMintsNothingWhenEveryRepositoryIsRefused(t *testing.T) {
	t.Run("before any discovery", func(t *testing.T) {
		app, installation, server := newQueuedJobsFixture(t, nil)
		read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{privateRepo(1, "alpha")}, "slipway-linux")
		if err != nil || !read.DiscoveryPending || len(read.Refused) != 1 || len(server.mintBodies()) != 0 || len(server.requested()) != 0 {
			t.Fatalf("read=%+v err=%v mints=%d", read, err, len(server.mintBodies()))
		}
	})
	t.Run("only ineligible repositories", func(t *testing.T) {
		public := Repository{ID: 3, FullName: "owner/public"}
		app, installation, server := newQueuedJobsFixture(t, []Repository{public})
		read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{public}, "slipway-linux")
		if err != nil || read.DiscoveryPending || len(read.Refused) != 1 || len(server.mintBodies()) != 0 || len(server.requested()) != 0 {
			t.Fatalf("read=%+v err=%v mints=%d", read, err, len(server.mintBodies()))
		}
	})
}

func TestQueuedPoolJobsRequiresTheActionsPermission(t *testing.T) {
	for _, tc := range []struct {
		permission string
		granted    bool
	}{{"", false}, {"none", false}, {"read", true}, {"write", true}} {
		t.Run(tc.permission, func(t *testing.T) {
			alpha := privateRepo(1, "alpha")
			app, installation, server := newQueuedJobsFixture(t, []Repository{alpha})
			installation.Permissions = map[string]string{"administration": "write"}
			if tc.permission != "" {
				installation.Permissions["actions"] = tc.permission
			}
			if installation.GrantsActionsRead() != tc.granted {
				t.Fatalf("GrantsActionsRead() = %v, want %v", !tc.granted, tc.granted)
			}
			_, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha}, "slipway-linux")
			if tc.granted {
				if err != nil {
					t.Fatalf("granted installation refused: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrActionsReadNotGranted) || len(server.mintBodies()) != 0 {
				t.Fatalf("err=%v mints=%d, want ErrActionsReadNotGranted and no mint", err, len(server.mintBodies()))
			}
		})
	}
}

// A queued pool job inside an in-progress run -- a dependent job after a
// hosted one, or a later matrix leg -- is demand exactly like one in a queued
// run. The run listing's status is the RUN's, so both listings are read.
func TestQueuedPoolJobsCountsQueuedJobsInsideInProgressRuns(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newQueuedJobsFixture(t, []Repository{alpha})
	server.runs["owner/alpha queued"] = `{"workflow_runs":[{"id":11}]}`
	server.runs["owner/alpha in_progress"] = `{"workflow_runs":[{"id":12},{"id":11}]}` // 11 moved between listings
	server.jobs["owner/alpha 11"] = `{"jobs":[{"status":"queued","labels":["slipway-linux"]},{"status":"queued","labels":["ubuntu-latest"]}]}`
	server.jobs["owner/alpha 12"] = `{"jobs":[
		{"status":"completed","labels":["ubuntu-latest"]},
		{"status":"queued","labels":["SLIPWAY-LINUX"]},
		{"status":"in_progress","labels":["slipway-linux"]}]}`
	read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha}, "slipway-linux")
	if err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	if read.Counts[1] != 2 {
		t.Fatalf("count = %d, want 2 (one in the queued run, one inside the in-progress run)", read.Counts[1])
	}
	var sawQueued, sawInProgress bool
	jobCalls := 0
	for _, path := range server.requested() {
		sawQueued = sawQueued || strings.Contains(path, "status=queued")
		sawInProgress = sawInProgress || strings.Contains(path, "status=in_progress")
		if strings.Contains(path, "/jobs") {
			jobCalls++
			if !strings.Contains(path, "per_page=100") {
				t.Fatalf("job listing without per_page=100: %s", path)
			}
		}
	}
	if !sawQueued || !sawInProgress || jobCalls != 2 {
		t.Fatalf("queued=%v in_progress=%v job_calls=%d, want both listings and one job call per distinct run", sawQueued, sawInProgress, jobCalls)
	}
}

// maxRunsExaminedPerRepo is one budget shared by both listings, and it holds
// even when GitHub returns more runs than per_page asked for.
func TestQueuedPoolJobsSharesOneRunBudgetAcrossBothListings(t *testing.T) {
	runs := func(from, n int) string {
		ids := make([]string, n)
		for i := range n {
			ids[i] = fmt.Sprintf(`{"id":%d}`, from+i)
		}
		return `{"workflow_runs":[` + strings.Join(ids, ",") + `]}`
	}
	alpha := privateRepo(1, "alpha")
	app, installation, server := newQueuedJobsFixture(t, []Repository{alpha})
	server.runs["owner/alpha queued"] = runs(100, 7)
	server.runs["owner/alpha in_progress"] = runs(200, 9) // ignores the per_page it was sent
	if _, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha}, "slipway-linux"); err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	jobCalls := 0
	for _, path := range server.requested() {
		if strings.Contains(path, "status=queued") && !strings.Contains(path, "per_page="+strconv.Itoa(maxRunsExaminedPerRepo)) {
			t.Fatalf("queued listing did not ask for the full budget: %s", path)
		}
		if strings.Contains(path, "status=in_progress") && !strings.Contains(path, "per_page=3") {
			t.Fatalf("in-progress listing did not ask for only the remaining budget: %s", path)
		}
		if strings.Contains(path, "/jobs") {
			jobCalls++
		}
	}
	if jobCalls != maxRunsExaminedPerRepo {
		t.Fatalf("examined %d runs, want the cap %d", jobCalls, maxRunsExaminedPerRepo)
	}

	// A repository whose queued listing alone fills the budget never pays for
	// the in-progress listing.
	app, installation, server = newQueuedJobsFixture(t, []Repository{alpha})
	server.runs["owner/alpha queued"] = runs(100, maxRunsExaminedPerRepo)
	if _, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha}, "slipway-linux"); err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	for _, path := range server.requested() {
		if strings.Contains(path, "status=in_progress") {
			t.Fatalf("listed in-progress runs with no budget left: %s", path)
		}
	}
}

// One repository's 5xx or 301 (a rename) must not cost every other
// repository its reading.
func TestQueuedPoolJobsIsolatesAFailingRepository(t *testing.T) {
	alpha, beta, gamma := privateRepo(1, "alpha"), privateRepo(2, "beta"), privateRepo(3, "gamma")
	app, installation, server := newQueuedJobsFixture(t, []Repository{alpha, beta, gamma})
	server.respond["/repos/owner/alpha/actions/runs"] = func(w http.ResponseWriter) bool {
		w.WriteHeader(http.StatusBadGateway)
		return true
	}
	server.respond["/repos/owner/beta/actions/runs"] = func(w http.ResponseWriter) bool {
		w.Header().Set("Location", "/repositories/2/actions/runs")
		w.WriteHeader(http.StatusMovedPermanently)
		return true
	}
	server.runs["owner/gamma queued"] = `{"workflow_runs":[{"id":31}]}`
	server.jobs["owner/gamma 31"] = `{"jobs":[{"status":"queued","labels":["slipway-linux"]}]}`
	read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha, beta, gamma}, "slipway-linux")
	if err != nil {
		t.Fatalf("one repository's failure failed the whole read: %v", err)
	}
	if read.Failed != 2 || !strings.HasPrefix(read.FailedFirst, "owner/alpha: ") || !strings.Contains(read.FailedFirst, "502") {
		t.Fatalf("failed=%d first=%q, want 2 failures led by owner/alpha's 502", read.Failed, read.FailedFirst)
	}
	if _, ok := read.Counts[1]; ok {
		t.Fatalf("a failed repository was given a count: %v", read.Counts)
	}
	if _, ok := read.Counts[2]; ok {
		t.Fatalf("a redirected repository was given a count: %v", read.Counts)
	}
	if read.Counts[3] != 1 || read.RateLimited {
		t.Fatalf("counts=%v rate_limited=%v, want gamma read as 1", read.Counts, read.RateLimited)
	}
}

func TestQueuedPoolJobsStopsEarlyOnRateLimiting(t *testing.T) {
	alpha, beta, gamma := privateRepo(1, "alpha"), privateRepo(2, "beta"), privateRepo(3, "gamma")
	reset := time.Now().Add(10 * time.Minute).Truncate(time.Second)
	for _, tc := range []struct {
		name      string
		respond   func(http.ResponseWriter) bool
		wantUntil func(time.Time) bool
		limited   bool
		counted   []int64
	}{
		{
			name: "403 with an exhausted primary budget",
			respond: func(w http.ResponseWriter) bool {
				w.Header().Set("X-RateLimit-Remaining", "0")
				w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
				w.WriteHeader(http.StatusForbidden)
				return true
			},
			wantUntil: func(until time.Time) bool { return until.Equal(reset) },
			limited:   true, counted: []int64{1},
		},
		{
			name: "429 with Retry-After",
			respond: func(w http.ResponseWriter) bool {
				w.Header().Set("Retry-After", "30")
				w.WriteHeader(http.StatusTooManyRequests)
				return true
			},
			wantUntil: func(until time.Time) bool {
				wait := time.Until(until)
				return wait > 20*time.Second && wait <= 30*time.Second
			},
			limited: true, counted: []int64{1},
		},
		{
			name: "429 with no header waits GitHub's documented minute",
			respond: func(w http.ResponseWriter) bool {
				w.WriteHeader(http.StatusTooManyRequests)
				return true
			},
			wantUntil: func(until time.Time) bool {
				wait := time.Until(until)
				return wait > 50*time.Second && wait <= time.Minute
			},
			limited: true, counted: []int64{1},
		},
		{
			name: "a reset hours away is bounded to one hour",
			respond: func(w http.ResponseWriter) bool {
				w.Header().Set("X-RateLimit-Remaining", "0")
				w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(48*time.Hour).Unix(), 10))
				w.WriteHeader(http.StatusForbidden)
				return true
			},
			wantUntil: func(until time.Time) bool { return time.Until(until) <= maxRateLimitWait },
			limited:   true, counted: []int64{1},
		},
		{
			name: "a success below the floor stops after recording that repository",
			respond: func(w http.ResponseWriter) bool {
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(rateLimitFloor-1))
				w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
				fmt.Fprint(w, `{"workflow_runs":[]}`)
				return true
			},
			wantUntil: func(until time.Time) bool { return until.Equal(reset) },
			limited:   true, counted: []int64{1, 2},
		},
		{
			name: "a 403 without rate-limit headers is a per-repository failure",
			respond: func(w http.ResponseWriter) bool {
				w.WriteHeader(http.StatusForbidden)
				return true
			},
			limited: false, counted: []int64{1, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, installation, server := newQueuedJobsFixture(t, []Repository{alpha, beta, gamma})
			server.respond["/repos/owner/beta/actions/runs"] = tc.respond
			read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha, beta, gamma}, "slipway-linux")
			if err != nil {
				t.Fatalf("QueuedPoolJobs: %v", err)
			}
			if read.RateLimited != tc.limited {
				t.Fatalf("rate_limited = %v, want %v", read.RateLimited, tc.limited)
			}
			if tc.limited && !tc.wantUntil(read.RetryAfter) {
				t.Fatalf("retry after %v (in %v) is not what the headers reported", read.RetryAfter, time.Until(read.RetryAfter))
			}
			var counted []int64
			for id := range read.Counts {
				counted = append(counted, id)
			}
			slices.Sort(counted)
			if !slices.Equal(counted, tc.counted) {
				t.Fatalf("counted repositories %v, want %v", counted, tc.counted)
			}
			if tc.limited {
				for _, path := range server.requested() {
					if strings.HasPrefix(path, "/repos/owner/gamma/") {
						t.Fatalf("kept reading after the rate limit: %s", path)
					}
				}
			}
		})
	}
}

// homelab#1216: only a job whose labels are exactly the pool label is pool
// demand. A job that also asks for another label can never run here, and
// counting it made the unseen-demand probe fire forever.
func TestQueuedPoolJobsCountsOnlyJobsThePoolLabelAloneServes(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newQueuedJobsFixture(t, []Repository{alpha})
	server.runs["owner/alpha queued"] = `{"workflow_runs":[{"id":11}]}`
	server.jobs["owner/alpha 11"] = `{"jobs":[
		{"status":"queued","labels":["slipway-linux"]},
		{"status":"queued","labels":["slipway-linux","gpu"]},
		{"status":"queued","labels":["self-hosted","slipway-linux"]},
		{"status":"queued","labels":[]}]}`
	read, err := app.QueuedPoolJobs(context.Background(), installation, []Repository{alpha}, "slipway-linux")
	if err != nil {
		t.Fatalf("QueuedPoolJobs: %v", err)
	}
	if read.Counts[1] != 1 {
		t.Fatalf("count = %d, want 1 (only the job asking for exactly slipway-linux)", read.Counts[1])
	}
}
