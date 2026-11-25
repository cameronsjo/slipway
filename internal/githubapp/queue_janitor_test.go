package githubapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	janitorReadToken  = "janitor-read-token"
	janitorWriteToken = "janitor-write-token"
)

// janitorServer is a fake GitHub for a janitor pass. Every map is keyed as
// the comment says; anything unrouted is a 404, which the janitor must read
// as KEEP.
type janitorServer struct {
	t          *testing.T
	discovered []Repository

	mu           sync.Mutex
	queued       map[string][]map[string]any // full name -> queued runs, newest first
	workflowRuns map[string][]map[string]any // "full name workflowID" -> runs
	fresh        map[string]map[string]any   // "full name runID" -> the run a fresh GET returns
	jobs         map[string]string           // "full name runID" -> jobs body
	// jobsLater, when it holds a key, is the jobs body served on every read
	// of that key after the first: jobs that changed between the evaluation
	// and the pre-cancel re-check. The value "404" serves a 404 instead.
	jobsLater map[string]string
	jobReads  map[string]int
	// referenceJobs is how many jobs the default reference run (the
	// workflow's newest successful run on the queried branch, ID
	// janitorReferenceRunID, created referenceMinutes after janitorEpoch)
	// lists. noReference serves an empty successful-run listing instead.
	referenceJobs    int
	referenceMinutes int
	noReference      bool
	// referenceBranch, when set, is the head_branch the reference run
	// reports instead of the branch queried: GitHub ignoring the filter.
	referenceBranch string
	pulls           map[string]string // "full name number" -> pull request body
	// pullsLater, when it holds a key, is the pull request body served on
	// every read of that key after the first: a PR that changed between the
	// evaluation and the pre-cancel re-check.
	pullsLater   map[string]string
	pullReads    map[string]int
	tags         map[string][]string // full name -> tag names
	tagsStatus   int                 // non-zero fails every tag listing with it
	cancelStatus int                 // zero means 202
	// mintResponse, when set, replaces the default mint response, which
	// echoes exactly the requested permissions and repository IDs.
	mintResponse func(permissions map[string]string, ids []int64) string
	// discoveryGrant, when set, is the permissions object the discovery mint
	// response carries instead of exactly metadata:read. discoveryReadStatus,
	// when non-zero, fails the discovery listing with it.
	discoveryGrant      string
	discoveryReadStatus int

	mints   []string
	revokes []string
	cancels []string
	paths   []string
	// discoveryReads counts GET /installation/repositories.
	discoveryReads int
}

// janitorDiscoveryToken is the token the fake mints for a discovery request.
const janitorDiscoveryToken = "discovery-token"

func newJanitorServer(t *testing.T, discovered ...Repository) *janitorServer {
	return &janitorServer{t: t, discovered: discovered, queued: map[string][]map[string]any{},
		workflowRuns: map[string][]map[string]any{}, fresh: map[string]map[string]any{}, jobs: map[string]string{},
		jobsLater: map[string]string{}, jobReads: map[string]int{}, referenceJobs: 1, referenceMinutes: 600,
		pulls: map[string]string{}, pullsLater: map[string]string{}, pullReads: map[string]int{}, tags: map[string][]string{}}
}

// queuedPoolJob is a jobs listing with one job that carries only the pool
// label, is queued and has no runner.
const queuedPoolJob = `{"total_count":1,"jobs":[{"labels":["slipway-linux"],"status":"queued","runner_name":null}]}`

// janitorReferenceRunID is the default reference run: the workflow's newest
// successful run, whose job count every candidate must match.
const janitorReferenceRunID = 900

// completedPoolJobs is a finished run's listing of n pool jobs.
func completedPoolJobs(n int) string {
	jobs := make([]map[string]any, n)
	for i := range jobs {
		jobs[i] = map[string]any{"labels": []string{"slipway-linux"}, "status": "completed", "runner_name": fmt.Sprintf("slipway-linux-%d", i)}
	}
	body, _ := json.Marshal(map[string]any{"total_count": n, "jobs": jobs})
	return string(body)
}

func echoedJanitorGrant(permissions map[string]string, ids []int64) string {
	token := janitorReadToken
	if permissions["actions"] == "write" {
		token = janitorWriteToken
	}
	repos := make([]map[string]int64, len(ids))
	for i, id := range ids {
		repos[i] = map[string]int64{"id": id}
	}
	granted := map[string]string{"metadata": "read"}
	for name, level := range permissions {
		granted[name] = level
	}
	body, _ := json.Marshal(map[string]any{"token": token, "expires_at": mintExpiresAt, "permissions": granted, "repositories": repos})
	return string(body)
}

func (s *janitorServer) handler(w http.ResponseWriter, r *http.Request) {
	t := s.t
	t.Helper()
	switch {
	case r.URL.Path == "/installation/repositories":
		s.mu.Lock()
		s.discoveryReads++
		status := s.discoveryReadStatus
		s.mu.Unlock()
		if got := r.Header.Get("Authorization"); got != "Bearer "+janitorDiscoveryToken {
			t.Errorf("discovery used %q, want the discovery token", got)
		}
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		body, err := json.Marshal(map[string]any{"total_count": len(s.discovered), "repositories": s.discovered})
		if err != nil {
			t.Error(err)
		}
		w.Write(body)
		return
	case r.URL.Path == "/app/installations/42/access_tokens":
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var request struct {
			RepositoryIDs []int64           `json:"repository_ids"`
			Permissions   map[string]string `json:"permissions"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Error(err)
		}
		if request.RepositoryIDs == nil && request.Permissions["metadata"] == "read" {
			s.mu.Lock()
			grant := s.discoveryGrant
			s.mu.Unlock()
			if grant == "" {
				grant = `{"metadata":"read"}`
			}
			fmt.Fprintf(w, `{"token":%q,"permissions":%s}`, janitorDiscoveryToken, grant)
			return
		}
		s.mu.Lock()
		s.mints = append(s.mints, string(payload))
		respond := s.mintResponse
		s.mu.Unlock()
		if respond == nil {
			respond = echoedJanitorGrant
		}
		fmt.Fprint(w, respond(request.Permissions, request.RepositoryIDs))
		return
	case r.URL.Path == "/installation/token":
		s.mu.Lock()
		s.revokes = append(s.revokes, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/repos/"), "/")
	if len(parts) < 3 {
		t.Errorf("unexpected path %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fullName := parts[0] + "/" + parts[1]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paths = append(s.paths, r.Method+" "+r.URL.RequestURI())
	if r.Method == http.MethodPost {
		if len(parts) != 6 || parts[5] != "cancel" {
			t.Errorf("unexpected POST %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+janitorWriteToken {
			t.Errorf("cancel used %q, want the write token", got)
		}
		s.cancels = append(s.cancels, fullName+" "+parts[4])
		status := s.cancelStatus
		if status == 0 {
			status = http.StatusAccepted
		}
		w.WriteHeader(status)
		fmt.Fprint(w, `{}`)
		return
	}
	if got := r.Header.Get("Authorization"); got != "Bearer "+janitorReadToken {
		t.Errorf("read %s used %q, want the read token", r.URL.Path, got)
	}
	writeRuns := func(runs []map[string]any) {
		body, err := json.Marshal(map[string]any{"total_count": len(runs), "workflow_runs": runs})
		if err != nil {
			t.Error(err)
		}
		w.Write(body)
	}
	switch {
	case len(parts) == 4 && parts[2] == "actions" && parts[3] == "runs":
		writeRuns(s.queued[fullName])
	case len(parts) == 6 && parts[3] == "workflows" && parts[5] == "runs" && r.URL.Query().Get("status") == "success":
		// The reference read: the workflow's newest successful run for the
		// event, one per page.
		if s.noReference {
			writeRuns(nil)
			return
		}
		var workflowID int64
		fmt.Sscanf(parts[4], "%d", &workflowID)
		reference := with(pushRun(janitorReferenceRunID, "ref", s.referenceMinutes), func(run *janitorRun) {
			run.WorkflowID, run.Event, run.Status, run.Conclusion = workflowID, r.URL.Query().Get("event"), "completed", "success"
			run.HeadBranch = r.URL.Query().Get("branch")
			if s.referenceBranch != "" {
				run.HeadBranch = s.referenceBranch
			}
		})
		writeRuns([]map[string]any{runObject(reference)})
	case len(parts) == 6 && parts[3] == "workflows" && parts[5] == "runs":
		writeRuns(s.workflowRuns[fullName+" "+parts[4]])
	case len(parts) == 5 && parts[3] == "runs":
		if run, ok := s.fresh[fullName+" "+parts[4]]; ok {
			body, _ := json.Marshal(run)
			w.Write(body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case len(parts) == 6 && parts[5] == "jobs":
		key := fullName + " " + parts[4]
		s.jobReads[key]++
		if later, ok := s.jobsLater[key]; ok && s.jobReads[key] > 1 {
			if later == "404" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, later)
			return
		}
		if body, ok := s.jobs[key]; ok {
			fmt.Fprint(w, body)
			return
		}
		if parts[4] == fmt.Sprint(janitorReferenceRunID) {
			fmt.Fprint(w, completedPoolJobs(s.referenceJobs))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case len(parts) == 4 && parts[2] == "pulls":
		key := fullName + " " + parts[3]
		s.pullReads[key]++
		if later, ok := s.pullsLater[key]; ok && s.pullReads[key] > 1 {
			fmt.Fprint(w, later)
			return
		}
		if body, ok := s.pulls[key]; ok {
			fmt.Fprint(w, body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case len(parts) == 3 && parts[2] == "tags":
		if s.tagsStatus != 0 {
			w.WriteHeader(s.tagsStatus)
			return
		}
		perPage, page := 0, 0
		fmt.Sscanf(r.URL.Query().Get("per_page"), "%d", &perPage)
		fmt.Sscanf(r.URL.Query().Get("page"), "%d", &page)
		if perPage <= 0 || page <= 0 {
			t.Errorf("tag listing without pagination: %s", r.URL.RequestURI())
		}
		names := s.tags[fullName]
		start, end := min((page-1)*perPage, len(names)), min(page*perPage, len(names))
		entries := make([]map[string]any, 0, end-start)
		for _, name := range names[start:end] {
			entries = append(entries, map[string]any{"name": name, "commit": map[string]any{"sha": "abc"}})
		}
		body, _ := json.Marshal(entries)
		w.Write(body)
	default:
		t.Errorf("unexpected path %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *janitorServer) snapshot() (mints, revokes, cancels, paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.mints), slices.Clone(s.revokes), slices.Clone(s.cancels), slices.Clone(s.paths)
}

// runObject renders a fixture run as GitHub's run object.
func runObject(run janitorRun) map[string]any {
	prs := make([]map[string]any, 0, len(run.PullRequests))
	for _, pr := range run.PullRequests {
		prs = append(prs, map[string]any{"number": pr.Number, "base": map[string]any{"repo": map[string]any{"id": pr.BaseRepositoryID}}})
	}
	object := map[string]any{"id": run.ID, "workflow_id": run.WorkflowID, "head_branch": run.HeadBranch, "head_sha": run.HeadSHA,
		"event": run.Event, "status": run.Status, "conclusion": nil, "run_attempt": run.RunAttempt,
		"created_at": run.CreatedAt.Format(time.RFC3339), "head_repository": map[string]any{"id": run.HeadRepositoryID},
		"pull_requests": prs, "html_url": "https://example.invalid/should-not-be-logged"}
	if run.Conclusion != "" {
		object["conclusion"] = run.Conclusion
	}
	return object
}

// newJanitorFixture returns an App for the janitor and an installation that
// grants what `enforce` needs. The server discovers `discovered`.
func newJanitorFixture(t *testing.T, enforce bool, discovered ...Repository) (*App, Installation, *janitorServer) {
	t.Helper()
	server := newJanitorServer(t, discovered...)
	app, installation := fixture(t, server.handler)
	installation.Selection = "all"
	installation.Permissions = map[string]string{"actions": "read", "pull_requests": "read"}
	if enforce {
		installation.Permissions["actions"] = "write"
	}
	return app, installation, server
}

// supersededPushFixture queues an older push run (100) and a newer push run
// (101) on main in owner/alpha, with pool-only jobs, tags that do not include
// main, and both still queued at the fresh GET. Run 100 is superseded by run
// 101.
func supersededPushFixture(server *janitorServer, repo Repository) {
	older, newer := pushRun(100, "old", 0), pushRun(101, "new", 5)
	older.HeadRepositoryID, newer.HeadRepositoryID = repo.ID, repo.ID
	server.queued[repo.FullName] = []map[string]any{runObject(newer), runObject(older)}
	server.fresh[repo.FullName+" 100"] = runObject(older)
	server.fresh[repo.FullName+" 101"] = runObject(newer)
	server.jobs[repo.FullName+" 100"] = queuedPoolJob
	server.jobs[repo.FullName+" 101"] = queuedPoolJob
	server.tags[repo.FullName] = []string{"v1.0.0", "Main", "main-v2"}
}

func assertRevoked(t *testing.T, revokes []string, want ...string) {
	t.Helper()
	got, expected := slices.Clone(revokes), slices.Clone(want)
	slices.Sort(got)
	slices.Sort(expected)
	if !slices.Equal(got, expected) {
		t.Fatalf("revoked %q, want exactly %q", revokes, want)
	}
}

func TestQueueJanitorDryRunReportsWithoutWriting(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, false, alpha)
	supersededPushFixture(server, alpha)
	now := janitorEpoch.Add(2 * time.Hour)
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", false, now)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	mints, revokes, cancels, _ := server.snapshot()
	if len(mints) != 1 || mints[0] != `{"repository_ids":[1],"permissions":{"actions":"read","pull_requests":"read"}}` {
		t.Fatalf("mints = %q, want exactly one read mint for repository 1", mints)
	}
	if len(cancels) != 0 {
		t.Fatalf("dry-run cancelled %q", cancels)
	}
	assertRevoked(t, revokes, janitorDiscoveryToken, janitorReadToken)
	if pass.WouldCancel != 1 || pass.Cancelled != 0 || pass.Examined != 2 || pass.SkippedErrors != 0 {
		t.Fatalf("pass = %+v, want examined=2 would_cancel=1", pass)
	}
	action := pass.Actions[0]
	if action.RunID != 100 || action.Rule != JanitorRuleSuperseded || action.Cancelled || action.Age != 2*time.Hour ||
		action.URL != "https://github.com/owner/alpha/actions/runs/100" || action.Repository != "owner/alpha" ||
		action.WorkflowID != 7 || action.Jobs != 1 || action.RefRunID != janitorReferenceRunID || action.RefJobs != 1 {
		t.Fatalf("action = %+v", action)
	}
}

func TestQueueJanitorEnforceMintsExactlyActionsWriteAndCancels(t *testing.T) {
	alpha, beta := privateRepo(1, "alpha"), privateRepo(2, "beta")
	app, installation, server := newJanitorFixture(t, true, alpha, beta)
	supersededPushFixture(server, alpha)
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha, beta}, "slipway-linux", true, janitorEpoch.Add(time.Hour))
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	mints, revokes, cancels, _ := server.snapshot()
	want := []string{
		`{"repository_ids":[1,2],"permissions":{"actions":"read","pull_requests":"read"}}`,
		// Scoped to the one repository with a confirmed candidate, not both.
		`{"repository_ids":[1],"permissions":{"actions":"write"}}`,
	}
	if !slices.Equal(mints, want) {
		t.Fatalf("mints\n got %q\nwant %q", mints, want)
	}
	if !slices.Equal(cancels, []string{"owner/alpha 100"}) {
		t.Fatalf("cancels = %q, want only run 100", cancels)
	}
	assertRevoked(t, revokes, janitorDiscoveryToken, janitorReadToken, janitorWriteToken)
	if pass.Cancelled != 1 || pass.WouldCancel != 0 || !pass.Actions[0].Cancelled {
		t.Fatalf("pass = %+v, want one cancel", pass)
	}
}

// Staged break: the status re-check. The listing says queued; the fresh GET
// immediately before the cancel says in_progress.
func TestQueueJanitorKeepsARunThatStartedBeforeTheCancel(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	supersededPushFixture(server, alpha)
	server.fresh["owner/alpha 100"]["status"] = "in_progress"
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	_, revokes, cancels, paths := server.snapshot()
	if len(cancels) != 0 || pass.Cancelled != 0 {
		t.Fatalf("cancelled a run that had started: cancels=%q pass=%+v", cancels, pass)
	}
	if !slices.Contains(paths, "GET /repos/owner/alpha/actions/runs/100") {
		t.Fatalf("no fresh GET of run 100 before deciding: %q", paths)
	}
	assertRevoked(t, revokes, janitorDiscoveryToken, janitorReadToken, janitorWriteToken)
}

// Staged break: the pre-cancel jobs re-read (review I1). Every job reads
// queued with no runner when the run is evaluated; immediately before the
// cancel a runner holds one, a job has started, a job was created since, or
// the re-read fails. The run still reads queued, and it is kept, in both
// modes, with a kept line saying why.
func TestQueueJanitorKeepsARunAJobStartedOnBeforeTheCancel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		later   string
		reason  string
		skipped int
	}{
		{"a runner took the queued job", `{"total_count":1,"jobs":[{"labels":["slipway-linux"],"status":"queued","runner_name":"slipway-linux-3"}]}`,
			"a job has a runner", 0},
		{"the job is in progress", `{"total_count":1,"jobs":[{"labels":["slipway-linux"],"status":"in_progress","runner_name":"slipway-linux-3"}]}`,
			"a job has a runner", 0},
		{"the job is waiting on an environment", `{"total_count":1,"jobs":[{"labels":["slipway-linux"],"status":"waiting","runner_name":null}]}`,
			"a job is not queued", 0},
		{"a job was created since the evaluation", `{"total_count":2,"jobs":[{"labels":["slipway-linux"],"status":"queued","runner_name":null},{"labels":["slipway-linux"],"status":"queued","runner_name":null}]}`,
			janitorKeptJobCountDiffers, 0},
		{"the re-read fails", "404", "jobs re-read failed", 1},
	} {
		for _, enforce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s enforce=%t", tc.name, enforce), func(t *testing.T) {
				alpha := privateRepo(1, "alpha")
				app, installation, server := newJanitorFixture(t, enforce, alpha)
				supersededPushFixture(server, alpha)
				server.jobsLater["owner/alpha 100"] = tc.later
				pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", enforce, janitorEpoch)
				if err != nil {
					t.Fatalf("queueJanitorPass: %v", err)
				}
				_, _, cancels, _ := server.snapshot()
				if len(cancels) != 0 || pass.Cancelled != 0 || pass.WouldCancel != 0 || len(pass.Actions) != 0 {
					t.Fatalf("cancels=%q pass=%+v, want the run kept", cancels, pass)
				}
				want := []JanitorKept{{Repository: "owner/alpha", RunID: 100, Rule: JanitorRuleSuperseded, Reason: tc.reason}}
				if !slices.Equal(pass.Kept, want) || pass.SkippedErrors != tc.skipped {
					t.Fatalf("kept=%+v skipped=%d, want %+v and %d skipped", pass.Kept, pass.SkippedErrors, want, tc.skipped)
				}
				server.mu.Lock()
				reads := server.jobReads["owner/alpha 100"]
				server.mu.Unlock()
				if reads != 2 {
					t.Fatalf("run 100's jobs read %d times, want 2: the evaluation and the re-check", reads)
				}
			})
		}
	}
}

// Staged break: the job-count rule (review I2). The run's one listed job is
// a pool job, but the workflow's newest successful run on the branch had two
// jobs, predates the run, had no successful run at all, or has a job listing
// that cannot be read whole: a job GitHub has not created yet cannot be ruled
// out, so the run is kept at evaluation and counted.
func TestQueueJanitorKeepsARunWhoseWorkflowMayHaveUncreatedJobs(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*janitorServer)
	}{
		{"the workflow had two jobs", func(s *janitorServer) { s.referenceJobs = 2 }},
		// The re-review's scenario: a push added a needs: job, and the
		// newest success predates that push, so its count is the old
		// file's. Only a success created after the run can vouch for it.
		{"the newest success predates the run", func(s *janitorServer) { s.referenceMinutes = -60 }},
		{"the newest success was created with the run", func(s *janitorServer) { s.referenceMinutes = 0 }},
		{"the newest success is on another branch", func(s *janitorServer) { s.referenceBranch = "other" }},
		{"the workflow has no successful run", func(s *janitorServer) { s.noReference = true }},
		{"the reference job listing is partial", func(s *janitorServer) {
			s.jobs[fmt.Sprintf("owner/alpha %d", janitorReferenceRunID)] = `{"total_count":2,"jobs":[{"labels":["slipway-linux"],"status":"completed","runner_name":"r"}]}`
		}},
		{"the reference job listing is empty", func(s *janitorServer) { s.referenceJobs = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alpha := privateRepo(1, "alpha")
			app, installation, server := newJanitorFixture(t, true, alpha)
			supersededPushFixture(server, alpha)
			tc.set(server)
			pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
			if err != nil {
				t.Fatalf("queueJanitorPass: %v", err)
			}
			mints, _, cancels, paths := server.snapshot()
			if len(cancels) != 0 || len(mints) != 1 || len(pass.Actions) != 0 || pass.KeptUnprovenJobs != 1 || pass.SkippedErrors != 0 {
				t.Fatalf("cancels=%q mints=%d pass=%+v, want no cancel, no write mint and one run kept unproven", cancels, len(mints), pass)
			}
			if !slices.Contains(paths, "GET /repos/owner/alpha/actions/workflows/7/runs?branch=main&event=push&per_page=1&status=success") {
				t.Fatalf("the workflow's newest successful run was not read: %q", paths)
			}
		})
	}
}

// A reference read that fails keeps the run and counts the failure.
func TestQueueJanitorKeepsARunWhoseReferenceCannotBeRead(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	supersededPushFixture(server, alpha)
	server.jobs[fmt.Sprintf("owner/alpha %d", janitorReferenceRunID)] = `not json`
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	if _, _, cancels, _ := server.snapshot(); len(cancels) != 0 || pass.SkippedErrors != 1 || len(pass.Actions) != 0 {
		t.Fatalf("cancels=%q pass=%+v, want no cancel and one skipped error", cancels, pass)
	}
}

// A fresh GET that fails keeps the run and counts the failure.
func TestQueueJanitorKeepsARunWhoseFreshReadFails(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	supersededPushFixture(server, alpha)
	delete(server.fresh, "owner/alpha 100")
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	if _, _, cancels, _ := server.snapshot(); len(cancels) != 0 || pass.SkippedErrors != 1 {
		t.Fatalf("cancels=%q skipped=%d, want no cancel and one skipped error", cancels, pass.SkippedErrors)
	}
}

// Staged break: the visibility re-check. The caller hands over a repository
// it believes private; the janitor's own discovery says it is public. It
// must never reach a mint or a read.
func TestQueueJanitorRefusesARepositoryItsDiscoveryRecordsPublic(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	public := Repository{ID: 2, FullName: "owner/public", Private: false}
	app, installation, server := newJanitorFixture(t, true, alpha, public)
	supersededPushFixture(server, alpha)
	supersededPushFixture(server, public)
	callerCopy := public
	callerCopy.Private = true
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha, callerCopy}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	mints, _, cancels, paths := server.snapshot()
	for _, mint := range mints {
		if strings.Contains(mint, `"repository_ids":[1,2]`) || strings.Contains(mint, `[2]`) || strings.Contains(mint, `,2]`) {
			t.Fatalf("a mint covered the public repository: %s", mint)
		}
	}
	for _, path := range append(paths, cancels...) {
		if strings.Contains(path, "owner/public") {
			t.Fatalf("the public repository was read or cancelled: %s", path)
		}
	}
	if !slices.Equal(pass.Refused, []string{"owner/public"}) || !slices.Equal(cancels, []string{"owner/alpha 100"}) {
		t.Fatalf("refused=%q cancels=%q, want owner/public refused and only alpha's run cancelled", pass.Refused, cancels)
	}
}

// The write mint re-applies the mint rule itself, from the discovery record,
// so it refuses a repository the record does not hold without calling GitHub.
func TestJanitorWriteTokenReappliesTheMintRule(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	if _, err := app.janitorDiscover(context.Background(), installation); err != nil {
		t.Fatalf("discovery: %v", err)
	}
	for _, repo := range []Repository{privateRepo(3, "undiscovered"), {ID: 1, FullName: "owner/renamed", Private: true}, {ID: 4, FullName: "other/alpha", Private: true}} {
		if token, err := app.janitorWriteToken(context.Background(), installation, []Repository{alpha, repo}); err == nil || token != "" {
			t.Fatalf("write token for %s: token=%q err=%v, want a refusal", repo.FullName, token, err)
		}
	}
	if mints, _, _, _ := server.snapshot(); len(mints) != 0 {
		t.Fatalf("a refused write token still reached GitHub: %q", mints)
	}
	if token, err := app.janitorWriteToken(context.Background(), installation, []Repository{alpha}); err != nil || token != janitorWriteToken {
		t.Fatalf("write token for a discovered private repository: token=%q err=%v", token, err)
	}
}

// A mint that grants anything other than exactly what was asked is revoked
// unused and fails the pass before any read.
func TestQueueJanitorRevokesAnOverGrantedToken(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	for _, tc := range []struct {
		name    string
		respond func(map[string]string, []int64) string
	}{
		{"an extra permission", func(p map[string]string, ids []int64) string {
			p = map[string]string{"actions": "read", "pull_requests": "read", "contents": "read"}
			return echoedJanitorGrant(p, ids)
		}},
		{"write instead of read", func(p map[string]string, ids []int64) string {
			body := echoedJanitorGrant(map[string]string{"actions": "write", "pull_requests": "read"}, ids)
			return strings.Replace(body, janitorWriteToken, janitorReadToken, 1)
		}},
		{"a missing permission", func(p map[string]string, ids []int64) string {
			return echoedJanitorGrant(map[string]string{"actions": "read"}, ids)
		}},
		{"an extra repository", func(p map[string]string, ids []int64) string {
			return echoedJanitorGrant(p, append(ids, 99))
		}},
		{"no repository list", func(p map[string]string, ids []int64) string {
			return echoedJanitorGrant(p, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, installation, server := newJanitorFixture(t, false, alpha)
			supersededPushFixture(server, alpha)
			server.mintResponse = tc.respond
			if _, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", false, janitorEpoch); err == nil {
				t.Fatal("an over-granted token was accepted")
			}
			_, revokes, _, paths := server.snapshot()
			assertRevoked(t, revokes, janitorDiscoveryToken, janitorReadToken)
			if len(paths) != 0 {
				t.Fatalf("read with a refused token: %q", paths)
			}
		})
	}
}

// The permissions each mode needs are checked before anything is called.
func TestQueueJanitorRefusesAnInstallationMissingAPermission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		permissions map[string]string
		enforce     bool
		missing     string
	}{
		{"dry-run without pull requests", map[string]string{"actions": "read"}, false, "pull_requests:read"},
		{"dry-run without actions", map[string]string{"pull_requests": "read"}, false, "actions:read"},
		{"enforce with read only", map[string]string{"actions": "read", "pull_requests": "read"}, true, "actions:write"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alpha := privateRepo(1, "alpha")
			app, installation, server := newJanitorFixture(t, false, alpha)
			installation.Permissions = tc.permissions
			_, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", tc.enforce, janitorEpoch)
			if !errors.Is(err, ErrJanitorPermissionsMissing) || !strings.Contains(err.Error(), tc.missing) {
				t.Fatalf("err = %v, want ErrJanitorPermissionsMissing naming %s", err, tc.missing)
			}
			if mints, _, _, paths := server.snapshot(); len(mints)+len(paths) != 0 {
				t.Fatalf("a refused installation still reached GitHub: mints=%q paths=%q", mints, paths)
			}
		})
	}
	if missing := (Installation{Permissions: map[string]string{"actions": "write", "pull_requests": "write"}}).JanitorMissingPermissions(true); len(missing) != 0 {
		t.Fatalf("write grants did not satisfy the reads: %q", missing)
	}
}

// tagPages returns n distinct tag names, none of them "main".
func tagPages(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("v0.0.%d", i)
	}
	return names
}

// Staged break: the tag-collision keep. Plan case: a tag push, end to end.
// head_branch is the tag name, a tag of that name is listed, and the run is
// kept. The tag sits on the LAST page, so a reader that stops after page 1
// cancels it.
func TestQueueJanitorKeepsATagPush(t *testing.T) {
	for _, tc := range []struct {
		name string
		tags []string
	}{
		{"on the first page", []string{"v1.2.3"}},
		{"on a later page", append(tagPages(2*janitorTagsPerPage), "v1.2.3")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alpha := privateRepo(1, "alpha")
			app, installation, server := newJanitorFixture(t, true, alpha)
			older, newer := pushRun(100, "old", 0), pushRun(101, "new", 5)
			older.HeadRepositoryID, newer.HeadRepositoryID = alpha.ID, alpha.ID
			older.HeadBranch, newer.HeadBranch = "v1.2.3", "v1.2.3"
			server.queued["owner/alpha"] = []map[string]any{runObject(newer), runObject(older)}
			server.fresh["owner/alpha 100"] = runObject(older)
			server.jobs["owner/alpha 100"] = queuedPoolJob
			server.jobs["owner/alpha 101"] = queuedPoolJob
			server.tags["owner/alpha"] = tc.tags
			pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
			if err != nil {
				t.Fatalf("queueJanitorPass: %v", err)
			}
			_, _, cancels, paths := server.snapshot()
			if len(cancels) != 0 || pass.Cancelled != 0 {
				t.Fatalf("cancelled a tag push: %q", cancels)
			}
			if !slices.Contains(paths, "GET /repos/owner/alpha/tags?page=1&per_page=100") {
				t.Fatalf("tags not read: %q", paths)
			}
			for _, path := range paths {
				if strings.Contains(path, "/branches") {
					t.Fatalf("the branches endpoint, which needs contents:read, was called: %s", path)
				}
			}
		})
	}
}

// A push run is kept whenever the tag listing cannot be read completely: an
// error, or a listing still full at the page cap.
func TestQueueJanitorKeepsAPushWhenTheTagsCannotBeReadCompletely(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		tags   []string
	}{
		{"a 403", http.StatusForbidden, nil},
		{"a 500", http.StatusInternalServerError, nil},
		{"more tags than the page cap reads", 0, tagPages(janitorTagPageCap * janitorTagsPerPage)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alpha := privateRepo(1, "alpha")
			app, installation, server := newJanitorFixture(t, true, alpha)
			supersededPushFixture(server, alpha)
			server.tagsStatus = tc.status
			if tc.tags != nil {
				server.tags["owner/alpha"] = tc.tags
			}
			pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
			if err != nil {
				t.Fatalf("queueJanitorPass: %v", err)
			}
			mints, _, cancels, paths := server.snapshot()
			if len(cancels) != 0 || pass.Cancelled != 0 || len(mints) != 1 || pass.SkippedErrors == 0 {
				t.Fatalf("cancels=%q mints=%d skipped=%d, want no cancel, no write mint and a skipped error", cancels, len(mints), pass.SkippedErrors)
			}
			if tc.tags != nil {
				if last := fmt.Sprintf("GET /repos/owner/alpha/tags?page=%d&per_page=100", janitorTagPageCap); !slices.Contains(paths, last) {
					t.Fatalf("the cap page was not read: %q", paths)
				}
				if slices.Contains(paths, fmt.Sprintf("GET /repos/owner/alpha/tags?page=%d&per_page=100", janitorTagPageCap+1)) {
					t.Fatalf("read past the page cap: %q", paths)
				}
			}
		})
	}
}

// The tag listing is read once per repository per pass, however many push
// runs it has.
func TestQueueJanitorReadsTagsOncePerRepository(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, false, alpha)
	older, middle, newer := pushRun(100, "a", 0), pushRun(101, "b", 5), pushRun(102, "c", 10)
	for _, run := range []*janitorRun{&older, &middle, &newer} {
		run.HeadRepositoryID = alpha.ID
		server.fresh[fmt.Sprintf("owner/alpha %d", run.ID)] = runObject(*run)
		server.jobs[fmt.Sprintf("owner/alpha %d", run.ID)] = queuedPoolJob
	}
	server.queued["owner/alpha"] = []map[string]any{runObject(newer), runObject(middle), runObject(older)}
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", false, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	_, _, _, paths := server.snapshot()
	reads, referenceReads := 0, 0
	for _, path := range paths {
		if strings.Contains(path, "/tags?") {
			reads++
		}
		if strings.Contains(path, "status=success") {
			referenceReads++
		}
	}
	if reads != 1 || referenceReads != 1 || pass.WouldCancel != 2 {
		t.Fatalf("tag reads=%d reference reads=%d would_cancel=%d, want one of each read and two would-cancels", reads, referenceReads, pass.WouldCancel)
	}
}

// R2 end to end: a PR run from the repository itself, the PR closed after
// the run was created. The PR read uses the read token, and the run is
// cancelled under closed-pr.
func TestQueueJanitorCancelsARunForAPRClosedAfterIt(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	run := prRun(200, "sha", 0, 9)
	server.queued["owner/alpha"] = []map[string]any{runObject(run)}
	server.fresh["owner/alpha 200"] = runObject(run)
	server.jobs["owner/alpha 200"] = queuedPoolJob
	server.pulls["owner/alpha 9"] = `{"number":9,"state":"closed","closed_at":"2026-09-24T00:30:00Z","base":{"repo":{"id":1}}}`
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch.Add(time.Hour))
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	if _, _, cancels, _ := server.snapshot(); !slices.Equal(cancels, []string{"owner/alpha 200"}) || pass.Actions[0].Rule != JanitorRuleClosedPR {
		t.Fatalf("cancels=%q actions=%+v, want run 200 under closed-pr", cancels, pass.Actions)
	}
}

// Staged break: the R2 pull request re-check. The PR reads closed when the
// run is evaluated and open again at the pre-cancel re-check: reopened in
// between. The run is kept, in both modes.
func TestQueueJanitorKeepsARunWhosePRReopenedBeforeTheCancel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		later string
	}{
		{"reopened", `{"number":9,"state":"open","closed_at":null,"base":{"repo":{"id":1}}}`},
		{"closed again, before the run", `{"number":9,"state":"closed","closed_at":"2026-09-23T23:00:00Z","base":{"repo":{"id":1}}}`},
		{"re-based onto another repository", `{"number":9,"state":"closed","closed_at":"2026-09-24T00:30:00Z","base":{"repo":{"id":55}}}`},
	} {
		for _, enforce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s enforce=%t", tc.name, enforce), func(t *testing.T) {
				alpha := privateRepo(1, "alpha")
				app, installation, server := newJanitorFixture(t, enforce, alpha)
				run := prRun(200, "sha", 0, 9)
				server.queued["owner/alpha"] = []map[string]any{runObject(run)}
				server.fresh["owner/alpha 200"] = runObject(run)
				server.jobs["owner/alpha 200"] = queuedPoolJob
				server.pulls["owner/alpha 9"] = `{"number":9,"state":"closed","closed_at":"2026-09-24T00:30:00Z","base":{"repo":{"id":1}}}`
				server.pullsLater["owner/alpha 9"] = tc.later
				pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", enforce, janitorEpoch.Add(time.Hour))
				if err != nil {
					t.Fatalf("queueJanitorPass: %v", err)
				}
				_, _, cancels, _ := server.snapshot()
				if len(cancels) != 0 || pass.Cancelled != 0 || pass.WouldCancel != 0 || len(pass.Actions) != 0 {
					t.Fatalf("cancels=%q pass=%+v, want the run kept", cancels, pass)
				}
				server.mu.Lock()
				reads := server.pullReads["owner/alpha 9"]
				server.mu.Unlock()
				if reads != 2 {
					t.Fatalf("PR 9 read %d times, want 2: the evaluation and the re-check", reads)
				}
			})
		}
	}
}

// A failed PR re-check keeps the run and counts the failure.
func TestQueueJanitorKeepsARunWhosePRReCheckFails(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	run := prRun(200, "sha", 0, 9)
	server.queued["owner/alpha"] = []map[string]any{runObject(run)}
	server.fresh["owner/alpha 200"] = runObject(run)
	server.jobs["owner/alpha 200"] = queuedPoolJob
	server.pulls["owner/alpha 9"] = `{"number":9,"state":"closed","closed_at":"2026-09-24T00:30:00Z","base":{"repo":{"id":1}}}`
	server.pullsLater["owner/alpha 9"] = `not json`
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch.Add(time.Hour))
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	if _, _, cancels, _ := server.snapshot(); len(cancels) != 0 || pass.SkippedErrors != 1 {
		t.Fatalf("cancels=%q skipped=%d, want no cancel and one skipped error", cancels, pass.SkippedErrors)
	}
}

// Staged breaks: the discovery-token revoke and the discovery grant check.
// The janitor's discovery token is revoked on every path, and a grant other
// than exactly metadata:read is refused before any repository is listed.
func TestQueueJanitorDiscoveryTokenIsVerifiedAndRevoked(t *testing.T) {
	for _, tc := range []struct {
		name          string
		grant         string
		readStatus    int
		repos         func(Repository) []Repository
		wantErr       bool
		wantDiscovery int // GET /installation/repositories calls
	}{
		{"a completed pass", "", 0, nil, false, 1},
		{"a pass with no accepted repository", "", 0, func(Repository) []Repository {
			return []Repository{{ID: 9, FullName: "owner/undiscovered", Private: true}}
		}, false, 1},
		{"a failed discovery listing", "", http.StatusInternalServerError, nil, true, 1},
		{"a grant wider than metadata:read", `{"metadata":"read","actions":"write"}`, 0, nil, true, 0},
		{"a grant of metadata:write", `{"metadata":"write"}`, 0, nil, true, 0},
		{"a grant with no metadata", `{"actions":"read"}`, 0, nil, true, 0},
		{"no permissions echoed", `null`, 0, nil, true, 0},
		{"an empty permissions object", `{}`, 0, nil, true, 0},
		{"permissions that are not an object", `"read"`, 0, nil, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alpha := privateRepo(1, "alpha")
			app, installation, server := newJanitorFixture(t, false, alpha)
			supersededPushFixture(server, alpha)
			server.discoveryGrant = tc.grant
			server.discoveryReadStatus = tc.readStatus
			repos := []Repository{alpha}
			if tc.repos != nil {
				repos = tc.repos(alpha)
			}
			_, err := app.queueJanitorPass(context.Background(), installation, repos, "slipway-linux", false, janitorEpoch)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %t", err, tc.wantErr)
			}
			mints, revokes, _, paths := server.snapshot()
			if n := countOf(revokes, janitorDiscoveryToken); n != 1 {
				t.Fatalf("discovery token revoked %d times, want once: %q", n, revokes)
			}
			server.mu.Lock()
			reads := server.discoveryReads
			server.mu.Unlock()
			if reads != tc.wantDiscovery {
				t.Fatalf("discovery listing read %d times, want %d", reads, tc.wantDiscovery)
			}
			if tc.wantErr && (len(mints) != 0 || len(paths) != 0) {
				t.Fatalf("a failed discovery still minted or read: mints=%q paths=%q", mints, paths)
			}
		})
	}
}

// The janitor's discovery revokes its token even when the pass's context is
// already cancelled: the revoke runs on a context that ignores the cancel.
func TestQueueJanitorDiscoveryRevokesOnACancelledContext(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	server := newJanitorServer(t, alpha)
	ctx, cancel := context.WithCancel(context.Background())
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/installation/repositories" {
			cancel()
		}
		server.handler(w, r)
	})
	installation.Selection = "all"
	installation.Permissions = map[string]string{"actions": "read", "pull_requests": "read"}
	if _, err := app.queueJanitorPass(ctx, installation, []Repository{alpha}, "slipway-linux", false, janitorEpoch); err == nil {
		t.Fatal("a pass whose context was cancelled mid-discovery succeeded")
	}
	if _, revokes, _, _ := server.snapshot(); !slices.Equal(revokes, []string{janitorDiscoveryToken}) {
		t.Fatalf("revokes = %q, want exactly the discovery token", revokes)
	}
}

// The pool App's Discover is unchanged by the janitor's variant: it neither
// checks the grant nor revokes its token. This pins that behavior; the pool
// discovery is owned elsewhere.
func TestPoolDiscoverStillNeitherVerifiesNorRevokes(t *testing.T) {
	for _, grant := range []string{"", `,"permissions":{"metadata":"read","administration":"write"}`, `,"permissions":"read"`} {
		var deletes atomic.Int32
		app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodDelete:
				deletes.Add(1)
				w.WriteHeader(http.StatusNoContent)
			case r.URL.Path == "/app/installations/42/access_tokens":
				fmt.Fprintf(w, `{"token":"pool-discovery"%s}`, grant)
			case r.URL.Path == "/installation/repositories":
				fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":1,"full_name":"owner/a","private":true}]}`)
			default:
				t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
			}
		})
		repos, err := app.Discover(context.Background(), installation)
		if err != nil || len(repos) != 1 {
			t.Fatalf("grant %q: repos=%v err=%v", grant, repos, err)
		}
		if deletes.Load() != 0 {
			t.Fatalf("grant %q: pool Discover revoked its token %d times, want never", grant, deletes.Load())
		}
	}
}

func countOf(values []string, want string) int {
	n := 0
	for _, value := range values {
		if value == want {
			n++
		}
	}
	return n
}

// R1 via the workflow listing: the newer run already finished successfully,
// so it is not in the queued listing and must be found by the second read,
// which is built with url.Values.
func TestQueueJanitorFindsASupersedingRunThatAlreadyFinished(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, false, alpha)
	older := pushRun(100, "old", 0)
	older.HeadBranch = "feat/a b"
	newer := with(older, func(r *janitorRun) {
		r.ID, r.HeadSHA, r.CreatedAt, r.Status, r.Conclusion = 101, "new", janitorEpoch.Add(time.Minute), "completed", "success"
	})
	server.queued["owner/alpha"] = []map[string]any{runObject(older)}
	server.workflowRuns["owner/alpha 7"] = []map[string]any{runObject(newer), runObject(older)}
	server.fresh["owner/alpha 100"] = runObject(older)
	server.jobs["owner/alpha 100"] = queuedPoolJob
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", false, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	_, _, _, paths := server.snapshot()
	if !slices.Contains(paths, "GET /repos/owner/alpha/actions/workflows/7/runs?branch=feat%2Fa+b&event=push&per_page=50") {
		t.Fatalf("workflow listing not read with an encoded query: %q", paths)
	}
	if pass.WouldCancel != 1 {
		t.Fatalf("pass = %+v, want one would-cancel", pass)
	}
}

// Any read error keeps the run: a missing jobs listing is a 404 here.
func TestQueueJanitorKeepsARunWhoseJobsCannotBeRead(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	supersededPushFixture(server, alpha)
	delete(server.jobs, "owner/alpha 100")
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	mints, _, cancels, _ := server.snapshot()
	if len(cancels) != 0 || len(mints) != 1 || pass.SkippedErrors != 1 {
		t.Fatalf("cancels=%q mints=%d skipped=%d, want no cancel, no write mint, one skipped error", cancels, len(mints), pass.SkippedErrors)
	}
}

// A cancel GitHub does not accept with 202 is not counted as a cancel.
func TestQueueJanitorCountsARefusedCancelAsSkipped(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	app, installation, server := newJanitorFixture(t, true, alpha)
	supersededPushFixture(server, alpha)
	server.cancelStatus = http.StatusConflict
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	if pass.Cancelled != 0 || pass.SkippedErrors != 1 || len(pass.Actions) != 0 {
		t.Fatalf("pass = %+v, want no cancel and one skipped error", pass)
	}
}

// Below the rate-limit floor the pass stops and cancels nothing.
func TestQueueJanitorStopsAtTheRateLimitFloor(t *testing.T) {
	alpha := privateRepo(1, "alpha")
	server := newJanitorServer(t, alpha)
	supersededPushFixture(server, alpha)
	app, installation := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/repos/") {
			w.Header().Set("X-RateLimit-Remaining", "5")
		}
		server.handler(w, r)
	})
	installation.Selection = "all"
	installation.Permissions = map[string]string{"actions": "write", "pull_requests": "read"}
	pass, err := app.queueJanitorPass(context.Background(), installation, []Repository{alpha}, "slipway-linux", true, janitorEpoch)
	if err != nil {
		t.Fatalf("queueJanitorPass: %v", err)
	}
	_, revokes, cancels, paths := server.snapshot()
	if !pass.RateLimited || len(cancels) != 0 || len(paths) != 1 {
		t.Fatalf("rate_limited=%t cancels=%q paths=%q, want a stop after the first read", pass.RateLimited, cancels, paths)
	}
	assertRevoked(t, revokes, janitorDiscoveryToken, janitorReadToken)
}

// The queued listing is read oldest first: page 1 for the total, then the
// LAST pages, where GitHub keeps the oldest runs.
func TestJanitorQueuedRunsReadsTheOldestPages(t *testing.T) {
	var requested []string
	var mu sync.Mutex
	app, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requested = append(requested, r.URL.Query().Get("page"))
		mu.Unlock()
		page := r.URL.Query().Get("page")
		run := pushRun(0, "sha", 0)
		fmt.Sscanf(page, "%d", &run.ID)
		run.CreatedAt = janitorEpoch.Add(-time.Duration(run.ID) * time.Hour)
		body, _ := json.Marshal(map[string]any{"total_count": 1000, "workflow_runs": []map[string]any{runObject(run)}})
		w.Write(body)
	})
	var pass QueueJanitorPass
	runs, truncated, err := app.janitorQueuedRuns(context.Background(), "owner/alpha", "token", &pass)
	if err != nil {
		t.Fatalf("janitorQueuedRuns: %v", err)
	}
	if !slices.Equal(requested, []string{"1", "9", "10"}) || !truncated {
		t.Fatalf("pages %q truncated=%t, want 1, 9, 10 and truncated", requested, truncated)
	}
	if len(runs) != 3 || runs[0].ID != 10 || runs[2].ID != 1 {
		t.Fatalf("runs not oldest first: %+v", runs)
	}
}

func TestJanitorInstallationRequiresThePoolsAccount(t *testing.T) {
	app, _ := fixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/app/installations/77" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"id":77,"account":{"login":"Owner"},"repository_selection":"selected","permissions":{"actions":"write","pull_requests":"read","metadata":"read"}}`)
	})
	installation, err := app.JanitorInstallation(context.Background(), 77, "owner")
	if err != nil || installation.ID != 77 || installation.Permissions["actions"] != "write" {
		t.Fatalf("installation=%+v err=%v", installation, err)
	}
	if _, err := app.JanitorInstallation(context.Background(), 77, "someone-else"); err == nil {
		t.Fatal("an installation on another account was accepted")
	}
	if _, err := app.JanitorInstallation(context.Background(), 0, "owner"); err == nil {
		t.Fatal("installation ID 0 was accepted")
	}
}
