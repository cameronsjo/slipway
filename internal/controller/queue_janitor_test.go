package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

func TestParseQueueJanitorModeFailsClosedToOff(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want QueueJanitorMode
		ok   bool
	}{
		{"off", QueueJanitorOff, true},
		{"dry-run", QueueJanitorDryRun, true},
		{"enforce", QueueJanitorEnforce, true},
		// Exact lowercase only, the same set verify-manifests.py pins.
		{" Enforce ", QueueJanitorOff, false},
		{"ENFORCE", QueueJanitorOff, false},
		{"Dry-Run", QueueJanitorOff, false},
		{"enforce ", QueueJanitorOff, false},
		{" dry-run", QueueJanitorOff, false},
		{"Off", QueueJanitorOff, false},
		{"", QueueJanitorOff, false},
		{"on", QueueJanitorOff, false},
		{"true", QueueJanitorOff, false},
		{"dryrun", QueueJanitorOff, false},
		{"enforce,dry-run", QueueJanitorOff, false},
	} {
		if got, ok := ParseQueueJanitorMode(tc.raw); got != tc.want || ok != tc.ok {
			t.Errorf("ParseQueueJanitorMode(%q) = %q, %t; want %q, %t", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

// scriptedJanitorSource records every pass and returns one scripted result.
type scriptedJanitorSource struct {
	result  QueueJanitorResult
	err     error
	calls   int
	repos   []RepoRef
	label   string
	enforce []bool
}

func (s *scriptedJanitorSource) Pass(_ context.Context, repos []RepoRef, label string, enforce bool) (QueueJanitorResult, error) {
	s.calls++
	s.repos, s.label = repos, label
	s.enforce = append(s.enforce, enforce)
	return s.result, s.err
}

func captureJanitor(janitor *QueueJanitor) (*[]string, *bytes.Buffer) {
	var lines []string
	var logged bytes.Buffer
	janitor.print = func(line string) { lines = append(lines, line) }
	janitor.log = slog.New(slog.NewTextHandler(&logged, nil))
	return &lines, &logged
}

func TestQueueJanitorPassPrintsOneAuditLinePerActionAndTheSummary(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 2)
	source := &scriptedJanitorSource{result: QueueJanitorResult{Examined: 9, Cancelled: 1, Capped: true, SkippedErrors: 2, KeptUnprovenJobs: 3,
		Actions: []QueueJanitorAction{{Repository: "owner/repo1", RunID: 100, Rule: "superseded", WorkflowID: 7, Jobs: 2, RefRunID: 90, RefJobs: 2,
			Age: 90*time.Minute + 400*time.Millisecond, URL: "https://github.com/owner/repo1/actions/runs/100", Cancelled: true}},
		Kept: []QueueJanitorKept{{Repository: "owner/repo1", RunID: 99, Rule: "superseded", Reason: "a job has a runner"}}}}
	janitor := NewQueueJanitor(s, source, "linux", QueueJanitorEnforce, 10*time.Minute)
	lines, _ := captureJanitor(janitor)
	janitor.Pass(ctx)
	want := []string{
		`janitor kept repo=owner/repo1 run=99 rule=superseded reason="a job has a runner"`,
		"janitor cancelled repo=owner/repo1 run=100 rule=superseded workflow_id=7 jobs=2 ref_run=90 ref_jobs=2 age=1h30m0s url=https://github.com/owner/repo1/actions/runs/100",
		"janitor mode=enforce examined=9 cancelled=1 would_cancel=0 capped=true skipped_errors=2 kept_unproven_jobs=3",
	}
	if strings.Join(*lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("lines\n got %q\nwant %q", *lines, want)
	}
	if source.label != "slipway-linux" || len(source.repos) != 2 || source.enforce[0] != true {
		t.Fatalf("source called with label=%q repos=%v enforce=%v", source.label, source.repos, source.enforce)
	}
}

func TestQueueJanitorDryRunNeverAsksForEnforce(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 1)
	source := &scriptedJanitorSource{result: QueueJanitorResult{Examined: 1, WouldCancel: 1,
		Actions: []QueueJanitorAction{{Repository: "owner/repo1", RunID: 7, Rule: "closed-pr", Age: time.Hour, URL: "u"}}}}
	janitor := NewQueueJanitor(s, source, "linux", QueueJanitorDryRun, 10*time.Minute)
	lines, _ := captureJanitor(janitor)
	janitor.Pass(ctx)
	if len(source.enforce) != 1 || source.enforce[0] {
		t.Fatalf("dry-run asked the source for enforce: %v", source.enforce)
	}
	if (*lines)[0] != "janitor would_cancel repo=owner/repo1 run=7 rule=closed-pr workflow_id=0 jobs=0 ref_run=0 ref_jobs=0 age=1h0m0s url=u" ||
		(*lines)[1] != "janitor mode=dry-run examined=1 cancelled=0 would_cancel=1 capped=false skipped_errors=0 kept_unproven_jobs=0" {
		t.Fatalf("lines = %q", *lines)
	}
}

// The summary is printed for a failed pass too, with the failure counted.
func TestQueueJanitorPrintsTheSummaryWhenThePassFails(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 1)
	source := &scriptedJanitorSource{err: errors.New("mint refused")}
	janitor := NewQueueJanitor(s, source, "linux", QueueJanitorEnforce, 10*time.Minute)
	lines, logged := captureJanitor(janitor)
	janitor.Pass(ctx)
	if len(*lines) != 1 || (*lines)[0] != "janitor mode=enforce examined=0 cancelled=0 would_cancel=0 capped=false skipped_errors=1 kept_unproven_jobs=0" {
		t.Fatalf("lines = %q", *lines)
	}
	if !strings.Contains(logged.String(), "mint refused") {
		t.Fatalf("the failure was not logged: %s", logged)
	}
}

// The summary is printed when there is nothing to pass over, too.
func TestQueueJanitorPrintsTheSummaryWithNoServedRepositories(t *testing.T) {
	s, _ := openTestStore(t, 2)
	source := &scriptedJanitorSource{}
	janitor := NewQueueJanitor(s, source, "linux", QueueJanitorDryRun, 10*time.Minute)
	lines, _ := captureJanitor(janitor)
	janitor.Pass(ctx)
	if source.calls != 0 || len(*lines) != 1 || !strings.HasPrefix((*lines)[0], "janitor mode=dry-run examined=0") {
		t.Fatalf("calls=%d lines=%q", source.calls, *lines)
	}
}

func TestQueueJanitorBacksOffUntilTheReportedReset(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 1)
	clock := time.Now().UTC()
	reset := clock.Add(30 * time.Minute)
	source := &scriptedJanitorSource{result: QueueJanitorResult{RateLimited: true, RetryAfter: reset}}
	janitor := NewQueueJanitor(s, source, "linux", QueueJanitorDryRun, 10*time.Minute)
	captureJanitor(janitor)
	janitor.now = func() time.Time { return clock }
	janitor.Pass(ctx)
	if delay := janitor.nextDelay(); delay != 30*time.Minute {
		t.Fatalf("next pass in %v, want the 30m until the reset", delay)
	}
	clock = reset.Add(-time.Second)
	janitor.Pass(ctx)
	if source.calls != 1 {
		t.Fatalf("passed %d times before the reset, want 1", source.calls)
	}
	clock = reset
	janitor.Pass(ctx)
	if source.calls != 2 {
		t.Fatalf("did not resume at the reset: calls=%d", source.calls)
	}
}

func TestADisabledQueueJanitorDoesNothing(t *testing.T) {
	s, _ := openTestStore(t, 2)
	prepare(t, s, 1)
	source := &scriptedJanitorSource{}
	for _, janitor := range []*QueueJanitor{
		NewQueueJanitor(s, source, "linux", QueueJanitorOff, time.Minute),
		NewQueueJanitor(s, source, "linux", QueueJanitorMode("bogus"), time.Minute),
		NewQueueJanitor(s, nil, "linux", QueueJanitorEnforce, time.Minute),
	} {
		lines, _ := captureJanitor(janitor)
		janitor.Pass(ctx)
		done := make(chan struct{})
		go func() {
			janitor.Run(context.Background())
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("a disabled janitor's Run did not return")
		}
		if source.calls != 0 || len(*lines) != 0 {
			t.Fatalf("a disabled janitor passed: calls=%d lines=%q", source.calls, *lines)
		}
	}
}

// The permissions are checked once, at construction. A missing one is ONE
// error line that names it and says a restart is needed after the grant, and
// the janitor is off. It never refuses to build the controller.
func TestGitHubQueueJanitorChecksPermissionsAtStartup(t *testing.T) {
	s, _ := openTestStore(t, 2)
	app := testGitHubApp(t)
	for _, tc := range []struct {
		name        string
		mode        QueueJanitorMode
		permissions map[string]string
		enabled     bool
		missing     string
	}{
		{"dry-run granted", QueueJanitorDryRun, map[string]string{"actions": "read", "pull_requests": "read"}, true, ""},
		{"enforce granted", QueueJanitorEnforce, map[string]string{"actions": "write", "pull_requests": "read"}, true, ""},
		{"dry-run without pull requests", QueueJanitorDryRun, map[string]string{"actions": "read"}, false, "missing_permission=pull_requests:read"},
		{"enforce without actions write", QueueJanitorEnforce, map[string]string{"actions": "read", "pull_requests": "read"}, false, "missing_permission=actions:write"},
		{"enforce with nothing", QueueJanitorEnforce, map[string]string{}, false, "missing_permission=actions:read,pull_requests:read,actions:write"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installation := githubapp.Installation{ID: 77, Selection: "selected", Permissions: tc.permissions}
			var logged bytes.Buffer
			janitor, err := NewGitHubQueueJanitor(s, app, installation, "linux", tc.mode, time.Minute, slog.New(slog.NewTextHandler(&logged, nil)))
			if err != nil || janitor == nil {
				t.Fatalf("construction refused: %v", err)
			}
			if lines := strings.Count(strings.TrimSpace(logged.String()), "\n") + 1; lines != 1 {
				t.Fatalf("want exactly one log line, got %d: %s", lines, logged.String())
			}
			if janitor.Enabled() != tc.enabled {
				t.Fatalf("enabled = %t, want %t: %s", janitor.Enabled(), tc.enabled, logged.String())
			}
			if !tc.enabled && (!strings.Contains(logged.String(), tc.missing) || !strings.Contains(logged.String(), "restart the controller") ||
				!strings.Contains(logged.String(), "level=ERROR")) {
				t.Fatalf("the disabling line does not name %s and the restart: %s", tc.missing, logged.String())
			}
		})
	}
	var logged bytes.Buffer
	janitor, err := NewGitHubQueueJanitor(s, nil, githubapp.Installation{}, "linux", QueueJanitorOff, time.Minute, slog.New(slog.NewTextHandler(&logged, nil)))
	if err != nil || janitor.Enabled() || strings.Count(logged.String(), "Queue janitor off.") != 1 {
		t.Fatalf("off: err=%v enabled=%t log=%s", err, janitor.Enabled(), logged.String())
	}
}
