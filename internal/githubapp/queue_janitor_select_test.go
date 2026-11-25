package githubapp

import (
	"fmt"
	"testing"
	"time"
)

var janitorEpoch = time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)

const janitorRepoID = 1

// pushRun is a queued first-attempt push run on main in the fixture
// repository, created `minutes` after janitorEpoch.
func pushRun(id int64, sha string, minutes int) janitorRun {
	return janitorRun{ID: id, WorkflowID: 7, HeadBranch: "main", HeadSHA: sha, HeadRepositoryID: janitorRepoID,
		Event: "push", Status: "queued", RunAttempt: 1, CreatedAt: janitorEpoch.Add(time.Duration(minutes) * time.Minute)}
}

// prRun is a queued first-attempt pull_request run for PR `number` from the
// fixture repository itself.
func prRun(id int64, sha string, minutes int, number int64) janitorRun {
	run := pushRun(id, sha, minutes)
	run.Event, run.HeadBranch = "pull_request", "feature"
	run.PullRequests = []janitorRunPR{{Number: number, BaseRepositoryID: janitorRepoID}}
	return run
}

func with(run janitorRun, edit func(*janitorRun)) janitorRun {
	edit(&run)
	return run
}

func poolJobs(labels ...[]string) *janitorJobs {
	jobs := &janitorJobs{TotalCount: len(labels)}
	for _, set := range labels {
		jobs.Jobs = append(jobs.Jobs, janitorJob{Labels: set, Status: "queued"})
	}
	return jobs
}

// oneJobReference is a workflow whose newest successful run on the branch,
// created ten hours after every fixture run, had one job.
func oneJobReference() *janitorReference {
	return &janitorReference{RunID: 900, CreatedAt: janitorEpoch.Add(10 * time.Hour), JobCount: 1}
}

var onlyPoolLabel = []string{"slipway-linux"}

// supersededFacts is the baseline every row below edits: an older push run
// on main, a newer queued push run on a different commit, pool-only jobs and
// no tag named main. As written it is cancelled under R1.
func supersededFacts() janitorFacts {
	return janitorFacts{
		Run: pushRun(100, "old", 0), RepoID: janitorRepoID, Label: "slipway-linux", Reference: oneJobReference(),
		Jobs: poolJobs(onlyPoolLabel), NoTagNamedHeadBranch: true,
		Newer: []janitorRun{pushRun(101, "new", 5)},
	}
}

func closedPR(number int64, closedMinutes int) map[int64]janitorPR {
	return map[int64]janitorPR{number: {Number: number, State: "closed", BaseRepositoryID: janitorRepoID,
		ClosedAt: janitorEpoch.Add(time.Duration(closedMinutes) * time.Minute)}}
}

// closedPRFacts is a pull_request run for PR 9 created at +0, the PR closed
// at +30, and no newer run: cancelled under R2.
func closedPRFacts() janitorFacts {
	return janitorFacts{
		Run: prRun(200, "sha", 0, 9), RepoID: janitorRepoID, Label: "slipway-linux", Reference: oneJobReference(),
		Jobs: poolJobs(onlyPoolLabel), PullRequests: closedPR(9, 30),
	}
}

func TestDecideJanitorRun(t *testing.T) {
	type row struct {
		name  string
		facts func() janitorFacts
		want  string
	}
	// keptFor is a row that must be kept for exactly `reason`, so a row
	// meant to reach one guard cannot pass on another.
	type keptFor struct {
		name   string
		facts  func() janitorFacts
		reason string
	}
	edit := func(base func() janitorFacts, change func(*janitorFacts)) func() janitorFacts {
		return func() janitorFacts {
			facts := base()
			change(&facts)
			return facts
		}
	}
	newerState := func(status, conclusion string) func() janitorFacts {
		return edit(supersededFacts, func(f *janitorFacts) {
			f.Newer[0].Status, f.Newer[0].Conclusion = status, conclusion
		})
	}
	rows := []row{
		{"a push run superseded by a newer queued run is cancelled", supersededFacts, JanitorRuleSuperseded},
		{"a newer in-progress run supersedes", newerState("in_progress", ""), JanitorRuleSuperseded},
		{"a newer successful run supersedes", newerState("completed", "success"), JanitorRuleSuperseded},
		{"a newer failed run supersedes", newerState("completed", "failure"), JanitorRuleSuperseded},
		// Plan case: a newer run that was cancelled or hit startup_failure.
		{"a newer cancelled run does not supersede", newerState("completed", "cancelled"), ""},
		{"a newer startup_failure run does not supersede", newerState("completed", "startup_failure"), ""},
		{"a newer skipped run does not supersede", newerState("completed", "skipped"), ""},
		// Staged break: the different-head_sha requirement.
		{"a newer run on the SAME commit does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].HeadSHA = "old" }), ""},
		{"a newer run with no head SHA does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].HeadSHA = "" }), ""},
		{"a run created at the same instant does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].CreatedAt = f.Run.CreatedAt }), ""},
		{"an OLDER run on a different commit does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].CreatedAt = f.Run.CreatedAt.Add(-time.Minute) }), ""},
		{"a newer run of another workflow does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].WorkflowID = 8 }), ""},
		{"a newer run from another head repository does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].HeadRepositoryID = 99 }), ""},
		{"a newer run with no head repository does not supersede", edit(supersededFacts, func(f *janitorFacts) {
			f.Run.HeadRepositoryID, f.Newer[0].HeadRepositoryID = 0, 0
		}), ""},
		{"a newer run on another branch does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].HeadBranch = "other" }), ""},
		{"a newer run from another event does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer[0].Event = "pull_request" }), ""},
		{"the run itself listed as newer does not supersede", edit(supersededFacts, func(f *janitorFacts) { f.Newer = []janitorRun{f.Run} }), ""},
		{"no newer runs keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Newer = nil }), ""},
		// Staged break: the event allowlist.
		{"a workflow_dispatch run is kept", edit(supersededFacts, func(f *janitorFacts) {
			f.Run.Event, f.Newer[0].Event = "workflow_dispatch", "workflow_dispatch"
		}), ""},
		{"a schedule run is kept", edit(supersededFacts, func(f *janitorFacts) { f.Run.Event, f.Newer[0].Event = "schedule", "schedule" }), ""},
		{"a merge_group run is kept", edit(supersededFacts, func(f *janitorFacts) { f.Run.Event, f.Newer[0].Event = "merge_group", "merge_group" }), ""},
		// Plan case: a tag push. head_branch carries the tag name, and a
		// tag of that name exists.
		{"a tag push is kept", edit(supersededFacts, func(f *janitorFacts) {
			f.Run.HeadBranch, f.Newer[0].HeadBranch, f.NoTagNamedHeadBranch = "v1.2.3", "v1.2.3", false
		}), ""},
		// Plan case: a re-run.
		{"a re-run is kept", edit(supersededFacts, func(f *janitorFacts) { f.Run.RunAttempt = 2 }), ""},
		{"a run the listing no longer calls queued is kept", edit(supersededFacts, func(f *janitorFacts) { f.Run.Status = "in_progress" }), ""},
		{"a run with no created_at is kept", edit(supersededFacts, func(f *janitorFacts) { f.Run.CreatedAt = time.Time{} }), ""},
		// The job-count rule lets a multi-job run through when the workflow
		// had exactly that many jobs.
		{"a two-job run of a two-job workflow is cancelled", edit(supersededFacts, func(f *janitorFacts) {
			f.Jobs, f.Reference.JobCount = poolJobs(onlyPoolLabel, onlyPoolLabel), 2
		}), JanitorRuleSuperseded},
		// The label guard's other rows are in the keptFor table below. Plan
		// case: a partial job list.
		{"the pool label matches case-insensitively", edit(supersededFacts, func(f *janitorFacts) { f.Jobs = poolJobs([]string{"slipway-linux"}) }), JanitorRuleSuperseded},
		{"a partial job list keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs.TotalCount = 2 }), ""},
		{"an empty job list keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs = poolJobs() }), ""},
		{"unread jobs keep the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs = nil }), ""},
		{"an empty pool label keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Label = "" }), ""},
		// pull_request R1.
		{"a pull_request run superseded on the same PR is cancelled", func() janitorFacts {
			return janitorFacts{Run: prRun(300, "old", 0, 9), RepoID: janitorRepoID, Label: "slipway-linux", Reference: oneJobReference(),
				Jobs: poolJobs(onlyPoolLabel), Newer: []janitorRun{prRun(301, "new", 5, 9)}}
		}, JanitorRuleSuperseded},
		{"a newer run for a different PR does not supersede", func() janitorFacts {
			return janitorFacts{Run: prRun(300, "old", 0, 9), RepoID: janitorRepoID, Label: "slipway-linux", Reference: oneJobReference(),
				Jobs: poolJobs(onlyPoolLabel), Newer: []janitorRun{prRun(301, "new", 5, 10)}}
		}, ""},
		// Plan case: pull_request_target is allowed, under the same rules.
		{"a pull_request_target run superseded on the same PR is cancelled", func() janitorFacts {
			old, newer := prRun(400, "old", 0, 9), prRun(401, "new", 5, 9)
			old.Event, newer.Event = "pull_request_target", "pull_request_target"
			return janitorFacts{Run: old, RepoID: janitorRepoID, Label: "slipway-linux", Reference: oneJobReference(), Jobs: poolJobs(onlyPoolLabel), Newer: []janitorRun{newer}}
		}, JanitorRuleSuperseded},
		{"a pull_request_target run of a PR closed after it was created is cancelled", edit(closedPRFacts, func(f *janitorFacts) {
			f.Run.Event = "pull_request_target"
		}), JanitorRuleClosedPR},
		// Plan case: a fork head repo. Its pull_requests[] is empty.
		{"a fork PR run is kept even when a newer fork run exists", func() janitorFacts {
			old, newer := prRun(500, "old", 0, 9), prRun(501, "new", 5, 9)
			old.HeadRepositoryID, newer.HeadRepositoryID = 77, 77
			old.PullRequests, newer.PullRequests = nil, nil
			return janitorFacts{Run: old, RepoID: janitorRepoID, Label: "slipway-linux", Reference: oneJobReference(), Jobs: poolJobs(onlyPoolLabel),
				Newer: []janitorRun{newer}, PullRequests: closedPR(9, 30)}
		}, ""},
		{"a fork PR run that lists its PR is still kept by R2", edit(closedPRFacts, func(f *janitorFacts) { f.Run.HeadRepositoryID = 77 }), ""},
		// R2.
		{"a PR run created before the PR closed is cancelled", closedPRFacts, JanitorRuleClosedPR},
		// Plan case: a closed-type PR run, created by the close itself.
		{"a closed-type PR run created after the close is kept", edit(closedPRFacts, func(f *janitorFacts) {
			f.Run.CreatedAt = janitorEpoch.Add(31 * time.Minute)
		}), ""},
		{"a PR run created at the close instant is kept", edit(closedPRFacts, func(f *janitorFacts) {
			f.Run.CreatedAt = janitorEpoch.Add(30 * time.Minute)
		}), ""},
		{"an open PR keeps the run", edit(closedPRFacts, func(f *janitorFacts) {
			f.PullRequests = map[int64]janitorPR{9: {Number: 9, State: "open", BaseRepositoryID: janitorRepoID}}
		}), ""},
		{"a closed PR with no closed_at keeps the run", edit(closedPRFacts, func(f *janitorFacts) {
			f.PullRequests = map[int64]janitorPR{9: {Number: 9, State: "closed", BaseRepositoryID: janitorRepoID}}
		}), ""},
		{"a PR that was not read keeps the run", edit(closedPRFacts, func(f *janitorFacts) { f.PullRequests = nil }), ""},
		{"one open PR among two keeps the run", edit(closedPRFacts, func(f *janitorFacts) {
			f.Run.PullRequests = append(f.Run.PullRequests, janitorRunPR{Number: 10, BaseRepositoryID: janitorRepoID})
			f.PullRequests[10] = janitorPR{Number: 10, State: "open", BaseRepositoryID: janitorRepoID}
		}), ""},
		{"a PR in another repository keeps the run", edit(closedPRFacts, func(f *janitorFacts) {
			f.Run.PullRequests[0].BaseRepositoryID = 55
		}), ""},
		{"a looked-up PR based in another repository keeps the run", edit(closedPRFacts, func(f *janitorFacts) {
			pr := f.PullRequests[9]
			pr.BaseRepositoryID = 55
			f.PullRequests[9] = pr
		}), ""},
		{"R2 never applies to a push", edit(closedPRFacts, func(f *janitorFacts) {
			f.Run.Event, f.NoTagNamedHeadBranch = "push", true
		}), ""},
		{"a closed-PR run with a foreign job label is kept", edit(closedPRFacts, func(f *janitorFacts) {
			f.Jobs = poolJobs([]string{"self-hosted", "slipway-linux"})
		}), ""},
	}
	for _, tc := range []keptFor{
		// Staged break: the label guard. Plan case: a job with extra labels.
		{"a job with extra labels keeps the run", edit(supersededFacts, func(f *janitorFacts) {
			f.Jobs = poolJobs(onlyPoolLabel, []string{"slipway-linux", "gpu"})
			f.Reference.JobCount = 2
		}), "a job's labels are not exactly the pool label"},
		{"a job on another runner label keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs = poolJobs([]string{"ubuntu-latest"}) }),
			"a job's labels are not exactly the pool label"},
		{"a job with no labels keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs = poolJobs([]string{}) }),
			"a job's labels are not exactly the pool label"},
		// Staged break: runner_name and job status (review I1). A job a
		// runner holds, or any job not queued, has started work.
		{"a job with a runner keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs.Jobs[0].RunnerName = "slipway-linux-7" }),
			"a job has a runner"},
		{"an in_progress job keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs.Jobs[0].Status = "in_progress" }),
			"a job is not queued"},
		{"a completed job keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs.Jobs[0].Status = "completed" }),
			"a job is not queued"},
		{"a waiting job keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs.Jobs[0].Status = "waiting" }),
			"a job is not queued"},
		{"a job with no status keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Jobs.Jobs[0].Status = "" }),
			"a job is not queued"},
		// Staged break: the job-count rule (review I2). A queued run lists
		// only the jobs GitHub has created; a needs: job comes later.
		{"no workflow job count keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Reference = nil }),
			janitorKeptJobCountUnknown},
		{"a reference with no jobs keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Reference.JobCount = 0 }),
			janitorKeptJobCountUnknown},
		{"a reference with no run keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Reference.RunID = 0 }),
			janitorKeptJobCountUnknown},
		{"a workflow with a job not yet created keeps the run", edit(supersededFacts, func(f *janitorFacts) { f.Reference.JobCount = 2 }),
			janitorKeptJobCountDiffers},
		// Re-review: a success from before the run may have run an older
		// workflow file, without a needs: job this run's file added.
		{"a reference that predates the run keeps it", edit(supersededFacts, func(f *janitorFacts) {
			f.Reference.CreatedAt = f.Run.CreatedAt.Add(-time.Minute)
		}), janitorKeptJobCountStale},
		{"a reference created with the run keeps it", edit(supersededFacts, func(f *janitorFacts) {
			f.Reference.CreatedAt = f.Run.CreatedAt
		}), janitorKeptJobCountStale},
		{"a reference with no created_at keeps the run", edit(supersededFacts, func(f *janitorFacts) {
			f.Reference.CreatedAt = time.Time{}
		}), janitorKeptJobCountStale},
		{"a run listing more jobs than the workflow had keeps the run", edit(supersededFacts, func(f *janitorFacts) {
			f.Jobs = poolJobs(onlyPoolLabel, onlyPoolLabel)
		}), janitorKeptJobCountDiffers},
		{"a closed-PR run with a job not yet created is kept", edit(closedPRFacts, func(f *janitorFacts) { f.Reference.JobCount = 3 }),
			janitorKeptJobCountDiffers},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule, kept := decideJanitorRun(tc.facts())
			if rule != "" || kept != tc.reason {
				t.Fatalf("decideJanitorRun() = %q, kept for %q; want kept for %q", rule, kept, tc.reason)
			}
		})
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			facts := tc.facts()
			rule, kept := decideJanitorRun(facts)
			if rule != tc.want {
				t.Fatalf("decideJanitorRun() rule = %q (kept: %q), want %q", rule, kept, tc.want)
			}
			if rule == "" && kept == "" {
				t.Fatal("a kept run carries no reason")
			}
		})
	}
}

// Plan case: a run that changes from queued to in_progress before the
// cancel. Staged break: the status re-check.
func TestJanitorStillCancellable(t *testing.T) {
	listed := pushRun(100, "old", 0)
	for _, tc := range []struct {
		name  string
		fresh janitorRun
		want  bool
	}{
		{"still queued", listed, true},
		{"now in progress", with(listed, func(r *janitorRun) { r.Status = "in_progress" }), false},
		{"now completed", with(listed, func(r *janitorRun) { r.Status = "completed" }), false},
		{"re-run since the listing", with(listed, func(r *janitorRun) { r.RunAttempt = 2 }), false},
		{"a different run", with(listed, func(r *janitorRun) { r.ID = 999 }), false},
		{"a different commit", with(listed, func(r *janitorRun) { r.HeadSHA = "other" }), false},
		{"an empty response", janitorRun{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := janitorStillCancellable(tc.fresh, listed); got != tc.want {
				t.Fatalf("janitorStillCancellable() = %t, want %t", got, tc.want)
			}
		})
	}
}

// The whole pre-cancel re-check on fresh reads (review I1, I2): the run, then
// its jobs as re-read immediately before the cancel, held to the job count
// the evaluation matched.
func TestJanitorPreCancelRefusal(t *testing.T) {
	listed := pushRun(100, "old", 0)
	for _, tc := range []struct {
		name      string
		fresh     janitorRun
		jobs      *janitorJobs
		reference *janitorReference
		want      string
	}{
		{"still queued, one pool job, one-job workflow", listed, poolJobs(onlyPoolLabel), oneJobReference(), ""},
		{"the run started", with(listed, func(r *janitorRun) { r.Status = "in_progress" }), poolJobs(onlyPoolLabel), oneJobReference(),
			"run no longer queued, first-attempt and on the same commit"},
		{"a runner took the job, run still queued", listed, with2(poolJobs(onlyPoolLabel), func(j *janitorJobs) { j.Jobs[0].RunnerName = "slipway-linux-3" }),
			oneJobReference(), "a job has a runner"},
		{"the job started, run still queued", listed, with2(poolJobs(onlyPoolLabel), func(j *janitorJobs) { j.Jobs[0].Status = "in_progress" }),
			oneJobReference(), "a job is not queued"},
		{"a job created since the evaluation", listed, poolJobs(onlyPoolLabel, onlyPoolLabel), oneJobReference(), janitorKeptJobCountDiffers},
		{"a job on another runner created since the evaluation", listed, poolJobs(onlyPoolLabel, []string{"ubuntu-latest"}),
			&janitorReference{RunID: 900, JobCount: 2}, "a job's labels are not exactly the pool label"},
		{"no reference", listed, poolJobs(onlyPoolLabel), nil, janitorKeptJobCountUnknown},
		{"a reference older than the run", listed, poolJobs(onlyPoolLabel),
			&janitorReference{RunID: 900, CreatedAt: listed.CreatedAt.Add(-time.Hour), JobCount: 1}, janitorKeptJobCountStale},
		{"no jobs listed", listed, poolJobs(), oneJobReference(), "no jobs listed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := janitorPreCancelRefusal(tc.fresh, listed, *tc.jobs, "slipway-linux", tc.reference); got != tc.want {
				t.Fatalf("janitorPreCancelRefusal() = %q, want %q", got, tc.want)
			}
		})
	}
}

func with2(jobs *janitorJobs, edit func(*janitorJobs)) *janitorJobs {
	edit(jobs)
	return jobs
}

func candidatesFor(repoID int64, count, startMinute int) []janitorCandidate {
	repo := Repository{ID: repoID, FullName: fmt.Sprintf("owner/repo%d", repoID), Private: true}
	var out []janitorCandidate
	for i := range count {
		run := pushRun(repoID*1000+int64(i), "sha", startMinute+i)
		out = append(out, janitorCandidate{Repository: repo, Run: run, Rule: JanitorRuleSuperseded})
	}
	return out
}

// Plan case: both caps.
func TestApplyJanitorCaps(t *testing.T) {
	t.Run("under both caps nothing is dropped", func(t *testing.T) {
		selected, capped := applyJanitorCaps([][]janitorCandidate{candidatesFor(1, 3, 0), candidatesFor(2, 2, 0)}, 10, 50)
		if len(selected) != 5 || capped {
			t.Fatalf("selected %d capped=%t, want 5 and false", len(selected), capped)
		}
	})
	t.Run("the per-repository cap keeps the oldest ten", func(t *testing.T) {
		// Supplied newest first, to prove the cap sorts rather than truncates.
		candidates := candidatesFor(1, 12, 0)
		for i, j := 0, len(candidates)-1; i < j; i, j = i+1, j-1 {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		}
		selected, capped := applyJanitorCaps([][]janitorCandidate{candidates}, 10, 50)
		if len(selected) != 10 || !capped {
			t.Fatalf("selected %d capped=%t, want 10 and true", len(selected), capped)
		}
		for i, candidate := range selected {
			if want := janitorEpoch.Add(time.Duration(i) * time.Minute); !candidate.Run.CreatedAt.Equal(want) {
				t.Fatalf("selected[%d] created %v, want %v (oldest first)", i, candidate.Run.CreatedAt, want)
			}
		}
	})
	t.Run("exactly the per-repository cap is not capped", func(t *testing.T) {
		if _, capped := applyJanitorCaps([][]janitorCandidate{candidatesFor(1, 10, 0)}, 10, 50); capped {
			t.Fatal("ten candidates against a cap of ten reported capped")
		}
	})
	t.Run("the per-pass cap spreads across repositories", func(t *testing.T) {
		// Repository 1's backlog is the oldest by far; a plain oldest-first cut
		// would give it everything before any other repository.
		byRepo := [][]janitorCandidate{candidatesFor(1, 10, 0)}
		for repo := int64(2); repo <= 6; repo++ {
			byRepo = append(byRepo, candidatesFor(repo, 10, 1000))
		}
		selected, capped := applyJanitorCaps(byRepo, 10, 50)
		if len(selected) != 50 || !capped {
			t.Fatalf("selected %d capped=%t, want 50 and true", len(selected), capped)
		}
		perRepo := map[int64]int{}
		for _, candidate := range selected {
			perRepo[candidate.Repository.ID]++
		}
		for repo := int64(1); repo <= 6; repo++ {
			if perRepo[repo] < 8 || perRepo[repo] > 10 {
				t.Fatalf("repository %d received %d of 50; want each of six within 8..10: %v", repo, perRepo[repo], perRepo)
			}
		}
		if selected[0].Repository.ID != 1 || !selected[0].Run.CreatedAt.Equal(janitorEpoch) {
			t.Fatalf("the first selection is not the oldest candidate overall: %+v", selected[0].Run)
		}
	})
	t.Run("exactly the per-pass cap is not capped", func(t *testing.T) {
		byRepo := [][]janitorCandidate{}
		for repo := int64(1); repo <= 5; repo++ {
			byRepo = append(byRepo, candidatesFor(repo, 10, 0))
		}
		if selected, capped := applyJanitorCaps(byRepo, 10, 50); len(selected) != 50 || capped {
			t.Fatalf("selected %d capped=%t, want 50 and false", len(selected), capped)
		}
	})
}
