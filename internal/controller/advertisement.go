package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/actions/scaleset"
)

// AdvertisementPlan is one polling sweep's per-scale-set maximum capacity plus
// the demand that sweep could not plan. Capacity preserves every charged worker
// and then hands the remaining admission to repositories with unmet demand --
// assigned jobs and available ones -- in durable round-robin order, so a busy
// repository grows to the platform cap while every active repository receives
// a turn.
//
// Deferred is GitHub's demand minus what this sweep planned. Demand is not a
// controller invariant: GitHub's broker assigns jobs to every allowlisted
// repository at once, and a shared pool is normally asked for more than its
// admission. Deferred records that overflow so the next sweep can serve it;
// it is never an error.
type AdvertisementPlan struct {
	Capacity map[SetKey]int
	Deferred int
}

// PlanAdvertisements must be called once for each complete sweep of active
// sessions. Every supplied session receives a value, including zero, so callers
// keep polling lifecycle events while the free-slot opportunity rotates.
func (s *Store) PlanAdvertisements(ctx context.Context, sessions []SetKey) (AdvertisementPlan, error) {
	return s.PlanAdvertisementsExcept(ctx, sessions, nil)
}

// PlanAdvertisementsExcept plans over sessions while leaving out skipped: sets
// that are durably active but have no live session this cycle, because their
// resume failed (SessionManager.Degraded). They are neither planned nor
// counted by the completeness check below, which still refuses any other
// active session missing from the sweep.
func (s *Store) PlanAdvertisementsExcept(ctx context.Context, sessions, skipped []SetKey) (AdvertisementPlan, error) {
	return s.PlanAdvertisementsWith(ctx, sessions, skipped, nil)
}

// PlanAdvertisementsWith is PlanAdvertisementsExcept plus noREST: sets that are
// planned and polled, but whose REST demand must not raise capacity this
// sweep, because their last poll failed and AcquireNext will not reserve for
// them (SessionManager.unreservableKeys). Their occupancy is still planned, so
// a running worker is never un-advertised.
func (s *Store) PlanAdvertisementsWith(ctx context.Context, sessions, skipped, noREST []SetKey) (AdvertisementPlan, error) {
	skip := make(map[SetKey]bool, len(skipped))
	for _, key := range skipped {
		skip[key] = true
	}
	withoutREST := make(map[SetKey]bool, len(noREST))
	for _, key := range noREST {
		withoutREST[key] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var empty AdvertisementPlan
	if !s.ready {
		return empty, ErrInventoryUnknown
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return empty, err
	}
	keys := append([]SetKey(nil), sessions...)
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].RepositoryID == keys[j].RepositoryID {
			return keys[i].ScaleSetID < keys[j].ScaleSetID
		}
		return keys[i].RepositoryID < keys[j].RepositoryID
	})
	for i, key := range keys {
		if key.RepositoryID <= 0 || key.ScaleSetID <= 0 || (i > 0 && keys[i-1].RepositoryID == key.RepositoryID) {
			return empty, errors.New("invalid or duplicate advertisement session")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer tx.Rollback()
	// One instant for the whole sweep, so the stale-assignment cutoff and the
	// statistics freshness window cannot disagree about when "now" was.
	planningAt := time.Now().UTC()
	journaled, err := journaledDemand(ctx, tx, planningAt.Add(-staleAssignmentWindow))
	if err != nil {
		return empty, err
	}
	demand := map[int64]int{}
	restQueued := map[int64]int{}
	restReadAt := map[int64]time.Time{}
	rows, err := tx.QueryContext(ctx, `SELECT q.repo_id,q.set_id,ss.statistics,ss.observed
FROM sessions q JOIN repositories r ON r.id=q.repo_id
JOIN scale_sets ss ON ss.repo_id=q.repo_id AND ss.set_id=q.set_id
LEFT JOIN managed_sets m ON m.repo_id=q.repo_id
WHERE q.phase='active' AND r.eligible=1
AND (m.repo_id IS NULL OR (m.phase='active' AND m.remote_id=q.set_id))
ORDER BY q.repo_id,q.set_id`)
	if err != nil {
		return empty, err
	}
	var active []SetKey
	for rows.Next() {
		var key SetKey
		var statisticsJSON, observed string
		if err := rows.Scan(&key.RepositoryID, &key.ScaleSetID, &statisticsJSON, &observed); err != nil {
			rows.Close()
			return empty, err
		}
		if skip[key] {
			continue
		}
		var statistics *scaleset.RunnerScaleSetStatistic
		if err := json.Unmarshal([]byte(statisticsJSON), &statistics); err != nil || statistics == nil {
			rows.Close()
			return empty, errors.New("advertisement session lacks a durable demand observation")
		}
		if err := (Envelope{MessageID: 1, Statistics: *statistics}).validate(); err != nil {
			rows.Close()
			return empty, err
		}
		// THE JOURNAL IS THE FLOOR, THE STATISTIC IS ONLY A CEILING RAISE.
		//
		// scale_sets.statistics is written on session CREATE, on a refresh that
		// carries statistics, and on a poll envelope -- and github.com's refresh
		// carries none. After a resume, the counts are therefore frozen at
		// whatever the previous process last saw, and planCapacity hands headroom
		// strictly by them. A frozen non-zero count took the only slot every
		// sweep while the repository with real queued jobs was advertised 0 for
		// 100 minutes (2026-09-22).
		//
		// So demand starts from rows this controller can actually see, and a
		// statistic raises it only when it was observed since this process
		// started. A frozen statistic can no longer outrank an empty job table.
		//
		// AVAILABLE JOBS COUNT TOO, AND LEAVING THEM OUT WAS ITS OWN STARVATION.
		// A repository this pool has never advertised capacity for has NO
		// journaled rows and NO assigned jobs, because GitHub's broker assigns
		// only to a scale set that advertised. Its queued work exists solely as
		// TotalAvailableJobs in the poll envelope, so planning from journaled
		// rows plus TotalAssignedJobs read it as idle: three repositories each
		// held a job queued on slipway-linux for 45+ minutes while the pool sat at
		// admission=2 free=2 charged=0, reachable only through the one rotating
		// exploratory slot (2026-09-23).
		//
		// An available job is NOT ours and is never charged a slot: it raises
		// only what this sweep ADVERTISES, bounded by admission, and the fairness
		// pass hands out one slot per repository per pass, so several repositories
		// with available work share the headroom instead of one taking it all.
		//
		// AND THE FIVE-MINUTE WINDOW CANNOT APPLY TO AN AVAILABLE JOB, BECAUSE
		// AN AVAILABLE JOB IS EXACTLY WHAT STOPS RE-OBSERVING. A poll that finds
		// nothing returns 202 with no envelope, so `scale_sets.statistics` is
		// rewritten only when something happens -- and for a repository whose job
		// is queued against a scale set that has advertised nothing, nothing
		// happens. Its `available=1` is observed once and then ages out, after
		// which the planner sees zero and the job is reachable solely through the
		// single rotating exploratory slot: 48 subscriptions, one probe a sweep,
		// a ~45-minute discovery latency that reads as a stuck queue (2026-09-23).
		//
		// So the window guards the ASSIGNED count, which is ours and journal-
		// backed, and the AVAILABLE count buys ONE slot -- all GitHub needs
		// before it will assign. A stale available count therefore costs at most
		// what the exploratory slot already costs, and the fairness pass hands
		// one slot per repository per pass, so it cannot monopolise headroom the
		// way the frozen pre-rotation statistic did.
		//
		// NEITHER DOES THE LOWER BOUND APPLY TO IT, AND THAT IS THE SAME OUTAGE
		// ONE DOOR ALONG. A restart makes every prior observation INHERITED, and
		// a session RESUME does not rewrite `scale_sets.statistics` -- only a
		// create, a refresh carrying statistics, or an envelope does. A
		// repository whose queue GitHub will not describe until we advertise is
		// therefore refused again from the first sweep after every restart, and
		// heals only when the exploratory rotation reaches it. Measured
		// 2026-09-23 immediately after deploying the window half of this fix:
		// every one of 48 subscriptions logged
		// `reason=observed-before-this-process-started` and the pool sat at
		// `free=2 charged=0` with nine repositories holding queued `slipway-linux`
		// jobs.
		//
		// An unreadable instant is still refused. That one is a parse failure
		// rather than an old reading, and the fail-closed direction costs only
		// the exploratory slot this arm was sized against anyway.
		want := journaled[key.RepositoryID]
		// REST-observed queued jobs are added after occupancy is known, below.
		// A set whose poll failed gets none: nothing can be reserved for it.
		if rest := s.restDemandWant(key.RepositoryID, planningAt); rest > 0 && !withoutREST[key] {
			restQueued[key.RepositoryID] = rest
			restReadAt[key.RepositoryID] = s.restDemand[key.RepositoryID].observedAt
		}
		statistical := statistics.TotalAssignedJobs + statistics.TotalAvailableJobs
		freshness := classifyStatistics(observed, s.started, planningAt)
		if freshness == statisticsCurrent {
			want = max(want, statistical)
		} else if freshness != statisticsUnreadable && statistics.TotalAvailableJobs > 0 {
			want = max(want, 1)
		} else if statistical > want {
			// Only when the discarded statistic would actually have raised the
			// floor. A repository whose ignored counts are zero changes nothing,
			// and logging it every sweep would bury the case that matters.
			slog.Info("Ignored a scale-set demand statistic the planner cannot trust. "+
				"Planning from journaled job rows instead.",
				"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID,
				"reason", string(freshness),
				"statistic_total_assigned_jobs", statistics.TotalAssignedJobs,
				"statistic_total_available_jobs", statistics.TotalAvailableJobs,
				"journaled_demand", want, "observed", observed)
		}
		demand[key.RepositoryID] = want
		active = append(active, key)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return empty, err
	}
	if err := rows.Close(); err != nil {
		return empty, err
	}
	if len(active) != len(keys) {
		return empty, errors.New("advertisement sweep omitted or added an active session")
	}
	for i := range keys {
		if keys[i] != active[i] {
			return empty, errors.New("advertisement sweep omitted or added an active session")
		}
	}
	occupied := map[int64]int{}
	rows, err = tx.QueryContext(ctx, `SELECT repo_id,COUNT(*) FROM leases WHERE phase!='released' GROUP BY repo_id`)
	if err != nil {
		return empty, err
	}
	used := 0
	for rows.Next() {
		var repoID int64
		var count int
		if err := rows.Scan(&repoID, &count); err != nil {
			rows.Close()
			return empty, err
		}
		occupied[repoID] = count
		used += count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return empty, err
	}
	if err := rows.Close(); err != nil {
		return empty, err
	}
	// REST-OBSERVED QUEUED JOBS COUNT ON TOP OF THE REPOSITORY'S RUNNING WORK.
	//
	// Demand is a repository's desired TOTAL, busy workers included: planCapacity
	// starts each repository at its occupied count and adds slots while the
	// total is below demand. A REST count is jobs not yet running, so it is
	// added to occupancy, the way a current statistic's available count is
	// added to its assigned count. It used to be an absolute 1, so a repository
	// with one job running could never use a free slot for its next queued job
	// (homelab, 2026-09-25: advertised 1 for 13 cycles with 1-3 slots free and
	// 4 runs queued). It also covers the gap statistics cannot: a job queued
	// while the queue sessions were deaf, or during a restart, never produces
	// an envelope.
	//
	// A READING IS ONLY AS NEW AS ITS READ. It changes only on the next
	// successful read (every interval; trusted for two), so a job that was
	// queued at the read and has since been leased would count twice, once as
	// occupancy and once in the stale count, for up to two intervals. Every
	// lease created after the read is therefore taken off the count, whatever
	// its phase now: a job leased and already finished since the read is not
	// queued either. Both instants are this controller's clock (observedAt is
	// stamped when the read returns, lease `created` at insert). A lease
	// created while the read was in flight is not subtracted, so such a job
	// can still count twice until the next read. See classifyRESTDemand for
	// the freshness rule.
	for repoID, rest := range restQueued {
		since, err := leasesCreatedSince(ctx, tx, repoID, restReadAt[repoID], true)
		if err != nil {
			return empty, err
		}
		if rest -= since; rest > 0 {
			demand[repoID] = max(demand[repoID], occupied[repoID]+rest)
		}
	}
	usable := s.admission
	// This one IS an invariant: admission shrank underneath workers the journal
	// still charges, and no plan can describe that. Demand above admission is
	// the ordinary state of a shared pool and is deferred below instead.
	if used > usable {
		return empty, errors.New("journal occupancy exceeds current platform admission")
	}
	var cursor int64
	if err := tx.QueryRowContext(ctx, `SELECT advertise_repo FROM platform WHERE id=1`).Scan(&cursor); err != nil {
		return empty, err
	}
	plan, chosen, granted := planCapacity(keys, occupied, demand, usable-used, cursor)
	if granted {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE platform SET advertise_repo=? WHERE id=1`, chosen)); err != nil {
			return empty, err
		}
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return plan, nil
}

// statisticsFreshness is why a durable statistics observation may or may not be
// planned from. It is a string so the reason can go straight into the log line
// an operator reads.
type statisticsFreshness string

const (
	// statisticsCurrent is the only value the planner trusts.
	statisticsCurrent statisticsFreshness = "current"
	// statisticsInherited was observed before this process started, so it came
	// from the previous one and nothing has re-confirmed it.
	statisticsInherited statisticsFreshness = "observed-before-this-process-started"
	// statisticsAgedOut was observed by this process but too long ago.
	statisticsAgedOut statisticsFreshness = "older-than-the-staleness-window"
	// statisticsUnreadable has no usable instant at all.
	statisticsUnreadable statisticsFreshness = "observation-instant-missing-or-unparsable"
)

// classifyStatistics decides whether a durable statistics observation may raise
// the demand floor. It is pure so both edges of the window are table-testable.
//
// THE LOWER BOUND alone was not enough, and the gap is the same starvation shape
// one step further out. A poll that finds nothing returns HTTP 202 with no
// envelope, so `RecordEnvelope` never runs and `scale_sets.statistics` is not
// rewritten. An idle repository therefore stops re-observing, and a count that
// has since gone to zero stays believed for the life of the process: an
// `available=1` never re-observed keeps claiming a turn on alternate sweeps
// forever, taking slots from repositories with real work.
//
// So a statistic must be BOTH after this process's start AND recent. The window
// is staleAssignmentWindow, the same five minutes an unconfirmed broker
// assignment gets, and for the same reason: it is this pool's standing bound on
// how long an unproven claim may hold a resource. A repository with live work
// re-observes well inside it, because every accepted envelope rewrites the
// instant.
//
// The stored instant is RFC3339Nano, which trims trailing zeros, so a string
// comparison is wrong -- "…:00Z" sorts after "…:00.5Z" -- and the value is
// parsed instead. An empty or unparsable instant is not fresh, which is the
// fail-closed direction: the planner falls back to journaled rows.
func classifyStatistics(observed string, started, now time.Time) statisticsFreshness {
	if observed == "" {
		return statisticsUnreadable
	}
	at, err := time.Parse(time.RFC3339Nano, observed)
	if err != nil {
		return statisticsUnreadable
	}
	if at.Before(started) {
		return statisticsInherited
	}
	if now.Sub(at) > staleAssignmentWindow {
		return statisticsAgedOut
	}
	return statisticsCurrent
}

// journaledDemand counts, per repository, the work this controller can see and
// has not yet charged a slot for. It is the demand floor planCapacity plans
// from, and unlike a scale-set statistic it cannot freeze: every row in it was
// written by an envelope this journal recorded.
//
// 'assigned-stale' IS counted, but only WHILE IT IS YOUNG. Advertising capacity
// only asks GitHub for work; it never mints a runner, and advertising for a
// stale row is exactly what invites the broker reassignment that re-confirms
// it. Minting stays fail-closed in ReserveNext, where being wrong costs a
// runner.
//
// THE AGE BOUND IS THE WHOLE POINT. Unbounded, a job GitHub had re-routed --
// which no envelope will ever name again -- claimed the pool's demand forever:
// repo1=1 repo2=0 on ten consecutive sweeps, the starvation shape recreated by
// its own fix.
//
// THE ROW IS RETAINED, NOT DELETED, and that is not a preference.
// leases(repo_id,set_id,job_key) is a foreign key into jobs, leases are never
// deleted (only moved to phase 'released'), and foreign_keys is ON -- so
// deleting a stale row that any released lease still references raised
// `FOREIGN KEY constraint failed`, which Maintain returned and which aborts the
// whole cycle before polling, provisioning or draining. The pool stops
// entirely. Retaining is also what a later event of either shape resolves back
// to, and it sidesteps the mergeJobState rank wedge a terminal state would
// cause.
//
// Ages are compared in Go, never in SQL: jobs.offered is RFC3339Nano, which
// trims trailing zeros, so "…:00Z" sorts after "…:00.5Z". An unparsable or
// empty instant does not count, which is the fail-closed direction.
func journaledDemand(ctx context.Context, tx *sql.Tx, staleCutoff time.Time) (map[int64]int, error) {
	rows, err := tx.QueryContext(ctx, selectableJobsQuery(`j.repo_id,j.state,j.offered`, demandJobs, ``))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	demand := map[int64]int{}
	for rows.Next() {
		var repoID int64
		var state, offered string
		if err := rows.Scan(&repoID, &state, &offered); err != nil {
			return nil, err
		}
		if state == jobStateAssignedStale && !staleAssignmentCounts(offered, staleCutoff) {
			continue
		}
		demand[repoID]++
	}
	return demand, rows.Err()
}

// leasesCreatedSince counts one repository's leases, in any phase, created
// after at. The SQL bound is coarse (RFC3339Nano does not sort as text within
// one second, so it starts two seconds early); the exact test is in Go.
func leasesCreatedSince(ctx context.Context, tx *sql.Tx, repoID int64, at time.Time, withProbes bool) (int, error) {
	if at.IsZero() {
		return 0, nil
	}
	// A probe lease (unseen_demand.go) serves no queued job of its own, so the
	// unseen-demand reading leaves it out: counting it would make the REST
	// count look served and restart the unseen clock under its own probe.
	probes := ``
	if !withProbes {
		probes = ` AND job_key NOT LIKE '` + unseenJobKeyPrefix + `%'`
	}
	rows, err := tx.QueryContext(ctx, `SELECT created FROM leases WHERE repo_id=? AND created>=?`+probes,
		repoID, at.Add(-2*time.Second).UTC().Format("2006-01-02T15:04:05Z"))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var created string
		if err := rows.Scan(&created); err != nil {
			return 0, err
		}
		t, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return 0, fmt.Errorf("lease created time %q is unreadable: %w", created, err)
		}
		if t.After(at) {
			count++
		}
	}
	return count, rows.Err()
}

// staleAssignmentCounts reports whether an unconfirmed broker assignment is
// still young enough to advertise for.
func staleAssignmentCounts(offered string, cutoff time.Time) bool {
	if offered == "" {
		return false
	}
	at, err := time.Parse(time.RFC3339Nano, offered)
	return err == nil && at.After(cutoff)
}

// planCapacity is the whole allocation decision, with no I/O, so every case the
// live pool hits is table-testable. headroom is the admission left after the
// journal's charged leases, which are preserved first: a running worker is
// never un-planned. chosen is the repository that received the last slot, and
// granted says whether the durable cursor should move to it.
//
// demand is per repository and covers assigned AND available work, so a
// repository whose queued jobs GitHub has not assigned yet still competes.
func planCapacity(keys []SetKey, occupied, demand map[int64]int, headroom int, cursor int64) (AdvertisementPlan, int64, bool) {
	plan := AdvertisementPlan{Capacity: make(map[SetKey]int, len(keys))}
	for _, key := range keys {
		plan.Capacity[key] = occupied[key.RepositoryID]
	}
	order := rotateFrom(keys, cursor)
	var chosen int64
	granted := false
	// FIRST PASS: one slot per repository with unmet demand, in cursor order.
	//
	// DEMAND IS SERVED BEFORE THE EXPLORATORY SLOT. The probe used to compete in
	// this pass on equal terms, one repository per sweep in cursor order. With
	// headroom 1 and 48 repositories that walk almost always landed on an idle
	// one, so a repository with ~36 queued jobs and a fresh REST reading got one
	// slot about every 48 sweeps while the log showed `deferred=4` every cycle
	// (2026-09-24). Round-robin among the repositories with demand is kept: the
	// cursor moves to whoever was served, so the next sweep starts after it.
	for _, key := range order {
		if headroom == 0 {
			break
		}
		if plan.Capacity[key] >= demand[key.RepositoryID] {
			continue
		}
		plan.Capacity[key]++
		headroom--
		chosen, granted = key.RepositoryID, true
	}
	// EXPLORATORY SLOT: only once every repository with demand holds a slot.
	// Nothing journaled and nothing charged means the repository advertises zero,
	// and GitHub assigns nothing to a scale set that did not ask for capacity,
	// so without a probe it could never be offered work.
	//
	// The probe was once starved by demand outright (it ran after every demand
	// pass, so a one-slot pool with a backlog never probed). It no longer carries
	// discovery alone: the REST demand read (rest_demand.go) sees queued jobs on
	// every served repository whether or not it was advertised, and a fresh
	// reading puts that repository in the demand pass above. The probe remains
	// for a repository whose REST reading is missing or stale, and it runs
	// whenever headroom exceeds the repositories with unmet demand.
	//
	// The cursor follows the probe, so successive probes walk the idle
	// repositories instead of re-probing the first one after a busy neighbour.
	// Still at most ONE exploratory slot per sweep. Repeated polls must not
	// promise the same free slot to two repositories.
	explored := false
	for _, key := range order {
		if headroom == 0 {
			break
		}
		if plan.Capacity[key] != 0 || demand[key.RepositoryID] != 0 {
			continue
		}
		plan.Capacity[key]++
		headroom--
		chosen, granted, explored = key.RepositoryID, true, true
		break
	}
	// LATER PASSES: demand only. The exploratory slot is spent.
	for headroom > 0 {
		served := false
		for _, key := range order {
			if headroom == 0 {
				break
			}
			if plan.Capacity[key] >= demand[key.RepositoryID] {
				continue
			}
			plan.Capacity[key]++
			headroom--
			served = true
			if !explored {
				chosen, granted = key.RepositoryID, true
			}
		}
		if !served {
			break
		}
	}
	for _, key := range order {
		if unmet := demand[key.RepositoryID] - plan.Capacity[key]; unmet > 0 {
			plan.Deferred += unmet
		}
	}
	return plan, chosen, granted
}

// rotateFrom orders the sorted, repository-unique keys starting at the first
// repository after cursor, wrapping when the cursor is at or past the last one.
func rotateFrom(keys []SetKey, cursor int64) []SetKey {
	start := 0
	for i, key := range keys {
		if key.RepositoryID > cursor {
			start = i
			break
		}
	}
	return append(append([]SetKey(nil), keys[start:]...), keys[:start]...)
}
