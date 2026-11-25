package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// UNSEEN DEMAND: WORK GITHUB REPORTS THAT NO ENVELOPE EVER NAMED.
//
// Every worker this controller starts used to need a queue envelope first: a
// JobAvailable or JobAssigned became a job row, and ReserveNext minted a worker
// only for a row. Statistics and the REST demand read only raised what a poll
// ADVERTISED. So when GitHub reported work but sent no envelope, the pool sat
// idle with free slots for as long as the silence lasted. On 2026-10-01 the
// REST read showed two queued slipway-linux jobs on one repository from 21:00 to
// 23:16 while every long-poll answered 202, ten restarts and 42-session
// rotations later (homelab#1216).
//
// Upstream's reference listener does not wait for envelopes at all: it sizes
// its runners from statistics.TotalAssignedJobs. This is the narrow version of
// that. When a set's reported demand exceeds everything this controller already
// holds for it, for unseenDemandAfter with no envelope from that set, one PROBE
// job row is journaled for it. The ordinary path then reserves a slot for the
// row, registers a JIT runner on that exact scale set and starts a worker, all
// bounded by admission, the provisioning breaker and the capacity wait like any
// other job.
//
// WHAT A PROBE CAN AND CANNOT DO. The runner is registered to the scale set, not
// to a job. If GitHub has work for that set, the runner can be handed it; the
// JobStarted and JobCompleted envelopes then advance the probe's lease by runner
// identity (settleRunnerEventTx), exactly as for a job GitHub moved to another
// worker. If GitHub has nothing for it, the runner idles and exits, and
// RetireTerminatedWorkers (or the five-minute unused-worker timeout) cleans it
// up. That a runner registered this way is offered a job GitHub has not
// announced is NOT proven live; the probe is cheap either way.
//
// THE BOUNDS. One probe per set at a time (probeLive), no new one within
// unseenProbeRetry of the last, none for a set that heard an envelope within
// unseenDemandAfter, none before this process has been up that long, and none
// after unseenProbeLimit probes with no envelope from the set in between. A
// probe row is never trusted across a restart (distrustUnseenProbes).

const (
	// unseenJobKeyPrefix is the job-key namespace for probe rows. It cannot
	// collide with an event's key: those are a decimal request ID or "job:"+jobId
	// (jobKey), and resolveJobKeyTx matches a probe row by neither, because a
	// probe row carries no runner_request_id and an empty job_id.
	unseenJobKeyPrefix = "unseen:"
	// unseenDemandAfter is how long reported demand must go without an envelope
	// from its set before a probe is journaled. A healthy set answers a queued
	// job in seconds; the REST read that reports it runs every five minutes.
	// It stays well inside the ten-minute demand deaf window, so a process gets
	// to try a probe before the watchdog restarts it.
	unseenDemandAfter = 3 * time.Minute
	// unseenProbeRetry is the least time between two probes for one set. A probe
	// that finds nothing costs one JIT registration and a worker that exits in
	// about thirty seconds; this bounds that cost for a repository whose queued
	// job this pool can never run.
	unseenProbeRetry = 10 * time.Minute
	// unseenProbeLimit is how many probes in a row a set gets with no envelope
	// from it in between. A probe GitHub hands a job produces envelopes (the
	// JobStarted at least), which reset the count; a probe that finds nothing
	// exits 0 and is cleaned up like any idle worker, so nothing else would ever
	// stop a set whose reported demand this pool cannot serve.
	unseenProbeLimit = 3
)

// isUnseenJobKey reports whether a job key names a probe row.
func isUnseenJobKey(key string) bool { return strings.HasPrefix(key, unseenJobKeyPrefix) }

// unseenDemand is one set's reported demand next to what the journal already
// holds for it. Probe rows and probe leases are left out of every count, so the
// clock measures real demand only; ProbeLive carries them instead.
type unseenDemand struct {
	// Assigned is a CURRENT statistic's TotalAssignedJobs (see
	// classifyStatistics); zero when the statistic is not current. GitHub counts
	// a job assigned to the set whether or not it is running yet.
	Assigned int
	// Queued is the fresh REST count of queued pool jobs, less the leases
	// created since that read. Jobs that are not running yet.
	Queued int
	// Reservable is journaled rows ReserveNext could take on this set.
	Reservable int
	// Starting is unreleased leases before running: reserved through
	// provisioning.
	Starting int
	// Running is leases in the running phase.
	Running int
	// ProbeLive is a probe row still offered, or one whose lease is unreleased.
	ProbeLive bool
}

// unseen is how much reported demand the journal does not account for. A
// statistic counts running jobs, so it is compared with everything held; the
// REST count does not, so it is compared with what has not started running.
func (d unseenDemand) unseen() int {
	held := d.Reservable + d.Starting
	return max(d.Assigned-(held+d.Running), d.Queued-held, 0)
}

// unseenClock is one set's process-local timing: when its unseen demand began,
// when it last delivered an envelope, and when it was last probed.
type unseenClock struct {
	Since        time.Time
	LastEnvelope time.Time
	LastProbe    time.Time
	// Probes is how many probes this set has had since its last envelope.
	Probes int
}

// unseenVerdict is what decideUnseenProbe decided for one set.
type unseenVerdict int

const (
	unseenWait unseenVerdict = iota
	unseenProbe
	// unseenExhausted is a set that would be probed but has had
	// unseenProbeLimit probes with no envelope in between.
	unseenExhausted
)

// decideUnseenProbe is the whole decision, with no I/O. It returns the clock's
// new Since (zero once nothing is unseen) and the verdict.
func decideUnseenProbe(demand unseenDemand, clock unseenClock, started, now time.Time) (time.Time, unseenVerdict) {
	if demand.unseen() == 0 {
		return time.Time{}, unseenWait
	}
	since := clock.Since
	if since.IsZero() || since.After(now) {
		since = now
	}
	if demand.ProbeLive {
		return since, unseenWait
	}
	// The quiet period starts at the latest of: the demand appearing, the set's
	// last envelope, and this process starting. A set GitHub is still talking
	// to is served by the envelope path; a process that just started has not
	// had a fair chance to hear anything.
	quiet := since
	for _, at := range []time.Time{clock.LastEnvelope, started} {
		if at.After(quiet) {
			quiet = at
		}
	}
	if now.Sub(quiet) < unseenDemandAfter {
		return since, unseenWait
	}
	if !clock.LastProbe.IsZero() && now.Sub(clock.LastProbe) < unseenProbeRetry {
		return since, unseenWait
	}
	if clock.Probes >= unseenProbeLimit {
		return since, unseenExhausted
	}
	return since, unseenProbe
}

// UnseenProbe is one probe row ProbeUnseenDemand journaled.
type UnseenProbe struct {
	Set    SetKey
	JobKey string
	Demand int
}

// ProbeUnseenDemand journals at most one probe row per candidate set whose
// reported demand has gone unseen for unseenDemandAfter (see the file comment).
// Candidates are the sets whose poll this cycle returned an empty 202: a set
// that delivered a message is being heard, and a set whose poll failed has no
// session to reserve through. It returns the probes it journaled.
func (s *Store) ProbeUnseenDemand(ctx context.Context, candidates []SetKey) ([]UnseenProbe, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return nil, nil
	}
	if s.unseenSince == nil {
		s.unseenSince = map[SetKey]time.Time{}
	}
	if s.unseenProbed == nil {
		s.unseenProbed = map[SetKey]time.Time{}
	}
	if s.unseenWarned == nil {
		s.unseenWarned = map[SetKey]bool{}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	var probes []UnseenProbe
	var journaled []func()
	for _, key := range candidates {
		demand, ok, err := s.unseenDemandTx(ctx, tx, key, now)
		if err != nil {
			return nil, err
		}
		if !ok {
			delete(s.unseenSince, key)
			continue
		}
		probed, err := consecutiveProbesTx(ctx, tx, key)
		if err != nil {
			return nil, err
		}
		clock := unseenClock{Since: s.unseenSince[key], LastEnvelope: s.setEnvelope[key], LastProbe: s.unseenProbed[key], Probes: probed}
		since, verdict := decideUnseenProbe(demand, clock, s.started, now)
		if since.IsZero() {
			delete(s.unseenSince, key)
		} else {
			s.unseenSince[key] = since
		}
		if verdict == unseenExhausted && !s.unseenWarned[key] {
			s.unseenWarned[key] = true
			slog.Warn("Stopped probing a scale set: its reported demand stayed unseen through every probe, and no envelope arrived from it in between. Probing resumes after its next envelope or a restart.",
				"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID,
				"probes", clock.Probes, "unseen", demand.unseen(), "statistic_total_assigned_jobs", demand.Assigned, "rest_queued", demand.Queued)
		}
		if verdict != unseenProbe {
			continue
		}
		jobKey := unseenJobKeyPrefix + uuid.NewString()
		stamp := now.Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(repo_id,set_id,job_key,runner_request_id,job_id,state,payload,offered) VALUES(?,?,?,0,'',?,'{}',?)`,
			key.RepositoryID, key.ScaleSetID, jobKey, jobStateQueued, stamp); err != nil {
			return nil, err
		}
		probes = append(probes, UnseenProbe{Set: key, JobKey: jobKey, Demand: demand.unseen()})
		journaled = append(journaled, func() {
			slog.Info("Reported demand had no envelope; journaled a probe job so a runner is registered on the scale set.",
				"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID,
				"unseen", demand.unseen(), "statistic_total_assigned_jobs", demand.Assigned, "rest_queued", demand.Queued,
				"reservable", demand.Reservable, "starting", demand.Starting, "running", demand.Running,
				"unseen_since", since.Format(time.RFC3339), "last_set_envelope", formatOptionalInstant(clock.LastEnvelope))
		})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// Logged only once the rows exist: a rolled-back sweep journaled nothing.
	for _, logLine := range journaled {
		logLine()
	}
	for _, probe := range probes {
		s.unseenProbed[probe.Set] = now
	}
	return probes, nil
}

// formatOptionalInstant renders an instant for a log field, "none" when zero.
func formatOptionalInstant(at time.Time) string {
	if at.IsZero() {
		return "none"
	}
	return at.UTC().Format(time.RFC3339)
}

// unseenDemandTx reads one set's demand. ok is false when the set has no active
// session on an eligible, verified managed set -- the same rule the advertisement
// planner applies -- so nothing is journaled for it.
func (s *Store) unseenDemandTx(ctx context.Context, tx *sql.Tx, key SetKey, now time.Time) (unseenDemand, bool, error) {
	var demand unseenDemand
	var statisticsJSON, observed string
	err := tx.QueryRowContext(ctx, `SELECT ss.statistics,ss.observed
FROM sessions q JOIN repositories r ON r.id=q.repo_id
JOIN scale_sets ss ON ss.repo_id=q.repo_id AND ss.set_id=q.set_id
LEFT JOIN managed_sets m ON m.repo_id=q.repo_id
WHERE q.repo_id=? AND q.set_id=? AND q.phase='active' AND r.eligible=1
AND (m.repo_id IS NULL OR (m.phase='active' AND m.remote_id=q.set_id))`,
		key.RepositoryID, key.ScaleSetID).Scan(&statisticsJSON, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		return demand, false, nil
	}
	if err != nil {
		return demand, false, err
	}
	var statistics *scaleset.RunnerScaleSetStatistic
	if json.Unmarshal([]byte(statisticsJSON), &statistics) == nil && statistics != nil &&
		classifyStatistics(observed, s.started, now) == statisticsCurrent {
		demand.Assigned = statistics.TotalAssignedJobs
	}
	if rest := s.restDemandWant(key.RepositoryID, now); rest > 0 {
		since, err := leasesCreatedSince(ctx, tx, key.RepositoryID, s.restDemand[key.RepositoryID].observedAt, false)
		if err != nil {
			return demand, false, err
		}
		demand.Queued = max(rest-since, 0)
	}
	// Nothing reported means nothing unseen, whatever the journal holds, so the
	// remaining counts are skipped. Nearly every quiet set takes this exit.
	if demand.Assigned == 0 && demand.Queued == 0 {
		return demand, true, nil
	}
	notProbe := ` AND j.job_key NOT LIKE '` + unseenJobKeyPrefix + `%'`
	if err := tx.QueryRowContext(ctx, selectableJobsQuery(`COUNT(*)`, reservableJobs, ` AND j.repo_id=? AND j.set_id=?`+notProbe),
		key.RepositoryID, key.ScaleSetID).Scan(&demand.Reservable); err != nil {
		return demand, false, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT
 COALESCE(SUM(phase IN ('reserved','acquire-intent','acquire-unknown','acquired','provisioning')),0),
 COALESCE(SUM(phase='running'),0)
FROM leases WHERE repo_id=? AND set_id=? AND job_key NOT LIKE '`+unseenJobKeyPrefix+`%'`,
		key.RepositoryID, key.ScaleSetID).Scan(&demand.Starting, &demand.Running); err != nil {
		return demand, false, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs j WHERE j.repo_id=? AND j.set_id=? AND j.job_key LIKE '`+unseenJobKeyPrefix+`%'
AND (j.state IN ('queued','assigned') OR EXISTS(SELECT 1 FROM leases l WHERE l.repo_id=j.repo_id AND l.set_id=j.set_id AND l.job_key=j.job_key AND l.phase!='released')))`,
		key.RepositoryID, key.ScaleSetID).Scan(&demand.ProbeLive); err != nil {
		return demand, false, err
	}
	return demand, true, nil
}

// distrustUnseenProbes retires every probe row a previous process journaled and
// nobody reserved, inside the transaction that opens the journal. A probe stands
// for demand that process measured, at that time; the new process measures its
// own and probes again if the demand is still unseen.
func distrustUnseenProbes(tx *sql.Tx) error {
	_, err := tx.Exec(`UPDATE jobs SET state='unavailable' WHERE job_key LIKE '`+unseenJobKeyPrefix+`%' AND state IN (?,?,?)`,
		jobStateQueued, jobStateAssigned, jobStateAssignedStale)
	return err
}

// recordSetEnvelope stamps the instant one set last delivered an envelope. The
// caller holds s.mu.
func (s *Store) recordSetEnvelope(key SetKey, at time.Time) {
	if s.setEnvelope == nil {
		s.setEnvelope = map[SetKey]time.Time{}
	}
	if at.After(s.setEnvelope[key]) {
		s.setEnvelope[key] = at
	}
	// An envelope is the set being heard, which is what a probe that got a job
	// produces. The consecutive-probe count resets with it on its own, because it
	// is read from the journal (consecutiveProbesTx); only the one-per-process
	// warning is cleared here.
	delete(s.unseenWarned, key)
}

// consecutiveProbesTx counts the probe rows journaled for a set since that
// set's last recorded envelope. It is read from the journal, not kept in
// memory, so a restart does not reset it: with fresh REST demand and no
// envelope the deaf watchdog restarts the process about every ten minutes,
// each process probed once, and a per-process count never reached
// unseenProbeLimit (homelab#1219). A message row is written for every accepted
// envelope, and messages are inserted in arrival order, so the newest row is
// the set's last envelope. Instants are compared in Go: RFC3339Nano does not
// sort as text. An unreadable instant counts the probe, the side that stops
// probing sooner.
func consecutiveProbesTx(ctx context.Context, tx *sql.Tx, key SetKey) (int, error) {
	var lastEnvelope time.Time
	var received string
	err := tx.QueryRowContext(ctx, `SELECT received FROM messages WHERE repo_id=? AND set_id=? ORDER BY rowid DESC LIMIT 1`,
		key.RepositoryID, key.ScaleSetID).Scan(&received)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return 0, err
	default:
		if lastEnvelope, err = time.Parse(time.RFC3339Nano, received); err != nil {
			return 0, fmt.Errorf("message received instant %q is unreadable: %w", received, err)
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT offered FROM jobs WHERE repo_id=? AND set_id=? AND job_key LIKE '`+unseenJobKeyPrefix+`%'`,
		key.RepositoryID, key.ScaleSetID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var offered string
		if err := rows.Scan(&offered); err != nil {
			return 0, err
		}
		at, parseErr := time.Parse(time.RFC3339Nano, offered)
		if parseErr != nil || at.After(lastEnvelope) {
			count++
		}
	}
	return count, rows.Err()
}
