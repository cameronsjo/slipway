package controller

import "time"

// sessionsLookDeaf decides whether this process's queue sessions have gone
// silent for long enough that they must be assumed dead rather than merely
// idle. A long-poll session that dies without erroring and one that is simply
// waiting for demand are indistinguishable from any local signal -- GitHub can
// hold a subscription open, ACK nothing, and never send a message again. The
// only thing that tells them apart is how long it has been since ANY
// subscription last produced a real envelope.
//
// lastEnvelopeAt is zero when this process has not yet received one. A
// freshly started process is given the full window from processStarted before
// it can be judged deaf, so a controller that has simply not been up for
// `window` yet is never flagged on that basis alone.
//
// NOTHING IS DEAF WHILE A WORKER IS CHARGED. GitHub sends an envelope only on
// a job event, so one job running longer than the window produces no envelope
// between its start and its completion -- and a healthy controller would be
// killed mid-job. A running worker talks to GitHub directly and needs nothing
// from the controller's sessions, so their silence costs nothing until the
// worker finishes. Once workersCharged drops to zero the clock applies again;
// the worst case is one job's length of added blindness.
//
// An UNKNOWN count (negative: the journal read failed this cycle) is treated as
// charged, not idle. Deciding to kill the process on a reading we could not
// take would turn a transient journal error into a restart of a controller that
// may be mid-job; the next cycle that reads cleanly decides instead.
//
// workersCharged MUST be the count of leases whose worker is plausibly running
// a job (Store.ActiveWorkerLeases), never every non-released lease
// (Store.ChargedSlots). A lease stuck in quarantined or acquire-unknown holds
// a slot without a worker doing anything for GitHub, and #912's original gate
// counted it anyway -- one stuck lease disabled the deaf check forever, on
// exactly the pool that was broken. See ActiveWorkerLeases for the phase set.
func sessionsLookDeaf(lastEnvelopeAt, processStarted, now time.Time, window time.Duration, workersCharged int, demand DemandSignal) bool {
	if workersCharged != 0 {
		return false
	}
	since, applied, judged := silenceClock(lastEnvelopeAt, processStarted, window, demand)
	if !judged {
		return false
	}
	return !now.Before(since.Add(applied))
}

// DemandSignal is the REST demand reading the deaf watchdog keys on (#1011).
//
// Known is true while at least one repository has a fresh REST reading. Queued
// is the total of fresh queued pool jobs; Since is when that total last went
// from zero to positive (zero when Queued is 0). Window is how long sessions
// may stay silent while jobs are queued before they count as deaf.
type DemandSignal struct {
	Known  bool
	Queued int
	Since  time.Time
	Window time.Duration
}

// silenceClock picks the clock and window a silent process is judged on.
//
// WITH A FRESH REST READING, SILENCE IS ONLY DEAF WHEN THERE IS WORK TO HEAR.
// An idle pool gets no envelopes because nothing happens, so the old idle-only
// rule restarted a healthy controller about once every 30 minutes (17 restarts
// in 18 h, 2026-09-25) while waiting the full 30 minutes in a real outage. Now
// nothing queued means not deaf, and queued jobs with no envelope for
// demand.Window (10 min) means deaf. The clock starts at the latest of the last
// envelope, the process start, and when demand appeared, so a job queued a
// minute ago cannot make an hour of idle silence count against the sessions.
//
// WITHOUT A FRESH READING (REST failing, or not read yet), the idle rule
// stands as the fallback, so a broken REST read cannot switch the watchdog off.
func silenceClock(lastEnvelopeAt, processStarted time.Time, window time.Duration, demand DemandSignal) (since time.Time, applied time.Duration, judged bool) {
	since = processStarted
	if lastEnvelopeAt.After(since) {
		since = lastEnvelopeAt
	}
	if !demand.Known || demand.Window <= 0 {
		return since, window, true
	}
	if demand.Queued <= 0 {
		return since, 0, false
	}
	if demand.Since.After(since) {
		since = demand.Since
	}
	return since, demand.Window, true
}

// Rung is which non-destructive-first recovery step applies to a process
// whose queue sessions have been silent. None means keep waiting; a silent
// process gets one intermediate action -- closing idle HTTP connections, to
// rule out stale keep-alive without a restart -- before the destructive one.
type Rung string

const (
	RungNone             Rung = "none"
	RungCloseConnections Rung = "close-connections"
	RungExit             Rung = "exit"
)

// sessionRecoveryRung decides which rung a silent process is at, using
// exactly the same charged/idle logic as sessionsLookDeaf: an active worker
// (see its doc) means nothing is deaf at any silence length. The idle clock
// has two thresholds instead of one: half the deaf window closes idle HTTP
// connections once per silent episode -- cause B in the 2026-09-23
// investigation, stale pooled HTTP/2 connections, is cheap to rule out before
// the full window's destructive exit, which stays exactly what #912 built.
//
// rungAlreadyFired says the close-connections rung already ran for the
// CURRENT silent episode. The caller resets it the moment a fresh envelope
// arrives (recordEnvelopeReceived's the trigger), so a process silent well
// past half the window is not asked to close its idle connections every
// cycle -- once per episode is the whole point of ruling something out.
func sessionRecoveryRung(lastEnvelopeAt, processStarted, now time.Time, window time.Duration, activeWorkers int, rungAlreadyFired bool, demand DemandSignal) Rung {
	if activeWorkers != 0 {
		return RungNone
	}
	since, applied, judged := silenceClock(lastEnvelopeAt, processStarted, window, demand)
	if !judged {
		return RungNone
	}
	silence := now.Sub(since)
	switch {
	case silence < 0:
		return RungNone
	case silence >= applied:
		return RungExit
	case silence >= applied/2 && !rungAlreadyFired:
		return RungCloseConnections
	default:
		return RungNone
	}
}
