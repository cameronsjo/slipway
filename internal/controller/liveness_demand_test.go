package controller

import (
	"testing"
	"time"
)

// #1011: with a fresh REST reading, silence is deaf only while pool jobs are
// queued, on the demand window, counted from the latest of the last envelope,
// the process start, and when demand appeared. With no fresh reading the idle
// rule is the fallback.
func TestSessionsLookDeafKeysOnQueuedDemand(t *testing.T) {
	const idle = 30 * time.Minute
	const demandWindow = 10 * time.Minute
	started := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	envelope := started.Add(5 * time.Minute)
	tests := []struct {
		name    string
		now     time.Time
		charged int
		demand  DemandSignal
		want    bool
	}{
		{"idle pool, fresh reading, nothing queued: never deaf, however long the silence",
			envelope.Add(6 * time.Hour), 0, DemandSignal{Known: true, Queued: 0, Window: demandWindow}, false},
		{"queued since before the last envelope, 10 min of silence: deaf",
			envelope.Add(demandWindow), 0, DemandSignal{Known: true, Queued: 3, Since: started, Window: demandWindow}, true},
		{"queued, 9m59s of silence: not yet",
			envelope.Add(demandWindow - time.Second), 0, DemandSignal{Known: true, Queued: 3, Since: started, Window: demandWindow}, false},
		{"demand appeared 1 min ago after an hour of idle silence: clock starts at demand, not deaf",
			envelope.Add(time.Hour), 0, DemandSignal{Known: true, Queued: 1, Since: envelope.Add(59 * time.Minute), Window: demandWindow}, false},
		{"demand appeared 10 min ago after an hour of idle silence: deaf",
			envelope.Add(time.Hour), 0, DemandSignal{Known: true, Queued: 1, Since: envelope.Add(50 * time.Minute), Window: demandWindow}, true},
		{"queued and silent, but a worker is active: never deaf",
			envelope.Add(time.Hour), 1, DemandSignal{Known: true, Queued: 3, Since: started, Window: demandWindow}, false},
		{"no fresh reading: idle fallback, 30 min of silence is deaf",
			envelope.Add(idle), 0, DemandSignal{}, true},
		{"no fresh reading: idle fallback, 29 min is not",
			envelope.Add(idle - time.Minute), 0, DemandSignal{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sessionsLookDeaf(envelope, started, tt.now, idle, tt.charged, tt.demand); got != tt.want {
				t.Errorf("sessionsLookDeaf = %t, want %t (demand %+v)", got, tt.want, tt.demand)
			}
		})
	}
}

func TestSessionRecoveryRungUsesTheDemandWindow(t *testing.T) {
	const idle = 30 * time.Minute
	started := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	demand := DemandSignal{Known: true, Queued: 2, Since: started, Window: 10 * time.Minute}
	cases := []struct {
		silence time.Duration
		fired   bool
		want    Rung
	}{
		{4 * time.Minute, false, RungNone},
		{5 * time.Minute, false, RungCloseConnections},
		{5 * time.Minute, true, RungNone},
		{10 * time.Minute, true, RungExit},
	}
	for _, c := range cases {
		if got := sessionRecoveryRung(started, started, started.Add(c.silence), idle, 0, c.fired, demand); got != c.want {
			t.Errorf("silence %s fired=%t: rung %q, want %q", c.silence, c.fired, got, c.want)
		}
	}
	quiet := DemandSignal{Known: true, Queued: 0, Window: 10 * time.Minute}
	if got := sessionRecoveryRung(started, started, started.Add(3*time.Hour), idle, 0, false, quiet); got != RungNone {
		t.Errorf("idle pool with a fresh zero reading: rung %q, want %q", got, RungNone)
	}
}

// demandSince starts when the fresh total goes positive, holds across further
// positive readings, and clears on a zero reading; a stale reading is not Known.
func TestRESTDemandSignalTracksWhenDemandAppeared(t *testing.T) {
	store := &Store{restDemand: map[int64]restDemandReading{}}
	t0 := time.Date(2026, 9, 26, 1, 0, 0, 0, time.UTC)
	fresh := 4 * time.Minute

	if got := store.RESTDemandSignal(t0); got.Known || got.Queued != 0 {
		t.Fatalf("before any read: %+v, want unknown and zero", got)
	}
	store.RecordRESTDemand(map[int64]int{1: 0, 2: 0}, t0, t0.Add(fresh))
	if got := store.RESTDemandSignal(t0); !got.Known || got.Queued != 0 || !got.Since.IsZero() {
		t.Fatalf("fresh zero read: %+v, want known, zero, no since", got)
	}
	t1 := t0.Add(2 * time.Minute)
	store.RecordRESTDemand(map[int64]int{1: 2, 2: 0}, t1, t1.Add(fresh))
	t2 := t1.Add(2 * time.Minute)
	store.RecordRESTDemand(map[int64]int{1: 1, 2: 3}, t2, t2.Add(fresh))
	if got := store.RESTDemandSignal(t2); !got.Known || got.Queued != 4 || !got.Since.Equal(t1) {
		t.Fatalf("demand held across reads: %+v, want known, 4, since %v", got, t1)
	}
	t3 := t2.Add(2 * time.Minute)
	store.RecordRESTDemand(map[int64]int{1: 0, 2: 0}, t3, t3.Add(fresh))
	if got := store.RESTDemandSignal(t3); got.Queued != 0 || !got.Since.IsZero() {
		t.Fatalf("drained: %+v, want zero and no since", got)
	}
	if got := store.RESTDemandSignal(t3.Add(fresh + time.Second)); got.Known {
		t.Fatalf("after the reading went stale: %+v, want unknown (idle fallback)", got)
	}
}

// The deaf verdict carries the silence it was judged on. A process that heard
// nothing at all has LastEnvelopeAge -1, which the exit line used to round to
// "for 0s" on every deaf exit of 2026-10-01; Silence is the real length.
func TestTheDeafSummaryCarriesTheSilenceItJudged(t *testing.T) {
	store, _ := openTestStore(t, 2)
	var calls []string
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls}, &cycleLifecycle{calls: &calls}, 0)
	if err != nil {
		t.Fatal(err)
	}
	daemon.store = store
	daemon.deafAfter = 30 * time.Minute
	daemon.demandDeafAfter = 10 * time.Minute
	now := time.Now().UTC()
	store.RecordRESTDemand(map[int64]int{1: 2}, now, now.Add(time.Hour))
	store.mu.Lock()
	store.started = now.Add(-12 * time.Minute)
	store.demandSince = now.Add(-20 * time.Minute)
	store.mu.Unlock()
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	summary := result.Summary
	if !result.SessionsDeaf || summary.LastEnvelopeAge >= 0 || !summary.SilenceJudged {
		t.Fatalf("want a deaf verdict with no envelope this process: deaf=%t age=%s judged=%t", result.SessionsDeaf, summary.LastEnvelopeAge, summary.SilenceJudged)
	}
	if summary.Silence < 12*time.Minute || summary.Silence > 13*time.Minute {
		t.Fatalf("silence = %s, want about 12m (process start is later than the demand)", summary.Silence)
	}
}
