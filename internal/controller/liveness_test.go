package controller

import (
	"testing"
	"time"
)

func TestSessionsLookDeaf(t *testing.T) {
	base := time.Date(2026, 9, 23, 4, 0, 0, 0, time.UTC)
	window := 30 * time.Minute

	tests := []struct {
		name           string
		lastEnvelopeAt time.Time
		processStarted time.Time
		now            time.Time
		charged        int
		want           bool
	}{
		{
			name:           "freshly started, no envelope yet, within window",
			lastEnvelopeAt: time.Time{},
			processStarted: base,
			now:            base.Add(10 * time.Minute),
			want:           false,
		},
		{
			name:           "freshly started, no envelope yet, window elapsed",
			lastEnvelopeAt: time.Time{},
			processStarted: base,
			now:            base.Add(30 * time.Minute),
			want:           true,
		},
		{
			name:           "envelope just under the window ago",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(29*time.Minute + 59*time.Second),
			want:           false,
		},
		{
			name:           "envelope exactly at the window",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(30 * time.Minute),
			want:           true,
		},
		{
			// The measured outage: 04:23:56Z to 13:50Z with nothing charged.
			name:           "envelope long past the window, nothing charged",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(9 * time.Hour),
			want:           true,
		},
		{
			name: "process started after the last envelope (restart resets the clock)",
			// A restarted process re-observes liveness from scratch; a stale
			// lastEnvelopeAt from a struct a caller reused must not out-rank a
			// later processStarted.
			lastEnvelopeAt: base.Add(-3 * time.Hour),
			processStarted: base,
			now:            base.Add(29 * time.Minute),
			want:           false,
		},
		{
			name:           "now before started is not deaf",
			lastEnvelopeAt: time.Time{},
			processStarted: base,
			now:            base.Add(-time.Minute),
			want:           false,
		},
		{
			// GitHub sends nothing between a job's start and its completion, so a
			// single job longer than the window is silence with a healthy session.
			// Killing the controller here would restart it mid-job.
			name:           "long-running job: window elapsed but a worker is charged",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(45 * time.Minute),
			charged:        1,
			want:           false,
		},
		{
			name:           "both slots busy past the window",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(3 * time.Hour),
			charged:        2,
			want:           false,
		},
		{
			// A failed journal read reports charged=-1. Unknown is not idle: a
			// kill decision must not rest on a reading that was not taken.
			name:           "unknown charged count is never judged deaf",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(9 * time.Hour),
			charged:        -1,
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sessionsLookDeaf(tt.lastEnvelopeAt, tt.processStarted, tt.now, window, tt.charged, DemandSignal{})
			if got != tt.want {
				t.Errorf("sessionsLookDeaf(last=%v, started=%v, now=%v, window=%v, charged=%d) = %t, want %t",
					tt.lastEnvelopeAt, tt.processStarted, tt.now, window, tt.charged, got, tt.want)
			}
		})
	}
}

func TestSessionRecoveryRung(t *testing.T) {
	base := time.Date(2026, 9, 23, 4, 0, 0, 0, time.UTC)
	window := 30 * time.Minute

	tests := []struct {
		name           string
		lastEnvelopeAt time.Time
		processStarted time.Time
		now            time.Time
		activeWorkers  int
		rungFired      bool
		want           Rung
	}{
		{
			name:           "fresh silence, well under half the window",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(5 * time.Minute),
			want:           RungNone,
		},
		{
			name:           "just under half the window",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(14*time.Minute + 59*time.Second),
			want:           RungNone,
		},
		{
			name:           "exactly half the window, rung not yet fired",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(15 * time.Minute),
			want:           RungCloseConnections,
		},
		{
			name:           "past half the window but the rung already fired this episode",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(20 * time.Minute),
			rungFired:      true,
			want:           RungNone,
		},
		{
			name:           "the full window still means exit, even if the rung fired",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(30 * time.Minute),
			rungFired:      true,
			want:           RungExit,
		},
		{
			// Mirrors sessionsLookDeaf: a charged worker suppresses every rung,
			// not just the exit -- closing idle connections on a healthy,
			// mid-job controller would achieve nothing worth the noise.
			name:           "a running job suppresses every rung, however long the silence",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(3 * time.Hour),
			activeWorkers:  1,
			want:           RungNone,
		},
		{
			// An unknown (negative) active count is treated as busy, same as
			// sessionsLookDeaf: a reading never taken must not authorize an action.
			name:           "an unknown active-worker count suppresses every rung",
			lastEnvelopeAt: base,
			processStarted: base.Add(-2 * time.Hour),
			now:            base.Add(3 * time.Hour),
			activeWorkers:  -1,
			want:           RungNone,
		},
		{
			name:           "now before started is not deaf",
			lastEnvelopeAt: time.Time{},
			processStarted: base,
			now:            base.Add(-time.Minute),
			want:           RungNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sessionRecoveryRung(tt.lastEnvelopeAt, tt.processStarted, tt.now, window, tt.activeWorkers, tt.rungFired, DemandSignal{})
			if got != tt.want {
				t.Errorf("sessionRecoveryRung(last=%v, started=%v, now=%v, window=%v, active=%d, fired=%t) = %q, want %q",
					tt.lastEnvelopeAt, tt.processStarted, tt.now, window, tt.activeWorkers, tt.rungFired, got, tt.want)
			}
		})
	}
}
