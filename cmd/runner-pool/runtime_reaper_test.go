package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
)

type countingReaper struct {
	calls atomic.Int32
	fail  bool
}

func (r *countingReaper) Reap(context.Context) (controller.ReapResult, error) {
	r.calls.Add(1)
	if r.fail {
		return controller.ReapResult{}, errors.New("inventory list failed")
	}
	return controller.ReapResult{Ran: true}, nil
}

// The reap loop runs on its own ticker, keeps going after a failed pass (the
// cycle still reaps on its own schedule), and stops with its context.
func TestTheReapLoopTicksSurvivesFailuresAndStops(t *testing.T) {
	r := &countingReaper{fail: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); runReaper(ctx, r, 5*time.Millisecond) }()
	deadline := time.After(2 * time.Second)
	for r.calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("reap passes after 2 s = %d, want at least 3 despite failures", r.calls.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the reap loop did not stop with its context")
	}
}

func TestTheReapIntervalDefaultsAndCanBeTurnedOff(t *testing.T) {
	base := RuntimeConfig{
		Platform: "linux", InstallationRepo: "cameronsjo/homelab", AppIDFile: "/run/app/id", AppKeyFile: "/run/app/key",
		StateDir: "/var/lib/runner-pool", Capacity: -1, LegacySlots: -1, BootstrapURL: "https://runner-pool.example:8443",
		BootstrapAddress: "127.0.0.1:8443", BootstrapCertificateFile: "/run/tls/cert", BootstrapKeyFile: "/run/tls/key",
		CycleInterval: time.Second, RepositoryAllowlist: []string{"cameronsjo/workout-tracker"},
	}
	got, err := base.Validate()
	if err != nil {
		t.Fatal(err)
	}
	if got.ReapInterval != defaultReapInterval {
		t.Fatalf("unset reap interval validated to %s, want the default %s", got.ReapInterval, defaultReapInterval)
	}
	off := base
	off.ReapInterval = -1
	if got, err = off.Validate(); err != nil || got.ReapInterval >= 0 {
		t.Fatalf("a negative reap interval must stay negative (reaper off): got %s, %v", got.ReapInterval, err)
	}
}
