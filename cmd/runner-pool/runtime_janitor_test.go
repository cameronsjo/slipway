package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/controller"
	"github.com/cameronsjo/slipway/internal/githubapp"
)

func janitorBaseConfig() RuntimeConfig {
	return RuntimeConfig{
		Platform:                 "linux",
		InstallationRepo:         "cameronsjo/homelab",
		AppIDFile:                "/run/app/id",
		AppKeyFile:               "/run/app/key",
		StateDir:                 "/var/lib/runner-pool",
		Capacity:                 -1,
		LegacySlots:              -1,
		BootstrapURL:             "https://runner-pool.example:8443",
		BootstrapAddress:         "127.0.0.1:8443",
		BootstrapCertificateFile: "/run/tls/cert",
		BootstrapKeyFile:         "/run/tls/key",
	}
}

func TestRuntimeConfigQueueJanitorDefaultsAndFailsClosed(t *testing.T) {
	config, err := janitorBaseConfig().Validate()
	if err != nil {
		t.Fatalf("Validate(): %v", err)
	}
	if config.QueueJanitor != "off" || config.QueueJanitorInterval != defaultQueueJanitorInterval || config.queueJanitorRejected != "" {
		t.Fatalf("unset janitor is not off at the default interval: %+v", config)
	}
	for _, mode := range []string{"off", "dry-run", "enforce"} {
		base := janitorBaseConfig()
		base.QueueJanitor = mode
		config, err := base.Validate()
		if err != nil || config.QueueJanitor != mode || config.queueJanitorRejected != "" {
			t.Fatalf("mode %q: config=%q rejected=%q err=%v", mode, config.QueueJanitor, config.queueJanitorRejected, err)
		}
	}
	// An unknown value fails closed to off; it does not stop the controller.
	for _, mode := range []string{"on", "Enforce!", "dryrun"} {
		base := janitorBaseConfig()
		base.QueueJanitor = mode
		config, err := base.Validate()
		if err != nil || config.QueueJanitor != "off" || config.queueJanitorRejected != mode {
			t.Fatalf("mode %q: config=%q rejected=%q err=%v, want off and the value recorded", mode, config.QueueJanitor, config.queueJanitorRejected, err)
		}
	}
	base := janitorBaseConfig()
	base.QueueJanitorInterval = 30 * time.Second
	if _, err := base.Validate(); err == nil || !strings.Contains(err.Error(), "queue-janitor-interval") {
		t.Fatalf("an interval below the floor was accepted: %v", err)
	}
}

// Every way the janitor can be unconfigured leaves it off with exactly one
// log line, and never fails the build.
func TestBuildQueueJanitorIsOffWithOneLineWhenUnconfigured(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "APP_ID")
	if err := os.WriteFile(idFile, []byte("123"), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := githubapp.Installation{ID: 42}
	pool.Account.Login = "cameronsjo"
	for _, tc := range []struct {
		name string
		edit func(*RuntimeConfig)
		want string
	}{
		{"off", func(c *RuntimeConfig) { c.QueueJanitor = "off" }, "Queue janitor off."},
		{"an unknown mode", func(c *RuntimeConfig) { c.QueueJanitor = "on" }, "fails closed to off"},
		{"no janitor App configured", func(c *RuntimeConfig) { c.QueueJanitor = "dry-run" }, "no janitor GitHub App is configured"},
		{"one file missing from the config", func(c *RuntimeConfig) {
			c.QueueJanitor, c.JanitorAppIDFile, c.JanitorAppKeyFile = "enforce", idFile, filepath.Join(dir, "APP_PRIVATE_KEY")
		}, "no janitor GitHub App is configured"},
		// The optional Secret is absent: the paths are configured, the files
		// are not there.
		{"the Secret is absent", func(c *RuntimeConfig) {
			c.QueueJanitor = "dry-run"
			c.JanitorAppIDFile = filepath.Join(dir, "absent", "APP_ID")
			c.JanitorAppKeyFile = filepath.Join(dir, "absent", "APP_PRIVATE_KEY")
			c.JanitorInstallationIDFile = filepath.Join(dir, "absent", "INSTALLATION_ID")
		}, "could not be read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := janitorBaseConfig()
			tc.edit(&config)
			config, err := config.Validate()
			if err != nil {
				t.Fatalf("Validate(): %v", err)
			}
			var logged bytes.Buffer
			janitor, err := buildQueueJanitor(context.Background(), config, pool, nil, slog.New(slog.NewTextHandler(&logged, nil)))
			if err != nil || janitor == nil {
				t.Fatalf("buildQueueJanitor refused: %v", err)
			}
			if janitor.Enabled() {
				t.Fatal("an unconfigured janitor is enabled")
			}
			if lines := strings.Count(strings.TrimSpace(logged.String()), "\n") + 1; lines != 1 || !strings.Contains(logged.String(), tc.want) {
				t.Fatalf("want one line containing %q, got %d: %s", tc.want, lines, logged.String())
			}
		})
	}
}

// The janitor is a second background task beside the REST demand tracker;
// the combined task returns only once both have.
func TestRunConcurrentlyWaitsForEveryTask(t *testing.T) {
	var finished atomic.Int32
	task := func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(30 * time.Millisecond)
		finished.Add(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runConcurrently(task, task)(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runConcurrently did not return")
	}
	if finished.Load() != 2 {
		t.Fatalf("returned with %d of 2 tasks finished", finished.Load())
	}
}

// The disabled janitor is a real task: Run returns at once, so it never
// holds the daemon's shutdown open.
func TestADisabledJanitorTaskReturnsImmediately(t *testing.T) {
	janitor := controller.NewQueueJanitor(nil, nil, "linux", controller.QueueJanitorOff, time.Minute)
	done := make(chan struct{})
	go func() {
		runConcurrently(janitor.Run)(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a disabled janitor held its task open")
	}
}
