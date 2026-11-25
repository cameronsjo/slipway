package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Run must not return while the background task (the REST demand tracker)
// is still running: the caller closes the store the task reads and writes as
// soon as Run returns. The task here outlives cancellation by a deliberate
// delay, standing in for an in-flight Refresh.
func TestDaemonRuntimeWaitsForTheBackgroundTaskBeforeReturning(t *testing.T) {
	cycle := &fakeRuntimeCycle{calls: make(chan struct{}, 16)}
	server := &fakeRuntimeServer{started: make(chan struct{})}
	runtime, err := newDaemonRuntime(cycle, server, time.Hour, nil)
	if err != nil {
		t.Fatalf("newDaemonRuntime() error = %v", err)
	}
	started := make(chan struct{})
	var finished atomic.Bool
	runtime.background = func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		finished.Store(true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() after cancellation error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
	if !finished.Load() {
		t.Fatal("Run() returned while the background task was still running")
	}
}
