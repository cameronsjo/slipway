package controller

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

// A pending sizing change plus an idle journal stops the cycle before any
// session or provisioning work, and reports that the process should restart.
// Placing the check after Maintain is what lets the pool drain at all: Maintain
// is what drives finished leases to 'released'.
func TestCycleStopsForADrainedSizingChange(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	before := Config{Platform: "linux", InstallationID: 42, Capacity: 1, RepositoryAllowlist: testAllowlist}
	seed, err := Open(dir, before)
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, seed, 1)[0]
	offer(t, seed, key, 1)
	lease := mustReserve(t, seed)
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	after := before
	after.Capacity = 2
	store, err := Open(dir, after)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	var calls []string
	daemon, err := newDaemon(cycleRegistry{calls: &calls}, cycleSessions{calls: &calls, polls: 2},
		&cycleLifecycle{calls: &calls}, 6)
	if err != nil {
		t.Fatal(err)
	}
	daemon.store = store

	// Busy: the cycle runs to completion exactly as it did before.
	result, err := daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.RestartForSizing {
		t.Fatalf("a busy journal asked for a restart: calls=%v", calls)
	}
	if !slices.Contains(calls, "poll") {
		t.Fatalf("the busy cycle skipped its ordinary work: %v", calls)
	}
	if !result.Summary.SizingPending || result.Summary.PendingCapacity != 2 {
		t.Fatalf("the summary does not carry the pending change: %+v", result.Summary)
	}

	// Drained: the cycle stops after maintain, before sessions or provisioning.
	if err := store.ReconcileInventory(ctx, true, []string{lease.ID}); err != nil {
		t.Fatal(err)
	}
	finish(t, store, lease)
	calls = nil
	result, err = daemon.Cycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RestartForSizing {
		t.Fatalf("a drained pending change did not ask for a restart: calls=%v", calls)
	}
	want := []string{"refresh", "manage", "inventory", "maintain"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("the draining cycle did work past maintain: %v", calls)
	}
}
