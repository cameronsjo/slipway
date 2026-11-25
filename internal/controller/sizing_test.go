package controller

import (
	"bytes"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// sizedConfig is the production-shaped config for one platform instance, with
// only the two reconfigurable numbers varying.
func sizedConfig(platform string, capacity, legacy int) Config {
	return Config{Platform: platform, InstallationID: 42, Capacity: capacity,
		LegacySlots: legacy, RepositoryAllowlist: testAllowlist}
}

// readPlatformRow reads the durable row directly, with the store closed. The
// refusal tests assert the ROW, not just the error: a refusal that still wrote
// the new numbers would satisfy an error-only assertion.
func readPlatformRow(t *testing.T, dir string) (name string, installation int64, capacity, legacy int) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "pool.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT name,installation,capacity,legacy_slots FROM platform WHERE id=1`).
		Scan(&name, &installation, &capacity, &legacy); err != nil {
		t.Fatal(err)
	}
	return name, installation, capacity, legacy
}

func assertPlatformRow(t *testing.T, dir string, capacity, legacy int) {
	t.Helper()
	_, _, gotCapacity, gotLegacy := readPlatformRow(t, dir)
	if gotCapacity != capacity || gotLegacy != legacy {
		t.Fatalf("platform row is capacity=%d legacy_slots=%d, want %d and %d",
			gotCapacity, gotLegacy, capacity, legacy)
	}
}

// captureControllerLog swaps the process default logger for the duration of one
// test. No test in this package runs in parallel, so the swap is contained.
func captureControllerLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buffer
}

// seedIdleJournal builds a journal with discovered repositories and no lease,
// then closes it. That is the state a stopped, drained controller leaves.
func seedIdleJournal(t *testing.T, config Config) (string, SetKey) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	store, err := Open(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, store, 1)[0]
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, key
}

// A capacity or legacy-slot change is adopted on an idle journal, and the new
// number is immediately usable: the point of raising Linux to two workers is a
// second reservable slot, not a row that merely reads 2.
func TestIdleJournalAdoptsNewSizing(t *testing.T) {
	for _, tc := range []struct {
		name          string
		platform      string
		before, after Config
		wantPoolSlots int
	}{
		{name: "raise capacity", platform: "linux",
			before: sizedConfig("linux", 1, 0), after: sizedConfig("linux", 2, 0), wantPoolSlots: 2},
		{name: "lower capacity", platform: "linux",
			before: sizedConfig("linux", 2, 0), after: sizedConfig("linux", 1, 0), wantPoolSlots: 1},
		{name: "release the legacy slot", platform: "macos",
			before: sizedConfig("macos", 2, 1), after: sizedConfig("macos", 2, 0), wantPoolSlots: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, key := seedIdleJournal(t, tc.before)
			log := captureControllerLog(t)

			store, err := Open(dir, tc.after)
			if err != nil {
				t.Fatalf("an idle journal refused a sizing change: %v", err)
			}
			t.Cleanup(func() { store.Close() })
			assertPlatformRow(t, dir, tc.after.Capacity, tc.after.LegacySlots)
			if !strings.Contains(log.String(), "Reconciled platform sizing") {
				t.Fatalf("no INFO line names the sizing change: %q", log.String())
			}

			// The new ceiling has to be reservable, in both directions: one more
			// slot after a raise, and exactly one fewer after a lowering.
			if err := store.ReconcileInventory(ctx, true, nil); err != nil {
				t.Fatal(err)
			}
			offer(t, store, key, tc.wantPoolSlots+1)
			for n := 0; n < tc.wantPoolSlots; n++ {
				if _, err := store.ReserveNext(ctx); err != nil {
					t.Fatalf("slot %d of %d was not reservable after the change: %v", n, tc.wantPoolSlots, err)
				}
			}
			if _, err := store.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
				t.Fatalf("reserving past the new ceiling of %d returned %v, want ErrCapacity", tc.wantPoolSlots, err)
			}
		})
	}
}

// The same three changes, DEFERRED while the journal is busy. Refusing to open
// would deadlock the pool: the change arrives as a restart, and with no
// controller alive nothing can drive the running lease to 'released', so the
// journal could never become idle. The process must open on the journal's own
// sizing instead.
func TestBusyJournalDefersNewSizing(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after Config
		// occupy makes the journal non-quiescent and returns the phrase the WARN
		// must contain to name what it found.
		occupy func(t *testing.T, store *Store, key SetKey) string
		// wantRequested is the change the WARN must name.
		wantRequested string
		// wantPoolSlots is what stays reservable: always the journal's sizing.
		wantPoolSlots int
	}{
		{name: "raise capacity with an active lease",
			before: sizedConfig("linux", 1, 0), after: sizedConfig("linux", 2, 0),
			occupy: reserveOneLease, wantRequested: "capacity 1 -> 2", wantPoolSlots: 1},
		{name: "lower capacity with an active lease",
			before: sizedConfig("linux", 2, 0), after: sizedConfig("linux", 1, 0),
			occupy: reserveOneLease, wantRequested: "capacity 2 -> 1", wantPoolSlots: 2},
		{name: "release the legacy slot with an active lease",
			before: sizedConfig("macos", 2, 1), after: sizedConfig("macos", 2, 0),
			occupy: reserveOneLease, wantRequested: "legacy_slots 1 -> 0", wantPoolSlots: 1},
		{name: "raise capacity with a pending worker resource",
			before: sizedConfig("linux", 1, 0), after: sizedConfig("linux", 2, 0),
			occupy: leaveWorkerResourcePresent, wantRequested: "capacity 1 -> 2", wantPoolSlots: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			store, err := Open(dir, tc.before)
			if err != nil {
				t.Fatal(err)
			}
			key := prepare(t, store, 1)[0]
			offer(t, store, key, 4)
			wantActivity := tc.occupy(t, store, key)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			log := captureControllerLog(t)

			reopened, err := Open(dir, tc.after)
			if err != nil {
				t.Fatalf("a busy journal refused to open, which deadlocks the pool: %v", err)
			}
			t.Cleanup(func() { reopened.Close() })
			for _, want := range []string{"Deferred a platform sizing change", tc.wantRequested, wantActivity} {
				if !strings.Contains(log.String(), want) {
					t.Fatalf("the deferral WARN %q does not carry %q", log.String(), want)
				}
			}
			if strings.Contains(log.String(), "Reconciled platform sizing") {
				t.Fatalf("a deferred change still logged an adoption: %q", log.String())
			}
			// The row, not just the log: a deferral that wrote anyway would leave
			// the next restart reading a journal that disagrees with its leases.
			assertPlatformRow(t, dir, tc.before.Capacity, tc.before.LegacySlots)

			capacity, legacy := reopened.PlatformSlots()
			if capacity != tc.before.Capacity || legacy != tc.before.LegacySlots {
				t.Fatalf("effective sizing is %d/%d, want the journal's %d/%d",
					capacity, legacy, tc.before.Capacity, tc.before.LegacySlots)
			}
			pendingCapacity, pendingLegacy, pending := reopened.PendingSizing()
			if !pending || pendingCapacity != tc.after.Capacity || pendingLegacy != tc.after.LegacySlots {
				t.Fatalf("pending sizing is %d/%d (pending=%t), want %d/%d",
					pendingCapacity, pendingLegacy, pending, tc.after.Capacity, tc.after.LegacySlots)
			}

			// The OLD ceiling is what this process hands out, in both directions.
			if err := reopened.ReconcileInventory(ctx, true, nil); err != nil {
				t.Fatal(err)
			}
			charged, err := reopened.ActiveLeases(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for n := len(charged); n < tc.wantPoolSlots; n++ {
				if _, err := reopened.ReserveNext(ctx); err != nil {
					t.Fatalf("slot %d of the journal's %d was not reservable: %v", n, tc.wantPoolSlots, err)
				}
			}
			if _, err := reopened.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
				t.Fatalf("reserving past the journal's ceiling of %d returned %v, want ErrCapacity", tc.wantPoolSlots, err)
			}
		})
	}
}

// Once the pool drains, the deferred change becomes adoptable: the store latches
// out of new work, and a restart from the same directory adopts it. Without the
// drain signal the new numbers would wait for an unrelated restart forever.
func TestDrainedPendingSizingSignalsRestartAndThenAdopts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store, err := Open(dir, sizedConfig("linux", 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, store, 1)[0]
	// Exactly one job, so releasing its lease empties the offer queue too.
	// TestDrainWaitsForOfferedJobs covers a queue that is not empty.
	offer(t, store, key, 1)
	lease := mustReserve(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	deferred, err := Open(dir, sizedConfig("linux", 2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := deferred.ReconcileInventory(ctx, true, []string{lease.ID}); err != nil {
		t.Fatal(err)
	}
	if drain, err := deferred.DrainForPendingSizing(ctx); err != nil || drain {
		t.Fatalf("a busy journal signalled a restart: drain=%t err=%v", drain, err)
	}
	// The negative control above is what makes this positive one mean anything:
	// a signal that fired on every cycle would also fire here.
	finish(t, deferred, lease)
	drain, err := deferred.DrainForPendingSizing(ctx)
	if err != nil || !drain {
		t.Fatalf("an idle journal with a pending change did not signal a restart: drain=%t err=%v", drain, err)
	}
	if err := deferred.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := deferred.ReserveNext(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("a draining controller handed out a slot: %v", err)
	}
	if err := deferred.Close(); err != nil {
		t.Fatal(err)
	}

	log := captureControllerLog(t)
	adopted, err := Open(dir, sizedConfig("linux", 2, 0))
	if err != nil {
		t.Fatalf("the restart did not adopt the drained change: %v", err)
	}
	t.Cleanup(func() { adopted.Close() })
	if !strings.Contains(log.String(), "Reconciled platform sizing") {
		t.Fatalf("the adopting restart logged no INFO line: %q", log.String())
	}
	assertPlatformRow(t, dir, 2, 0)
	if _, _, pending := adopted.PendingSizing(); pending {
		t.Fatal("the adopted change is still reported as pending")
	}
}

// The journal's pair becomes EFFECTIVE sizing on a deferral, so it must clear
// the same platform ceiling the flags do. It is not trusted input: a journal
// written by another binary or another flag set (the inert bundle still carries
// -capacity=6) would otherwise hand out slots the host cannot support. Before
// the deferral path existed, such a journal refused to open, so failing closed
// here is no regression.
func TestJournalSizingAboveTheCeilingIsRefused(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store, err := Open(dir, sizedConfig("linux", 2, 0))
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, store, 1)[0]
	offer(t, store, key, 6)
	mustReserve(t, store) // Busy, so the reopen takes the deferral path.
	if _, err := store.db.Exec(`UPDATE platform SET capacity=17,legacy_slots=0 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir, sizedConfig("linux", 2, 0))
	if err == nil {
		capacity, legacy := reopened.PlatformSlots()
		reopened.Close()
		t.Fatalf("a journal above the ceiling became effective sizing %d/%d", capacity, legacy)
	}
	for _, want := range []string{"capacity=17", "ceiling of 16", "move the controller state directory aside"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not carry %q", err, want)
		}
	}
}

// An offered job with no lease yet postpones the restart. The lease and worker
// arms cannot see it: it is a row in `jobs`, not a charged slot. Exiting on it
// strands the job for the restart window -- launchd throttles to 60s -- and
// GitHub retries an unclaimed assignment only about three times.
func TestDrainWaitsForOfferedJobs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	store, err := Open(dir, sizedConfig("linux", 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	key := prepare(t, store, 1)[0]
	offer(t, store, key, 1)
	lease := mustReserve(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	deferred, err := Open(dir, sizedConfig("linux", 2, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { deferred.Close() })
	if err := deferred.ReconcileInventory(ctx, true, []string{lease.ID}); err != nil {
		t.Fatal(err)
	}
	// A second job arrives and is offered but never reserved, then the only
	// lease finishes. Every lease and worker row is now terminal.
	offer(t, deferred, key, 2)
	finish(t, deferred, lease)
	if leases, err := deferred.ActiveLeases(ctx); err != nil || len(leases) != 0 {
		t.Fatalf("the lease arm is not quiescent, so this test would pass for the wrong reason: %v %v", leases, err)
	}
	if drain, err := deferred.DrainForPendingSizing(ctx); err != nil || drain {
		t.Fatalf("the controller exited with an offered job unclaimed: drain=%t err=%v", drain, err)
	}

	// Draining the queue is what releases the restart. Without this control the
	// assertion above would also pass for a drain that never fires.
	if _, err := deferred.db.Exec(`UPDATE jobs SET state='terminal'`); err != nil {
		t.Fatal(err)
	}
	if drain, err := deferred.DrainForPendingSizing(ctx); err != nil || !drain {
		t.Fatalf("an empty queue and an idle journal did not release the restart: drain=%t err=%v", drain, err)
	}
}

// A store with no pending change never latches, whatever the journal looks like.
// Without this the drain could stop an ordinary controller at its first idle
// moment, which is every controller, most of the time.
func TestDrainNeverFiresWithoutAPendingChange(t *testing.T) {
	store, _ := openTestStore(t, 2)
	prepare(t, store, 1)
	if drain, err := store.DrainForPendingSizing(ctx); err != nil || drain {
		t.Fatalf("an idle store with no pending change signalled a restart: drain=%t err=%v", drain, err)
	}
}

// reserveOneLease holds a physical slot in the 'reserved' phase.
func reserveOneLease(t *testing.T, store *Store, key SetKey) string {
	t.Helper()
	mustReserve(t, store)
	return "1 lease(s) in phase reserved"
}

// leaveWorkerResourcePresent records a platform resource that still exists
// while every lease is released. The row is written directly because that
// combination is UNREACHABLE TODAY: FinishCleanup refuses without WorkerAbsent,
// so no supported path releases a lease whose worker is still present. The arm
// is defence in depth -- it costs one query and it removes the need to re-derive
// that implication every time the lifecycle changes.
func leaveWorkerResourcePresent(t *testing.T, store *Store, key SetKey) string {
	t.Helper()
	lease := mustReserve(t, store)
	finish(t, store, lease)
	if _, err := store.db.Exec(`INSERT INTO worker_resources(lease_id,resource_id,phase,created,startup_deadline,updated)
VALUES(?,?,'present',?,?,?)`, lease.ID, lease.RunnerName, timestamp(), timestamp(), timestamp()); err != nil {
		t.Fatal(err)
	}
	return "1 worker resource(s) in phase present"
}

// Identity is not sizing: a platform-name or installation mismatch means this
// state directory belongs to another pool, and no amount of idleness makes
// adopting its leases correct.
func TestIdentityMismatchStillRefuses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after Config
		want  []string
	}{
		{name: "platform name", after: sizedConfig("macos", 2, 0),
			want: []string{"platform identity differs", `journal has "linux"`, `configuration has "macos"`, "immutable"}},
		{name: "installation", after: Config{Platform: "linux", InstallationID: 43, Capacity: 2,
			RepositoryAllowlist: testAllowlist},
			want: []string{"installation identity differs", "journal has 42", "configuration has 43", "immutable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, _ := seedIdleJournal(t, sizedConfig("linux", 2, 0))
			reopened, err := Open(dir, tc.after)
			if err == nil {
				reopened.Close()
				t.Fatal("a journal was adopted by another pool identity")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal %q does not carry %q", err, want)
				}
			}
			name, installation, _, _ := readPlatformRow(t, dir)
			if name != "linux" || installation != 42 {
				t.Fatalf("a refused identity still rewrote the row: %q %d", name, installation)
			}
		})
	}
}

// An ordinary restart changes nothing and says nothing. Without this, the INFO
// line would fire on every boot and stop being a signal that sizing moved.
func TestUnchangedSizingIsASilentNoOp(t *testing.T) {
	config := sizedConfig("linux", 2, 0)
	dir, _ := seedIdleJournal(t, config)
	log := captureControllerLog(t)
	store, err := Open(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if log.Len() != 0 {
		t.Fatalf("an unchanged restart logged %q", log.String())
	}
	assertPlatformRow(t, dir, 2, 0)
}
