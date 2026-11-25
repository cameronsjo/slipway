package controller

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

// rewindJournalVersion rewrites only the version pragma, leaving the rest of the
// journal intact. That is the shape an older binary's state directory presents:
// tables this binary's CREATE ... IF NOT EXISTS statements leave alone, and a
// version that says the rows inside are keyed on an identity that no longer
// exists.
func rewindJournalVersion(t *testing.T, s *Store, version int) {
	t.Helper()
	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, version)); err != nil {
		t.Fatal(err)
	}
	var actual int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != version {
		t.Fatalf("journal version after rewind = %d, want %d", actual, version)
	}
}

// The job-key re-key has no data migration on purpose: every jobs, leases and
// acquisition_evidence row in an older journal is keyed on a runner request ID
// that a broker-assigned job does not have. Opening one must refuse, and the
// refusal must say what to do about it -- a silent half-upgrade would leave
// leases whose job identity cannot be recomputed from anything on disk.
func TestMismatchedJournalIsRefusedWithTheRightRecovery(t *testing.T) {
	// An OLDER journal is disposable: its rows are keyed on an identity that no
	// longer exists, so moving it aside is the recovery. A NEWER one was written
	// by a controller that still understands it, so telling the operator to move
	// it aside would destroy live state -- the recovery there is the other
	// binary. One message for both cases is wrong for one of them.
	for _, tc := range []struct {
		name    string
		version int
		want    []string
		refuse  []string
	}{
		// Version 11 -- older than the oldest upgradable version. 12 and 13 are
		// the versions this binary UPGRADES (see journalUpgrades), so the refusal
		// case has to be older than the chain reaches; journalVersion-1 would
		// test the 13 -> 14 migration and call it a refusal.
		{name: "older", version: journalVersionStaleAssignments - 2,
			want:   []string{"cannot be upgraded", "move the controller state directory aside", "restart"},
			refuse: []string{"newer controller"}},
		{name: "newer", version: journalVersion + 1,
			want:   []string{"written by a newer controller", "run that binary", "do not move this one aside"},
			refuse: []string{"cannot be upgraded"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, dir := openTestStore(t, 2)
			keys := prepare(t, s, 1)
			offer(t, s, keys[0], 1)
			rewindJournalVersion(t, s, tc.version)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir, Config{
				Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist,
			})
			if err == nil {
				reopened.Close()
				t.Fatal("a mismatched journal was opened")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("refusal %q does not tell the operator to %q", err, want)
				}
			}
			for _, refuse := range tc.refuse {
				if strings.Contains(err.Error(), refuse) {
					t.Fatalf("refusal %q gives the other case's recovery (%q)", err, refuse)
				}
			}
		})
	}
}

// The upgrade chain, from EVERY one of its entry points. A version-12 journal
// runs 12 -> 13 -> 14 -> 15 in one open; a version-13 one starts at the second
// step, and a version-14 one runs only the last (the refused_envelopes table,
// homelab#1139). All must keep live work: refusing any would strand whatever
// leases it holds, which is why these steps are additive rather than a re-key.
//
// One fixture per entry point, because the chain is the thing at risk: a step
// missing from journalUpgrades refuses exactly the journals that start at or
// below it.
func TestOlderJournalsAreUpgradedInPlace(t *testing.T) {
	for _, from := range []int{12, 13, 14} {
		t.Run(fmt.Sprintf("from %d", from), func(t *testing.T) {
			s, dir := openTestStore(t, 2)
			keys := prepare(t, s, 1)
			offer(t, s, keys[0], 1)
			lease := mustReserve(t, s)
			// The shape a real pre-14 journal has: no archived/disabled columns.
			if from < 14 {
				for _, column := range []string{"archived", "disabled"} {
					if _, err := s.db.Exec(`ALTER TABLE repositories DROP COLUMN ` + column); err != nil {
						t.Fatal(err)
					}
				}
			}
			// The shape every pre-15 journal has: no refused_envelopes table.
			if _, err := s.db.Exec(`DROP TABLE refused_envelopes`); err != nil {
				t.Fatal(err)
			}
			rewindJournalVersion(t, s, from)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			upgraded, err := Open(dir, Config{
				Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist,
			})
			if err != nil {
				t.Fatalf("a version-%d journal was refused instead of upgraded: %v", from, err)
			}
			defer upgraded.Close()
			if current, err := upgraded.Lease(ctx, lease.ID); err != nil || current != lease {
				t.Fatalf("the upgrade lost a live lease: %+v %v", current, err)
			}
			var version int
			if err := upgraded.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != journalVersion {
				t.Fatalf("user_version after upgrade is %d (want %d): %v", version, journalVersion, err)
			}
			if _, err := upgraded.Repositories(ctx); err != nil {
				t.Fatalf("the upgraded journal cannot be read back: %v", err)
			}
			var tables int
			if err := upgraded.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='refused_envelopes'`).Scan(&tables); err != nil || tables != 1 {
				t.Fatalf("the upgraded journal has no refused_envelopes table: %d %v", tables, err)
			}
		})
	}
}

// TestRolledBackJournalRollsForwardWithItsRefusals pins the forward half of
// the documented rollback (PRAGMA user_version=14 so a version-14 image opens
// the journal). The version-14 image itself is not run here: that would need
// the pre-#1139 source built as a second binary. Its half rests on that source
// never naming refused_envelopes, so it leaves the table alone. What this pins
// is the return: a journal rewound to 14 that still holds the table and its
// rows reopens on this binary with every refusal intact, because the 14 -> 15
// step is a no-op and CREATE TABLE IF NOT EXISTS keeps the existing table.
func TestRolledBackJournalRollsForwardWithItsRefusals(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	session := uuid.New()
	shape := refusedShape{events: 2, available: 1, completed: 1}
	if _, err := s.RecordRefusal(ctx, key, session, 9, refusalForeignRepository, shape); err != nil {
		t.Fatal(err)
	}
	rewindJournalVersion(t, s, journalVersionRefusedEnvelopes-1)
	reopened := reopenTestStore(t, s, dir, 1)
	var version, refusals, events int
	var eventTypes string
	if err := reopened.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != journalVersion {
		t.Fatalf("user_version after rolling forward is %d (want %d): %v", version, journalVersion, err)
	}
	if err := reopened.db.QueryRow(`SELECT refusals,events,event_types FROM refused_envelopes WHERE session=? AND message_id=9`,
		session.String()).Scan(&refusals, &events, &eventTypes); err != nil || refusals != 1 || events != 2 || eventTypes != "JobAvailable,JobCompleted" {
		t.Fatalf("the refusal did not survive the rollback round trip: refusals=%d events=%d types=%q %v", refusals, events, eventTypes, err)
	}
	if got, err := reopened.RecordRefusal(ctx, key, session, 9, refusalForeignRepository, shape); err != nil || got != 2 {
		t.Fatalf("the count did not continue after rolling forward: %d %v", got, err)
	}
}

func TestCurrentJournalStillOpens(t *testing.T) {
	s, dir := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 1)
	// 11, not journalVersion-1: 12 and 13 are now upgraded rather than refused,
	// so rewinding one step would open cleanly and make the control below vacuous.
	rewindJournalVersion(t, s, journalVersionStaleAssignments-2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dir, Config{
		Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist,
	}); err == nil {
		reopened.Close()
		t.Fatal("the mismatched-journal refusal did not fire; the control below would be meaningless")
	}

	// The positive control: the same directory at the current version opens. A
	// refusal that fired on every journal would satisfy the assertion above.
	rewound, err := sql.Open("sqlite", filepath.Join(dir, "pool.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rewound.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, journalVersion)); err != nil {
		t.Fatal(err)
	}
	if err := rewound.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := Open(dir, Config{
		Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist,
	})
	if err != nil {
		t.Fatalf("a current journal was refused as well; the version check is not what refused: %v", err)
	}
	t.Cleanup(func() { current.Close() })
}

// B6: a request entering the queued state does get a durable
// instant, and a repeated offer for the same request keeps the first one — a
// reset would restart a wait that is already accumulating.
func TestOfferInstantIsRecordedOnceForAQueuedRequest(t *testing.T) {
	s, _ := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 1)

	var first string
	if err := s.db.QueryRow(`SELECT offered FROM jobs WHERE repo_id=? AND set_id=? AND job_key='1'`,
		keys[0].RepositoryID, keys[0].ScaleSetID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if first == "" {
		t.Fatal("a newly queued request carries no offer instant")
	}
	if got := offerInstant(jobStateQueued, jobStateQueued, first); got != first {
		t.Fatalf("a repeated offer moved the instant from %q to %q", first, got)
	}
	if got := offerInstant(jobStateRunning, jobStateQueued, first); got != "" {
		t.Fatalf("leaving the queued state kept an offer instant %q", got)
	}
}

// TestJournalTwelveUpgradesInPlace pins BOTH steps from the one version the
// deployed pilot journal is actually at. Version 13 adds the 'assigned-stale'
// jobs.state VALUE -- no column, no constraint -- so every row a version-12
// journal holds is already in a state 13 understands; version 14 then adds
// repositories.archived and repositories.disabled. The deployed journal must
// therefore be upgraded, not discarded: refusing it would demand a
// state-directory wipe for a change that rewrites no job row.
//
// The demotion assertion is why this test is separate from
// TestOlderJournalsAreUpgradedInPlace: a v12 journal carries broker assignments
// the restart cannot trust, and an upgrade that stamped the version without
// running distrustResumedAssignments is exactly the starvation #895 fixes.
func TestJournalTwelveUpgradesInPlace(t *testing.T) {
	const brokerJobID = "workflow-job-upgraded-1"
	s, dir := openTestStore(t, 2)
	keys := prepare(t, s, 1)
	offer(t, s, keys[0], 1)
	key := mustJobKey(t, event(keys[0], 1, scaleset.MessageTypeJobAvailable))
	brokerAssign(t, s, keys[0], brokerJobID, 2)
	// The shape a real version-12 journal has: no archived/disabled columns, so
	// the 13 -> 14 ALTER TABLE step has something to do.
	for _, column := range []string{"archived", "disabled"} {
		if _, err := s.db.Exec(`ALTER TABLE repositories DROP COLUMN ` + column); err != nil {
			t.Fatal(err)
		}
	}
	rewindJournalVersion(t, s, journalVersionStaleAssignments-1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(dir, Config{
		Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist,
	})
	if err != nil {
		t.Fatalf("a version-12 journal was refused rather than upgraded: %v", err)
	}
	t.Cleanup(func() { upgraded.Close() })

	var version int
	if err := upgraded.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != journalVersion {
		t.Fatalf("the journal opened but was left at version %d", version)
	}
	// The pre-existing row survived the upgrade and is still reservable, which
	// is what "no row is rewritten" has to mean in practice.
	if state := jobState(t, upgraded, keys[0], key); state != jobStateQueued {
		t.Fatalf("an existing job row did not survive the upgrade: state=%q", state)
	}
	// The 12 -> 13 step's whole effect: a broker assignment inherited across the
	// upgrade is demoted rather than re-served.
	if state := jobState(t, upgraded, keys[0], jobKeyStringPrefix+brokerJobID); state != jobStateAssignedStale {
		t.Fatalf("the upgrade left an inherited broker assignment trusted: state=%q", state)
	}
	// The 13 -> 14 step's: both columns are back and readable.
	if _, err := upgraded.db.Exec(`SELECT archived, disabled FROM repositories`); err != nil {
		t.Fatalf("the upgrade did not add the repository activity columns: %v", err)
	}
	if err := upgraded.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := upgraded.ReserveNext(ctx); err != nil {
		t.Fatalf("an upgraded journal could not serve its existing work: %v", err)
	}
}
