// Package controller implements the shared pool's durable scheduling state.
// The API probe remains a separate command and never enables this controller.
package controller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// journalVersion is the only controller journal shape this binary can open.
// Version 12 keys jobs, leases and acquisition evidence on a job key rather
// than a runner request ID. Version 13 adds the 'assigned-stale' jobs.state
// value (homelab#895). Version 14 adds repositories.archived and
// repositories.disabled (homelab#896), which the choke-point refusal in
// githubapp.ScaleSetClient could not see before: the row carried only
// id/full_name/private, so both fields arrived false for every repository the
// journal reconstructed and the archived/disabled arm of that refusal was
// inert. Version 15 adds the refused_envelopes table (homelab#1139): the
// per-message refusal count that lets a queue envelope refused the same way
// three times be acknowledged and quarantined instead of blocking its session
// forever.
//
// From 12 these are ADDITIVE upgrades, unlike every earlier step: a new state
// value and columns with defaults, overwritten by the next complete discovery,
// so no row is lost and the alternative -- refusing the journal -- would strand
// its live leases.
//
// A NEW STATE VALUE NEEDS A VERSION BUMP EXACTLY LIKE A NEW COLUMN. jobs.state
// is TEXT with no CHECK, so a version-12 binary opens a version-12 journal
// holding 'assigned-stale' rows without complaint -- and then ranks that state
// at the unknown-state rank of 100 (jobStateRank), which no event can outrank.
// The job is wedged permanently under the UNIQUE(repo_id,set_id,job_key) key:
// it can never be reserved and a later event can never open a second row for
// it. The bump makes the older binary refuse to open the journal instead, which
// is the whole point of the version.
const journalVersion = 15

// journalVersionStaleAssignments is the version that introduced it.
const journalVersionStaleAssignments = 13

// journalVersionRefusedEnvelopes is the version that introduced the
// refused_envelopes table.
const journalVersionRefusedEnvelopes = 15

// journalUpgrades maps each upgradable version to the step that takes it to the
// NEXT one. A version absent from this map cannot be upgraded at all.
//
// AN EXPLICIT MAP, NOT `version == journalVersion-1`. That test accepted
// whatever happened to be one below the constant, so at journalVersion 14 a
// version-12 journal would still have been accepted and stamped 14 -- skipping
// the 12->13 step entirely, silently, with no row rewritten and no error. The
// map makes each step something a future version has to declare.
//
// 12 -> 13 adds the 'assigned-stale' jobs.state VALUE. No column, no
// constraint, and every existing row is already in a state 13 understands, so
// the step rewrites nothing; distrustResumedAssignments then runs as it does on
// every Open.
//
// 13 -> 14 adds repositories.archived and repositories.disabled with defaults,
// so it is an ALTER TABLE per missing column and the next complete discovery
// overwrites both.
//
// 14 -> 15 adds the refused_envelopes TABLE. Like 12 -> 13 the step itself
// rewrites nothing: the CREATE TABLE IF NOT EXISTS block in initialize creates
// the table, and no existing row changes meaning.
var journalUpgrades = map[int]func(*sql.Tx) error{
	journalVersionStaleAssignments - 1: func(*sql.Tx) error { return nil },
	journalVersionStaleAssignments:     addRepositoryActivityColumns,
	journalVersionRefusedEnvelopes - 1: func(*sql.Tx) error { return nil },
}

// upgradePath is the ordered list of versions whose steps must run to carry
// `from` up to `to`, or an error naming the first version with no step. Pure, so
// the skipped-step case is testable without a hypothetical journal on disk.
func upgradePath(from, to int) ([]int, error) {
	if from == 0 || from == to {
		return nil, nil
	}
	if from > to {
		return nil, fmt.Errorf("controller journal version %d is newer than %d", from, to)
	}
	var path []int
	for version := from; version < to; version++ {
		if _, ok := journalUpgrades[version]; !ok {
			return nil, fmt.Errorf("controller journal version %d cannot be upgraded to %d: no migration step is declared for version %d; stop the daemon, move the controller state directory aside, and restart to build a new journal",
				from, to, version)
		}
		path = append(path, version)
	}
	return path, nil
}

var (
	ErrInventoryUnknown = errors.New("worker inventory has not been reconciled")
	ErrCapacity         = errors.New("no platform capacity available")
	ErrNoDemand         = errors.New("no eligible queued demand")
	ErrDiscoveryStale   = errors.New("repository discovery is missing, failed, stale, or clock-invalid")
	// ErrAbsenceNotProven means ConfirmWorkerAbsent was called without both
	// physical process and storage absence proven -- ordinarily because the
	// worker is still tearing down. It is the one Maintain retirement outcome
	// that must NOT fail the whole cycle: see orchestrator.go's destroy loop
	// (homelab#911). A caller that only widens this to any ConfirmWorkerAbsent
	// error would also swallow a genuine store defect (e.g. the identity/phase
	// mismatch changedOne reports), so match this sentinel exactly.
	ErrAbsenceNotProven = errors.New("physical worker process and storage absence are both required")
	// ErrWitnessRefusedBeforeCreate means the host witness refused the
	// inventory a backend Create reads BEFORE it marks a create attempt or sends
	// anything that could make a worker. A backend may wrap it only at that
	// point, because ProvisionNext reads it as "nothing was created" and rolls
	// the worker intent back to absent, exactly as it does for ErrCapacity.
	// Unlike ErrCapacity it is still a cycle error: a refusal is logged and
	// retried, never swallowed as an orderly stop (homelab#1213).
	ErrWitnessRefusedBeforeCreate = errors.New("host witness refused the inventory read before worker creation; nothing was created")
)

type Config struct {
	Platform       string
	InstallationID int64
	Capacity       int
	LegacySlots    int
	// RepositoryAllowlist names every repository that may receive a pool scale
	// set, as lowercase owner/name. It is not journal identity -- changing it
	// must not demand a journal reconcile, and its effect is already recorded
	// in the repositories.eligible column -- so initialize compares the four
	// persisted fields rather than the whole struct.
	//
	// Empty means every private, active repository in the App installation --
	// the operator decision that onboarding a private repo must not need a
	// homelab PR. It is not "no filter": private-only, archived, disabled and
	// owner refusals are unconditional. See eligibleForPool.
	RepositoryAllowlist []string
	// MaxRepositories bounds how many repositories may hold a pool scale set,
	// and therefore how many queue sessions one controller polls per cycle.
	// Zero adopts defaultMaxRepositories. It exists because an empty allowlist
	// makes the served set track the account rather than a reviewed list.
	MaxRepositories int
}

const (
	// defaultMaxRepositories is the cap an unset MaxRepositories adopts. Eighty
	// covers every private repository in the account (~70) with headroom, which
	// is the point: a cap BELOW the eligible set leaves repositories refused
	// forever with no symptom except a job that queues, so the default must not
	// be the thing that silently decides which repos the pool serves.
	defaultMaxRepositories = 80
	// maxRepositoriesCeiling is the highest cap this controller will start with.
	// It bounds GROWTH -- how many repositories may be onboarded -- and the poll
	// sweep is what makes a number this large worth stating: each served
	// repository is one concurrent long poll per cycle.
	maxRepositoriesCeiling = 120
)

// ManagementCycle is what the last Reconcile decided about onboarding. It is
// process-local and derived, like the admission fence beside it: a restart
// re-derives it from the next cycle.
type ManagementCycle struct {
	// Creating is how many repositories entered the scale-set creation path.
	Creating int
	// CreateDeferred is how many new repositories the per-cycle creation budget
	// held back. Non-zero means onboarding is in progress, not that anything failed.
	CreateDeferred int
	// OverCap is how many eligible new repositories MaxRepositories refused.
	// Non-zero is an operator decision waiting to be made, and it persists
	// across cycles until the cap is raised or a repository goes private-inactive.
	OverCap int
	// OverCapFirstName is the full name of the first refusal. A repository ID is
	// not something an operator can act on without a second lookup; the name is.
	OverCapFirstName string
	// Orphaned is how many repositories lost eligibility while still owning a
	// scale set, and OrphanedFirstName names the first. The controller never
	// deletes the remote resource (see Store.markSetRemoval), so these persist
	// across cycles and restarts until an operator clears it by hand -- which is
	// exactly why the count exists: nothing else anywhere mentions it.
	Orphaned          int
	OrphanedFirstName string
}

func (s *Store) recordManagementCycle(cycle ManagementCycle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.management = cycle
}

// ManagementCycle reports the last Reconcile's onboarding decision for the
// per-cycle operator line.
func (s *Store) ManagementCycle() ManagementCycle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.management
}

type Store struct {
	db        *sql.DB
	lock      *os.File
	config    Config
	allowlist map[string]bool
	mu        sync.Mutex
	ready     bool // Deliberately false on every process start, never restored from disk.
	admission int  // Total current commitments allowed by a trusted inventory observation.
	// retiredUnused is cumulative since process start, like every other counter
	// the discovery report publishes. A rate is the difference between two
	// reports, and a restart legitimately zeroes it.
	retiredUnused int
	// eventShape is the opt-in wire-shape diagnostic, nil when disabled. It is
	// read from the environment once here at Open, never per event, and it is
	// deliberately not reachable from any published report.
	eventShape *slog.Logger
	// capacityWait latches when a provisioning attempt was deferred for
	// headroom, and clears when an inventory observes at least one free slot.
	//
	// DERIVED AND PROCESS-LOCAL, NOT JOURNALED, and that is deliberate. It
	// describes a condition of the HOST right now, exactly like `ready` and
	// `admission` beside it, and those reset on restart on purpose. A journaled
	// latch would outlive the shortage that set it and would need its own
	// expiry and clearing rules -- a second thing to get wrong, guarding
	// against a bug that does not exist yet. A restart simply re-observes the
	// host, which is the same answer the latch was holding.
	capacityWait bool
	// observation is the last backend inventory this process saw.
	// ReconcileBackendInventory persists none of it, and persisting it would be
	// wrong: the admission fence it belongs to resets on restart too.
	observation InventoryObservation
	// pending is the sizing this process was configured with but could not adopt,
	// because the journal was busy at Open. Nil is the ordinary case. While it is
	// set, config.Capacity and config.LegacySlots carry the JOURNAL's numbers, so
	// every live reader of s.config already sees the effective sizing.
	//
	// Written once during initialize, before Open returns, and read-only after.
	pending *sizingRequest
	// management is the last Reconcile's onboarding decision, for the cycle line.
	management ManagementCycle
	// draining latches when a deferred change becomes adoptable. It closes
	// ReserveNext so no new slot is handed out between that decision and the
	// process exiting for its supervisor to restart.
	draining bool
	// started is when this process opened the journal. It is the freshness line
	// for a scale set's durable statistics: an observation older than this one
	// was written by a previous process and describes demand nobody has
	// re-observed since. Process-local and never journaled, like ready,
	// admission and capacityWait beside it -- it is a fact about THIS process.
	started time.Time
	// lastEnvelope is when this process last recorded a queue envelope from ANY
	// subscription -- the one true signal GitHub is still talking to it. Zero
	// means none yet this process. Process-local and never journaled, like
	// started beside it: a session restart re-observes liveness from scratch,
	// it does not inherit a previous process's silence or chatter.
	lastEnvelope time.Time
	// terminated is the lease set whose backend worker was last observed with a
	// finished runner container, keyed by lease ID and carrying its exit code.
	// Replaced wholesale by each trusted inventory, so a worker that comes back
	// is no longer in it. Process-local for the same reason `observation` is.
	terminated map[string]int
	// vanished tracks present workers that complete inventories stopped
	// listing, and vanishedProven is the subset missing for long enough to
	// retire (homelab#1094). Process-local and replaced with each accepted
	// inventory, like terminated. See trackVanishedWorkers.
	vanished       map[string]vanishedWorker
	vanishedProven map[string]bool
	// restDemand is the last REST-observed queued-pool-job reading per
	// repository ID. Process-local and never journaled, exactly like started
	// and lastEnvelope above: a REST reading is a fact about now, gathered
	// independently of the queue sessions, and a restart re-observes it from
	// scratch rather than trusting what a previous process last saw. See
	// classifyRESTDemand for how a reading here becomes advertised demand.
	restDemand map[int64]restDemandReading
	// demandSince is when the fresh REST total last went from zero to
	// positive; zero while nothing is queued. The deaf watchdog starts its
	// clock here (#1011). Guarded by mu, like restDemand.
	demandSince time.Time
	// setEnvelope, unseenSince and unseenProbed are the per-set clocks
	// ProbeUnseenDemand decides on: when each set last delivered an envelope,
	// when its unseen demand began, and when it was last probed. Process-local
	// and guarded by mu, like lastEnvelope.
	setEnvelope  map[SetKey]time.Time
	unseenSince  map[SetKey]time.Time
	unseenProbed map[SetKey]time.Time
	// unseenWarned records the one WARN per process at unseenProbeLimit; the
	// count itself is journaled (consecutiveProbesTx, homelab#1219).
	unseenWarned map[SetKey]bool
}

// sizingRequest is the capacity and legacy-slot count the flags asked for.
type sizingRequest struct {
	capacity    int
	legacySlots int
}

// platformCeiling is the qualification ceiling per platform. Linux is sixteen:
// each Kata VM charges 30.5GiB of thin pool (three 10GiB writable disks plus
// 0.5GiB, homelab#983), so sixteen simultaneous workers charge 488GiB of the
// 576GiB pool homelab#983 grew to, and the worker /27 has sixteen slot
// addresses, .2 to .17 (homelab#960). A platform absent from this map is not
// servable, which is why zero means refuse rather than unlimited.
func platformCeiling(platform string) int {
	return map[string]int{"linux": 16, "macos": 2}[platform]
}

// validSizing is the ceiling rule, on its own so BOTH pairs are held to it: the
// flags at Open, and the journal's own pair before a deferral makes it the
// effective sizing. Only the flags used to be checked, and a journal row is not
// trusted input -- it was written by whatever binary and flags ran last, and the
// inert manifest bundle still carries -capacity=6.
func validSizing(platform string, capacity, legacySlots int) bool {
	ceiling := platformCeiling(platform)
	return ceiling > 0 && capacity >= 0 && capacity <= ceiling && legacySlots >= 0 && legacySlots <= capacity
}

func Open(directory string, config Config) (*Store, error) {
	if config.InstallationID <= 0 || !validSizing(config.Platform, config.Capacity, config.LegacySlots) {
		return nil, errors.New("invalid platform, installation, or qualification capacity")
	}
	allowlist, err := parseRepositoryAllowlist(config.RepositoryAllowlist)
	if err != nil {
		return nil, err
	}
	if config.MaxRepositories == 0 {
		config.MaxRepositories = defaultMaxRepositories
	}
	if config.MaxRepositories < 1 || config.MaxRepositories > maxRepositoriesCeiling {
		return nil, fmt.Errorf("max-repositories must be 1..%d; it bounds how many repositories the pool may onboard, and each one it serves is a concurrent long poll every cycle", maxRepositoriesCeiling)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("controller state directory must be private (0700) and not a symlink")
	}
	openPrivate := func(name string) (*os.File, error) {
		path := filepath.Join(directory, name)
		fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if err != nil {
			return nil, err
		}
		file := os.NewFile(uintptr(fd), path)
		stat, err := file.Stat()
		if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 {
			file.Close()
			return nil, errors.New("controller state file must be private and regular")
		}
		return file, nil
	}
	lock, err := openPrivate("pool.lock")
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another controller holds the platform allocation lock")
	}
	file, err := openPrivate("pool.sqlite")
	if err != nil {
		lock.Close()
		return nil, err
	}
	file.Close()
	abs, err := filepath.Abs(directory)
	if err != nil {
		lock.Close()
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(abs, "pool.sqlite")}
	u.RawQuery = url.Values{"_pragma": {"journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)", "busy_timeout(5000)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, lock: lock, config: config, allowlist: allowlist,
		started: time.Now().UTC(), terminated: map[string]int{}, restDemand: map[int64]restDemandReading{}}
	if eventShapeLogEnabled(os.Getenv(eventShapeLogVariable)) {
		s.eventShape = slog.Default()
	}
	if err := s.initialize(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return errors.Join(s.db.Close(), s.lock.Close()) }

// addRepositoryActivityColumns is the 13 -> 14 upgrade. It adds only what is
// missing: an ALTER for an existing column is an error, and a journal that
// already carries one of the two (an interrupted upgrade) must still finish
// rather than refuse forever.
func addRepositoryActivityColumns(tx *sql.Tx) error {
	rows, err := tx.Query(`PRAGMA table_info(repositories)`)
	if err != nil {
		return err
	}
	present := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		present[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, column := range []string{"archived", "disabled"} {
		if present[column] {
			continue
		}
		if _, err := tx.Exec(`ALTER TABLE repositories ADD COLUMN ` + column + ` INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) initialize() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	// Journal version 12 re-keys jobs, leases and acquisition evidence from the
	// runner request ID to a job key, because GitHub's broker assigns a job with
	// no request ID at all. There is deliberately NO data migration: every row
	// in an older journal is keyed on an identity that no longer exists, and
	// only one pilot host holds state, which is disposable. An older journal is
	// refused with the recovery it needs rather than silently half-upgraded.
	if version > journalVersion {
		// Moving this journal aside would destroy state a NEWER controller wrote
		// and still understands. The recovery is the other binary, not this one.
		return fmt.Errorf("controller journal version %d was written by a newer controller than this one (%d): run that binary, or restore an older state directory; do not move this one aside",
			version, journalVersion)
	}
	// 12 -> 13 IS A REAL MIGRATION AND THE PILOT JOURNAL IS NOT DISCARDED.
	// Version 13 adds a jobs.state VALUE, not a column or a constraint, so
	// nothing in a version-12 journal has to be rewritten: every existing row is
	// already in a state 13 understands. The upgrade is the version write below,
	// and distrustResumedAssignments then demotes the inherited broker
	// assignments as it does on every Open.
	//
	// The bump still matters in the other direction -- a version-13 journal is
	// refused by a version-12 binary above, which is what stops that binary
	// wedging an 'assigned-stale' row it cannot rank.
	//
	// 13 -> 14 is the ALTER TABLE step, and it runs BEFORE the CREATE TABLE
	// block below so a journal written by an older binary opens with its live
	// leases intact.
	//
	// 14 -> 15 adds the refused_envelopes table. The CREATE TABLE IF NOT EXISTS
	// block below creates it, so the declared step is a no-op, and a version-14
	// binary never reads the table.
	//
	// Every step between the journal's version and this binary's must be
	// declared, and they run in order. The pilot journal is upgraded in place
	// rather than discarded whenever a path exists.
	path, err := upgradePath(version, journalVersion)
	if err != nil {
		return err
	}
	for _, step := range path {
		if err := journalUpgrades[step](tx); err != nil {
			return fmt.Errorf("controller journal upgrade from version %d failed: %w", step, err)
		}
		slog.Info("Applied a controller journal upgrade step.", "from", step, "to", step+1, "target", journalVersion)
	}
	_, err = tx.Exec(`
CREATE TABLE IF NOT EXISTS platform (
 id INTEGER PRIMARY KEY CHECK(id=1), name TEXT NOT NULL, installation INTEGER NOT NULL,
 capacity INTEGER NOT NULL, legacy_slots INTEGER NOT NULL, cursor_repo INTEGER NOT NULL DEFAULT 0,
 advertise_repo INTEGER NOT NULL DEFAULT 0, provisioning_quarantine_until TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS repositories (
 id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, private INTEGER NOT NULL,
 eligible INTEGER NOT NULL, discovered TEXT NOT NULL,
 archived INTEGER NOT NULL DEFAULT 0, disabled INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS scale_sets (
 repo_id INTEGER PRIMARY KEY REFERENCES repositories(id), set_id INTEGER NOT NULL CHECK(set_id>0),
 statistics TEXT NOT NULL DEFAULT 'null', observed TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS messages (
 repo_id INTEGER NOT NULL, set_id INTEGER NOT NULL, session TEXT NOT NULL, message_id INTEGER NOT NULL,
 payload TEXT NOT NULL, received TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(repo_id,set_id,session,message_id));
CREATE TABLE IF NOT EXISTS refused_envelopes (
 repo_id INTEGER NOT NULL, set_id INTEGER NOT NULL, session TEXT NOT NULL,
 message_id INTEGER NOT NULL CHECK(message_id>0), reason TEXT NOT NULL,
 refusals INTEGER NOT NULL CHECK(refusals>0), events INTEGER NOT NULL DEFAULT 0 CHECK(events>=0),
 event_types TEXT NOT NULL DEFAULT '', first_refused TEXT NOT NULL, last_refused TEXT NOT NULL,
 acked TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(repo_id,set_id,session,message_id));
CREATE TABLE IF NOT EXISTS jobs (
 ordinal INTEGER PRIMARY KEY AUTOINCREMENT, repo_id INTEGER NOT NULL, set_id INTEGER NOT NULL,
 job_key TEXT NOT NULL CHECK(job_key!=''), runner_request_id INTEGER NOT NULL DEFAULT 0,
 job_id TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL, payload TEXT NOT NULL,
 offered TEXT NOT NULL DEFAULT '', UNIQUE(repo_id,set_id,job_key));
CREATE TABLE IF NOT EXISTS leases (
 id TEXT PRIMARY KEY, slot INTEGER NOT NULL, repo_id INTEGER NOT NULL, set_id INTEGER NOT NULL,
 job_key TEXT NOT NULL, runner_request_id INTEGER NOT NULL DEFAULT 0,
 phase TEXT NOT NULL, runner_name TEXT NOT NULL UNIQUE,
 created TEXT NOT NULL, updated TEXT NOT NULL, diagnostic TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(repo_id,set_id,job_key) REFERENCES jobs(repo_id,set_id,job_key));
CREATE UNIQUE INDEX IF NOT EXISTS one_live_slot ON leases(slot) WHERE phase!='released';
CREATE UNIQUE INDEX IF NOT EXISTS one_live_job ON leases(repo_id,set_id,job_key) WHERE phase!='released';
CREATE TABLE IF NOT EXISTS sessions (
 owner TEXT PRIMARY KEY, repo_id INTEGER NOT NULL, set_id INTEGER NOT NULL,
 session_id TEXT NOT NULL DEFAULT '', phase TEXT NOT NULL,
 created TEXT NOT NULL, updated TEXT NOT NULL, queue_sha256 BLOB NOT NULL DEFAULT X'');
CREATE UNIQUE INDEX IF NOT EXISTS one_open_session ON sessions(repo_id,set_id) WHERE phase!='closed';
CREATE TABLE IF NOT EXISTS discovery_state (
 id INTEGER PRIMARY KEY CHECK(id=1), last_success TEXT NOT NULL DEFAULT '',
 last_attempt TEXT NOT NULL DEFAULT '', diagnostic TEXT NOT NULL DEFAULT '');
INSERT OR IGNORE INTO discovery_state(id) VALUES(1);
CREATE TABLE IF NOT EXISTS managed_sets (
 repo_id INTEGER PRIMARY KEY REFERENCES repositories(id), name TEXT NOT NULL UNIQUE,
 remote_id INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL, diagnostic TEXT NOT NULL DEFAULT '', updated TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jit_transfers (
 lease_id TEXT PRIMARY KEY REFERENCES leases(id), token_hash BLOB NOT NULL,
 phase TEXT NOT NULL, runner_id INTEGER NOT NULL DEFAULT 0, revoked INTEGER NOT NULL DEFAULT 0,
 removal TEXT NOT NULL DEFAULT '', diagnostic TEXT NOT NULL DEFAULT '',
 created TEXT NOT NULL, expires TEXT NOT NULL, updated TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS worker_resources (
 lease_id TEXT PRIMARY KEY REFERENCES leases(id), resource_id TEXT NOT NULL UNIQUE,
 phase TEXT NOT NULL, created TEXT NOT NULL, startup_deadline TEXT NOT NULL,
 destroy_started TEXT NOT NULL DEFAULT '', cleanup_deadline TEXT NOT NULL DEFAULT '',
 diagnostic TEXT NOT NULL DEFAULT '', updated TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS provisioning_failures (
 id INTEGER PRIMARY KEY AUTOINCREMENT, lease_id TEXT NOT NULL REFERENCES leases(id),
 occurred TEXT NOT NULL, diagnostic TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS acquisition_evidence (
 lease_id TEXT PRIMARY KEY REFERENCES leases(id), job_key TEXT NOT NULL,
 outcome TEXT NOT NULL CHECK(outcome IN ('declined','accepted','completed')),
 source TEXT NOT NULL, runner_lease_id TEXT NOT NULL DEFAULT '', runner_id INTEGER NOT NULL DEFAULT 0, runner_name TEXT NOT NULL DEFAULT '',
 observed TEXT NOT NULL, diagnostic TEXT NOT NULL DEFAULT '');
`)
	if err != nil {
		return err
	}
	// Every pre-12 migration step was removed with the job-key re-key: an older
	// journal is refused above rather than upgraded, so none of them can run.
	if _, err := tx.Exec(`PRAGMA user_version=` + strconv.Itoa(journalVersion)); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT OR IGNORE INTO platform(id,name,installation,capacity,legacy_slots) VALUES(1,?,?,?,?)`,
		s.config.Platform, s.config.InstallationID, s.config.Capacity, s.config.LegacySlots)
	if err != nil {
		return err
	}
	var existing Config
	if err := tx.QueryRow(`SELECT name,installation,capacity,legacy_slots FROM platform WHERE id=1`).Scan(
		&existing.Platform, &existing.InstallationID, &existing.Capacity, &existing.LegacySlots); err != nil {
		return err
	}
	// The four persisted fields, not the whole struct: RepositoryAllowlist is
	// deployment configuration rather than journal identity, and its effect is
	// already recorded per repository in the repositories.eligible column.
	//
	// Two of the four are IDENTITY and two are SIZING, and they are not the same
	// kind of thing. Platform and installation say which pool this journal is;
	// adopting another instance's leases would be a correctness failure with no
	// recovery, so a mismatch always refuses. Capacity and legacy slots say how
	// big the pool is, which is an operator decision that legitimately changes --
	// and until this split existed there was no way to change it at all, because
	// the refusal named a "reconcile configuration explicitly" that no flag, mode
	// or document provided. Raising the Linux pool from one worker to two
	// crash-looped the controller on exactly that message.
	if existing.Platform != s.config.Platform {
		return fmt.Errorf("journal platform identity differs: journal has %q, configuration has %q; platform identity is immutable, so this state directory belongs to another pool instance",
			existing.Platform, s.config.Platform)
	}
	if existing.InstallationID != s.config.InstallationID {
		return fmt.Errorf("journal installation identity differs: journal has %d, configuration has %d; installation identity is immutable, so this state directory belongs to another App installation",
			existing.InstallationID, s.config.InstallationID)
	}
	if err := s.reconcilePlatformSizing(tx, existing); err != nil {
		return err
	}
	if err := distrustResumedAssignments(tx); err != nil {
		return err
	}
	if err := distrustUnseenProbes(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// distrustResumedAssignments demotes every broker-assigned job this process
// inherited, inside the same transaction that opens the journal, so nothing can
// read the trusted state first.
//
// WHY ONLY runner_request_id=0. A queued job is claimed through AcquireJobs,
// which asks GitHub and gets an answer, so a restart costs nothing: a job
// GitHub gave away is declined and the reservation released. A broker-assigned
// job has no request to acquire, so AcquireReserved settles it ACCEPTED on the
// controller's own inference (see AcquisitionSourceBrokerAssignment). Across a
// restart that inference is unfounded -- the assignment may have timed out and
// been re-routed, or the job may have finished -- and acting on it mints a JIT
// runner GitHub then offers nothing. Measured 2026-09-22: workers minted about
// every four minutes, each connecting, idling and exiting 0.
//
// WHAT REVIVES A STALE ROW. A fresh envelope naming the same job, and nothing
// else. There is no per-job confirmation API: AcquireJobs needs a positive
// request ID, and an acknowledged message is never redelivered because the
// durable cursor moved past it. A broker reassignment retry arrives as a new
// JobAssigned and outranks 'assigned-stale' (see jobStateRank); a JobCompleted
// outranks it too and leaves it terminal, which is how "the job is gone" is
// recorded. The row is kept rather than deleted so a later event of either
// shape still resolves to it instead of opening a second row.
// THE STAMP IS WHAT BOUNDS THE ROW'S DEMAND, and it is written here because
// this is the instant the assignment became unproven. jobs.offered already
// means "when this row entered the state it is in", so it carries the stale
// clock with no new column. journaledDemand ignores a stale row older than
// staleAssignmentWindow; the row itself is RETAINED, because deleting it
// violates the leases foreign key (a released lease still references the job)
// and aborts every Maintain, and because a retained row is what a later event
// resolves back to.
//
// A row already 'assigned-stale' keeps its ORIGINAL stamp: the UPDATE matches
// only 'assigned'. That is deliberate -- re-stamping on each Open would let a
// crash-looping controller keep a phantom counted forever.
func distrustResumedAssignments(tx *sql.Tx) error {
	result, err := tx.Exec(`UPDATE jobs SET state=?,offered=? WHERE state=? AND runner_request_id=0`,
		jobStateAssignedStale, timestamp(), jobStateAssigned)
	if err != nil {
		return err
	}
	demoted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if demoted == 0 {
		return nil
	}
	slog.Info("Distrusted broker-assigned jobs inherited from a previous controller process. "+
		"They cannot be reserved until a fresh envelope re-confirms or completes them.",
		"jobs", demoted, "state", jobStateAssignedStale)
	return nil
}

// sizingChange is one reconfigurable platform column whose journal value and
// configured value disagree. Neither is a secret: both are small integers the
// manifest and the plist already state in the clear.
type sizingChange struct {
	field      string
	journal    int
	configured int
}

func (c sizingChange) String() string {
	return fmt.Sprintf("%s %d -> %d", c.field, c.journal, c.configured)
}

// reconcilePlatformSizing adopts a new capacity or legacy-slot count onto a
// QUIESCENT journal, inside the caller's migration transaction so a deferral
// leaves the row exactly as it was.
//
// IT NEVER REFUSES TO OPEN. Refusing would deadlock the pool: the change
// arrives as a restart, so a job running at that moment leaves a non-released
// lease, the new process would exit, the supervisor would restart it, and with
// no controller alive nothing drives that lease to 'released' -- FinishCleanup
// is its only writer. The journal could never become idle again. So a busy
// journal DEFERS: this process runs on the journal's own sizing, and the daemon
// drains and exits once the pool goes idle, letting the restart adopt it.
//
// Quiescent means nothing durable is sized by the old numbers:
//   - no lease outside its one terminal phase, 'released'. Every other phase in
//     the Phase constants -- reserved, acquire-intent, acquire-unknown,
//     acquired, provisioning, running, cleanup-pending, quarantined -- holds a
//     physical slot, because Transition never releases one and FinishCleanup is
//     the only writer of 'released'.
//   - no worker_resources row outside its one terminal phase, 'absent'. The
//     other three -- create-intent, present, destroy-intent -- each describe a
//     platform resource that exists or may exist.
//   - consequently no slot index held, which is the condition a LOWERING needs:
//     the live-slot set is exactly the non-released leases (see the partial
//     index one_live_slot), so refusing on those refuses any lowering that would
//     otherwise strand a slot above the new ceiling. Slots are never truncated.
//
// A live JIT transfer needs no separate arm: the lifecycle releases a lease only
// once its registration is recorded absent, so a live transfer implies a
// non-released lease, which the first arm already catches.
func (s *Store) reconcilePlatformSizing(tx *sql.Tx, existing Config) error {
	var changes []sizingChange
	if existing.Capacity != s.config.Capacity {
		changes = append(changes, sizingChange{"capacity", existing.Capacity, s.config.Capacity})
	}
	if existing.LegacySlots != s.config.LegacySlots {
		changes = append(changes, sizingChange{"legacy_slots", existing.LegacySlots, s.config.LegacySlots})
	}
	if len(changes) == 0 {
		// Unchanged configuration is a no-op that writes nothing and says
		// nothing: every ordinary restart takes this path.
		return nil
	}
	activity, err := journalActivity(tx)
	if err != nil {
		return err
	}
	described := make([]string, len(changes))
	for i, change := range changes {
		described[i] = change.String()
	}
	if len(activity) > 0 {
		// The requested numbers are remembered and the EFFECTIVE ones become the
		// journal's, before anything reads s.config. Both directions are safe
		// while deferred: a pending raise keeps the old, smaller ceiling, and a
		// pending lowering keeps the old, larger one, which the live leases were
		// already allocated against.
		// The journal's pair is about to become the EFFECTIVE sizing, so it has to
		// clear the same ceiling the flags did. It is not trusted input: it was
		// written by whatever binary and flags ran last, and the inert manifest
		// bundle still carries -capacity=6. Without this a journal row of 17 with
		// flags of 2 would hand out seventeen concurrent slots under a ceiling of
		// sixteen.
		// Fail closed -- before this branch existed, such a journal refused to
		// open at all, so refusing is no worse than the behaviour it replaces.
		if !validSizing(s.config.Platform, existing.Capacity, existing.LegacySlots) {
			return fmt.Errorf("journal platform sizing is capacity=%d legacy_slots=%d, which exceeds the %s ceiling of %d or holds back more slots than exist; this journal cannot be run as-is, so move the controller state directory aside and restart to build a new one",
				existing.Capacity, existing.LegacySlots, s.config.Platform, platformCeiling(s.config.Platform))
		}
		s.pending = &sizingRequest{capacity: s.config.Capacity, legacySlots: s.config.LegacySlots}
		s.config.Capacity, s.config.LegacySlots = existing.Capacity, existing.LegacySlots
		slog.Warn("Deferred a platform sizing change because the pool is not idle. Requested: "+strings.Join(described, ", ")+
			". Effective sizing is the journal's until the pool drains. Active: "+strings.Join(activity, "; "),
			"platform", s.config.Platform,
			"effective_capacity", existing.Capacity, "requested_capacity", s.pending.capacity,
			"effective_legacy_slots", existing.LegacySlots, "requested_legacy_slots", s.pending.legacySlots)
		return nil
	}
	if _, err := tx.Exec(`UPDATE platform SET capacity=?,legacy_slots=? WHERE id=1`,
		s.config.Capacity, s.config.LegacySlots); err != nil {
		return err
	}
	slog.Info("Reconciled platform sizing on an idle journal. Changes: "+strings.Join(described, ", "),
		"platform", s.config.Platform,
		"journal_capacity", existing.Capacity, "configured_capacity", s.config.Capacity,
		"journal_legacy_slots", existing.LegacySlots, "configured_legacy_slots", s.config.LegacySlots)
	return nil
}

// PendingSizing reports a sizing change this process could not adopt at Open.
// While it is set, s.config carries the JOURNAL's numbers, not the flags'.
func (s *Store) PendingSizing() (capacity, legacySlots int, pending bool) {
	if s.pending == nil {
		return 0, 0, false
	}
	return s.pending.capacity, s.pending.legacySlots, true
}

// DrainForPendingSizing latches this controller out of new work once a deferred
// sizing change can finally be adopted, and reports whether the process should
// now exit so its supervisor restarts it onto the idle-journal path.
//
// The quiescence read and the latch happen under the SAME mutex ReserveNext
// takes, so no reservation can slip in between deciding the pool is idle and
// closing it: a concurrent ReserveNext either runs first (and the check then
// sees its lease) or waits and finds the latch set.
//
// THE DRAIN PREDICATE IS STRICTER THAN OPEN'S. Open only has to protect state
// sized by the old numbers, and an unclaimed job is not. Exiting is different:
// a broker-ASSIGNED job journaled but not yet reserved survives the restart, but
// nothing claims it for the restart window -- launchd's ThrottleInterval is 60s
// on macOS -- and GitHub retries an unclaimed assignment only about three times.
// So the drain also waits for the offer queue to empty.
func (s *Store) DrainForPendingSizing(ctx context.Context) (bool, error) {
	if s.pending == nil {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return true, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	activity, err := journalActivity(tx)
	if err != nil {
		return false, err
	}
	offered, err := offeredJobs(tx)
	if err != nil {
		return false, err
	}
	if offered > 0 {
		activity = append(activity, fmt.Sprintf("%d offered job(s) not yet reserved", offered))
	}
	if len(activity) > 0 {
		return false, nil
	}
	s.draining = true
	return true, nil
}

// journalActivity reports every durable thing a sizing change would disturb, as
// operator-readable phrases. Empty means quiescent.
func journalActivity(tx *sql.Tx) ([]string, error) {
	var activity []string
	counts, err := phaseCounts(tx, `SELECT phase,COUNT(*) FROM leases WHERE phase!='released' GROUP BY phase ORDER BY phase`)
	if err != nil {
		return nil, err
	}
	for _, count := range counts {
		activity = append(activity, fmt.Sprintf("%d lease(s) in phase %s", count.total, count.phase))
	}
	// Held slots add no REFUSAL power over the lease arm above -- a held slot is
	// exactly a non-released lease -- but they are the only part of this message
	// an operator can act on for a LOWERING: "slot(s) held: 1" says which index
	// is above the new ceiling, where "1 lease(s) in phase running" does not.
	slots, err := heldSlots(tx)
	if err != nil {
		return nil, err
	}
	if len(slots) > 0 {
		activity = append(activity, "slot(s) held: "+strings.Join(slots, ","))
	}
	counts, err = phaseCounts(tx, `SELECT phase,COUNT(*) FROM worker_resources WHERE phase!='absent' GROUP BY phase ORDER BY phase`)
	if err != nil {
		return nil, err
	}
	for _, count := range counts {
		activity = append(activity, fmt.Sprintf("%d worker resource(s) in phase %s", count.total, count.phase))
	}
	return activity, nil
}

// offeredJobs counts the work this controller could claim right now: the same
// selectability predicate ReserveNext uses, minus its ordering. Mirroring that
// predicate rather than counting every queued/assigned row is deliberate -- a
// job whose repository is ineligible or whose scale set is not active will never
// be reserved by anyone, so counting it would stall the drain forever.
//
// The two offered states are 'queued' and 'assigned' (see offeredState in
// inbox.go); ReserveNext moves a job to 'reserved' as it takes it, so an offered
// job never has a live lease.
//
// 'assigned-stale' is deliberately NOT counted, for the same reason an
// ineligible repository is not: nothing can reserve such a row until a fresh
// envelope re-confirms it, and that may never come, so counting it would stall
// the drain forever.
func offeredJobs(tx *sql.Tx) (int, error) {
	var offered int
	err := tx.QueryRow(selectableJobsQuery(`COUNT(*)`, reservableJobs, ``)).Scan(&offered)
	return offered, err
}

type phaseCount struct {
	phase string
	total int
}

func phaseCounts(tx *sql.Tx, query string) ([]phaseCount, error) {
	rows, err := tx.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var counts []phaseCount
	for rows.Next() {
		var count phaseCount
		if err := rows.Scan(&count.phase, &count.total); err != nil {
			return nil, err
		}
		counts = append(counts, count)
	}
	return counts, rows.Err()
}

func heldSlots(tx *sql.Tx) ([]string, error) {
	rows, err := tx.Query(`SELECT slot FROM leases WHERE phase!='released' ORDER BY slot`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var slots []string
	for rows.Next() {
		var slot int
		if err := rows.Scan(&slot); err != nil {
			return nil, err
		}
		slots = append(slots, strconv.Itoa(slot))
	}
	return slots, rows.Err()
}

func timestamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func changedOne(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("journal identity or expected phase changed")
	}
	return nil
}

// ReconcileInventory must receive a complete successful backend listing scoped
// to this platform's owned workers. Missing workers do not release anything:
// a create/acquire response may have been lost. Unknown workers block admission.
func (s *Store) ReconcileInventory(ctx context.Context, complete bool, leaseIDs []string) error {
	return s.reconcileInventory(ctx, complete, leaseIDs, s.config.Capacity-s.config.LegacySlots)
}

func (s *Store) reconcileInventory(ctx context.Context, complete bool, leaseIDs []string, admission int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = false
	s.admission = 0
	if !complete {
		return ErrInventoryUnknown
	}
	if admission < 0 || admission > s.config.Capacity-s.config.LegacySlots {
		return errors.New("inventory admission exceeds configured platform slots")
	}
	seen := map[string]bool{}
	for _, id := range leaseIDs {
		var phase string
		err := s.db.QueryRowContext(ctx, `SELECT phase FROM leases WHERE id=?`, id).Scan(&phase)
		if err != nil || phase == "released" || seen[id] {
			return errors.New("untracked, duplicate, or previously released worker; retain platform fence")
		}
		seen[id] = true
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var active int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&active); err != nil {
		return err
	}
	if active > admission {
		return errors.New("existing obligations exceed current inventory admission; retain platform fence")
	}
	s.admission = admission
	s.ready = true
	return nil
}
