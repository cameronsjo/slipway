package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/google/uuid"
)

type SetKey struct {
	RepositoryID int64
	ScaleSetID   int
}

// Event preserves wire ordering across event types. A job ID identifies a job
// within one scale set, never globally, and it is not a tombstone: GitHub
// cancels and reassigns the same job ID when nobody picks the assignment up.
type Event struct {
	scaleset.JobMessageBase
	RunnerID   int    `json:"runnerId,omitempty"`
	RunnerName string `json:"runnerName,omitempty"`
	Result     string `json:"result,omitempty"`
}

type Envelope struct {
	MessageID  int
	Statistics scaleset.RunnerScaleSetStatistic
	Events     []Event
}

const (
	jobStateQueued   = "queued"
	jobStateAssigned = "assigned"
	jobStateRunning  = "running"
	jobStateTerminal = "terminal"
	// jobStateAssignedStale is a broker-assigned job this process inherited from
	// a previous one. GitHub's broker sends such a job as a lone JobAssigned with
	// runnerRequestId 0, and there is NO API that re-confirms it: AcquireJobs
	// needs a positive request ID, and an already-acknowledged message is never
	// redelivered because the durable cursor advanced past it. So the only
	// confirmation signal that exists is a FRESH envelope naming the same job --
	// a broker reassignment retry, or its completion.
	//
	// Pre-restart 'assigned' is therefore not evidence: the controller cannot
	// tell a still-live assignment from one GitHub re-routed or completed while
	// the process was down, and AcquireReserved self-certifies acceptance for
	// request ID 0 without asking anyone. Marking it stale is what stops
	// ReserveNext minting a JIT runner for a job nobody has.
	jobStateAssignedStale = "assigned-stale"
)

// jobKeyStringPrefix separates the two identity namespaces so a decimal request
// ID and a string job ID can never collide on one key.
const jobKeyStringPrefix = "job:"

// maxJobIDLength bounds the string half of a job key before it reaches SQLite.
// GitHub's job IDs are short opaque identifiers; this is generous headroom, not
// a measured limit.
const maxJobIDLength = 128

// jobKey is the durable identity of one workflow job. GitHub delivers work in
// two shapes and both must work: a JobAvailable carries a positive
// runnerRequestId and is acquired through AcquireJobs (GHES today), while a
// broker-assigned job arrives as a lone JobAssigned with runnerRequestId 0 and
// is identified only by the string jobId (github.com, measured 2026-09-20).
// The key is the decimal request ID when there is one, and "job:"+jobId
// otherwise, with the jobId lowercased so a case-variant spelling of the same
// identifier cannot open a second row.
//
// This is only the key an event can propose on its own. GitHub is not known to
// keep one shape for a job's whole lifetime, so RecordEnvelope resolves identity
// against the journal (resolveJobKeyTx) rather than trusting one event.
func jobKey(event Event) (string, error) {
	if event.RunnerRequestID < 0 {
		return "", errors.New("negative runner request identity")
	}
	if event.RunnerRequestID > 0 {
		return strconv.FormatInt(event.RunnerRequestID, 10), nil
	}
	if !acceptableJobID(event.JobID) {
		return "", errors.New("job identity is absent, oversized, or not printable ASCII")
	}
	return jobKeyStringPrefix + strings.ToLower(event.JobID), nil
}

// jobKeyCandidates is every key one event could stand for: the request-keyed
// one when it carries a positive request ID, and the job-keyed one when its
// jobId is acceptable. An event may support both, and the two can already name
// different durable rows -- which is a conflict, not a choice.
func jobKeyCandidates(event Event) (requestKey, jobIDKey string) {
	if event.RunnerRequestID > 0 {
		requestKey = strconv.FormatInt(event.RunnerRequestID, 10)
	}
	if acceptableJobID(event.JobID) {
		jobIDKey = jobKeyStringPrefix + strings.ToLower(event.JobID)
	}
	return requestKey, jobIDKey
}

// acceptableJobID bounds the shape of a string job identity before it becomes a
// key: non-empty, bounded, and printable ASCII with no spaces or control bytes.
func acceptableJobID(id string) bool {
	if id == "" || len(id) > maxJobIDLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

func stateForJobEvent(event Event) string {
	switch event.MessageType {
	case scaleset.MessageTypeJobAvailable:
		return jobStateQueued
	case scaleset.MessageTypeJobAssigned:
		return jobStateAssigned
	case scaleset.MessageTypeJobStarted:
		return jobStateRunning
	case scaleset.MessageTypeJobCompleted:
		return jobStateTerminal
	default:
		return "unavailable"
	}
}

// offeredState reports whether a job state is one a reservation can still
// select, which is what the durable offer instant measures the wait for. Both
// queued (JobAvailable) and assigned (broker JobAssigned) are selectable.
func offeredState(state string) bool {
	return state == jobStateQueued || state == jobStateAssigned
}

// offerInstant decides the durable jobs.offered value for a state write. A job
// entering a selectable state gets the current instant; one that is already
// selectable keeps the instant it got, so a redelivered or repeated offer
// cannot reset a wait that is already accumulating; anything else has no offer
// in flight and carries the empty instant.
func offerInstant(next, current, currentOffered string) string {
	if !offeredState(next) {
		return ""
	}
	if offeredState(current) && currentOffered != "" {
		return currentOffered
	}
	return timestamp()
}

func jobStateRank(state string) int {
	switch state {
	case "unavailable":
		return 0
	// Below every offered state on purpose. A stale row must be outranked by any
	// fresh offer event so re-confirmation moves it forward; without this case it
	// would fall through to the unknown-state rank of 100 and no event could ever
	// revive it.
	case jobStateAssignedStale:
		return 5
	case jobStateQueued:
		return 10
	case jobStateAssigned:
		return 20
	case string(Reserved):
		return 30
	case string(Acquired):
		return 40
	case jobStateRunning:
		return 50
	case jobStateTerminal:
		return 60
	case string(CleanupPending), string(Quarantined):
		return 70
	case string(Released):
		return 80
	default:
		// Unknown states are future or operator-controlled states. Preserve them
		// until a dedicated migration/reconciliation path understands them.
		return 100
	}
}

// mergeJobState prevents a stale queue event from moving a request backward.
// Events within one wire envelope retain their order: a cancellation followed
// by a reoffer in the same envelope is a valid new offer. A later cancellation
// may also be reoffered once, while successful terminal work remains terminal.
//
// A reoffer arrives as JobAvailable under the acquire model and as a fresh
// JobAssigned under the broker model: GitHub retries an assignment nobody
// picked up by cancelling it and assigning the same job again, up to three
// times, so refusing the second shape drops the retry by rank.
func mergeJobState(current string, previous Event, incoming Event, sameEnvelope bool) (string, bool) {
	next := stateForJobEvent(incoming)
	if sameEnvelope {
		return next, true
	}
	effective := current
	if current == "unavailable" && previous.MessageType != "" {
		effective = stateForJobEvent(previous)
	}
	reoffer := incoming.MessageType == scaleset.MessageTypeJobAvailable || incoming.MessageType == scaleset.MessageTypeJobAssigned
	if effective == jobStateTerminal && reoffer &&
		previous.MessageType == scaleset.MessageTypeJobCompleted && strings.EqualFold(previous.Result, "canceled") {
		return next, true
	}
	if jobStateRank(next) > jobStateRank(effective) {
		return next, true
	}
	return current, false
}

// Refusal reasons are the fixed words a refused_envelopes row and the
// quarantine log carry. They are vocabulary, never derived from err.Error(),
// which can carry wire values (a message type, presence flags).
const (
	refusalInvalidEnvelope     = "invalid-envelope"
	refusalForeignRepository   = "foreign-repository"
	refusalRedeliveryChanged   = "redelivery-changed"
	refusalJobIdentityConflict = "job-identity-conflict"
)

// envelopeRefusal marks an error as a DETERMINISTIC refusal: the same bytes
// will be refused the same way on every redelivery, so counting it toward
// quarantine is safe. Every error left unwrapped -- SQL, a missing scale-set
// row, runner settlement, encoding -- may be transient and keeps the endless
// retry.
type envelopeRefusal struct {
	reason string
	err    error
}

func (e *envelopeRefusal) Error() string { return e.err.Error() }
func (e *envelopeRefusal) Unwrap() error { return e.err }

func refuse(reason string, err error) error {
	if err == nil {
		return nil
	}
	return &envelopeRefusal{reason: reason, err: err}
}

// refusalReasonOf reports the fixed reason word when err is a deterministic
// envelope refusal, and false for anything that may be transient. Pure.
func refusalReasonOf(err error) (string, bool) {
	var refusal *envelopeRefusal
	if errors.As(err, &refusal) {
		return refusal.reason, true
	}
	return "", false
}

// ParseEnvelope consumes the wire response before the SDK partitions events by
// type and discards unknown types. AcquireJobURL is deliberately not persisted:
// acquisition uses request IDs via the SDK, never a URL supplied by an event.
//
// Every failure here is a property of the bytes alone, so each is an
// invalid-envelope refusal (see refusalReasonOf).
func ParseEnvelope(body []byte) (Envelope, error) {
	envelope, err := parseEnvelope(body)
	return envelope, refuse(refusalInvalidEnvelope, err)
}

func parseEnvelope(body []byte) (Envelope, error) {
	var wire struct {
		MessageID   int                               `json:"messageId"`
		MessageType string                            `json:"messageType"`
		Body        string                            `json:"body"`
		Statistics  *scaleset.RunnerScaleSetStatistic `json:"statistics"`
	}
	if len(body) > 8<<20 {
		return Envelope{}, errors.New("queue envelope exceeds bounded inbox size")
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return Envelope{}, errors.New("invalid queue envelope JSON")
	}
	if wire.MessageID <= 0 || wire.MessageType != "RunnerScaleSetJobMessages" || wire.Statistics == nil {
		return Envelope{}, errors.New("invalid queue envelope identity or statistics")
	}
	envelope := Envelope{MessageID: wire.MessageID, Statistics: *wire.Statistics}
	if wire.Body != "" {
		if err := json.Unmarshal([]byte(wire.Body), &envelope.Events); err != nil {
			return Envelope{}, errors.New("invalid ordered queue events")
		}
	}
	return envelope, envelope.validate()
}

func (e Envelope) validate() error {
	if e.MessageID <= 0 || len(e.Events) > 1024 {
		return errors.New("invalid message ID or oversized event batch")
	}
	s := e.Statistics
	for _, n := range []int{s.TotalAvailableJobs, s.TotalAcquiredJobs, s.TotalAssignedJobs, s.TotalRunningJobs, s.TotalRegisteredRunners, s.TotalBusyRunners, s.TotalIdleRunners} {
		if n < 0 {
			return errors.New("negative scale-set statistics")
		}
	}
	for _, event := range e.Events {
		// Identity is now the job key: a positive request ID or an acceptable
		// string job ID. An event carrying neither cannot be journaled at all.
		_, keyErr := jobKey(event)
		if keyErr != nil || event.OwnerName == "" || event.RepositoryName == "" {
			// Names which identity field is absent and on which event type, and
			// never a value: the first live job proved the bare message
			// undiagnosable, and the envelope is not logged by design.
			return fmt.Errorf("event missing repository or job identity: type=%q request_id_present=%t job_id_present=%t job_id_acceptable=%t owner_present=%t repository_present=%t",
				event.MessageType, event.RunnerRequestID > 0, event.JobID != "", acceptableJobID(event.JobID),
				event.OwnerName != "", event.RepositoryName != "")
		}
		switch event.MessageType {
		case scaleset.MessageTypeJobAvailable, scaleset.MessageTypeJobAssigned, scaleset.MessageTypeJobStarted, scaleset.MessageTypeJobCompleted:
		default:
			return errors.New("unknown queue event type; do not acknowledge")
		}
	}
	return nil
}

// Discover atomically records a complete, already validated App discovery.
// Revocation disables future allocation while preserving existing reservations.
func (s *Store) Discover(ctx context.Context, repos []githubapp.Repository) error {
	seen := map[int64]bool{}
	names := map[string]bool{}
	for _, repo := range repos {
		if repo.ID <= 0 || strings.Count(repo.FullName, "/") != 1 || strings.HasPrefix(repo.FullName, "/") || strings.HasSuffix(repo.FullName, "/") || seen[repo.ID] || names[strings.ToLower(repo.FullName)] {
			return errors.New("invalid or duplicate repository discovery")
		}
		seen[repo.ID], names[strings.ToLower(repo.FullName)] = true, true
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE repositories SET eligible=0`); err != nil {
		return err
	}
	for _, repo := range repos {
		// Explicit columns, and archived/disabled among them: the scale-set
		// client's archived/disabled refusal reads the repository the JOURNAL
		// reconstructs, so a column that is not persisted makes that arm of the
		// choke point permanently false.
		_, err := tx.ExecContext(ctx, `INSERT INTO repositories(id,full_name,private,eligible,discovered,archived,disabled)
VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET full_name=excluded.full_name,private=excluded.private,
eligible=excluded.eligible,discovered=excluded.discovered,archived=excluded.archived,disabled=excluded.disabled`,
			repo.ID, repo.FullName, repo.Private, eligibleForPool(repo, s.allowlist), timestamp(), repo.Archived, repo.Disabled)
		if err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE discovery_state SET last_success=?,last_attempt=?,diagnostic='' WHERE id=1`, timestamp(), timestamp()); err != nil {
		return err
	}
	return tx.Commit()
}

// BindSet records an API-verified repository-scoped scale-set identity. Replacing
// an incarnation is deliberately not an implicit upsert over its old journal.
func (s *Store) BindSet(ctx context.Context, key SetKey) error {
	if key.RepositoryID <= 0 || key.ScaleSetID <= 0 {
		return errors.New("invalid scale-set identity")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO scale_sets(repo_id,set_id) VALUES(?,?) ON CONFLICT(repo_id) DO NOTHING`, key.RepositoryID, key.ScaleSetID)
	if err != nil {
		return err
	}
	var actual int
	if err := s.db.QueryRowContext(ctx, `SELECT set_id FROM scale_sets WHERE repo_id=?`, key.RepositoryID).Scan(&actual); err != nil {
		return err
	}
	if actual != key.ScaleSetID {
		return errors.New("scale-set incarnation changed; reconcile instead of replacing its identity")
	}
	return nil
}

// RecordEnvelope commits the ordered sanitized envelope and offered requests in
// one transaction. It does not infer desired worker count from event cardinality:
// GitHub may truncate events, so callers use the persisted aggregate statistics.
func (s *Store) RecordEnvelope(ctx context.Context, key SetKey, session uuid.UUID, envelope Envelope) error {
	if session == uuid.Nil {
		return errors.New("missing message session identity")
	}
	if err := envelope.validate(); err != nil {
		return refuse(refusalInvalidEnvelope, err)
	}
	data, err := json.Marshal(envelope)
	if err != nil || len(data) > 8<<20 {
		return errors.New("cannot encode bounded queue envelope")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var fullName string
	if err := tx.QueryRowContext(ctx, `SELECT r.full_name FROM repositories r JOIN scale_sets s ON s.repo_id=r.id WHERE r.id=? AND s.set_id=?`, key.RepositoryID, key.ScaleSetID).Scan(&fullName); err != nil {
		return err
	}
	for _, event := range envelope.Events {
		if !strings.EqualFold(event.OwnerName+"/"+event.RepositoryName, fullName) {
			return refuse(refusalForeignRepository, errors.New("queue event belongs to a different repository"))
		}
	}
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT payload FROM messages WHERE repo_id=? AND set_id=? AND session=? AND message_id=?`, key.RepositoryID, key.ScaleSetID, session.String(), envelope.MessageID).Scan(&previous)
	if err == nil {
		var old Envelope
		if err := json.Unmarshal([]byte(previous), &old); err != nil {
			return err
		}
		if old.MessageID != envelope.MessageID || old.Statistics != envelope.Statistics {
			return refuse(refusalRedeliveryChanged, errors.New("redelivered envelope identity has different statistics"))
		}
		oldEvents, _ := json.Marshal(old.Events)
		newEvents, _ := json.Marshal(envelope.Events)
		if string(oldEvents) != string(newEvents) {
			return refuse(refusalRedeliveryChanged, errors.New("redelivered envelope identity has different events"))
		}
		// Do not apply an old envelope twice, including after a lost ACK response.
		if err := tx.Commit(); err != nil {
			return err
		}
		// A duplicate that was already applied is still GitHub talking on this
		// wire, so it still counts as heard -- only after the commit settles
		// which outcome this delivery had.
		s.recordEnvelopeReceived(key, time.Now().UTC())
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	stats, _ := json.Marshal(envelope.Statistics)
	if err := changedOne(tx.ExecContext(ctx, `UPDATE scale_sets SET statistics=?,observed=? WHERE repo_id=? AND set_id=?`, string(stats), timestamp(), key.RepositoryID, key.ScaleSetID)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO messages(repo_id,set_id,session,message_id,payload,received) VALUES(?,?,?,?,?,?)`,
		key.RepositoryID, key.ScaleSetID, session.String(), envelope.MessageID, string(data), timestamp()); err != nil {
		return err
	}
	changedJobs := map[string]bool{}
	for _, event := range envelope.Events {
		payload, _ := json.Marshal(event)
		// Identity comes from the journal, not from this event alone: a job can
		// change shape between events, and a per-event key splits it in two.
		resolution, keyErr := resolveJobKeyTx(ctx, tx, key, event)
		if keyErr != nil {
			return keyErr
		}
		eventKey := resolution.Key
		s.logEventShape(event, resolution)
		var currentState, currentPayload, currentOffered string
		lookupErr := tx.QueryRowContext(ctx, `SELECT state,payload,offered FROM jobs WHERE repo_id=? AND set_id=? AND job_key=?`,
			key.RepositoryID, key.ScaleSetID, eventKey).Scan(&currentState, &currentPayload, &currentOffered)
		if errors.Is(lookupErr, sql.ErrNoRows) {
			state := stateForJobEvent(event)
			if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(repo_id,set_id,job_key,runner_request_id,job_id,state,payload,offered) VALUES(?,?,?,?,?,?,?,?)`,
				key.RepositoryID, key.ScaleSetID, eventKey, event.RunnerRequestID, normalizedJobID(event), state, string(payload), offerInstant(state, "", "")); err != nil {
				return err
			}
			changedJobs[eventKey] = true
			if err := settleRunnerEventTx(ctx, tx, key, event, eventKey); err != nil {
				return err
			}
			continue
		}
		if lookupErr != nil {
			return lookupErr
		}
		// The durable row claimed this event. If the event brought an identity
		// half the row did not have, record it: AcquireJobs needs the real int64,
		// and each half is what links a later event of the other shape back here.
		if err := backfillJobIdentityTx(ctx, tx, key, eventKey, event); err != nil {
			return err
		}
		var previousEvent Event
		if decoded, ok := decodeEventPayload(currentPayload); ok {
			previousEvent = decoded
		}
		state, update := mergeJobState(currentState, previousEvent, event, changedJobs[eventKey])
		if update {
			if err := changedOne(tx.ExecContext(ctx, `UPDATE jobs SET state=?,payload=?,offered=? WHERE repo_id=? AND set_id=? AND job_key=?`,
				state, string(payload), offerInstant(state, currentState, currentOffered), key.RepositoryID, key.ScaleSetID, eventKey)); err != nil {
				return err
			}
			changedJobs[eventKey] = true
		}
		// A started/completed event can settle an uncertain acquisition only
		// when its runner identity matches the durable JIT identity for this
		// exact job. Aggregate statistics are insufficient.
		if err := settleRunnerEventTx(ctx, tx, key, event, eventKey); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Only a committed envelope counts as heard: a refused one must not mask a
	// silent controller behind a Liveness clock GitHub never actually advanced.
	s.recordEnvelopeReceived(key, time.Now().UTC())
	logEnvelopeSummary(key, envelope)
	return nil
}

// logEnvelopeSummary reports one accepted envelope at INFO, always on. It is
// the low-volume counterpart to the opt-in per-event shape log: one line per
// poll that returned a message, counts only.
//
// The request-ID split is the load-bearing part. A JobAssigned with request ID
// 0 is the github.com broker shape that AcquireJobs cannot confirm, and during
// the 2026-09-22 starvation nothing anywhere said how many of those the
// controller was holding. Counts and message types only -- never a job ID, a
// repository name, or an owner.
func logEnvelopeSummary(key SetKey, envelope Envelope) {
	counts := map[string]int{}
	var brokerAssigned, requestKeyed int
	for _, event := range envelope.Events {
		counts[string(event.MessageType)]++
		if event.RunnerRequestID > 0 {
			requestKeyed++
			continue
		}
		// Only a JobAssigned with request ID 0 is the broker shape this counter
		// exists to surface. A JobStarted or JobCompleted routinely carries no
		// request ID and says nothing about how the job was handed out, so
		// counting those inflated the number the operator reads as "unconfirmable
		// assignments I am holding".
		if event.MessageType == scaleset.MessageTypeJobAssigned {
			brokerAssigned++
		}
	}
	slog.Info("Recorded a queue envelope.",
		"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID,
		"message_id", envelope.MessageID, "events", len(envelope.Events),
		"job_available", counts[string(scaleset.MessageTypeJobAvailable)],
		"job_assigned", counts[string(scaleset.MessageTypeJobAssigned)],
		"job_started", counts[string(scaleset.MessageTypeJobStarted)],
		"job_completed", counts[string(scaleset.MessageTypeJobCompleted)],
		"job_assigned_request_id_zero", brokerAssigned, "request_id_present", requestKeyed,
		"total_assigned_jobs", envelope.Statistics.TotalAssignedJobs,
		"total_available_jobs", envelope.Statistics.TotalAvailableJobs)
}

// eventShapeLogVariable opts a controller into the wire-shape diagnostic. Only
// the JobAssigned shape has ever been measured live, so the pilot needs to see
// what GitHub actually sends for the rest of a job's lifetime. Off unless the
// variable is exactly "1", and read once at Open, never per event.
const eventShapeLogVariable = "SJO_RUNNER_POOL_EVENT_SHAPE_LOG"

// eventShapeLogEnabled is the whole opt-in decision, pure so it can be pinned
// without an ambient environment. Exactly "1" enables it; anything else, "true"
// and "yes" included, leaves the diagnostic off.
func eventShapeLogEnabled(value string) bool { return value == "1" }

// logEventShape reports the shape of one accepted event: presence booleans and
// fixed vocabulary only. It never emits a value, a job ID or the resolved key,
// and it is not part of any published report -- it goes to the process log for
// an operator reading the pilot, and nowhere else.
func (s *Store) logEventShape(event Event, resolution jobResolution) {
	if s.eventShape == nil {
		return
	}
	resolved := "created"
	if resolution.Matched {
		resolved = "matched"
	}
	s.eventShape.Info("runner-pool queue event shape",
		"message_type", string(event.MessageType),
		"request_id_present", event.RunnerRequestID > 0,
		"job_id_present", event.JobID != "",
		"runner_name_present", event.RunnerName != "",
		"result_present", event.Result != "",
		"key_namespace", resolution.Namespace,
		"resolution", resolved)
}

// jobResolution is how one event's identity was settled against the journal.
// Namespace and Matched are fixed vocabulary for the shape diagnostic; neither
// they nor anything derived from them carries a value.
type jobResolution struct {
	Key       string
	Namespace string // "request" or "job"
	Matched   bool   // true when an existing durable row claimed this event
}

// resolveJobKeyTx settles which durable job an event belongs to, inside the
// caller's transaction and scoped to one scale set.
//
// The event alone is not enough. A job first seen as a broker JobAssigned
// (runnerRequestId 0, identified by jobId) may later send events that do carry a
// request ID, and the reverse is equally possible; only the JobAssigned shape
// has ever been measured. Keying on the event alone splits such a job across two
// rows, which loses the completion, strands the lease in provisioning, and lets
// RetireUnusedWorkers destroy the worker while the job is still running.
//
// So both candidate keys are looked up, plus the backfilled runner_request_id
// linkage. Exactly one matching row claims the event and keeps its own key --
// the key never changes once written, because leases and evidence reference it.
// Two different matching rows are an identity conflict and refuse the envelope,
// which leaves it unacknowledged for redelivery rather than merging two jobs.
func resolveJobKeyTx(ctx context.Context, tx *sql.Tx, key SetKey, event Event) (jobResolution, error) {
	requestKey, jobIDKey := jobKeyCandidates(event)
	if requestKey == "" && jobIDKey == "" {
		return jobResolution{}, errors.New("queue event carries no usable job identity")
	}
	matched := map[string]bool{}
	collect := func(query string, args ...any) error {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var found string
			if err := rows.Scan(&found); err != nil {
				return err
			}
			matched[found] = true
		}
		return rows.Err()
	}
	if jobIDKey != "" {
		// job_id catches the mirror case: a row created request-keyed whose jobId
		// was recorded alongside it, now addressed by an event that carries only
		// the jobId.
		if err := collect(`SELECT job_key FROM jobs WHERE repo_id=? AND set_id=? AND (job_key=? OR job_id=?)`,
			key.RepositoryID, key.ScaleSetID, jobIDKey, normalizedJobID(event)); err != nil {
			return jobResolution{}, err
		}
	}
	if requestKey != "" {
		// runner_request_id catches a job whose key is job-keyed and whose request
		// ID was backfilled by an earlier event in this same lifetime.
		if err := collect(`SELECT job_key FROM jobs WHERE repo_id=? AND set_id=? AND (job_key=? OR runner_request_id=?)`,
			key.RepositoryID, key.ScaleSetID, requestKey, event.RunnerRequestID); err != nil {
			return jobResolution{}, err
		}
	}
	if len(matched) > 1 {
		// Presence only: which fields the event carried, never their values.
		return jobResolution{}, refuse(refusalJobIdentityConflict, fmt.Errorf("queue event matches two different durable jobs; do not acknowledge: type=%q request_id_present=%t job_id_present=%t",
			event.MessageType, event.RunnerRequestID > 0, event.JobID != ""))
	}
	for found := range matched {
		return jobResolution{Key: found, Namespace: keyNamespace(found), Matched: true}, nil
	}
	created := requestKey
	if created == "" {
		created = jobIDKey
	}
	return jobResolution{Key: created, Namespace: keyNamespace(created)}, nil
}

// keyNamespace names which identity namespace a key came from. It is fixed
// vocabulary for the diagnostic; the key itself is never published.
func keyNamespace(key string) string {
	if strings.HasPrefix(key, jobKeyStringPrefix) {
		return "job"
	}
	return "request"
}

// normalizedJobID is the jobId as it is stored and compared: lowercased, and
// empty when the event's jobId is not an acceptable identity.
func normalizedJobID(event Event) string {
	if !acceptableJobID(event.JobID) {
		return ""
	}
	return strings.ToLower(event.JobID)
}

// backfillJobIdentityTx records the half of a job's identity that arrived after
// its durable row was created: a request ID on a job-keyed row, or a jobId on a
// request-keyed one. Both are what let a later event of the other shape resolve
// back to this same row. It never changes job_key -- leases and acquisition
// evidence reference that -- and it never overwrites a value already present.
func backfillJobIdentityTx(ctx context.Context, tx *sql.Tx, key SetKey, jobKey string, event Event) error {
	if event.RunnerRequestID > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET runner_request_id=? WHERE repo_id=? AND set_id=? AND job_key=? AND runner_request_id=0`,
			event.RunnerRequestID, key.RepositoryID, key.ScaleSetID, jobKey); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE leases SET runner_request_id=? WHERE repo_id=? AND set_id=? AND job_key=? AND runner_request_id=0 AND phase!='released'`,
			event.RunnerRequestID, key.RepositoryID, key.ScaleSetID, jobKey); err != nil {
			return err
		}
	}
	if id := normalizedJobID(event); id != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET job_id=? WHERE repo_id=? AND set_id=? AND job_key=? AND job_id=''`,
			id, key.RepositoryID, key.ScaleSetID, jobKey); err != nil {
			return err
		}
	}
	return nil
}

func decodeEventPayload(payload string) (Event, bool) {
	var event Event
	if err := json.Unmarshal([]byte(payload), &event); err != nil || event.MessageType == "" {
		return Event{}, false
	}
	return event, true
}

type PendingMessage struct {
	Set     SetKey
	Session uuid.UUID
	Envelope
}

func (s *Store) PendingMessages(ctx context.Context) ([]PendingMessage, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT repo_id,set_id,session,payload FROM messages WHERE acked=0 ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []PendingMessage
	for rows.Next() {
		var item PendingMessage
		var session, payload string
		if err := rows.Scan(&item.Set.RepositoryID, &item.Set.ScaleSetID, &session, &payload); err != nil {
			return nil, err
		}
		if item.Session, err = uuid.Parse(session); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(payload), &item.Envelope); err != nil {
			return nil, err
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

func (s *Store) MarkAcknowledged(ctx context.Context, key SetKey, session uuid.UUID, messageID int) error {
	return changedOne(s.db.ExecContext(ctx, `UPDATE messages SET acked=1 WHERE repo_id=? AND set_id=? AND session=? AND message_id=?`, key.RepositoryID, key.ScaleSetID, session.String(), messageID))
}

// refusalQuarantineThreshold is how many refusals of the same kind one message
// may collect before it is acknowledged without being applied. Three rides out
// a refusal that is really a race the next redelivery settles, and still frees
// a session whose queue one envelope would otherwise block forever.
//
// The value relies on discovery running every minute and on its three-minute
// fence (Registry.Run): a foreign-repository refusal can be a stale full_name
// that the next discovery corrects. Lowering it needs that timing analysis
// redone first.
const refusalQuarantineThreshold = 3

// refusedShape is all a quarantine keeps of a refused envelope's contents: the
// event count and a count per known message type. Never a value. A body that
// failed to parse has no trusted events, so its shape is the zero value.
type refusedShape struct {
	events                                  int
	available, assigned, started, completed int
}

// shapeOf counts an envelope's events by type. Pure. An event of any other type
// counts toward events only; RecordEnvelope refuses those before this runs.
func shapeOf(envelope Envelope) refusedShape {
	shape := refusedShape{events: len(envelope.Events)}
	for _, event := range envelope.Events {
		switch event.MessageType {
		case scaleset.MessageTypeJobAvailable:
			shape.available++
		case scaleset.MessageTypeJobAssigned:
			shape.assigned++
		case scaleset.MessageTypeJobStarted:
			shape.started++
		case scaleset.MessageTypeJobCompleted:
			shape.completed++
		}
	}
	return shape
}

// typeList is the comma list of the known type names present, in a fixed
// order. It is built from vocabulary, never copied off the wire.
func (r refusedShape) typeList() string {
	var names []string
	for _, kind := range []struct {
		name  scaleset.MessageType
		count int
	}{
		{scaleset.MessageTypeJobAvailable, r.available},
		{scaleset.MessageTypeJobAssigned, r.assigned},
		{scaleset.MessageTypeJobStarted, r.started},
		{scaleset.MessageTypeJobCompleted, r.completed},
	} {
		if kind.count > 0 {
			names = append(names, string(kind.name))
		}
	}
	return strings.Join(names, ",")
}

// RecordRefusal counts one deterministic refusal of a message and returns the
// new count. It runs in its own transaction because the refused one rolled
// back. A refusal of a DIFFERENT kind restarts the count at one: quarantine is
// for an envelope refused the same way every time. No payload is stored --
// only identity, the fixed reason word, the event count, the type names and
// timestamps.
func (s *Store) RecordRefusal(ctx context.Context, key SetKey, session uuid.UUID, messageID int, reason string, shape refusedShape) (int, error) {
	if session == uuid.Nil || messageID <= 0 || reason == "" {
		return 0, errors.New("refusal needs a session, a positive message ID and a reason")
	}
	now := timestamp()
	var refusals int
	err := s.db.QueryRowContext(ctx, `INSERT INTO refused_envelopes(repo_id,set_id,session,message_id,reason,refusals,events,event_types,first_refused,last_refused)
VALUES(?,?,?,?,?,1,?,?,?,?) ON CONFLICT(repo_id,set_id,session,message_id) DO UPDATE SET
 refusals=CASE WHEN refused_envelopes.reason=excluded.reason THEN refused_envelopes.refusals+1 ELSE 1 END,
 reason=excluded.reason, events=excluded.events, event_types=excluded.event_types, last_refused=excluded.last_refused
RETURNING refusals`,
		key.RepositoryID, key.ScaleSetID, session.String(), messageID, reason, shape.events, shape.typeList(), now, now).Scan(&refusals)
	return refusals, err
}

// AckIfPresent acknowledges this session's messages row for messageID if one
// exists and is still unacknowledged, and reports whether this call flipped it
// from 0 to 1. A missing or already-acknowledged row is not an error. The
// single UPDATE is its own transaction, so the flip and its report agree.
func (s *Store) AckIfPresent(ctx context.Context, key SetKey, session uuid.UUID, messageID int) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE messages SET acked=1 WHERE repo_id=? AND set_id=? AND session=? AND message_id=? AND acked=0`,
		key.RepositoryID, key.ScaleSetID, session.String(), messageID)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return changed == 1, nil
}

// RevertAcknowledged undoes an AckIfPresent flip, for a quarantine whose
// DELETE failed: GitHub still holds the message, so the cursor must not pass it.
func (s *Store) RevertAcknowledged(ctx context.Context, key SetKey, session uuid.UUID, messageID int) error {
	return changedOne(s.db.ExecContext(ctx, `UPDATE messages SET acked=0 WHERE repo_id=? AND set_id=? AND session=? AND message_id=? AND acked=1`,
		key.RepositoryID, key.ScaleSetID, session.String(), messageID))
}

// MarkQuarantined records that a refused message was acknowledged without
// being applied. It never touches the messages table. Any refusal can have a
// messages row: a delivery that committed and lost its ACK, then was refused
// on redelivery for any reason. quarantineRefused acknowledges that row BEFORE
// the DELETE, so a crash between the DELETE and this stamp cannot pin the
// cursor.
func (s *Store) MarkQuarantined(ctx context.Context, key SetKey, session uuid.UUID, messageID int) error {
	return changedOne(s.db.ExecContext(ctx, `UPDATE refused_envelopes SET acked=? WHERE repo_id=? AND set_id=? AND session=? AND message_id=?`,
		timestamp(), key.RepositoryID, key.ScaleSetID, session.String(), messageID))
}

// Cursor advances only over successfully acknowledged deliveries in this exact
// session. It is not copied into a replacement session with unproved semantics.
func (s *Store) Cursor(ctx context.Context, key SetKey, session uuid.UUID) (int, error) {
	var cursor int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(message_id),0) FROM messages
WHERE repo_id=? AND set_id=? AND session=? AND acked=1 AND message_id<COALESCE((
 SELECT MIN(message_id) FROM messages WHERE repo_id=? AND set_id=? AND session=? AND acked=0),9223372036854775807)`,
		key.RepositoryID, key.ScaleSetID, session.String(), key.RepositoryID, key.ScaleSetID, session.String()).Scan(&cursor)
	return cursor, err
}

func (s *Store) Statistics(ctx context.Context, key SetKey) (*scaleset.RunnerScaleSetStatistic, error) {
	var data string
	if err := s.db.QueryRowContext(ctx, `SELECT statistics FROM scale_sets WHERE repo_id=? AND set_id=?`, key.RepositoryID, key.ScaleSetID).Scan(&data); err != nil {
		return nil, err
	}
	var statistics *scaleset.RunnerScaleSetStatistic
	if err := json.Unmarshal([]byte(data), &statistics); err != nil {
		return nil, err
	}
	return statistics, nil
}

type AcknowledgmentAPI interface {
	DeleteMessage(context.Context, int) error
}

// Receive must be given the raw wire response, not a remarshal of the SDK's
// partitioned slices. Parsing/persistence failure does not acknowledge a
// message here. The one exception lives in QueueSession.Poll: a message
// refused the same deterministic way refusalQuarantineThreshold times (see
// refusalReasonOf) is acknowledged unapplied and recorded in refused_envelopes.
// A transient failure never counts toward it.
// A failed DELETE keeps the durable envelope pending and leaves the cursor back.
func (s *Store) Receive(ctx context.Context, key SetKey, session uuid.UUID, body []byte, api AcknowledgmentAPI) error {
	envelope, err := ParseEnvelope(body)
	if err != nil {
		return err
	}
	if err := s.RecordEnvelope(ctx, key, session, envelope); err != nil {
		return err
	}
	if err := api.DeleteMessage(ctx, envelope.MessageID); err != nil {
		return errors.New("queue acknowledgment outcome unknown; durable envelope retained")
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	return s.MarkAcknowledged(saveCtx, key, session, envelope.MessageID)
}
