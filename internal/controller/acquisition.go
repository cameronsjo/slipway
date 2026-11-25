package controller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/actions/scaleset"
)

// AcquisitionOutcome is the durable result of one request-ID acquisition.
// Unknown transport outcomes intentionally have no evidence row: an empty
// aggregate or a timeout cannot establish which operation GitHub applied.
type AcquisitionOutcome string

const (
	AcquisitionDeclined  AcquisitionOutcome = "declined"
	AcquisitionAccepted  AcquisitionOutcome = "accepted"
	AcquisitionCompleted AcquisitionOutcome = "completed"
)

const (
	AcquisitionSourceAPI         = "acquire-response"
	AcquisitionSourceRunnerEvent = "runner-event"
	// AcquisitionSourceBrokerAssignment marks evidence the controller INFERRED
	// rather than evidence GitHub gave. A broker-assigned job carries no runner
	// request ID, so no AcquireJobs call is possible and GitHub answers nothing;
	// the acceptance recorded under this source means only "GitHub assigned this
	// job to this scale set and we took it". It is deliberately a separate
	// constant from acquire-response so the two can never be read as one.
	AcquisitionSourceBrokerAssignment = "broker-assignment"
	// AcquisitionSourceUnseenDemand marks a probe lease (see unseen_demand.go).
	// There is no job and no assignment behind it, only GitHub's report of work
	// no envelope named, so its acceptance means "this controller chose to
	// register a runner on the set" and nothing more. It is kept apart from
	// broker-assignment for the same reason that one is kept apart from
	// acquire-response.
	AcquisitionSourceUnseenDemand = "unseen-demand"
)

// AcquisitionSettlement is trusted-side evidence. JobKey must be the job
// reserved by the lease; a runner event additionally must identify the
// exact JIT runner persisted for that lease.
type AcquisitionSettlement struct {
	JobKey        string
	Outcome       AcquisitionOutcome
	Source        string
	RunnerLeaseID string
	RunnerID      int
	RunnerName    string
}

// AcquisitionEvidence is the durable record used by final lease release.
type AcquisitionEvidence struct {
	LeaseID       string
	JobKey        string
	Outcome       AcquisitionOutcome
	Source        string
	RunnerLeaseID string
	RunnerID      int
	RunnerName    string
	Observed      string
	Diagnostic    string
}

const acquisitionEvidenceColumns = `lease_id,job_key,outcome,source,runner_lease_id,runner_id,runner_name,observed,diagnostic`
const qualifiedLeaseColumns = `l.id,l.slot,l.repo_id,l.set_id,l.job_key,l.runner_request_id,l.phase,l.runner_name,l.created,l.updated,l.diagnostic`

func changedZeroOrOne(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed > 1 {
		return errors.New("journal lifecycle update changed multiple identities")
	}
	return nil
}

func readAcquisitionEvidence(row interface{ Scan(...any) error }) (AcquisitionEvidence, error) {
	var evidence AcquisitionEvidence
	err := row.Scan(&evidence.LeaseID, &evidence.JobKey, &evidence.Outcome, &evidence.Source,
		&evidence.RunnerLeaseID, &evidence.RunnerID, &evidence.RunnerName, &evidence.Observed, &evidence.Diagnostic)
	return evidence, err
}

func (s *Store) AcquisitionEvidence(ctx context.Context, leaseID string) (AcquisitionEvidence, error) {
	return readAcquisitionEvidence(s.db.QueryRowContext(ctx,
		`SELECT `+acquisitionEvidenceColumns+` FROM acquisition_evidence WHERE lease_id=?`, leaseID))
}

// AcquisitionSettled is deliberately derived from the journal, rather than
// from a caller's boolean. It is the only acquisition proof accepted by
// FinishCleanup.
func (s *Store) AcquisitionSettled(ctx context.Context, leaseID string) (bool, error) {
	var settled bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
SELECT 1 FROM acquisition_evidence e JOIN leases l ON l.id=e.lease_id
WHERE e.lease_id=? AND e.job_key=l.job_key AND e.outcome IN ('declined','accepted','completed'))`, leaseID).Scan(&settled)
	return settled, err
}

func acquisitionOutcomeRank(outcome AcquisitionOutcome) int {
	switch outcome {
	case AcquisitionDeclined:
		return 1
	case AcquisitionAccepted:
		return 2
	case AcquisitionCompleted:
		return 3
	default:
		return 0
	}
}

func validateAcquisitionSettlement(lease Lease, settlement AcquisitionSettlement, runnerLease *Lease, jitRunnerID int, jitPresent bool) error {
	if settlement.JobKey == "" || settlement.JobKey != lease.JobKey {
		return errors.New("acquisition evidence job identity does not match lease")
	}
	if acquisitionOutcomeRank(settlement.Outcome) == 0 {
		return errors.New("invalid acquisition evidence outcome")
	}
	switch settlement.Source {
	case AcquisitionSourceAPI:
		if settlement.Outcome != AcquisitionDeclined && settlement.Outcome != AcquisitionAccepted {
			return errors.New("API acquisition response cannot prove job completion")
		}
		if settlement.RunnerLeaseID != "" || settlement.RunnerID != 0 || settlement.RunnerName != "" {
			return errors.New("API acquisition response cannot carry runner identity")
		}
	case AcquisitionSourceBrokerAssignment:
		// The narrowest possible widening: acceptance only, and only for a lease
		// whose job genuinely has no runner request ID. A job that does have one
		// must go through AcquireJobs and answer with acquire-response.
		if settlement.Outcome != AcquisitionAccepted {
			return errors.New("a broker assignment can only record acceptance")
		}
		if lease.RequestID != 0 {
			return errors.New("a job with a runner request identity must be settled by its acquire response")
		}
		if settlement.RunnerLeaseID != "" || settlement.RunnerID != 0 || settlement.RunnerName != "" {
			return errors.New("a broker assignment cannot carry runner identity")
		}
		if isUnseenJobKey(lease.JobKey) {
			return errors.New("a probe job was never assigned by the broker; settle it as unseen demand")
		}
	case AcquisitionSourceUnseenDemand:
		// Acceptance only, and only for a probe row, which never has a request.
		if settlement.Outcome != AcquisitionAccepted {
			return errors.New("an unseen-demand probe can only record acceptance")
		}
		if !isUnseenJobKey(lease.JobKey) || lease.RequestID != 0 {
			return errors.New("only a probe job can be settled as unseen demand")
		}
		if settlement.RunnerLeaseID != "" || settlement.RunnerID != 0 || settlement.RunnerName != "" {
			return errors.New("an unseen-demand probe cannot carry runner identity")
		}
	case AcquisitionSourceRunnerEvent:
		if settlement.Outcome != AcquisitionAccepted && settlement.Outcome != AcquisitionCompleted {
			return errors.New("runner event cannot prove a definite API decline")
		}
		if !jitPresent || runnerLease == nil || runnerLease.ID != settlement.RunnerLeaseID ||
			runnerLease.Set != lease.Set || runnerLease.Phase == Released || jitRunnerID <= 0 ||
			settlement.RunnerID <= 0 || settlement.RunnerName == "" || settlement.RunnerID != jitRunnerID ||
			settlement.RunnerName != runnerLease.RunnerName {
			return errors.New("runner event identity does not match an owned durable JIT runner")
		}
	default:
		return errors.New("unrecognized acquisition evidence source")
	}
	return nil
}

func recordAcquisitionEvidenceTx(ctx context.Context, tx *sql.Tx, lease Lease, settlement AcquisitionSettlement, runnerLease *Lease, jitRunnerID int, jitPresent bool) error {
	if err := validateAcquisitionSettlement(lease, settlement, runnerLease, jitRunnerID, jitPresent); err != nil {
		return err
	}
	var existing AcquisitionEvidence
	var err error
	existing, err = readAcquisitionEvidence(tx.QueryRowContext(ctx,
		`SELECT `+acquisitionEvidenceColumns+` FROM acquisition_evidence WHERE lease_id=?`, lease.ID))
	if err == nil {
		if existing.JobKey != settlement.JobKey {
			return errors.New("existing acquisition evidence has a different job identity")
		}
		existingRank := acquisitionOutcomeRank(existing.Outcome)
		newRank := acquisitionOutcomeRank(settlement.Outcome)
		if existing.Outcome == settlement.Outcome && existing.Source == settlement.Source &&
			existing.RunnerLeaseID == settlement.RunnerLeaseID && existing.RunnerID == settlement.RunnerID &&
			existing.RunnerName == settlement.RunnerName {
			return nil
		}
		if existingRank > newRank {
			if existing.Outcome != settlement.Outcome {
				return fmt.Errorf("conflicting acquisition evidence would move from %s to %s", existing.Outcome, settlement.Outcome)
			}
			return nil
		}
		if existingRank == newRank {
			// A started event may add the exact runner identity to an API
			// acceptance without weakening the result. Other same-rank
			// differences are stale or conflicting and leave the stronger
			// durable record untouched.
			if existing.Outcome == AcquisitionAccepted && existing.RunnerLeaseID == "" && existing.RunnerID == 0 &&
				settlement.Source == AcquisitionSourceRunnerEvent {
				// Continue to the update below.
			} else {
				return nil
			}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if lease.Phase == Released {
		return errors.New("released lease cannot receive acquisition evidence")
	}
	now := timestamp()
	if err == nil {
		if err := changedOne(tx.ExecContext(ctx, `UPDATE acquisition_evidence SET job_key=?,outcome=?,source=?,runner_lease_id=?,runner_id=?,runner_name=?,observed=?,diagnostic='' WHERE lease_id=?`,
			settlement.JobKey, settlement.Outcome, settlement.Source, settlement.RunnerLeaseID, settlement.RunnerID, settlement.RunnerName, now, lease.ID)); err != nil {
			return err
		}
	} else if err := changedOne(tx.ExecContext(ctx, `INSERT INTO acquisition_evidence(lease_id,job_key,outcome,source,runner_lease_id,runner_id,runner_name,observed) VALUES(?,?,?,?,?,?,?,?)`,
		lease.ID, settlement.JobKey, settlement.Outcome, settlement.Source, settlement.RunnerLeaseID, settlement.RunnerID, settlement.RunnerName, now)); err != nil {
		return err
	}

	// A definitive API response moves the lease out of the intent phase. A
	// runner event can settle an unknown POST, but it never skips provisioning.
	var to Phase
	switch settlement.Outcome {
	case AcquisitionDeclined:
		to = CleanupPending
	case AcquisitionAccepted:
		if lease.Phase == AcquisitionIntent || lease.Phase == AcquisitionUnknown {
			to = Acquired
		}
	case AcquisitionCompleted:
		// A completion settles the request operation, but an unknown request
		// has no own worker intent in this controller path. Keep that lease
		// fenced until an operator/live reconciliation records its own worker
		// and registration absence; the observed runner is cleaned separately.
	}
	if to != "" && lease.Phase != to {
		if settlement.Outcome == AcquisitionDeclined && lease.Phase != AcquisitionIntent {
			return errors.New("definite decline arrived after acquisition intent was no longer active")
		}
		if err := changedOne(tx.ExecContext(ctx, `UPDATE leases SET phase=?,diagnostic=CASE WHEN ?='declined' THEN 'acquisition-declined' ELSE diagnostic END,updated=? WHERE id=? AND phase=?`,
			to, settlement.Outcome, now, lease.ID, lease.Phase)); err != nil {
			return err
		}
		if settlement.Outcome == AcquisitionDeclined {
			if err := changedOne(tx.ExecContext(ctx, `UPDATE jobs SET state='unavailable' WHERE repo_id=? AND set_id=? AND job_key=?`,
				lease.Set.RepositoryID, lease.Set.ScaleSetID, lease.JobKey)); err != nil {
				return err
			}
		}
	}
	return nil
}

// SettleAcquisition records one authoritative result and is idempotent for the
// same result. It is also the narrow API used by acquisition and queue-event
// reconciliation, so an unknown response cannot be guessed into success.
func (s *Store) SettleAcquisition(ctx context.Context, leaseID string, settlement AcquisitionSettlement) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	lease, err := readLease(tx.QueryRowContext(ctx, `SELECT `+leaseColumns+` FROM leases WHERE id=?`, leaseID))
	if err != nil {
		return err
	}
	var runnerLease *Lease
	var jitRunnerID int
	var jitPresent bool
	if settlement.Source == AcquisitionSourceRunnerEvent {
		if settlement.RunnerLeaseID == "" {
			return errors.New("runner event evidence lacks observed runner lease identity")
		}
		observed, readErr := readLease(tx.QueryRowContext(ctx, `SELECT `+leaseColumns+` FROM leases WHERE id=?`, settlement.RunnerLeaseID))
		if readErr != nil {
			return readErr
		}
		runnerLease = &observed
		err = tx.QueryRowContext(ctx, `SELECT runner_id FROM jit_transfers WHERE lease_id=? AND removal!='absent'`, settlement.RunnerLeaseID).Scan(&jitRunnerID)
		if err == nil {
			jitPresent = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if err := recordAcquisitionEvidenceTx(ctx, tx, lease, settlement, runnerLease, jitRunnerID, jitPresent); err != nil {
		return err
	}
	return tx.Commit()
}

// settleRunnerEventTx separates job identity from runner identity. GitHub
// may execute job A on the worker reserved while another job B was
// being acquired, so the job's lease receives acquisition evidence while
// the observed runner lease advances its own lifecycle independently.
//
// The lookup keys on the job key, which RecordEnvelope resolved against the
// journal rather than from this event alone. Only the JobAssigned shape has been
// measured live; whether later events of a broker-assigned job carry a request
// ID is unknown, and either way they settle the same durable row.
func settleRunnerEventTx(ctx context.Context, tx *sql.Tx, key SetKey, event Event, eventKey string) error {
	requestLease, err := readLease(tx.QueryRowContext(ctx, `SELECT `+leaseColumns+` FROM leases
WHERE repo_id=? AND set_id=? AND job_key=? AND phase!='released'`, key.RepositoryID, key.ScaleSetID, eventKey))
	jobLeaseMissing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !jobLeaseMissing {
		return err
	}
	if event.MessageType != scaleset.MessageTypeJobStarted && event.MessageType != scaleset.MessageTypeJobCompleted {
		return nil
	}
	observedRunner, err := readLease(tx.QueryRowContext(ctx, `SELECT `+qualifiedLeaseColumns+` FROM leases l JOIN jit_transfers j ON j.lease_id=l.id
WHERE l.repo_id=? AND l.set_id=? AND l.runner_name=? AND l.phase!='released' AND j.runner_id=? AND j.removal!='absent'`,
		key.RepositoryID, key.ScaleSetID, event.RunnerName, event.RunnerID))
	if errors.Is(err, sql.ErrNoRows) {
		// A mismatched or foreign runner event is still a valid inbox event,
		// but it cannot settle an acquisition or advance an unowned worker.
		if jobLeaseMissing {
			slog.Info("A runner event matched neither the job's lease nor any owned runner; nothing to settle.",
				"message_type", event.MessageType, "job_key", eventKey, "runner_name", event.RunnerName,
				"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if jobLeaseMissing && (event.RunnerID <= 0 || event.RunnerName == "") {
		// The fallback advances a worker on runner identity alone, so it needs
		// the whole identity; an event without one settles nothing.
		slog.Info("A runner event's job lease is already released and the event names no runner; nothing to settle.",
			"message_type", event.MessageType, "job_key", eventKey,
			"repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID)
		return nil
	}
	if jobLeaseMissing {
		// THE JOB'S OWN LEASE IS GONE, BUT THE WORKER THAT RAN IT IS NOT
		// (homelab#996). GitHub swaps jobs between workers: job A was acquired
		// by lease Y and ran on worker X. Once Y is released, A's JobCompleted
		// finds no job lease. This used to return nil right there, before the
		// runner match below, so X's lease stayed `running` forever and its slot
		// was lost. There is no acquisition to settle (Y already settled), but
		// the runner that emitted the event still advances its own lifecycle.
		slog.Info("A runner event's job lease is already released; advancing the worker that ran it.",
			"message_type", event.MessageType, "job_key", eventKey, "runner_name", event.RunnerName,
			"lease_id", observedRunner.ID, "repository_id", key.RepositoryID, "scale_set_id", key.ScaleSetID)
		return reconcileObservedRunnerTx(ctx, tx, observedRunner, event.MessageType)
	}
	var jitRunnerID int
	if err := tx.QueryRowContext(ctx, `SELECT runner_id FROM jit_transfers WHERE lease_id=? AND removal!='absent'`, observedRunner.ID).Scan(&jitRunnerID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	outcome := AcquisitionAccepted
	if event.MessageType == scaleset.MessageTypeJobCompleted {
		outcome = AcquisitionCompleted
	}
	settlement := AcquisitionSettlement{
		JobKey: eventKey, Outcome: outcome, Source: AcquisitionSourceRunnerEvent,
		RunnerLeaseID: observedRunner.ID, RunnerID: event.RunnerID, RunnerName: event.RunnerName,
	}
	if err := recordAcquisitionEvidenceTx(ctx, tx, requestLease, settlement, &observedRunner, jitRunnerID, true); err != nil {
		return err
	}
	return reconcileObservedRunnerTx(ctx, tx, observedRunner, event.MessageType)
}

// reconcileObservedRunnerTx applies execution evidence to the worker that
// actually emitted it. It never resurrects a cleanup or released lease, and
// it requires the worker resource journal before changing lifecycle phase.
func reconcileObservedRunnerTx(ctx context.Context, tx *sql.Tx, runner Lease, messageType scaleset.MessageType) error {
	var workerPhase string
	if err := tx.QueryRowContext(ctx, `SELECT phase FROM worker_resources WHERE lease_id=?`, runner.ID).Scan(&workerPhase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	if workerPhase != "present" && workerPhase != "destroy-intent" {
		return nil
	}
	var from, to Phase
	switch messageType {
	case scaleset.MessageTypeJobStarted:
		from, to = Provisioning, Running
	case scaleset.MessageTypeJobCompleted:
		return changedZeroOrOne(tx.ExecContext(ctx, `UPDATE leases SET phase=?,updated=? WHERE id=? AND phase IN (?,?)`,
			CleanupPending, timestamp(), runner.ID, Provisioning, Running))
	default:
		return nil
	}
	return changedZeroOrOne(tx.ExecContext(ctx, `UPDATE leases SET phase=?,updated=? WHERE id=? AND phase=?`, to, timestamp(), runner.ID, from))
}
