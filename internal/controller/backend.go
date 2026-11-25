package controller

import (
	"context"
	"errors"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

// WorkerObservation is emitted only by a trusted platform adapter after listing
// the platform's complete owned resource scope. ResourceID is chosen before
// creation and must remain equal to the lease's durable runner/resource name.
type WorkerObservation struct {
	LeaseID    string
	ResourceID string
	// Terminated is true only when the adapter positively saw this worker's
	// runner process finish. False means running, starting, or not measured --
	// never "probably done". An adapter that cannot tell leaves it false, which
	// keeps that platform on the five-minute unused-worker timeout alone.
	Terminated bool
	// ExitCode accompanies Terminated and is meaningless without it. A runner
	// that is offered no job exits 0, which is why a zero here is a diagnosis
	// rather than a success.
	ExitCode int
}

// BackendInventory covers VM processes and every writable worker storage
// resource, not merely Kubernetes Pods or Tart's controller database. FreeSlots
// is the number of additional workers that fit current CPU, memory, disk and
// platform overhead. The controller still applies its qualification ceiling.
type BackendInventory struct {
	Complete  bool
	FreeSlots int
	// Binding names which dimension is short when FreeSlots is low. The
	// controller never interprets it -- it is an adapter constant carried
	// through to the cycle line so an operator is told cpu from memory from
	// storage rather than being handed a zero.
	Binding string
	Workers []WorkerObservation
}

// WorkerCreateIntent carries durable identities, public bootstrap routing and
// the one-use per-lease capability. The adapter must inject the capability only
// into that worker and never return it from Inventory or persist it after use.
type WorkerCreateIntent struct {
	LeaseID         string
	ResourceID      string
	Slot            int
	BootstrapURL    string
	BootstrapToken  bootstrap.Token
	StartupDeadline time.Time
}

// WorkerAbsence is positive cleanup evidence from the trusted adapter. Both the
// process/VM and all writable storage must be absent before it can contribute to
// Store.FinishCleanup; a timeout or API 404 alone is not this observation.
type WorkerAbsence struct {
	LeaseID       string
	ResourceID    string
	ProcessAbsent bool
	StorageAbsent bool
}

// WorkerBackend is shared by Kubernetes and Tart adapters. Create is a
// single-attempt operation: callers journal intent first and reconcile an
// uncertain result through Inventory instead of replaying it. Destroy is
// journaled first too, but Maintain sends it again for a worker a complete
// inventory still lists after redeleteBackoff (homelab#1094), so an adapter's
// Destroy must be exact and must read an already-absent worker as absence,
// never as an error it cannot recover from.
type WorkerBackend interface {
	Inventory(context.Context) (BackendInventory, error)
	Create(context.Context, WorkerCreateIntent) (WorkerObservation, error)
	Destroy(context.Context, WorkerObservation) (WorkerAbsence, error)
}

// InventoryObservation is the last backend inventory this process saw. It is
// deliberately process-local: ReconcileBackendInventory persists none of it,
// and the admission fence it describes resets on restart too, so a persisted
// copy would outlive its own validity and read healthy for a controller that
// has established nothing since it started.
//
// UntrackedSeen is a boolean and not a count because ReconcileBackendInventory
// returns on the first untracked resource — a count would read one for any
// number of them. Any true value fences the whole controller and never
// self-resolves.
//
// Diagnostic is drawn from a fixed vocabulary this package owns. No adapter
// error text and no storage message reaches it.
type InventoryObservation struct {
	Complete      bool
	FreeSlots     int
	ObservedAt    string
	UntrackedSeen bool
	Diagnostic    string
	// Binding names the dimension that ran out first, straight from the
	// adapter's closed vocabulary. `FreeSlots: 0` alone cannot tell a full thin
	// pool from a node whose memory is committed to idle CronJob limits.
	Binding string
}

// Inventory reports that observation. It never blocks on the journal.
func (s *Store) Inventory() InventoryObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observation
}

// Inventory diagnostics. These are the composer's whole vocabulary for this
// section; the adapter's own error is never one of them.
const (
	inventoryIncomplete = "inventory-incomplete"
	inventoryUntracked  = "untracked-resource"
	inventoryIdentity   = "duplicate-or-empty-identity"
	inventoryOverdrawn  = "obligations-exceed-headroom"
	inventoryDeadline   = "invalid-quarantine-deadline"
	inventoryJournal    = "journal-read-failed"
)

// InventoryDiagnostics is that vocabulary, exported so a consumer can check a
// diagnostic against it rather than restating the six values. The monitoring
// report publishes this field, and "the adapter's own error is never one of
// them" is a promise made here and kept at the publication point: anything not
// in this list is collapsed there rather than printed.
//
// Exported as the values themselves, not as copies -- a second list of the same
// six strings is exactly the drift these constants exist to prevent.
var InventoryDiagnostics = []string{
	inventoryIncomplete,
	inventoryUntracked,
	inventoryIdentity,
	inventoryOverdrawn,
	inventoryDeadline,
	inventoryJournal,
}

// observe records the outcome of one inventory reconciliation. The caller holds
// s.mu. A refusal is recorded as fully as a success, because a fenced
// controller is exactly the state the report exists to make visible.
func (s *Store) observe(inventory BackendInventory, untracked bool, diagnostic string) {
	s.observation = InventoryObservation{
		Complete: inventory.Complete && diagnostic == "", FreeSlots: inventory.FreeSlots,
		ObservedAt: timestamp(), UntrackedSeen: untracked, Diagnostic: diagnostic,
		Binding: inventory.Binding,
	}
	if inventory.FreeSlots < 0 {
		s.observation.FreeSlots = 0
	}
}

// ReconcileBackendInventory establishes both identity completeness and current
// admission headroom. Recorded leases omitted from the listing remain charged;
// any untracked or renamed resource fences all new sessions and reservations.
func (s *Store) ReconcileBackendInventory(ctx context.Context, inventory BackendInventory) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = false
	s.admission = 0
	// Cleared with the fence it travels with. A fenced controller has proved
	// nothing about any worker, and retiring a lease on a stale reading would be
	// acting on exactly the evidence the fence says not to trust.
	s.terminated = map[string]int{}
	// Proof is cleared with the fence too, but the tracking itself is kept: an
	// incomplete inventory neither advances a worker's grace nor resets it.
	s.vanishedProven = nil
	if !inventory.Complete || inventory.FreeSlots < 0 {
		s.observe(inventory, false, inventoryIncomplete)
		return ErrInventoryUnknown
	}
	seenLeases := map[string]bool{}
	seenResources := map[string]bool{}
	// Rebuilt from this listing alone, never merged with the last one: a worker
	// missing from the current inventory is not still terminated, it is gone.
	terminated := map[string]int{}
	for _, worker := range inventory.Workers {
		if worker.LeaseID == "" || worker.ResourceID == "" || seenLeases[worker.LeaseID] || seenResources[worker.ResourceID] {
			s.observe(inventory, false, inventoryIdentity)
			return errors.New("backend inventory contains an empty or duplicate identity; retain platform fence")
		}
		lease, err := s.Lease(ctx, worker.LeaseID)
		physicalPhase := err == nil && (lease.Phase == Provisioning || lease.Phase == Running ||
			lease.Phase == CleanupPending || lease.Phase == Quarantined)
		record, recordErr := s.Worker(ctx, worker.LeaseID)
		resourcePhase := recordErr == nil && (record.Phase == "create-intent" || record.Phase == "present" || record.Phase == "destroy-intent")
		if !physicalPhase || !resourcePhase || lease.RunnerName != worker.ResourceID || record.ResourceID != worker.ResourceID {
			s.observe(inventory, true, inventoryUntracked)
			return errors.New("backend inventory contains an untracked, released, or renamed resource; retain platform fence")
		}
		seenLeases[worker.LeaseID] = true
		seenResources[worker.ResourceID] = true
		if worker.Terminated {
			terminated[worker.LeaseID] = worker.ExitCode
		}
	}
	if err := ctx.Err(); err != nil {
		s.observe(inventory, false, inventoryJournal)
		return err
	}
	vanished, vanishedProven, err := s.trackVanishedWorkers(ctx, seenLeases, time.Now().UTC())
	if err != nil {
		s.observe(inventory, false, inventoryJournal)
		return err
	}
	var active int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE phase!='released'`).Scan(&active); err != nil {
		s.observe(inventory, false, inventoryJournal)
		return err
	}
	var quarantineUntil string
	if err := s.db.QueryRowContext(ctx, `SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&quarantineUntil); err != nil {
		s.observe(inventory, false, inventoryJournal)
		return err
	}
	usable := s.config.Capacity - s.config.LegacySlots
	admission := min(usable, active+inventory.FreeSlots)
	if quarantineUntil != "" {
		until, err := time.Parse(time.RFC3339Nano, quarantineUntil)
		if err != nil {
			s.observe(inventory, false, inventoryDeadline)
			return errors.New("invalid provisioning quarantine deadline; retain platform fence")
		}
		if time.Now().Before(until) {
			admission = active
		} else if _, err := s.db.ExecContext(ctx, `UPDATE platform SET provisioning_quarantine_until='' WHERE id=1 AND provisioning_quarantine_until=?`, quarantineUntil); err != nil {
			s.observe(inventory, false, inventoryJournal)
			return err
		}
	}
	if active > admission {
		s.observe(inventory, false, inventoryOverdrawn)
		return errors.New("existing obligations exceed current backend headroom; retain platform fence")
	}
	s.observe(inventory, false, "")
	s.admission = admission
	s.terminated = terminated
	s.vanished, s.vanishedProven = vanished, vanishedProven
	// The latch clears on an OBSERVED free slot, which is the only evidence
	// that the shortage which set it has passed. Anything weaker -- a timer, a
	// successful cycle, a restart of the loop -- would clear it on hope.
	if inventory.FreeSlots >= 1 {
		s.capacityWait = false
	}
	s.ready = true
	return nil
}
