package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cameronsjo/slipway/internal/bootstrap"
)

// blockingBackend is a backend whose Create waits until its context ends, the
// way a Pod POST or a Kata boot is cut short when SIGTERM cancels the daemon.
// It is safe for concurrent creates, unlike backendFixture.
type blockingBackend struct {
	mu       sync.Mutex
	entered  chan struct{}
	destroys int
}

func (b *blockingBackend) Inventory(context.Context) (BackendInventory, error) {
	return BackendInventory{Complete: true, FreeSlots: 4}, nil
}

func (b *blockingBackend) Create(ctx context.Context, _ WorkerCreateIntent) (WorkerObservation, error) {
	b.entered <- struct{}{}
	<-ctx.Done()
	return WorkerObservation{}, ctx.Err()
}

func (b *blockingBackend) Destroy(_ context.Context, worker WorkerObservation) (WorkerAbsence, error) {
	b.mu.Lock()
	b.destroys++
	b.mu.Unlock()
	return WorkerAbsence{LeaseID: worker.LeaseID, ResourceID: worker.ResourceID, ProcessAbsent: true, StorageAbsent: true}, nil
}

// jitBroker prepares a transfer for any lease and settles registration cleanup
// the way the real broker does once the worker is proven absent.
func jitBroker(s *Store) *brokerFixture {
	broker := &brokerFixture{}
	broker.prepare = func(ctx context.Context, id string) (bootstrap.Token, error) {
		token, err := bootstrap.NewToken()
		if err != nil {
			return token, err
		}
		if err := s.Transition(ctx, id, Acquired, Provisioning); err != nil {
			return token, err
		}
		hash := token.Hash()
		now := timestamp()
		_, err = s.db.Exec(`INSERT INTO jit_transfers(lease_id,token_hash,phase,created,expires,updated)
VALUES(?,?,'ready',?,?,?)`, id, hash[:], now, time.Now().Add(workerStartupTimeout).UTC().Format(time.RFC3339Nano), now)
		return token, err
	}
	broker.cleanup = func(_ context.Context, id string, absent bool) (bool, error) {
		if !absent {
			return false, nil
		}
		_, err := s.db.Exec(`UPDATE jit_transfers SET removal='absent' WHERE lease_id=?`, id)
		return err == nil, err
	}
	return broker
}

func failureRows(t *testing.T, s *Store) (int, string) {
	t.Helper()
	var rows int
	var until string
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM provisioning_failures`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT provisioning_quarantine_until FROM platform WHERE id=1`).Scan(&until); err != nil {
		t.Fatal(err)
	}
	return rows, until
}

// homelab#1196 review I1. A SIGTERM during a four-wide burst cancels four
// creates at once. Each used to write a provisioning_failures row, four is
// past the threshold of three, and the breaker it set lives in SQLite, so the
// new pod started quarantined for fifteen minutes. A shutdown writes no rows,
// and every lease it interrupted is still retired and released by Maintain.
func TestAShutdownDuringABurstWritesNoFailureRowsAndStillCleansUp(t *testing.T) {
	s, _ := openTestStore(t, 4)
	leases := acquiredLeases(t, s, 4)
	var queueMu sync.Mutex
	queue := append([]Lease{}, leases...)
	allocator := allocatorFunc(func(context.Context) (Lease, error) {
		queueMu.Lock()
		defer queueMu.Unlock()
		next := queue[0]
		queue = queue[1:]
		return next, nil
	})
	backend := &blockingBackend{entered: make(chan struct{}, len(leases))}
	lifecycle, err := newPoolLifecycle(s, allocator, jitBroker(s), backend, "https://controller.example")
	if err != nil {
		t.Fatal(err)
	}

	daemonCtx, shutdown := context.WithCancel(ctx)
	defer shutdown()
	var wg sync.WaitGroup
	errs := make(chan error, len(leases))
	for range leases {
		wg.Go(func() {
			_, err := lifecycle.ProvisionNext(daemonCtx)
			errs <- err
		})
	}
	for range leases {
		<-backend.entered // every create is in flight
	}
	shutdown()
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted create returned %v, want context.Canceled", err)
		}
	}

	if rows, until := failureRows(t, s); rows != 0 || until != "" {
		t.Fatalf("a shutdown wrote %d provisioning_failures rows (breaker until %q), want 0 and no breaker", rows, until)
	}
	for _, lease := range leases {
		if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Quarantined {
			t.Fatalf("interrupted lease %s = %+v %v, want quarantined for cleanup", lease.ID, stored, err)
		}
	}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
			t.Fatalf("interrupted lease %s was not cleaned up: %+v %v", lease.ID, stored, err)
		}
	}
	if backend.destroys != len(leases) {
		t.Fatalf("Maintain retired %d interrupted workers, want %d", backend.destroys, len(leases))
	}
}

// The same rule before the backend call: JIT preparation cut short by a
// shutdown fences the lease and revokes the transfer, without a failure row.
func TestAShutdownDuringJITPreparationWritesNoFailureRow(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	broker := jitBroker(s)
	lifecycle.broker = broker
	daemonCtx, shutdown := context.WithCancel(ctx)
	defer shutdown()
	prepare := broker.prepare
	broker.prepare = func(callCtx context.Context, id string) (bootstrap.Token, error) {
		if _, err := prepare(callCtx, id); err != nil {
			return bootstrap.Token{}, err
		}
		shutdown()
		return bootstrap.Token{}, callCtx.Err()
	}
	if _, err := lifecycle.ProvisionNext(daemonCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted preparation returned %v, want context.Canceled", err)
	}
	if rows, until := failureRows(t, s); rows != 0 || until != "" {
		t.Fatalf("a shutdown during JIT preparation wrote %d rows (breaker until %q), want 0", rows, until)
	}
	var revoked int
	if err := s.db.QueryRow(`SELECT revoked FROM jit_transfers WHERE lease_id=?`, lease.ID).Scan(&revoked); err != nil || revoked != 1 {
		t.Fatalf("interrupted preparation left the transfer revoked=%d: %v", revoked, err)
	}
	backend.inventory = BackendInventory{Complete: true, FreeSlots: 1}
	if err := lifecycle.ReconcileInventory(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Released {
		t.Fatalf("interrupted preparation was not cleaned up: %+v %v", stored, err)
	}
}

// expiringContext reports context.DeadlineExceeded once expire is called, and
// never before. A real WithTimeout races the setup work ahead of Create under
// load; this fires only when the test says so.
type expiringContext struct {
	context.Context
	expire context.CancelFunc
}

func (c expiringContext) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

// A deadline is not a shutdown. workerStartupTimeout expiring means a create
// really was too slow, and that still counts toward the breaker. The deadline
// fires from inside Create, so it cannot land before the create is reached.
func TestACreatePastItsDeadlineStillWritesAFailureRow(t *testing.T) {
	s, _ := openTestStore(t, 1)
	lease := acquiredLease(t, s)
	_, backend, lifecycle := lifecycleFixtures(t, s, lease)
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	slowCtx := expiringContext{Context: child, expire: cancel}
	backend.create = func(callCtx context.Context, _ WorkerCreateIntent) (WorkerObservation, error) {
		slowCtx.expire()
		<-callCtx.Done()
		return WorkerObservation{}, callCtx.Err()
	}
	if _, err := lifecycle.ProvisionNext(slowCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow create returned %v, want context.DeadlineExceeded", err)
	}
	if rows, _ := failureRows(t, s); rows != 1 {
		t.Fatalf("a create past its deadline wrote %d failure rows, want 1", rows)
	}
}
