package controller

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/bootstrap"
	"github.com/cameronsjo/slipway/internal/githubapp"
)

type jitAPI interface {
	GenerateJitRunnerConfig(context.Context, *scaleset.RunnerScaleSetJitRunnerSetting, int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(context.Context, string) (*scaleset.RunnerReference, error)
	RemoveRunner(context.Context, int64) error
}

// JITRecord contains only reconciliation metadata. Payload and bearer token are
// never journaled. A creation intent with no runner ID remains uncertain even
// after an empty lookup; service-side creation may still complete later.
type JITRecord struct {
	LeaseID, Phase, Removal, Diagnostic string
	Created, Expires, Updated           string
	RunnerID                            int
	Revoked                             bool
}

func (s *Store) JIT(ctx context.Context, id string) (JITRecord, error) {
	var record JITRecord
	err := s.db.QueryRowContext(ctx, `SELECT lease_id,phase,removal,diagnostic,created,expires,updated,runner_id,revoked FROM jit_transfers WHERE lease_id=?`, id).
		Scan(&record.LeaseID, &record.Phase, &record.Removal, &record.Diagnostic, &record.Created, &record.Expires, &record.Updated, &record.RunnerID, &record.Revoked)
	return record, err
}

type jitSecret struct {
	hash    [32]byte
	payload []byte
	expires time.Time
}

// JITBroker owns trusted-side registration and an HTTPS worker handoff handler.
// One broker belongs to one Store/process. Restart deliberately loses payloads;
// durable transfer records prohibit generating or delivering them again.
// The production server must provide TLS, header/write deadlines and bounded
// connections; exposing this handler alone is not network-isolation acceptance.
type JITBroker struct {
	store      *Store
	client     func(context.Context, Lease) (jitAPI, error)
	operations [64]sync.Mutex // Bounded locks; unrelated leases usually proceed independently.
	mu         sync.Mutex
	secrets    map[string]*jitSecret
	counters   BootstrapCounters
}

// BootstrapCounters is capability issuance as counts only. It names no lease,
// capability, runner or payload: the whole point of the bootstrap contract is
// that those values do not leave the trusted process, and a monitoring body is
// outside it.
//
// Delivered, Expired and Failed are cumulative since process start, so a rate
// is the difference between two reports and a restart legitimately zeroes them.
// Outstanding is a live count, not a counter.
type BootstrapCounters struct {
	// Outstanding capabilities are issued, unexpired and unconsumed.
	Outstanding int
	// Delivered means the payload bytes were written to a worker. That is
	// transport progress, not proof that the runner executed.
	Delivered int
	// Expired counts capabilities forgotten at their startup deadline without
	// ever being collected.
	Expired int
	// Failed counts issuance and delivery outcomes the broker recorded as
	// failures, whatever the stage.
	Failed int
}

// Counters reports capability issuance. Outstanding is read live; the rest are
// cumulative since process start.
func (b *JITBroker) Counters() BootstrapCounters {
	b.mu.Lock()
	defer b.mu.Unlock()
	counters := b.counters
	counters.Outstanding = len(b.secrets)
	return counters
}

func (b *JITBroker) count(delivered, expired, failed int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.counters.Delivered += delivered
	b.counters.Expired += expired
	b.counters.Failed += failed
}

func NewJITBroker(s *Store, app *githubapp.App, installation githubapp.Installation) (*JITBroker, error) {
	if s == nil || app == nil || installation.ID != s.config.InstallationID {
		return nil, errors.New("JIT App installation differs from journal")
	}
	return newJITBroker(s, func(ctx context.Context, lease Lease) (jitAPI, error) {
		var repo githubapp.Repository
		// Cleanup reads the recorded repository, but ScaleSetClient refuses one
		// absent from the App's last complete discovery, so cleanup after a
		// revoked or unrefreshed grant fails at "JIT cleanup client unavailable".
		// That retains the obligation instead of inventing absence.
		err := s.db.QueryRowContext(ctx, `SELECT id,full_name,private FROM repositories WHERE id=?`, lease.Set.RepositoryID).
			Scan(&repo.ID, &repo.FullName, &repo.Private)
		if err != nil {
			return nil, err
		}
		return app.ScaleSetClient(installation, repo)
	}), nil
}

func newJITBroker(s *Store, client func(context.Context, Lease) (jitAPI, error)) *JITBroker {
	return &JITBroker{store: s, client: client, secrets: map[string]*jitSecret{}}
}

func (b *JITBroker) operation(id string) *sync.Mutex {
	hash := sha256.Sum256([]byte(id))
	return &b.operations[int(hash[0])%len(b.operations)]
}

func (b *JITBroker) discard(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if secret := b.secrets[id]; secret != nil {
		clear(secret.payload)
		delete(b.secrets, id)
	}
}

// Expire forgets undelivered payloads at their startup deadline. Call it from
// the lifecycle tick; journaled obligations remain for the cleanup reconciler.
func (b *JITBroker) Expire() {
	b.mu.Lock()
	var expired []string
	for id, secret := range b.secrets {
		if !time.Now().Before(secret.expires) {
			expired = append(expired, id)
		}
	}
	b.mu.Unlock()
	for _, id := range expired {
		op := b.operation(id)
		op.Lock()
		b.discard(id)
		op.Unlock()
	}
	b.count(0, len(expired), 0)
}

func (b *JITBroker) fail(ctx context.Context, id, diagnostic string) error {
	b.count(0, 0, 1)
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	err := changedOne(b.store.db.ExecContext(saveCtx, `UPDATE jit_transfers SET diagnostic=?,updated=? WHERE lease_id=?`, diagnostic, timestamp(), id))
	return errors.Join(errors.New(diagnostic), err)
}

// beginJIT charges provisioning and persists the token digest atomically. A
// second call cannot replace a token or create a second registration intent.
func (s *Store) beginJIT(ctx context.Context, id string, hash [32]byte, expires time.Time) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return Lease{}, ErrInventoryUnknown
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return Lease{}, err
	}
	lease, err := s.Lease(ctx, id)
	if err != nil {
		return Lease{}, err
	}
	if err := s.managedCurrent(ctx, lease.Set); err != nil {
		return Lease{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback()
	now := timestamp()
	if err := changedOne(tx.ExecContext(ctx, `UPDATE leases SET phase='provisioning',updated=? WHERE id=? AND phase='acquired'
AND EXISTS(SELECT 1 FROM repositories r WHERE r.id=leases.repo_id AND r.eligible=1)`, now, id)); err != nil {
		return Lease{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jit_transfers(lease_id,token_hash,phase,created,expires,updated) VALUES(?,?,'planned',?,?,?)`,
		id, hash[:], now, expires.UTC().Format(time.RFC3339Nano), now); err != nil {
		return Lease{}, err
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, err
	}
	lease.Phase, lease.Updated = Provisioning, now
	return lease, nil
}

func (s *Store) jitIntent(ctx context.Context, lease Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return ErrInventoryUnknown
	}
	if err := s.discoveryCurrent(ctx); err != nil {
		return err
	}
	return changedOne(s.db.ExecContext(ctx, `UPDATE jit_transfers SET phase='jit-intent',updated=? WHERE lease_id=? AND phase='planned' AND revoked=0
AND EXISTS(SELECT 1 FROM leases l JOIN repositories r ON r.id=l.repo_id WHERE l.id=jit_transfers.lease_id AND l.phase='provisioning' AND r.eligible=1)
AND NOT EXISTS(SELECT 1 FROM managed_sets m WHERE m.repo_id=? AND (m.phase!='active' OR m.remote_id!=?))`,
		timestamp(), lease.ID, lease.Set.RepositoryID, lease.Set.ScaleSetID))
}

func validRunner(runner *scaleset.RunnerReference, lease Lease, knownID int) bool {
	return runner != nil && runner.ID > 0 && runner.Name == lease.RunnerName && runner.RunnerScaleSetID == lease.Set.ScaleSetID &&
		(knownID == 0 || runner.ID == knownID)
}

// Prepare returns only a per-worker bootstrap token, never JIT or App material.
// The backend must still journal/check its creation intent before starting a VM.
func (b *JITBroker) Prepare(ctx context.Context, id string) (bootstrap.Token, error) {
	op := b.operation(id)
	op.Lock()
	defer op.Unlock()
	var empty bootstrap.Token
	token, err := bootstrap.NewToken()
	if err != nil {
		return empty, err
	}
	expires := time.Now().Add(bootstrap.Lifetime)
	lease, err := b.store.beginJIT(ctx, id, token.Hash(), expires)
	if err != nil {
		return empty, err
	}
	api, err := b.client(ctx, lease)
	if err != nil {
		return empty, b.fail(ctx, id, "JIT repository client unavailable")
	}
	existing, err := api.GetRunnerByName(ctx, lease.RunnerName)
	if err != nil {
		return empty, b.fail(ctx, id, "JIT preflight lookup failed")
	}
	if existing != nil {
		return empty, b.fail(ctx, id, "runner name existed before JIT intent; refuse adoption")
	}
	if err := b.store.jitIntent(ctx, lease); err != nil {
		return empty, err
	}
	config, apiErr := api.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: lease.RunnerName, WorkFolder: "_work"}, lease.Set.ScaleSetID)
	if apiErr != nil || config == nil || !validRunner(config.Runner, lease, 0) {
		return empty, b.fail(ctx, id, "JIT creation outcome unknown; retain intent and reservation")
	}
	// Persist an observed identity even when its payload is invalid or the caller
	// was canceled. This permits exact registration cleanup without another POST.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if err := changedOne(b.store.db.ExecContext(saveCtx, `UPDATE jit_transfers SET runner_id=?,phase='jit-observed',updated=? WHERE lease_id=? AND phase='jit-intent'`,
		config.Runner.ID, timestamp(), id)); err != nil {
		return empty, err
	}
	payload := []byte(config.EncodedJITConfig)
	config.EncodedJITConfig = "" // Go cannot promise erasure of SDK string copies.
	if !bootstrap.ValidJIT(payload) || ctx.Err() != nil || !time.Now().Before(expires) {
		clear(payload)
		return empty, b.fail(ctx, id, "JIT payload invalid, expired, or canceled; cleanup required")
	}
	if err := changedOne(b.store.db.ExecContext(ctx, `UPDATE jit_transfers SET phase='ready',updated=? WHERE lease_id=? AND phase='jit-observed' AND revoked=0
AND EXISTS(SELECT 1 FROM leases l WHERE l.id=jit_transfers.lease_id AND l.phase='provisioning')`, timestamp(), id)); err != nil {
		clear(payload)
		return empty, err
	}
	b.mu.Lock()
	b.secrets[id] = &jitSecret{hash: token.Hash(), payload: payload, expires: expires}
	b.mu.Unlock()
	return token, nil
}

func (b *JITBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	refuse := func() { http.Error(w, "bootstrap unavailable", http.StatusForbidden) }
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/leases/"), "/jit")
	path, err := bootstrap.Path(id)
	if err != nil || r.TLS == nil || r.Method != http.MethodGet || r.URL.Path != path || r.URL.RawPath != "" ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 ||
		len(r.Header.Values("Authorization")) != 1 {
		refuse()
		return
	}
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		refuse()
		return
	}
	token, err := bootstrap.ParseToken(strings.TrimPrefix(header, "Bearer "))
	if err != nil {
		refuse()
		return
	}
	hash := token.Hash()
	// Invalid capabilities neither wait on a worker operation nor touch SQLite.
	b.mu.Lock()
	secret := b.secrets[id]
	valid := secret != nil && subtle.ConstantTimeCompare(hash[:], secret.hash[:]) == 1
	b.mu.Unlock()
	if !valid {
		refuse()
		return
	}
	op := b.operation(id)
	op.Lock()
	defer op.Unlock()
	b.mu.Lock()
	secret = b.secrets[id]
	b.mu.Unlock()
	if secret == nil {
		refuse()
		return
	}
	if !time.Now().Before(secret.expires) {
		b.discard(id)
		refuse()
		return
	}
	// Commit consumption before headers/body. A partial write, lost response,
	// canceled request, or crash must never permit another delivery.
	err = changedOne(b.store.db.ExecContext(r.Context(), `UPDATE jit_transfers SET phase='stream-intent',updated=? WHERE lease_id=? AND phase='ready' AND revoked=0 AND token_hash=?
AND EXISTS(SELECT 1 FROM leases l WHERE l.id=jit_transfers.lease_id AND l.phase='provisioning')`, timestamp(), id, hash[:]))
	if err != nil {
		b.discard(id)
		refuse()
		return
	}
	defer b.discard(id)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(secret.payload)))
	n, writeErr := w.Write(secret.payload)
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), persistenceTimeout)
	defer cancel()
	if writeErr != nil || n != len(secret.payload) {
		_ = b.fail(saveCtx, id, "JIT stream interrupted; capability consumed and cleanup required")
		return
	}
	// This means the HTTP writer accepted bytes, not runner/job execution proof.
	if err := changedOne(b.store.db.ExecContext(saveCtx, `UPDATE jit_transfers SET phase='stream-written',updated=? WHERE lease_id=? AND phase='stream-intent'`, timestamp(), id)); err != nil {
		_ = b.fail(saveCtx, id, "JIT stream outcome could not be committed; capability remains consumed")
		return
	}
	b.count(1, 0, 0)
}

// CleanupRegistration is trusted-side only. workerAbsent must come from the
// backend's physical VM/storage reconciliation, never a worker callback or just
// Pod absence. It revokes delivery, removes only a positively identified owned
// registration, and reads back absence. It never releases the lease or settles
// its acquisition; those are independent lifecycle obligations.
func (b *JITBroker) CleanupRegistration(ctx context.Context, id string, workerAbsent bool) (bool, error) {
	op := b.operation(id)
	op.Lock()
	defer op.Unlock()
	b.discard(id)
	lease, err := b.store.Lease(ctx, id)
	if err != nil {
		return false, err
	}
	if lease.Phase != CleanupPending && lease.Phase != Quarantined {
		return false, errors.New("lease is not in cleanup")
	}
	if err := changedOne(b.store.db.ExecContext(ctx, `UPDATE jit_transfers SET revoked=1,updated=? WHERE lease_id=?`, timestamp(), id)); err != nil {
		return false, err
	}
	if !workerAbsent {
		return false, b.fail(ctx, id, "physical worker absence is unverified; registration cleanup deferred")
	}
	record, err := b.store.JIT(ctx, id)
	if err != nil {
		return false, err
	}
	api, err := b.client(ctx, lease)
	if err != nil {
		return false, b.fail(ctx, id, "JIT cleanup client unavailable")
	}
	runner, err := api.GetRunnerByName(ctx, lease.RunnerName)
	if err != nil {
		return false, b.fail(ctx, id, "JIT cleanup lookup failed")
	}
	if runner == nil {
		if record.RunnerID == 0 && record.Phase != "planned" {
			if !unknownJITCreationSettled(record, time.Now()) {
				return false, b.fail(ctx, id, "empty lookup cannot settle unknown JIT creation")
			}
			// homelab#1095 (D9): see unknownJITCreationSettled.
			slog.Warn("An uncertain JIT creation is settled as absent: the runner lookup is still empty long after the POST.",
				"lease_id", id, "runner_name", lease.RunnerName, "jit_phase", record.Phase, "jit_created", record.Created,
				"window", unknownJITSettleWindow.String())
		}
		return b.markRunnerAbsent(ctx, id)
	}
	if record.Phase == "planned" || !validRunner(runner, lease, record.RunnerID) || record.Removal == "absent" {
		return false, b.fail(ctx, id, "runner cleanup identity collision; refuse removal")
	}
	if record.Removal == "remove-intent" {
		// REACHED ONLY WITH THE RUNNER STILL PRESENT, and that is why it stays
		// a refusal. The `runner == nil` branch above already records absence
		// for a lease whose delete did take effect -- `record.RunnerID` is
		// non-zero by then, so it takes the markRunnerAbsent path -- and a
		// failed lookup returns earlier still. So the only way here is: the
		// registration is genuinely alive after a delete that did not land.
		//
		// The next cycle's own fresh GetRunnerByName IS the bounded re-check;
		// it recovers the moment the runner is actually gone. What was missing
		// was never a re-check. It was that this refusal used to abort
		// `Maintain` for EVERY other lease -- fixed at the call site, not here.
		// The delete is still never replayed blindly.
		return false, b.fail(ctx, id, "runner removal remains uncertain; the registration is still present")
	}
	if err := changedOne(b.store.db.ExecContext(ctx, `UPDATE jit_transfers SET runner_id=?,removal='remove-intent',updated=? WHERE lease_id=? AND removal=''`, runner.ID, timestamp(), id)); err != nil {
		return false, err
	}
	// Even a failed DELETE may have taken effect. Read back instead of replaying.
	_ = api.RemoveRunner(ctx, int64(runner.ID))
	runner, err = api.GetRunnerByName(ctx, lease.RunnerName)
	if err != nil || runner != nil {
		return false, b.fail(ctx, id, "runner removal absence unverified; retain reservation")
	}
	return b.markRunnerAbsent(ctx, id)
}

func (b *JITBroker) markRunnerAbsent(ctx context.Context, id string) (bool, error) {
	err := changedOne(b.store.db.ExecContext(ctx, `UPDATE jit_transfers SET removal='absent',diagnostic='',updated=? WHERE lease_id=?`, timestamp(), id))
	return err == nil, err
}
