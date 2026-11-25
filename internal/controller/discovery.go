package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
)

const discoveryInterval = time.Minute

// DiscoveryFreshness is the window admission requires of the App snapshot. It
// is exported so the discovery report can publish the threshold beside the
// boolean it produces, rather than a reader trusting an unstated constant.
const DiscoveryFreshness = 3 * discoveryInterval

const discoveryFreshness = DiscoveryFreshness

type DiscoveryStatus struct {
	LastSuccess string
	LastAttempt string
	Diagnostic  string
}

func (s *Store) DiscoveryStatus(ctx context.Context) (DiscoveryStatus, error) {
	var status DiscoveryStatus
	err := s.db.QueryRowContext(ctx, `SELECT last_success,last_attempt,diagnostic FROM discovery_state WHERE id=1`).Scan(
		&status.LastSuccess, &status.LastAttempt, &status.Diagnostic)
	return status, err
}

func (s *Store) discoveryCurrent(ctx context.Context) error {
	status, err := s.DiscoveryStatus(ctx)
	if err != nil {
		return err
	}
	observed, err := time.Parse(time.RFC3339Nano, status.LastSuccess)
	age := time.Since(observed)
	if err != nil || age < 0 || age >= discoveryFreshness || status.Diagnostic != "" {
		return ErrDiscoveryStale
	}
	return nil
}

func (s *Store) discoveryFailed(ctx context.Context, diagnostic string) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	_, err := s.db.ExecContext(saveCtx, `UPDATE discovery_state SET last_attempt=?,diagnostic=? WHERE id=1`, timestamp(), diagnostic)
	return errors.Join(errors.New(diagnostic), err)
}

// parseRepositoryAllowlist normalizes and validates the configured repository
// allowlist. A malformed entry is an error at startup rather than a silently
// dropped pin. An EMPTY list is legal and means "every private, active
// repository the App installation can see" -- see eligibleForPool for why that
// is safe and what still refuses.
func parseRepositoryAllowlist(entries []string) (map[string]bool, error) {
	allowlist := map[string]bool{}
	for _, entry := range entries {
		name := strings.ToLower(strings.TrimSpace(entry))
		if name == "" {
			continue
		}
		owner, repo, found := strings.Cut(name, "/")
		if !found || owner == "" || repo == "" || strings.Contains(repo, "/") {
			return nil, fmt.Errorf("repository allowlist entry %q is not an owner/name pair", entry)
		}
		allowlist[name] = true
	}
	return allowlist, nil
}

// eligibleForPool decides whether one discovered repository may receive an
// slipway-linux scale set. The allowlist selects; the rest of the rule refuses, and
// the refusals are unconditional:
//
//   - The repository must be private. ADR-0003 forbids a self-hosted runner on a
//     public repository because a fork pull request is attacker-authored by
//     definition. githubapp.Repository.Private is a plain bool, so a response
//     with the field missing decodes as false and is treated as public.
//   - The repository must not be archived or disabled.
//   - Refresh separately refuses any discovered repository outside the
//     installation account, so owner is not re-derived here.
//
// An EMPTY allowlist means every private, active repository in the App
// installation, which is the operator decision that onboarding a private repo
// must not need a homelab PR. It is not "no filter": the private-only,
// archived, disabled and owner refusals above are the security boundary and
// none of them is conditional on the list. A NON-EMPTY allowlist keeps its
// original pin semantics, and membership alone is still not sufficient -- a
// public repository placed in the allowlist is refused, because the allowlist
// is an operator's convenience and ADR-0003 is not.
//
// The unbounded-onboarding risk an empty list used to carry is answered by
// bounds rather than by refusal: Registry.Reconcile creates at most
// createBudget scale sets per cycle and serves at most Config.MaxRepositories
// repositories at once.
func eligibleForPool(repo githubapp.Repository, allowlist map[string]bool) bool {
	if !repo.Private || repo.Archived || repo.Disabled {
		return false
	}
	if len(allowlist) == 0 {
		return true
	}
	return allowlist[strings.ToLower(repo.FullName)]
}

// RepositoryState is read directly from discovery/ownership state for the
// orchestrator and monitoring. It needs no hand-maintained repository list.
type RepositoryState struct {
	Repository githubapp.Repository
	Eligible   bool
	Discovered string
	Name       string
	ScaleSetID int
	Phase      string
	Diagnostic string
	Observed   string
}

func (s *Store) Repositories(ctx context.Context) ([]RepositoryState, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.full_name,r.private,r.archived,r.disabled,r.eligible,r.discovered,
COALESCE(m.name,''),COALESCE(ss.set_id,0),COALESCE(m.phase,''),COALESCE(m.diagnostic,''),COALESCE(m.updated,'')
FROM repositories r LEFT JOIN managed_sets m ON m.repo_id=r.id LEFT JOIN scale_sets ss ON ss.repo_id=r.id ORDER BY r.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var repos []RepositoryState
	for rows.Next() {
		var repo RepositoryState
		if err := rows.Scan(&repo.Repository.ID, &repo.Repository.FullName, &repo.Repository.Private,
			&repo.Repository.Archived, &repo.Repository.Disabled, &repo.Eligible,
			&repo.Discovered, &repo.Name, &repo.ScaleSetID, &repo.Phase, &repo.Diagnostic, &repo.Observed); err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, rows.Err()
}

type discoveryAPI interface {
	InstallationFor(context.Context, string) (githubapp.Installation, error)
	Discover(context.Context, githubapp.Installation) ([]githubapp.Repository, error)
	// RepositoryEligible answers from the App's own last complete discovery,
	// which the App records before the journal commit and independently of it.
	RepositoryEligible(githubapp.Repository) bool
}

type scaleSetAPI interface {
	ListRunnerScaleSets(context.Context, int) ([]scaleset.RunnerScaleSet, error)
	GetRunnerScaleSetByID(context.Context, int) (*scaleset.RunnerScaleSet, error)
	CreateRunnerScaleSet(context.Context, *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
	// NO DELETE, deliberately. The controller has no way to remove a remote
	// scale set, so no cycle can spend an irreversible Administration-write
	// DELETE on one discovery snapshot. See Store.markSetRemoval.
}

type setClientFactory func(githubapp.Installation, githubapp.Repository) (scaleSetAPI, error)
type cachedSetClient struct {
	fullName string
	api      scaleSetAPI
}

// Registry owns discovery and repository-scoped scale-set management. Run is not
// wired to the CLI yet; calling it may create scale sets and requires the same
// deployment authority as the eventual controller. It never starts workers.
type Registry struct {
	store        *Store
	app          discoveryAPI
	bootstrap    string
	newClient    setClientFactory
	refreshMu    sync.Mutex
	reconcileMu  sync.Mutex
	mu           sync.Mutex
	installation githubapp.Installation
	// clients holds one Administration-write client per ELIGIBLE repository, and
	// an entry is dropped the moment eligibility is lost. Nothing retains a
	// client past that point: a repository that just went public would otherwise
	// keep a live Administration:write credential, which is what ADR-0003
	// refuses, and the only thing such a client could do here is a delete the
	// controller deliberately does not perform. Refresh drops entries only once
	// the journal commits, so clientFor also re-checks every cache hit against
	// the App's discovery and evicts on refusal (homelab#921).
	clients map[int64]cachedSetClient
}

func NewRegistry(s *Store, app *githubapp.App, bootstrap string) (*Registry, error) {
	if s == nil || app == nil || strings.Count(bootstrap, "/") != 1 || strings.HasPrefix(bootstrap, "/") || strings.HasSuffix(bootstrap, "/") {
		return nil, errors.New("registry requires a store, App and bootstrap repository")
	}
	return newRegistry(s, app, bootstrap, func(installation githubapp.Installation, repo githubapp.Repository) (scaleSetAPI, error) {
		return app.ScaleSetClient(installation, repo)
	}), nil
}

func newRegistry(s *Store, app discoveryAPI, bootstrap string, factory setClientFactory) *Registry {
	return &Registry{store: s, app: app, bootstrap: bootstrap, newClient: factory,
		clients: map[int64]cachedSetClient{}}
}

// Refresh commits only a complete snapshot from the selected or
// all-repositories installation.
// Failed/partial discovery preserves the old snapshot for diagnostics and fences
// new work until refresh succeeds. Silence also expires it after three minutes.
func (r *Registry) Refresh(ctx context.Context) error {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	installation, err := r.app.InstallationFor(ctx, r.bootstrap)
	if err != nil || installation.ID != r.store.config.InstallationID || (installation.Selection != "selected" && installation.Selection != "all") ||
		installation.Permissions["administration"] != "write" || !strings.EqualFold(installation.Account.Login, strings.Split(r.bootstrap, "/")[0]) {
		return r.store.discoveryFailed(ctx, "selected or all-repositories App installation could not be verified")
	}
	repos, err := r.app.Discover(ctx, installation)
	if err != nil {
		return r.store.discoveryFailed(ctx, "complete App repository discovery failed")
	}
	for _, repo := range repos {
		if !strings.EqualFold(strings.Split(repo.FullName, "/")[0], installation.Account.Login) {
			return r.store.discoveryFailed(ctx, "repository discovery returned a foreign owner")
		}
	}
	if err := r.store.Discover(ctx, repos); err != nil {
		return r.store.discoveryFailed(ctx, "repository snapshot could not be committed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.installation = installation
	eligible := map[int64]bool{}
	for _, repo := range repos {
		eligible[repo.ID] = eligibleForPool(repo, r.store.allowlist)
	}
	// Losing eligibility drops the client immediately. Nothing retains one for a
	// repository the choke point would now refuse to build one for.
	for id := range r.clients {
		if !eligible[id] {
			delete(r.clients, id)
		}
	}
	return nil
}

func (r *Registry) clientFor(repo githubapp.Repository) (scaleSetAPI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.installation.ID != r.store.config.InstallationID {
		return nil, ErrDiscoveryStale
	}
	// A cached client never went back through ScaleSetClient, so the App's
	// latest complete discovery is asked again on every use. Refresh's eviction
	// waits on the journal commit; this check does not, so a public flip the App
	// has recorded ends the client here even when that commit failed.
	if !r.app.RepositoryEligible(repo) {
		delete(r.clients, repo.ID)
		return nil, errors.New("repository not private and active in the App's last complete discovery")
	}
	if cached, ok := r.clients[repo.ID]; ok && cached.fullName == repo.FullName {
		return cached.api, nil
	}
	client, err := r.newClient(r.installation, repo)
	if err != nil {
		return nil, errors.New("repository scale-set client initialization failed")
	}
	r.clients[repo.ID] = cachedSetClient{fullName: repo.FullName, api: client}
	return client, nil
}

// createBudget is how many repositories may enter the scale-set CREATION path
// in one Reconcile. Creation is eager -- there is no webhook and the broker
// cannot offer a job to a scale set that does not exist yet, so nothing can
// make it lazy -- and with an empty allowlist the eligible set is every private
// repository in the installation at once. The budget spreads that one-time cost
// over cycles: ~70 private repositories onboard in about fourteen minutes at
// the one-minute cycle, five Administration-write POSTs at a time, instead of
// seventy in the first Refresh.
const createBudget = 5

// selectForReconcile is the whole bounding decision, with no I/O, so every case
// is table-testable. A repository that already has a managed row or a bound
// scale set is ALWAYS reconciled: it may be mid-flight with an uncertain
// creation outcome, and leaving it unvisited is how that uncertainty becomes
// permanent. Only a genuinely new repository is budgeted or capped.
//
// The cap counts established repositories first, so raising MaxRepositories
// admits new ones and lowering it never unmanages an existing one.
func selectForReconcile(repos []RepositoryState, budget, maxRepositories int) ([]RepositoryState, ManagementCycle) {
	var summary ManagementCycle
	selected := make([]RepositoryState, 0, len(repos))
	established := 0
	for _, repo := range repos {
		if repo.Eligible && (repo.Phase != "" || repo.ScaleSetID != 0) {
			established++
		}
	}
	for _, repo := range repos {
		if !repo.Eligible {
			continue
		}
		if repo.Phase != "" || repo.ScaleSetID != 0 {
			selected = append(selected, repo)
			continue
		}
		if established >= maxRepositories {
			summary.OverCap++
			// The FIRST refused repository, by discovery order. A bare count says
			// the cap is biting and nothing else; an operator needs a name to
			// decide whether to raise it, and this is the only place one exists.
			if summary.OverCapFirstName == "" {
				summary.OverCapFirstName = repo.Repository.FullName
			}
			continue
		}
		if summary.Creating >= budget {
			summary.CreateDeferred++
			continue
		}
		established++
		summary.Creating++
		selected = append(selected, repo)
	}
	return selected, summary
}

// Reconcile limits simultaneous repository API operations independently of VM
// capacity. A stalled repository cannot serialize every other repository behind
// it, and clients are reused instead of minting fresh admin credentials each tick.
func (r *Registry) Reconcile(ctx context.Context) error {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	if err := r.store.discoveryCurrent(ctx); err != nil {
		return err
	}
	repos, err := r.store.Repositories(ctx)
	if err != nil {
		return err
	}
	selected, summary := selectForReconcile(repos, createBudget, r.store.config.MaxRepositories)
	summary.Orphaned, summary.OrphanedFirstName = r.reportOrphans(ctx, repos)
	r.store.recordManagementCycle(summary)
	var wg sync.WaitGroup
	var failuresMu sync.Mutex
	var failures []error
	work := make(chan RepositoryState)
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repo := range work {
				callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := r.reconcileRepository(callCtx, repo)
				cancel()
				if err != nil {
					failuresMu.Lock()
					failures = append(failures, err)
					failuresMu.Unlock()
				}
			}
		}()
	}
send:
	for _, repo := range selected {
		select {
		case work <- repo:
		case <-ctx.Done():
			break send
		}
	}
	close(work)
	wg.Wait()
	return errors.Join(append(failures, ctx.Err())...)
}

// Run discovers immediately and every minute. Management runs independently so
// slow API readback does not delay discovery or revocation of repository grants.
// The observer receives safe diagnostic errors; an eventual daemon must expose
// these together with the durable per-repository state in its health/alerts.
// Calls to the observer are serialized across the two loops.
func (r *Registry) Run(ctx context.Context, observe func(error)) error {
	var reportMu sync.Mutex
	report := func(err error) {
		if err != nil && observe != nil {
			reportMu.Lock()
			defer reportMu.Unlock()
			observe(err)
		}
	}
	discoveryTicker := time.NewTicker(discoveryInterval)
	defer discoveryTicker.Stop()
	managementTicker := time.NewTicker(discoveryInterval)
	defer managementTicker.Stop()
	wake := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.refreshLoop(ctx, discoveryTicker.C, func(err error) {
			if err != nil {
				report(err)
				return
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		})
	}()
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-managementTicker.C:
		}
		report(r.Reconcile(ctx))
	}
}

func (r *Registry) refreshLoop(ctx context.Context, ticks <-chan time.Time, observe func(error)) {
	for {
		if ctx.Err() != nil {
			return
		}
		observe(r.Refresh(ctx))
		select {
		case <-ctx.Done():
			return
		case <-ticks:
		}
	}
}
