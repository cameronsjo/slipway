package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/google/uuid"
)

type discoveryFixture struct {
	mu           sync.Mutex
	installation githubapp.Installation
	repos        []githubapp.Repository
	err          error
	// recorded mirrors githubapp.App's discovery snapshot: set only by a
	// complete Discover, nil until the first one.
	recorded map[int64]githubapp.Repository
}

func newDiscoveryFixture() *discoveryFixture {
	d := &discoveryFixture{installation: githubapp.Installation{ID: 42, Selection: "selected", Permissions: map[string]string{"administration": "write"}}}
	d.installation.Account.Login = "owner"
	d.repos = []githubapp.Repository{testRepository(1, "owner/repo1")}
	return d
}
func (d *discoveryFixture) InstallationFor(context.Context, string) (githubapp.Installation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.installation, nil
}
func (d *discoveryFixture) Discover(context.Context, githubapp.Installation) ([]githubapp.Repository, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err == nil {
		d.recorded = map[int64]githubapp.Repository{}
		for _, repo := range d.repos {
			d.recorded[repo.ID] = repo
		}
	}
	return append([]githubapp.Repository(nil), d.repos...), d.err
}

// RepositoryEligible applies githubapp.App.RepositoryEligible's rule to the
// fixture's recorded snapshot. The rule itself is pinned in githubapp.
func (d *discoveryFixture) RepositoryEligible(repo githubapp.Repository) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, ok := d.recorded[repo.ID]
	return ok && record.FullName == repo.FullName && record.Private && !record.Archived && !record.Disabled
}
func (d *discoveryFixture) configure(fn func()) { d.mu.Lock(); defer d.mu.Unlock(); fn() }

type registryAPI struct {
	t          *testing.T
	s          *Store
	repoID     int64
	mu         sync.Mutex
	sets       []scaleset.RunnerScaleSet
	creates    int
	deletes    int
	listErr    error
	loseCreate bool
	omitCreate bool
	createHook func(*scaleset.RunnerScaleSet)
	listHook   func(context.Context)
}

func copySets(sets []scaleset.RunnerScaleSet) []scaleset.RunnerScaleSet {
	body, _ := json.Marshal(sets)
	var copy []scaleset.RunnerScaleSet
	json.Unmarshal(body, &copy)
	return copy
}
func (a *registryAPI) ListRunnerScaleSets(ctx context.Context, group int) ([]scaleset.RunnerScaleSet, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if group != personalRunnerGroup {
		a.t.Errorf("foreign runner group: %d", group)
	}
	if a.listHook != nil {
		a.listHook(ctx)
	}
	return copySets(a.sets), a.listErr
}
func (a *registryAPI) GetRunnerScaleSetByID(_ context.Context, id int) (*scaleset.RunnerScaleSet, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, set := range copySets(a.sets) {
		if set.ID == id {
			return &set, nil
		}
	}
	return nil, scaleset.NotFoundError
}
func (a *registryAPI) CreateRunnerScaleSet(_ context.Context, request *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.creates++
	own, err := a.s.managedSet(ctx, a.repoID)
	if err != nil || own.phase != "create-intent" || own.name != request.Name {
		a.t.Errorf("create preceded durable ownership: %+v %v", own, err)
	}
	set := copySets([]scaleset.RunnerScaleSet{*request})[0]
	set.ID = 100 + int(a.repoID)
	if a.createHook != nil {
		a.createHook(&set)
	}
	if !a.omitCreate {
		a.sets = append(a.sets, set)
	}
	if a.loseCreate {
		return nil, errors.New("private-api-body-sentinel")
	}
	return &set, nil
}

// DeleteRunnerScaleSet is a TRIPWIRE, not a fixture behaviour. scaleSetAPI has
// no delete method, so production cannot reach this; it exists so that
// reintroducing one -- and calling it -- fails a test rather than quietly
// spending an irreversible Administration-write call.
func (a *registryAPI) DeleteRunnerScaleSet(context.Context, int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deletes++
	a.t.Error("the controller issued a scale-set DELETE; that call is the operator's, not the daemon's")
	return nil
}
func (a *registryAPI) configure(fn func()) { a.mu.Lock(); defer a.mu.Unlock(); fn() }

type registryHarness struct {
	t            *testing.T
	s            *Store
	discovery    *discoveryFixture
	mu           sync.Mutex
	apis         map[int64]*registryAPI
	clientCounts map[int64]int
}

func newRegistryHarness(t *testing.T, s *Store) *registryHarness {
	return &registryHarness{t: t, s: s, discovery: newDiscoveryFixture(), apis: map[int64]*registryAPI{}, clientCounts: map[int64]int{}}
}
func (h *registryHarness) api(id int64) *registryAPI {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.apis[id] == nil {
		h.apis[id] = &registryAPI{t: h.t, s: h.s, repoID: id}
	}
	return h.apis[id]
}
func (h *registryHarness) registry() *Registry {
	return newRegistry(h.s, h.discovery, "owner/repo1", func(installation githubapp.Installation, repo githubapp.Repository) (scaleSetAPI, error) {
		if installation.ID != 42 {
			h.t.Errorf("foreign installation: %d", installation.ID)
		}
		api := h.api(repo.ID)
		h.mu.Lock()
		h.clientCounts[repo.ID]++
		h.mu.Unlock()
		return api, nil
	})
}
func refreshRegistry(t *testing.T, r *Registry) {
	t.Helper()
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryOnboardsFromAppGrantWithoutRepositoryConfiguration(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	h.discovery.configure(func() {
		h.discovery.repos = append(h.discovery.repos,
			testRepository(2, "owner/private"), githubapp.Repository{ID: 3, FullName: "owner/archive", Private: true, Archived: true})
	})
	r := h.registry()
	refreshRegistry(t, r)
	repos, err := s.Repositories(ctx)
	if err != nil || len(repos) != 3 {
		t.Fatalf("discovery snapshot: %+v %v", repos, err)
	}
	names := map[int64]string{}
	for _, repo := range repos {
		if repo.Repository.ID == 3 {
			if repo.Eligible || repo.ScaleSetID != 0 {
				t.Fatal("archived repository activated")
			}
			continue
		}
		if repo.Phase != "active" || repo.ScaleSetID != 100+int(repo.Repository.ID) || !strings.HasPrefix(repo.Name, "slipway-linux-") {
			t.Fatalf("repository not automatically ready: %+v", repo)
		}
		names[repo.Repository.ID] = repo.Name
		api := h.api(repo.Repository.ID)
		api.configure(func() {
			if len(api.sets[0].Labels) != 1 || api.sets[0].Labels[0].Name != "slipway-linux" {
				t.Error("workflow interface changed")
			}
		})
	}
	// App grant is the only input change; no worker, registration or monitoring
	// configuration is created for this newly selected repository.
	h.discovery.configure(func() {
		h.discovery.repos = append(h.discovery.repos, testRepository(4, "owner/new-canary"))
	})
	refreshRegistry(t, r)
	if current, err := s.managedSet(ctx, 4); err != nil || current.phase != "active" {
		t.Fatalf("new grant not onboarded: %+v %v", current, err)
	}
	for id, name := range names {
		current, err := s.managedSet(ctx, id)
		if err != nil || current.name != name {
			t.Fatalf("resource renamed: %+v %v", current, err)
		}
		api := h.api(id)
		api.configure(func() {
			if api.creates != 1 {
				t.Error("resource recreated")
			}
		})
		h.mu.Lock()
		count := h.clientCounts[id]
		h.mu.Unlock()
		if count != 1 {
			t.Fatal("admin client recreated every discovery tick")
		}
	}
}

func TestRegistryRecoversLostCreateByPersistedName(t *testing.T) {
	for _, createdRemotely := range []bool{true, false} {
		t.Run(fmt.Sprint(createdRemotely), func(t *testing.T) {
			s, dir := openTestStore(t, 1)
			h := newRegistryHarness(t, s)
			api := h.api(1)
			api.configure(func() { api.loseCreate = true; api.omitCreate = !createdRemotely })
			r := h.registry()
			if err := r.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			if err := r.Reconcile(ctx); err == nil || strings.Contains(err.Error(), "private-api-body-sentinel") {
				t.Fatalf("create uncertainty hidden/leaked: %v", err)
			}
			before, err := s.managedSet(ctx, 1)
			if err != nil || before.phase != "create-intent" || before.remoteID != 0 {
				t.Fatalf("intent lost: %+v %v", before, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := Open(dir, s.config)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			h.s = restarted
			api.configure(func() { api.s = restarted })
			r = h.registry()
			if err := r.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			err = r.Reconcile(ctx)
			if (err == nil) != createdRemotely {
				t.Fatalf("ownership recovery outcome: %v", err)
			}
			after, err := restarted.managedSet(ctx, 1)
			if err != nil || after.name != before.name || (after.phase == "active") != createdRemotely {
				t.Fatalf("ownership changed: %+v %v", after, err)
			}
			api.configure(func() {
				if api.creates != 1 {
					t.Fatal("uncertain create replayed")
				}
			})
		})
	}
}

func TestRegistryReadbackPolicyHasNoLabelFallback(t *testing.T) {
	for _, scenario := range []string{"name added", "label dropped", "extra label", "updates enabled", "duplicate label", "wrong group", "wrong name"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			h := newRegistryHarness(t, s)
			api := h.api(1)
			api.configure(func() {
				api.createHook = func(set *scaleset.RunnerScaleSet) {
					switch scenario {
					case "name added":
						set.Labels = append(set.Labels, scaleset.Label{Name: set.Name})
					case "label dropped":
						set.Labels = []scaleset.Label{{Name: set.Name}}
					case "extra label":
						set.Labels = append(set.Labels, scaleset.Label{Name: "ubuntu-latest"})
					case "updates enabled":
						set.RunnerSetting.DisableUpdate = false
					case "duplicate label":
						set.Labels = append(set.Labels, set.Labels[0])
					case "wrong group":
						set.RunnerGroupID++
					case "wrong name":
						set.Name = "unowned"
					}
				}
			})
			r := h.registry()
			if err := r.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			err := r.Reconcile(ctx)
			if (err == nil) != (scenario == "name added") {
				t.Fatalf("policy result: %v", err)
			}
			rows, err := s.Repositories(ctx)
			if err != nil || len(rows) != 1 || (rows[0].ScaleSetID != 0) != (scenario == "name added") {
				t.Fatalf("unverified set activated: %+v %v", rows, err)
			}
			api.configure(func() {
				if api.creates != 1 {
					t.Fatal("policy failure repeated create")
				}
			})
		})
	}
}

func TestRegistryOwnershipConflictsCannotBeAdopted(t *testing.T) {
	for _, scenario := range []string{"foreign label", "foreign name", "duplicate IDs", "preexisting random name", "unmanaged binding"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			h := newRegistryHarness(t, s)
			r := h.registry()
			if err := r.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			api := h.api(1)
			foreign := scaleset.RunnerScaleSet{ID: 900, Name: "someone-else", RunnerGroupID: 1}
			switch scenario {
			case "foreign label":
				foreign.Labels = []scaleset.Label{{Name: "slipway-linux"}}
			case "foreign name":
				foreign.Name = "slipway-linux"
			case "preexisting random name":
				repos, _ := s.Repositories(ctx)
				planned, err := s.planSet(ctx, repos[0])
				if err != nil {
					t.Fatal(err)
				}
				foreign.Name = planned.name
			case "unmanaged binding":
				if err := s.BindSet(ctx, SetKey{1, 999}); err != nil {
					t.Fatal(err)
				}
			}
			api.configure(func() {
				api.sets = []scaleset.RunnerScaleSet{foreign}
				if scenario == "duplicate IDs" {
					other := foreign
					other.Name = "duplicate"
					api.sets = append(api.sets, other)
				}
			})
			if err := r.Reconcile(ctx); err == nil {
				t.Fatal("ownership conflict accepted")
			}
			api.configure(func() {
				if api.creates != 0 {
					t.Fatal("created into conflicting namespace")
				}
			})
		})
	}
}

func TestRegistryOwnershipDriftFencesAcquisitionAndRecovers(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	r := h.registry()
	refreshRegistry(t, r)
	key := SetKey{1, 101}
	if err := s.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	offer(t, s, key, 2)
	lease := mustReserve(t, s)
	api := h.api(1)
	api.configure(func() { api.sets[0].ID++ })
	if err := r.Reconcile(ctx); err == nil {
		t.Fatal("incarnation drift accepted")
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrNoDemand) {
		t.Fatalf("unverified ownership allocated work: %v", err)
	}
	calls := 0
	acquire := acquireFunc(func(_ context.Context, ids []int64) ([]int64, error) { calls++; return ids, nil })
	if err := s.AcquireReserved(ctx, lease.ID, acquire); err == nil || calls != 0 {
		t.Fatal("unverified ownership acquired work")
	}
	api.configure(func() { api.sets[0].ID-- })
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.AcquireReserved(ctx, lease.ID, acquire); err != nil || calls != 1 {
		t.Fatalf("verified recovery failed: %v", err)
	}
}

func TestDiscoveryFailuresRetainSnapshotAndFenceNewWork(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	r := h.registry()
	refreshRegistry(t, r)
	key := SetKey{1, 101}
	if err := s.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	offer(t, s, key, 2)
	lease := mustReserve(t, s)
	h.discovery.configure(func() { h.discovery.repos = nil; h.discovery.err = errors.New("partial-response-private-sentinel") })
	if err := r.Refresh(ctx); err == nil || strings.Contains(err.Error(), "private-sentinel") {
		t.Fatalf("failed discovery result: %v", err)
	}
	if repos, err := s.Repositories(ctx); err != nil || len(repos) != 1 || !repos[0].Eligible {
		t.Fatalf("failed discovery fabricated removal: %+v %v", repos, err)
	}
	if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrDiscoveryStale) {
		t.Fatalf("failed discovery allowed allocation: %v", err)
	}
	if err := s.AcquireReserved(ctx, lease.ID, acquireFunc(func(context.Context, []int64) ([]int64, error) {
		t.Error("API reached after failed discovery")
		return nil, nil
	})); !errors.Is(err, ErrDiscoveryStale) {
		t.Fatalf("failed discovery acquired: %v", err)
	}
	// A complete empty snapshot is a real revocation, unlike the failed response.
	h.discovery.configure(func() { h.discovery.err = nil })
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if repos, err := s.Repositories(ctx); err != nil || len(repos) != 1 || repos[0].Eligible {
		t.Fatalf("revocation not recorded: %+v %v", repos, err)
	}
	if stored, err := s.Lease(ctx, lease.ID); err != nil || stored.Phase != Reserved {
		t.Fatalf("revocation changed the existing unacquired reservation: %+v %v", stored, err)
	}
}

func TestDiscoverySilenceAndClockErrorsExpireAdmission(t *testing.T) {
	for _, observed := range []string{time.Now().Add(-4 * time.Minute).UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), "invalid"} {
		t.Run(observed, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			key := prepare(t, s, 1)[0]
			offer(t, s, key, 1)
			if _, err := s.db.Exec(`UPDATE discovery_state SET last_success=?`, observed); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ReserveNext(ctx); !errors.Is(err, ErrDiscoveryStale) {
				t.Fatalf("stale discovery admitted work: %v", err)
			}
		})
	}
}

func TestRegistryFailedWriteAndRevocationRaceCannotCreate(t *testing.T) {
	for _, scenario := range []string{"write failure", "revoked during list", "renamed during list"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := openTestStore(t, 1)
			h := newRegistryHarness(t, s)
			r := h.registry()
			if err := r.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			api := h.api(1)
			if scenario == "write failure" {
				if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
					t.Fatal(err)
				}
			} else {
				api.configure(func() {
					api.listHook = func(context.Context) {
						var repos []githubapp.Repository
						if scenario == "renamed during list" {
							repos = []githubapp.Repository{testRepository(1, "owner/renamed")}
						}
						if err := s.Discover(ctx, repos); err != nil {
							t.Errorf("discovery race: %v", err)
						}
					}
				})
			}
			if err := r.Reconcile(ctx); err == nil {
				t.Fatal("mutation fence bypassed")
			}
			api.configure(func() {
				if api.creates != 0 {
					t.Fatal("create reached API after failed fence")
				}
			})
		})
	}
}

func TestDiscoveryTickOnboardsNewGrant(t *testing.T) {
	s, _ := openTestStore(t, 1)
	h := newRegistryHarness(t, s)
	r := h.registry()
	ticks := make(chan time.Time)
	observations := make(chan error, 2)
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); r.refreshLoop(loopCtx, ticks, func(err error) { observations <- err }) }()
	await := func() {
		t.Helper()
		select {
		case err := <-observations:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("discovery tick did not complete")
		}
	}
	await()
	h.discovery.configure(func() {
		h.discovery.repos = append(h.discovery.repos, testRepository(2, "owner/second"))
	})
	ticks <- time.Now()
	await()
	if repos, err := s.Repositories(ctx); err != nil || len(repos) != 2 {
		t.Fatalf("grant not discovered on tick: %+v %v", repos, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("discovery did not stop")
	}
}

func TestRegistryUsesNativeSDKAndStableExplicitLabel(t *testing.T) {
	s, _ := openTestStore(t, 1)
	d := newDiscoveryFixture()
	f := newSDKFixture(t, s, SetKey{1, 101})
	var remote *scaleset.RunnerScaleSet // Only accessed under the SDK fixture mutex.
	f.configure(func() {
		f.override = func(w http.ResponseWriter, req *http.Request) bool {
			const path = "/tenant/_apis/runtime/runnerscalesets"
			if req.URL.Path == path && req.Method == "GET" {
				if req.URL.Query().Get("runnerGroupId") != "1" {
					t.Error("SDK did not scope the runner group")
				}
				sets := []scaleset.RunnerScaleSet{}
				if remote != nil {
					sets = append(sets, *remote)
				}
				json.NewEncoder(w).Encode(map[string]any{"count": len(sets), "value": sets})
				return true
			}
			if req.URL.Path == path && req.Method == "POST" {
				var created scaleset.RunnerScaleSet
				if err := json.NewDecoder(req.Body).Decode(&created); err != nil {
					t.Errorf("SDK create JSON: %v", err)
				}
				planned, err := s.managedSet(ctx, 1)
				if err != nil || planned.phase != "create-intent" || created.Name != planned.name {
					t.Errorf("SDK mutation before persisted name: %+v %v", planned, err)
				}
				if created.Name == "slipway-linux" || len(created.Labels) != 1 || created.Labels[0].Name != "slipway-linux" || created.Labels[0].Type != "System" || !created.RunnerSetting.DisableUpdate {
					t.Errorf("native SDK changed requested routing: %+v", created)
				}
				created.ID = 101
				remote = &created
				json.NewEncoder(w).Encode(remote)
				return true
			}
			if req.URL.Path == path+"/101" && req.Method == "GET" {
				json.NewEncoder(w).Encode(remote)
				return true
			}
			return false
		}
	})
	r := newRegistry(s, d, "owner/repo1", func(githubapp.Installation, githubapp.Repository) (scaleSetAPI, error) { return f.client, nil })
	refreshRegistry(t, r)
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if f.count("POST", "/tenant/_apis/runtime/runnerscalesets") != 1 {
		t.Fatal("native SDK creation repeated")
	}
}

func TestRegistryRecoversWriteFailureAfterRemoteCreate(t *testing.T) {
	s, _ := openTestStore(t, 1)
	h := newRegistryHarness(t, s)
	api := h.api(1)
	api.configure(func() {
		api.createHook = func(*scaleset.RunnerScaleSet) {
			if _, err := s.db.Exec(`PRAGMA query_only=ON`); err != nil {
				t.Errorf("inject write failure: %v", err)
			}
		}
	})
	r := h.registry()
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(ctx); err == nil {
		t.Fatal("post-create journal failure hidden")
	}
	if own, err := s.managedSet(ctx, 1); err != nil || own.phase != "create-intent" {
		t.Fatalf("create intent lost: %+v %v", own, err)
	}
	if _, err := s.db.Exec(`PRAGMA query_only=OFF`); err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	api.configure(func() {
		if api.creates != 1 {
			t.Fatal("write failure repeated remote create")
		}
	})
}

func TestRegistryRevalidatesInstallationAndRenamedRepository(t *testing.T) {
	s, _ := openTestStore(t, 1)
	h := newRegistryHarness(t, s)
	r := h.registry()
	refreshRegistry(t, r)
	before, err := s.managedSet(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	h.discovery.configure(func() { h.discovery.repos[0].FullName = "owner/renamed" })
	refreshRegistry(t, r)
	after, err := s.managedSet(ctx, 1)
	if err != nil || after.name != before.name || after.remoteID != before.remoteID {
		t.Fatalf("rename replaced owned set: %+v %v", after, err)
	}
	h.mu.Lock()
	clientCount := h.clientCounts[1]
	h.mu.Unlock()
	if clientCount != 2 {
		t.Fatal("rename retained a client for the old repository URL")
	}
	h.discovery.configure(func() { h.discovery.installation.ID = 999 })
	if err := r.Refresh(ctx); err == nil {
		t.Fatal("switched App installation")
	}
	if err := r.Reconcile(ctx); !errors.Is(err, ErrDiscoveryStale) {
		t.Fatalf("foreign installation left admission open: %v", err)
	}
}

func TestSlowManagementDoesNotDelayRepositoryDiscovery(t *testing.T) {
	s, _ := openTestStore(t, 1)
	h := newRegistryHarness(t, s)
	r := h.registry()
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	api := h.api(1)
	api.configure(func() {
		api.listHook = func(ctx context.Context) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	managed := make(chan error, 1)
	go func() { managed <- r.Reconcile(callCtx) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("management did not start")
	}
	h.discovery.configure(func() {
		h.discovery.repos = append(h.discovery.repos, testRepository(2, "owner/new-grant"))
	})
	refreshed := make(chan error, 1)
	go func() { refreshed <- r.Refresh(ctx) }()
	select {
	case err := <-refreshed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("discovery waited behind management")
	}
	if repos, err := s.Repositories(ctx); err != nil || len(repos) != 2 {
		t.Fatalf("new grant hidden by slow management: %+v %v", repos, err)
	}
	cancel()
	select {
	case <-managed:
	case <-time.After(5 * time.Second):
		t.Fatal("management did not cancel")
	}
}

func TestRegistryRunCreatesAndStopsWithContext(t *testing.T) {
	s, _ := openTestStore(t, 1)
	h := newRegistryHarness(t, s)
	r := h.registry()
	created := make(chan struct{})
	api := h.api(1)
	api.configure(func() { api.createHook = func(*scaleset.RunnerScaleSet) { close(created) } })
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(runCtx, nil) }()
	select {
	case <-created:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not discover and manage immediately")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run leaked a loop after cancellation")
	}
}

// A restart preserves the lease and the session, and a journal whose discovery
// has never been stamped is fenced rather than trusted.
func TestRestartPreservesSessionAndFencesUnstampedDiscovery(t *testing.T) {
	s, dir := openTestStore(t, 1)
	key := prepare(t, s, 1)[0]
	offer(t, s, key, 1)
	lease := mustReserve(t, s)
	sessionID := uuid.NewString()
	if _, err := s.db.Exec(`INSERT INTO sessions(owner,repo_id,set_id,session_id,phase,created,updated) VALUES('prior-owner',?,?,?,'active',?,?)`, key.RepositoryID, key.ScaleSetID, sessionID, timestamp(), timestamp()); err != nil {
		t.Fatal(err)
	}
	// Clear discovery's stamp in this disposable fixture, leaving the lease and
	// session intact: that is the journal a restart mid-onboarding presents.
	if _, err := s.db.Exec(`DROP TABLE discovery_state`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(dir, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if current, err := restarted.Lease(ctx, lease.ID); err != nil || current != lease {
		t.Fatalf("restart changed a lease: %+v %v", current, err)
	}
	sessions, err := restarted.PendingSessions(ctx)
	if err != nil || len(sessions) != 1 || sessions[0].SessionID != sessionID || sessions[0].Owner != "prior-owner" {
		t.Fatalf("restart lost session: %+v %v", sessions, err)
	}
	if err := restarted.ReconcileInventory(ctx, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ReserveNext(ctx); !errors.Is(err, ErrDiscoveryStale) {
		t.Fatalf("restart trusted unstamped discovery: %v", err)
	}
}

// The eligibility gate the pilot depends on. Before it, Registry.Refresh and
// Store.Discover marked every non-archived, non-disabled repository eligible,
// so the first discovery cycle planned an slipway-linux scale set on every
// repository in the App installation -- public ones included, which ADR-0003
// forbids outright.
//
// Each case names what would happen without the gate, because a test that only
// asserts a refusal cannot tell a working bound from a predicate that never
// runs. The private-and-allowlisted row is the positive control.
func TestEligibilityRequiresPrivateAndAllowlistMembership(t *testing.T) {
	allowlist, err := parseRepositoryAllowlist([]string{"Owner/Listed", "owner/also-listed"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		repo githubapp.Repository
		want bool
	}{
		{"private and allowlisted", githubapp.Repository{ID: 1, FullName: "owner/listed", Private: true}, true},
		{"allowlist match is case-insensitive", githubapp.Repository{ID: 1, FullName: "Owner/Listed", Private: true}, true},
		{"public but allowlisted", githubapp.Repository{ID: 2, FullName: "owner/listed"}, false},
		{"private but unlisted", githubapp.Repository{ID: 3, FullName: "owner/unlisted", Private: true}, false},
		{"private field absent decodes as public", githubapp.Repository{ID: 4, FullName: "owner/listed"}, false},
		{"archived", githubapp.Repository{ID: 5, FullName: "owner/listed", Private: true, Archived: true}, false},
		{"disabled", githubapp.Repository{ID: 6, FullName: "owner/listed", Private: true, Disabled: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := eligibleForPool(tc.repo, allowlist); got != tc.want {
				t.Fatalf("eligibleForPool(%+v) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}

}

// The empty-allowlist mode: every private, active repository is served, and the
// refusals that are the security boundary are untouched by it. The
// private-and-unlisted row is the positive control -- it is refused with a
// pinned list above and admitted here, which is the whole behaviour change.
func TestEmptyAllowlistServesEveryPrivateActiveRepository(t *testing.T) {
	for _, allowlist := range []map[string]bool{nil, {}} {
		for _, tc := range []struct {
			name string
			repo githubapp.Repository
			want bool
		}{
			{"private and unlisted", githubapp.Repository{ID: 1, FullName: "owner/unlisted", Private: true}, true},
			{"public", githubapp.Repository{ID: 2, FullName: "owner/public"}, false},
			{"private field absent decodes as public", githubapp.Repository{ID: 3, FullName: "owner/absent"}, false},
			{"archived", githubapp.Repository{ID: 4, FullName: "owner/archive", Private: true, Archived: true}, false},
			{"disabled", githubapp.Repository{ID: 5, FullName: "owner/disabled", Private: true, Disabled: true}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if got := eligibleForPool(tc.repo, allowlist); got != tc.want {
					t.Fatalf("eligibleForPool(%+v) = %v, want %v", tc.repo, got, tc.want)
				}
			})
		}
	}
}

// The owner refusal is Refresh's, not eligibleForPool's, and an empty allowlist
// is exactly where that matters: nothing else names an owner any more. Refresh
// must refuse the whole discovery snapshot rather than mark a foreign
// repository eligible.
func TestEmptyAllowlistStillRefusesAForeignOwner(t *testing.T) {
	s, _ := openTestStore(t, 2)
	s.allowlist = map[string]bool{}
	h := newRegistryHarness(t, s)
	h.discovery.configure(func() {
		h.discovery.repos = append(h.discovery.repos, testRepository(2, "someone-else/private"))
	})
	if err := h.registry().Refresh(ctx); err == nil {
		t.Fatal("Refresh accepted a discovery snapshot carrying a foreign owner")
	}
	repos, err := s.Repositories(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, repo := range repos {
		if repo.Repository.FullName == "someone-else/private" && repo.Eligible {
			t.Fatal("a foreign owner was marked eligible under an empty allowlist")
		}
	}
}

// Onboarding is bounded by cycle, not by list. Creation is eager -- there is no
// webhook, and the broker cannot offer a job to a scale set that does not exist
// -- so the budget is what keeps an empty allowlist from creating a scale set
// for every private repository in one Refresh, and the cap is what keeps one
// controller's poll sweep finishable.
func TestReconcileSelectionBudgetsCreationAndCapsRepositories(t *testing.T) {
	repos := func(count int) []RepositoryState {
		var states []RepositoryState
		for n := 1; n <= count; n++ {
			states = append(states, RepositoryState{Repository: testRepository(int64(n), fmt.Sprintf("owner/repo%d", n)), Eligible: true})
		}
		return states
	}
	t.Run("new repositories are rate limited per cycle", func(t *testing.T) {
		selected, summary := selectForReconcile(repos(12), 5, 40)
		if len(selected) != 5 || summary.Creating != 5 || summary.CreateDeferred != 7 || summary.OverCap != 0 {
			t.Fatalf("selection: %d selected, %+v", len(selected), summary)
		}
	})
	t.Run("established repositories are never budgeted or capped", func(t *testing.T) {
		states := repos(6)
		for i := range states {
			states[i].Phase = "active"
			states[i].ScaleSetID = 100 + i
		}
		selected, summary := selectForReconcile(states, 5, 2)
		if len(selected) != 6 || summary != (ManagementCycle{}) {
			t.Fatalf("an established repository was held back: %d selected, %+v", len(selected), summary)
		}
	})
	t.Run("a mid-flight repository is visited even when the budget is spent", func(t *testing.T) {
		states := repos(8)
		states[7].Phase = "create-intent"
		selected, summary := selectForReconcile(states, 1, 40)
		if summary.Creating != 1 || summary.CreateDeferred != 6 || len(selected) != 2 {
			t.Fatalf("selection: %d selected, %+v", len(selected), summary)
		}
		if selected[len(selected)-1].Repository.ID != 8 {
			t.Fatalf("the mid-flight repository was not reconciled: %+v", selected)
		}
	})
	t.Run("the cap refuses new repositories above it", func(t *testing.T) {
		states := repos(10)
		for i := range states[:3] {
			states[i].Phase = "active"
		}
		selected, summary := selectForReconcile(states, 5, 4)
		if summary.Creating != 1 || summary.OverCap != 6 || len(selected) != 4 {
			t.Fatalf("selection: %d selected, %+v", len(selected), summary)
		}
	})
	t.Run("an ineligible repository is never selected", func(t *testing.T) {
		states := repos(3)
		states[1].Eligible = false
		selected, summary := selectForReconcile(states, 5, 40)
		if len(selected) != 2 || summary.Creating != 2 {
			t.Fatalf("an ineligible repository was selected: %d, %+v", len(selected), summary)
		}
	})
}

// The production budget itself, not a number the test supplies. Passing 5 by
// hand pins the ALGORITHM and leaves the constant free to drift to 70, which is
// the value this bound exists to prevent.
func TestCreateBudgetConstantBoundsOneCycle(t *testing.T) {
	var repos []RepositoryState
	for n := 1; n <= 12; n++ {
		repos = append(repos, RepositoryState{Repository: testRepository(int64(n), fmt.Sprintf("owner/repo%d", n)), Eligible: true})
	}
	selected, summary := selectForReconcile(repos, createBudget, defaultMaxRepositories)
	if createBudget != 5 || len(selected) != 5 || summary.Creating != 5 || summary.CreateDeferred != 7 {
		t.Fatalf("createBudget=%d admitted %d repositories: %+v", createBudget, len(selected), summary)
	}
}

// An archived repository must be refused where the Administration-write client
// is built, and that refusal reads the repository the JOURNAL reconstructs. The
// columns were not persisted, so Archived and Disabled arrived false for every
// repository and that arm of the choke point could never fire.
func TestDiscoveryPersistsArchivedAndDisabledForTheChokePoint(t *testing.T) {
	s, _ := openTestStore(t, 2)
	repos := []githubapp.Repository{
		testRepository(1, "owner/repo1"),
		{ID: 2, FullName: "owner/archive", Private: true, Archived: true},
		{ID: 3, FullName: "owner/repo3", Private: true, Disabled: true},
	}
	if err := s.Discover(ctx, repos); err != nil {
		t.Fatal(err)
	}
	states, err := s.Repositories(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]githubapp.Repository{}
	for _, state := range states {
		seen[state.Repository.ID] = state.Repository
	}
	if !seen[2].Archived {
		t.Fatal("an archived repository was reconstructed as active; the scale-set client cannot refuse it")
	}
	if !seen[3].Disabled {
		t.Fatal("a disabled repository was reconstructed as active; the scale-set client cannot refuse it")
	}
	if seen[1].Archived || seen[1].Disabled {
		t.Fatalf("an active repository was reconstructed as archived or disabled: %+v", seen[1])
	}
	// The choke point's own refusal of these two flags is pinned next door, in
	// githubapp.TestScaleSetClientRefusesPublicArchivedOrDisabled. What was
	// missing is the input it reads, which is what this test covers.
}

// Eligibility loss marks intent and reports; it issues no DELETE.
//
// The controller deliberately cannot delete a remote scale set: that call is
// irreversible, the evidence for it is one discovery snapshot in which absence
// and a refused read look identical, and performing it for a repository that
// just went public means wielding Administration:write on a public repository.
// So the leftover resource is REPORTED and an operator decides. The assertions
// below are the durable state and the count; the `deletes` spy is a tripwire
// that fires if a delete path is ever reintroduced.
func TestEligibilityLossMarksIntentAndReportsOrphanedWithoutDeleting(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	h.discovery.configure(func() {
		h.discovery.repos = append(h.discovery.repos, testRepository(2, "owner/private"))
	})
	r := h.registry()
	refreshRegistry(t, r)
	if set, err := s.managedSet(ctx, 2); err != nil || set.phase != "active" {
		t.Fatalf("fixture did not onboard the repository: %+v %v", set, err)
	}

	// It goes archived: still discovered, no longer eligible.
	h.discovery.configure(func() {
		h.discovery.repos[1] = githubapp.Repository{ID: 2, FullName: "owner/private", Private: true, Archived: true}
	})
	refreshRegistry(t, r)

	set, err := s.managedSet(ctx, 2)
	if err != nil || set.phase != "remove-intent" {
		t.Fatalf("eligibility loss did not record removal intent: %+v %v", set, err)
	}
	cycle := s.ManagementCycle()
	if cycle.Orphaned != 1 || cycle.OrphanedFirstName != "owner/private" {
		t.Fatalf("the orphan was not reported by name: %+v", cycle)
	}
	api := h.api(2)
	api.configure(func() {
		if api.deletes != 0 {
			t.Errorf("a DELETE was issued for an ineligible repository: %d call(s)", api.deletes)
		}
		if len(api.sets) != 1 {
			t.Errorf("the remote scale set was modified: %+v", api.sets)
		}
	})

	// It persists: the next cycle reports the same orphan, because nothing in
	// this controller can clear it.
	refreshRegistry(t, r)
	if cycle := s.ManagementCycle(); cycle.Orphaned != 1 {
		t.Fatalf("the orphan stopped being reported without anyone clearing it: %+v", cycle)
	}
}

// The cap is a startup gate, not a clamp: a value one cycle cannot poll is
// refused rather than quietly reduced.
func TestMaxRepositoriesIsBoundedAtOpen(t *testing.T) {
	dir := t.TempDir()
	base := Config{Platform: "linux", InstallationID: 42, Capacity: 2, RepositoryAllowlist: testAllowlist}
	for _, value := range []int{-1, maxRepositoriesCeiling + 1} {
		config := base
		config.MaxRepositories = value
		if s, err := Open(filepath.Join(dir, fmt.Sprintf("state%d", value)), config); err == nil {
			s.Close()
			t.Fatalf("Open accepted max-repositories %d", value)
		}
	}
	config := base
	s, err := Open(filepath.Join(dir, "default"), config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.config.MaxRepositories != defaultMaxRepositories {
		t.Fatalf("an unset cap did not adopt the default: %d", s.config.MaxRepositories)
	}
}

func TestRepositoryAllowlistParsingFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []string
	}{
		{"bare name", []string{"workout-tracker"}},
		{"missing owner", []string{"/workout-tracker"}},
		{"missing name", []string{"cameronsjo/"}},
		{"three segments", []string{"cameronsjo/workout-tracker/extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseRepositoryAllowlist(tc.entries); err == nil {
				t.Fatalf("parseRepositoryAllowlist(%q) accepted an unusable list", tc.entries)
			}
		})
	}
	parsed, err := parseRepositoryAllowlist([]string{" cameronsjo/Workout-Tracker ", ""})
	if err != nil || !parsed["cameronsjo/workout-tracker"] || len(parsed) != 1 {
		t.Fatalf("parseRepositoryAllowlist did not normalize a valid entry: %v %v", parsed, err)
	}
}

// Store.Discover writes the repositories.eligible column every other query
// joins on, so the gate has to hold there as well as in Registry.Refresh.
func TestDiscoverPersistsEligibilityForAllowlistedPrivateRepositoriesOnly(t *testing.T) {
	s, _ := openTestStore(t, 2)
	repos := []githubapp.Repository{
		testRepository(1, "owner/repo1"),
		{ID: 2, FullName: "owner/repo2"},
		{ID: 3, FullName: "owner/unlisted-private", Private: true},
	}
	if err := s.Discover(ctx, repos); err != nil {
		t.Fatal(err)
	}
	states, err := s.Repositories(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eligible := map[string]bool{}
	for _, state := range states {
		eligible[state.Repository.FullName] = state.Eligible
	}
	if !eligible["owner/repo1"] {
		t.Fatal("an allowlisted private repository was not planned")
	}
	if eligible["owner/repo2"] {
		t.Fatal("a public repository was planned; ADR-0003 forbids a self-hosted runner there")
	}
	if eligible["owner/unlisted-private"] {
		t.Fatal("an unlisted private repository was planned")
	}
}

// The operator keeps the App on "All repositories". Refresh must accept that
// and "selected", and refuse anything else or a lost Administration grant.
func TestRefreshAcceptsSelectedAndAllInstallations(t *testing.T) {
	for _, tc := range []struct {
		selection, administration string
		refused                   bool
	}{
		{"selected", "write", false},
		{"all", "write", false},
		{"", "write", true},
		{"none", "write", true},
		{"all", "read", true},
	} {
		t.Run(tc.selection+"/"+tc.administration, func(t *testing.T) {
			s, _ := openTestStore(t, 2)
			h := newRegistryHarness(t, s)
			h.discovery.configure(func() {
				h.discovery.installation.Selection = tc.selection
				h.discovery.installation.Permissions = map[string]string{"administration": tc.administration}
			})
			err := h.registry().Refresh(ctx)
			if (err != nil) != tc.refused {
				t.Fatalf("selection=%q administration=%q err=%v", tc.selection, tc.administration, err)
			}
			status, statusErr := s.DiscoveryStatus(ctx)
			if statusErr != nil {
				t.Fatal(statusErr)
			}
			repos, reposErr := s.Repositories(ctx)
			if reposErr != nil {
				t.Fatal(reposErr)
			}
			if tc.refused {
				if !strings.Contains(status.Diagnostic, "App installation could not be verified") {
					t.Fatalf("refusal not recorded in discovery state: %+v", status)
				}
				if len(repos) != 0 {
					t.Fatalf("refused installation committed a snapshot: %+v", repos)
				}
				return
			}
			if status.Diagnostic != "" || status.LastSuccess == "" {
				t.Fatalf("accepted installation left a failed discovery state: %+v", status)
			}
			if len(repos) != 1 || repos[0].Repository.FullName != "owner/repo1" {
				t.Fatalf("accepted installation did not commit its snapshot: %+v", repos)
			}
		})
	}
}

// An all-repositories installation discovers every repository in the account,
// public ones included. GitHub's selection is not the pool's bound: the
// private-only rule decides eligibility, and no scale-set client — which would
// carry Administration write — is ever requested for a public repository.
func TestRefreshRefusesPublicRepositoryUnderAllInstallation(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	public := githubapp.Repository{ID: 5, FullName: "owner/public"}
	h.discovery.configure(func() {
		h.discovery.installation.Selection = "all"
		h.discovery.repos = append(h.discovery.repos, public)
	})
	refreshRegistry(t, h.registry())
	repos, err := s.Repositories(ctx)
	if err != nil || len(repos) != 2 {
		t.Fatalf("discovery snapshot: %+v %v", repos, err)
	}
	for _, repo := range repos {
		if repo.Repository.ID != public.ID {
			continue
		}
		if repo.Eligible || repo.ScaleSetID != 0 {
			t.Fatalf("public repository activated: %+v", repo)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clientCounts[public.ID] != 0 {
		t.Fatalf("admin client requested for a public repository: %d", h.clientCounts[public.ID])
	}
	if h.clientCounts[1] != 1 {
		t.Fatalf("private repository lost its client: %d", h.clientCounts[1])
	}
}
