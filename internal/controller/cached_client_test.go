package controller

import (
	"context"
	"testing"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

// homelab#921: Refresh evicts a cached Administration-write client only after
// the journal commit succeeds, and the App records its discovery BEFORE that
// commit. A public flip followed by a failed commit therefore left the cached
// client usable. clientFor must re-ask the App's discovery on every cache hit,
// refuse, and evict -- without rebuilding and without touching the API.
func TestCachedClientIsRefusedAfterAPublicFlipTheJournalNeverCommitted(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	r := h.registry()
	refreshRegistry(t, r)
	if h.clientCounts[1] != 1 {
		t.Fatalf("fixture did not build the client: %d", h.clientCounts[1])
	}

	// The journal commit fails from here on; the App's discovery still lands.
	if _, err := s.db.Exec(`CREATE TRIGGER reject_discovery BEFORE UPDATE ON discovery_state BEGIN SELECT RAISE(FAIL,'fixture refusal'); END`); err != nil {
		t.Fatal(err)
	}
	h.discovery.configure(func() { h.discovery.repos[0] = githubapp.Repository{ID: 1, FullName: "owner/repo1"} })
	if err := r.Refresh(ctx); err == nil {
		t.Fatal("refresh reported success; the journal commit was meant to fail")
	}

	// The window is real: the journal still says private and eligible, and
	// Refresh's own eviction never ran.
	states, err := s.Repositories(ctx)
	if err != nil || len(states) != 1 || !states[0].Eligible || !states[0].Repository.Private {
		t.Fatalf("the journal moved; this test no longer reaches the #921 window: %+v %v", states, err)
	}
	r.mu.Lock()
	_, cached := r.clients[1]
	r.mu.Unlock()
	if !cached {
		t.Fatal("Refresh evicted the client; this test no longer reaches the #921 window")
	}

	api := h.api(1)
	var createsBefore int
	api.configure(func() {
		createsBefore = api.creates
		api.listHook = func(context.Context) { t.Error("the refused client reached the scale-set API") }
	})
	client, err := r.clientFor(states[0].Repository)
	if err == nil || client != nil {
		t.Fatal("a cached Administration-write client was returned for a repository discovery reports public")
	}
	r.mu.Lock()
	_, cached = r.clients[1]
	r.mu.Unlock()
	if cached {
		t.Fatal("the refused client was not evicted")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clientCounts[1] != 1 {
		t.Fatalf("the refusal rebuilt a client: %d builds", h.clientCounts[1])
	}
	api.configure(func() {
		if api.creates != createsBefore || api.deletes != 0 {
			t.Errorf("the refusal spent an API call: creates=%d (was %d) deletes=%d", api.creates, createsBefore, api.deletes)
		}
	})
}

// homelab#924: clientFor asks the App's discovery, but the create gate asked
// only the journal. A public flip the App records after clientFor passed, and
// before the journal commits, left the journal saying eligible=1, so the POST
// went out. The create gate must ask the App's discovery too, and refuse while
// the managed row is still 'planned' -- not after 'create-intent', which would
// strand it as an uncertain creation.
func TestCreationIsRefusedWhenTheAppSeesAPublicFlipAfterClientFor(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	r := h.registry()
	if err := r.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	api := h.api(1)
	api.configure(func() {
		// ListRunnerScaleSets runs after clientFor and before the create gate:
		// the exact window. Only the App's snapshot moves; the journal does not.
		api.listHook = func(context.Context) {
			h.discovery.configure(func() {
				h.discovery.recorded[1] = githubapp.Repository{ID: 1, FullName: "owner/repo1"}
			})
		}
	})
	if err := r.Reconcile(ctx); err == nil {
		t.Fatal("reconcile reported success for a repository the App now reports public")
	}
	states, err := s.Repositories(ctx)
	if err != nil || len(states) != 1 || !states[0].Eligible {
		t.Fatalf("the journal moved; this test no longer reaches the #924 window: %+v %v", states, err)
	}
	api.configure(func() {
		if api.creates != 0 {
			t.Errorf("a scale set was created on a repository the App reports public: creates=%d", api.creates)
		}
	})
	own, err := s.managedSet(ctx, 1)
	if err != nil || own.phase != "planned" {
		t.Fatalf("the refusal left the managed row past 'planned': %+v %v", own, err)
	}
}

// The re-check costs the happy path nothing: an eligible repository keeps the
// one client it was given, across calls, without a rebuild.
func TestCachedClientIsReusedWhileDiscoveryStillAdmitsIt(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	r := h.registry()
	refreshRegistry(t, r)
	repo := testRepository(1, "owner/repo1")
	first, err := r.clientFor(repo)
	if err != nil || first == nil {
		t.Fatalf("eligible repository refused: %v", err)
	}
	second, err := r.clientFor(repo)
	if err != nil || second != first {
		t.Fatalf("the cached client was not reused: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clientCounts[1] != 1 {
		t.Fatalf("the cached client was rebuilt: %d builds", h.clientCounts[1])
	}
}

// With no complete discovery recorded by the App, even a cached client is
// refused and dropped: absence of evidence is not a grant.
func TestCachedClientIsRefusedWithoutAnyDiscovery(t *testing.T) {
	s, _ := openTestStore(t, 2)
	h := newRegistryHarness(t, s)
	r := h.registry()
	repo := testRepository(1, "owner/repo1")
	r.mu.Lock()
	r.installation = h.discovery.installation
	r.clients[repo.ID] = cachedSetClient{fullName: repo.FullName, api: h.api(repo.ID)}
	r.mu.Unlock()

	if client, err := r.clientFor(repo); err == nil || client != nil {
		t.Fatal("a cached client was returned with no discovery to authorize it")
	}
	r.mu.Lock()
	_, cached := r.clients[repo.ID]
	r.mu.Unlock()
	if cached {
		t.Fatal("the refused client was not evicted")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clientCounts[repo.ID] != 0 {
		t.Fatalf("a client was built with no discovery: %d", h.clientCounts[repo.ID])
	}
}
