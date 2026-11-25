package probe

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/actions/scaleset"
	"github.com/cameronsjo/slipway/internal/githubapp"
)

type fakeAPI struct {
	t          *testing.T
	store      *Store
	set        *scaleset.RunnerScaleSet
	runner     *scaleset.RunnerReference
	lostCreate bool
	lostJIT    bool
	lostDelete bool
	failDelete bool
	creates    int
}

func (f *fakeAPI) intent(stage string) {
	f.t.Helper()
	pending, err := f.store.Pending()
	if err != nil || pending == nil || pending.Stage != stage {
		f.t.Fatalf("API mutation without durable %s intent: %+v %v", stage, pending, err)
	}
}
func (f *fakeAPI) GetRunnerScaleSet(ctx context.Context, group int, name string) (*scaleset.RunnerScaleSet, error) {
	if group != 1 {
		f.t.Fatalf("lookup did not select default runner group: %d", group)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.set, nil
}
func (f *fakeAPI) CreateRunnerScaleSet(_ context.Context, set *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error) {
	f.intent("create-intent")
	if set.RunnerGroupID != 1 {
		f.t.Fatalf("create did not select default runner group: %d", set.RunnerGroupID)
	}
	f.creates++
	f.set = &scaleset.RunnerScaleSet{ID: 17, Name: set.Name, RunnerGroupID: set.RunnerGroupID}
	if f.lostCreate {
		return nil, errors.New("response lost after creation")
	}
	return f.set, nil
}
func (f *fakeAPI) GenerateJitRunnerConfig(_ context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, setID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	f.intent("jit-intent")
	f.runner = &scaleset.RunnerReference{ID: 23, Name: setting.Name, RunnerScaleSetID: setID}
	if f.lostJIT {
		return nil, errors.New("response lost after JIT creation")
	}
	return &scaleset.RunnerScaleSetJitRunnerConfig{Runner: f.runner, EncodedJITConfig: "secret-must-not-reach-journal"}, nil
}
func (f *fakeAPI) GetRunnerByName(ctx context.Context, _ string) (*scaleset.RunnerReference, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.runner, nil
}
func (f *fakeAPI) RemoveRunner(_ context.Context, _ int64) error {
	f.intent("remove-runner-intent")
	if f.failDelete {
		return errors.New("delete refused")
	}
	f.runner = nil
	if f.lostDelete {
		return errors.New("delete response lost")
	}
	return nil
}
func (f *fakeAPI) DeleteRunnerScaleSet(_ context.Context, _ int) error {
	f.intent("remove-set-intent")
	if f.failDelete {
		return errors.New("delete refused")
	}
	f.set = nil
	if f.lostDelete {
		return errors.New("delete response lost")
	}
	return nil
}

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "journal")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}
func begin(t *testing.T, s *Store) *Attempt {
	t.Helper()
	a, err := s.Begin(42, githubapp.Repository{ID: 1, FullName: "owner/repo"})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAPILifecycleAndCredentialOmission(t *testing.T) {
	s, dir := newStore(t)
	a := begin(t, s)
	f := &fakeAPI{t: t, store: s}
	if err := Run(context.Background(), s, a, f); err != nil {
		t.Fatal(err)
	}
	if err := Cleanup(s, a, f); err != nil {
		t.Fatal(err)
	}
	if !a.Done || f.creates != 1 || f.set != nil || f.runner != nil {
		t.Fatalf("incomplete lifecycle: %+v", a)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("secret-must-not-reach-journal")) {
			t.Fatal("JIT secret persisted")
		}
	}
}

func TestLostResponsesReconcileAfterRestart(t *testing.T) {
	for _, stage := range []string{"create", "jit", "delete"} {
		t.Run(stage, func(t *testing.T) {
			s, dir := newStore(t)
			a := begin(t, s)
			f := &fakeAPI{t: t, store: s, lostCreate: stage == "create", lostJIT: stage == "jit", lostDelete: stage == "delete"}
			err := Run(context.Background(), s, a, f)
			if stage != "delete" && err == nil {
				t.Fatal("lost response was accepted")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			f.store = reopened
			pending, err := reopened.Pending()
			if err != nil || pending == nil {
				t.Fatalf("lost reservation after restart: %v", err)
			}
			if _, err := reopened.Begin(42, a.Repo); err == nil {
				t.Fatal("allocated replacement before reconciliation")
			}
			if err := Cleanup(reopened, pending, f); err != nil {
				t.Fatal(err)
			}
			if !pending.Done || f.creates != 1 {
				t.Fatalf("duplicate or unreclaimed attempt: %+v", pending)
			}
		})
	}
}

func TestCleanupFailureRetainsCapacity(t *testing.T) {
	s, _ := newStore(t)
	a := begin(t, s)
	f := &fakeAPI{t: t, store: s, failDelete: true}
	if err := Run(context.Background(), s, a, f); err != nil {
		t.Fatal(err)
	}
	if err := Cleanup(s, a, f); err == nil {
		t.Fatal("failed cleanup passed")
	}
	if a.Done {
		t.Fatal("failed cleanup released capacity")
	}
	if _, err := s.Begin(42, a.Repo); err == nil {
		t.Fatal("cleanup-pending reservation overcommitted")
	}
	f.failDelete = false
	if err := Cleanup(s, a, f); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(42, a.Repo); err != nil {
		t.Fatalf("verified absence did not release capacity: %v", err)
	}
}

func TestIdentityMismatchQuarantines(t *testing.T) {
	s, _ := newStore(t)
	a := begin(t, s)
	f := &fakeAPI{t: t, store: s}
	if err := Run(context.Background(), s, a, f); err != nil {
		t.Fatal(err)
	}
	f.runner.RunnerScaleSetID = 999
	if err := Cleanup(s, a, f); err == nil || f.runner == nil || a.Done {
		t.Fatal("removed a foreign runner or released capacity")
	}
}

func TestNoMutationOnCancelledStartOrCollision(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "collision"}[collision], func(t *testing.T) {
			s, _ := newStore(t)
			a := begin(t, s)
			f := &fakeAPI{t: t, store: s}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if collision {
				f.set = &scaleset.RunnerScaleSet{ID: 99, Name: a.Name}
			} else {
				cancel()
			}
			if err := Run(ctx, s, a, f); err == nil {
				t.Fatal("expected refusal")
			}
			if err := Cleanup(s, a, f); err != nil {
				t.Fatal(err)
			}
			if f.creates != 0 || !a.Done || (collision && f.set == nil) {
				t.Fatal("mutated a collision or retained unallocated work")
			}
		})
	}
}

func TestSingletonAndPrivateState(t *testing.T) {
	s, dir := newStore(t)
	if other, err := Open(dir); err == nil {
		other.Close()
		t.Fatal("second writer acquired allocation lock")
	}
	a := begin(t, s)
	if _, err := s.Begin(42, a.Repo); err == nil {
		t.Fatal("second active probe allocated")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir); err == nil {
		other.Close()
		t.Fatal("insecure state directory accepted")
	}
}

func TestFailedCommitCannotClaimRelease(t *testing.T) {
	s, _ := newStore(t)
	a := begin(t, s)
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(a, "absent", true); err == nil {
		t.Fatal("closed database accepted release")
	}
	if a.Done || a.Stage != "reserved" {
		t.Fatalf("uncommitted state escaped: %+v", a)
	}
}
