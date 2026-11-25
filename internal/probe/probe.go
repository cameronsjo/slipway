// Package probe exercises the repository API foundation with one durable
// reservation. It never acquires jobs or starts a worker.
package probe

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/actions/scaleset"
)

type API interface {
	GetRunnerScaleSet(context.Context, int, string) (*scaleset.RunnerScaleSet, error)
	CreateRunnerScaleSet(context.Context, *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
	DeleteRunnerScaleSet(context.Context, int) error
	GenerateJitRunnerConfig(context.Context, *scaleset.RunnerScaleSetJitRunnerSetting, int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(context.Context, string) (*scaleset.RunnerReference, error)
	RemoveRunner(context.Context, int64) error
}

// Run persists create/JIT intent before each mutation. Returning an error leaves
// the slot reserved; Cleanup is the only release path. JIT bytes are checked for
// presence then discarded. API minting is not proof of single-use consumption.
func Run(ctx context.Context, s *Store, a *Attempt, api API) error {
	existing, err := api.GetRunnerScaleSet(ctx, a.GroupID, a.Name)
	if err != nil {
		return fmt.Errorf("lookup set: %w", err)
	}
	if existing != nil {
		return errors.New("unexpected existing scale set; refuse to adopt")
	}
	if err := s.Save(a, "create-intent", false); err != nil {
		return err
	}
	set, err := api.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{Name: a.Name, RunnerGroupID: a.GroupID, Labels: []scaleset.Label{{Name: a.Name}}, RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true}})
	if err != nil {
		return fmt.Errorf("create set: %w", err)
	}
	if set == nil || set.ID <= 0 || set.Name != a.Name || set.RunnerGroupID != a.GroupID {
		return errors.New("invalid created set identity")
	}
	a.SetID = set.ID
	if err := s.Save(a, "set-created", false); err != nil {
		return err
	}
	confirmed, err := api.GetRunnerScaleSet(ctx, a.GroupID, a.Name)
	if err != nil {
		return err
	}
	if confirmed == nil || confirmed.ID != a.SetID || confirmed.Name != a.Name || confirmed.RunnerGroupID != a.GroupID {
		return errors.New("created set readback disagrees")
	}
	if err := s.Save(a, "jit-intent", false); err != nil {
		return err
	}
	jit, err := api.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: a.RunnerName, WorkFolder: "_work"}, a.SetID)
	if err != nil {
		return fmt.Errorf("generate JIT: %w", err)
	}
	if jit == nil || jit.EncodedJITConfig == "" || jit.Runner == nil || jit.Runner.ID <= 0 || jit.Runner.Name != a.RunnerName || jit.Runner.RunnerScaleSetID != a.SetID {
		return errors.New("invalid JIT runner identity")
	}
	a.RunnerID = jit.Runner.ID
	jit.EncodedJITConfig = ""
	return s.Save(a, "jit-minted", false)
}

// Cleanup uses only the journal's random, exact identities, never labels/globs.
// It reconciles missing mutation responses and verifies absence after deletion.
// A fresh independent deadline permits cleanup after cancellation of Run.
func Cleanup(s *Store, a *Attempt, api API) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if a.Stage == "reserved" {
		// Run has not persisted create intent and therefore cannot have created
		// anything. In particular, do not delete an unrelated name collision.
		return s.Save(a, "no-mutations", true)
	}
	if err := s.Save(a, "cleanup-pending", false); err != nil {
		return err
	}
	set, err := api.GetRunnerScaleSet(ctx, a.GroupID, a.Name)
	if err != nil {
		return err
	}
	if set != nil {
		if set.ID <= 0 || set.Name != a.Name || set.RunnerGroupID != a.GroupID || (a.SetID != 0 && set.ID != a.SetID) {
			return errors.New("scale set identity changed; quarantine")
		}
		a.SetID = set.ID
		if err := s.Save(a, "cleanup-pending", false); err != nil {
			return err
		}
	}
	runner, err := api.GetRunnerByName(ctx, a.RunnerName)
	if err != nil {
		return err
	}
	if runner != nil {
		if runner.ID <= 0 || runner.Name != a.RunnerName || a.SetID <= 0 || runner.RunnerScaleSetID != a.SetID || (a.RunnerID != 0 && runner.ID != a.RunnerID) {
			return errors.New("runner identity changed; quarantine")
		}
		a.RunnerID = runner.ID
		if err := s.Save(a, "remove-runner-intent", false); err != nil {
			return err
		}
		deleteErr := api.RemoveRunner(ctx, int64(runner.ID))
		remaining, err := api.GetRunnerByName(ctx, a.RunnerName)
		if err != nil {
			return err
		}
		if remaining != nil {
			return errors.Join(errors.New("runner still present; reservation retained"), deleteErr)
		}
	}
	if set != nil {
		if err := s.Save(a, "remove-set-intent", false); err != nil {
			return err
		}
		deleteErr := api.DeleteRunnerScaleSet(ctx, set.ID)
		remaining, err := api.GetRunnerScaleSet(ctx, a.GroupID, a.Name)
		if err != nil {
			return err
		}
		if remaining != nil {
			return errors.Join(errors.New("scale set still present; reservation retained"), deleteErr)
		}
	}
	return s.Save(a, "absent", true)
}
