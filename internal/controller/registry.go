package controller

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/actions/scaleset"
	"github.com/google/uuid"
)

const personalRunnerGroup = 1 // Default repository group, proved by the API canary.

type managedSet struct {
	repoID   int64
	name     string
	remoteID int
	phase    string
}

func (s *Store) managedSet(ctx context.Context, repoID int64) (managedSet, error) {
	var set managedSet
	err := s.db.QueryRowContext(ctx, `SELECT repo_id,name,remote_id,phase FROM managed_sets WHERE repo_id=?`, repoID).
		Scan(&set.repoID, &set.name, &set.remoteID, &set.phase)
	return set, err
}

func (s *Store) planSet(ctx context.Context, repo RepositoryState) (managedSet, error) {
	if err := s.discoveryCurrent(ctx); err != nil {
		return managedSet{}, err
	}
	// A random persisted resource name proves ownership after an uncertain POST.
	// The explicit workflow label remains the stable platform interface.
	_, err := s.db.ExecContext(ctx, `INSERT INTO managed_sets(repo_id,name,phase,updated)
SELECT id,?,'planned',? FROM repositories WHERE id=? AND full_name=? AND eligible=1
ON CONFLICT(repo_id) DO NOTHING`, "slipway-"+s.config.Platform+"-"+uuid.NewString(), timestamp(), repo.Repository.ID, repo.Repository.FullName)
	if err != nil {
		return managedSet{}, err
	}
	return s.managedSet(ctx, repo.Repository.ID)
}

func (s *Store) setFailed(ctx context.Context, repoID int64, diagnostic string) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	// Stop new acquisition on loss of ownership verification. Preserve every
	// lease, session and remote ID so recovery can resume from evidence.
	err := changedOne(s.db.ExecContext(saveCtx, `UPDATE managed_sets SET diagnostic=?,updated=?,
phase=CASE WHEN phase='active' THEN 'unverified' ELSE phase END WHERE repo_id=?`, diagnostic, timestamp(), repoID))
	return errors.Join(errors.New(diagnostic), err)
}

// markSetRemoval records that a repository lost eligibility while it still owns
// a scale set. It is a durable note and nothing else: no API call follows it.
//
// THE CONTROLLER DOES NOT DELETE THE REMOTE SCALE SET, DELIBERATELY. It would
// have to spend an irreversible Administration-write DELETE on the strength of
// one discovery snapshot, in which "this repository is gone" and "this read was
// refused" arrive as the same absence; and deleting a scale set for a
// repository that just went public means holding an Administration:write
// credential for a public repository, which is the exact thing ADR-0003
// refuses. A leftover scale set on a repository nobody serves is cosmetic. The
// trade is not worth an irreversible call made on ambiguous evidence, so the
// controller reports and the operator decides -- see
// hosts/ms-a2/github-runner-pool/README.md for the manual cleanup.
func (s *Store) markSetRemoval(ctx context.Context, repoID int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE managed_sets SET phase='remove-intent',diagnostic='',updated=?
WHERE repo_id=? AND phase!='remove-intent' AND NOT EXISTS(SELECT 1 FROM repositories r WHERE r.id=managed_sets.repo_id AND r.eligible=1)`,
		timestamp(), repoID)
	return err
}

// reportOrphans marks and counts every repository that lost eligibility while
// still owning a scale set, and names the first. That count is the operator's
// only signal that a remote resource outlived its grant: nothing else anywhere
// mentions it, and it persists across cycles and restarts until the operator
// clears it by hand.
func (r *Registry) reportOrphans(ctx context.Context, repos []RepositoryState) (int, string) {
	orphaned, first := 0, ""
	for _, repo := range repos {
		if repo.Eligible || (repo.Phase == "" && repo.ScaleSetID == 0) {
			continue
		}
		// A failed mark still counts: the repository is orphaned either way, and
		// an unwritable note must not read as nothing to report.
		_ = r.store.markSetRemoval(ctx, repo.Repository.ID)
		orphaned++
		if first == "" {
			first = repo.Repository.FullName
		}
	}
	return orphaned, first
}

func (r *Registry) reconcileRepository(ctx context.Context, repo RepositoryState) error {
	own, err := r.store.planSet(ctx, repo)
	if err != nil {
		return err
	}
	fail := func(diagnostic string) error { return r.store.setFailed(ctx, repo.Repository.ID, diagnostic) }
	if repo.ScaleSetID > 0 && own.remoteID == 0 && own.phase == "planned" {
		return fail("existing scale-set binding lacks a managed ownership record")
	}
	api, err := r.clientFor(repo.Repository)
	if err != nil {
		return fail("repository scale-set client is unavailable")
	}
	sets, err := api.ListRunnerScaleSets(ctx, personalRunnerGroup)
	if err != nil {
		return fail("complete scale-set ownership listing failed")
	}
	label := "slipway-" + r.store.config.Platform
	var found *scaleset.RunnerScaleSet
	ids, names := map[int]bool{}, map[string]bool{}
	for _, set := range sets {
		name := strings.ToLower(set.Name)
		if set.ID <= 0 || set.Name == "" || set.RunnerGroupID != personalRunnerGroup || ids[set.ID] || names[name] {
			return fail("scale-set ownership listing has invalid or duplicate identities")
		}
		ids[set.ID], names[name] = true, true
		if set.Name == own.name {
			found = &set
			continue
		}
		if strings.EqualFold(set.Name, label) {
			return fail("platform workflow label belongs to an unmanaged scale set")
		}
		for _, candidate := range set.Labels {
			if strings.EqualFold(candidate.Name, label) {
				return fail("platform workflow label belongs to an unmanaged scale set")
			}
		}
	}
	if found != nil {
		if own.phase == "planned" {
			return fail("owned resource name existed before creation intent; refuse adoption")
		}
		if own.remoteID != 0 && found.ID != own.remoteID {
			return fail("managed scale-set incarnation changed; retain original identity")
		}
		return r.confirmSet(ctx, own, found.ID, api)
	}
	if own.phase != "planned" {
		return fail("managed scale set absent or creation outcome unknown; do not repeat POST")
	}
	if err := r.store.discoveryCurrent(ctx); err != nil {
		return err
	}
	// The journal below lags the App: the App records a public flip before the
	// journal commits it, so eligible=1 alone can admit a POST on a repository
	// that went public after clientFor passed (homelab#924). Ask the App too,
	// while the row is still 'planned', so a refusal strands nothing.
	if !r.app.RepositoryEligible(repo.Repository) {
		return fail("scale-set creation fenced: repository not private and active in the App's last complete discovery")
	}
	// The exact eligibility/name check closes a discovery revocation or rename
	// race between the earlier list and this durable create intent.
	if err := changedOne(r.store.db.ExecContext(ctx, `UPDATE managed_sets SET phase='create-intent',diagnostic='',updated=?
WHERE repo_id=? AND phase='planned' AND EXISTS(SELECT 1 FROM repositories r WHERE r.id=managed_sets.repo_id AND r.full_name=? AND r.eligible=1)
AND NOT EXISTS(SELECT 1 FROM scale_sets ss WHERE ss.repo_id=managed_sets.repo_id)`, timestamp(), own.repoID, repo.Repository.FullName)); err != nil {
		return fail("scale-set creation fenced by changed discovery or ownership")
	}
	created, apiErr := api.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{Name: own.name, RunnerGroupID: personalRunnerGroup,
		Labels: []scaleset.Label{{Name: label}}, RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true}})
	if apiErr != nil || created == nil || created.ID <= 0 || created.Name != own.name || created.RunnerGroupID != personalRunnerGroup {
		return fail("scale-set creation outcome uncertain; reconcile persisted name")
	}
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	defer cancel()
	if err := changedOne(r.store.db.ExecContext(saveCtx, `UPDATE managed_sets SET remote_id=?,phase='created',updated=? WHERE repo_id=? AND phase='create-intent'`,
		created.ID, timestamp(), own.repoID)); err != nil {
		return err
	}
	own.remoteID, own.phase = created.ID, "created"
	return r.confirmSet(ctx, own, created.ID, api)
}

func validManagedSet(set *scaleset.RunnerScaleSet, own managedSet, id int, label string) bool {
	if set == nil || set.ID != id || set.ID <= 0 || set.Name != own.name || set.RunnerGroupID != personalRunnerGroup || !set.RunnerSetting.DisableUpdate {
		return false
	}
	// GitHub may include its resource name as an additional label. Accept that
	// known system label, but no unrelated workflow selectors or duplicates.
	seen := map[string]bool{}
	for _, candidate := range set.Labels {
		name := strings.ToLower(candidate.Name)
		if seen[name] || (name != label && name != own.name) {
			return false
		}
		seen[name] = true
	}
	return seen[label]
}

func (r *Registry) confirmSet(ctx context.Context, own managedSet, id int, api scaleSetAPI) error {
	confirmed, err := api.GetRunnerScaleSetByID(ctx, id)
	if err != nil {
		return r.store.setFailed(ctx, own.repoID, "managed scale-set ID readback failed")
	}
	if !validManagedSet(confirmed, own, id, "slipway-"+r.store.config.Platform) {
		return r.store.setFailed(ctx, own.repoID, "scale-set readback violates identity, label, or update policy")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO scale_sets(repo_id,set_id) VALUES(?,?) ON CONFLICT(repo_id) DO NOTHING`, own.repoID, id); err != nil {
		return err
	}
	var bound int
	if err := tx.QueryRowContext(ctx, `SELECT set_id FROM scale_sets WHERE repo_id=?`, own.repoID).Scan(&bound); err != nil {
		return err
	}
	if bound != id {
		return errors.New("managed set cannot replace an existing incarnation")
	}
	if err := changedOne(tx.ExecContext(ctx, `UPDATE managed_sets SET remote_id=?,phase='active',diagnostic='',updated=?
WHERE repo_id=? AND name=? AND phase!='planned' AND (remote_id=0 OR remote_id=?)`, id, timestamp(), own.repoID, own.name, id)); err != nil {
		return err
	}
	return tx.Commit()
}

// managedCurrent is an additional fence once a repository enters automatic
// management. The state-layer API can still represent an explicitly bound set
// without an ownership row; the production Registry never adopts such a row.
func (s *Store) managedCurrent(ctx context.Context, key SetKey) error {
	var phase string
	var id int
	err := s.db.QueryRowContext(ctx, `SELECT phase,remote_id FROM managed_sets WHERE repo_id=?`, key.RepositoryID).Scan(&phase, &id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if phase != "active" || id != key.ScaleSetID {
		return errors.New("managed scale set is not currently verified")
	}
	return nil
}
