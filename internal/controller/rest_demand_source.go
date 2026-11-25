package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

// appRESTDemandSource adapts *githubapp.App to RESTDemandSource, the one seam
// RESTDemandTracker calls through. It carries no credential itself -- App
// mints and scopes the read token per call, exactly as ScaleSetClient does
// for Administration:write elsewhere in this package.
type appRESTDemandSource struct {
	app          *githubapp.App
	installation githubapp.Installation
}

// NewGitHubRESTDemandSource builds the production RESTDemandSource. installation
// must be the same one Registry and the session manager already hold; this
// reader mints its own actions:read token from it and never widens App's
// existing Administration-write credential to do so. It returns
// githubapp.ErrActionsReadNotGranted when the installation does not grant the
// `actions` permission, so the caller can disable the reader rather than mint
// a token GitHub would refuse on every read.
func NewGitHubRESTDemandSource(app *githubapp.App, installation githubapp.Installation) (RESTDemandSource, error) {
	if app == nil || installation.ID <= 0 {
		return nil, errors.New("REST demand source requires an App and a verified installation")
	}
	if !installation.GrantsActionsRead() {
		return nil, githubapp.ErrActionsReadNotGranted
	}
	return &appRESTDemandSource{app: app, installation: installation}, nil
}

// NewGitHubRESTDemandTracker is the daemon's REST demand tracker. REST demand
// is optional: when the installation lacks actions:read it logs ONE error
// naming the missing permission and its consequence, and returns a disabled
// tracker whose Run returns at once -- it never refuses to start the
// controller. logger nil uses slog.Default().
func NewGitHubRESTDemandTracker(store *Store, app *githubapp.App, installation githubapp.Installation,
	platform string, interval time.Duration, logger *slog.Logger) (*RESTDemandTracker, error) {
	if logger == nil {
		logger = slog.Default()
	}
	source, err := NewGitHubRESTDemandSource(app, installation)
	if errors.Is(err, githubapp.ErrActionsReadNotGranted) {
		logger.Error("REST demand disabled: the GitHub App installation lacks actions:read; queued jobs will be discovered only by session envelopes.",
			"missing_permission", "actions:read", "installation", installation.ID,
			// The daemon loop's stalled-cycle backstop needs a fresh demand
			// reading, so it never fires without one (homelab#1211).
			"failed_cycle_restart_backstop", "inert")
		source, err = nil, nil
	}
	if err != nil {
		return nil, err
	}
	tracker := NewRESTDemandTracker(store, source, platform, interval)
	tracker.log = logger
	return tracker, nil
}

func (s *appRESTDemandSource) QueuedPoolJobs(ctx context.Context, repos []RepoRef, label string) (RESTDemandResult, error) {
	appRepos := make([]githubapp.Repository, len(repos))
	for i, ref := range repos {
		appRepos[i] = githubapp.Repository{ID: ref.ID, FullName: ref.FullName}
	}
	read, err := s.app.QueuedPoolJobs(ctx, s.installation, appRepos, label)
	result := RESTDemandResult{Counts: read.Counts, APICalls: read.APICalls, RateRemaining: read.RateRemaining,
		RateLimitKnown: read.RateLimitKnown, Refused: read.Refused, DiscoveryPending: read.DiscoveryPending,
		Failed: read.Failed, FailedFirst: read.FailedFirst, RateLimited: read.RateLimited, RetryAfter: read.RetryAfter}
	return result, err
}
