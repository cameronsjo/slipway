package controller

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
)

// appQueueJanitorSource adapts the JANITOR's *githubapp.App to
// QueueJanitorSource. It is never the pool App: actions:write exists only on
// the janitor's own installation.
type appQueueJanitorSource struct {
	app          *githubapp.App
	installation githubapp.Installation
}

func (s *appQueueJanitorSource) Pass(ctx context.Context, repos []RepoRef, label string, enforce bool) (QueueJanitorResult, error) {
	appRepos := make([]githubapp.Repository, len(repos))
	for i, ref := range repos {
		appRepos[i] = githubapp.Repository{ID: ref.ID, FullName: ref.FullName}
	}
	pass, err := s.app.QueueJanitorPass(ctx, s.installation, appRepos, label, enforce)
	result := QueueJanitorResult{Examined: pass.Examined, Cancelled: pass.Cancelled, WouldCancel: pass.WouldCancel,
		KeptUnprovenJobs: pass.KeptUnprovenJobs, Capped: pass.Capped, SkippedErrors: pass.SkippedErrors,
		FirstError: pass.FirstError, Refused: pass.Refused, APICalls: pass.APICalls, RateLimited: pass.RateLimited,
		RetryAfter: pass.RetryAfter}
	for _, action := range pass.Actions {
		result.Actions = append(result.Actions, QueueJanitorAction{Repository: action.Repository, RunID: action.RunID,
			Rule: action.Rule, WorkflowID: action.WorkflowID, Jobs: action.Jobs, RefRunID: action.RefRunID,
			RefJobs: action.RefJobs, Age: action.Age, URL: action.URL, Cancelled: action.Cancelled})
	}
	for _, kept := range pass.Kept {
		result.Kept = append(result.Kept, QueueJanitorKept{Repository: kept.Repository, RunID: kept.RunID,
			Rule: kept.Rule, Reason: kept.Reason})
	}
	return result, err
}

// NewGitHubQueueJanitor is the daemon's queue janitor. The janitor is
// optional and never refuses to start the controller: off, or an
// installation missing a permission the mode needs, returns a disabled
// janitor after ONE log line. The installation's permissions are read once,
// at startup, so a permission granted later takes effect only after the
// controller restarts, and the line says so. logger nil uses slog.Default().
func NewGitHubQueueJanitor(store *Store, app *githubapp.App, installation githubapp.Installation, platform string,
	mode QueueJanitorMode, interval time.Duration, logger *slog.Logger) (*QueueJanitor, error) {
	if logger == nil {
		logger = slog.Default()
	}
	var source QueueJanitorSource
	switch {
	case mode != QueueJanitorDryRun && mode != QueueJanitorEnforce:
		logger.Info("Queue janitor off.", "mode", QueueJanitorOff)
		mode = QueueJanitorOff
	case app == nil || installation.ID <= 0:
		return nil, errors.New("queue janitor requires its App and a verified installation")
	default:
		if missing := installation.JanitorMissingPermissions(mode == QueueJanitorEnforce); len(missing) > 0 {
			logger.Error("Queue janitor disabled: the janitor GitHub App installation lacks a permission this mode needs. Grant it, then restart the controller; permissions are read only at startup.",
				"missing_permission", strings.Join(missing, ","), "mode", mode, "installation", installation.ID)
			mode = QueueJanitorOff
			break
		}
		source = &appQueueJanitorSource{app: app, installation: installation}
		logger.Info("Queue janitor on.", "mode", mode, "interval", interval, "installation", installation.ID)
	}
	janitor := NewQueueJanitor(store, source, platform, mode, interval)
	janitor.log = logger
	return janitor, nil
}
