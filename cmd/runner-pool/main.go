// runner-pool keeps the bounded API probes separate from the production
// scheduler. Unsupported or incomplete platform configuration fails startup.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cameronsjo/slipway/internal/githubapp"
	"github.com/cameronsjo/slipway/internal/probe"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

type receipt struct {
	Mode                  string                 `json:"mode"`
	Installation          int64                  `json:"installation,omitempty"`
	Repositories          []githubapp.Repository `json:"repositories,omitempty"`
	Attempt               *probe.Attempt         `json:"attempt,omitempty"`
	APIProven             bool                   `json:"scale_set_jit_session_apis_proven"`
	WorkerExecutionProven bool                   `json:"worker_execution_proven"`
	FailedStage           string                 `json:"failed_stage,omitempty"`
	Failure               string                 `json:"failure,omitempty"`
	HTTP                  githubapp.HTTPFailure  `json:"last_http"`
	CleanupFailure        string                 `json:"cleanup_failure,omitempty"`
}

// exitSessionsDeaf is the daemon's exit code for a deliberate deaf-session
// restart, distinct from exitFailure so an operator (or a supervisor policy)
// can tell "GitHub stopped talking to us" apart from any other fatal error at
// a glance, without parsing the log line. A restart-required session error and
// a stalled run of failing cycles exit with it too (homelab#1211): each is a
// fault only a new process clears, and the log line names which one fired.
const (
	exitFailure      = 1
	exitSessionsDeaf = 3
)

func main() {
	// Suppress client-go's klog stderr chatter (e.g. benign WebSocket attach
	// close errors on every worker bootstrap). SetLogger(logr.Discard()) mutes
	// klog.ErrorS structured calls that bypass SetOutput. Structured operational
	// logs use Go's standard logger / slog.
	klog.LogToStderr(false)
	klog.SetOutput(io.Discard)
	klog.SetLogger(logr.Discard())
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if exitsForRestart(err) {
			os.Exit(exitSessionsDeaf)
		}
		os.Exit(exitFailure)
	}
}

func run() error {
	mode := flag.String("mode", "discover", "discover, probe, recover, or daemon")
	bootstrap := flag.String("installation-repo", "cameronsjo/homelab", "explicit repository selecting the existing App installation")
	repoName := flag.String("repo", "", "repository for the bounded API probe")
	idFile := flag.String("app-id-file", "", "existing mounted App ID file")
	keyFile := flag.String("app-key-file", "", "existing mounted App key file")
	state := flag.String("state-dir", "", "private persistent journal directory")
	platform := flag.String("platform", "", "daemon platform: linux or macos")
	capacity := flag.Int("capacity", -1, "daemon qualification capacity; defaults to the platform ceiling")
	legacySlots := flag.Int("legacy-slots", -1, "daemon slots reserved for legacy workers; defaults by platform")
	bootstrapURL := flag.String("bootstrap-url", "", "daemon HTTPS bootstrap origin with explicit port")
	bootstrapAddress := flag.String("bootstrap-address", "", "daemon bind address with explicit port")
	bootstrapCertificate := flag.String("bootstrap-certificate-file", "", "daemon bootstrap TLS certificate")
	bootstrapKey := flag.String("bootstrap-key-file", "", "daemon bootstrap TLS private key")
	platformConfig := flag.String("platform-config-file", "", "daemon non-secret platform backend configuration")
	cycleInterval := flag.Duration("daemon-interval", time.Minute, "daemon controller cycle interval")
	// EMPTY is the default posture: every private, active repository the App
	// installation can see, so onboarding a private repo is an edit to its own
	// `runs-on` and nothing else. Public, archived and disabled repositories are
	// refused regardless, and the served set is bounded by -max-repositories and
	// the per-cycle creation budget. See controller.eligibleForPool.
	repositoryAllowlist := flag.String("repository-allowlist", "", "comma-separated owner/name repositories to pin; empty serves every private, active repository")
	maxRepositories := flag.Int("max-repositories", 0, "daemon bound on repositories onboarded; each served one is a long poll per cycle; 0 uses the default (80)")
	sessionDeafAfter := flag.Duration("session-deaf-after", defaultSessionDeafAfter, "daemon window with no queue envelope from any subscription before sessions are judged dead and the process exits for restart")
	failedCycleRestartAfter := flag.Duration("failed-cycle-restart-after", defaultFailedCycleRestartAfter, "daemon window of consecutive cycles failing in the session stage (global Sync or whole-sweep poll failure), while a fresh REST reading shows queued pool jobs, before the process exits for restart; must exceed -daemon-interval")
	restDemandInterval := flag.Duration("rest-demand-interval", defaultRESTDemandInterval, "daemon interval between REST reads of queued-pool-job demand, independent of the queue-session envelopes; minimum 1m")
	queueJanitor := flag.String("queue-janitor", "off", "daemon queue janitor mode: off, dry-run or enforce; any other value is treated as off")
	reapInterval := flag.Duration("reap-interval", defaultReapInterval, "daemon interval between reaper passes, which tear down finished workers and refill their slots between cycles; negative turns the reaper off")
	createConcurrency := flag.Int("create-concurrency", 1, "daemon worker creations one provisioning pass runs at once, 1..8; the first of each pass runs alone; 1 is serial and is the rollback; linux only")
	queueJanitorInterval := flag.Duration("queue-janitor-interval", defaultQueueJanitorInterval, "daemon interval between queue janitor passes; minimum 1m")
	janitorIDFile := flag.String("janitor-app-id-file", "", "daemon queue janitor's own GitHub App ID file; empty leaves the janitor off")
	janitorKeyFile := flag.String("janitor-app-key-file", "", "daemon queue janitor's own GitHub App private key file; empty leaves the janitor off")
	janitorInstallationFile := flag.String("janitor-installation-id-file", "", "daemon queue janitor's installation ID file; empty leaves the janitor off")
	mutate := flag.Bool("allow-api-mutations", false, "permit creation/cleanup of this probe's exact random scale set and runner")
	flag.Parse()
	if flag.NArg() != 0 || (*mode != "discover" && *mode != "probe" && *mode != "recover" && *mode != "daemon") || *idFile == "" || *keyFile == "" || *state == "" {
		return errors.New("supply a valid mode, App file paths, and state-dir; see -help")
	}
	if *mode == "daemon" {
		return runDaemon(RuntimeConfig{
			Platform:                  *platform,
			InstallationRepo:          *bootstrap,
			AppIDFile:                 *idFile,
			AppKeyFile:                *keyFile,
			StateDir:                  *state,
			Capacity:                  *capacity,
			LegacySlots:               *legacySlots,
			BootstrapURL:              *bootstrapURL,
			BootstrapAddress:          *bootstrapAddress,
			BootstrapCertificateFile:  *bootstrapCertificate,
			BootstrapKeyFile:          *bootstrapKey,
			PlatformConfigFile:        *platformConfig,
			CycleInterval:             *cycleInterval,
			RepositoryAllowlist:       splitRepositoryAllowlist(*repositoryAllowlist),
			MaxRepositories:           *maxRepositories,
			SessionDeafAfter:          *sessionDeafAfter,
			FailedCycleRestartAfter:   *failedCycleRestartAfter,
			RESTDemandInterval:        *restDemandInterval,
			QueueJanitor:              *queueJanitor,
			QueueJanitorInterval:      *queueJanitorInterval,
			ReapInterval:              *reapInterval,
			CreateConcurrency:         *createConcurrency,
			JanitorAppIDFile:          *janitorIDFile,
			JanitorAppKeyFile:         *janitorKeyFile,
			JanitorInstallationIDFile: *janitorInstallationFile,
		})
	}
	if *mode != "discover" && (!*mutate || *repoName == "") {
		return errors.New("probe/recover requires explicit repo and allow-api-mutations")
	}
	app, err := githubapp.New(*idFile, *keyFile)
	if err != nil {
		return err
	}
	store, err := probe.Open(*state)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result := receipt{Mode: *mode}
	fail := func(stage string, err error) error {
		result.FailedStage = stage
		result.Failure = githubapp.SafeError(err)
		result.HTTP = app.LastHTTP()
		if encodeErr := json.NewEncoder(os.Stdout).Encode(result); encodeErr != nil {
			return encodeErr
		}
		return errors.New("foundation probe failed; inspect JSON receipt and retain its journal")
	}
	installation, err := app.InstallationFor(ctx, *bootstrap)
	if err != nil {
		return fail("installation", err)
	}
	result.Installation = installation.ID
	repos, err := app.Discover(ctx, installation)
	if err != nil {
		return fail("discovery", err)
	}
	if err := store.Discovery(installation.ID, repos); err != nil {
		return fail("persist-discovery", err)
	}
	result.Repositories = repos
	if *mode == "discover" {
		result.HTTP = app.LastHTTP()
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	selected, err := eligibleProbeRepository(repos, *repoName)
	if err != nil {
		return fail("eligible-repository", err)
	}
	client, err := app.ScaleSetClient(installation, *selected)
	if err != nil {
		return fail("client", err)
	}
	pending, err := store.Pending()
	if err != nil {
		return fail("journal", err)
	}
	if pending != nil {
		result.Attempt = pending
		if pending.InstallationID != installation.ID || pending.Repo.ID != selected.ID || pending.Repo.FullName != selected.FullName {
			return fail("pending-repository-mismatch", errors.New("recover exact pending repository first"))
		}
		if *mode != "recover" {
			return fail("recovery-required", errors.New("pending reservation requires recover mode"))
		}
		if err := probe.Cleanup(store, pending, client); err != nil {
			return fail("cleanup", err)
		}
		result.HTTP = app.LastHTTP()
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	if *mode == "recover" {
		return fail("no-pending-attempt", errors.New("no pending probe"))
	}
	attempt, err := store.Begin(installation.ID, *selected)
	if err != nil {
		return fail("reserve", err)
	}
	result.Attempt = attempt
	probeErr := probe.Run(ctx, store, attempt, client)
	if probeErr == nil {
		probeErr = store.Save(attempt, "session-intent", false)
		if probeErr == nil {
			session, sessionErr := client.MessageSessionClient(ctx, attempt.SetID, attempt.Name)
			probeErr = sessionErr
			if sessionErr == nil {
				if session.Session().SessionID == uuid.Nil || session.Session().Statistics == nil {
					probeErr = errors.New("invalid scale-set message session")
				}
				// Never receive/acquire a job in this API-only probe.
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
				probeErr = errors.Join(probeErr, session.Close(closeCtx))
				closeCancel()
			}
		}
	}
	result.APIProven = probeErr == nil
	failedStage := attempt.Stage
	result.HTTP = app.LastHTTP()
	if probeErr != nil {
		result.FailedStage = failedStage
		result.Failure = githubapp.SafeError(probeErr)
	}
	cleanupErr := probe.Cleanup(store, attempt, client)
	if cleanupErr != nil {
		result.CleanupFailure = githubapp.SafeError(cleanupErr)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		return err
	}
	if probeErr != nil || cleanupErr != nil {
		return errors.New("foundation probe failed; inspect JSON receipt and retain its journal")
	}
	return nil
}

// eligibleProbeRepository picks the probe's repository from the discovery
// snapshot. The installation's repository set is not the pool's bound: an
// all-repositories installation discovers public repositories too, so the
// private-and-active rule is applied here rather than inherited from GitHub's
// selection.
func eligibleProbeRepository(repos []githubapp.Repository, name string) (*githubapp.Repository, error) {
	for _, repo := range repos {
		if repo.FullName != name {
			continue
		}
		if !repo.Private || repo.Archived || repo.Disabled {
			return nil, errors.New("probe requires a private, active repository; public, archived and disabled repositories are refused")
		}
		return &repo, nil
	}
	return nil, errors.New("repository not present in the selected or all-repositories installation")
}
