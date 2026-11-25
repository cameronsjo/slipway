# Limitations and non-goals

## Non-goals

- **Public repositories.** The pool runs untrusted pull-request code only from private repositories. Public repositories belong on hosted runners or a separate fleet whose threat model assumes strangers.
- **Deployment secrets.** Jobs that sign, publish or deploy stay on hosted runners, so no long-lived credential reaches a pool worker. That includes cloud tokens, signing keys and OIDC trust. The one exception is an Apple code-signing identity plus an App Store Connect API key on the macOS pool, for a TestFlight upload from `main`, held on an in-guest RAM disk (`docs/decisions/0017-apple-signing-in-ephemeral-pool-guests.md` in homelab). Notarization, the Sparkle key, and everything on the Linux pool stay hosted.
- **Autoscaling across machines.** One controller per platform manages the workers on one host. There is no multi-host scheduling.
- **Organizations and GitHub Enterprise Server.** The pool was built and measured against github.com with a GitHub user account, not an organization. The GHES job shape (`JobAvailable` plus `AcquireJobs`) is handled in code but has not been run live.
- **Speed.** Each job boots a fresh microVM and pulls its images, so it starts slower than a warm hosted runner. The pool trades start latency for isolation and cost.

## Hard limits

| Limit | Where it comes from |
|---|---|
| Linux worker slots | Thin-pool storage first, then memory. Each worker is a fixed-size VM with three writable thin devices. The worker subnet and host guard are sized to the slot count. |
| macOS worker slots | Apple's two-concurrent-macOS-VM limit per host, shared with any other macOS VM on that host. |
| Repositories per controller | One long-poll subscription per repository scale set. The ceiling is configurable; at about 50 repositories the exploratory rotation takes about 45 minutes. |
| Egress | Public TCP 443 only. No port 80, no LAN, no host services. |
| Docker storage per job | dockerd's data root is on its own 10 GiB thin device (homelab#983), so a job can unpack images up to about 9 GiB and then hits `ENOSPC`. Before that change it was a 4 GiB memory-backed volume, and a large image stalled mid-extraction. |
| Slot turnover | About 3.6 minutes of idle slot time between one job ending and the next worker starting. Many short jobs cost more than one long one. |

## What's not solved yet

- **No operator command for journal state.** Inspecting or releasing a lease means reading the controller's SQLite journal directly. There is no status or release subcommand.
- **Run-level wedges.** Some queued runs never receive a runner while newer ones do. The cause is unknown; new runs are the workaround.
- **Hosted fallback for the pool's own CI.** When the pool is broken, its own fixes wait on it. A hand-run hosted copy of the required checks exists, but has not been proven against a live pull request.
- **Monitoring.** A starving repository is visible only in the controller's cycle line. There is no alert on "queued jobs with nothing advertised".
- **Teardown held by the Docker sidecar.** A worker Pod stays `Running` after its runner exits, which holds the slot an extra cycle and leaves Pods behind when the controller is stopped.
