# Runner pool field guide

This guide records what running a self-hosted GitHub Actions runner pool taught us that the documentation did not. The pool runs each job in a fresh microVM (Kata on Linux, Tart on macOS), drives GitHub's scale-set queue API directly, and serves the private repositories of a single GitHub account.

Each entry follows one shape:

- **Expected:** what a reasonable operator would assume.
- **Actually:** what happens.
- **Cost:** what it does to you when you don't know.
- **Code:** where this repository handles it, as `path:line` relative to `services/runner-pool/`.
- **Status:** `fixed`, `open`, or `by design`.

Line numbers are as of the commit that last updated this guide. Entries marked *unverified* rest on a single observation that has not been reproduced.

## Pages

| Page | What it covers |
|---|---|
| [GitHub scale-set API behavior](github-scale-set-api-behavior.md) | How jobs are actually delivered, identified and lost |
| [Capacity and scheduling](capacity-and-scheduling.md) | How the pool decides which repository gets a slot, and the ways that starves |
| [Isolation and network](isolation-and-network.md) | The worker network, the host guard, and what egress a job gets |
| [Images and reproducibility](images-and-reproducibility.md) | Building, locking and publishing the controller and worker images |
| [macOS on Tart](macos-tart.md) | The macOS pool and the platform limits it runs into |
| [Operations and recovery](operations-and-recovery.md) | Running it day to day, and the traps in the CI around it |
| [Limitations and non-goals](limitations-and-non-goals.md) | What the pool will not do, and what is not solved yet |

## The trust model in one paragraph

A job is untrusted code. It runs in a microVM with its own kernel, on a worker network that a host firewall fences to public HTTPS only. The controller holds the GitHub App key and never hands it to a worker: a worker receives a single-use just-in-time (JIT) runner configuration and nothing else. The pool serves private repositories only, because a public repository lets anyone open a pull request that runs on your hardware. Jobs that need deployment secrets stay on GitHub-hosted runners, so no long-lived credential reaches the pool. The one exception, an Apple signing identity for a TestFlight upload on the macOS pool, is in [Limitations and non-goals](limitations-and-non-goals.md).
