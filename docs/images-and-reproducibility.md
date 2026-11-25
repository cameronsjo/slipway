# Images and reproducibility

The controller, the host witness and the Linux worker ship as OCI images, and the deployment pins them by digest. Two lock files record what was built and what was inspected ([`image/README.md`](../image/README.md)). The rules below keep those pins honest.

## Go binaries reproduce; the images wrapping them do not

- **Expected:** building the same source twice gives the same image digest.
- **Actually:** with `-trimpath -buildvcs=false CGO_ENABLED=0` and a pinned toolchain, the binaries reproduce byte for byte. Two `docker buildx build --no-cache` runs of the same image, on the same machine minutes apart, still produced different layer, config and manifest digests.
- **Cost:** every rebuild invalidates the lock, and the tempting fix is to re-pin whatever came out. That approves an image nobody compared.
- **Code:** set `SOURCE_DATE_EPOCH` to a **fixed constant** and pass `rewrite-timestamp=true` to the OCI exporter (`--output type=oci,dest=<file>,rewrite-timestamp=true`). Both are required. Deriving the epoch from the commit time moves every digest on every commit.
- **Status:** `fixed`. Before re-pinning any digest, build twice with `--no-cache` and compare. If they differ, fix the build, not the lock.

## An image lock has two sides

- **Expected:** a hash mismatch between the lock and the build is fixed by updating the hash.
- **Actually:** each image has a source lock (the sha256 of each binary) and a separate review lock (inspection metadata of an actually built OCI archive). Writing a new hash into one desyncs it from the other. Writing it into both claims an inspection that never happened.
- **Cost:** a thirty-second edit defeats a supply-chain control on a privileged component.
- **Code:** the documented procedure builds, exports and re-inspects with `image/verify-review-images.py`, which only verifies and writes nothing. A convenience target that runs without binary arguments never hashes a built binary, so "image check passed" says nothing about the locks.
- **Status:** `by design`. Never hand-edit a lock hash to make a gate pass.

## A release round has a fixed order

The order that has held up:

1. Build twice and confirm both builds are byte-identical.
2. Update both locks and repin the manifests.
3. Commit.
4. Publish to the registry and read each image back by digest.
5. Only then push the branch that pins the digest.

- **Expected:** you can push the pin and publish afterwards.
- **Actually:** a cluster that reconciles from `main` pulls whatever `main` pins. An unpublished digest is an image-pull failure the moment it merges.
- **Cost:** a failed rollout of the component that runs every other job, which is hard to fix through CI that depends on it.
- **Code:** manifests pin `registry/…@sha256:<digest>`, and a manifest check compares them with the review lock.
- **Status:** `by design`. Never pin an unpublished digest. If the publish script reads its lock path from a default, point it at the round's own working tree; a stale path fails the lock check.

## CI caches survive a runner move

- **Expected:** moving a repository to a new runner type starts with clean tool caches.
- **Actually:** an Actions cache is keyed by OS and architecture, not by runner. After a move, `gitleaks-action` restores the old cache, prints `cache restored`, and then fails with `Unable to locate executable file: gitleaks`, because the cached tool layout differs on the new worker.
- **Cost:** the red step names the binary, not the cache, so the cache is the last thing anyone suspects.
- **Code:** none; this is a one-time purge per migrated repository (`gh cache list -R <repo>`, then `gh cache delete <id> -R <repo>`).
- **Status:** `by design` (GitHub). Do not change the action's version to chase it.
