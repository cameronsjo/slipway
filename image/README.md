# Runner pool images

The Linux worker, controller, and host witness have separate image sources and
separate trust boundaries.

- `linux/` is the untrusted one-job Kata worker. It contains the native Actions
  runner, Docker/buildx, and the one-use bootstrap launcher.
- `controller/` is a `scratch` image containing the static controller and a
  public CA bundle copied from the already pinned runner base. It runs as UID
  and GID 65532.
- `host-witness/` is a `scratch` image containing the static witness and a tiny
  static host-tool launcher. It contains no App or Kubernetes credential. The
  launcher enters a read-only `/host` root and permits only the installed K3s
  `crictl`, K3s `ctr`, and `lvs` paths recorded in its lock.

Build the binaries and verify their exact digests before an image build:

```sh
make build OUTPUT_DIR=/private/tmp/slipway-build
make image-check-control-plane \
  CONTROLLER_BINARY=/private/tmp/slipway-build/runner-pool-linux-amd64 \
  WITNESS_BINARY=/private/tmp/slipway-build/runner-pool-host-witness-linux-amd64 \
  HOST_TOOL_BINARY=/private/tmp/slipway-build/runner-pool-host-tool-linux-amd64
```

The review builds use named contexts so no binary is copied into the source
tree.

**Both reproducibility settings below are load-bearing, and the digests in this
file are only meaningful with them.** Without `SOURCE_DATE_EPOCH` and the OCI
exporter's `rewrite-timestamp=true`, two `--no-cache` builds of the same image on
the same machine, minutes apart, produce different layer, config, manifest and
archive digests -- measured, not assumed. That is why an earlier lock could not
be reproduced by anyone, including whoever wrote it. With both set, two
independent builds agree byte for byte, worker image included.

`SOURCE_DATE_EPOCH` is a **fixed constant**, deliberately not derived from the
commit or the clock: deriving it would change the digests on every commit and
invalidate the lock each time, which is the opposite of what it is for.

```sh
export SOURCE_DATE_EPOCH=1757721600

docker buildx build --pull=false --platform linux/amd64 \
  --build-context worker=/private/tmp/slipway-build \
  -f image/linux/Dockerfile -t slipway-linux:review image/linux
docker buildx build --pull=false --platform linux/amd64 \
  --build-context controller=/private/tmp/slipway-build \
  -f image/controller/Dockerfile -t sjo-runner-pool-controller:review image/controller

docker buildx build --pull=false --platform linux/amd64 \
  --build-context witness=/private/tmp/slipway-build \
  --build-context host-tool=/private/tmp/slipway-build \
  -f image/host-witness/Dockerfile -t sjo-runner-pool-host-witness:review image/host-witness
```

Export each build as an OCI archive with `rewrite-timestamp=true`, for example
`--output type=oci,dest=<file>.oci,rewrite-timestamp=true`, and pass all three to
`verify-review-images.py`. The verifier checks their content-addressed blobs,
platform, process configuration, lack of implicit volumes, locked payloads,
and worker trust-root permissions without loading or running an image.

```sh
python3 image/verify-review-images.py \
  /private/tmp/sjo-runner-pool-evidence/linux-images/controller.oci \
  /private/tmp/sjo-runner-pool-evidence/linux-images/host-witness.oci \
  /private/tmp/sjo-runner-pool-evidence/linux-images/worker.oci
```

## The privileged dockerd scan expires

`review-image-lock.json`'s `dockerd.scan_record` is the only entry whose
evidence has a shelf life. Every other binding in the lock ties a record to an
**image** — a digest, a platform, a config. The scan's findings are a statement
about a vulnerability database on a particular **day**.

So a CVE published tomorrow moves no digest here, fails no other gate, and
changes not one byte of the record. The counts simply stop describing the image
while everything stays green. `check-review-lock.py` therefore refuses a
`scanned_on` older than `SCAN_MAX_AGE_DAYS` (30).

**A refusal here does not mean the pin is wrong.** The pinned digest is still
exactly what was scanned; what expired is the claim that the findings describe
it. Re-scan the pinned digest, update `scanned_on`, `findings` and `verdict`,
and state the new CRITICAL count in the PR.

`scanned: false` is no longer a state this lock can express. It used to be, from
when the image had genuinely never been examined and saying so was honest — but
it made deletion the cheapest bypass in the file: dropping the record skipped
the digest, platform and config bindings *and* the age bound together, and the
checker still printed success. An unscanned privileged sidecar is not a state
this pool may run in.

The *function* takes `today` as a parameter so tests can pin it — a verdict that
depends on the wall clock is one nobody can write a negative control for. The
*gate* calls it with the real `date.today()`, and that wiring is tested
separately: the earlier tests all called the function directly, and deleting the
call from `check_dockerd` left every one of them green.

The current reviewed artifacts are bound by `review-image-lock.json`. `make`
invokes the module's Go 1.26.4 toolchain explicitly so CI and operator builds
produce the same locked binaries.

**The manifest digests are not restated here.** Read them from the lock:

```sh
python3 -c "
import json
lock = json.load(open('review-image-lock.json'))
for role in ('controller', 'witness', 'worker'):
    print(role.ljust(11), lock[role]['manifest_digest'])
"
```

An earlier version of this paragraph listed all three inline, and two of them
went stale at the next republish while reading as current — both were real
digests of real older images, so nothing looked wrong. The lock is the one copy.

The worker built from the `2026-11-14.1` archive reports Actions runner 2.338.2,
Docker 29.9.3, and Buildx 0.39.0. That round (#1349) moved the dockerd pin from
`29.8.1-dind` to `29.9.3-dind` and dropped one COPY (below).

**The 29.9.x dind image ships no `containerd` binary**
(docker-library/docker#585). dockerd falls back to its embedded containerd
(moby/moby#53388) and logs `Running with experimental embedded-containerd mode`
at startup. That is expected on this pin, not a fault. The worker Dockerfile's
`COPY` of `/usr/local/bin/containerd` was dropped with it, so `DIND_COPY_COUNT`
is 9 and the worker `file_count` moved by one, to 31959.

The `2026-11-13.1` round before it moved only the runner version. GitHub refuses
runner v2.336.0 (`Runner version v2.336.0 is deprecated and cannot receive
messages.`), and the worker execs `Runner.Listener run` directly, so it cannot
self-update inside the pod.

**The whole third-party runner base was rebuilt, not just the runner.** The new
base is `2.338.2-ubuntu-noble`. The old pin was digest-only, and no tag serves it
any more, so the variant was matched by content: its build history, with the
base-layer hash and the runner version taken out, is identical to
`2.338.2-ubuntu-noble`'s. None of the base's nine layers carries over: the
Ubuntu rootfs, the package and tool layer (rebuilt upstream on 2026-10-25), and
myoung34's `entrypoint.sh`, which the worker never runs, all changed. The only
part checked against an upstream source is the runner tree. `/actions-runner`
matches `actions-runner-linux-x64-2.338.2.tar.gz` (sha256 `baebf204…147c40`)
file for file. The OS packages were not compared or scanned. Among the locked
payload hashes only `Runner.Listener` changed, and `file_count` moved with the
new base.

**The runner base is a pin with an expiry.** GitHub stops serving an old runner
release, and nothing in the image updates it. When the pool's `runner` containers
log `is deprecated and cannot receive messages`, bump `runner_base` to the same
variant at the current release.

**Published** to the Gitea registry with `skopeo copy --preserve-digests` from
those exact archives. The three images are **not all on one tag**, because a
republish only covers the images that actually changed, and **the current tag
differs per image**:

| Image | Current tag | In the registry? |
|---|---|---|
| `sjo-runner-pool-controller` | `2026-09-22.6` | yes |
| `sjo-runner-pool-host-witness` | `2026-09-22.6` | yes |
| `sjo-runner-pool-linux-worker` | `2026-11-14.1` | yes, by the operator step that precedes the merge (below) |

The worker moved alone on 2026-11-14 for the dockerd pin bump (#1349), as it did
on 2026-11-13 for the runner base bump and on 2026-09-17 for the first dockerd
pin bump (#765). None of them touched the control plane.

**A worker round has three manual steps, and the order is fixed.**

1. Publish the archive by hand with `skopeo copy --preserve-digests`, then read
   it back by digest. `publish.sh` covers only the controller and witness.
2. Pre-pull the digest on MS-A2 as root with
   `hosts/ms-a2/github-runner-pool/stages/stage-3p-image-pull.sh --standalone --manifest-sha256 <hex>`,
   where `<hex>` is the manifest sha256 that
   `hosts/ms-a2/github-runner-pool/stages/prepare-host-files.sh` printed when
   it staged the tree. The worker Pod has no `imagePullSecrets`, so an
   unpulled digest fails at worker `Create`. **Do not run it the obvious way**,
   with `sudo bash` straight from the staged tree in your home. The stage
   parses `lib.sh` from that user-writable tree before it verifies anything,
   and its `python3 -c` calls import from the working directory. Root must
   first copy the tree into a directory only root can write and check that
   copy against the manifest hash. It must then run the stage from the copy,
   with the working directory set to `/`. Fixing this in the stage scripts
   themselves is tracked in Linear CAM-700.
3. Merge the pin PR by hand. `pin-guard.py` refuses any worker digest change,
   so `merge-when-green.sh` will not merge it.

Do not merge before the registry readback of the new digest matches
`review-image-lock.json` and the node holds it.

Every superseded control-plane tag still exists and still serves its older
image, so verifying against one reports a mismatch on a coherent tree. Do not
read a tag's existence as evidence it names the reviewed digest -- read the
digest.

`2026-09-22.6` is the pair this tree pins -- everything `2026-09-22.5` carried,
plus one merged controller change. **Advertise to repositories with available
jobs** (#900): demand now reads `TotalAvailableJobs` alongside
`TotalAssignedJobs`, so a repository the pool has never advertised capacity for
can be served. Such a repository has no journaled rows and no assigned jobs --
GitHub's broker assigns only to a scale set that advertised -- so its queued work
exists solely as that statistic, and reading assigned jobs alone scored it idle.
Three repositories each held a job queued for 45+ minutes while the pool logged
`admission=2 free=2 charged=0`. The same change gives a statistic an upper age
bound as well as a lower one: a poll that finds nothing carries no statistics, so
without it an idle repository's stale count is believed for the life of the
process.

**The journal does not move this round.** It stays at version 14, so nothing
migrates and a rollback to `2026-09-22.5` is a plain image revert. `-capacity`
is unchanged at 1. The pair is **not published yet**. Every earlier
control-plane tag is superseded, not partial, and each still serves its own
older image, so verifying against one reports a mismatch on a coherent tree.

**`2026-09-22.5` is published and is now superseded, and it is the tag running
in production today.** It carries the defect this round fixes: a repository the
pool has never advertised for reaches a slot only through the single rotating
exploratory slot, roughly one chance per sweep of the whole roster. It is a
starvation, not a crash -- every component reads healthy while jobs sit queued.
The journal it opened is version 14, which is the version this round keeps.

**`2026-09-22.4` is published and is superseded.** After any controller restart
its Linux pool starves, because inherited broker assignments stay trusted and
consume the pool's capacity on work that no longer exists.

**`2026-09-22.3` is published and is superseded.** Its controller errors every
cycle once assigned demand exceeds admission, which is the multi-repo case, so
the pool serves nothing while every component reads healthy.

**`2026-09-22.2` is published and is superseded.** It carries no known defect of
its own; it predates both the installation-scope change and this round, so
deploying it runs a controller that refuses an all-repositories installation.

**`2026-09-22.1` is published and must NOT be deployed.** It carries the
availability regression a security review caught after publication: any image
unpack onto the dedicated snapshotter made the witness return an incomplete
scope, so admission stayed fenced for the length of a pull. It is superseded
rather than deleted, because deleting a published tag that a node may already
have pulled replaces one failure with a less legible one.

**The worker image did NOT move in the `2026-09-22.6` round, so that round owed
no root pre-pull.** The
worker binary does not import `internal/controller` or `internal/githubapp`, the
packages this round changes -- `go list -deps ./cmd/runner-pool-worker` matches
neither, across 203 listed dependencies -- and `image/linux/entrypoint.sh` and
its Dockerfile are untouched: a fresh `--no-cache` build of the worker
reproduced `sha256:ff934a8e...`, the manifest, config and payload set the lock
already carried, and the worker binary hashed to the value
`image/linux/image-lock.json` already held. Its **archive** hash did move, which
is the buildkit case the next section describes and not an image change -- the
worker entry's relock touched that one field and nothing else. The witness binary moved again with
no witness source change, because it links that package (`go list -deps
./cmd/runner-pool-host-witness`). The question is asked per round rather than
answered once: an earlier round moved the worker on an `entrypoint.sh` change
with an unchanged binary.

### `archive_sha256` does not reproduce across buildkit versions; the manifest digest does

Measured on the `2026-09-22.4` round. The worker image rebuilt to a
**byte-identical manifest, config and payload set** --
`verify-review-images.py` matched every one -- while its OCI **tar wrapper**
hashed differently from the value recorded when it was published. Nothing about
the image changed; the archive is a container for it, and the two are not the
same claim.

So `archive_sha256` is a record of the file that was handed to `skopeo`, useful
for the publish script's before-copy check within one session. It is **not** a
reproducibility gate, and a mismatch there with a matching `manifest_digest` is
not evidence of tampering. The worker's archive value DID need refreshing on the
`2026-09-22.6` round, and only that value: its manifest, config, payload set and
`published_reference` all reproduced, so the image is the one already published
and the one the deployment and the admission policy pin.

The host tool did **not** move this round: a fresh build reproduced
`7e8ca4ba...`, so both launchers in the witness image are the same binary they
were. The witness payload is still three files.

Each registry manifest digest is read back and must equal the `manifest_digest` in
`review-image-lock.json`, so the thing published is the thing reviewed. Those
references are recorded per image as `published_reference`, and
`check-review-lock.py` derives the expected value from `manifest_digest`; note
that catches a half edit only -- what binds a digest to a real image is
`verify-review-images.py`, and what binds the bundle to the lock is
`verify-manifests.py`.

An earlier `2026-09-13` tag was published on `go1.26.3` and has been **deleted**:
that toolchain carries CVE-2026-27145 (`crypto/x509`), CVE-2026-42504 (`mime`)
and CVE-2026-42507 (`net/textproto`), all fixed in `1.26.4`, which this build
uses. Nothing is imported on MS-A2; the bundle still renders with `replicas: 0`
and is not in `hosts/ms-a2/kustomization.yaml`.

The checked-in locks identify source inputs and binary payloads. They do not
stand in for the final image digest or live inspection. Publication, import,
and deployment remain separate steps after the exact images are built and
reviewed.
