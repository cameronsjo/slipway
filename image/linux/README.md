# Linux worker image source

This directory is the reviewable source bundle for the Linux pool worker image.
It combines the digest-pinned native GitHub Actions runner base with the
digest-pinned Docker engine/buildx payload and the existing
`runner-pool-worker` launcher. It is source only: no image is published or
deployed by these files.

`bootstrap-ca.pem` is the public trust root for the internal controller Service.
Its SHA-256 is part of `image-lock.json`; the matching server leaf and private
key live only in the SOPS-encrypted MS-A2 manifest bundle. Changing this trust
root requires a new worker image and digest. The CA private key was discarded
after the current leaf was signed, so rotation creates a new trust root rather
than extending this one.

The image is used for both sides of the one-job Kata VM. The runner container
starts `slipway-bootstrap` as UID `1001:GID121`. It reports its
downward-projected Pod name and UID, reads one `CAP` frame only after the
controller verifies that identity, sends an `ACK`, and execs the existing
`runner-pool-worker`. The capability exists only in the attach stream and the
initial runner process; it is never written to a Pod, Secret, file, or argv.

The backend may start the Docker sidecar from this same image with
`dockerd-entrypoint.sh` and its explicit Unix-socket arguments. Only that
sidecar is privileged inside Kata. The runner remains non-root; its workspace,
Docker socket, Docker state, temporary directory, and runner support directory
are separate per-job mounts at the paths recorded in `image-lock.json`. The
Dockerfile declares no `VOLUME`, so the controller can attest fresh paths and
cleanup independently.

Run the offline source checks from the service directory:

```sh
make image-check
python3 image/linux/test_bundle.py -v
```

After building the existing worker binary, verify the exact binary artifact
against the lock before an image build:

```sh
make build OUTPUT_DIR=/private/tmp/slipway-build
make image-check-worker WORKER_BINARY=/private/tmp/slipway-build/runner-pool-worker-linux-amd64
```

The build itself requires the two locked base images to be present in the local
builder cache; it does not use a floating tag:

```sh
docker buildx build --pull=false --platform linux/amd64 \
  --build-context worker=/private/tmp/slipway-build \
  -f image/linux/Dockerfile -t slipway-linux:review image/linux
```

The native runner release contained by the locked runner image remains a live
image-content gate: before activation, inspect `/actions-runner/bin/Runner.Listener`
and record its version and digest from the built image. The lock prevents base
image drift, while the backend acceptance still has to prove fresh workspaces,
Docker/buildx, container actions, job containers, services, network policy and
physical cleanup in the actual Kata VM.
