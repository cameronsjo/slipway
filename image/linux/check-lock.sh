#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
worker=
source_only=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --worker)
            [[ $# -ge 2 ]] || { echo '--worker requires a path' >&2; exit 2; }
            worker=$2
            shift 2
            ;;
        --source-only)
            source_only=true
            shift
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 2
            ;;
    esac
done

python3 - "$root" "$worker" "$source_only" <<'PY'
import hashlib
import json
import re
import stat
import sys
from pathlib import Path

root = Path(sys.argv[1])
worker = sys.argv[2]
source_only = sys.argv[3] == "true"
lock = json.loads((root / "image-lock.json").read_text())
dockerfile = (root / "Dockerfile").read_text()
entrypoint = (root / "entrypoint.sh").read_text()

if lock.get("schema") != 1 or lock.get("platform") != "linux/amd64":
    raise SystemExit("unsupported Linux image lock")
for key in ("runner_base", "docker_base", "worker_binary", "worker_sha256", "bootstrap_ca", "bootstrap_ca_sha256"):
    if not isinstance(lock.get(key), str) or not lock[key]:
        raise SystemExit(f"missing lock field: {key}")
for key in ("runner_base", "docker_base"):
    if "@sha256:" not in lock[key] or not re.fullmatch(r".+@sha256:[0-9a-f]{64}", lock[key]):
        raise SystemExit(f"{key} is not digest-pinned")
    if f"FROM {lock[key]} AS " not in dockerfile:
        raise SystemExit(f"Dockerfile does not use locked {key}")

from_refs = re.findall(r"^FROM\s+(\S+)", dockerfile, re.MULTILINE)
if len(from_refs) != 2 or any("@sha256:" not in ref for ref in from_refs):
    raise SystemExit("every external Dockerfile base must be digest-pinned")
if re.search(r"^\s*VOLUME\s+", dockerfile, re.MULTILINE):
    raise SystemExit("the worker image must not declare an implicit volume")
required_docker_paths = (
    "/usr/local/bin/docker",
    "/usr/local/bin/dockerd",
    "/usr/local/bin/dockerd-entrypoint.sh",
    "/usr/local/libexec/docker/cli-plugins/docker-buildx",
)
for path in required_docker_paths:
    if path not in dockerfile:
        raise SystemExit(f"missing Docker component: {path}")
if "COPY --from=worker /runner-pool-worker-linux-amd64 /usr/local/bin/runner-pool-worker" not in dockerfile:
    raise SystemExit("Dockerfile does not copy the locked worker binary")
ca = root / lock["bootstrap_ca"]
if not ca.is_file() or hashlib.sha256(ca.read_bytes()).hexdigest() != lock["bootstrap_ca_sha256"]:
    raise SystemExit("bootstrap CA does not match image-lock.json")
if f"COPY --chmod=0444 {lock['bootstrap_ca']} /etc/runner-pool/bootstrap/ca.pem" not in dockerfile:
    raise SystemExit("Dockerfile does not install the locked bootstrap CA readably")
if b"BEGIN CERTIFICATE" not in ca.read_bytes():
    raise SystemExit("bootstrap CA is not a PEM certificate")
if "USER 1001:121" not in dockerfile or "WORKDIR /actions-runner" not in dockerfile:
    raise SystemExit("Dockerfile does not set the non-root runner identity/root")
if "groupmod --non-unique --gid 121 docker" not in dockerfile or "groupadd --system --non-unique --gid 121 docker" not in dockerfile:
    raise SystemExit("Dockerfile does not bind the Docker socket group to GID121")
paths = lock.get("paths", {})
for name in ("workspace", "support", "docker_socket", "docker_root", "tmp"):
    if not isinstance(paths.get(name), str) or paths[name] not in dockerfile:
        raise SystemExit(f"missing locked worker path: {name}")
if "DOCKER_HOST=unix:///var/run/docker.sock" not in dockerfile:
    raise SystemExit("Docker client is not pinned to the job-local Unix socket")
if "ENTRYPOINT [\"/usr/local/bin/slipway-bootstrap\"]" not in dockerfile:
    raise SystemExit("Dockerfile does not use the bootstrap entrypoint")
for marker in ("HELLO %s %s", "'CAP '*", "ACK %s %s", 'exec "$@"'):
    if marker not in entrypoint:
        raise SystemExit(f"bootstrap entrypoint is missing handshake marker: {marker}")
if "SLIPWAY_BOOTSTRAP_TOKEN" not in entrypoint or "SLIPWAY_BOOTSTRAP_EXPECTED_UID" not in entrypoint:
    raise SystemExit("bootstrap entrypoint does not preserve the authenticated worker contract")

if worker:
    path = Path(worker)
    if not path.is_file() or not (path.stat().st_mode & stat.S_IXUSR):
        raise SystemExit("worker binary must be an executable regular file")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != lock["worker_sha256"]:
        raise SystemExit("worker binary does not match image-lock.json")
elif not source_only:
    raise SystemExit("supply --worker for artifact verification or --source-only")

print("linux worker image lock: source checks passed" if not worker else "linux worker image lock: source and worker checks passed")
PY
