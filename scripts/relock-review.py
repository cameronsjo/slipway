#!/usr/bin/env python3
"""Relock one role in review-image-lock.json from a real OCI archive.

Every value is READ OUT of the archive with verify-review-images.py's own blob
and layer readers. Nothing is typed in: a hand-edited hash is a claim about an
image nobody inspected, which is the failure this script exists to prevent.

usage: relock-review.py <lock.json> <role> <archive.oci> <registry-repo>
"""

import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

lock_path, role, archive_path, repo = (
    Path(sys.argv[1]),
    sys.argv[2],
    Path(sys.argv[3]),
    sys.argv[4],
)

spec = importlib.util.spec_from_file_location(
    "verifier", lock_path.parent / "verify-review-images.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

lock = json.loads(lock_path.read_text())
entry = lock[role]

archive_bytes = archive_path.read_bytes()
entry["archive_sha256"] = verifier.digest(archive_bytes)

with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as archive:
    index = json.load(archive.extractfile("index.json"))
    if len(index.get("manifests", [])) != 1:
        raise SystemExit(f"{role}: expected one OCI manifest")
    descriptor = index["manifests"][0]
    if descriptor.get("platform") != {"architecture": "amd64", "os": "linux"}:
        raise SystemExit(f"{role}: expected linux/amd64")
    manifest = json.loads(verifier.read_blob(archive, descriptor))
    config = json.loads(verifier.read_blob(archive, manifest["config"]))
    files, _ = verifier.read_files(archive, manifest["layers"])

    entry["manifest_digest"] = descriptor["digest"]
    entry["config_digest"] = manifest["config"]["digest"]
    entry["config"] = {
        "user": config["config"].get("User") or "",
        "entrypoint": config["config"].get("Entrypoint"),
        "working_dir": config["config"].get("WorkingDir") or "/",
        "implicit_volumes": bool(config["config"].get("Volumes")),
    }
    if "file_count" in entry:
        entry["file_count"] = len(files)
    # Keep the SELECTED payload set exactly as reviewed -- this refreshes the
    # recorded hash of each locked path, and refuses rather than silently
    # dropping one that the new image no longer contains.
    for name in entry["files"]:
        if name not in files:
            raise SystemExit(f"{role}: locked payload {name} is absent from the new image")
        entry["files"][name] = files[name]

entry["published_reference"] = f"{repo}@{entry['manifest_digest']}"

lock_path.write_text(json.dumps(lock, indent=2, sort_keys=False) + "\n")
print(f"{role}: manifest {entry['manifest_digest']} archive sha256:{entry['archive_sha256']}")
