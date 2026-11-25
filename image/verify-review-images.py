#!/usr/bin/env python3
"""Verify the three exact runner-pool OCI review archives."""

import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile


HERE = Path(__file__).resolve().parent


def digest(content):
    return hashlib.sha256(content).hexdigest()


def read_blob(archive, descriptor):
    algorithm, expected = descriptor["digest"].split(":", 1)
    if algorithm != "sha256":
        raise RuntimeError("only SHA256 OCI descriptors are accepted")
    content = archive.extractfile(f"blobs/sha256/{expected}").read()
    if len(content) != descriptor["size"] or digest(content) != expected:
        raise RuntimeError("OCI blob digest or size mismatch")
    return content


def read_files(archive, layers):
    """Returns (files, directories). Directories are collected SEPARATELY and
    are not part of the locked payload, so adding them churns no lock -- but
    without them nothing could see the defect this exists for: a file locked at
    a perfectly good mode, unreachable because a parent directory had no
    execute bit. verify_reachability below is the consumer."""
    files = {}
    directories = {}
    for descriptor in layers:
        content = read_blob(archive, descriptor)
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:*") as layer:
            for entry in layer:
                name = entry.name.removeprefix("./").rstrip("/")
                parent, _, base = name.rpartition("/")
                if base == ".wh..wh..opq":
                    prefix = parent + "/"
                    files = {key: value for key, value in files.items() if not key.startswith(prefix)}
                    directories = {key: value for key, value in directories.items() if not key.startswith(prefix)}
                elif base.startswith(".wh."):
                    removed = f"{parent}/{base[4:]}".lstrip("/")
                    files.pop(removed, None)
                    directories.pop(removed, None)
                elif entry.isdir():
                    directories[name] = entry.mode
                elif entry.isfile():
                    files[name] = {
                        "sha256": digest(layer.extractfile(entry).read()),
                        "mode": f"{entry.mode:04o}",
                    }
                elif entry.issym() or entry.islnk():
                    files[name] = {"link": entry.linkname, "mode": f"{entry.mode:04o}"}
    return files, directories


def verify_reachability(role, files, directories, required):
    """Assert the worker can actually reach what it needs, as uid 1001.

    THE DEFECT THIS NAMES. `COPY --chmod=0444` applies the mode to the parent
    directories BuildKit creates as well as to the file, so
    /etc/runner-pool/bootstrap/ca.pem shipped mode 0444 inside two 0444
    directories. Every check in the Dockerfile ran as root, which ignores a
    missing traverse bit, and the lock records file modes only -- so the image
    verified clean and the worker died with `cannot read public bootstrap CA`
    after a completely successful bootstrap handshake (2026-09-21 13:51Z).

    Every parent component must be other-executable and the leaf
    other-readable, because the worker shares neither uid nor gid with root.
    """
    for path, access in sorted(required.items()):
        components = path.split("/")
        for depth in range(1, len(components)):
            parent = "/".join(components[:depth])
            if parent not in directories:
                raise RuntimeError(f"{role}: {path} has no directory entry for {parent}")
            if not directories[parent] & 0o001:
                raise RuntimeError(
                    f"{role}: {parent} is mode {directories[parent]:04o}; the worker cannot traverse it to reach {path}")
        entry = files.get(path)
        if entry is None:
            raise RuntimeError(f"{role}: {path} is absent")
        mode = int(entry["mode"], 8)
        if not mode & 0o004:
            raise RuntimeError(f"{role}: {path} is mode {mode:04o}; the worker cannot read it")
        if "x" in access and not mode & 0o001:
            raise RuntimeError(f"{role}: {path} is mode {mode:04o}; the worker cannot execute it")


def verify(role, path, expected):
    archive_bytes = path.read_bytes()
    if digest(archive_bytes) != expected["archive_sha256"]:
        raise RuntimeError(f"{role}: archive differs from reviewed artifact")
    with tarfile.open(fileobj=io.BytesIO(archive_bytes)) as archive:
        index = json.load(archive.extractfile("index.json"))
        if len(index.get("manifests", [])) != 1:
            raise RuntimeError(f"{role}: expected one OCI manifest")
        descriptor = index["manifests"][0]
        if descriptor.get("platform") != {"architecture": "amd64", "os": "linux"}:
            raise RuntimeError(f"{role}: expected linux/amd64")
        if descriptor["digest"] != expected["manifest_digest"]:
            raise RuntimeError(f"{role}: manifest differs from reviewed artifact")
        manifest = json.loads(read_blob(archive, descriptor))
        if manifest["config"]["digest"] != expected["config_digest"]:
            raise RuntimeError(f"{role}: config differs from reviewed artifact")
        config = json.loads(read_blob(archive, manifest["config"]))
        if config.get("architecture") != "amd64" or config.get("os") != "linux":
            raise RuntimeError(f"{role}: config is not linux/amd64")
        actual_config = {
            "user": config["config"].get("User") or "",
            "entrypoint": config["config"].get("Entrypoint"),
            "working_dir": config["config"].get("WorkingDir") or "/",
            "implicit_volumes": bool(config["config"].get("Volumes")),
        }
        if actual_config != expected["config"]:
            raise RuntimeError(f"{role}: unexpected process configuration")
        files, directories = read_files(archive, manifest["layers"])
        selected = {name: files.get(name) for name in expected["files"]}
        if selected != expected["files"]:
            raise RuntimeError(f"{role}: selected payload differs from reviewed artifact")
        if "file_count" in expected and len(files) != expected["file_count"]:
            raise RuntimeError(f"{role}: scratch image contains unexpected files")
        verify_reachability(role, files, directories, expected.get("reachable_as_worker", {}))
        return {
            "manifest_digest": descriptor["digest"],
            "config_digest": manifest["config"]["digest"],
            "file_count": len(files),
        }


def main(arguments):
    if len(arguments) != 3:
        raise SystemExit("usage: verify-review-images.py CONTROLLER.oci WITNESS.oci WORKER.oci")
    lock = json.loads((HERE / "review-image-lock.json").read_text())
    paths = dict(zip(("controller", "witness", "worker"), map(Path, arguments)))
    result = {role: verify(role, paths[role], lock[role]) for role in paths}
    print(json.dumps({"verified": True, "images": result}, sort_keys=True))


if __name__ == "__main__":
    main(sys.argv[1:])
