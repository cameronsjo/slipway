#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DIGEST = re.compile(r".+@sha256:[0-9a-f]{64}$")


def read_lock(name: str) -> tuple[Path, dict, str]:
    directory = ROOT / name
    lock = json.loads((directory / "image-lock.json").read_text())
    dockerfile = (directory / "Dockerfile").read_text()
    if lock.get("schema") != 1 or lock.get("platform") != "linux/amd64":
        raise SystemExit(f"unsupported {name} image lock")
    return directory, lock, dockerfile


def check_binary(expected: str, path: str | None) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", expected or ""):
        raise SystemExit("image lock has an invalid binary digest")
    if path is None:
        return
    binary = Path(path)
    if not binary.is_file() or not binary.stat().st_mode & stat.S_IXUSR:
        raise SystemExit("locked binary must be an executable regular file")
    if hashlib.sha256(binary.read_bytes()).hexdigest() != expected:
        raise SystemExit("binary does not match image-lock.json")


def check_controller(path: str | None) -> None:
    _, lock, dockerfile = read_lock("controller")
    base = lock.get("ca_base", "")
    if not DIGEST.fullmatch(base) or f"FROM {base} AS certs" not in dockerfile:
        raise SystemExit("controller CA base is not locked")
    if re.findall(r"^FROM\s+(\S+)", dockerfile, re.MULTILINE) != [base, "scratch"]:
        raise SystemExit("controller image has an unexpected base set")
    for marker in (
        "COPY --from=certs /etc/ssl/certs/ca-certificates.crt",
        "COPY --from=controller /runner-pool-linux-amd64 /usr/local/bin/runner-pool",
        "USER 65532:65532",
        'ENTRYPOINT ["/usr/local/bin/runner-pool"]',
    ):
        if marker not in dockerfile:
            raise SystemExit(f"controller image is missing: {marker}")
    check_binary(lock.get("binary_sha256"), path)


def check_witness(path: str | None, host_tool_path: str | None) -> None:
    _, lock, dockerfile = read_lock("host-witness")
    if re.findall(r"^FROM\s+(\S+)", dockerfile, re.MULTILINE) != ["scratch"]:
        raise SystemExit("host witness image must be scratch")
    tools = lock.get("host_tools")
    if not isinstance(tools, dict) or set(tools) != {"crictl", "ctr"}:
        raise SystemExit("host witness image lock must name exactly two inspection tools")
    source = (ROOT.parent / "cmd" / "runner-pool-host-tool" / "main.go").read_text()
    for tool in tools.values():
        if not isinstance(tool, str) or tool not in source:
            raise SystemExit("host witness tool mapping does not match its lock")
    for marker in (
        "COPY --from=witness /runner-pool-host-witness-linux-amd64",
        "COPY --from=host-tool /runner-pool-host-tool-linux-amd64 /usr/local/bin/sjo-host-crictl",
        "COPY --from=host-tool /runner-pool-host-tool-linux-amd64 /usr/local/bin/sjo-host-ctr",
        "USER root",
        'ENTRYPOINT ["/usr/local/bin/runner-pool-host-witness"]',
    ):
        if marker not in dockerfile:
            raise SystemExit(f"host witness image is missing: {marker}")
    if any(term in dockerfile for term in ("APP_PRIVATE_KEY", "APP_ID", "serviceaccount")):
        raise SystemExit("host witness source references a controller credential")
    check_binary(lock.get("binary_sha256"), path)
    check_binary(lock.get("host_tool_sha256"), host_tool_path)


parser = argparse.ArgumentParser()
parser.add_argument("--controller")
parser.add_argument("--witness")
parser.add_argument("--host-tool")
args = parser.parse_args()
check_controller(args.controller)
check_witness(args.witness, args.host_tool)
print("control-plane image locks: source checks passed" if not args.controller and not args.witness and not args.host_tool else "control-plane image locks: source and binary checks passed")
