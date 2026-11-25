import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


class ControlPlaneImageTest(unittest.TestCase):
    def run_check(self, root, *args):
        return subprocess.run(
            ["python3", str(Path(root) / "check-control-plane.py"), *args],
            cwd=root,
            text=True,
            capture_output=True,
        )

    def copy_bundle(self):
        temporary = Path(tempfile.mkdtemp(prefix="sjo-control-plane-image-test-"))
        image = temporary / "image"
        image.mkdir()
        shutil.copy2(ROOT / "check-control-plane.py", image)
        shutil.copytree(ROOT / "controller", image / "controller")
        shutil.copytree(ROOT / "host-witness", image / "host-witness")
        tool = temporary / "cmd" / "runner-pool-host-tool"
        tool.mkdir(parents=True)
        shutil.copy2(ROOT.parent / "cmd" / "runner-pool-host-tool" / "main.go", tool)
        return image

    def test_source_locks_pass_without_network(self):
        result = self.run_check(ROOT)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mutable_controller_base_is_rejected(self):
        temporary = self.copy_bundle()
        try:
            dockerfile = temporary / "controller" / "Dockerfile"
            dockerfile.write_text(dockerfile.read_text().replace(
                "myoung34/github-runner@sha256:590cdc6864129ed5a7799f86fba536f29690b9f2ae988ef8a6b7d8da7ae4596d",
                "myoung34/github-runner:latest",
            ))
            self.assertNotEqual(self.run_check(temporary).returncode, 0)
        finally:
            shutil.rmtree(temporary.parent)

    def test_changed_host_tool_path_is_rejected(self):
        temporary = self.copy_bundle()
        try:
            source = temporary.parent / "cmd" / "runner-pool-host-tool" / "main.go"
            # This mutated the lvs path until lvs was retired from the launcher
            # (a root host timer publishes the thin-pool report now, and the
            # witness reads a file). The string was simply absent afterwards, so
            # the "mutation" changed nothing and the check passed -- a test that
            # could no longer fail, quietly. Mutate a path the launcher still has.
            original = "/var/lib/rancher/k3s/data/current/bin/ctr"
            self.assertIn(original, source.read_text(), "the launcher no longer maps ctr; this mutation reaches nothing")
            source.write_text(source.read_text().replace(original, "/usr/local/bin/ctr"))
            result = self.run_check(temporary)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("tool mapping", result.stderr)
        finally:
            shutil.rmtree(temporary.parent)

    def test_changed_binary_is_rejected(self):
        temporary = self.copy_bundle()
        try:
            binary = temporary / "controller-binary"
            binary.write_bytes(b"changed controller")
            binary.chmod(0o755)
            lock_path = temporary / "controller" / "image-lock.json"
            lock = json.loads(lock_path.read_text())
            lock["binary_sha256"] = "0" * 64
            lock_path.write_text(json.dumps(lock, indent=2) + "\n")
            result = self.run_check(temporary, "--controller", str(binary))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("does not match", result.stderr)
        finally:
            shutil.rmtree(temporary.parent)


if __name__ == "__main__":
    unittest.main()
