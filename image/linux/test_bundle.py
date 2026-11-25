import hashlib
import json
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


class LinuxWorkerImageBundleTest(unittest.TestCase):
    def run_check(self, directory, *args):
        return subprocess.run(
            [str(Path(directory) / "check-lock.sh"), *args],
            cwd=directory,
            text=True,
            capture_output=True,
        )

    def copy_bundle(self):
        destination = Path(tempfile.mkdtemp(prefix="slipway-image-test-"))
        for name in ("Dockerfile", "entrypoint.sh", "bootstrap-ca.pem", "image-lock.json", "check-lock.sh"):
            shutil.copy2(ROOT / name, destination / name)
        (destination / "check-lock.sh").chmod(0o755)
        (destination / "entrypoint.sh").chmod(0o755)
        return destination

    def test_source_lock_and_entrypoint_pass_without_network_or_worker(self):
        result = self.run_check(ROOT, "--source-only")
        self.assertEqual(result.returncode, 0, result.stderr)
        subprocess.run(["bash", "-n", str(ROOT / "entrypoint.sh")], check=True)

    def test_entrypoint_handshake_precedes_worker_exec(self):
        temporary = Path(tempfile.mkdtemp(prefix="slipway-entrypoint-test-"))
        try:
            fake_bin = temporary / "bin"
            fake_bin.mkdir()
            fake_id = fake_bin / "id"
            fake_id.write_text("#!/bin/sh\n[ \"$1\" = -u ] && echo 1001 || echo 121\n")
            fake_id.chmod(0o755)
            worker = temporary / "worker"
            worker.write_text(
                "#!/bin/sh\n"
                "[ \"${SLIPWAY_BOOTSTRAP_TOKEN:-}\" = \"$EXPECTED_TOKEN\" ]\n"
                "[ -z \"${SLIPWAY_BOOTSTRAP_EXPECTED_NAME:-}\" ]\n"
                "[ -z \"${SLIPWAY_BOOTSTRAP_EXPECTED_UID:-}\" ]\n"
                "printf 'WORKER %s\\n' \"$1\"\n"
            )
            worker.chmod(0o755)
            token = "A" * 43
            environment = {
                "PATH": str(fake_bin) + ":/usr/bin:/bin",
                "SLIPWAY_BOOTSTRAP_EXPECTED_NAME": "worker-1",
                "SLIPWAY_BOOTSTRAP_EXPECTED_UID": "pod-uid-1",
                "EXPECTED_TOKEN": token,
            }
            result = subprocess.run(
                [str(ROOT / "entrypoint.sh"), str(worker), "started"],
                # THE PROMPT COMES FIRST. Feeding only CAP would now be
                # refused: the worker no longer announces itself unasked.
                input=f"HELLO?\nCAP {token}\n",
                text=True,
                capture_output=True,
                env=environment,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stdout.splitlines(),
                ["HELLO worker-1 pod-uid-1", "ACK worker-1 pod-uid-1", "WORKER started"],
            )
        finally:
            shutil.rmtree(temporary)

    def test_entrypoint_rejects_non_capability_frame(self):
        token = "A" * 43
        with tempfile.TemporaryDirectory(prefix="slipway-entrypoint-test-") as directory:
            fake_bin = Path(directory)
            fake_id = fake_bin / "id"
            fake_id.write_text("#!/bin/sh\n[ \"$1\" = -u ] && echo 1001 || echo 121\n")
            fake_id.chmod(0o755)
            result = subprocess.run(
                [str(ROOT / "entrypoint.sh"), "/bin/true"],
                input=f"HELLO?\nTOKEN {token}\n",
                text=True,
                capture_output=True,
                env={
                    "PATH": str(fake_bin) + ":/usr/bin:/bin",
                    "SLIPWAY_BOOTSTRAP_EXPECTED_NAME": "worker-1",
                    "SLIPWAY_BOOTSTRAP_EXPECTED_UID": "pod-uid-1",
                },
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("ACK", result.stdout)

    def test_unpinned_base_is_rejected(self):
        temporary = self.copy_bundle()
        try:
            dockerfile = (temporary / "Dockerfile").read_text()
            dockerfile = dockerfile.replace(
                "docker@sha256:7bf70dcb85c822e3fa6f2d332143e474b99147b9d7b39102b0cfe8b04a7dd61b",
                "docker:latest",
            )
            (temporary / "Dockerfile").write_text(dockerfile)
            result = self.run_check(temporary, "--source-only")
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue("digest" in result.stderr or "locked" in result.stderr, result.stderr)
        finally:
            shutil.rmtree(temporary)

    def test_worker_digest_is_checked_offline(self):
        temporary = self.copy_bundle()
        try:
            worker = temporary / "runner-pool-worker-linux-amd64"
            worker.write_bytes(b"offline-worker-fixture")
            worker.chmod(worker.stat().st_mode | stat.S_IXUSR)
            lock_path = temporary / "image-lock.json"
            lock = json.loads(lock_path.read_text())
            lock["worker_sha256"] = hashlib.sha256(worker.read_bytes()).hexdigest()
            lock_path.write_text(json.dumps(lock, indent=2) + "\n")
            result = self.run_check(temporary, "--worker", str(worker))
            self.assertEqual(result.returncode, 0, result.stderr)
            worker.write_bytes(b"tampered-worker-fixture")
            result = self.run_check(temporary, "--worker", str(worker))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("does not match", result.stderr)
        finally:
            shutil.rmtree(temporary)

    def test_changed_bootstrap_ca_is_rejected(self):
        temporary = self.copy_bundle()
        try:
            (temporary / "bootstrap-ca.pem").write_text("changed CA\n")
            result = self.run_check(temporary, "--source-only")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("bootstrap CA", result.stderr)
        finally:
            shutil.rmtree(temporary)

    def test_unreadable_bootstrap_ca_is_rejected(self):
        temporary = self.copy_bundle()
        try:
            dockerfile = temporary / "Dockerfile"
            dockerfile.write_text(dockerfile.read_text().replace("COPY --chmod=0444 bootstrap-ca.pem", "COPY bootstrap-ca.pem"))
            result = self.run_check(temporary, "--source-only")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("readably", result.stderr)
        finally:
            shutil.rmtree(temporary)




class EntrypointSpeaksOnlyWhenAskedTest(unittest.TestCase):
    """The worker must not announce itself before it is asked.

    A Kubernetes attach does not replay output emitted before the attach, and
    the controller attaches about ten seconds after the Pod is Ready. The old
    entrypoint printed its identity at startup, so the line landed in the
    container log and never on the attach stream -- and every worker creation
    failed waiting for it. Proven on the cluster with a throwaway Kata pod.
    """

    def run_entrypoint(self, stdin, argv=("/bin/true",)):
        directory = Path(tempfile.mkdtemp(prefix="sjo-runner-entrypoint-ask-"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        fake_id = directory / "id"
        fake_id.write_text("#!/bin/sh\n[ \"$1\" = -u ] && echo 1001 || echo 121\n")
        fake_id.chmod(0o755)
        return subprocess.run(
            [str(ROOT / "entrypoint.sh"), *argv],
            input=stdin, text=True, capture_output=True, timeout=30,
            env={
                "PATH": str(directory) + ":/usr/bin:/bin",
                "SLIPWAY_BOOTSTRAP_EXPECTED_NAME": "worker-1",
                "SLIPWAY_BOOTSTRAP_EXPECTED_UID": "pod-uid-1",
            },
        )

    def test_no_identity_is_printed_before_the_prompt(self):
        """The regression itself. stdin closes immediately, so the entrypoint
        gets EOF on its first read -- and must have said nothing by then."""
        result = self.run_entrypoint("")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "", "the worker announced itself before being asked")
        self.assertIn("stdin closed", result.stderr)

    def test_a_wrong_prompt_gets_no_identity_and_no_echo(self):
        """Two claims, and the second is the one worth stating: a worker that
        echoed what arrived would turn its own attach stream into a reflector."""
        result = self.run_entrypoint("WHO ARE YOU\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("HELLO", result.stdout)
        self.assertNotIn("WHO ARE YOU", result.stdout + result.stderr)

    def test_an_oversized_prompt_is_refused(self):
        result = self.run_entrypoint("H" * 300 + "\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("HELLO", result.stdout)

    def test_the_identity_follows_the_prompt_and_nothing_else_does(self):
        """The positive control. Without it every assertion above is satisfied
        by an entrypoint that refuses everything."""
        result = self.run_entrypoint("HELLO?\n")
        # Refused at the capability step, having answered the prompt exactly once.
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.splitlines(), ["HELLO worker-1 pod-uid-1"])
        self.assertIn("stdin closed before the capability", result.stderr)


if __name__ == "__main__":
    unittest.main()
