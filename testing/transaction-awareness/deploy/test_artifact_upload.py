import hashlib
from pathlib import Path
from subprocess import CompletedProcess
import tempfile
import threading
import unittest

from lab import upload_artifacts


class ArtifactUploadTests(unittest.TestCase):
    def test_two_parallel_uploads_verify_each_file_before_atomic_rename(self):
        with tempfile.TemporaryDirectory(prefix="gateway-artifact-test-") as directory:
            jar = Path(directory) / "test.jar"
            jar.write_bytes(b"test artifact bytes")
            expected = hashlib.sha256(jar.read_bytes()).hexdigest()
            lock = threading.Lock()
            barrier = threading.Barrier(2)
            active = 0
            peak = 0
            events = {}

            def kubectl(*parts, **options):
                nonlocal active, peak
                if parts[0] == "cp":
                    pod = parts[2].split(":")[0]
                    with lock:
                        active += 1
                        peak = max(peak, active)
                        events[pod] = ["copy"]
                    barrier.wait(timeout=5)
                    with lock:
                        active -= 1
                    self.assertEqual(options["timeout"], 300)
                else:
                    pod = parts[1]
                    if parts[5] == "sha256sum":
                        events[pod].append("verify")
                        self.assertTrue(options["capture"])
                        return CompletedProcess([], 0, expected + "  /artifact/upload.jar\n")
                    self.assertEqual(parts[5:], ("mv", "/artifact/upload.jar", "/artifact/launch.jar"))
                    events[pod].append("rename")
                return CompletedProcess([], 0, "")

            pods = ["gateway-test-" + str(index) for index in range(6)]
            upload_artifacts(kubectl, pods, jar)
            self.assertEqual(peak, 2)
            self.assertEqual(events, {pod: ["copy", "verify", "rename"] for pod in pods})

    def test_checksum_failure_never_renames_that_upload(self):
        with tempfile.TemporaryDirectory(prefix="gateway-artifact-test-") as directory:
            jar = Path(directory) / "test.jar"
            jar.write_bytes(b"test artifact bytes")
            calls = []

            def kubectl(*parts, **options):
                calls.append(parts)
                return CompletedProcess([], 0, "incorrect checksum")

            with self.assertRaises(RuntimeError):
                upload_artifacts(kubectl, ["gateway-test"], jar)
            self.assertEqual(len(calls), 2)
            self.assertNotIn("mv", [item for call in calls for item in call])

    def test_slow_upload_timeout_is_explicit_and_bounded(self):
        with tempfile.TemporaryDirectory(prefix="gateway-artifact-test-") as directory:
            jar = Path(directory) / "test.jar"
            jar.write_bytes(b"test artifact bytes")
            expected = hashlib.sha256(jar.read_bytes()).hexdigest()
            timeouts = []

            def kubectl(*parts, **options):
                if parts[0] == "cp":
                    timeouts.append(options["timeout"])
                return CompletedProcess([], 0, expected)

            upload_artifacts(kubectl, ["gateway-test"], jar, timeout=900)
            self.assertEqual(timeouts, [900])
            for timeout in [0, 901, True]:
                with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                    upload_artifacts(kubectl, ["gateway-test"], jar, timeout=timeout)


if __name__ == "__main__":
    unittest.main()
