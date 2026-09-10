"""Check bounded client preparation without backend traffic."""

from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch

import load_open_loop
import test_load_open_loop as fixtures
from test_load_open_loop import Transport


class ExecutorSetupTests(unittest.TestCase):
    def test_all_workers_start_before_the_first_backend_request(self):
        original_start = threading.Thread.start
        started = []

        def slow_start(thread):
            time.sleep(.06)
            original_start(thread)
            started.append(thread)

        class CheckedTransport(Transport):
            def request(self, *args):
                self_test.assertEqual(len(started), 4)
                return super().request(*args)

        self_test = self
        with patch("threading.Thread.start", slow_start):
            result = fixtures.OpenLoopTests().run_load(CheckedTransport(), concurrency=4, duration=.2, rate=20)
        self.assertEqual(result["counts"]["started"], 4)
        self.assertEqual(result["client_dropped_requests"], 0)
        self.assertFalse(result["errors"])
        self.assertTrue(all(not thread.is_alive() for thread in started))

    def test_preparation_itself_does_not_dispatch_http(self):
        with ThreadPoolExecutor(max_workers=4) as executor, patch.object(Transport, "request") as request:
            load_open_loop.prepare_workers(executor, 4, timeout=1)
            self.assertEqual(len(executor._threads), 4)
            request.assert_not_called()

    def test_expired_setup_releases_every_worker_barrier(self):
        class SlowSubmit(ThreadPoolExecutor):
            def submit(self, *args):
                future = super().submit(*args)
                time.sleep(.04)
                return future

        started = time.monotonic()
        with SlowSubmit(max_workers=4) as executor:
            with self.assertRaises(TimeoutError):
                load_open_loop.prepare_workers(executor, 4, timeout=.01)
        self.assertLess(time.monotonic() - started, 1)
        self.assertTrue(all(not thread.is_alive() for thread in executor._threads))

    def test_submit_failure_releases_already_waiting_workers(self):
        class FailedSubmit(ThreadPoolExecutor):
            submitted = 0

            def submit(self, *args):
                self.submitted += 1
                if self.submitted == 2:
                    raise RuntimeError("Synthetic thread allocation failure")
                return super().submit(*args)

        with FailedSubmit(max_workers=4) as executor:
            with self.assertRaisesRegex(RuntimeError, "allocation"):
                load_open_loop.prepare_workers(executor, 4, timeout=1)
        self.assertTrue(all(not thread.is_alive() for thread in executor._threads))


if __name__ == "__main__":
    unittest.main()
