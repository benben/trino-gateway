"""Check bounded process coordination without backend traffic."""

from datetime import datetime, timezone
import json
import functools
import io
import multiprocessing
import os
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from load_open_loop import OpenLoop


class TerminalTransport:
    def request(self, *args):
        return 200, b'{"data":[["blue"]]}'

    def close(self):
        pass


class BarrierTransport(TerminalTransport):
    def __init__(self, released):
        self.released = released

    def request(self, index, method, *args):
        if method == "POST":
            return 200, b'{"nextUri":"/result"}'
        if not self.released.is_set():
            raise AssertionError("Cleanup started before the parent completion snapshot")
        return super().request(index, method, *args)


class ReadyTransport(TerminalTransport):
    def __init__(self, ready):
        self.ready = ready

    def request(self, *args):
        if self.ready.value != 8:
            raise AssertionError("HTTP started before every child was ready")
        return super().request(*args)


def delayed_ready_worker(connection, index, configuration, factory):
    from load_multiprocess import ChildPhase, worker
    original = ChildPhase.start

    def start(phase, elapsed):
        if index == 7:
            time.sleep(.1)
        ready = factory.args[0]
        with ready.get_lock():
            ready.value += 1
        return original(phase, elapsed)

    ChildPhase.start = start
    worker(connection, index, configuration, factory)


def partial_frame(connection):
    os.write(connection.fileno(), struct.pack("!i", 100) + b"{")
    time.sleep(10)


def missing_ready_worker(connection, index, configuration, factory):
    partial_frame(connection)


def cleanup_failure(connection, index, configuration, factory):
    from load_multiprocess import ChildPhase, send
    phase = ChildPhase(connection, index, configuration["rate"])
    start, _, _ = phase.start(0)
    time.sleep(max(0, start - time.monotonic()))
    phase.complete(.01, .001)
    send(connection, {"kind": "error", "process_index": index, "error_type": "SyntheticCleanupFailure"})
    connection.close()


class MultiprocessTests(unittest.TestCase):
    def test_explicit_process_option_defaults_to_one(self):
        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100,
                        duration=.1, concurrency=8, transport=TerminalTransport(), processes=1)
        self.assertEqual(loop.processes, 1)

    def test_eight_process_profile_rejects_unbounded_or_indivisible_capacity(self):
        for count, rate, concurrency in [(2, 100, 128), (8, 100, 127), (8, 1, 128), (8, 1000, 513)]:
            with self.subTest(count=count, rate=rate), self.assertRaises(ValueError):
                OpenLoop(["https://gateway"], ["group"], "runtime", rate=rate,
                         duration=.1, concurrency=concurrency, processes=count)
        for options in ({"duration": .001}, {"concurrency": 128.0}, {"maximum_lag": float("nan")},
                        {"duration": float("nan")}, {"timeout": float("inf")}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                settings = dict(rate=100, duration=.1, concurrency=128, processes=8)
                settings.update(options)
                OpenLoop(["https://gateway"], ["group"], "runtime", **settings)

    def test_cli_eight_process_failed_warmup_does_not_start_measurement(self):
        import load_open_loop
        warmup = {"errors": {"synthetic": 1}, "client_bottleneck_detected": False,
                  "unconsumed_continuations": 0}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            arguments = ["load_open_loop.py", "--rate", "100", "--processes", "8", "--output", str(output)]
            with patch("sys.argv", arguments), patch.dict(os.environ, {"TX_ALLOW_FIXTURE_MUTATION": "yes"}), \
                    patch("load_open_loop.OpenLoop") as runner, patch("sys.stdout", new_callable=io.StringIO):
                runner.return_value.run.return_value = warmup
                self.assertEqual(load_open_loop.main(), 1)
            self.assertEqual(runner.call_count, 1)
            receipt = json.loads(output.read_text())
            self.assertTrue(receipt["measurement_not_started"])
            self.assertIsNone(receipt["successful_http_rps"])
            self.assertEqual(receipt["warmup"], warmup)

    def test_delayed_eighth_worker_cannot_allow_early_http(self):
        from load_multiprocess import run_multiprocess
        ready = multiprocessing.get_context("spawn").Value("i", 0)
        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100,
                        duration=.08, concurrency=8, processes=8)
        result = run_multiprocess(loop, worker_target=delayed_ready_worker,
                                  transport_factory=functools.partial(ReadyTransport, ready))
        self.assertFalse(result["errors"], result)
        self.assertEqual(result["counts"]["completed"], 8)

    def test_watchdog_expiry_during_aggregation_cannot_return_valid_receipt(self):
        import load_aggregate
        from load_multiprocess import run_multiprocess, Supervision
        original, supervisors = Supervision.__init__, []

        def initialize(supervisor, timeout):
            original(supervisor, timeout)
            supervisors.append(supervisor)

        def aggregate(*args, **kwargs):
            supervisors[0].expired.set()
            return {"errors": {}, "client_bottleneck_detected": False}

        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100,
                        duration=.08, concurrency=8, processes=8)
        with patch.object(Supervision, "__init__", initialize), patch.object(load_aggregate, "aggregate_reports", aggregate):
            result = run_multiprocess(loop, transport_factory=TerminalTransport)
        self.assertTrue(result["errors"])
        self.assertIsNone(result["successful_http_rps"])
        self.assertTrue(supervisors[0].clean)

    def test_shared_phase_uses_global_ordinals_and_common_cutoff(self):
        from load_multiprocess import ordinals
        partitions = [list(ordinals(index, 100, .13)) for index in range(8)]
        self.assertEqual(sorted(item for part in partitions for item in part), list(range(13)))
        self.assertEqual(partitions[7], [7])
        self.assertEqual(partitions[0], [0, 8])

    def test_all_eight_finish_before_any_cleanup_and_raw_arrays_are_not_exported(self):
        from load_multiprocess import run_multiprocess
        from load_open_loop import read_client_cpu
        released = multiprocessing.get_context("spawn").Event()
        snapshots = []

        def cpu():
            snapshots.append(read_client_cpu())
            if len(snapshots) == 3:
                released.set()
            return snapshots[-1]

        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100,
                        duration=.08, concurrency=8, processes=8)
        with patch("load_open_loop.read_client_cpu", side_effect=cpu):
            result = run_multiprocess(loop, transport_factory=functools.partial(BarrierTransport, released))
        self.assertNotIn("multiprocess_coordination_failure", result["errors"], result)
        self.assertFalse(result["errors"])
        self.assertEqual(result["counts"]["started"], 8)
        self.assertEqual(result["counts"]["cleanup_completed"], 8)
        self.assertEqual(len(result["child_measurements"]), 8)
        self.assertEqual(len(snapshots), 3)
        self.assertNotIn("raw_samples", json.dumps(result))

    def test_partial_frame_cannot_block_phase_deadline_or_leave_child(self):
        from load_multiprocess import Supervision
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=partial_frame, args=(child,))
        process.start()
        child.close()
        started = time.monotonic()
        supervisor = Supervision(2)
        supervisor.attach(process, parent)
        try:
            with self.assertRaises(TimeoutError):
                supervisor.collect("ready", time.monotonic() + .3)
        finally:
            supervisor.close()
        self.assertFalse(process.is_alive())
        self.assertTrue(all(not reader.is_alive() for reader in supervisor.readers))
        self.assertLess(time.monotonic() - started, 2)

    def test_watchdog_stops_partial_sender_without_parent_polling(self):
        from load_multiprocess import Supervision
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=partial_frame, args=(child,))
        process.start()
        child.close()
        supervisor = Supervision(.2)
        supervisor.attach(process, parent)
        time.sleep(.4)
        try:
            self.assertTrue(supervisor.expired.is_set())
            process.join(timeout=.5)
            self.assertFalse(process.is_alive())
        finally:
            supervisor.close()

    def test_missing_ready_fails_with_one_shared_setup_budget(self):
        from load_multiprocess import run_multiprocess
        before = {child.pid for child in multiprocessing.active_children()}
        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100,
                        duration=.08, concurrency=8, processes=8)
        started = time.monotonic()
        result = run_multiprocess(loop, worker_target=missing_ready_worker, setup_timeout=.2)
        self.assertTrue(result["errors"])
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, before)

    def test_spawn_from_runpy_rejects_missed_start_without_backend_dispatch(self):
        source = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "receipt.json"
            launch = "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_path(sys.argv.pop(1),run_name='__main__')"
            command = [sys.executable, "-c", launch, str(source), str(source / "load_open_loop.py"),
                       "--rate", "100", "--duration", ".08", "--warmup", "0", "--concurrency", "8",
                       "--processes", "8", "--measurement-start-utc", "2000-01-01T00:00:00Z", "--output", str(output)]
            environment = dict(os.environ, TX_ALLOW_FIXTURE_MUTATION="yes", TX_GATEWAY_URLS="https://unused.invalid",
                               TX_LOAD_ROUTING_GROUPS="fixture", TX_QUERY_AUTHORIZATION="runtime")
            environment.pop("TX_CA_FILE", None)
            result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1, result.stderr)
            receipt = json.loads(output.read_text())
            self.assertEqual(receipt["failure_type"], "ValueError")
            self.assertEqual(receipt["completed_child_receipts"], [])
            self.assertNotIn("raw_samples", result.stdout)

    def test_child_cleanup_failure_invalidates_unknown_throughput(self):
        from load_multiprocess import run_multiprocess
        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100,
                        duration=.08, concurrency=8, processes=8)
        result = run_multiprocess(loop, worker_target=cleanup_failure)
        self.assertTrue(result["errors"])
        self.assertIsNone(result["successful_http_rps"])
        self.assertTrue(result["uncertain_backend_work"])
        self.assertTrue(result["no_automatic_retry"])

    def test_each_child_rotates_groups_and_all_gateway_indices_without_synthetic_counts(self):
        class RecordingTransport(TerminalTransport):
            def __init__(self):
                self.requests = []

            def request(self, index, method, path, body, headers):
                self.requests.append((index, headers["X-Trino-Routing-Group"]))
                return super().request(index, method, path, body, headers)

        for rate in (100, 1000):
            total = 0
            for index in range(8):
                transport = RecordingTransport()
                loop = OpenLoop(["https://gateway"] * 100, ["one", "two"], "runtime", rate=rate / 8,
                                duration=8, concurrency=1, transport=transport)
                loop.rotation_offset = index
                loop.window_beginning, loop.window_end = time.monotonic(), time.monotonic() + 8
                for sequence in range(int(rate * 8 / 8)):
                    loop.perform(sequence, time.monotonic(), loop.window_end)
                self.assertEqual(set(row[0] for row in transport.requests), set(range(100)))
                self.assertEqual(set(row[1] for row in transport.requests), {"one", "two"})
                self.assertEqual(transport.requests[0], (index, ["one", "two"][index % 2]))
                total += loop.counts["started"]
            self.assertEqual(total, rate * 8)

    def test_child_waits_for_completion_barrier_before_cleanup(self):
        class ContinuationTransport(TerminalTransport):
            def request(self, index, method, *args):
                if method == "POST":
                    return 200, b'{"nextUri":"/result"}'
                self_test.assertTrue(phase.released)
                return super().request(index, method, *args)

        class Phase:
            process_index = 0
            global_rate = 100
            released = False

            def start(self, elapsed):
                return time.monotonic(), datetime.now(timezone.utc), None

            def complete(self, elapsed, cpu):
                self_test.assertFalse(self.released)
                self.released = True

        self_test, phase = self, Phase()
        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=12.5,
                        duration=.01, concurrency=1, transport=ContinuationTransport())
        with patch("load_open_loop.read_client_cpu") as read_cpu:
            result = loop.run(_phase=phase)
        read_cpu.assert_not_called()
        self.assertEqual(result["final_inflight"], 0)
        self.assertEqual(result["scheduled_http_requests"], 1)
        self.assertEqual(result["counts"]["cleanup_completed"], 1)
        self.assertFalse(result["errors"])


if __name__ == "__main__":
    unittest.main()
