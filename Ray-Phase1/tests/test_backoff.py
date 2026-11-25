"""Replacement backoff: an attempt that keeps failing is replaced at a growing interval."""

import unittest

from orchestrator import Orchestrator


def spec(replicas: int) -> dict:
    return {
        "job_id": "job",
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": 50}},
        "replicas": replicas,
        "resources": {"cpu": 1},
    }


class Backoff(unittest.TestCase):
    def setUp(self):
        self.now = [0.0]
        self.orch = Orchestrator(
            monotonic=lambda: self.now[0], backoff_base_s=1.0, backoff_max_s=8.0, stable_after_s=10.0
        )

    def at(self, seconds: float) -> None:
        self.now[0] = seconds

    def start_healthy(self, replicas: int) -> list[str]:
        self.orch.submit_job(spec(replicas))
        started = list(self.orch.reconcile("job").start)
        for instance_id in started:
            self.orch.report_healthy(instance_id, pid=1, node_id="n", progress=0)
        return started

    def test_the_first_failure_is_replaced_at_once(self):
        (first,) = self.start_healthy(1)
        self.at(30.0)
        self.orch.report_death(first)
        self.assertEqual(len(self.orch.reconcile("job").start), 1)

    def test_a_replacement_that_fails_without_running_waits_before_the_next(self):
        (first,) = self.start_healthy(1)
        self.at(30.0)
        self.orch.report_death(first)
        (second,) = self.orch.reconcile("job").start
        self.orch.report_death(second)  # crashes on start: second failure in a row

        self.at(30.9)
        self.assertEqual(self.orch.reconcile("job").start, ())
        self.at(31.0)
        self.assertEqual(len(self.orch.reconcile("job").start), 1)

    def test_the_wait_doubles_with_each_failure_up_to_the_maximum(self):
        (attempt,) = self.start_healthy(1)
        self.at(30.0)
        delays = []
        for _ in range(7):
            self.orch.report_death(attempt)
            failed_at = self.now[0]
            while not (started := self.orch.reconcile("job").start):
                self.at(round(self.now[0] + 0.25, 2))
            delays.append(round(self.now[0] - failed_at, 2))
            (attempt,) = started
        self.assertEqual(delays, [0.0, 1.0, 2.0, 4.0, 8.0, 8.0, 8.0])

    def test_a_stable_run_resets_the_chain(self):
        (first,) = self.start_healthy(1)
        self.at(30.0)
        self.orch.report_death(first)
        (second,) = self.orch.reconcile("job").start
        self.orch.report_healthy(second, pid=2, node_id="n", progress=0)
        self.at(41.0)  # RUNNING for 11 s: stable
        self.orch.report_death(second)
        self.assertEqual(len(self.orch.reconcile("job").start), 1)

    def test_independent_failures_are_each_replaced_at_once(self):
        first, second = self.start_healthy(2)
        self.at(30.0)
        self.orch.report_death(first)
        self.orch.report_death(second)  # same moment, e.g. their node was lost
        self.assertEqual(len(self.orch.reconcile("job").start), 2)

    def test_a_waiting_failure_does_not_block_a_scale_up(self):
        (first,) = self.start_healthy(1)
        self.at(30.0)
        self.orch.report_death(first)
        (second,) = self.orch.reconcile("job").start
        self.orch.report_death(second)
        self.orch.set_replicas("job", 2)
        self.assertEqual(len(self.orch.reconcile("job").start), 1)  # only the new replica
        reasons = [e["reason"] for e in self.orch.events() if e["event"] == "instance_requested"]
        self.assertEqual(reasons[-1], "scale_up")

    def test_lowering_the_goal_drops_a_waiting_failure(self):
        (first,) = self.start_healthy(1)
        self.at(30.0)
        self.orch.report_death(first)
        (second,) = self.orch.reconcile("job").start
        self.orch.report_death(second)
        self.orch.stop_job("job")
        self.orch.reconcile("job")
        self.orch.set_replicas("job", 1)
        self.at(30.1)  # before the 1 s backoff would have expired
        self.assertEqual(len(self.orch.reconcile("job").start), 1)

    def test_each_wait_is_recorded(self):
        (first,) = self.start_healthy(1)
        self.at(30.0)
        self.orch.report_death(first)
        (second,) = self.orch.reconcile("job").start
        self.orch.report_death(second)
        backoffs = [e for e in self.orch.events() if e["event"] == "replacement_backoff"]
        self.assertEqual([(e["instance_id"], e["failures_in_row"], e["delay_s"]) for e in backoffs], [(second, 2, 1.0)])

    def test_a_zero_base_disables_the_backoff(self):
        orch = Orchestrator(monotonic=lambda: 0.0, backoff_base_s=0.0)
        orch.submit_job(spec(1))
        (attempt,) = orch.reconcile("job").start
        for _ in range(5):
            orch.report_death(attempt)
            (attempt,) = orch.reconcile("job").start


if __name__ == "__main__":
    unittest.main()
