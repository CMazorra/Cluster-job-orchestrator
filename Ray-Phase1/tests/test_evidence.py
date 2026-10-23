"""Attempt identity, annotations and fault timelines in the session history."""

import itertools
import unittest

from orchestrator import Orchestrator
from orchestrator.evidence import fault_timeline


def spec(replicas: int) -> dict:
    return {
        "job_id": "job",
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": 50}},
        "replicas": replicas,
        "resources": {"cpu": 1},
    }


def healthy(orch: Orchestrator, *instance_ids: str) -> None:
    for instance_id in instance_ids:
        orch.report_healthy(instance_id, pid=1, node_id="n", progress=0)


def requested(orch: Orchestrator) -> list[tuple]:
    return [
        (e["instance_id"], e["reason"], e.get("replaces"))
        for e in orch.events()
        if e["event"] == "instance_requested"
    ]


class AttemptIdentity(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator(max_missed_probes=2)
        self.orch.submit_job(spec(2))

    def test_first_attempts_are_initial_and_later_ones_scale_up(self):
        healthy(self.orch, *self.orch.reconcile("job").start)
        self.orch.set_replicas("job", 3)
        self.orch.reconcile("job")
        self.assertEqual(
            requested(self.orch),
            [("job/1", "initial", None), ("job/2", "initial", None), ("job/3", "scale_up", None)],
        )

    def test_a_failed_attempt_is_replaced_by_name(self):
        healthy(self.orch, *self.orch.reconcile("job").start)
        self.orch.report_death("job/1")
        self.orch.reconcile("job")
        self.assertEqual(requested(self.orch)[-1], ("job/3", "replacement", "job/1"))

    def test_an_attempt_given_up_after_missed_probes_is_replaced_by_name(self):
        healthy(self.orch, *self.orch.reconcile("job").start)
        for _ in range(2):
            self.orch.report_unresponsive("job/2")
        self.orch.reconcile("job")
        self.assertEqual(requested(self.orch)[-1], ("job/3", "replacement", "job/2"))

    def test_a_failure_no_longer_needed_is_not_replaced_later(self):
        healthy(self.orch, *self.orch.reconcile("job").start)
        self.orch.report_death("job/1")
        self.orch.set_replicas("job", 1)
        self.orch.reconcile("job")  # one attempt still covers the goal
        self.orch.set_replicas("job", 2)
        self.orch.reconcile("job")
        self.assertEqual(requested(self.orch)[-1], ("job/3", "scale_up", None))

    def test_a_stopped_attempt_is_not_a_failure_to_replace(self):
        healthy(self.orch, *self.orch.reconcile("job").start)
        self.orch.set_replicas("job", 1)
        (stopped,) = self.orch.reconcile("job").kill
        self.orch.report_death(stopped)
        self.orch.set_replicas("job", 2)
        self.orch.reconcile("job")
        self.assertEqual(requested(self.orch)[-1], ("job/3", "scale_up", None))


class Annotations(unittest.TestCase):
    def test_an_annotation_changes_no_state(self):
        orch = Orchestrator()
        orch.submit_job(spec(1))
        healthy(orch, *orch.reconcile("job").start)
        before = orch.get_job("job")["instances"]
        orch.annotate("fault_injected", instance_id="job/1", method="crash")
        self.assertEqual(orch.get_job("job")["instances"], before)
        self.assertEqual(orch.events()[-1]["kind"], "fault_injected")

    def test_every_event_carries_a_monotonic_stamp(self):
        ticks = itertools.count(10)
        orch = Orchestrator(monotonic=lambda: float(next(ticks)))
        orch.submit_job(spec(1))
        orch.reconcile("job")
        self.assertEqual([e["m"] for e in orch.events()], [10.0, 11.0])


class FaultTimeline(unittest.TestCase):
    def test_durations_come_from_the_monotonic_stamps(self):
        now = [0.0]
        orch = Orchestrator(monotonic=lambda: now[0])
        orch.submit_job(spec(2))
        healthy(orch, *orch.reconcile("job").start)

        now[0] = 10.0
        orch.annotate("fault_injected", instance_id="job/1", method="crash")
        now[0] = 10.5
        orch.report_death("job/1")
        (replacement,) = orch.reconcile("job").start
        now[0] = 12.0
        healthy(orch, replacement)

        timeline = fault_timeline(orch.events(), "job/1")
        self.assertEqual(timeline["replacement_id"], replacement)
        self.assertEqual(
            (timeline["detection_s"], timeline["replacement_request_s"], timeline["recovery_s"], timeline["total_s"]),
            (0.5, 0.0, 1.5, 2.0),
        )

    def test_a_hung_attempt_is_suspected_first_and_failed_by_a_decision(self):
        now = [0.0]
        orch = Orchestrator(max_missed_probes=3, monotonic=lambda: now[0])
        orch.submit_job(spec(1))
        healthy(orch, *orch.reconcile("job").start)

        now[0] = 10.0
        orch.annotate("fault_injected", instance_id="job/1", method="hang")
        for second in (12.0, 14.5, 17.0):
            now[0] = second
            orch.report_unresponsive("job/1")
        now[0] = 17.5
        (replacement,) = orch.reconcile("job").start
        now[0] = 18.0
        healthy(orch, replacement)

        timeline = fault_timeline(orch.events(), "job/1")
        self.assertEqual(timeline["failed_by"], "instance_decision")
        self.assertEqual(
            (timeline["suspicion_s"], timeline["detection_s"], timeline["recovery_s"], timeline["total_s"]),
            (2.0, 7.5, 0.5, 8.0),
        )

    def test_a_crash_is_failed_by_an_observation_without_suspicion(self):
        orch = Orchestrator()
        orch.submit_job(spec(1))
        healthy(orch, *orch.reconcile("job").start)
        orch.annotate("fault_injected", instance_id="job/1", method="crash")
        orch.report_death("job/1")
        timeline = fault_timeline(orch.events(), "job/1")
        self.assertEqual(timeline["failed_by"], "instance_observed")
        self.assertIsNone(timeline["suspicion_s"])

    def test_steps_that_did_not_happen_are_none(self):
        orch = Orchestrator()
        orch.submit_job(spec(1))
        healthy(orch, *orch.reconcile("job").start)
        orch.annotate("fault_injected", instance_id="job/1", method="crash")
        timeline = fault_timeline(orch.events(), "job/1")
        self.assertIsNone(timeline["replacement_id"])
        self.assertIsNone(timeline["detection_s"])
        self.assertIsNone(timeline["total_s"])


if __name__ == "__main__":
    unittest.main()
