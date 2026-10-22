"""Executable examples of the external behavior, for the Ray-independent domain part.

Run from the Ray-Phase1 folder with:
    python -m unittest discover -s tests -v
"""

import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from orchestrator import ErrorCode, InstanceState, Orchestrator, OrchestratorError, ReconcileActions

FIXED_NOW = datetime(2026, 10, 18, 12, 0, 0, tzinfo=timezone.utc)

SPEC = {
    "job_id": "demo-001",
    "workload": {"entrypoint": "ticker", "args": {"interval_ms": 250}},
    "replicas": 2,
    "resources": {"cpu": 1},
    "placement": {"node_label": "zone-a"},
}


def spec_with(**changes):
    spec = copy.deepcopy(SPEC)
    spec.update(changes)
    return spec


def run_actions(orch, actions, node_id="node-A"):
    """Fake runtime: every new attempt starts and answers, every kill succeeds."""
    for instance_id in actions.start:
        orch.report_healthy(instance_id, pid=1000 + int(instance_id.split("/")[1]), node_id=node_id, progress=0)
    for instance_id in actions.kill:
        orch.report_death(instance_id)


class SubmitAndQuery(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator(clock=lambda: FIXED_NOW)

    def test_submit_returns_job_id_and_query_shows_goal_without_instances(self):
        self.assertEqual(self.orch.submit_job(SPEC), "demo-001")
        self.assertEqual(
            self.orch.get_job("demo-001"),
            {
                "job_id": "demo-001",
                "desired_replicas": 2,
                "observed": {"running": 0, "pending": 0, "unknown": 0, "stopping": 0},
                "instances": [],
                "observed_at": "2026-10-18T12:00:00.000Z",
            },
        )

    def test_placement_is_optional(self):
        spec = copy.deepcopy(SPEC)
        del spec["placement"]
        self.assertEqual(self.orch.submit_job(spec), "demo-001")

    def test_zero_replicas_is_a_valid_goal(self):
        self.orch.submit_job(spec_with(replicas=0))
        self.assertEqual(self.orch.get_job("demo-001")["desired_replicas"], 0)

    def test_fractional_cpu_is_accepted(self):
        self.orch.submit_job(spec_with(resources={"cpu": 0.5}))

    def test_unknown_job_is_job_not_found(self):
        with self.assertRaises(OrchestratorError) as ctx:
            self.orch.get_job("missing")
        self.assertEqual(ctx.exception.code, ErrorCode.JOB_NOT_FOUND)


class RepeatedSubmit(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator()
        self.orch.submit_job(SPEC)

    def test_same_id_and_same_spec_returns_same_job(self):
        # Lost-response case: the client retries with the same spec.
        self.assertEqual(self.orch.submit_job(copy.deepcopy(SPEC)), "demo-001")
        submitted = [e for e in self.orch.events() if e["event"] == "job_submitted"]
        self.assertEqual(len(submitted), 1)

    def test_same_id_with_different_spec_is_rejected(self):
        with self.assertRaises(OrchestratorError) as ctx:
            self.orch.submit_job(spec_with(replicas=3))
        self.assertEqual(ctx.exception.code, ErrorCode.JOB_ALREADY_EXISTS)
        self.assertEqual(self.orch.get_job("demo-001")["desired_replicas"], 2)

    def test_stored_spec_is_not_aliased_to_the_caller_dict(self):
        spec = copy.deepcopy(SPEC)
        spec["job_id"] = "demo-002"
        self.orch.submit_job(spec)
        spec["workload"]["args"]["interval_ms"] = 999
        with self.assertRaises(OrchestratorError) as ctx:
            self.orch.submit_job(spec)
        self.assertEqual(ctx.exception.code, ErrorCode.JOB_ALREADY_EXISTS)


class InvalidSpec(unittest.TestCase):
    CASES = {
        "not an object": ["demo-001"],
        "missing job_id": {k: v for k, v in SPEC.items() if k != "job_id"},
        "empty job_id": spec_with(job_id=""),
        "job_id with slash": spec_with(job_id="a/b"),
        "unknown field": spec_with(priority=1),
        "unknown workload": spec_with(workload={"entrypoint": "rm -rf", "args": {}}),
        "zero interval": spec_with(workload={"entrypoint": "ticker", "args": {"interval_ms": 0}}),
        "boolean interval": spec_with(workload={"entrypoint": "ticker", "args": {"interval_ms": True}}),
        "negative replicas": spec_with(replicas=-1),
        "fractional replicas": spec_with(replicas=1.5),
        "boolean replicas": spec_with(replicas=True),
        "zero cpu": spec_with(resources={"cpu": 0}),
        "infinite cpu": spec_with(resources={"cpu": float("inf")}),
        "unsupported memory": spec_with(resources={"cpu": 1, "memory_mb": 128}),
        "empty label": spec_with(placement={"node_label": ""}),
    }

    def test_each_case_is_invalid_spec_and_registers_nothing(self):
        for name, spec in self.CASES.items():
            with self.subTest(name):
                orch = Orchestrator()
                with self.assertRaises(OrchestratorError) as ctx:
                    orch.submit_job(spec)
                self.assertEqual(ctx.exception.code, ErrorCode.INVALID_SPEC)
                self.assertEqual(orch.events(), [])


class InstanceCorrelation(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator(clock=lambda: FIXED_NOW)
        self.orch.submit_job(SPEC)

    def test_attempts_get_distinct_ids_and_start_pending(self):
        actions = self.orch.reconcile("demo-001")
        self.assertEqual(actions.start, ("demo-001/1", "demo-001/2"))
        view = self.orch.get_job("demo-001")
        self.assertEqual(view["observed"]["pending"], 2)
        self.assertEqual([i["state"] for i in view["instances"]], ["PENDING", "PENDING"])

    def test_health_report_links_attempt_to_process_and_node(self):
        instance_id = self.orch.reconcile("demo-001").start[0]
        self.orch.report_healthy(instance_id, pid=1234, node_id="node-A", progress=7)

        self.assertEqual(
            self.orch.get_job("demo-001")["instances"][0],
            {"instance_id": "demo-001/1", "state": "RUNNING", "pid": 1234, "node_id": "node-A", "progress": 7},
        )
        observed = [e for e in self.orch.events() if e["event"] == "instance_observed"]
        self.assertEqual(
            observed,
            [
                {
                    "t": "2026-10-18T12:00:00.000Z",
                    "event": "instance_observed",
                    "job_id": "demo-001",
                    "instance_id": "demo-001/1",
                    "from_state": "PENDING",
                    "state": "RUNNING",
                    "pid": 1234,
                    "node_id": "node-A",
                    "progress": 7,
                }
            ],
        )

    def test_repeating_the_same_report_adds_no_event(self):
        instance_id = self.orch.reconcile("demo-001").start[0]
        self.orch.report_healthy(instance_id, pid=1, node_id="node-A", progress=3)
        before = len(self.orch.events())
        self.orch.report_healthy(instance_id, pid=1, node_id="node-A", progress=3)
        self.assertEqual(len(self.orch.events()), before)

    def test_finished_attempts_stay_listed_but_are_not_counted(self):
        failed, replacement = self.orch.reconcile("demo-001").start
        self.orch.report_death(failed)
        view = self.orch.get_job("demo-001")
        self.assertEqual([i["instance_id"] for i in view["instances"]], [failed, replacement])
        self.assertEqual(view["observed"], {"running": 0, "pending": 1, "unknown": 0, "stopping": 0})
        self.assertEqual(self.orch.active_instances("demo-001"), [replacement])

    def test_attempt_numbers_are_per_job(self):
        self.orch.submit_job(spec_with(job_id="demo-002", replicas=1))
        self.orch.reconcile("demo-001")
        self.assertEqual(self.orch.reconcile("demo-002").start, ("demo-002/1",))

    def test_reconciling_unknown_job_is_job_not_found(self):
        with self.assertRaises(OrchestratorError) as ctx:
            self.orch.reconcile("missing")
        self.assertEqual(ctx.exception.code, ErrorCode.JOB_NOT_FOUND)

    def test_events_can_be_saved_as_jsonl(self):
        self.orch.set_replicas("demo-001", 1)
        self.orch.reconcile("demo-001")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            self.orch.dump_events(path)
            lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([e["event"] for e in lines], ["job_submitted", "replicas_set", "instance_requested"])


class SetReplicas(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator()
        self.orch.submit_job(SPEC)

    def test_sets_the_absolute_goal_and_returns_it(self):
        self.assertEqual(self.orch.set_replicas("demo-001", 3), 3)
        self.assertEqual(self.orch.get_job("demo-001")["desired_replicas"], 3)

    def test_repeating_the_same_value_changes_nothing(self):
        self.orch.set_replicas("demo-001", 3)
        self.orch.set_replicas("demo-001", 3)
        changes = [e for e in self.orch.events() if e["event"] == "replicas_set"]
        self.assertEqual(len(changes), 1)
        self.assertEqual((changes[0]["from_replicas"], changes[0]["replicas"]), (2, 3))

    def test_only_the_goal_changes_until_the_next_reconcile(self):
        self.orch.set_replicas("demo-001", 3)
        self.assertEqual(self.orch.get_job("demo-001")["instances"], [])

    def test_resubmitting_the_original_spec_keeps_the_new_goal(self):
        self.orch.set_replicas("demo-001", 5)
        self.assertEqual(self.orch.submit_job(copy.deepcopy(SPEC)), "demo-001")
        self.assertEqual(self.orch.get_job("demo-001")["desired_replicas"], 5)

    def test_stop_job_sets_zero_and_the_job_can_be_scaled_again(self):
        self.assertEqual(self.orch.stop_job("demo-001"), 0)
        self.assertEqual(self.orch.stop_job("demo-001"), 0)
        self.assertEqual(self.orch.set_replicas("demo-001", 1), 1)

    def test_invalid_values_are_rejected(self):
        for value in (-1, 1.5, True, "3"):
            with self.subTest(value=value):
                with self.assertRaises(OrchestratorError) as ctx:
                    self.orch.set_replicas("demo-001", value)
                self.assertEqual(ctx.exception.code, ErrorCode.INVALID_ARGUMENT)
        self.assertEqual(self.orch.get_job("demo-001")["desired_replicas"], 2)

    def test_unknown_job_is_job_not_found(self):
        for operation in (lambda: self.orch.set_replicas("missing", 1), lambda: self.orch.stop_job("missing")):
            with self.subTest(operation=operation):
                with self.assertRaises(OrchestratorError) as ctx:
                    operation()
                self.assertEqual(ctx.exception.code, ErrorCode.JOB_NOT_FOUND)


class Reconcile(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator(max_missed_probes=3)
        self.orch.submit_job(spec_with(replicas=1))

    def states(self):
        return [i["state"] for i in self.orch.get_job("demo-001")["instances"]]

    def running(self):
        return self.orch.get_job("demo-001")["observed"]["running"]

    def test_sequence_1_3_1_0_converges_without_leftovers(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.assertEqual(self.running(), 1)

        self.orch.set_replicas("demo-001", 3)
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.assertEqual(self.running(), 3)

        self.orch.set_replicas("demo-001", 1)
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.assertEqual(self.running(), 1)

        self.orch.stop_job("demo-001")
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.assertEqual(self.orch.active_instances("demo-001"), [])
        self.assertEqual(self.states(), ["STOPPED", "STOPPED", "STOPPED"])
        self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions())

    def test_pending_attempts_are_not_duplicated(self):
        # Not enough capacity: the attempt stays PENDING across many passes.
        (instance_id,) = self.orch.reconcile("demo-001").start
        for _ in range(5):
            self.orch.report_unresponsive(instance_id)
            self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions())
        self.assertEqual(self.states(), ["PENDING"])

    def test_scale_down_stops_pending_before_running_and_newest_first(self):
        self.orch.set_replicas("demo-001", 3)
        old, middle, new = self.orch.reconcile("demo-001").start
        self.orch.report_healthy(old, pid=1, node_id="n", progress=0)
        self.orch.report_healthy(middle, pid=2, node_id="n", progress=0)
        # "new" is still PENDING.
        self.orch.set_replicas("demo-001", 1)
        self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions(kill=(new, middle)))
        self.assertEqual(self.states(), ["RUNNING", "STOPPING", "STOPPING"])

    def test_death_after_requested_stop_is_stopped_not_failed(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.orch.stop_job("demo-001")
        (instance_id,) = self.orch.reconcile("demo-001").kill
        self.orch.report_death(instance_id)
        self.assertEqual(self.states(), ["STOPPED"])
        self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions())

    def test_unexpected_death_is_replaced_with_a_new_attempt(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.orch.report_death("demo-001/1")
        self.assertEqual(self.orch.reconcile("demo-001").start, ("demo-001/2",))
        self.assertEqual(self.states(), ["FAILED", "PENDING"])

    def test_with_zero_goal_a_lost_attempt_is_not_replaced(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.orch.set_replicas("demo-001", 0)
        self.orch.report_death("demo-001/1")
        self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions())

    def test_unknown_counts_until_k_missed_probes_then_is_killed_and_replaced(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        for _ in range(2):
            self.orch.report_unresponsive("demo-001/1")
            self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions())
        self.assertEqual(self.states(), ["UNKNOWN"])

        self.orch.report_unresponsive("demo-001/1")  # third missed probe
        self.assertEqual(
            self.orch.reconcile("demo-001"),
            ReconcileActions(start=("demo-001/2",), kill=("demo-001/1",)),
        )
        self.assertEqual(self.states(), ["FAILED", "PENDING"])

    def test_answering_again_clears_uncertainty(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.orch.report_unresponsive("demo-001/1")
        self.orch.report_unresponsive("demo-001/1")
        self.orch.report_healthy("demo-001/1", pid=1001, node_id="node-A", progress=9)
        self.orch.report_unresponsive("demo-001/1")
        self.orch.report_unresponsive("demo-001/1")
        self.assertEqual(self.orch.reconcile("demo-001"), ReconcileActions())
        self.assertEqual(self.states(), ["UNKNOWN"])

    def test_late_answer_does_not_revive_a_given_up_attempt(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        for _ in range(3):
            self.orch.report_unresponsive("demo-001/1")
        self.orch.reconcile("demo-001")
        self.orch.report_healthy("demo-001/1", pid=1001, node_id="node-A", progress=10)
        self.assertEqual(self.states(), ["FAILED", "PENDING"])
        self.assertIn("stale_report_ignored", [e["event"] for e in self.orch.events()])

    def test_illegal_transition_is_a_controller_bug(self):
        run_actions(self.orch, self.orch.reconcile("demo-001"))
        self.orch.report_death("demo-001/1")
        instance = self.orch._instances["demo-001/1"]
        with self.assertRaises(ValueError):
            instance.move_to(InstanceState.RUNNING)


if __name__ == "__main__":
    unittest.main()
