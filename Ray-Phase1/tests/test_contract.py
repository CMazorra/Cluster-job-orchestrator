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

from orchestrator import ErrorCode, InstanceState, Orchestrator, OrchestratorError

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
        first = self.orch.add_instance("demo-001")
        second = self.orch.add_instance("demo-001")
        self.assertEqual((first, second), ("demo-001/1", "demo-001/2"))
        view = self.orch.get_job("demo-001")
        self.assertEqual(view["observed"]["pending"], 2)
        self.assertEqual([i["state"] for i in view["instances"]], ["PENDING", "PENDING"])

    def test_observation_links_attempt_to_process_and_node(self):
        instance_id = self.orch.add_instance("demo-001")
        self.orch.update_instance(instance_id, state=InstanceState.RUNNING, pid=1234, node_id="node-A", progress=7)

        self.assertEqual(
            self.orch.get_job("demo-001")["instances"],
            [{"instance_id": "demo-001/1", "state": "RUNNING", "pid": 1234, "node_id": "node-A", "progress": 7}],
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

    def test_repeating_the_same_observation_adds_no_event(self):
        instance_id = self.orch.add_instance("demo-001")
        self.orch.update_instance(instance_id, progress=3)
        before = len(self.orch.events())
        self.orch.update_instance(instance_id, progress=3)
        self.assertEqual(len(self.orch.events()), before)

    def test_finished_attempts_stay_listed_but_are_not_counted(self):
        failed = self.orch.add_instance("demo-001")
        replacement = self.orch.add_instance("demo-001")
        self.orch.update_instance(failed, state=InstanceState.FAILED)
        view = self.orch.get_job("demo-001")
        self.assertEqual([i["instance_id"] for i in view["instances"]], [failed, replacement])
        self.assertEqual(view["observed"], {"running": 0, "pending": 1, "unknown": 0, "stopping": 0})
        self.assertEqual(self.orch.active_instances("demo-001"), [replacement])

    def test_attempt_numbers_are_per_job(self):
        self.orch.submit_job(spec_with(job_id="demo-002"))
        self.orch.add_instance("demo-001")
        self.assertEqual(self.orch.add_instance("demo-002"), "demo-002/1")

    def test_adding_to_unknown_job_is_job_not_found(self):
        with self.assertRaises(OrchestratorError) as ctx:
            self.orch.add_instance("missing")
        self.assertEqual(ctx.exception.code, ErrorCode.JOB_NOT_FOUND)

    def test_events_can_be_saved_as_jsonl(self):
        self.orch.add_instance("demo-001")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            self.orch.dump_events(path)
            lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([e["event"] for e in lines], ["job_submitted", "instance_requested"])


if __name__ == "__main__":
    unittest.main()
