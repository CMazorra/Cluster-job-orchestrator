"""Translation between runtime observations and domain calls, with a fake runtime.

No Ray is needed: the fake runtime returns whatever outcome a test wants and
records what the controller asked for.
"""

import unittest

from orchestrator import Orchestrator, RuntimeController
from orchestrator.runtime import (
    DEAD,
    HEALTHY,
    KILL_REQUESTED,
    MISSING,
    UNRESPONSIVE,
    InstanceSpec,
    Observation,
    Termination,
)


class FakeRuntime:
    def __init__(self) -> None:
        self.outcomes: dict[str, Observation] = {}
        self.started: list[tuple[str, InstanceSpec]] = []
        self.terminated: list[str] = []
        self.probe_rounds: list[list[str]] = []

    def start(self, instance_id: str, spec: InstanceSpec) -> None:
        self.started.append((instance_id, spec))

    def probe_all(self, instance_ids: list[str]) -> dict[str, Observation]:
        self.probe_rounds.append(list(instance_ids))
        return {i: self.outcomes.get(i, Observation(i, UNRESPONSIVE)) for i in instance_ids}

    def terminate(self, instance_id: str) -> Termination:
        self.terminated.append(instance_id)
        return Termination(instance_id, KILL_REQUESTED)

    def healthy(self, instance_id: str, progress: int = 0) -> None:
        self.outcomes[instance_id] = Observation(instance_id, HEALTHY, pid=42, node_id="node-1", progress=progress)

    def dead(self, instance_id: str) -> None:
        self.outcomes[instance_id] = Observation(instance_id, DEAD)


def spec(replicas: int, node_label: str | None = None) -> dict:
    data = {
        "job_id": "job",
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": 50}},
        "replicas": replicas,
        "resources": {"cpu": 0.5},
    }
    if node_label is not None:
        data["placement"] = {"node_label": node_label}
    return data


class ControllerTranslation(unittest.TestCase):
    def setUp(self):
        self.orch = Orchestrator(max_missed_probes=3)
        self.runtime = FakeRuntime()
        self.controller = RuntimeController(self.orch, self.runtime)

    def submit(self, replicas: int, node_label: str | None = None) -> None:
        self.orch.submit_job(spec(replicas, node_label))

    def states(self) -> list[str]:
        return [i["state"] for i in self.orch.get_job("job")["instances"]]

    def test_act_starts_missing_attempts_with_the_job_resources(self):
        self.submit(1, node_label="zone-a")
        actions = self.controller.act("job")
        self.assertEqual(actions.start, ("job/1",))
        self.assertEqual(self.runtime.started, [("job/1", InstanceSpec(interval_ms=50, cpu=0.5, node_label="zone-a"))])
        self.assertEqual(self.states(), ["PENDING"])

    def test_one_probe_round_covers_every_active_attempt(self):
        self.submit(3)
        self.controller.act("job")
        self.controller.observe("job")
        self.assertEqual(self.runtime.probe_rounds, [["job/1", "job/2", "job/3"]])

    def test_a_timeout_keeps_a_new_attempt_pending(self):
        self.submit(1)
        self.controller.act("job")
        self.controller.observe("job")
        self.assertEqual(self.states(), ["PENDING"])

    def test_a_healthy_answer_copies_identity_and_progress(self):
        self.submit(1)
        self.controller.act("job")
        self.runtime.healthy("job/1", progress=7)
        self.controller.observe("job")
        instance = self.orch.get_job("job")["instances"][0]
        self.assertEqual(
            (instance["state"], instance["pid"], instance["node_id"], instance["progress"]),
            ("RUNNING", 42, "node-1", 7),
        )

    def test_an_answer_without_identity_is_uncertainty_not_death(self):
        self.submit(1)
        self.controller.act("job")
        self.runtime.healthy("job/1")
        self.controller.observe("job")
        self.runtime.outcomes["job/1"] = Observation("job/1", HEALTHY, pid=None, node_id=None)
        self.controller.observe("job")
        self.assertEqual(self.states(), ["UNKNOWN"])

    def test_a_kill_is_only_a_request_until_a_probe_confirms_the_death(self):
        self.submit(1)
        self.controller.act("job")
        self.runtime.healthy("job/1")
        self.controller.observe("job")
        self.orch.stop_job("job")

        self.controller.act("job")
        self.assertEqual(self.runtime.terminated, ["job/1"])
        self.assertEqual(self.states(), ["STOPPING"])

        self.controller.observe("job")  # the process still answers: the stop is in flight
        self.assertEqual(self.states(), ["STOPPING"])

        self.runtime.dead("job/1")
        self.controller.observe("job")
        self.assertEqual(self.states(), ["STOPPED"])

    def test_a_late_answer_does_not_revive_a_stopped_attempt(self):
        self.submit(1)
        self.controller.act("job")
        self.orch.stop_job("job")
        self.controller.act("job")
        self.runtime.dead("job/1")
        self.controller.observe("job")
        self.runtime.healthy("job/1", progress=99)
        self.controller.observe("job")
        self.assertEqual(self.states(), ["STOPPED"])

    def test_a_pending_attempt_is_not_requested_twice(self):
        self.submit(2)
        for _ in range(3):
            self.controller.step("job")
        self.assertEqual(len(self.runtime.started), 2)

    def test_a_confirmed_death_is_replaced_by_a_new_attempt(self):
        self.submit(1)
        self.controller.act("job")
        self.runtime.healthy("job/1")
        self.controller.observe("job")
        self.runtime.dead("job/1")
        actions = self.controller.step("job")
        self.assertEqual(actions.start, ("job/2",))
        self.assertEqual(self.states(), ["FAILED", "PENDING"])

    def test_an_attempt_the_runtime_never_started_counts_as_dead(self):
        self.submit(1)
        self.controller.act("job")
        self.runtime.outcomes["job/1"] = Observation("job/1", MISSING)
        self.controller.observe("job")
        self.assertEqual(self.states(), ["FAILED"])

    def test_given_up_attempt_is_killed_and_replaced_in_the_same_cycle(self):
        self.submit(1)
        self.controller.act("job")
        self.runtime.healthy("job/1")
        self.controller.observe("job")
        self.runtime.outcomes["job/1"] = Observation("job/1", UNRESPONSIVE)
        for _ in range(3):
            self.controller.observe("job")
        actions = self.controller.act("job")
        self.assertEqual((actions.kill, actions.start), (("job/1",), ("job/2",)))
        self.assertEqual(self.runtime.terminated, ["job/1"])


if __name__ == "__main__":
    unittest.main()
