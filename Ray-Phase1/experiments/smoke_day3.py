"""Smoke test of the day 3 reconcile loop, against the double of Ray.

It runs the same 1 -> 3 -> 1 -> 0 sequence as ``day3_scale.py`` with the double
of ``ray``, and then checks the translation between the runtime and the domain
on its own, with a fake runtime. It proves the glue runs and reports what it
should; it proves nothing about Ray's scheduler or admission control.

The output of this script is not evidence and is not saved as such.

Usage (no Ray needed):

    python experiments/smoke_day3.py
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE / "stub"))
sys.path.insert(0, str(HERE))

import day3_scale

from orchestrator.api import Orchestrator
from ray_runtime import DEAD, HEALTHY, MISSING, UNRESPONSIVE, InstanceSpec, Observation, Termination

PREFIX = "[smoke3]"


def _report(results: dict) -> list[str]:
    failures = []
    for check in results["checks"]:
        print(f"{PREFIX} [{'OK' if check['ok'] else 'FAIL'}] {check['name']}: {check['detail']}")
        if not check["ok"]:
            failures.append(check["name"])
    return failures


def _spec(replicas: int) -> dict:
    return {
        "job_id": "smoke3",
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": 50}},
        "replicas": replicas,
        "resources": {"cpu": 1.0},
    }


class FakeRuntime:
    """Returns whatever outcome the check wants; records what the glue asked for."""

    def __init__(self, outcomes: dict[str, Observation]) -> None:
        self.outcomes = outcomes
        self.started: list[tuple[str, InstanceSpec]] = []
        self.terminated: list[str] = []

    def probe(self, instance_id: str) -> Observation:
        return self.outcomes.get(instance_id, Observation(instance_id, MISSING))

    def start(self, instance_id: str, spec: InstanceSpec) -> None:
        self.started.append((instance_id, spec))

    def terminate(self, instance_id: str) -> Termination:
        self.terminated.append(instance_id)
        return Termination(instance_id, "terminated")


def _state(orchestrator: Orchestrator, job_id: str, instance_id: str) -> str:
    for instance in orchestrator.get_job(job_id)["instances"]:
        if instance["instance_id"] == instance_id:
            return instance["state"]
    return "?"


def _phrase(failures: list[str], name: str, ok: bool, detail: str) -> None:
    print(f"{PREFIX} [{'OK' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        failures.append(name)


def _translation_checks() -> list[str]:
    """The translation itself: each runtime outcome must become the right domain call."""
    failures: list[str] = []
    orchestrator = Orchestrator()
    job_id = orchestrator.submit_job(_spec(1))

    runtime = FakeRuntime({"smoke3/1": Observation("smoke3/1", UNRESPONSIVE)})
    controller = day3_scale.RuntimeController(orchestrator, runtime)

    started, _ = controller.act(job_id)
    _phrase(failures, "act pide el intento que falta", started == ("smoke3/1",), f"start={started}")
    _phrase(failures, "el intento nace PENDING", _state(orchestrator, job_id, "smoke3/1") == "PENDING", "")

    controller.observe(job_id)
    state = _state(orchestrator, job_id, "smoke3/1")
    _phrase(failures, "un timeout deja el intento PENDING, no lo mata", state == "PENDING", f"estado {state}")

    runtime.outcomes["smoke3/1"] = Observation(
        "smoke3/1", HEALTHY, pid=42, node_id="node-1", progress=7
    )
    controller.observe(job_id)
    _phrase(failures, "una respuesta sana lo pasa a RUNNING", _state(orchestrator, job_id, "smoke3/1") == "RUNNING", "")
    instance = orchestrator.get_job(job_id)["instances"][0]
    _phrase(
        failures,
        "la salud copia pid, nodo y progreso al dominio",
        (instance["pid"], instance["node_id"], instance["progress"]) == (42, "node-1", 7),
        f"pid={instance['pid']} node={instance['node_id']} progress={instance['progress']}",
    )

    orchestrator.set_replicas(job_id, 0)
    controller.act(job_id)
    _phrase(
        failures,
        "una muerte tras pedir la parada es STOPPED, no FAILED",
        _state(orchestrator, job_id, "smoke3/1") == "STOPPED",
        f"estado {_state(orchestrator, job_id, 'smoke3/1')}",
    )

    runtime.outcomes["smoke3/1"] = Observation("smoke3/1", HEALTHY, pid=42, node_id="node-1", progress=99)
    controller.observe(job_id)
    _phrase(
        failures,
        "una respuesta tardía no revive un intento terminado",
        _state(orchestrator, job_id, "smoke3/1") == "STOPPED",
        f"estado {_state(orchestrator, job_id, 'smoke3/1')}",
    )

    # A pending attempt counts toward the goal: asking again must not duplicate it.
    orchestrator = Orchestrator()
    job_id = orchestrator.submit_job(_spec(2))
    runtime = FakeRuntime({"smoke3/1": Observation("smoke3/1", UNRESPONSIVE), "smoke3/2": Observation("smoke3/2", UNRESPONSIVE)})
    controller = day3_scale.RuntimeController(orchestrator, runtime)
    controller.act(job_id)
    controller.observe(job_id)
    controller.act(job_id)
    _phrase(failures, "un pendiente cuenta: no se pide otro igual", len(runtime.started) == 2, f"pedidos {len(runtime.started)}")

    runtime.outcomes["smoke3/2"] = Observation("smoke3/2", DEAD)
    controller.observe(job_id)
    _phrase(failures, "una muerte confirmada deja el intento FAILED", _state(orchestrator, job_id, "smoke3/2") == "FAILED", "")
    controller.act(job_id)
    replacement = [iid for iid, _ in runtime.started if iid not in ("smoke3/1", "smoke3/2")]
    _phrase(failures, "la muerte confirmada pide un reemplazo nuevo", len(replacement) == 1, f"reemplazo {replacement}")

    return failures


def main() -> int:
    print(f"{PREFIX} ==== smoke test with the double of ray ====")
    print(f"{PREFIX} no real processes, no real scheduler: this is not evidence")

    args = day3_scale.build_parser().parse_args([])
    args.cpus = 4
    args.cycle = 0.1
    args.deadline = 15.0
    args.probe_timeout = 0.5
    args.hold_cycles = 2

    failures = _report(day3_scale.run_scale(args))
    failures += _translation_checks()

    if failures:
        print(f"{PREFIX} FAILED: {failures}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
