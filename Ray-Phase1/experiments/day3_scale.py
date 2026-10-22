"""Day 3: the reconcile loop, against the goal sequence 1 -> 3 -> 1 -> 0.

The domain decides (``Orchestrator``); the runtime acts (``RayRuntime``). This
script is the glue between the two: it turns observations into reports and
decisions into process actions. It makes no decision of its own: every
transition happens inside the domain.

Usage (needs Ray, inside WSL2):

    python experiments/day3_scale.py
    python experiments/day3_scale.py --output resultados.json --events eventos.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import ray

from orchestrator.api import Orchestrator
from ray_runtime import HEALTHY, UNRESPONSIVE, InstanceSpec, RayRuntime

PREFIX = "[dia3]"
DEFAULT_PROBE_TIMEOUT_S = 2.0
# States that count toward the desired replicas. Mirrors the domain on purpose:
# the invariant is checked from the outside, not asked to the domain.
COUNTED = ("PENDING", "RUNNING", "UNKNOWN")
ALL_STATES = ("PENDING", "RUNNING", "UNKNOWN", "STOPPING", "STOPPED", "FAILED")
LETTER = {"PENDING": "P", "RUNNING": "R", "UNKNOWN": "U", "STOPPING": "S", "STOPPED": "T", "FAILED": "F"}


class RuntimeController:
    """Translates between the domain and the runtime. It decides nothing."""

    def __init__(self, orchestrator: Orchestrator, runtime: RayRuntime) -> None:
        self._orchestrator = orchestrator
        self._runtime = runtime
        self.starts: list[str] = []
        self.kills: list[str] = []

    def observe(self, job_id: str) -> None:
        """Asks the runtime about every active attempt and reports the answer to the domain."""
        for instance_id in self._orchestrator.active_instances(job_id):
            observation = self._runtime.probe(instance_id)
            if (
                observation.outcome == HEALTHY
                and observation.pid is not None
                and observation.node_id is not None
            ):
                self._orchestrator.report_healthy(
                    instance_id,
                    pid=observation.pid,
                    node_id=observation.node_id,
                    progress=observation.progress or 0,
                )
            elif observation.outcome == UNRESPONSIVE:
                # A timeout is uncertainty: the domain decides how many it tolerates.
                self._orchestrator.report_unresponsive(instance_id)
            else:
                # In this layer "dead" and "missing" both mean: there is no process.
                self._orchestrator.report_death(instance_id)

    def act(self, job_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Asks the domain what to do and does it: kill first, then start."""
        actions = self._orchestrator.reconcile(job_id)
        for instance_id in actions.kill:
            self._runtime.terminate(instance_id)
            # The domain asked for these; a death after a requested stop is a stop,
            # not a failure. The domain tells the two apart; we just report the death.
            self._orchestrator.report_death(instance_id)
            self.kills.append(instance_id)
        spec = InstanceSpec.from_job_spec(self._orchestrator.job_spec(job_id))
        for instance_id in actions.start:
            self._runtime.start(instance_id, spec)
            self.starts.append(instance_id)
        return actions.start, actions.kill


@dataclass(frozen=True)
class Phase:
    name: str
    desired: int


def _counts(orchestrator: Orchestrator, job_id: str) -> tuple[dict[str, int], dict[str, str]]:
    job = orchestrator.get_job(job_id)
    counts = {state: 0 for state in ALL_STATES}
    instances: dict[str, str] = {}
    for instance in job["instances"]:
        counts[instance["state"]] += 1
        instances[instance["instance_id"]] = instance["state"]
    return counts, instances


def _settled(counts: dict[str, int], desired: int) -> bool:
    """The goal is met and nothing is still moving: no pending, no doubt, no stop in flight."""
    return (
        counts["RUNNING"] == desired
        and counts["PENDING"] == 0
        and counts["UNKNOWN"] == 0
        and counts["STOPPING"] == 0
    )


def _active_ids(instances: dict[str, str]) -> list[str]:
    return [iid for iid, state in instances.items() if state in COUNTED]


def _cycle(
    controller: RuntimeController,
    orchestrator: Orchestrator,
    job_id: str,
    phase: str,
    index: int,
    desired: int,
) -> dict[str, Any]:
    controller.observe(job_id)
    started, killed = controller.act(job_id)
    counts, instances = _counts(orchestrator, job_id)
    rendered = ", ".join(f"{iid}({LETTER[state]})" for iid, state in instances.items())
    print(
        f"{PREFIX} {phase} ciclo {index}: objetivo {desired} | "
        f"R {counts['RUNNING']} P {counts['PENDING']} U {counts['UNKNOWN']} "
        f"S {counts['STOPPING']} T {counts['STOPPED']} F {counts['FAILED']} | "
        f"+{len(started)} -{len(killed)} | {rendered}"
    )
    return {
        "phase": phase,
        "cycle": index,
        "desired": desired,
        "counts": counts,
        "instances": instances,
        "started": list(started),
        "killed": list(killed),
        "counted": sum(counts[state] for state in COUNTED),
    }


def _run_phase(
    controller: RuntimeController,
    orchestrator: Orchestrator,
    job_id: str,
    phase: Phase,
    *,
    cycle_seconds: float,
    deadline_seconds: float,
) -> dict[str, Any]:
    orchestrator.set_replicas(job_id, phase.desired)
    _, instances_before = _counts(orchestrator, job_id)
    active_before = _active_ids(instances_before)
    started_at = time.time()
    rows: list[dict[str, Any]] = []
    index = 0
    while time.time() - started_at < deadline_seconds:
        index += 1
        rows.append(_cycle(controller, orchestrator, job_id, phase.name, index, phase.desired))
        counts, instances = _counts(orchestrator, job_id)
        if _settled(counts, phase.desired):
            return {
                "name": phase.name,
                "desired": phase.desired,
                "settled": True,
                "cycles": index,
                "elapsed_seconds": round(time.time() - started_at, 3),
                "counts": counts,
                "instances": instances,
                "active_before": active_before,
                "trace": rows,
            }
        time.sleep(cycle_seconds)
    counts, instances = _counts(orchestrator, job_id)
    print(f"{PREFIX} {phase.name}: NOT settled within {deadline_seconds} s")
    return {
        "name": phase.name,
        "desired": phase.desired,
        "settled": False,
        "cycles": index,
        "elapsed_seconds": round(time.time() - started_at, 3),
        "counts": counts,
        "instances": instances,
        "active_before": active_before,
        "trace": rows,
    }


def _runtime_env() -> dict[str, Any]:
    """Lets the workers import our module, which lives outside the package."""
    current = os.environ.get("PYTHONPATH", "")
    python_path = os.pathsep.join(part for part in (current, str(ROOT)) if part)
    return {"env_vars": {"PYTHONPATH": python_path}}


def _start_cluster(args: argparse.Namespace) -> dict[str, Any]:
    if args.address:
        ray.init(address=args.address, include_dashboard=True, runtime_env=_runtime_env())
        mode = f"external cluster ({args.address})"
    else:
        ray.init(num_cpus=args.cpus, include_dashboard=True, runtime_env=_runtime_env())
        mode = f"local cluster ({args.cpus} logical CPUs)"
    node_id = None
    try:
        node_id = str(ray.get_runtime_context().get_node_id())
    except Exception:
        pass
    return {
        "mode": mode,
        "ray_version": getattr(ray, "__version__", "unknown"),
        "driver_pid": os.getpid(),
        "driver_node_id": node_id,
        "cluster_resources": ray.cluster_resources(),
        "cpus": args.cpus,
        "cycle_seconds": args.cycle,
        "probe_timeout": args.probe_timeout,
        "max_missed_probes": args.max_missed_probes,
        "sequence": [phase.desired for phase in _phases(args.sequence)],
    }


def _phases(sequence: str) -> list[Phase]:
    values = [int(part) for part in sequence.split(",") if part.strip()]
    if not values or any(value < 0 for value in values):
        raise ValueError("the sequence must be a non-empty list of non-negative integers")
    names = ["inicial", "subida", "bajada", "parada"]
    return [
        Phase(name=names[index] if index < len(names) else f"fase-{index}", desired=value)
        for index, value in enumerate(values)
    ]


def _job_spec(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "job_id": args.job_id,
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": args.interval_ms}},
        "replicas": _phases(args.sequence)[0].desired,
        "resources": {"cpu": args.cpu_per_instance},
    }


def _checks(
    phases: list[dict[str, Any]],
    trace: list[dict[str, Any]],
    starts: list[str],
    kills: list[str],
    hold_started: int,
    hold_ended: int,
    hold_starts: int,
    runtime: RayRuntime,
    sequence: list[int],
) -> list[dict[str, Any]]:
    expected_starts = sum(max(0, after - before) for before, after in zip([0] + sequence, sequence))
    by_name = {phase["name"]: phase for phase in phases}
    up = by_name.get("subida")
    down = by_name.get("bajada")
    stop = by_name.get("parada")

    survivor = []
    if down is not None:
        survivor = [iid for iid, state in down["instances"].items() if state == "RUNNING"]

    over_desired = [row for row in trace if row["counted"] != row["desired"]]
    all_settled = all(phase["settled"] for phase in phases)
    running_ok = all(phase["counts"]["RUNNING"] == phase["desired"] for phase in phases if phase["settled"])

    checks = [
        {
            "name": "todas las fases convergen al objetivo",
            "ok": all_settled,
            "detail": ", ".join(
                f"{phase['name']}={phase['desired']}:{'si' if phase['settled'] else 'NO'}"
                for phase in phases
            ),
        },
        {
            "name": "cada fase terminó con RUNNING igual al objetivo",
            "ok": running_ok,
            "detail": ", ".join(
                f"{phase['name']}: {phase['counts']['RUNNING']}/{phase['desired']}" for phase in phases
            ),
        },
        {
            "name": "el pendiente no se multiplica",
            "ok": not over_desired,
            "detail": f"{len(over_desired)} ciclos con intentos contados != objetivo",
        },
        {
            "name": "no se piden intentos de más",
            "ok": len(starts) == expected_starts,
            "detail": f"pedidos {len(starts)}, esperados {expected_starts}",
        },
        {
            "name": "con el objetivo a cero no se recrea nada",
            "ok": hold_ended == hold_started and hold_starts == 0,
            "detail": f"intentos {hold_started} -> {hold_ended}, nuevos {hold_starts}",
        },
        {
            "name": "los procesos terminan y no queda ningún actor",
            "ok": runtime.instances() == [],
            "detail": f"actores vivos {runtime.instances()}",
        },
        {
            "name": "los identificadores de intento no se repiten",
            "ok": len(set(starts)) == len(starts) and len(set(kills)) == len(kills),
            "detail": f"pedidos {starts}, terminados {kills}",
        },
    ]
    if sequence == [1, 3, 1, 0]:
        checks.insert(
            2,
            {
                "name": "la subida deja 3 réplicas sanas",
                "ok": bool(up and up["settled"] and up["counts"]["RUNNING"] == 3),
                "detail": f"RUNNING {up['counts']['RUNNING'] if up else '?'}",
            },
        )
        checks.insert(
            3,
            {
                "name": "la bajada deja un solo intento, y es uno anterior",
                "ok": bool(
                    down
                    and down["settled"]
                    and down["counts"]["RUNNING"] == 1
                    and len(survivor) == 1
                    and survivor[0] in down["active_before"]
                ),
                "detail": f"sobrevive {survivor}, STOPPED {down['counts']['STOPPED'] if down else '?'}",
            },
        )
        checks.insert(
            4,
            {
                "name": "la parada total no deja procesos activos",
                "ok": bool(
                    stop
                    and stop["settled"]
                    and sum(stop["counts"][state] for state in COUNTED) == 0
                ),
                "detail": f"activos {sum(stop['counts'][state] for state in COUNTED) if stop else '?'}",
            },
        )
    return checks


def run_scale(args: argparse.Namespace) -> dict[str, Any]:
    """Runs the 1 -> 3 -> 1 -> 0 sequence and returns the raw numbers."""
    phases = _phases(args.sequence)
    meta = _start_cluster(args)
    orchestrator = Orchestrator(max_missed_probes=args.max_missed_probes)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)

    job_id = orchestrator.submit_job(_job_spec(args))
    print(f"{PREFIX} job {job_id!r} submitted with goal {phases[0].desired}")

    phase_results: list[dict[str, Any]] = []
    trace: list[dict[str, Any]] = []
    for phase in phases:
        result = _run_phase(
            controller,
            orchestrator,
            job_id,
            phase,
            cycle_seconds=args.cycle,
            deadline_seconds=args.deadline,
        )
        trace.extend(result["trace"])
        phase_results.append(result)

    _, before = _counts(orchestrator, job_id)
    starts_before = len(controller.starts)
    for index in range(1, args.hold_cycles + 1):
        trace.append(_cycle(controller, orchestrator, job_id, "espera", index, 0))
        time.sleep(args.cycle)
    _, after = _counts(orchestrator, job_id)
    print(f"{PREFIX} {args.hold_cycles} cycles with goal 0 and nothing is recreated")

    checks = _checks(
        phase_results,
        trace,
        controller.starts,
        controller.kills,
        len(before),
        len(after),
        len(controller.starts) - starts_before,
        runtime,
        [phase.desired for phase in phases],
    )
    runtime.shutdown()

    print(f"{PREFIX} --- comprobaciones ---")
    for check in checks:
        print(f"{PREFIX} [{'OK' if check['ok'] else 'FAIL'}] {check['name']}: {check['detail']}")

    results = {
        "meta": meta,
        "job_id": job_id,
        "phases": phase_results,
        "trace": trace,
        "starts": controller.starts,
        "kills": controller.kills,
        "checks": checks,
        "events": orchestrator.events(),
    }
    meta["events"] = len(orchestrator.events())
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile loop over 1 -> 3 -> 1 -> 0.")
    parser.add_argument("--job-id", default="dia3", help="job id submitted to the domain")
    parser.add_argument("--sequence", default="1,3,1,0", help="absolute goals, comma separated")
    parser.add_argument("--interval-ms", type=int, default=100, help="ticker interval")
    parser.add_argument("--cpu-per-instance", type=float, default=1.0, help="logical CPU per instance")
    parser.add_argument("--cpus", type=int, default=4, help="logical CPUs of a local cluster")
    parser.add_argument("--address", default=None, help="connect to a running cluster instead")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds a phase may take")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--max-missed-probes", type=int, default=3, help="K: probes before giving up")
    parser.add_argument("--hold-cycles", type=int, default=3, help="idle cycles with goal 0")
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        results = run_scale(args)
    finally:
        ray.shutdown()

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"{PREFIX} results written to {args.output}")
    if args.events is not None:
        args.events.parent.mkdir(parents=True, exist_ok=True)
        args.events.write_text(
            "\n".join(json.dumps(event, ensure_ascii=False) for event in results["events"]),
            encoding="utf-8",
        )
        print(f"{PREFIX} events written to {args.events}")

    failed = [check["name"] for check in results["checks"] if not check["ok"]]
    if failed:
        print(f"{PREFIX} FAILED: {failed}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
