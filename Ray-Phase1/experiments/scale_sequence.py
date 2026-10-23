"""The reconcile loop against a sequence of absolute goals, by default 1 -> 3 -> 1 -> 0.

The domain decides (``Orchestrator``), the runtime acts (``RayRuntime``) and the
controller translates between them. This script only drives the goals and
checks the outcome; every state transition happens inside the domain.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/scale_sequence.py
    python experiments/scale_sequence.py --output /tmp/scale.json --events /tmp/scale.jsonl
    python experiments/scale_sequence.py --labels zone-a --placement zone-a
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from common import alive_tickers, print_checks, print_meta, start_cluster, wait_for_free_cpus

import ray

from orchestrator import Orchestrator, RuntimeController
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[scale]"
STATES = ("PENDING", "RUNNING", "UNKNOWN", "STOPPING", "STOPPED", "FAILED")
LETTER = {"PENDING": "P", "RUNNING": "R", "UNKNOWN": "U", "STOPPING": "S", "STOPPED": "T", "FAILED": "F"}
PHASE_NAMES = ("initial", "scale-up", "scale-down", "stop")
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--job-id", default="scale-demo", help="job id submitted to the domain")
    parser.add_argument("--sequence", default="1,3,1,0", help="absolute goals, comma separated")
    parser.add_argument("--interval-ms", type=int, default=100, help="ticker interval")
    parser.add_argument("--cpu-per-instance", type=float, default=1.0, help="logical CPU per instance")
    parser.add_argument("--cpus", type=int, default=4, help="logical CPUs of the local cluster")
    parser.add_argument("--labels", nargs="*", default=(), help="node labels the local node declares")
    parser.add_argument("--placement", default=None, help="node label the job requires")
    parser.add_argument("--address", default=None, help="connect to a running cluster instead")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds a phase may take")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--max-missed-probes", type=int, default=3, help="K: missed probes before giving up")
    parser.add_argument("--hold-cycles", type=int, default=3, help="idle cycles with goal 0 at the end")
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def _sequence(text: str) -> list[int]:
    values = [int(part) for part in text.split(",") if part.strip()]
    if not values or any(value < 0 for value in values):
        raise ValueError("the sequence must be a non-empty list of non-negative integers")
    return values


def _phase_name(index: int) -> str:
    return PHASE_NAMES[index] if index < len(PHASE_NAMES) else f"phase-{index + 1}"


def _snapshot(orchestrator: Orchestrator, job_id: str) -> tuple[dict[str, int], dict[str, str]]:
    instances = {i["instance_id"]: i["state"] for i in orchestrator.get_job(job_id)["instances"]}
    counts = {state: sum(1 for s in instances.values() if s == state) for state in STATES}
    return counts, instances


def _settled(counts: dict[str, int], desired: int) -> bool:
    """The goal is met and nothing is still moving: no pending, no doubt, no stop in flight."""
    return counts["RUNNING"] == desired and counts["PENDING"] == counts["UNKNOWN"] == counts["STOPPING"] == 0


def _cycle(controller: RuntimeController, orchestrator: Orchestrator, job_id: str, phase: str, index: int, desired: int) -> dict[str, Any]:
    actions = controller.step(job_id)
    counts, instances = _snapshot(orchestrator, job_id)
    rendered = ", ".join(f"{i}({LETTER[s]})" for i, s in instances.items())
    print(
        f"{PREFIX} {phase} cycle {index}: goal {desired} | "
        + " ".join(f"{LETTER[s]} {counts[s]}" for s in STATES)
        + f" | +{len(actions.start)} -{len(actions.kill)} | {rendered}"
    )
    return {"phase": phase, "cycle": index, "desired": desired, "counts": counts, "instances": instances}


def _run_phase(controller: RuntimeController, orchestrator: Orchestrator, job_id: str, name: str, desired: int, args: argparse.Namespace) -> dict[str, Any]:
    orchestrator.set_replicas(job_id, desired)
    active_before = orchestrator.active_instances(job_id)
    started_at = time.monotonic()
    rows: list[dict[str, Any]] = []
    while time.monotonic() - started_at < args.deadline:
        rows.append(_cycle(controller, orchestrator, job_id, name, len(rows) + 1, desired))
        if _settled(rows[-1]["counts"], desired):
            break
        time.sleep(args.cycle)
    counts, instances = _snapshot(orchestrator, job_id)
    settled = _settled(counts, desired)
    if not settled:
        print(f"{PREFIX} {name}: NOT settled within {args.deadline} s")
    return {
        "name": name,
        "desired": desired,
        "settled": settled,
        "cycles": len(rows),
        "elapsed_seconds": round(time.monotonic() - started_at, 3),
        "counts": counts,
        "instances": instances,
        "active_before": active_before,
        "trace": rows,
    }


def _checks(phases: list[dict[str, Any]], controller: RuntimeController, sequence: list[int], hold_new: int, cpus_back: float | None, alive_after: Any) -> list[dict[str, Any]]:
    expected_starts = sum(max(0, after - before) for before, after in zip([0] + sequence, sequence))
    stopped_through_stopping = [
        phase["name"]
        for phase, before in zip(phases[1:], phases)
        if phase["desired"] < before["desired"] and not any(row["counts"]["STOPPING"] for row in phase["trace"])
    ]
    checks = [
        {
            "name": "every phase settles on its goal",
            "ok": all(phase["settled"] for phase in phases),
            "detail": ", ".join(f"{p['name']}={p['desired']}: RUNNING {p['counts']['RUNNING']}" for p in phases),
        },
        {
            "name": "no attempt is requested beyond the goal changes",
            "ok": len(controller.starts) == expected_starts,
            "detail": f"requested {len(controller.starts)}, expected {expected_starts}",
        },
        {
            "name": "a scale-down waits in STOPPING until Ray confirms the death",
            "ok": not stopped_through_stopping,
            "detail": "skipped STOPPING in " + str(stopped_through_stopping) if stopped_through_stopping else "observed",
        },
        {
            "name": "a scale-down keeps an older attempt",
            "ok": all(
                set(i for i, s in p["instances"].items() if s == "RUNNING") <= set(p["active_before"])
                for p, before in zip(phases[1:], phases)
                if p["desired"] < before["desired"]
            ),
            "detail": "survivors were already active before the change",
        },
        {
            "name": "with goal 0 nothing is recreated",
            "ok": hold_new == 0,
            "detail": f"new attempts while holding: {hold_new}",
        },
        {
            "name": "attempt ids are never reused",
            "ok": len(set(controller.starts)) == len(controller.starts),
            "detail": f"requested {controller.starts}",
        },
        {
            "name": "the logical CPUs come back",
            "ok": cpus_back is not None,
            "detail": f"back after {cpus_back} s" if cpus_back is not None else f"not back in {RESOURCE_TIMEOUT_S} s",
        },
        {
            "name": "Ray's state API reports no ticker alive",
            "ok": alive_after == [],
            "detail": f"alive {alive_after}",
        },
    ]
    return checks


def run(args: argparse.Namespace) -> dict[str, Any]:
    sequence = _sequence(args.sequence)
    meta = start_cluster(cpus=args.cpus, address=args.address, labels=tuple(args.labels))
    meta.update(
        {
            "sequence": sequence,
            "cycle_seconds": args.cycle,
            "probe_timeout_s": args.probe_timeout,
            "max_missed_probes": args.max_missed_probes,
            "placement": args.placement,
        }
    )
    print(f"{PREFIX} ==== reconcile loop over {sequence} ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator(max_missed_probes=args.max_missed_probes)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)

    spec: dict[str, Any] = {
        "job_id": args.job_id,
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": args.interval_ms}},
        "replicas": sequence[0],
        "resources": {"cpu": args.cpu_per_instance},
    }
    if args.placement:
        spec["placement"] = {"node_label": args.placement}
    job_id = orchestrator.submit_job(spec)

    phases = [
        _run_phase(controller, orchestrator, job_id, _phase_name(index), desired, args)
        for index, desired in enumerate(sequence)
    ]

    starts_before = len(controller.starts)
    for index in range(1, args.hold_cycles + 1):
        time.sleep(args.cycle)
        _cycle(controller, orchestrator, job_id, "hold", index, sequence[-1])
    hold_new = len(controller.starts) - starts_before

    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S) if sequence[-1] == 0 else 0.0
    alive_after = alive_tickers() if sequence[-1] == 0 else []
    runtime.shutdown()

    return {
        "meta": meta,
        "job_id": job_id,
        "phases": phases,
        "starts": controller.starts,
        "kills": controller.kills,
        "checks": _checks(phases, controller, sequence, hold_new, cpus_back, alive_after),
        "events": orchestrator.events(),
    }


def main() -> int:
    args = build_parser().parse_args()
    try:
        results = run(args)
    finally:
        ray.shutdown()

    failed = print_checks(PREFIX, results["checks"])
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"{PREFIX} results written to {args.output}")
    if args.events is not None:
        args.events.parent.mkdir(parents=True, exist_ok=True)
        args.events.write_text(
            "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in results["events"]), encoding="utf-8"
        )
        print(f"{PREFIX} events written to {args.events}")
    if failed:
        print(f"{PREFIX} FAILED: {failed}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
