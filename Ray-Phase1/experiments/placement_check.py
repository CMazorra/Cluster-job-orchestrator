"""Placement constraint: an instance only runs on a node that declares its label.

The local node declares one label. Two jobs are submitted: one asks for that
label and should run; the other asks for a label no node declares and should
stay PENDING, with a single attempt, however many cycles pass. The constraint
must not be relaxed and the waiting attempt must not be duplicated.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/placement_check.py
    python experiments/placement_check.py --declared zone-a --missing zone-b --cycles 10
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

PREFIX = "[placement]"
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--declared", default="zone-a", help="label the local node declares")
    parser.add_argument("--missing", default="zone-b", help="label no node declares")
    parser.add_argument("--cpus", type=int, default=2, help="logical CPUs of the local cluster")
    parser.add_argument("--cycles", type=int, default=8, help="control cycles to run")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    return parser


def _spec(job_id: str, label: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": 100}},
        "replicas": 1,
        "resources": {"cpu": 1},
        "placement": {"node_label": label},
    }


def _states(orchestrator: Orchestrator, job_id: str) -> list[str]:
    return [i["state"] for i in orchestrator.get_job(job_id)["instances"]]


def run(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus, labels=(args.declared,))
    print(f"{PREFIX} ==== placement: node declares {args.declared!r}, nobody declares {args.missing!r} ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator()
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    placed = orchestrator.submit_job(_spec("placed", args.declared))
    unplaceable = orchestrator.submit_job(_spec("unplaceable", args.missing))

    for index in range(1, args.cycles + 1):
        for job_id in (placed, unplaceable):
            controller.step(job_id)
        print(
            f"{PREFIX} cycle {index}: placed {_states(orchestrator, placed)} | "
            f"unplaceable {_states(orchestrator, unplaceable)}"
        )
        time.sleep(args.cycle)

    placed_states = _states(orchestrator, placed)
    unplaceable_states = _states(orchestrator, unplaceable)
    placed_node = orchestrator.get_job(placed)["instances"][0]["node_id"]

    for job_id in (placed, unplaceable):
        orchestrator.stop_job(job_id)
    for _ in range(20):
        for job_id in (placed, unplaceable):
            controller.step(job_id)
        if not orchestrator.active_instances(placed) and not orchestrator.active_instances(unplaceable):
            break
        time.sleep(args.cycle)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    checks = [
        {
            "name": "the job asking for a declared label runs",
            "ok": placed_states == ["RUNNING"],
            "detail": f"states {placed_states} on node {placed_node}",
        },
        {
            "name": "the job asking for a missing label stays PENDING",
            "ok": unplaceable_states == ["PENDING"],
            "detail": f"states {unplaceable_states} after {args.cycles} cycles",
        },
        {
            "name": "the waiting attempt is not duplicated",
            "ok": len(unplaceable_states) == 1,
            "detail": f"{len(unplaceable_states)} attempt(s)",
        },
        {
            "name": "both jobs stop without leftovers",
            "ok": not orchestrator.active_instances(placed) and not orchestrator.active_instances(unplaceable),
            "detail": f"placed {_states(orchestrator, placed)}, unplaceable {_states(orchestrator, unplaceable)}",
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
    return {"meta": meta, "checks": checks, "events": orchestrator.events()}


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
    if failed:
        print(f"{PREFIX} FAILED: {failed}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
