"""One ticker workload, queryable and terminable, in two processes.

What this script shows:

* two instances of the same workload live in two different processes: their
  PIDs differ from each other and from the driver's;
* both advance their counter at the same time, so their activity windows
  overlap;
* both can be terminated: Ray confirms each death and the logical CPUs come back.

It is an instrument, not a system test: it prints what it observed and can
write the raw numbers to a JSON file. It does not use the domain.

The actors are requested first and waited for afterwards: asking for the first
instance and blocking on it before asking for the second one would serialize
the start-up and hide the concurrency.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/two_instances.py --output /tmp/two_instances.json
    python experiments/two_instances.py --instances 3 --cpus 3
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

from orchestrator.runtime import DEAD, HEALTHY, InstanceSpec
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[two-instances]"
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--instances", type=int, default=2, help="instances to ask for")
    parser.add_argument("--cpus", type=int, default=2, help="logical CPUs of the local cluster")
    parser.add_argument("--cpu-per-instance", type=float, default=1.0, help="logical CPU each instance reserves")
    parser.add_argument("--interval-ms", type=int, default=250, help="counter period")
    parser.add_argument("--samples", type=int, default=3, help="progress samples")
    parser.add_argument("--sample-seconds", type=float, default=1.0, help="seconds between samples")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S, help="deadline of one probe round")
    parser.add_argument("--startup-timeout", type=float, default=20.0, help="seconds to wait for the start-up")
    parser.add_argument("--address", default=None, help="connect to a running cluster (e.g. 'auto')")
    parser.add_argument("--output", type=Path, default=None, help="JSON file for the raw results")
    return parser


def _wait_for(runtime: RayRuntime, instance_ids: list[str], outcome: str, timeout: float) -> dict[str, Any]:
    """Probes in rounds until every instance reports ``outcome`` or the time runs out."""
    deadline = time.monotonic() + timeout
    while True:
        observations = runtime.probe_all(instance_ids)
        if all(o.outcome == outcome for o in observations.values()) or time.monotonic() >= deadline:
            return observations
        time.sleep(0.1)


def _overlap_seconds(windows: dict[str, dict[str, Any]]) -> tuple[float, str | None]:
    """Seconds during which every instance was active, or why it cannot be told.

    The windows come from the clock of each actor, so the overlap is only
    meaningful when every instance reported one and all run on one machine.
    """
    if not windows:
        return 0.0, "there are no windows"
    for window in windows.values():
        if window["started_at"] is None or window["last_tick_at"] is None:
            return 0.0, "an instance never reported its activity window"
    start = max(window["started_at"] for window in windows.values())
    end = min(window["last_tick_at"] for window in windows.values())
    return round(max(0.0, end - start), 3), None


def run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus, address=args.address)
    print(f"{PREFIX} ==== ticker workload in {args.instances} processes ====")
    print_meta(PREFIX, meta)

    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    instance_ids = [f"two/{n}" for n in range(1, args.instances + 1)]
    spec = InstanceSpec(interval_ms=args.interval_ms, cpu=args.cpu_per_instance)

    requested_at = time.monotonic()
    for instance_id in instance_ids:
        runtime.start(instance_id, spec)
    print(f"{PREFIX} {len(instance_ids)} instances requested, none of them waited for yet")

    first = _wait_for(runtime, instance_ids, HEALTHY, args.startup_timeout)
    healthy_after = round(time.monotonic() - requested_at, 3)
    for instance_id, observation in first.items():
        print(
            f"{PREFIX} {instance_id}: {observation.outcome} | pid {observation.pid} | "
            f"node {observation.node_id} | progress {observation.progress}"
        )
    print(f"{PREFIX} all answered within {healthy_after} s of the request")

    samples: list[dict[str, Any]] = []
    last = first
    for index in range(args.samples):
        time.sleep(args.sample_seconds)
        last = runtime.probe_all(instance_ids)
        samples.append({"t": time.monotonic() - requested_at, "progress": {i: o.progress for i, o in last.items()}})
        rendered = ", ".join(f"{i}={o.progress}" for i, o in last.items())
        print(f"{PREFIX} sample {index + 1}/{args.samples} at t+{samples[-1]['t']:.3f}s: {rendered}")

    alive_before = alive_tickers()
    windows = {i: {"started_at": o.started_at, "last_tick_at": o.last_tick_at} for i, o in last.items()}
    overlap, overlap_note = _overlap_seconds(windows)

    terminations = [runtime.terminate(instance_id) for instance_id in instance_ids]
    confirmed = _wait_for(runtime, instance_ids, DEAD, args.startup_timeout)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()

    pids = [last[i].pid for i in instance_ids]
    checks = [
        {
            "name": "each instance runs in its own process",
            "ok": None not in pids and len(set(pids)) == len(pids),
            "detail": f"pids {pids}",
        },
        {
            "name": "no instance runs in the driver process",
            "ok": meta["driver_pid"] not in pids,
            "detail": f"driver {meta['driver_pid']}",
        },
        {
            "name": "Ray's state API sees the same processes",
            "ok": alive_before is not None and sorted(a["pid"] for a in alive_before) == sorted(pids),
            "detail": f"state API pids {None if alive_before is None else [a['pid'] for a in alive_before]}",
        },
        {
            "name": "progress advances in every instance",
            "ok": all(
                first[i].progress is not None and last[i].progress is not None and last[i].progress > first[i].progress
                for i in instance_ids
            ),
            "detail": ", ".join(f"{i}: {first[i].progress} -> {last[i].progress}" for i in instance_ids),
        },
        {
            "name": "the activity windows overlap",
            "ok": overlap > 0,
            "detail": f"{overlap} s of overlap" + (f" ({overlap_note})" if overlap_note else ""),
        },
        {
            "name": "Ray confirms every death after the kill request",
            "ok": all(o.outcome == DEAD for o in confirmed.values()),
            "detail": ", ".join(f"{i}: {o.outcome} ({o.detail})" for i, o in confirmed.items()),
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
    return {
        "meta": meta,
        "samples": samples,
        "instances": {i: last[i]._asdict() for i in instance_ids},
        "activity_windows": windows,
        "overlap_seconds": overlap,
        "terminations": [t._asdict() for t in terminations],
        "confirmations": {i: o._asdict() for i, o in confirmed.items()},
        "checks": checks,
    }


def main() -> int:
    args = build_parser().parse_args()
    try:
        results = run_experiment(args)
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
