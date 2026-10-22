"""Day 2: one ticker workload, queryable and terminable, in two processes.

What this script shows:

* two instances of the same workload live in two different processes: their
  PIDs differ from each other and from the driver's;
* both advance their counter at the same time, so their activity windows
  overlap;
* both can be terminated, and the logical CPUs come back.

It is an instrument, not a system test: it prints what it observed and writes
the raw numbers to a JSON file. It does not import the domain.

The actors are requested first and waited for afterwards: asking for the first
instance and blocking on it before asking for the second one would serialize
the start up and hide the concurrency.

Usage (inside WSL2, with the Ray virtual environment):

    python experiments/day2_ticker.py --output /tmp/dia2.json
    python experiments/day2_ticker.py --instances 3 --cpus 3
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ray

from ray_runtime import HEALTHY, InstanceSpec, RayRuntime

PREFIX = "[dia2]"

DEFAULT_INSTANCES = 2
DEFAULT_CPUS = 2
DEFAULT_INTERVAL_MS = 250
DEFAULT_SAMPLES = 3
DEFAULT_SAMPLE_SECONDS = 1.0
DEFAULT_STARTUP_TIMEOUT_S = 20.0
DEFAULT_PROBE_TIMEOUT_S = 2.0
DEFAULT_RESOURCE_TIMEOUT_S = 5.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--instances", type=int, default=DEFAULT_INSTANCES, help="instances to ask for")
    parser.add_argument("--cpus", type=int, default=DEFAULT_CPUS, help="logical CPUs of the cluster")
    parser.add_argument(
        "--cpu-per-instance", type=float, default=1.0, help="logical CPU each instance reserves"
    )
    parser.add_argument("--interval-ms", type=int, default=DEFAULT_INTERVAL_MS, help="counter period")
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES, help="progress samples per instance")
    parser.add_argument(
        "--sample-seconds", type=float, default=DEFAULT_SAMPLE_SECONDS, help="seconds between samples"
    )
    parser.add_argument(
        "--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S, help="deadline of one query"
    )
    parser.add_argument(
        "--startup-timeout", type=float, default=DEFAULT_STARTUP_TIMEOUT_S, help="seconds to wait for the start up"
    )
    parser.add_argument(
        "--address", default=None, help="connect to a running cluster (e.g. 'auto') instead of starting one"
    )
    parser.add_argument("--output", type=Path, default=None, help="JSON file for the raw results")
    return parser


def _runtime_env() -> dict[str, Any]:
    """Lets the workers import our module, which lives outside the package.

    Ray serializes an actor class by reference when the class is in an
    importable module, so the worker has to be able to import ``ray_runtime``.
    Without this, the actor fails to start with an import error that has
    nothing to do with our logic.
    """
    current = os.environ.get("PYTHONPATH", "")
    python_path = os.pathsep.join(part for part in (current, str(ROOT)) if part)
    return {"env_vars": {"PYTHONPATH": python_path}}


def _start_cluster(args: argparse.Namespace) -> dict[str, Any]:
    if args.address:
        ray.init(address=args.address, include_dashboard=True, runtime_env=_runtime_env())
        mode = f"external cluster ({args.address})"
    else:
        ray.init(
            num_cpus=args.cpus,
            include_dashboard=True,
            runtime_env=_runtime_env(),
        )
        mode = f"local cluster ({args.cpus} logical CPUs)"
    meta = {
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "mode": mode,
        "driver_pid": os.getpid(),
        "driver_node_id": str(ray.get_runtime_context().get_node_id()),
        "cluster_resources": dict(ray.cluster_resources()),
        "initial_available_resources": dict(ray.available_resources()),
        "instances_requested": args.instances,
        "cpu_per_instance": args.cpu_per_instance,
        "interval_ms": args.interval_ms,
        "probe_timeout_s": args.probe_timeout,
    }
    print(f"{PREFIX} ==== two instances of the ticker workload ====")
    print(f"{PREFIX} ray {meta['ray_version']} | python {meta['python_version']}")
    if meta["ray_version"].endswith("-stub"):
        print(f"{PREFIX} WARNING: this is the double of ray, not the real one")
    print(f"{PREFIX} {mode}")
    print(f"{PREFIX} driver pid {meta['driver_pid']} | node {meta['driver_node_id']}")
    print(f"{PREFIX} cluster resources   : {meta['cluster_resources']}")
    print(f"{PREFIX} available resources : {meta['initial_available_resources']}")
    return meta


def _wait_until(predicate: Any, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return predicate()


def _first_observation(runtime: RayRuntime, instance_id: str, timeout: float) -> Any:
    deadline = time.time() + timeout
    while True:
        observation = runtime.probe(instance_id)
        if observation.outcome == HEALTHY or time.time() >= deadline:
            return observation
        time.sleep(0.1)


def _alive_actors() -> list[dict[str, Any]]:
    try:
        from ray.util.state import list_actors
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"state API not available: {exc!r}"}]
    return [
        {
            "actor_id": str(getattr(actor, "actor_id", "")),
            "class_name": str(getattr(actor, "class_name", "")),
            "name": str(getattr(actor, "name", "")),
            "node_id": str(getattr(actor, "node_id", "")),
            "pid": getattr(actor, "pid", None),
            "state": str(getattr(actor, "state", "")),
        }
        for actor in list_actors()
    ]


def _overlap_seconds(windows: dict[str, dict[str, Any]]) -> tuple[float, str | None]:
    """Seconds during which every instance was active, and why it cannot be told.

    The windows come from the clock of each actor, so the overlap is only
    meaningful when every instance reported one.
    """
    if not windows:
        return 0.0, "there are no windows"
    for window in windows.values():
        if window["started_at"] is None or window["last_tick_at"] is None:
            return 0.0, "an instance never reported its activity window"
    overlap_start = max(window["started_at"] for window in windows.values())
    overlap_end = min(window["last_tick_at"] for window in windows.values())
    return round(max(0.0, overlap_end - overlap_start), 3), None


def _progress_advanced(instances: list[dict[str, Any]]) -> bool:
    """True when every instance counted more than at the beginning."""
    return all(
        instance["progress_first"] is not None
        and instance["progress_last"] is not None
        and instance["progress_last"] > instance["progress_first"]
        for instance in instances
    )


def run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    """Runs the day 2 walkthrough and returns the raw numbers."""
    meta = _start_cluster(args)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    results: dict[str, Any] = {"meta": meta, "checks": [], "instances": []}

    instance_ids = [f"dia2/{n}" for n in range(1, args.instances + 1)]
    spec = InstanceSpec(interval_ms=args.interval_ms, cpu=args.cpu_per_instance)

    requested_at = time.time()
    request_started = time.perf_counter()
    for instance_id in instance_ids:
        runtime.start(instance_id, spec)
    meta["request_seconds"] = round(time.perf_counter() - request_started, 6)
    meta["requested_before_waiting"] = True
    print(f"{PREFIX} {len(instance_ids)} instances requested, none of them waited for yet")

    first = {}
    for instance_id in instance_ids:
        first[instance_id] = _first_observation(runtime, instance_id, args.startup_timeout)
        observation = first[instance_id]
        print(
            f"{PREFIX} {instance_id}: pid {observation.pid} | node {observation.node_id} "
            f"| actor {observation.actor_id} | progress {observation.progress}"
        )

    samples: list[dict[str, Any]] = []
    for index in range(args.samples):
        if index:
            time.sleep(args.sample_seconds)
        stamp = time.time()
        row: dict[str, Any] = {"t": stamp, "progress": {}, "outcome": {}}
        for instance_id in instance_ids:
            observation = runtime.probe(instance_id)
            row["progress"][instance_id] = observation.progress
            row["outcome"][instance_id] = observation.outcome
        samples.append(row)
        rendered = ", ".join(
            f"{instance_id}={row['progress'][instance_id]}" for instance_id in instance_ids
        )
        print(f"{PREFIX} sample {index + 1}/{args.samples} at t+{stamp - requested_at:.3f}s: {rendered}")

    last = {instance_id: runtime.probe(instance_id) for instance_id in instance_ids}

    results["samples"] = samples
    results["instances"] = [
        {
            "instance_id": instance_id,
            "pid": last[instance_id].pid,
            "node_id": last[instance_id].node_id,
            "actor_id": last[instance_id].actor_id,
            "progress_first": first[instance_id].progress,
            "progress_last": last[instance_id].progress,
            "started_at": last[instance_id].started_at,
            "last_tick_at": last[instance_id].last_tick_at,
        }
        for instance_id in instance_ids
    ]
    results["actors_alive"] = _alive_actors()

    windows = {
        instance_id: {
            "started_at": last[instance_id].started_at,
            "last_tick_at": last[instance_id].last_tick_at,
        }
        for instance_id in instance_ids
    }
    results["activity_windows"] = windows
    results["overlap_seconds"], overlap_note = _overlap_seconds(windows)
    if overlap_note is not None:
        results["overlap_note"] = overlap_note

    terminations = [runtime.terminate(instance_id) for instance_id in instance_ids]
    terminations += runtime.shutdown()
    results["terminations"] = [termination._asdict() for termination in terminations]

    cpu_returned = _wait_until(
        lambda: ray.available_resources().get("CPU", 0)
        >= meta["cluster_resources"].get("CPU", 0),
        DEFAULT_RESOURCE_TIMEOUT_S,
    )
    results["available_resources_after"] = dict(ray.available_resources())
    results["actors_alive_after"] = _alive_actors()

    pids = [instance["pid"] for instance in results["instances"]]
    checks = [
        {
            "name": "cada instancia corre en un proceso distinto",
            "ok": len(set(pids)) == len(pids) and None not in pids,
            "detail": f"pids {pids}, driver {meta['driver_pid']}",
        },
        {
            "name": "ninguna instancia corre en el proceso del driver",
            "ok": meta["driver_pid"] not in pids,
            "detail": f"driver {meta['driver_pid']}",
        },
        {
            "name": "el progreso avanza en todas las instancias",
            "ok": _progress_advanced(results["instances"]),
            "detail": ", ".join(
                f"{instance['instance_id']}: {instance['progress_first']} -> {instance['progress_last']}"
                for instance in results["instances"]
            ),
        },
        {
            "name": "las ventanas de actividad se solapan",
            "ok": results["overlap_seconds"] > 0,
            "detail": f"{results['overlap_seconds']} s of overlap",
        },
        {
            "name": "todos los procesos se terminan",
            "ok": all(termination.outcome == "terminated" for termination in terminations),
            "detail": ", ".join(f"{t.instance_id}: {t.outcome}" for t in terminations),
        },
        {
            "name": "los recursos lógicos vuelven",
            "ok": bool(cpu_returned),
            "detail": f"available {results['available_resources_after']}",
        },
    ]
    results["checks"] = checks

    print(f"{PREFIX} --- comprobaciones ---")
    for check in checks:
        print(f"{PREFIX} [{'OK' if check['ok'] else 'FAIL'}] {check['name']}: {check['detail']}")
    print(f"{PREFIX} ventanas de actividad (solape {results['overlap_seconds']} s):")
    for instance_id, window in windows.items():
        if window["started_at"] is None or window["last_tick_at"] is None:
            print(f"{PREFIX}   {instance_id}: [ sin ventana de actividad ]")
            continue
        print(
            f"{PREFIX}   {instance_id}: [{window['started_at']:.3f} --> {window['last_tick_at']:.3f}]"
        )
    return results


def main() -> int:
    args = build_parser().parse_args()
    try:
        results = run_experiment(args)
    finally:
        ray.shutdown()

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"{PREFIX} results written to {args.output}")

    failed = [check["name"] for check in results["checks"] if not check["ok"]]
    if failed:
        print(f"{PREFIX} FAILED: {failed}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
