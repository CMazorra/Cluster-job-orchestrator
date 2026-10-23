"""Helpers shared by the experiments: cluster start-up and runtime-side evidence.

Run the experiments from the Ray-Phase1 folder, inside WSL2, with the Ray
virtual environment.
"""

from __future__ import annotations

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

# Capacity of a node-label resource. Each instance takes one unit, so a label
# restricts placement without limiting how many instances fit.
LABEL_CAPACITY = 1000

STATES = ("PENDING", "RUNNING", "UNKNOWN", "STOPPING", "STOPPED", "FAILED")
LETTER = {"PENDING": "P", "RUNNING": "R", "UNKNOWN": "U", "STOPPING": "S", "STOPPED": "T", "FAILED": "F"}


def snapshot(orchestrator: Any, job_id: str) -> tuple[dict[str, int], dict[str, str]]:
    """How many attempts are in each state, and the state of every attempt."""
    instances = {i["instance_id"]: i["state"] for i in orchestrator.get_job(job_id)["instances"]}
    counts = {state: sum(1 for s in instances.values() if s == state) for state in STATES}
    return counts, instances


def settled(counts: dict[str, int], desired: int) -> bool:
    """The goal is met and nothing is still moving: no pending, no doubt, no stop in flight."""
    return counts["RUNNING"] == desired and counts["PENDING"] == counts["UNKNOWN"] == counts["STOPPING"] == 0


def start_cluster(*, cpus: int, address: str | None = None, labels: tuple[str, ...] = ()) -> dict[str, Any]:
    """Starts a local cluster, or connects to a running one, and describes it."""
    if address:
        ray.init(address=address, include_dashboard=True)
        mode = f"external cluster ({address})"
    else:
        # Workers import ray_runtime from the driver's working directory, so the
        # driver must run from ROOT. A runtime_env PYTHONPATH was observed to keep
        # the CPUs of killed actors reserved, and a PYTHONPATH on the Windows
        # drive slowed the dashboard start-up past Ray's timeout.
        if Path.cwd().resolve() != ROOT:
            raise SystemExit(f"run the experiments from {ROOT}: Ray workers import ray_runtime from there")
        ray.init(
            num_cpus=cpus,
            resources={label: LABEL_CAPACITY for label in labels},
            include_dashboard=True,
        )
        mode = f"local cluster ({cpus} logical CPUs, labels {list(labels)})"
    return {
        "mode": mode,
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "driver_pid": os.getpid(),
        "driver_node_id": str(ray.get_runtime_context().get_node_id()),
        "cluster_resources": dict(ray.cluster_resources()),
    }


def print_meta(prefix: str, meta: dict[str, Any]) -> None:
    print(f"{prefix} ray {meta['ray_version']} | python {meta['python_version']}")
    print(f"{prefix} {meta['mode']}")
    print(f"{prefix} driver pid {meta['driver_pid']} | node {meta['driver_node_id']}")
    print(f"{prefix} cluster resources: {meta['cluster_resources']}")


def alive_tickers() -> list[dict[str, Any]] | None:
    """Ticker actors that Ray's state API reports as alive; None if the API is unavailable.

    This is an independent source: it asks Ray, not our own bookkeeping.
    """
    try:
        from ray.util.state import list_actors

        actors = list_actors(filters=[("state", "=", "ALIVE"), ("class_name", "=", "Ticker")])
    except Exception:  # noqa: BLE001
        return None
    return [
        {
            "actor_id": str(getattr(actor, "actor_id", "")),
            "node_id": str(getattr(actor, "node_id", "")),
            "pid": getattr(actor, "pid", None),
        }
        for actor in actors
    ]


def wait_for_free_cpus(expected: float, timeout: float) -> float | None:
    """Seconds until Ray reports at least ``expected`` free logical CPUs; None if it never did."""
    started = time.monotonic()
    while True:
        if ray.available_resources().get("CPU", 0) >= expected:
            return round(time.monotonic() - started, 3)
        if time.monotonic() - started >= timeout:
            return None
        time.sleep(0.1)


def print_checks(prefix: str, checks: list[dict[str, Any]]) -> list[str]:
    print(f"{prefix} --- checks ---")
    for check in checks:
        print(f"{prefix} [{'OK' if check['ok'] else 'FAIL'}] {check['name']}: {check['detail']}")
    return [check["name"] for check in checks if not check["ok"]]
