"""Observe remote calls and processes with Ray.

What this script shows:

* Submitting work (``.remote()``) and getting its result (``ray.get()``) are
  separate steps. Submitting two tasks before waiting does not cost the same
  as submitting and waiting one at a time.
* A remote call runs in another process: the actor PID differs from the
  driver PID.
* An actor's local state lives in its own process: it persists across calls
  and the driver only sees it through a method call.
* Asking for more work than the logical CPUs allow is not a failure: the
  extra task waits for a free CPU.

This is not a domain test: it is run by hand and its output is evidence, not
an assertion.

Usage (inside WSL2, with the Ray virtual environment):

    python experiments/watch.py --part all
    python experiments/watch.py --part a --output /tmp/watch.json

``--cpu`` and ``--duration`` allow repeating the measurement without editing
the code. Task timestamps come from each worker's clock, so the part C
timeline is only meaningful when all workers run on the same machine.
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

import ray

PREFIX = "[watch]"

DEFAULT_CPUS = 2
DEFAULT_DURATION_S = 2.0
PARTS = ("a", "b", "c")


# --- Helpers ------------------------------------------------------------------


def _node_id() -> str:
    """Node where the current call runs."""
    return str(ray.get_runtime_context().get_node_id())


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _pids(results: list[dict[str, Any]]) -> str:
    return ", ".join(str(r["pid"]) for r in results)


def _list_alive_actors() -> list[dict[str, Any]]:
    """Alive actors according to Ray's state API.

    This is an independent source: the actor reports its own PID from inside,
    while the state API reports which PID and node it has. If both agree, the
    observation is solid.

    The state API needs ``ray[default]``. If it is not available, the error is
    recorded and the run continues, because the other measurements do not
    depend on it.
    """
    try:
        from ray.util.state import list_actors

        raw = list_actors(filters=[("state", "=", "ALIVE")])
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"state API not available: {exc!r}"}]

    return [
        {
            "actor_id": str(getattr(actor, "actor_id", "")),
            "class_name": str(getattr(actor, "class_name", "")),
            "node_id": str(getattr(actor, "node_id", "")),
            "pid": getattr(actor, "pid", None),
            "state": str(getattr(actor, "state", "")),
        }
        for actor in raw
    ]


# --- Remote workload ----------------------------------------------------------


@ray.remote(num_cpus=1)
def sleep_task(seconds: float) -> dict[str, Any]:
    """Independent task: sleeps and reports which process ran it.

    Returning the PID proves the call ran in another process.
    """
    start = time.time()
    time.sleep(seconds)
    return {
        "pid": os.getpid(),
        "node_id": _node_id(),
        "start": start,
        "end": time.time(),
    }


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class Counter:
    """Minimal actor with local state.

    ``max_restarts=0`` and ``max_task_retries=0`` are Ray's defaults, written
    on purpose: Ray does not restart the actor, so replacing it is the
    controller's decision. If Ray restarted it, the loss of local state would
    be hidden.
    """

    def __init__(self) -> None:
        self.value = 0

    def increment(self) -> int:
        self.value += 1
        return self.value

    def status(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "pid": os.getpid(),
            "node_id": _node_id(),
        }


# --- Parts --------------------------------------------------------------------


def part_a(duration: float) -> dict[str, Any]:
    """Submit and wait one at a time, or submit both before waiting."""
    print(f"{PREFIX} --- Part A: submit and wait ---")

    # Warm-up: start the worker processes first, so their startup cost is not
    # charged only to the first mode.
    ray.get([sleep_task.remote(0) for _ in range(2)])

    t0 = time.perf_counter()
    first = ray.get(sleep_task.remote(duration))
    second = ray.get(sleep_task.remote(duration))
    a1_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    refs = [sleep_task.remote(duration) for _ in range(2)]
    a2 = ray.get(refs)
    a2_s = time.perf_counter() - t0

    print(f"{PREFIX} A1  submit and wait one at a time : 2 ray.get, {a1_s:.3f} s")
    print(f"{PREFIX}     pids: [{_pids([first, second])}]")
    print(f"{PREFIX} A2  submit both before waiting    : 1 ray.get, {a2_s:.3f} s")
    print(f"{PREFIX}     pids: [{_pids(a2)}]")

    table = _markdown_table(
        ["mode", "ray.get calls", "total time (s)", "pids"],
        [
            ["A1 submit and wait one at a time", "2", f"{a1_s:.3f}", _pids([first, second])],
            ["A2 submit both before waiting", "1", f"{a2_s:.3f}", _pids(a2)],
        ],
    )
    print(f"{PREFIX} {table}")

    return {
        "a1": {"seconds": a1_s, "ray_get_calls": 2, "results": [first, second]},
        "a2": {"seconds": a2_s, "ray_get_calls": 1, "results": a2},
    }


def part_b() -> dict[str, Any]:
    """Actor: remote call, local state, and the node where Ray placed it."""
    print(f"{PREFIX} --- Part B: actor and local state ---")

    driver_pid = os.getpid()
    driver_node = _node_id()

    actor = Counter.remote()
    try:
        initial = ray.get(actor.status.remote())
        after_one = ray.get(actor.increment.remote())
        after_two = ray.get(actor.increment.remote())
        final = ray.get(actor.status.remote())
        alive = _list_alive_actors()
    finally:
        ray.kill(actor)

    actor_pid = int(final["pid"])
    same_process = actor_pid == driver_pid
    print(f"{PREFIX} driver pid : {driver_pid}")
    print(
        f"{PREFIX} actor pid  : {actor_pid}  "
        f"({'SAME process: unexpected' if same_process else 'different: the call is remote'})"
    )
    print(f"{PREFIX} driver node_id : {driver_node}")
    print(f"{PREFIX} actor node_id  : {final['node_id']}")
    print(f"{PREFIX} initial state  : value={initial['value']}")
    print(f"{PREFIX} after 2 increments: value={final['value']} (actor local state)")
    print(f"{PREFIX} the driver cannot read that state without calling status()")
    print(f"{PREFIX} alive actors according to Ray's state API:")
    for entry in alive:
        if "error" in entry:
            print(f"{PREFIX}   {entry['error']}")
            continue
        print(
            f"{PREFIX}   {entry['class_name']} actor_id={entry['actor_id']} "
            f"pid={entry['pid']} node_id={entry['node_id']} state={entry['state']}"
        )

    return {
        "driver_pid": driver_pid,
        "actor_pid": actor_pid,
        "same_process": same_process,
        "driver_node": driver_node,
        "actor_node": final["node_id"],
        "initial_value": initial["value"],
        "value_after_two_increments": final["value"],
        "increments": [after_one, after_two],
        "alive_actors": alive,
    }


def part_c(duration: float) -> dict[str, Any]:
    """Submit one task more than the logical CPUs allow: the extra one waits."""
    print(f"{PREFIX} --- Part C: concurrency versus resources ---")

    cpus = int(ray.cluster_resources().get("CPU", 0))
    count = cpus + 1

    submitted = time.time()
    t0 = time.perf_counter()
    refs = [sleep_task.remote(duration) for _ in range(count)]
    results = ray.get(refs)
    total_s = time.perf_counter() - t0

    ordered = sorted(results, key=lambda r: r["start"])
    print(f"{PREFIX} {count} tasks of 1 CPU with {cpus} logical CPUs")
    print(f"{PREFIX} total time: {total_s:.3f} s")
    print(f"{PREFIX} timeline (s since submission):")
    for i, r in enumerate(ordered, start=1):
        print(
            f"{PREFIX}   task {i}: "
            f"[{r['start'] - submitted:.3f} --> {r['end'] - submitted:.3f}]  pid {r['pid']}"
        )

    overlapping = sum(
        1 for previous, current in zip(ordered, ordered[1:]) if current["start"] < previous["end"]
    )
    # The last task to start should only begin once another one has finished.
    last_waited = ordered[-1]["start"] >= min(r["end"] for r in ordered[:-1])
    print(f"{PREFIX} consecutive overlapping pairs: {overlapping}")
    if last_waited:
        print(f"{PREFIX} the last task did not fail: it waited until a CPU was free")
    else:
        print(f"{PREFIX} the last task did NOT wait: more CPUs than declared were available")

    return {
        "logical_cpus": cpus,
        "tasks_submitted": count,
        "seconds": total_s,
        "tasks": ordered,
        "overlapping_pairs": overlapping,
        "last_task_waited": last_waited,
    }


# --- Startup ------------------------------------------------------------------


def _start(args: argparse.Namespace) -> dict[str, Any]:
    if args.address:
        ray.init(address=args.address, include_dashboard=True)
        mode = f"external cluster ({args.address})"
    else:
        ray.init(num_cpus=args.cpu, include_dashboard=True)
        mode = f"local cluster ({args.cpu} logical CPUs)"
    meta = {
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "mode": mode,
        "driver_pid": os.getpid(),
        "driver_node_id": _node_id(),
        "duration_s": args.duration,
        "cluster_resources": dict(ray.cluster_resources()),
        "available_resources": dict(ray.available_resources()),
    }
    print(f"{PREFIX} ==== observe remote calls and processes ====")
    print(f"{PREFIX} ray {meta['ray_version']} | python {meta['python_version']}")
    print(f"{PREFIX} {mode}")
    print(f"{PREFIX} driver pid {meta['driver_pid']} | node {meta['driver_node_id']}")
    print(f"{PREFIX} cluster resources   : {meta['cluster_resources']}")
    print(f"{PREFIX} available resources : {meta['available_resources']}")
    return meta


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--part", choices=(*PARTS, "all"), default="all", help="which part to run")
    parser.add_argument("--output", type=Path, default=None, help="JSON file for the raw results")
    parser.add_argument(
        "--cpu", type=int, default=DEFAULT_CPUS, help="logical CPUs of the local cluster"
    )
    parser.add_argument(
        "--address",
        default=None,
        help="connect to a running cluster (e.g. 'auto') instead of starting a local one",
    )
    parser.add_argument(
        "--duration", type=float, default=DEFAULT_DURATION_S, help="seconds per task"
    )
    args = parser.parse_args()

    results: dict[str, Any] = {}
    try:
        meta = _start(args)
        if args.part in ("a", "all"):
            results["part_a"] = part_a(args.duration)
        if args.part in ("b", "all"):
            results["part_b"] = part_b()
        if args.part in ("c", "all"):
            results["part_c"] = part_c(args.duration)
    finally:
        ray.shutdown()

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps({"meta": meta, **results}, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"{PREFIX} results written to {args.output}")

    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
