"""The same client operation arrives more than once (scenario F9).

A network or a client library may deliver one request twice: right away, or
late, after newer requests. The script sends duplicates of every client
operation and checks what the contract promises: repeating an operation has no
extra effect. It then sends a duplicate late, after a newer request, to measure
what the contract does not promise: operations carry no request ID or version,
so a late duplicate is just another request and the last one to arrive wins.

Duplicates are produced on the client side: in this phase the client calls the
domain in the same process. The instances are real Ray actors, and Ray's state
API is used to count them independently.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/duplicate_requests.py
    python experiments/duplicate_requests.py --output /tmp/f9.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from common import alive_tickers, print_checks, print_meta, settled, snapshot, start_cluster, wait_for_free_cpus

import ray

from orchestrator import Orchestrator, OrchestratorError, RuntimeController
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[duplicate]"
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cpus", type=int, default=4, help="logical CPUs of the local cluster")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds to settle after each change")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def spec_for(job_id: str, replicas: int, interval_ms: int = 100) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": interval_ms}},
        "replicas": replicas,
        "resources": {"cpu": 1},
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus)
    meta.update({"cycle_seconds": args.cycle, "probe_timeout_s": args.probe_timeout})
    print(f"{PREFIX} ==== duplicated client requests ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator()
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    job = "dup-demo"
    steps: list[dict[str, Any]] = []

    def settle(goal: int) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < args.deadline:
            controller.step(job)
            if settled(snapshot(orchestrator, job)[0], goal):
                return True
            time.sleep(args.cycle)
        return False

    def send(operation: str, *call_args: Any, copies: int = 1) -> list[Any]:
        answers = []
        for copy in range(copies):
            orchestrator.annotate("request_delivered", operation=operation, copy=copy + 1)
            try:
                answers.append(getattr(orchestrator, operation)(*call_args))
            except OrchestratorError as exc:
                answers.append(exc.code.value)
        return answers

    def record(name: str, answers: list[Any], goal: int) -> None:
        before = len([e for e in orchestrator.events() if e["event"] == "instance_requested"])
        ok = settle(goal)
        alive = alive_tickers()
        view = orchestrator.get_job(job)
        steps.append(
            {
                "step": name,
                "answers": answers,
                "desired_replicas": view["desired_replicas"],
                "running": view["observed"]["running"],
                "alive_in_ray": None if alive is None else len(alive),
                "settled": ok,
                "attempts_requested_so_far": len([e for e in orchestrator.events() if e["event"] == "instance_requested"]),
                "attempts_requested_by_step": len([e for e in orchestrator.events() if e["event"] == "instance_requested"]) - before,
            }
        )
        s = steps[-1]
        print(
            f"{PREFIX} {name}: answers {answers}, goal {s['desired_replicas']}, running {s['running']}, "
            f"alive in Ray {s['alive_in_ray']}, attempts requested so far {s['attempts_requested_so_far']}"
        )

    # Immediate duplicates: each request arrives twice in a row.
    record("submit_job twice", send("submit_job", spec_for(job, 2), copies=2), 2)
    record("set_replicas(3) twice", send("set_replicas", job, 3, copies=2), 3)
    record("same ID, different spec", send("submit_job", spec_for(job, 2, interval_ms=200)), 3)

    # A late duplicate: set_replicas(4), then set_replicas(1), then the copy of the first.
    late = send("set_replicas", job, 4) + send("set_replicas", job, 1)
    record("set_replicas(4) then set_replicas(1)", late, 1)
    record("late copy of set_replicas(4) arrives", send("set_replicas", job, 4), 4)

    record("stop_job twice", send("stop_job", job, copies=2), 0)
    # A late copy of the original submit after the stop: the job must stay stopped.
    record("late copy of the original submit after the stop", send("submit_job", spec_for(job, 2)), 0)

    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    events = orchestrator.events()
    submitted = [e for e in events if e["event"] == "job_submitted"]
    replicas_set = [e for e in events if e["event"] == "replicas_set"]
    by_name = {s["step"]: s for s in steps}
    reverted = by_name["late copy of set_replicas(4) arrives"]

    checks = [
        {
            "name": "a duplicated submit registers one job and starts its replicas once",
            "ok": len(submitted) == 1
            and by_name["submit_job twice"]["answers"] == [job, job]
            and by_name["submit_job twice"]["attempts_requested_by_step"] == 2,
            "detail": f"job_submitted {len(submitted)}, attempts {by_name['submit_job twice']['attempts_requested_by_step']}",
        },
        {
            "name": "a duplicated set_replicas changes the goal once and starts no extra attempt",
            "ok": by_name["set_replicas(3) twice"]["attempts_requested_by_step"] == 1
            and sum(1 for e in replicas_set if e["replicas"] == 3) == 1,
            "detail": f"attempts {by_name['set_replicas(3) twice']['attempts_requested_by_step']}",
        },
        {
            "name": "a different spec under the same ID is rejected and changes nothing",
            "ok": by_name["same ID, different spec"]["answers"] == ["JOB_ALREADY_EXISTS"]
            and by_name["same ID, different spec"]["attempts_requested_by_step"] == 0,
            "detail": str(by_name["same ID, different spec"]["answers"]),
        },
        {
            "name": "after every step the goal is met and Ray runs exactly that many actors",
            "ok": all(s["settled"] and s["running"] == s["desired_replicas"] == s["alive_in_ray"] for s in steps),
            "detail": str([(s["desired_replicas"], s["running"], s["alive_in_ray"]) for s in steps]),
        },
        {
            "name": "a late duplicate of set_replicas undoes the newer goal (no request IDs: the last arrival wins)",
            "ok": reverted["desired_replicas"] == 4 and reverted["attempts_requested_by_step"] == 3,
            "detail": f"goal {reverted['desired_replicas']}, attempts started again {reverted['attempts_requested_by_step']}",
        },
        {
            "name": "a late duplicate of the original submit does not restart a stopped job",
            "ok": by_name["late copy of the original submit after the stop"]["desired_replicas"] == 0
            and by_name["late copy of the original submit after the stop"]["attempts_requested_by_step"] == 0,
            "detail": f"goal {by_name['late copy of the original submit after the stop']['desired_replicas']}",
        },
        {
            "name": "the job stops cleanly",
            "ok": cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    return {"meta": meta, "steps": steps, "checks": checks, "events": events}


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
