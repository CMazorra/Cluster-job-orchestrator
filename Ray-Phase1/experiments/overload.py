"""Many more replicas than the cluster can hold (scenario F16).

A job asks for far more replicas than there are CPUs. The cluster runs as many
as fit; the rest must wait as PENDING attempts, one per missing replica, with
no growth however long the overload lasts. While overloaded, one running
process crashes, and finally the goal drops back to what fits.

The script measures the cost of the overload for the controller: every cycle
probes the pending attempts too, and they never answer until they are placed.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/overload.py
    python experiments/overload.py --replicas 200 --output /tmp/f16.json
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

from common import alive_tickers, print_checks, print_meta, settled, snapshot, start_cluster, wait_for_free_cpus

import ray

from orchestrator import Orchestrator, RuntimeController
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[overload]"
RESOURCE_TIMEOUT_S = 60.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--replicas", type=int, default=40, help="replicas asked while overloaded")
    parser.add_argument("--fits", type=int, default=2, help="replicas before and after the overload")
    parser.add_argument("--cpus", type=int, default=4, help="logical CPUs of the local cluster")
    parser.add_argument("--seconds", type=float, default=30.0, help="duration of the overload")
    parser.add_argument("--baseline-cycles", type=int, default=10, help="cycles measured before the overload")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=90.0, help="seconds to settle after each change")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def _summary(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    return {
        "n": len(values),
        "median": round(statistics.median(values), 3),
        "max": round(max(values), 3),
        "min": round(min(values), 3),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus)
    meta.update(
        {
            "replicas": args.replicas,
            "fits": args.fits,
            "overload_seconds": args.seconds,
            "cycle_seconds": args.cycle,
            "probe_timeout_s": args.probe_timeout,
        }
    )
    print(f"{PREFIX} ==== {args.replicas} replicas on {args.cpus} CPUs ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator()
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    job_id = orchestrator.submit_job(
        {
            "job_id": "overload-demo",
            "workload": {"entrypoint": "ticker", "args": {"interval_ms": 100}},
            "replicas": args.fits,
            "resources": {"cpu": 1},
        }
    )

    def timed_step() -> dict[str, Any]:
        started = time.monotonic()
        controller.observe(job_id)
        observed = time.monotonic()
        actions = controller.act(job_id)
        done = time.monotonic()
        counts, _ = snapshot(orchestrator, job_id)
        return {
            "observe_s": observed - started,
            "act_s": done - observed,
            "starts": len(actions.start),
            "kills": len(actions.kill),
            "attempts": len(orchestrator.get_job(job_id)["instances"]),
            **counts,
        }

    def until(goal: int) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < args.deadline:
            timed_step()
            if settled(snapshot(orchestrator, job_id)[0], goal):
                return True
            time.sleep(args.cycle)
        return False

    until(args.fits)
    baseline = []
    for _ in range(args.baseline_cycles):
        baseline.append(timed_step())
        time.sleep(args.cycle)

    orchestrator.annotate("overload_started", replicas=args.replicas)
    orchestrator.set_replicas(job_id, args.replicas)
    overload = []
    started = time.monotonic()
    while time.monotonic() - started < args.seconds:
        overload.append(timed_step())
        time.sleep(args.cycle)
    attempts_before_crash = overload[-1]["attempts"]
    running_before_crash = overload[-1]["RUNNING"]

    victim = next(i for i in orchestrator.get_job(job_id)["instances"] if i["state"] == "RUNNING")["instance_id"]
    orchestrator.annotate("crash_injected", instance_id=victim)
    runtime.inject_fault(victim, "crash")
    crash_m = orchestrator.events()[-1]["m"]
    after_crash = []
    started = time.monotonic()
    while time.monotonic() - started < 15.0:
        after_crash.append(timed_step())
        time.sleep(args.cycle)

    running_ids = {i["instance_id"] for i in orchestrator.get_job(job_id)["instances"] if i["state"] == "RUNNING"}
    orchestrator.annotate("overload_ended", replicas=args.fits)
    orchestrator.set_replicas(job_id, args.fits)
    shrink_started = time.monotonic()
    shrunk = until(args.fits)
    shrink_s = round(time.monotonic() - shrink_started, 3)
    survivors = {i["instance_id"] for i in orchestrator.get_job(job_id)["instances"] if i["state"] == "RUNNING"}
    after = []
    for _ in range(args.baseline_cycles):
        after.append(timed_step())
        time.sleep(args.cycle)

    orchestrator.stop_job(job_id)
    stopped = until(0)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    events = orchestrator.events()
    failed = next((e for e in events if e.get("instance_id") == victim and e.get("state") == "FAILED"), None)
    replacement = next((e for e in events if e["event"] == "instance_requested" and e.get("replaces") == victim), None)
    took_cpu = next(
        (
            e
            for e in events
            if e["m"] > crash_m and e["event"] == "instance_observed" and e.get("from_state") == "PENDING" and e.get("state") == "RUNNING"
        ),
        None,
    )
    unknown_while_pending = [
        e for e in events if e["event"] == "instance_observed" and e.get("from_state") == "PENDING" and e.get("state") == "UNKNOWN"
    ]
    rows = overload + after_crash
    times = {
        "baseline_observe_s": _summary([r["observe_s"] for r in baseline]),
        "overload_observe_s": _summary([r["observe_s"] for r in overload]),
        "overload_act_s": _summary([r["act_s"] for r in overload]),
        "after_observe_s": _summary([r["observe_s"] for r in after]),
    }
    detection_s = None if failed is None else round(failed["m"] - crash_m, 3)
    print(f"{PREFIX} probe round: baseline {times['baseline_observe_s']}, overloaded {times['overload_observe_s']}")
    print(f"{PREFIX} decide and act while overloaded: {times['overload_act_s']}")
    print(f"{PREFIX} attempts while overloaded {sorted({r['attempts'] for r in overload})}, running {sorted({r['RUNNING'] for r in overload})}")
    print(
        f"{PREFIX} crash of {victim}: failed after {detection_s} s, replacement "
        f"{None if replacement is None else replacement['instance_id']}, "
        f"freed CPU taken by {None if took_cpu is None else took_cpu['instance_id']}"
    )
    print(f"{PREFIX} back to {args.fits} in {shrink_s} s; survivors {sorted(survivors)}")

    checks = [
        {
            "name": "the cluster runs as many replicas as fit, and the rest wait as PENDING",
            "ok": running_before_crash == args.cpus and overload[-1]["PENDING"] == args.replicas - args.cpus,
            "detail": f"running {running_before_crash}, pending {overload[-1]['PENDING']}",
        },
        {
            "name": "the pending attempts do not grow however long the overload lasts",
            "ok": attempts_before_crash == args.replicas and all(r["attempts"] <= args.replicas for r in overload),
            "detail": f"attempts {sorted({r['attempts'] for r in overload})} over {len(overload)} cycles",
        },
        {
            "name": "an attempt waiting for capacity is never taken for an unresponsive one",
            "ok": not unknown_while_pending,
            "detail": f"PENDING -> UNKNOWN transitions {len(unknown_while_pending)}",
        },
        {
            "name": "never more active attempts than the goal",
            "ok": all(r["PENDING"] + r["RUNNING"] + r["UNKNOWN"] <= args.replicas for r in rows),
            "detail": f"max active {max(r['PENDING'] + r['RUNNING'] + r['UNKNOWN'] for r in rows)}",
        },
        {
            "name": "a crash while overloaded is detected and replaced once",
            "ok": failed is not None and replacement is not None and after_crash[-1]["attempts"] == args.replicas + 1,
            "detail": f"failed after {detection_s} s, attempts {after_crash[-1]['attempts']}",
        },
        {
            "name": "shrinking the goal stops pending attempts first and keeps running ones",
            "ok": shrunk and survivors <= running_ids and len(survivors) == args.fits,
            "detail": f"survivors {sorted(survivors)} were running before the shrink",
        },
        {
            "name": "the job stops cleanly",
            "ok": stopped and cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    return {
        "meta": meta,
        "times": times,
        "detection_s": detection_s,
        "freed_cpu_taken_by": None if took_cpu is None else took_cpu["instance_id"],
        "replacement": None if replacement is None else replacement["instance_id"],
        "shrink_s": shrink_s,
        "cycles": {"baseline": baseline, "overload": overload, "after_crash": after_crash, "after": after},
        "checks": checks,
        "events": events,
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
