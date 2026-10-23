"""Crash of one actor process while the controller keeps running (scenario F1).

A job with two replicas runs until both are healthy. Then the process of one
attempt exits on its own (``os._exit``), without telling the controller. The
script checks what was lost, what survived and who restored the goal:

* lost: the local counter of the crashed attempt;
* survived: the goal, which lives in the controller, and the other attempt;
* restored by: the controller's reconcile pass, which requests one replacement.

Detection and recovery times come from the monotonic stamps of the events.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/crash_recovery.py
    python experiments/crash_recovery.py --output /tmp/crash.json --events /tmp/crash.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from common import (
    LETTER,
    STATES,
    alive_tickers,
    print_checks,
    print_meta,
    settled,
    snapshot,
    start_cluster,
    wait_for_free_cpus,
)

import ray

from orchestrator import Orchestrator, RuntimeController
from orchestrator.evidence import crash_timeline
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[crash]"
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--job-id", default="crash-demo", help="job id submitted to the domain")
    parser.add_argument("--replicas", type=int, default=2, help="desired replicas")
    parser.add_argument("--interval-ms", type=int, default=100, help="ticker interval")
    parser.add_argument("--cpus", type=int, default=2, help="logical CPUs of the local cluster")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--warm-seconds", type=float, default=1.5, help="healthy run time before the crash")
    parser.add_argument("--deadline", type=float, default=30.0, help="seconds to settle after each change")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--max-missed-probes", type=int, default=3, help="K: missed probes before giving up")
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def _cycle(controller: RuntimeController, orchestrator: Orchestrator, job_id: str, label: str, index: int) -> dict[str, Any]:
    actions = controller.step(job_id)
    view = orchestrator.get_job(job_id)
    counts, instances = snapshot(orchestrator, job_id)
    rendered = ", ".join(f"{i['instance_id']}({LETTER[i['state']]}:{i['progress']})" for i in view["instances"])
    print(
        f"{PREFIX} {label} cycle {index}: goal {view['desired_replicas']} | "
        + " ".join(f"{LETTER[s]} {counts[s]}" for s in STATES)
        + f" | +{len(actions.start)} -{len(actions.kill)} | {rendered}"
    )
    return {"label": label, "cycle": index, "desired": view["desired_replicas"], "counts": counts, "instances": instances}


def _until_settled(controller: RuntimeController, orchestrator: Orchestrator, job_id: str, desired: int, label: str, args: argparse.Namespace) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    started = time.monotonic()
    while time.monotonic() - started < args.deadline:
        rows.append(_cycle(controller, orchestrator, job_id, label, len(rows) + 1))
        if settled(rows[-1]["counts"], desired):
            break
        time.sleep(args.cycle)
    return rows


def _instance(orchestrator: Orchestrator, job_id: str, instance_id: str) -> dict[str, Any]:
    return next(i for i in orchestrator.get_job(job_id)["instances"] if i["instance_id"] == instance_id)


def run(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus)
    meta.update(
        {
            "replicas": args.replicas,
            "cycle_seconds": args.cycle,
            "probe_timeout_s": args.probe_timeout,
            "max_missed_probes": args.max_missed_probes,
        }
    )
    print(f"{PREFIX} ==== crash of one actor process, controller alive ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator(max_missed_probes=args.max_missed_probes)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    job_id = orchestrator.submit_job(
        {
            "job_id": args.job_id,
            "workload": {"entrypoint": "ticker", "args": {"interval_ms": args.interval_ms}},
            "replicas": args.replicas,
            "resources": {"cpu": 1},
        }
    )

    trace = _until_settled(controller, orchestrator, job_id, args.replicas, "start", args)
    warm_until = time.monotonic() + args.warm_seconds
    while time.monotonic() < warm_until:
        time.sleep(args.cycle)
        trace.append(_cycle(controller, orchestrator, job_id, "healthy", len(trace) + 1))

    running = [i for i in orchestrator.get_job(job_id)["instances"] if i["state"] == "RUNNING"]
    victim, survivor = running[0], running[1]
    print(f"{PREFIX} injecting a crash into {victim['instance_id']} (pid {victim['pid']}, progress {victim['progress']})")
    orchestrator.annotate("fault_injected", instance_id=victim["instance_id"], pid=victim["pid"], method="crash")
    runtime.crash(victim["instance_id"])

    trace += _until_settled(controller, orchestrator, job_id, args.replicas, "after-crash", args)
    recovered_counts = trace[-1]["counts"]
    recovered = settled(recovered_counts, args.replicas)

    events = orchestrator.events()
    timeline = crash_timeline(events, victim["instance_id"])
    replacement_id = timeline["replacement_id"]
    replacement = _instance(orchestrator, job_id, replacement_id) if replacement_id else None
    first_replacement_report = next(
        (e for e in events if e["event"] == "instance_observed" and e["instance_id"] == replacement_id and "progress" in e),
        None,
    )
    survivor_after = _instance(orchestrator, job_id, survivor["instance_id"])
    victim_after = _instance(orchestrator, job_id, victim["instance_id"])
    replacements = [e for e in events if e["event"] == "instance_requested" and e.get("reason") == "replacement"]
    survivor_changes = [
        e
        for e in events
        if e["event"] in ("instance_observed", "instance_decision")
        and e["instance_id"] == survivor["instance_id"]
        and "state" in e
        and e["m"] >= timeline_start(events, victim["instance_id"])
    ]

    print(f"{PREFIX} lost: the counter of {victim['instance_id']}, which was at {victim['progress']}")
    print(
        f"{PREFIX} survived: goal {orchestrator.get_job(job_id)['desired_replicas']} (held by the controller), "
        f"and {survivor['instance_id']}: progress {survivor['progress']} -> {survivor_after['progress']}"
    )
    print(
        f"{PREFIX} restored by: reconcile, which requested {replacement_id} "
        f"(reason replacement, replaces {victim['instance_id']})"
    )
    print(
        f"{PREFIX} timeline: detection {timeline['detection_s']} s | "
        f"recovery {timeline['recovery_s']} s | total {timeline['total_s']} s"
    )

    orchestrator.stop_job(job_id)
    trace += _until_settled(controller, orchestrator, job_id, 0, "stop", args)
    stopped = settled(trace[-1]["counts"], 0)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    checks = [
        {
            "name": "the crashed attempt ends FAILED, not STOPPED",
            "ok": victim_after["state"] == "FAILED",
            "detail": f"{victim['instance_id']}: {victim_after['state']}",
        },
        {
            "name": "exactly one replacement, naming the crashed attempt",
            "ok": len(replacements) == 1 and replacements[0].get("replaces") == victim["instance_id"],
            "detail": f"replacements {[(e['instance_id'], e.get('replaces')) for e in replacements]}",
        },
        {
            "name": "the goal is restored",
            "ok": recovered,
            "detail": f"RUNNING {recovered_counts['RUNNING']} of {args.replicas} after the crash",
        },
        {
            "name": "the goal never changed",
            "ok": all(row["desired"] == args.replicas for row in trace if row["label"] != "stop"),
            "detail": f"desired {sorted({row['desired'] for row in trace if row['label'] != 'stop'})}",
        },
        {
            "name": "the replacement is a new process",
            "ok": replacement is not None and first_replacement_report is not None
            and first_replacement_report.get("pid") not in (victim["pid"], survivor["pid"]),
            "detail": f"pid {first_replacement_report.get('pid') if first_replacement_report else None}, "
            f"crashed {victim['pid']}, survivor {survivor['pid']}",
        },
        {
            "name": "the replacement starts its counter again: local state was lost",
            "ok": first_replacement_report is not None and first_replacement_report["progress"] < victim["progress"],
            "detail": f"replacement first progress {first_replacement_report['progress'] if first_replacement_report else None}, "
            f"crashed attempt was at {victim['progress']}",
        },
        {
            "name": "the other attempt kept running and counting",
            "ok": not survivor_changes and survivor_after["pid"] == survivor["pid"]
            and (survivor_after["progress"] or 0) > (survivor["progress"] or 0),
            "detail": f"state changes {len(survivor_changes)}, progress {survivor['progress']} -> {survivor_after['progress']}",
        },
        {
            "name": "detection and recovery were measured",
            "ok": timeline["detection_s"] is not None and timeline["recovery_s"] is not None,
            "detail": f"detection {timeline['detection_s']} s, recovery {timeline['recovery_s']} s",
        },
        {
            "name": "the job stops without leftovers",
            "ok": stopped and cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    return {
        "meta": meta,
        "job_id": job_id,
        "victim": victim,
        "survivor_before": survivor,
        "survivor_after": survivor_after,
        "timeline": timeline,
        "trace": trace,
        "checks": checks,
        "events": events,
    }


def timeline_start(events: list[dict[str, Any]], instance_id: str) -> float:
    """Monotonic stamp of the fault injection into ``instance_id``."""
    return next(
        e["m"] for e in events if e["event"] == "annotation" and e.get("kind") == "fault_injected" and e.get("instance_id") == instance_id
    )


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
