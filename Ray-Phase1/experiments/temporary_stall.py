"""A process stops answering for a while and comes back (scenario F7).

A job runs with healthy replicas. One attempt at a time stalls: its process
stays alive and keeps counting, but does not answer health probes for a given
number of seconds, and then answers again. Several stall lengths are tried in
turn, each one after the job has settled again.

With a probe deadline T, a cycle C and K missed probes needed to give up, a
stall that ends before the K-th probe times out must only raise a suspicion:
the attempt goes back to RUNNING with its local counter intact. A longer stall
must end in a give-up, a kill and one replacement, and nothing the stalled
process does afterwards may bring it back.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/temporary_stall.py
    python experiments/temporary_stall.py --stalls 1,4,6,12 --output /tmp/f7.json
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

from orchestrator import Orchestrator, RuntimeController
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[stall]"
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stalls", default="1,4,6,12", help="comma-separated stall lengths in seconds")
    parser.add_argument("--replicas", type=int, default=2, help="desired replicas")
    parser.add_argument("--interval-ms", type=int, default=100, help="ticker interval")
    parser.add_argument("--cpus", type=int, default=4, help="logical CPUs of the local cluster")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds to settle after each change")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--max-missed-probes", type=int, default=3, help="K: missed probes before giving up")
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def give_up_bound(probe_timeout: float, cycle: float, k: int) -> float:
    """Seconds after the stall starts at which the K-th probe in a row times out.

    Each missed probe takes the full deadline, followed by one cycle of sleep.
    A stall that ends before this bound is answered by the K-th probe at the latest.
    """
    return k * probe_timeout + (k - 1) * cycle


def run(args: argparse.Namespace) -> dict[str, Any]:
    stalls = [float(value) for value in args.stalls.split(",") if value.strip()]
    bound = give_up_bound(args.probe_timeout, args.cycle, args.max_missed_probes)
    meta = start_cluster(cpus=args.cpus)
    meta.update(
        {
            "stalls_s": stalls,
            "give_up_bound_s": bound,
            "replicas": args.replicas,
            "cycle_seconds": args.cycle,
            "probe_timeout_s": args.probe_timeout,
            "max_missed_probes": args.max_missed_probes,
        }
    )
    print(f"{PREFIX} ==== temporary stalls {stalls} s; give-up expected beyond ~{bound} s ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator(max_missed_probes=args.max_missed_probes)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    job_id = orchestrator.submit_job(
        {
            "job_id": "stall-demo",
            "workload": {"entrypoint": "ticker", "args": {"interval_ms": args.interval_ms}},
            "replicas": args.replicas,
            "resources": {"cpu": 1},
        }
    )

    def until(done: Any, deadline: float) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < deadline:
            controller.step(job_id)
            if done(snapshot(orchestrator, job_id)[0]):
                return True
            time.sleep(args.cycle)
        return False

    def instance(instance_id: str) -> dict[str, Any]:
        return next(i for i in orchestrator.get_job(job_id)["instances"] if i["instance_id"] == instance_id)

    until(lambda counts: settled(counts, args.replicas), args.deadline)
    phases = []
    max_active = 0
    for seconds in stalls:
        victim = next(i for i in orchestrator.get_job(job_id)["instances"] if i["state"] == "RUNNING")
        victim_id = victim["instance_id"]
        progress_before = victim["progress"]
        orchestrator.annotate("stall_injected", instance_id=victim_id, seconds=seconds)
        runtime.inject_fault(victim_id, "stall", seconds=seconds)
        injected = time.monotonic()
        # Keep cycling past the end of the stall, until the job is settled again.
        while True:
            controller.step(job_id)
            counts, _ = snapshot(orchestrator, job_id)
            max_active = max(max_active, counts["PENDING"] + counts["RUNNING"] + counts["UNKNOWN"])
            elapsed = time.monotonic() - injected
            if (elapsed > seconds + args.probe_timeout and settled(counts, args.replicas)) or elapsed > args.deadline:
                break
            time.sleep(args.cycle)
        after = instance(victim_id)
        orchestrator.annotate("stall_phase_done", instance_id=victim_id)
        events = orchestrator.events()
        start_m = next(e["m"] for e in reversed(events) if e.get("kind") == "stall_injected")
        mine = [e for e in events if e["m"] >= start_m and e.get("instance_id") == victim_id]
        missed = max((e.get("missed_probes", 0) for e in mine if e["event"] == "instance_observed"), default=0)
        suspected = next((e["m"] for e in mine if e["event"] == "instance_observed" and e.get("state") == "UNKNOWN"), None)
        cleared = next(
            (e["m"] for e in mine if e["event"] == "instance_observed" and e.get("from_state") == "UNKNOWN" and e.get("state") == "RUNNING"),
            None,
        )
        given_up = next((e["m"] for e in mine if e.get("reason") == "given_up_after_missed_probes"), None)
        replacements = [
            e["instance_id"]
            for e in events
            if e["m"] >= start_m and e["event"] == "instance_requested" and e.get("replaces") == victim_id
        ]
        stale = [e for e in mine if e["event"] == "stale_report_ignored"]
        phase = {
            "stall_s": seconds,
            "instance_id": victim_id,
            "missed_probes": missed,
            "suspected_after_s": None if suspected is None else round(suspected - start_m, 3),
            "cleared_after_s": None if cleared is None else round(cleared - start_m, 3),
            "given_up_after_s": None if given_up is None else round(given_up - start_m, 3),
            "final_state": after["state"],
            "progress_before": progress_before,
            "progress_after": after["progress"],
            "replacements": replacements,
            "stale_reports_ignored": len(stale),
            "expected_give_up": seconds >= bound,
        }
        phases.append(phase)
        print(
            f"{PREFIX} stall {seconds:>4} s on {victim_id}: missed {missed}, "
            f"suspected {phase['suspected_after_s']}, cleared {phase['cleared_after_s']}, "
            f"given up {phase['given_up_after_s']}, state {after['state']}, "
            f"progress {progress_before} -> {after['progress']}, replacements {replacements}"
        )

    orchestrator.stop_job(job_id)
    stopped = until(lambda counts: settled(counts, 0), args.deadline)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    short = [p for p in phases if not p["expected_give_up"]]
    long = [p for p in phases if p["expected_give_up"]]
    k = args.max_missed_probes
    checks = [
        {
            "name": "a stall shorter than the probe deadline goes unnoticed",
            "ok": all(p["missed_probes"] == 0 for p in short if p["stall_s"] < args.probe_timeout),
            "detail": str([(p["stall_s"], p["missed_probes"]) for p in short if p["stall_s"] < args.probe_timeout]),
        },
        {
            "name": "a stall that ends before the K-th missed probe is only a suspicion",
            "ok": all(p["final_state"] == "RUNNING" and not p["replacements"] and p["missed_probes"] < k for p in short),
            "detail": str([(p["stall_s"], p["missed_probes"], p["final_state"]) for p in short]),
        },
        {
            "name": "the attempt that came back kept its local counter",
            "ok": all(p["progress_after"] > p["progress_before"] for p in short),
            "detail": str([(p["stall_s"], p["progress_before"], p["progress_after"]) for p in short]),
        },
        {
            "name": "a longer stall is given up after exactly K missed probes and replaced once",
            "ok": bool(long)
            and all(p["final_state"] == "FAILED" and p["missed_probes"] == k and len(p["replacements"]) == 1 for p in long),
            "detail": str([(p["stall_s"], p["missed_probes"], p["final_state"], p["replacements"]) for p in long]),
        },
        {
            "name": "a given-up attempt is never revived",
            "ok": all(instance_state == "FAILED" for instance_state in [p["final_state"] for p in long]),
            "detail": f"stale reports ignored {[p['stale_reports_ignored'] for p in long]}",
        },
        {
            "name": "never more active attempts than the goal",
            "ok": max_active <= args.replicas,
            "detail": f"max active {max_active}",
        },
        {
            "name": "the job stops cleanly",
            "ok": stopped and cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    return {"meta": meta, "phases": phases, "checks": checks, "events": orchestrator.events()}


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
