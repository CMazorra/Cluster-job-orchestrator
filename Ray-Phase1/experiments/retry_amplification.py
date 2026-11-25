"""Retry amplification: a workload that crashes as soon as it starts (scenario F17).

A job with two replicas runs until both are healthy. Then one attempt crashes,
and from that moment every new attempt of the job crashes right after it is
started, as a workload with a broken configuration would. After a while the
fault is removed, as if the configuration had been fixed.

With the replacement backoff, each failure of the chain waits twice as long as
the previous one before its replacement is requested. With ``--no-backoff`` the
controller replaces it on every cycle, which shows the amplification the
backoff prevents.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/retry_amplification.py
    python experiments/retry_amplification.py --no-backoff
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
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[retry]"
RESOURCE_TIMEOUT_S = 30.0
GRANULARITY_S = 0.05  # tolerance when comparing monotonic stamps of events


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--job-id", default="retry-demo", help="job id submitted to the domain")
    parser.add_argument("--replicas", type=int, default=2, help="desired replicas")
    parser.add_argument("--cpus", type=int, default=2, help="logical CPUs of the local cluster")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--loop-seconds", type=float, default=30.0, help="how long new attempts keep crashing")
    parser.add_argument("--deadline", type=float, default=90.0, help="seconds to settle after each change")
    parser.add_argument("--backoff-base", type=float, default=1.0, help="first backoff wait, in seconds")
    parser.add_argument("--backoff-max", type=float, default=30.0, help="maximum backoff wait, in seconds")
    parser.add_argument("--no-backoff", action="store_true", help="disable the backoff to show the amplification")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


class Loop:
    """Controller cycles; while ``crashing`` is set, every started attempt is crashed at once."""

    def __init__(self, orchestrator: Orchestrator, controller: RuntimeController, runtime: RayRuntime, job_id: str):
        self.orchestrator = orchestrator
        self.controller = controller
        self.runtime = runtime
        self.job_id = job_id
        self.crashing = False
        self.rows: list[dict[str, Any]] = []

    def step(self, label: str) -> dict[str, Any]:
        self.controller.observe(self.job_id)
        actions = self.controller.act(self.job_id)
        if self.crashing:
            for instance_id in actions.start:
                self.runtime.inject_fault(instance_id, "crash")
        view = self.orchestrator.get_job(self.job_id)
        counts, instances = snapshot(self.orchestrator, self.job_id)
        print(
            f"{PREFIX} {label} cycle {len(self.rows) + 1}: goal {view['desired_replicas']} | "
            + " ".join(f"{LETTER[s]} {counts[s]}" for s in STATES)
            + f" | +{len(actions.start)} -{len(actions.kill)} | attempts {len(view['instances'])}"
        )
        row = {
            "label": label,
            "desired": view["desired_replicas"],
            "counts": counts,
            "attempts": len(view["instances"]),
            "active": counts["PENDING"] + counts["RUNNING"] + counts["UNKNOWN"],
            "instances": instances,
        }
        self.rows.append(row)
        return row


def run(args: argparse.Namespace) -> dict[str, Any]:
    backoff_base = 0.0 if args.no_backoff else args.backoff_base
    meta = start_cluster(cpus=args.cpus)
    meta.update(
        {
            "replicas": args.replicas,
            "cycle_seconds": args.cycle,
            "loop_seconds": args.loop_seconds,
            "backoff_base_s": backoff_base,
            "backoff_max_s": args.backoff_max,
        }
    )
    print(f"{PREFIX} ==== crash loop, backoff {'off' if args.no_backoff else 'on'} ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator(backoff_base_s=backoff_base, backoff_max_s=max(args.backoff_max, backoff_base))
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    job_id = orchestrator.submit_job(
        {
            "job_id": args.job_id,
            "workload": {"entrypoint": "ticker", "args": {"interval_ms": 100}},
            "replicas": args.replicas,
            "resources": {"cpu": 1},
        }
    )
    loop = Loop(orchestrator, controller, runtime, job_id)

    started = time.monotonic()
    while time.monotonic() - started < args.deadline and not settled(loop.step("start")["counts"], args.replicas):
        time.sleep(args.cycle)
    running = [i for i in orchestrator.get_job(job_id)["instances"] if i["state"] == "RUNNING"]
    victim, survivor = running[0], running[1]

    print(f"{PREFIX} {victim['instance_id']} crashes, and every new attempt will crash on start")
    orchestrator.annotate("fault_injected", instance_id=victim["instance_id"], method="crash_loop")
    loop.crashing = True
    runtime.inject_fault(victim["instance_id"], "crash")
    loop_started = time.monotonic()
    while time.monotonic() - loop_started < args.loop_seconds:
        loop.step("crash-loop")
        time.sleep(args.cycle)

    print(f"{PREFIX} the fault is removed: new attempts start normally")
    orchestrator.annotate("fault_removed")
    loop.crashing = False
    healed_at = time.monotonic()
    healed = False
    while time.monotonic() - healed_at < args.deadline:
        if settled(loop.step("healed")["counts"], args.replicas):
            healed = True
            break
        time.sleep(args.cycle)
    heal_seconds = round(time.monotonic() - healed_at, 3)

    orchestrator.stop_job(job_id)
    stopped_at = time.monotonic()
    stopped = False
    while time.monotonic() - stopped_at < args.deadline:
        if settled(loop.step("stop")["counts"], 0):
            stopped = True
            break
        time.sleep(args.cycle)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    events = orchestrator.events()
    injected_m = next(e["m"] for e in events if e.get("kind") == "fault_injected")
    removed_m = next(e["m"] for e in events if e.get("kind") == "fault_removed")
    during = [e for e in events if injected_m <= e["m"] <= removed_m]
    replacements = [e for e in during if e["event"] == "instance_requested" and e.get("reason") == "replacement"]
    request_times = [round(e["m"] - injected_m, 2) for e in replacements]
    gaps = [round(b - a, 2) for a, b in zip(request_times, request_times[1:])]
    backoffs = {e["instance_id"]: e for e in events if e["event"] == "replacement_backoff"}
    early = [
        e["instance_id"]
        for e in replacements
        if e["replaces"] in backoffs
        and e["m"] + GRANULARITY_S < backoffs[e["replaces"]]["m"] + backoffs[e["replaces"]]["delay_s"]
    ]
    survivor_changes = [
        e
        for e in during
        if e["event"] in ("instance_observed", "instance_decision")
        and e.get("instance_id") == survivor["instance_id"]
        and "state" in e
    ]
    max_active = max(row["active"] for row in loop.rows if row["label"] in ("crash-loop", "healed"))
    attempts_total = len(orchestrator.get_job(job_id)["instances"])

    print(f"{PREFIX} replacements during {args.loop_seconds:.0f} s of crash loop: {len(replacements)}")
    print(f"{PREFIX} requested at (s after the first crash): {request_times}")
    print(f"{PREFIX} gaps between requests (s): {gaps}")
    print(f"{PREFIX} back to the goal {heal_seconds} s after the fault was removed")

    checks = [
        {
            "name": "the other replica kept running",
            "ok": not survivor_changes,
            "detail": f"state changes of {survivor['instance_id']}: {len(survivor_changes)}",
        },
        {
            "name": "the goal never changed",
            "ok": all(row["desired"] == args.replicas for row in loop.rows if row["label"] != "stop"),
            "detail": f"desired {sorted({row['desired'] for row in loop.rows if row['label'] != 'stop'})}",
        },
        {
            "name": "never more active attempts than the goal",
            "ok": max_active <= args.replicas,
            "detail": f"max active {max_active}",
        },
        {
            "name": "no replacement was requested before its backoff expired",
            "ok": not early,
            "detail": f"early replacements {early}",
        },
        {
            "name": "the goal is restored once the fault is removed",
            "ok": healed,
            "detail": f"after {heal_seconds} s",
        },
        {
            "name": "the job stops without leftovers",
            "ok": stopped and cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    if not args.no_backoff:
        checks.insert(
            3,
            {
                "name": "with backoff, the gaps between replacements grow",
                "ok": len(gaps) >= 3 and all(b + GRANULARITY_S >= a for a, b in zip(gaps, gaps[1:])) and gaps[-1] > gaps[0],
                "detail": f"gaps {gaps}",
            },
        )
    return {
        "meta": meta,
        "job_id": job_id,
        "replacements_during_loop": len(replacements),
        "request_times_s": request_times,
        "gaps_s": gaps,
        "heal_seconds": heal_seconds,
        "attempts_total": attempts_total,
        "trace": loop.rows,
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
