"""Loss of health answers (scenario F4).

A job runs with healthy replicas, but each health answer is dropped with a
fixed probability before it reaches the controller, as a lossy network would
do. The processes themselves never fail: every give-up is a false positive.

The loss is simulated on the controller side by wrapping the runtime: there is
no real network between the processes of one machine. A fixed seed makes each
run reproducible.

With K missed probes needed to give up, a healthy attempt is only abandoned
after K answers in a row are lost. The script compares the observed false
give-ups with the number expected for the loss probability.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/message_loss.py --loss 0.3 --seed 1
    python experiments/message_loss.py --loss 0.1 --seconds 60 --output /tmp/f4.json
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Any

from common import alive_tickers, print_checks, print_meta, settled, snapshot, start_cluster, wait_for_free_cpus

import ray

from orchestrator import Orchestrator, RuntimeController
from orchestrator.runtime import HEALTHY, UNRESPONSIVE, Observation
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[loss]"
RESOURCE_TIMEOUT_S = 30.0


class LossyRuntime:
    """Drops each healthy answer with probability ``loss``, as if it was lost on the way."""

    def __init__(self, runtime: RayRuntime, loss: float, seed: int):
        self._runtime = runtime
        self._loss = loss
        self._random = random.Random(seed)
        self.enabled = False
        self.answers = 0
        self.dropped = 0
        self.real_misses: list[dict[str, str]] = []

    def start(self, instance_id: str, spec: Any) -> None:
        self._runtime.start(instance_id, spec)

    def terminate(self, instance_id: str) -> Any:
        return self._runtime.terminate(instance_id)

    def probe_all(self, instance_ids: list[str]) -> dict[str, Observation]:
        observations = self._runtime.probe_all(instance_ids)
        if not self.enabled:
            return observations
        for instance_id, observation in observations.items():
            if observation.outcome != HEALTHY:
                self.real_misses.append({"instance_id": instance_id, "outcome": observation.outcome})
                continue
            self.answers += 1
            if self._random.random() < self._loss:
                self.dropped += 1
                observations[instance_id] = Observation(instance_id, UNRESPONSIVE, detail="answer lost (simulated)")
        return observations


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--loss", type=float, default=0.3, help="probability of losing each health answer")
    parser.add_argument("--seed", type=int, default=1, help="random seed, for reproducible runs")
    parser.add_argument("--seconds", type=float, default=60.0, help="duration of the lossy period")
    parser.add_argument("--replicas", type=int, default=2, help="desired replicas")
    parser.add_argument("--cpus", type=int, default=4, help="logical CPUs of the local cluster")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds to settle after each change")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--max-missed-probes", type=int, default=3, help="K: missed probes before giving up")
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def _expected_false_give_ups(probes: int, loss: float, k: int) -> float:
    """Expected give-ups over ``probes`` independent answers.

    Each give-up starts a fresh attempt with no missed probes, so give-ups form
    a renewal process: on average one every (1 - p^k) / ((1 - p) p^k) answers.
    """
    if loss <= 0:
        return 0.0
    return probes * (1 - loss) * loss**k / (1 - loss**k)


def run(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus)
    meta.update(
        {
            "loss": args.loss,
            "seed": args.seed,
            "lossy_seconds": args.seconds,
            "replicas": args.replicas,
            "cycle_seconds": args.cycle,
            "probe_timeout_s": args.probe_timeout,
            "max_missed_probes": args.max_missed_probes,
        }
    )
    print(f"{PREFIX} ==== lossy health answers: p = {args.loss}, K = {args.max_missed_probes}, seed {args.seed} ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator(max_missed_probes=args.max_missed_probes)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    lossy = LossyRuntime(runtime, args.loss, args.seed)
    controller = RuntimeController(orchestrator, lossy)
    job_id = orchestrator.submit_job(
        {
            "job_id": "loss-demo",
            "workload": {"entrypoint": "ticker", "args": {"interval_ms": 100}},
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

    until(lambda counts: settled(counts, args.replicas), args.deadline)
    orchestrator.annotate("loss_started", loss=args.loss, seed=args.seed)
    lossy.enabled = True
    rows = []
    started = time.monotonic()
    while time.monotonic() - started < args.seconds:
        controller.step(job_id)
        counts, _ = snapshot(orchestrator, job_id)
        rows.append(counts)
        time.sleep(args.cycle)
    lossy.enabled = False
    orchestrator.annotate("loss_stopped")
    recovered = until(lambda counts: settled(counts, args.replicas), args.deadline)

    orchestrator.stop_job(job_id)
    stopped = until(lambda counts: settled(counts, 0), args.deadline)
    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    events = orchestrator.events()
    start_m = next(e["m"] for e in events if e.get("kind") == "loss_started")
    stop_m = next(e["m"] for e in events if e.get("kind") == "loss_stopped")
    during = [e for e in events if start_m <= e["m"] <= stop_m]
    suspicions = [e for e in during if e["event"] == "instance_observed" and e.get("state") == "UNKNOWN"]
    cleared = [e for e in during if e["event"] == "instance_observed" and e.get("from_state") == "UNKNOWN" and e.get("state") == "RUNNING"]
    give_ups = [e for e in during if e["event"] == "instance_decision" and e.get("reason") == "given_up_after_missed_probes"]
    replacements = [e for e in during if e["event"] == "instance_requested" and e.get("reason") == "replacement"]
    # Each give-up must come right after the K-th missed probe of that attempt.
    premature = []
    for give_up in give_ups:
        missed = [
            e.get("missed_probes")
            for e in during
            if e["event"] == "instance_observed" and e.get("instance_id") == give_up["instance_id"] and "missed_probes" in e
            and e["m"] <= give_up["m"]
        ]
        if not missed or missed[-1] < args.max_missed_probes:
            premature.append(give_up["instance_id"])
    expected = round(_expected_false_give_ups(lossy.answers, args.loss, args.max_missed_probes), 2)
    max_active = max(row["PENDING"] + row["RUNNING"] + row["UNKNOWN"] for row in rows)

    print(f"{PREFIX} health answers {lossy.answers}, dropped {lossy.dropped} ({lossy.dropped / max(lossy.answers, 1):.0%})")
    print(f"{PREFIX} suspicions {len(suspicions)}, cleared by a later answer {len(cleared)}")
    print(f"{PREFIX} false give-ups {len(give_ups)} (expected about {expected}), replacements {len(replacements)}")
    print(f"{PREFIX} misses not caused by the simulated loss: {len(lossy.real_misses)}")

    checks = [
        {
            "name": "the loss rate matches the configured probability",
            "ok": lossy.answers > 0 and abs(lossy.dropped / lossy.answers - args.loss) < 0.1,
            "detail": f"{lossy.dropped}/{lossy.answers} dropped",
        },
        {
            "name": "an isolated lost answer only raises a suspicion",
            "ok": len(cleared) > 0 or args.loss == 0,
            "detail": f"{len(suspicions)} suspicions, {len(cleared)} cleared without replacement",
        },
        {
            "name": "no attempt is given up before K answers in a row are lost",
            "ok": not premature,
            "detail": f"premature give-ups {premature}",
        },
        {
            "name": "every give-up is replaced once",
            "ok": sorted(e["instance_id"] for e in give_ups) == sorted(e.get("replaces") for e in replacements),
            "detail": f"give-ups {[e['instance_id'] for e in give_ups]}, replacements {[e.get('replaces') for e in replacements]}",
        },
        {
            "name": "never more active attempts than the goal",
            "ok": max_active <= args.replicas,
            "detail": f"max active {max_active}",
        },
        {
            "name": "the goal is restored once the loss stops, and the job stops cleanly",
            "ok": recovered and stopped and cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    return {
        "meta": meta,
        "answers": lossy.answers,
        "dropped": lossy.dropped,
        "suspicions": len(suspicions),
        "cleared": len(cleared),
        "false_give_ups": len(give_ups),
        "expected_false_give_ups": expected,
        "replacements": len(replacements),
        "real_misses": lossy.real_misses,
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
