"""The response to a client operation is lost after it took effect (scenario F8).

Each client operation (submit_job, set_replicas, stop_job) is executed, but
its response never reaches the client, as if the connection dropped on the way
back. The client cannot tell whether the operation happened, so it retries.

The loss is simulated on the client side: in this phase the client calls the
domain in the same process, with no transport in between. The instances are
real Ray actors, and Ray's state API is used to count them independently.

Two client policies are compared:

* ``absolute``: the retry repeats the same request. The contract makes every
  operation safe to repeat, so the result must be one job and the goal asked.
* ``recompute``: the client wants "two more replicas" and, after a lost
  response, reads the goal again and recomputes it. This is the pattern an
  absolute operation is meant to avoid: the retry applies the change twice.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/lost_response.py
    python experiments/lost_response.py --output /tmp/f8.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable

from common import alive_tickers, print_checks, print_meta, settled, snapshot, start_cluster, wait_for_free_cpus

import ray

from orchestrator import Orchestrator, RuntimeController
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[lost-response]"
RESOURCE_TIMEOUT_S = 30.0
MAX_RETRIES = 3


class ResponseLost(Exception):
    """The operation was delivered, but its response did not come back."""


class LossyClient:
    """Calls the domain and, when asked to, drops the response of the next calls."""

    def __init__(self, orchestrator: Orchestrator):
        self._orchestrator = orchestrator
        self.lose_next = 0
        self.calls: list[dict[str, Any]] = []

    def call(self, operation: str, *args: Any) -> Any:
        result = getattr(self._orchestrator, operation)(*args)
        lost = self.lose_next > 0
        if lost:
            self.lose_next -= 1
        self.calls.append({"operation": operation, "args": list(args), "result": result, "response_lost": lost})
        self._orchestrator.annotate("client_call", operation=operation, response_lost=lost)
        if lost:
            raise ResponseLost(operation)
        return result

    def with_retries(self, request: Callable[[], Any]) -> tuple[Any, int]:
        """Repeats ``request`` until a response arrives; returns it and the number of sends."""
        for sends in range(1, MAX_RETRIES + 1):
            try:
                return request(), sends
            except ResponseLost:
                continue
        raise RuntimeError("no response after retries")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cpus", type=int, default=8, help="logical CPUs of the local cluster")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds to settle after each change")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


def spec_for(job_id: str, replicas: int) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "workload": {"entrypoint": "ticker", "args": {"interval_ms": 100}},
        "replicas": replicas,
        "resources": {"cpu": 1},
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    meta = start_cluster(cpus=args.cpus)
    meta.update({"cycle_seconds": args.cycle, "probe_timeout_s": args.probe_timeout, "max_retries": MAX_RETRIES})
    print(f"{PREFIX} ==== responses lost after the operation took effect ====")
    print_meta(PREFIX, meta)

    orchestrator = Orchestrator()
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    client = LossyClient(orchestrator)
    steps: list[dict[str, Any]] = []

    def settle(job_id: str, goal: int) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < args.deadline:
            controller.step(job_id)
            if settled(snapshot(orchestrator, job_id)[0], goal):
                return True
            time.sleep(args.cycle)
        return False

    def record(name: str, job_id: str, goal: int, sends: int, answer: Any) -> None:
        ok = settle(job_id, goal)
        alive = alive_tickers()
        view = orchestrator.get_job(job_id)
        step = {
            "step": name,
            "job_id": job_id,
            "sends": sends,
            "answer": answer,
            "desired_replicas": view["desired_replicas"],
            "running": view["observed"]["running"],
            "alive_in_ray": None if alive is None else len(alive),
            "settled": ok,
        }
        steps.append(step)
        print(
            f"{PREFIX} {name}: sent {sends} times, answer {answer!r}, goal {view['desired_replicas']}, "
            f"running {view['observed']['running']}, alive in Ray {step['alive_in_ray']}"
        )

    # Absolute client: every retry repeats the same request.
    job = "retry-demo"
    client.lose_next = 1
    answer, sends = client.with_retries(lambda: client.call("submit_job", spec_for(job, 2)))
    record("submit_job, response lost once", job, 2, sends, answer)

    # The response is lost and the retry only arrives after the controller already acted.
    client.lose_next = 1
    try:
        client.call("set_replicas", job, 4)
    except ResponseLost:
        settle(job, 4)
    answer, retries = client.with_retries(lambda: client.call("set_replicas", job, 4))
    record("set_replicas(4), retried after the change was applied", job, 4, 1 + retries, answer)

    client.lose_next = 2
    answer, sends = client.with_retries(lambda: client.call("set_replicas", job, 3))
    record("set_replicas(3), response lost twice", job, 3, sends, answer)

    # A late retry of the original submit, after the goal changed: must not reset it.
    client.lose_next = 1
    try:
        client.call("submit_job", spec_for(job, 2))
    except ResponseLost:
        pass
    answer, retries = client.with_retries(lambda: client.call("submit_job", spec_for(job, 2)))
    record("submit_job repeated after the goal changed", job, 3, 1 + retries, answer)

    client.lose_next = 1
    answer, sends = client.with_retries(lambda: client.call("stop_job", job))
    record("stop_job, response lost once", job, 0, sends, answer)

    # Recompute client: "two more replicas", recomputed from a fresh read on every retry.
    naive = "recompute-demo"
    client.with_retries(lambda: client.call("submit_job", spec_for(naive, 2)))
    settle(naive, 2)
    wanted = orchestrator.get_job(naive)["desired_replicas"] + 2
    client.lose_next = 1
    answer, sends = client.with_retries(
        lambda: client.call("set_replicas", naive, orchestrator.get_job(naive)["desired_replicas"] + 2)
    )
    record("recompute client: +2 replicas, response lost once", naive, answer, sends, answer)
    orchestrator.stop_job(naive)
    settle(naive, 0)

    cpus_back = wait_for_free_cpus(meta["cluster_resources"].get("CPU", 0), RESOURCE_TIMEOUT_S)
    alive_after = alive_tickers()
    runtime.shutdown()

    events = orchestrator.events()
    requested = [e for e in events if e["event"] == "instance_requested" and e["job_id"] == job]
    replacements = [e for e in requested if e.get("reason") == "replacement"]
    submitted = [e for e in events if e["event"] == "job_submitted"]
    repeated = [e for e in events if e["event"] == "job_submit_repeated" and e["job_id"] == job]
    absolute = [s for s in steps if s["job_id"] == job]
    naive_step = next(s for s in steps if s["job_id"] == naive)
    print(f"{PREFIX} attempts requested for {job}: {len(requested)}; replacements {len(replacements)}")

    checks = [
        {
            "name": "one job per submitted ID, however many times the submit was sent",
            "ok": sorted(e["job_id"] for e in submitted) == sorted([job, naive]) and len(repeated) == 3,
            "detail": f"job_submitted {[e['job_id'] for e in submitted]}, repeated submits {len(repeated)}",
        },
        {
            "name": "every retry got the same answer as the lost one",
            "ok": all(
                c["result"] == next(d["result"] for d in client.calls if d["operation"] == c["operation"] and d["args"] == c["args"])
                for c in client.calls
            ),
            "detail": f"{len(client.calls)} calls, {sum(c['response_lost'] for c in client.calls)} responses lost",
        },
        {
            "name": "after each absolute retry the goal is the one asked, and Ray runs that many actors",
            "ok": all(s["settled"] and s["running"] == s["desired_replicas"] == s["alive_in_ray"] for s in absolute[:-1])
            and absolute[-1]["desired_replicas"] == 0
            and absolute[-1]["settled"],
            "detail": str([(s["desired_replicas"], s["running"], s["alive_in_ray"]) for s in absolute]),
        },
        {
            "name": "a late repeated submit does not reset the goal",
            "ok": absolute[3]["desired_replicas"] == 3,
            "detail": f"goal after the late submit {absolute[3]['desired_replicas']}",
        },
        {
            "name": "no attempt is started twice: only the scale-ups requested attempts, none was a replacement",
            "ok": len(requested) == 4 and not replacements,
            "detail": f"{len(requested)} attempts requested, {len(replacements)} replacements",
        },
        {
            "name": "the recompute client applies its change twice (the pattern the contract avoids)",
            "ok": naive_step["desired_replicas"] == wanted + 2,
            "detail": f"wanted {wanted}, got {naive_step['desired_replicas']}",
        },
        {
            "name": "the jobs stop cleanly",
            "ok": cpus_back is not None and alive_after == [],
            "detail": f"CPUs back after {cpus_back} s, alive tickers {alive_after}",
        },
    ]
    return {"meta": meta, "steps": steps, "calls": client.calls, "checks": checks, "events": events}


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
