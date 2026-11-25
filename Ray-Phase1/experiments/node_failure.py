"""Loss of a whole node, then a new node joining.

Several nodes are simulated on one machine with Ray's cluster utilities: a head
node without CPUs and two worker nodes with 2 CPUs each. A job with four
replicas fills both worker nodes. Then one worker node is removed without a
graceful shutdown, which takes down every process it hosts:

* the attempts on the lost node must end FAILED and be replaced, one
  replacement each, while the attempts on the other node keep running;
* the replacements cannot fit anywhere, so they must wait in PENDING without
  multiplying;
* when a new node joins, the replacements must be placed on it.

Each probe round records how long it took and what Ray answered, because node
loss is where those answers are least predictable.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/node_failure.py
    python experiments/node_failure.py --output /tmp/node.json --events /tmp/node.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from common import LETTER, ROOT, STATES, alive_tickers, print_checks, settled, snapshot, wait_for_free_cpus

import ray
from ray.cluster_utils import Cluster

from orchestrator import Orchestrator, RuntimeController
from orchestrator.evidence import fault_timeline
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[node]"
RESOURCE_TIMEOUT_S = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--job-id", default="node-demo", help="job id submitted to the domain")
    parser.add_argument("--cpus-per-node", type=int, default=2, help="logical CPUs of each worker node")
    parser.add_argument("--interval-ms", type=int, default=100, help="ticker interval")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between cycles")
    parser.add_argument("--deadline", type=float, default=240.0, help="seconds each stage may take")
    parser.add_argument("--hold-cycles", type=int, default=4, help="cycles to watch the pending replacements")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--max-missed-probes", type=int, default=3, help="K: missed probes before giving up")
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    parser.add_argument("--events", type=Path, default=None, help="write the domain events as JSONL")
    return parser


class Run:
    """The controller loop of this experiment, recording every probe round."""

    def __init__(self, orchestrator: Orchestrator, controller: RuntimeController, job_id: str, cycle: float):
        self.orchestrator = orchestrator
        self.controller = controller
        self.job_id = job_id
        self.cycle = cycle
        self.rows: list[dict[str, Any]] = []

    def step(self, label: str) -> dict[str, Any]:
        started = time.monotonic()
        observations = self.controller.observe(self.job_id)
        probe_seconds = round(time.monotonic() - started, 3)
        actions = self.controller.act(self.job_id)
        view = self.orchestrator.get_job(self.job_id)
        counts, instances = snapshot(self.orchestrator, self.job_id)
        answers = Counter(o.outcome if o.outcome != "unresponsive" else f"unresponsive:{o.detail.split(' ')[0]}" for o in observations.values())
        rendered = ", ".join(
            f"{i['instance_id']}({LETTER[i['state']]}@{(i['node_id'] or '-')[:6]})" for i in view["instances"]
        )
        print(
            f"{PREFIX} {label} cycle {len(self.rows) + 1} (probe {probe_seconds:.1f}s): goal {view['desired_replicas']} | "
            + " ".join(f"{LETTER[s]} {counts[s]}" for s in STATES)
            + f" | +{len(actions.start)} -{len(actions.kill)} | {rendered}"
        )
        row = {
            "label": label,
            "probe_seconds": probe_seconds,
            "answers": dict(answers),
            "desired": view["desired_replicas"],
            "counts": counts,
            "instances": instances,
            "attempts": len(view["instances"]),
        }
        self.rows.append(row)
        return row

    def until(self, label: str, done: Any, deadline: float) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < deadline:
            if done(self.step(label)):
                return True
            time.sleep(self.cycle)
        return False


def _pid_alive(pid: int) -> bool:
    """Whether a process exists on this machine. Valid because the simulated nodes share it."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _by_node(orchestrator: Orchestrator, job_id: str) -> dict[str, list[str]]:
    placement: dict[str, list[str]] = {}
    for instance in orchestrator.get_job(job_id)["instances"]:
        if instance["state"] == "RUNNING":
            placement.setdefault(instance["node_id"], []).append(instance["instance_id"])
    return placement


def run(args: argparse.Namespace) -> dict[str, Any]:
    if Path.cwd().resolve() != ROOT:
        raise SystemExit(f"run the experiments from {ROOT}: Ray workers import ray_runtime from there")

    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0, "include_dashboard": True})
    nodes = [cluster.add_node(num_cpus=args.cpus_per_node) for _ in range(2)]
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()
    meta = {
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "mode": "simulated cluster: head without CPUs and two worker nodes on one machine",
        "cpus_per_node": args.cpus_per_node,
        "cycle_seconds": args.cycle,
        "probe_timeout_s": args.probe_timeout,
        "max_missed_probes": args.max_missed_probes,
    }
    print(f"{PREFIX} ==== loss of a node, then a new node joining ====")
    print(f"{PREFIX} ray {meta['ray_version']} | python {meta['python_version']} | {meta['mode']}")

    replicas = 2 * args.cpus_per_node
    orchestrator = Orchestrator(max_missed_probes=args.max_missed_probes)
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    job_id = orchestrator.submit_job(
        {
            "job_id": args.job_id,
            "workload": {"entrypoint": "ticker", "args": {"interval_ms": args.interval_ms}},
            "replicas": replicas,
            "resources": {"cpu": 1},
        }
    )
    loop = Run(orchestrator, controller, job_id, args.cycle)

    try:
        loop.until("start", lambda row: settled(row["counts"], replicas), args.deadline)
        placement_before = _by_node(orchestrator, job_id)
        victim_node = next(n for n in nodes if n.node_id in placement_before)
        lost = placement_before[victim_node.node_id]
        survivors = [i for node_id, ids in placement_before.items() if node_id != victim_node.node_id for i in ids]
        survivors_before = {
            i["instance_id"]: i for i in orchestrator.get_job(job_id)["instances"] if i["instance_id"] in survivors
        }
        print(f"{PREFIX} placement: " + " | ".join(f"{n[:6]}: {ids}" for n, ids in placement_before.items()))
        lost_pids = {
            i["instance_id"]: i["pid"] for i in orchestrator.get_job(job_id)["instances"] if i["instance_id"] in lost
        }
        print(f"{PREFIX} removing node {victim_node.node_id[:6]}, which hosts {lost} (pids {list(lost_pids.values())})")

        for instance_id in lost:
            orchestrator.annotate("fault_injected", instance_id=instance_id, node_id=victim_node.node_id, method="node_lost")
        cluster.remove_node(victim_node, allow_graceful=False)

        def lost_and_replaced(row: dict[str, Any]) -> bool:
            states = row["instances"]
            replaced = all(states.get(i) == "FAILED" for i in lost)
            return replaced and row["counts"]["PENDING"] == len(lost)

        recovered_to_pending = loop.until("node-lost", lost_and_replaced, args.deadline)
        # The replacements exist now: are the processes of the lost attempts really gone?
        alive_when_replaced = [pid for pid in lost_pids.values() if _pid_alive(pid)]
        hold_rows = []
        for _ in range(args.hold_cycles):
            time.sleep(args.cycle)
            hold_rows.append(loop.step("no-capacity"))

        print(f"{PREFIX} adding a new node with {args.cpus_per_node} CPUs")
        orchestrator.annotate("node_joined", cpus=args.cpus_per_node)
        # No cluster.wait_for_nodes() here: Ray may still count the lost node as alive
        # after the controller gave up on its attempts, so the node count never matches.
        # The controller loop below already waits, with its own deadline.
        new_node = cluster.add_node(num_cpus=args.cpus_per_node)
        restored = loop.until("node-joined", lambda row: settled(row["counts"], replicas), args.deadline)
        placement_after = _by_node(orchestrator, job_id)
        alive_when_running = [pid for pid in lost_pids.values() if _pid_alive(pid)]

        orchestrator.stop_job(job_id)
        stopped = loop.until("stop", lambda row: settled(row["counts"], 0), args.deadline)
        # Ray may still count the lost node's CPUs, so expect only those of the two live nodes.
        cpus_back = wait_for_free_cpus(2 * args.cpus_per_node, RESOURCE_TIMEOUT_S)
        alive_after = alive_tickers()
        runtime.shutdown()

        events = orchestrator.events()
        timelines = {i: fault_timeline(events, i) for i in lost}
        replacements = [e for e in events if e["event"] == "instance_requested" and e.get("reason") == "replacement"]
        replaced = sorted(e.get("replaces") for e in replacements)
        replacement_ids = [e["instance_id"] for e in replacements]
        # A replacement can fail too, for example if Ray places it on the lost node
        # while it still believes that node is alive; it is then replaced in turn.
        failed_replacements = [i for i in replacement_ids if snapshot(orchestrator, job_id)[1].get(i) == "FAILED"]
        survivors_after = {
            i["instance_id"]: i for i in orchestrator.get_job(job_id)["instances"] if i["instance_id"] in survivors
        }
        injected_at = min(e["m"] for e in events if e["event"] == "annotation" and e.get("kind") == "fault_injected")
        survivor_changes = [
            e
            for e in events
            if e["event"] in ("instance_observed", "instance_decision")
            and e["instance_id"] in survivors
            and "state" in e
            and e["m"] >= injected_at
            and e["m"] < next(x["m"] for x in events if x["event"] == "replicas_set")
        ]
        after_loss = [row for row in loop.rows if row["label"] in ("node-lost", "no-capacity")]
        unavailable_seen = sum(row["answers"].get("unresponsive:ActorUnavailableError", 0) for row in after_loss)
        slowest_round = max(row["probe_seconds"] for row in after_loss) if after_loss else None
        final_states = {i: s for i, s in snapshot(orchestrator, job_id)[1].items()}

        print(f"{PREFIX} slowest probe round after the loss: {slowest_round} s (deadline {args.probe_timeout} s)")
        print(f"{PREFIX} ActorUnavailableError answers after the loss: {unavailable_seen}")
        print(
            f"{PREFIX} lost processes still alive when replaced: {alive_when_replaced}; "
            f"when the replacements ran: {alive_when_running}"
        )
        print(f"{PREFIX} replacements that failed before the new node joined: {failed_replacements}")
        for instance_id, timeline in timelines.items():
            print(
                f"{PREFIX} {instance_id}: suspected {timeline['suspicion_s']} s | failed {timeline['detection_s']} s "
                f"(by {timeline['failed_by']}) | replacement {timeline['replacement_id']}"
            )

        checks = [
            {
                "name": "the replicas start spread over both worker nodes",
                "ok": len(placement_before) == 2 and all(len(ids) == args.cpus_per_node for ids in placement_before.values()),
                "detail": " | ".join(f"{n[:6]}: {len(ids)}" for n, ids in placement_before.items()),
            },
            {
                "name": "the attempts on the lost node end FAILED, not STOPPED",
                "ok": all(final_states.get(i) == "FAILED" for i in lost),
                "detail": f"{ {i: final_states.get(i) for i in lost} }",
            },
            {
                "name": "every lost attempt is replaced, and every replacement names a failed attempt",
                "ok": set(lost) <= set(replaced) and all(final_states.get(i) == "FAILED" for i in replaced),
                "detail": f"replacements for {replaced}; failed replacements {failed_replacements}",
            },
            {
                "name": "the attempts on the surviving node kept running",
                "ok": not survivor_changes
                and all(survivors_after[i]["pid"] == survivors_before[i]["pid"] for i in survivors)
                and all((survivors_after[i]["progress"] or 0) > (survivors_before[i]["progress"] or 0) for i in survivors),
                "detail": f"state changes {len(survivor_changes)}, survivors {survivors}",
            },
            {
                "name": "without capacity the replacements wait in PENDING without multiplying",
                "ok": recovered_to_pending
                and all(row["counts"]["PENDING"] == len(lost) for row in hold_rows)
                and len({row["attempts"] for row in hold_rows}) == 1,
                "detail": f"pending {[row['counts']['PENDING'] for row in hold_rows]}, attempts {[row['attempts'] for row in hold_rows]}",
            },
            {
                "name": "no lost process was still alive when its replacement ran",
                "ok": not alive_when_running,
                "detail": f"alive when requested {alive_when_replaced}, when running {alive_when_running}",
            },
            {
                "name": "the goal never changed",
                "ok": all(row["desired"] == replicas for row in loop.rows if row["label"] != "stop"),
                "detail": f"desired {sorted({row['desired'] for row in loop.rows if row['label'] != 'stop'})}",
            },
            {
                "name": "the replacements run on the node that joined, the survivors where they were",
                "ok": restored
                and len(placement_after.get(new_node.node_id, [])) == len(lost)
                and set(placement_after.get(new_node.node_id, [])) <= set(replacement_ids)
                and sorted(i for n, ids in placement_after.items() if n != new_node.node_id for i in ids) == sorted(survivors),
                "detail": f"on the new node: {placement_after.get(new_node.node_id, [])}",
            },
            {
                "name": "detection of every lost attempt was measured",
                "ok": all(t["detection_s"] is not None for t in timelines.values()),
                "detail": ", ".join(f"{i}: {t['detection_s']} s by {t['failed_by']}" for i, t in timelines.items()),
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
            "placement_before": placement_before,
            "lost_node": victim_node.node_id,
            "lost": lost,
            "new_node": new_node.node_id,
            "placement_after": placement_after,
            "timelines": timelines,
            "slowest_probe_round_s": slowest_round,
            "actor_unavailable_answers": unavailable_seen,
            "lost_pids": lost_pids,
            "lost_alive_when_replaced": alive_when_replaced,
            "lost_alive_when_running": alive_when_running,
            "failed_replacements": failed_replacements,
            "trace": loop.rows,
            "checks": checks,
            "events": events,
        }
    finally:
        ray.shutdown()
        cluster.shutdown()


def main() -> int:
    args = build_parser().parse_args()
    results = run(args)
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
