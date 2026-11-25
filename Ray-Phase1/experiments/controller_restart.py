"""Crash and restart of the controller (scenario F2).

The controller runs inside the driver process, so to crash it this experiment
uses two roles:

* the **observer** (the process you start) owns a Ray cluster that never goes
  down, starts the controller as a separate process, kills it without warning
  (SIGKILL), watches what happens to the instances through Ray's state API, and
  starts a new controller afterwards;
* the **controller** is a separate process that runs the orchestrator and the
  control loop and writes its view of the job to a status file on every cycle.

The prototype keeps its state only in memory, so the expected outcome is a
documented limitation: the restarted controller knows nothing about the job,
and the client has to submit it again.

Usage (from the Ray-Phase1 folder, inside WSL2):

    python experiments/controller_restart.py
    python experiments/controller_restart.py --output /tmp/f2.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from common import ROOT, alive_tickers, print_checks, settled, snapshot, wait_for_free_cpus

import ray
from ray.cluster_utils import Cluster

from orchestrator import ErrorCode, Orchestrator, OrchestratorError, RuntimeController
from ray_runtime import DEFAULT_PROBE_TIMEOUT_S, RayRuntime

PREFIX = "[restart]"
JOB_SPEC = {
    "job_id": "restart-demo",
    "workload": {"entrypoint": "ticker", "args": {"interval_ms": 100}},
    "replicas": 2,
    "resources": {"cpu": 1},
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--role", choices=("observer", "controller"), default="observer", help=argparse.SUPPRESS)
    parser.add_argument("--address", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--status-file", type=Path, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--session", type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument("--cpus", type=int, default=2, help="logical CPUs of the worker node")
    parser.add_argument("--cycle", type=float, default=0.5, help="seconds between control cycles")
    parser.add_argument("--deadline", type=float, default=60.0, help="seconds each stage may take")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT_S)
    parser.add_argument("--output", type=Path, default=None, help="write the results as JSON")
    return parser


# --- Controller role ------------------------------------------------------------


def _write_status(path: Path, status: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(status), encoding="utf-8")
    tmp.replace(path)


def run_controller(args: argparse.Namespace) -> int:
    """One controller session. Session 1 runs until it is killed; session 2 restarts and stops."""
    ray.init(address=args.address, logging_level="ERROR")
    orchestrator = Orchestrator()
    runtime = RayRuntime(probe_timeout=args.probe_timeout)
    controller = RuntimeController(orchestrator, runtime)
    status: dict[str, Any] = {"session": args.session, "pid": os.getpid(), "phase": "starting"}

    if args.session > 1:
        # A restarted controller first asks for the job it was managing before.
        try:
            orchestrator.get_job(JOB_SPEC["job_id"])
            status["job_known_after_restart"] = True
        except OrchestratorError as exc:
            status["job_known_after_restart"] = False
            status["get_job_error"] = exc.code.value if isinstance(exc.code, ErrorCode) else str(exc.code)
    job_id = orchestrator.submit_job(JOB_SPEC)

    def cycle(phase: str) -> dict[str, int]:
        controller.step(job_id)
        counts, _ = snapshot(orchestrator, job_id)
        status.update(
            {
                "phase": phase,
                "monotonic": time.monotonic(),
                "job": orchestrator.get_job(job_id),
                "counts": counts,
                "events": orchestrator.events(),
            }
        )
        _write_status(args.status_file, status)
        return counts

    if args.session == 1:
        while True:  # runs until the observer kills this process
            cycle("running")
            time.sleep(args.cycle)

    started = time.monotonic()
    while time.monotonic() - started < args.deadline and not settled(cycle("recovering"), JOB_SPEC["replicas"]):
        time.sleep(args.cycle)
    for _ in range(4):
        time.sleep(args.cycle)
        cycle("steady")
    orchestrator.stop_job(job_id)
    started = time.monotonic()
    while time.monotonic() - started < args.deadline and not settled(cycle("stopping"), 0):
        time.sleep(args.cycle)
    cycle("stopped")
    runtime.shutdown()
    ray.shutdown()
    return 0


# --- Observer role --------------------------------------------------------------


def _read_status(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _wait_status(path: Path, predicate: Any, deadline: float) -> dict[str, Any] | None:
    started = time.monotonic()
    while time.monotonic() - started < deadline:
        status = _read_status(path)
        if status is not None and predicate(status):
            return status
        time.sleep(0.2)
    return _read_status(path)


def _spawn(args: argparse.Namespace, address: str, status_file: Path, session: int) -> subprocess.Popen:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--role", "controller",
        "--address", address,
        "--status-file", str(status_file),
        "--session", str(session),
        "--cycle", str(args.cycle),
        "--deadline", str(args.deadline),
        "--probe-timeout", str(args.probe_timeout),
    ]
    return subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _pid_alive(pid: int) -> bool:
    """Whether a process exists on this machine. Valid because the simulated nodes share it."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _running_pids(status: dict[str, Any] | None) -> list[int]:
    if not status or "job" not in status:
        return []
    return sorted(i["pid"] for i in status["job"]["instances"] if i["state"] == "RUNNING")


def run_observer(args: argparse.Namespace) -> dict[str, Any]:
    if Path.cwd().resolve() != ROOT:
        raise SystemExit(f"run the experiments from {ROOT}: Ray workers import ray_runtime from there")

    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 0, "include_dashboard": True})
    cluster.add_node(num_cpus=args.cpus)
    ray.init(address=cluster.address, logging_level="ERROR")
    cluster.wait_for_nodes()
    meta = {
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "mode": "observer process owning a simulated cluster; controller in a separate process",
        "cycle_seconds": args.cycle,
        "probe_timeout_s": args.probe_timeout,
    }
    print(f"{PREFIX} ==== crash and restart of the controller ====")
    print(f"{PREFIX} ray {meta['ray_version']} | python {meta['python_version']}")

    status_dir = ROOT / ".restart-status"
    status_dir.mkdir(exist_ok=True)
    first_file, second_file = status_dir / "session1.json", status_dir / "session2.json"
    for path in (first_file, second_file):
        path.unlink(missing_ok=True)

    try:
        first = _spawn(args, cluster.address, first_file, 1)
        before = _wait_status(first_file, lambda s: s.get("counts", {}).get("RUNNING") == JOB_SPEC["replicas"], args.deadline)
        pids_before = _running_pids(before)
        alive_before = alive_tickers() or []
        print(f"{PREFIX} session 1 (pid {first.pid}) runs the job: instance pids {pids_before}")

        crash_at = time.monotonic()
        os.kill(first.pid, signal.SIGKILL)
        first.wait()
        print(f"{PREFIX} controller killed with SIGKILL")

        # What happens to the instances while no controller is alive?
        instances_gone_after = None
        started = time.monotonic()
        while time.monotonic() - started < args.deadline:
            alive = alive_tickers() or []
            if not alive:
                instances_gone_after = round(time.monotonic() - crash_at, 3)
                break
            time.sleep(0.2)
        alive_while_down = alive_tickers() or []
        processes_gone_after = None
        while time.monotonic() - started < args.deadline:
            if not any(_pid_alive(pid) for pid in pids_before):
                processes_gone_after = round(time.monotonic() - crash_at, 3)
                break
            time.sleep(0.05)
        print(f"{PREFIX} instances alive with no controller: {[a['pid'] for a in alive_while_down]}; "
              f"Ray reports them gone {instances_gone_after} s after the crash; "
              f"their processes are gone {processes_gone_after} s after it")

        restart_at = time.monotonic()
        second = _spawn(args, cluster.address, second_file, 2)
        recovered = _wait_status(second_file, lambda s: s.get("phase") in ("steady", "stopping", "stopped"), args.deadline)
        recovery_seconds = round((recovered or {}).get("monotonic", restart_at) - restart_at, 3) if recovered else None
        pids_after = _running_pids(recovered)
        print(f"{PREFIX} session 2 (pid {second.pid}): job known after restart: "
              f"{(recovered or {}).get('job_known_after_restart')} ({(recovered or {}).get('get_job_error')}); "
              f"instance pids after resubmitting {pids_after}")
        second.wait(timeout=args.deadline * 3)
        final = _read_status(second_file)

        cpus_back = wait_for_free_cpus(args.cpus, 30.0)
        alive_after = alive_tickers()
        ids_before = sorted(i["instance_id"] for i in (before or {}).get("job", {}).get("instances", []))
        ids_after = sorted(i["instance_id"] for i in (final or {}).get("job", {}).get("instances", []))
        reused_ids = sorted(set(ids_before) & set(ids_after))

        checks = [
            {
                "name": "before the crash the job runs with its goal",
                "ok": len(pids_before) == JOB_SPEC["replicas"] and sorted(a["pid"] for a in alive_before) == pids_before,
                "detail": f"pids {pids_before}, state API {[a['pid'] for a in alive_before]}",
            },
            {
                "name": "the instances do not survive their controller",
                "ok": instances_gone_after is not None and processes_gone_after is not None,
                "detail": f"state API: {instances_gone_after} s; processes: {processes_gone_after} s after the crash",
            },
            {
                "name": "the restarted controller does not know the job",
                "ok": (recovered or {}).get("job_known_after_restart") is False
                and (recovered or {}).get("get_job_error") == "JOB_NOT_FOUND",
                "detail": f"get_job after restart: {(recovered or {}).get('get_job_error')}",
            },
            {
                "name": "resubmitting the job restores the goal with new processes",
                "ok": len(pids_after) == JOB_SPEC["replicas"] and not set(pids_after) & set(pids_before),
                "detail": f"pids before {pids_before}, after {pids_after}",
            },
            {
                "name": "the second session stops cleanly",
                "ok": (final or {}).get("phase") == "stopped" and second.returncode == 0 and alive_after == [] and cpus_back is not None,
                "detail": f"exit code {second.returncode}, alive tickers {alive_after}, CPUs back after {cpus_back} s",
            },
        ]
        print(f"{PREFIX} attempt ids reused by the new session: {reused_ids}")
        return {
            "meta": meta,
            "pids_before": pids_before,
            "instances_gone_after_s": instances_gone_after,
            "processes_gone_after_s": processes_gone_after,
            "recovery_after_restart_s": recovery_seconds,
            "pids_after": pids_after,
            "reused_attempt_ids": reused_ids,
            "session1_last_status": before,
            "session2_final_status": final,
            "checks": checks,
        }
    finally:
        for path in (first_file, second_file):
            path.unlink(missing_ok=True)
        status_dir.rmdir() if status_dir.exists() and not any(status_dir.iterdir()) else None
        ray.shutdown()
        cluster.shutdown()


def main() -> int:
    args = build_parser().parse_args()
    if args.role == "controller":
        return run_controller(args)
    results = run_observer(args)
    failed = print_checks(PREFIX, results["checks"])
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"{PREFIX} results written to {args.output}")
    if failed:
        print(f"{PREFIX} FAILED: {failed}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
