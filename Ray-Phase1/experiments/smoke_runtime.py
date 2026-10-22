"""Smoke test of the day 2 flow, against the double of Ray.

It runs exactly the same walkthrough as ``day2_ticker.py``, but with the
double of ``ray`` instead of the real one. That proves the code runs, that the
checks are the ones we think they are, and that the error paths behave. It
proves nothing about Ray: the double has no processes, no scheduler and no
admission control.

The output of this script is not evidence and is not saved as such.

Usage (no Ray needed):

    python experiments/smoke_runtime.py
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE / "stub"))
sys.path.insert(0, str(HERE))

import day2_ticker

from ray_runtime import DEAD, MISSING, InstanceSpec, RayRuntime

PREFIX = "[smoke]"


def _report(results: dict) -> list[str]:
    failures = []
    for check in results["checks"]:
        print(f"{PREFIX} [{'OK' if check['ok'] else 'FAIL'}] {check['name']}: {check['detail']}")
        if not check["ok"]:
            failures.append(check["name"])
    return failures


def _error_paths() -> list[str]:
    """The paths the day 2 walkthrough does not reach, checked on their own."""
    failures = []
    runtime = RayRuntime(probe_timeout=0.5)
    runtime.start("smoke/1", InstanceSpec(interval_ms=50))

    missing = runtime.probe("smoke/never-started")
    print(f"{PREFIX} probe of an instance that was never started: {missing.outcome}")
    if missing.outcome != MISSING:
        failures.append("probe of an unknown instance should be 'missing'")

    runtime.terminate("smoke/1")
    again = runtime.terminate("smoke/1")
    print(f"{PREFIX} terminate of an already terminated instance: {again.outcome}")
    if again.outcome != "already_gone":
        failures.append("terminate of a terminated instance should be 'already_gone'")

    dead = runtime.probe("smoke/1")
    print(f"{PREFIX} probe of a terminated instance: {dead.outcome}")
    if dead.outcome != DEAD:
        failures.append("probe of a terminated instance should be 'dead'")

    return failures


def _degenerate_cases() -> list[str]:
    """The cases where an instance never came up. They must not break the script."""
    failures = []
    windows = {"a": {"started_at": 1.0, "last_tick_at": 3.0}, "b": {"started_at": None, "last_tick_at": None}}
    overlap, note = day2_ticker._overlap_seconds(windows)
    print(f"{PREFIX} overlap without a window: {overlap} s ({note})")
    if overlap != 0.0 or note is None:
        failures.append("an instance without a window must give no overlap and say why")

    empty, note = day2_ticker._overlap_seconds({})
    print(f"{PREFIX} overlap without instances: {empty} s ({note})")
    if empty != 0.0 or note is None:
        failures.append("without instances the overlap must be zero and say why")

    instances = [{"progress_first": None, "progress_last": None}]
    if day2_ticker._progress_advanced(instances):
        failures.append("an instance without progress must not count as advanced")
    else:
        print(f"{PREFIX} [OK] an instance without progress does not count as advanced")

    instances = [{"progress_first": 0, "progress_last": 3}]
    if not day2_ticker._progress_advanced(instances):
        failures.append("an instance that counted should count as advanced")
    else:
        print(f"{PREFIX} [OK] an instance that counted counts as advanced")

    return failures


def main() -> int:
    print(f"{PREFIX} ==== smoke test with the double of ray ====")
    print(f"{PREFIX} no real processes, no real scheduler: this is not evidence")

    args = day2_ticker.build_parser().parse_args([])
    args.instances = 2
    args.cpus = 2
    args.samples = 2
    args.sample_seconds = 0.3
    args.startup_timeout = 5.0

    failures = _report(day2_ticker.run_experiment(args))
    failures += _error_paths()
    failures += _degenerate_cases()

    if "orchestrator" in sys.modules:
        print(f"{PREFIX} [FAIL] the Ray layer must not import the domain")
        failures.append("the Ray layer must not import the domain")
    else:
        print(f"{PREFIX} [OK] the Ray layer does not import the domain")

    if failures:
        print(f"{PREFIX} FAILED: {failures}")
        return 1
    print(f"{PREFIX} done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
