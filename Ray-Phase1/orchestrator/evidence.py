"""Timelines computed from the session history.

Durations use the monotonic "m" stamp of each event, never the wall-clock "t",
because the wall clock can jump while the system runs.
"""

from __future__ import annotations

from typing import Any


def _first(events: list[dict[str, Any]], **match: Any) -> dict[str, Any] | None:
    for event in events:
        if all(event.get(key) == value for key, value in match.items()):
            return event
    return None


def _seconds(start: dict[str, Any] | None, end: dict[str, Any] | None) -> float | None:
    if start is None or end is None:
        return None
    return round(end["m"] - start["m"], 3)


def crash_timeline(events: list[dict[str, Any]], instance_id: str) -> dict[str, Any]:
    """What happened after a fault was injected into ``instance_id``.

    Each step is None when it did not happen. The steps are:

    * injected: the ``fault_injected`` annotation for the attempt;
    * detected: the domain recorded the attempt as FAILED;
    * replacement requested: an attempt was created to replace it;
    * replacement running: that attempt answered a health probe.
    """
    injected = _first(events, event="annotation", kind="fault_injected", instance_id=instance_id)
    detected = _first(events, event="instance_observed", instance_id=instance_id, state="FAILED")
    requested = _first(events, event="instance_requested", replaces=instance_id)
    replacement_id = requested["instance_id"] if requested else None
    running = (
        _first(events, event="instance_observed", instance_id=replacement_id, state="RUNNING")
        if replacement_id
        else None
    )
    return {
        "instance_id": instance_id,
        "replacement_id": replacement_id,
        "detection_s": _seconds(injected, detected),
        "replacement_request_s": _seconds(detected, requested),
        "recovery_s": _seconds(detected, running),
        "total_s": _seconds(injected, running),
    }
