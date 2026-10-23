"""Timelines computed from the session history.

Durations use the monotonic "m" stamp of each event, never the wall-clock "t",
because the wall clock can jump while the system runs.
"""

from __future__ import annotations

from typing import Any

# Events that can move an attempt to a new state: an observation reported by the
# runtime, or a decision taken by the controller (for example, giving up).
_STATE_EVENTS = ("instance_observed", "instance_decision")


def _first(events: list[dict[str, Any]], **match: Any) -> dict[str, Any] | None:
    for event in events:
        if all(event.get(key) == value for key, value in match.items()):
            return event
    return None


def _first_state(events: list[dict[str, Any]], instance_id: str | None, state: str) -> dict[str, Any] | None:
    for event in events:
        if event["event"] in _STATE_EVENTS and event.get("instance_id") == instance_id and event.get("state") == state:
            return event
    return None


def _seconds(start: dict[str, Any] | None, end: dict[str, Any] | None) -> float | None:
    if start is None or end is None:
        return None
    return round(end["m"] - start["m"], 3)


def fault_timeline(events: list[dict[str, Any]], instance_id: str) -> dict[str, Any]:
    """What happened after a fault was injected into ``instance_id``.

    Each step is None when it did not happen. The steps are:

    * injected: the ``fault_injected`` annotation for the attempt;
    * suspected: the attempt became UNKNOWN (a probe went unanswered);
    * failed: the attempt became FAILED, either because the runtime confirmed
      its death or because the controller gave up on it;
    * replacement requested: an attempt was created to replace it;
    * replacement running: that attempt answered a health probe.
    """
    injected = _first(events, event="annotation", kind="fault_injected", instance_id=instance_id)
    suspected = _first_state(events, instance_id, "UNKNOWN")
    failed = _first_state(events, instance_id, "FAILED")
    requested = _first(events, event="instance_requested", replaces=instance_id)
    replacement_id = requested["instance_id"] if requested else None
    running = _first_state(events, replacement_id, "RUNNING") if replacement_id else None
    return {
        "instance_id": instance_id,
        "replacement_id": replacement_id,
        "failed_by": failed["event"] if failed else None,
        "suspicion_s": _seconds(injected, suspected),
        "detection_s": _seconds(injected, failed),
        "replacement_request_s": _seconds(failed, requested),
        "recovery_s": _seconds(failed, running),
        "total_s": _seconds(injected, running),
    }
