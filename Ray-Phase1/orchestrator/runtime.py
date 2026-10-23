"""What the controller exchanges with a runtime, independent of any concrete runtime.

A runtime starts, probes and terminates the process of each instance. It
reports what it saw; it never decides what that means for the attempt.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple, Protocol

from .model import JobSpec

# Outcomes of one probe.
HEALTHY = "healthy"  # the process answered
UNRESPONSIVE = "unresponsive"  # no usable answer within the deadline: uncertainty, not death
DEAD = "dead"  # the runtime confirmed that the process is gone
MISSING = "missing"  # the runtime never started a process for this instance

# Outcomes of one termination request.
KILL_REQUESTED = "kill_requested"  # asked; the death is confirmed later by a probe
ALREADY_GONE = "already_gone"  # there was no live process to end


@dataclass(frozen=True)
class InstanceSpec:
    """What a runtime needs in order to start one instance."""

    interval_ms: int
    cpu: float = 1.0
    node_label: str | None = None

    @classmethod
    def from_job_spec(cls, spec: JobSpec) -> InstanceSpec:
        return cls(interval_ms=int(spec.args["interval_ms"]), cpu=float(spec.cpu), node_label=spec.node_label)


class Observation(NamedTuple):
    """The answer of one instance to one probe."""

    instance_id: str
    outcome: str
    pid: int | None = None
    node_id: str | None = None
    progress: int | None = None
    actor_id: str | None = None
    started_at: float | None = None
    last_tick_at: float | None = None
    detail: str = ""


class Termination(NamedTuple):
    """The result of asking for the end of one instance."""

    instance_id: str
    outcome: str
    detail: str = ""


class Runtime(Protocol):
    def start(self, instance_id: str, spec: InstanceSpec) -> None: ...

    def probe_all(self, instance_ids: list[str]) -> dict[str, Observation]: ...

    def terminate(self, instance_id: str) -> Termination: ...
