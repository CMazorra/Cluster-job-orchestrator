"""Domain model: job specification, instance attempts and client-facing errors.

Independent of Ray.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    INVALID_SPEC = "INVALID_SPEC"  # the submitted job spec is malformed
    INVALID_ARGUMENT = "INVALID_ARGUMENT"  # an operation argument other than a spec is invalid
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_ALREADY_EXISTS = "JOB_ALREADY_EXISTS"


class OrchestratorError(Exception):
    """Domain error shown to the client; never wraps Ray exceptions."""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(f"{code.value}: {message}")
        self.code = code
        self.message = message


class InstanceState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


# Attempts that may still have a live process: counted in "observed" and polled by the controller.
ACTIVE_STATES = frozenset(
    {InstanceState.PENDING, InstanceState.RUNNING, InstanceState.UNKNOWN, InstanceState.STOPPING}
)

# Attempts that count toward the desired replicas. Pending and uncertain attempts count too,
# so that reconciliation does not create duplicates; STOPPING ones are already leaving.
REPLICA_STATES = frozenset({InstanceState.PENDING, InstanceState.RUNNING, InstanceState.UNKNOWN})

ALLOWED_TRANSITIONS = {
    InstanceState.PENDING: frozenset({InstanceState.RUNNING, InstanceState.FAILED, InstanceState.STOPPING}),
    InstanceState.RUNNING: frozenset({InstanceState.UNKNOWN, InstanceState.FAILED, InstanceState.STOPPING}),
    InstanceState.UNKNOWN: frozenset({InstanceState.RUNNING, InstanceState.FAILED, InstanceState.STOPPING}),
    InstanceState.STOPPING: frozenset({InstanceState.STOPPED}),
    InstanceState.STOPPED: frozenset(),
    InstanceState.FAILED: frozenset(),
}

FINAL_STATES = frozenset({InstanceState.STOPPED, InstanceState.FAILED})

# No "/": an attempt ID is "<job_id>/<n>" and must split without ambiguity.
JOB_ID_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,64}")


def _invalid(message: str) -> OrchestratorError:
    return OrchestratorError(ErrorCode.INVALID_SPEC, message)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_positive_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _check_object(obj: Any, where: str, required: set[str], optional: frozenset[str] = frozenset()) -> None:
    if not isinstance(obj, dict):
        raise _invalid(f"{where} must be an object")
    missing = required - obj.keys()
    if missing:
        raise _invalid(f"{where}: missing fields {sorted(missing)}")
    unknown = obj.keys() - required - optional
    if unknown:
        raise _invalid(f"{where}: unknown fields {sorted(unknown)}")


def _validate_ticker_args(args: dict[str, Any]) -> None:
    _check_object(args, "workload.args", required={"interval_ms"})
    if not (_is_int(args["interval_ms"]) and args["interval_ms"] > 0):
        raise _invalid("workload.args.interval_ms must be a positive integer")


# Workloads shipped with the project; arbitrary commands are never executed.
WORKLOADS = {"ticker": _validate_ticker_args}


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    entrypoint: str
    args: dict[str, Any]
    replicas: int
    cpu: float
    node_label: str | None = None

    @classmethod
    def from_dict(cls, data: Any) -> JobSpec:
        _check_object(
            data,
            "spec",
            required={"job_id", "workload", "replicas", "resources"},
            optional=frozenset({"placement"}),
        )

        job_id = data["job_id"]
        if not (isinstance(job_id, str) and JOB_ID_PATTERN.fullmatch(job_id)):
            raise _invalid("job_id must have 1-64 characters from A-Z, a-z, 0-9, '.', '_' and '-'")

        workload = data["workload"]
        _check_object(workload, "workload", required={"entrypoint", "args"})
        entrypoint = workload["entrypoint"]
        if entrypoint not in WORKLOADS:
            raise _invalid(f"unknown workload: {entrypoint!r}")
        WORKLOADS[entrypoint](workload["args"])

        replicas = data["replicas"]
        if not (_is_int(replicas) and replicas >= 0):
            raise _invalid("replicas must be a non-negative integer")

        resources = data["resources"]
        _check_object(resources, "resources", required={"cpu"})
        if not _is_positive_number(resources["cpu"]):
            raise _invalid("resources.cpu must be a positive number")

        node_label = None
        if "placement" in data:
            placement = data["placement"]
            _check_object(placement, "placement", required={"node_label"})
            node_label = placement["node_label"]
            if not (isinstance(node_label, str) and node_label):
                raise _invalid("placement.node_label must be a non-empty string")

        return cls(
            job_id=job_id,
            entrypoint=entrypoint,
            args=dict(workload["args"]),
            replicas=replicas,
            cpu=resources["cpu"],
            node_label=node_label,
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "job_id": self.job_id,
            "workload": {"entrypoint": self.entrypoint, "args": dict(self.args)},
            "replicas": self.replicas,
            "resources": {"cpu": self.cpu},
        }
        if self.node_label is not None:
            data["placement"] = {"node_label": self.node_label}
        return data


@dataclass
class Instance:
    """One concrete attempt of a replica. A replacement is a new attempt with a new ID."""

    instance_id: str
    job_id: str
    state: InstanceState = InstanceState.PENDING
    pid: int | None = None
    node_id: str | None = None
    progress: int | None = None
    missed_probes: int = 0  # consecutive health probes without an answer while UNKNOWN

    def move_to(self, state: InstanceState) -> None:
        """Changes state; an illegal transition is a controller bug, not a client error."""
        if state not in ALLOWED_TRANSITIONS[self.state]:
            raise ValueError(f"illegal transition for {self.instance_id}: {self.state.value} -> {state.value}")
        self.state = state

    def to_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "state": self.state.value,
            "pid": self.pid,
            "node_id": self.node_id,
            "progress": self.progress,
        }


@dataclass
class Job:
    spec: JobSpec  # spec as originally submitted; replica changes do not modify it
    desired_replicas: int
    instances: list[Instance] = field(default_factory=list)
    next_attempt: int = 1


@dataclass(frozen=True)
class ReconcileActions:
    """What the runtime layer must do after a reconcile pass.

    The decisions are already recorded: new attempts exist as PENDING and the
    ones to kill are already STOPPING (scale-down) or FAILED (given up).
    """

    start: tuple[str, ...] = ()  # new attempts: create one process for each
    kill: tuple[str, ...] = ()  # attempts whose process must be terminated
