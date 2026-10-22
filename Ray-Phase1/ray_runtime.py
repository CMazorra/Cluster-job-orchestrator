"""Ray side of the orchestrator: the ticker workload and the operations on one instance.

This module belongs to Persona B. It knows how to create the process of an
instance, how to ask that process how it is doing and how to end it. It does
not know the domain: the caller says what to run, and the caller decides what
the answer means.

The domain lives in ``orchestrator/`` and is Ray-free. The glue that turns an
observation into a state change belongs to the controller, not here. The only
link between both sides is :class:`InstanceSpec`, which is built from the
domain's ``JobSpec`` without importing it.
"""

from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, NamedTuple

import ray
from ray.exceptions import GetTimeoutError, RayActorError

DEFAULT_PROBE_TIMEOUT_S = 2.0

HEALTHY = "healthy"
UNRESPONSIVE = "unresponsive"
DEAD = "dead"
MISSING = "missing"


@dataclass(frozen=True)
class InstanceSpec:
    """What the runtime needs in order to start one instance."""

    interval_ms: int
    cpu: float = 1.0
    node_label: str | None = None

    @classmethod
    def from_job_spec(cls, job_spec: Any) -> "InstanceSpec":
        """Builds the spec from the domain's ``JobSpec`` without importing the domain."""
        return cls(
            interval_ms=int(job_spec.args["interval_ms"]),
            cpu=float(job_spec.cpu),
            node_label=job_spec.node_label,
        )


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
    """The result of asking an instance to end."""

    instance_id: str
    outcome: str
    detail: str = ""


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class Ticker:
    """The test workload: a counter that advances on its own.

    The counter lives in the actor's own process, so it is lost when that
    process dies. ``status`` answers immediately even while the counter is
    advancing, because the counting happens in a background thread and not in
    the method.
    """

    def __init__(self, interval_ms: int) -> None:
        self._interval = interval_ms / 1000.0
        self._counter = 0
        self._started_at = time.time()
        self._last_tick_at: float | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._advance, name="ticker", daemon=True)
        self._thread.start()

    def _advance(self) -> None:
        while not self._stop.wait(self._interval):
            self._counter += 1
            self._last_tick_at = time.time()

    def status(self) -> dict[str, Any]:
        context = ray.get_runtime_context()
        return {
            "progress": self._counter,
            "pid": os.getpid(),
            "node_id": str(context.get_node_id()),
            "actor_id": _actor_id_of(context),
            "interval_ms": int(self._interval * 1000),
            "started_at": self._started_at,
            "last_tick_at": self._last_tick_at,
        }

    def crash(self) -> None:
        """Ends this process on purpose. The controller sees it as an unexpected death."""
        os._exit(1)


class RayRuntime:
    """Creates, observes and terminates the process of each instance.

    One instance is one Ray actor. The runtime keeps the handle of every actor
    it started, so an instance id always points to the same process.
    """

    def __init__(self, *, probe_timeout: float = DEFAULT_PROBE_TIMEOUT_S) -> None:
        self._probe_timeout = probe_timeout
        self._actors: dict[str, Any] = {}
        self._terminated: set[str] = set()

    @property
    def probe_timeout(self) -> float:
        return self._probe_timeout

    def start(self, instance_id: str, spec: InstanceSpec) -> None:
        """Requests the process of one instance. It does not wait for it to be ready.

        The actor is scheduled with the logical CPU of the instance and, if the
        instance asks for a node label, with that label as a custom resource.
        """
        if instance_id in self._actors:
            raise ValueError(f"instance {instance_id!r} is already running")
        options: dict[str, Any] = {"num_cpus": spec.cpu, "name": _actor_name(instance_id)}
        if spec.node_label:
            options["resources"] = {spec.node_label: 1}
        self._actors[instance_id] = Ticker.options(**options).remote(interval_ms=spec.interval_ms)

    def probe(self, instance_id: str) -> Observation:
        """Asks one instance how it is doing, with a deadline.

        A timeout is not a death: the outcome says "unresponsive" and the caller
        decides what that means for the state of the attempt.
        """
        actor = self._actors.get(instance_id)
        if actor is None:
            if instance_id in self._terminated:
                return Observation(instance_id, DEAD, detail="the process was already ended")
            return Observation(instance_id, MISSING, detail="no process was started for this instance")
        try:
            data = ray.get(actor.status.remote(), timeout=self._probe_timeout)
        except GetTimeoutError:
            return Observation(
                instance_id, UNRESPONSIVE, detail=f"no answer in {self._probe_timeout} s"
            )
        except RayActorError as exc:
            return Observation(instance_id, DEAD, detail=type(exc).__name__)
        return Observation(
            instance_id,
            HEALTHY,
            pid=data.get("pid"),
            node_id=data.get("node_id"),
            progress=data.get("progress"),
            actor_id=data.get("actor_id"),
            started_at=data.get("started_at"),
            last_tick_at=data.get("last_tick_at"),
        )

    def terminate(self, instance_id: str) -> Termination:
        """Ends the process of one instance so that it gives its resources back."""
        actor = self._actors.pop(instance_id, None)
        if actor is None:
            detail = (
                "the process was already ended"
                if instance_id in self._terminated
                else "no process was running"
            )
            return Termination(instance_id, "already_gone", detail=detail)
        self._terminated.add(instance_id)
        try:
            ray.kill(actor)
        except RayActorError as exc:
            return Termination(instance_id, "already_gone", detail=type(exc).__name__)
        return Termination(instance_id, "terminated")

    def shutdown(self) -> list[Termination]:
        """Ends every process that is still running."""
        return [self.terminate(instance_id) for instance_id in list(self._actors)]

    def instances(self) -> list[str]:
        """Ids of the instances whose process is still running."""
        return list(self._actors)


def _actor_id_of(context: Any) -> str | None:
    try:
        return str(context.get_actor_id())
    except Exception:
        return None


def _actor_name(instance_id: str) -> str:
    """A Ray actor name that is valid, readable and unique.

    The instance id contains a separator that a Ray name cannot contain, and
    two different ids can be read the same once sanitized, so a digest of the
    original id is appended.
    """
    readable = re.sub(r"[^A-Za-z0-9_-]", "-", instance_id)
    digest = hashlib.sha1(instance_id.encode("utf-8")).hexdigest()[:8]
    return f"ticker-{readable}-{digest}"
