"""Ray implementation of the runtime: the ticker workload and the operations on its actors.

One instance is one Ray actor. This module knows how to start the actor of an
instance, how to ask it how it is doing and how to end it. It does not decide
what an answer means: that belongs to the controller and the domain.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any

import ray
from ray.exceptions import ActorUnavailableError, RayActorError

from orchestrator.runtime import (
    ALREADY_GONE,
    DEAD,
    HEALTHY,
    KILL_REQUESTED,
    MISSING,
    UNRESPONSIVE,
    InstanceSpec,
    Observation,
    Termination,
)

DEFAULT_PROBE_TIMEOUT_S = 2.0

# Units of a node-label resource that one instance takes. Nodes declare a label
# with a large capacity, so the label restricts placement without limiting count.
LABEL_UNITS = 1


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class Ticker:
    """The test workload: a counter that advances on its own.

    The counter lives in the actor's own process, so it is lost when that
    process dies. ``status`` answers immediately even while the counter is
    advancing, because the counting happens in a background thread.
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
            "actor_id": str(context.get_actor_id()),
            "started_at": self._started_at,
            "last_tick_at": self._last_tick_at,
        }

    def crash(self) -> None:
        """Ends this process on purpose. The controller sees it as an unexpected death."""
        os._exit(1)


class RayRuntime:
    """Starts, probes and terminates the actor of each instance.

    The handle of an actor is kept until a probe confirms its death, so a kill
    request and a confirmed death are never confused.
    """

    def __init__(self, *, probe_timeout: float = DEFAULT_PROBE_TIMEOUT_S) -> None:
        self._probe_timeout = probe_timeout
        self._actors: dict[str, Any] = {}
        self._dead: set[str] = set()

    @property
    def probe_timeout(self) -> float:
        return self._probe_timeout

    def start(self, instance_id: str, spec: InstanceSpec) -> None:
        """Requests the actor of one instance. It does not wait for it to be ready."""
        if instance_id in self._actors or instance_id in self._dead:
            raise ValueError(f"instance {instance_id!r} was already started")
        options: dict[str, Any] = {"num_cpus": spec.cpu}
        if spec.node_label:
            options["resources"] = {spec.node_label: LABEL_UNITS}
        self._actors[instance_id] = Ticker.options(**options).remote(interval_ms=spec.interval_ms)

    def probe_all(self, instance_ids: list[str]) -> dict[str, Observation]:
        """Asks every instance how it is doing, all at once, with one shared deadline.

        Submitting every query before waiting means that N silent instances cost
        one timeout per round, not N.
        """
        observations: dict[str, Observation] = {}
        queries: dict[Any, str] = {}
        for instance_id in instance_ids:
            actor = self._actors.get(instance_id)
            if actor is not None:
                queries[actor.status.remote()] = instance_id
            elif instance_id in self._dead:
                observations[instance_id] = Observation(instance_id, DEAD, detail="death already confirmed")
            else:
                observations[instance_id] = Observation(instance_id, MISSING, detail="never started")

        if queries:
            ready, _ = ray.wait(list(queries), num_returns=len(queries), timeout=self._probe_timeout)
            answered = set(ready)
            for ref, instance_id in queries.items():
                if ref in answered:
                    observations[instance_id] = self._read(instance_id, ref)
                else:
                    observations[instance_id] = Observation(
                        instance_id, UNRESPONSIVE, detail=f"no answer in {self._probe_timeout} s"
                    )
        return {instance_id: observations[instance_id] for instance_id in instance_ids}

    def probe(self, instance_id: str) -> Observation:
        return self.probe_all([instance_id])[instance_id]

    def terminate(self, instance_id: str) -> Termination:
        """Asks Ray to end the actor. Its death is confirmed later, by a probe."""
        actor = self._actors.get(instance_id)
        if actor is None:
            return Termination(instance_id, ALREADY_GONE, detail="no live actor")
        ray.kill(actor)
        return Termination(instance_id, KILL_REQUESTED)

    def shutdown(self) -> list[Termination]:
        """Requests the end of every actor whose death was not confirmed yet."""
        return [self.terminate(instance_id) for instance_id in list(self._actors)]

    def instances(self) -> list[str]:
        """Instances whose death Ray has not confirmed yet."""
        return list(self._actors)

    def _read(self, instance_id: str, ref: Any) -> Observation:
        try:
            data = ray.get(ref)
        except ActorUnavailableError as exc:
            # Temporarily unreachable: it may answer later, so this is not a death.
            # It must be caught first, because it is a subclass of RayActorError.
            return Observation(instance_id, UNRESPONSIVE, detail=type(exc).__name__)
        except RayActorError as exc:
            self._actors.pop(instance_id, None)
            self._dead.add(instance_id)
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
