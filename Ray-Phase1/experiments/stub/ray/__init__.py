"""A double of the Ray module, good enough to exercise our code without Ray.

It is a development instrument, not evidence. There are no processes, no
scheduler and no real resources: the double never refuses an actor and never
runs anything in another process. What it does emulate is the API surface that
``ray_runtime.py`` uses, so that a mistake in our code shows up without a
cluster.

Two things are emulated with care, because the day 2 claims depend on them:

* every actor gets its own pid, so "two instances, two processes" can be
  checked without a cluster;
* an actor that is killed stops answering and gives its logical CPU back.
"""

from __future__ import annotations

import itertools
import os
from typing import Any

from .exceptions import GetTimeoutError, RayActorError, RayError

__version__ = "0.0.0-stub"

__all__ = [
    "GetTimeoutError",
    "RayActorError",
    "RayError",
    "available_resources",
    "cluster_resources",
    "get",
    "get_runtime_context",
    "init",
    "kill",
    "remote",
    "shutdown",
]

_NODE_ID = "node-stub"
_actor_ids = itertools.count(1)
_pids = itertools.count(4200)

_cluster_resources: dict[str, float] = {}
_available: dict[str, float] = {}
_actors: dict[str, "_ActorHandle"] = {}


class _RuntimeContext:
    """The context a call runs in. Inside an actor it reports that actor."""

    def __init__(self) -> None:
        self.node_id = _NODE_ID
        self.actor_id: str | None = None
        self.worker_id = "worker-stub"

    def get_node_id(self) -> str:
        return self.node_id

    def get_actor_id(self) -> str:
        if self.actor_id is None:
            raise RuntimeError("this call is not running inside an actor")
        return self.actor_id

    def get_worker_id(self) -> str:
        return self.worker_id


_context = _RuntimeContext()
_real_getpid = os.getpid


def get_runtime_context() -> _RuntimeContext:
    return _context


class _Ref:
    """A call that has been requested but not waited for."""

    def __init__(self, call: Any) -> None:
        self._call = call


class _Method:
    """A method of an actor, called as ``handle.method.remote(...)``."""

    def __init__(self, fn: Any, handle: "_ActorHandle") -> None:
        self._fn = fn
        self._handle = handle

    def remote(self, *args: Any, **kwargs: Any) -> _Ref:
        return _Ref(lambda: self._handle._invoke(self._fn, args, kwargs))


class _ActorHandle:
    """Stands for the process of an actor: it has an id, a pid and a node."""

    def __init__(self, cls: type, options: dict[str, Any], args: tuple, kwargs: dict) -> None:
        self._cls = cls
        self._options = options
        self._args = args
        self._kwargs = kwargs
        self._instance = cls(*args, **kwargs)
        self.actor_id = f"stub-actor-{next(_actor_ids):04d}"
        self.pid = next(_pids)
        self.node_id = _NODE_ID
        self.alive = True
        self.hung = False

    def _invoke(self, fn: Any, args: tuple, kwargs: dict) -> Any:
        if not self.alive:
            raise RayActorError(f"actor {self.actor_id} is not alive")
        if self.hung:
            raise GetTimeoutError(f"actor {self.actor_id} did not answer")
        previous_node, previous_actor = _context.node_id, _context.actor_id
        _context.node_id = self.node_id
        _context.actor_id = self.actor_id
        os.getpid = lambda: self.pid
        try:
            return fn(*args, **kwargs)
        finally:
            os.getpid = _real_getpid
            _context.node_id = previous_node
            _context.actor_id = previous_actor

    def __getattr__(self, name: str) -> _Method:
        if name.startswith("_"):
            raise AttributeError(name)
        return _Method(getattr(self._instance, name), self)


class _ActorClass:
    """What ``ray.remote`` returns: a class that can be adjusted and then started."""

    def __init__(self, cls: type, options: dict[str, Any]) -> None:
        self._cls = cls
        self._options = options

    def options(self, **overrides: Any) -> "_ActorClass":
        merged = dict(self._options)
        merged.update(overrides)
        return _ActorClass(self._cls, merged)

    def remote(self, *args: Any, **kwargs: Any) -> _ActorHandle:
        handle = _ActorHandle(self._cls, self._options, args, kwargs)
        _actors[handle.actor_id] = handle
        _reserve(self._options)
        return handle


def remote(*args: Any, **kwargs: Any) -> Any:
    """Declares an actor. Same shape as Ray's decorator."""
    if len(args) == 1 and not kwargs and isinstance(args[0], type):
        return _ActorClass(args[0], {})

    def decorator(cls: type) -> _ActorClass:
        return _ActorClass(cls, dict(kwargs))

    return decorator


def get(ref: Any, timeout: float | None = None) -> Any:
    """Waits for a call. The double answers immediately or raises."""
    if isinstance(ref, (list, tuple)):
        return [get(item, timeout=timeout) for item in ref]
    return ref._call()


def kill(actor: _ActorHandle) -> None:
    """Ends the process of an actor and gives its logical CPU back."""
    actor.alive = False
    _actors.pop(actor.actor_id, None)
    _release(actor._options)


def init(num_cpus: int = 1, **kwargs: Any) -> dict[str, Any]:
    """Starts a cluster with the given logical resources."""
    global _cluster_resources, _available
    _cluster_resources = {"CPU": float(num_cpus)}
    for name, amount in (kwargs.get("resources") or {}).items():
        _cluster_resources[name] = float(amount)
    _available = dict(_cluster_resources)
    return {}


def shutdown() -> None:
    """Ends the cluster and every actor in it."""
    global _available
    for handle in list(_actors.values()):
        handle.alive = False
    _actors.clear()
    _available = dict(_cluster_resources)


def cluster_resources() -> dict[str, float]:
    return dict(_cluster_resources)


def available_resources() -> dict[str, float]:
    return dict(_available)


def _reserve(options: dict[str, Any]) -> None:
    cpu = float(options.get("num_cpus", 0) or 0)
    if cpu:
        _available["CPU"] = _available.get("CPU", 0.0) - cpu
    for name, amount in (options.get("resources") or {}).items():
        _available[name] = _available.get(name, 0.0) - float(amount)


def _release(options: dict[str, Any]) -> None:
    cpu = float(options.get("num_cpus", 0) or 0)
    if cpu:
        _available["CPU"] = _available.get("CPU", 0.0) + cpu
    for name, amount in (options.get("resources") or {}).items():
        _available[name] = _available.get(name, 0.0) + float(amount)
