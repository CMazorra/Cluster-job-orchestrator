"""The double of the state API: it lists the actors that are alive.

The real API returns objects with attributes, not dictionaries, so the double
does the same.
"""

from __future__ import annotations

from typing import Any


class _ActorInfo:
    def __init__(self, actor_id: str, class_name: str, name: str, node_id: str, pid: int) -> None:
        self.actor_id = actor_id
        self.class_name = class_name
        self.name = name
        self.node_id = node_id
        self.pid = pid
        self.state = "ALIVE"


def list_actors(filters: Any = None) -> list[_ActorInfo]:
    import ray

    return [
        _ActorInfo(
            actor_id=handle.actor_id,
            class_name=handle._cls.__name__,
            name=str(handle._options.get("name", "")),
            node_id=handle.node_id,
            pid=handle.pid,
        )
        for handle in ray._actors.values()
        if handle.alive
    ]
