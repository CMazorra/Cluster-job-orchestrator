"""Domain operations over an in-memory registry.

State lives only in this process: closing the controller loses the session.
Operations are called sequentially; there is no protection against concurrent writers.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .model import (
    ACTIVE_STATES,
    ErrorCode,
    Instance,
    InstanceState,
    Job,
    JobSpec,
    OrchestratorError,
)

Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Orchestrator:
    def __init__(self, clock: Clock = _utc_now):
        self._clock = clock
        self._jobs: dict[str, Job] = {}
        self._instances: dict[str, Instance] = {}
        self._events: list[dict[str, Any]] = []

    # --- Client operations --------------------------------------------------------

    def submit_job(self, spec: dict[str, Any]) -> str:
        """Registers the goal for this session. Resubmitting the same spec is safe."""
        parsed = JobSpec.from_dict(spec)
        existing = self._jobs.get(parsed.job_id)
        if existing is not None:
            if existing.spec == parsed:
                self._record("job_submit_repeated", job_id=parsed.job_id)
                return parsed.job_id
            raise OrchestratorError(
                ErrorCode.JOB_ALREADY_EXISTS,
                f"job {parsed.job_id!r} already exists with a different spec",
            )
        self._jobs[parsed.job_id] = Job(spec=parsed, desired_replicas=parsed.replicas)
        self._record("job_submitted", job_id=parsed.job_id, spec=parsed.to_dict())
        return parsed.job_id

    def get_job(self, job_id: str) -> dict[str, Any]:
        job = self._job(job_id)
        counted = (InstanceState.RUNNING, InstanceState.PENDING, InstanceState.UNKNOWN, InstanceState.STOPPING)
        return {
            "job_id": job_id,
            "desired_replicas": job.desired_replicas,
            "observed": {
                state.value.lower(): sum(1 for i in job.instances if i.state is state) for state in counted
            },
            "instances": [instance.to_dict() for instance in job.instances],
            "observed_at": _iso(self._clock()),
        }

    # --- Controller-side operations -----------------------------------------------

    def add_instance(self, job_id: str) -> str:
        """Creates a PENDING attempt with ID "<job_id>/<n>"; n is never reused within a job."""
        job = self._job(job_id)
        instance = Instance(instance_id=f"{job_id}/{job.next_attempt}", job_id=job_id)
        job.next_attempt += 1
        job.instances.append(instance)
        self._instances[instance.instance_id] = instance
        self._record("instance_requested", job_id=job_id, instance_id=instance.instance_id)
        return instance.instance_id

    def update_instance(
        self,
        instance_id: str,
        *,
        state: InstanceState | None = None,
        pid: int | None = None,
        node_id: str | None = None,
        progress: int | None = None,
    ) -> None:
        """Stores an observation of the attempt and records what changed in the history.

        State transitions are not validated yet.
        """
        instance = self._instances[instance_id]
        changes: dict[str, Any] = {}
        if state is not None and state is not instance.state:
            changes["from_state"] = instance.state.value
            changes["state"] = state.value
            instance.state = state
        for name, value in (("pid", pid), ("node_id", node_id), ("progress", progress)):
            if value is not None and value != getattr(instance, name):
                setattr(instance, name, value)
                changes[name] = value
        if changes:
            self._record("instance_observed", job_id=instance.job_id, instance_id=instance_id, **changes)

    def active_instances(self, job_id: str) -> list[str]:
        return [i.instance_id for i in self._job(job_id).instances if i.state in ACTIVE_STATES]

    # --- Evidence -----------------------------------------------------------------

    def events(self) -> list[dict[str, Any]]:
        """Session history linking job, attempt and process."""
        return [dict(event) for event in self._events]

    def dump_events(self, path: str | Path) -> None:
        with open(path, "w", encoding="utf-8") as out:
            for event in self._events:
                out.write(json.dumps(event, ensure_ascii=False) + "\n")

    # --- Helpers ------------------------------------------------------------------

    def _job(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise OrchestratorError(ErrorCode.JOB_NOT_FOUND, f"job {job_id!r} does not exist")
        return job

    def _record(self, event: str, **fields: Any) -> None:
        self._events.append({"t": _iso(self._clock()), "event": event, **fields})
