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
    FINAL_STATES,
    REPLICA_STATES,
    ErrorCode,
    Instance,
    InstanceState,
    Job,
    JobSpec,
    OrchestratorError,
    ReconcileActions,
    _is_int,
)

Clock = Callable[[], datetime]

DEFAULT_MAX_MISSED_PROBES = 3

# Scale-down order: attempts that are not serving yet go first; RUNNING ones go last.
_STOP_PRIORITY = {InstanceState.PENDING: 0, InstanceState.UNKNOWN: 1, InstanceState.RUNNING: 2}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Orchestrator:
    def __init__(self, clock: Clock = _utc_now, max_missed_probes: int = DEFAULT_MAX_MISSED_PROBES):
        if not (_is_int(max_missed_probes) and max_missed_probes >= 1):
            raise ValueError("max_missed_probes must be a positive integer")
        self._clock = clock
        self._max_missed_probes = max_missed_probes
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

    def set_replicas(self, job_id: str, replicas: int) -> int:
        """Sets the absolute goal and returns the accepted value. Repeating it changes nothing.

        Only the goal changes here; instances are started or stopped by the next reconcile pass.
        """
        job = self._job(job_id)
        if not (_is_int(replicas) and replicas >= 0):
            raise OrchestratorError(ErrorCode.INVALID_ARGUMENT, "replicas must be a non-negative integer")
        if replicas != job.desired_replicas:
            self._record("replicas_set", job_id=job_id, from_replicas=job.desired_replicas, replicas=replicas)
            job.desired_replicas = replicas
        return job.desired_replicas

    def stop_job(self, job_id: str) -> int:
        """Same as setting zero replicas; the job stays registered and can be scaled up again."""
        return self.set_replicas(job_id, 0)

    # --- Controller-side operations -----------------------------------------------

    def job_ids(self) -> list[str]:
        return list(self._jobs)

    def job_spec(self, job_id: str) -> JobSpec:
        """Spec the runtime layer needs to start a process (workload, CPU, node label)."""
        return self._job(job_id).spec

    def reconcile(self, job_id: str) -> ReconcileActions:
        """Compares goal and attempts, records the decisions, and returns what the runtime must do.

        Running it again without new observations returns no actions: pending and
        uncertain attempts already count toward the goal.
        """
        job = self._job(job_id)
        kill: list[str] = []

        # Give up on attempts that stayed silent too long. Their process is killed before
        # being replaced, so a slow original and its replacement are never both active.
        for instance in job.instances:
            if instance.state is InstanceState.UNKNOWN and instance.missed_probes >= self._max_missed_probes:
                self._decide(instance, InstanceState.FAILED, "given_up_after_missed_probes")
                kill.append(instance.instance_id)

        replicas = [(index, i) for index, i in enumerate(job.instances) if i.state in REPLICA_STATES]
        missing = job.desired_replicas - len(replicas)

        start: list[str] = []
        if missing > 0:
            start = [self._add_instance(job) for _ in range(missing)]
        elif missing < 0:
            # Among attempts in the same state, the newest goes first.
            ordered = sorted(replicas, key=lambda pair: (_STOP_PRIORITY[pair[1].state], -pair[0]))
            for _, instance in ordered[:-missing]:
                self._decide(instance, InstanceState.STOPPING, "scale_down")
                kill.append(instance.instance_id)

        return ReconcileActions(start=tuple(start), kill=tuple(kill))

    def report_healthy(self, instance_id: str, *, pid: int, node_id: str, progress: int) -> None:
        """The attempt answered a health probe."""
        instance = self._instances[instance_id]
        if instance.state in FINAL_STATES:
            # Late answer from an attempt already given up or stopped: it must not revive it.
            self._record("stale_report_ignored", job_id=instance.job_id, instance_id=instance_id)
            return
        changes: dict[str, Any] = {}
        if instance.state in (InstanceState.PENDING, InstanceState.UNKNOWN):
            changes.update(self._move(instance, InstanceState.RUNNING))
        instance.missed_probes = 0
        for name, value in (("pid", pid), ("node_id", node_id), ("progress", progress)):
            if value != getattr(instance, name):
                setattr(instance, name, value)
                changes[name] = value
        self._observed(instance, changes)

    def report_unresponsive(self, instance_id: str) -> None:
        """A health probe got no answer within its deadline. A timeout alone does not prove death."""
        instance = self._instances[instance_id]
        if instance.state is InstanceState.RUNNING:
            changes = self._move(instance, InstanceState.UNKNOWN)
            instance.missed_probes = 1
            self._observed(instance, {**changes, "missed_probes": 1})
        elif instance.state is InstanceState.UNKNOWN:
            instance.missed_probes += 1
            self._observed(instance, {"missed_probes": instance.missed_probes})
        # PENDING may still be waiting for resources; STOPPING and final states need nothing.

    def report_death(self, instance_id: str) -> None:
        """The runtime confirmed that the attempt's process is gone."""
        instance = self._instances[instance_id]
        if instance.state in FINAL_STATES:
            return
        # A death after a requested stop is the stop completing, never a failure.
        target = InstanceState.STOPPED if instance.state is InstanceState.STOPPING else InstanceState.FAILED
        self._observed(instance, self._move(instance, target))

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

    def _add_instance(self, job: Job) -> str:
        """Creates a PENDING attempt with ID "<job_id>/<n>"; n is never reused within a job."""
        job_id = job.spec.job_id
        instance = Instance(instance_id=f"{job_id}/{job.next_attempt}", job_id=job_id)
        job.next_attempt += 1
        job.instances.append(instance)
        self._instances[instance.instance_id] = instance
        self._record("instance_requested", job_id=job_id, instance_id=instance.instance_id)
        return instance.instance_id

    def _move(self, instance: Instance, state: InstanceState) -> dict[str, Any]:
        from_state = instance.state
        instance.move_to(state)
        return {"from_state": from_state.value, "state": state.value}

    def _observed(self, instance: Instance, changes: dict[str, Any]) -> None:
        if changes:
            self._record("instance_observed", job_id=instance.job_id, instance_id=instance.instance_id, **changes)

    def _decide(self, instance: Instance, state: InstanceState, reason: str) -> None:
        changes = self._move(instance, state)
        self._record("instance_decision", job_id=instance.job_id, instance_id=instance.instance_id, reason=reason, **changes)

    def _record(self, event: str, **fields: Any) -> None:
        self._events.append({"t": _iso(self._clock()), "event": event, **fields})
