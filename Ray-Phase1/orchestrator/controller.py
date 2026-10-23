"""Glue between the domain and a runtime: observations go in, decisions come out.

The controller decides nothing. It reports what the runtime observed, asks the
domain what to do, and asks the runtime to do it.
"""

from __future__ import annotations

from .api import Orchestrator
from .model import ReconcileActions
from .runtime import DEAD, HEALTHY, MISSING, InstanceSpec, Observation, Runtime


class RuntimeController:
    def __init__(self, orchestrator: Orchestrator, runtime: Runtime) -> None:
        self._orchestrator = orchestrator
        self._runtime = runtime
        self.starts: list[str] = []
        self.kills: list[str] = []

    def observe(self, job_id: str) -> dict[str, Observation]:
        """Probes every active attempt in one round and reports each answer to the domain."""
        instance_ids = self._orchestrator.active_instances(job_id)
        observations = self._runtime.probe_all(instance_ids)
        for instance_id in instance_ids:
            observation = observations[instance_id]
            if observation.outcome == HEALTHY and observation.pid is not None and observation.node_id is not None:
                self._orchestrator.report_healthy(
                    instance_id,
                    pid=observation.pid,
                    node_id=observation.node_id,
                    progress=observation.progress or 0,
                )
            elif observation.outcome in (DEAD, MISSING):
                self._orchestrator.report_death(instance_id)
            else:
                # No answer, or an answer without identity: uncertainty, never a death.
                self._orchestrator.report_unresponsive(instance_id)
        return observations

    def act(self, job_id: str) -> ReconcileActions:
        """Asks the domain what to do and asks the runtime to do it: kills first, then starts.

        A kill is only requested here. The death is reported when a later probe
        confirms it, so a stopping attempt stays STOPPING until then.
        """
        actions = self._orchestrator.reconcile(job_id)
        for instance_id in actions.kill:
            self._runtime.terminate(instance_id)
            self.kills.append(instance_id)
        spec = InstanceSpec.from_job_spec(self._orchestrator.job_spec(job_id))
        for instance_id in actions.start:
            self._runtime.start(instance_id, spec)
            self.starts.append(instance_id)
        return actions

    def step(self, job_id: str) -> ReconcileActions:
        """One control cycle: observe, then act."""
        self.observe(job_id)
        return self.act(job_id)
