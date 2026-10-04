"""The scheduler's replicated state machine.

This is the `apply_callback` every scheduler replica's RaftNode calls, in the
same log order, once a command is committed. It is the single source of
truth for jobs and executors — there is no separate database; consistency
across replicas comes entirely from the Raft log, not from this class.

Everything here must be a pure, deterministic function of (current state,
command). No wall-clock reads, no randomness, no I/O: any value that looks
like "now" or a new id is generated once by the leader *before* it proposes
the command, and travels inside the command payload so every replica (and a
replay after a restart) computes the exact same result.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from shared import Executor, Job, JobStatus

TERMINAL_STATUSES = (JobStatus.SUCCEEDED.value, JobStatus.FAILED.value)


class SchedulerStateMachine:
    def __init__(self) -> None:
        self.jobs: dict[str, Job] = {}
        self.executors: dict[str, Executor] = {}

    def apply(self, command: dict) -> Any:
        handler = getattr(self, f"_apply_{command['type']}", None)
        if handler is None:
            raise ValueError(f"unknown Raft command type: {command['type']!r}")
        return handler(command)

    # -- command handlers, one per command["type"] -----------------------

    def _apply_submit_job(self, command: dict) -> Job:
        job = Job(
            id=command["id"],
            docker_image=command["docker_image"],
            command=command["command"],
            cpu_cores=command["cpu_cores"],
            memory_gb=command["memory_gb"],
            gpu_count=command.get("gpu_count", 0),
            gpu_type=command.get("gpu_type"),
            enqueued_at=datetime.fromisoformat(command["enqueued_at"]),
            status=JobStatus.PENDING.value,
        )
        self.jobs[job.id] = job
        return job

    def _apply_register_executor(self, command: dict) -> Executor:
        executor = Executor(
            id=command["id"],
            ip_address=command["ip_address"],
            available_cpu_cores=command["available_cpu_cores"],
            available_memory_gb=command["available_memory_gb"],
            available_gpu_count=command["available_gpu_count"],
            gpu_type=command.get("gpu_type"),
            data_center=command["data_center"],
        )
        self.executors[executor.id] = executor
        return executor

    def _apply_deregister_executor(self, command: dict) -> None:
        self.executors.pop(command["id"], None)
        return None

    def _apply_assign_job(self, command: dict) -> Job:
        job = self.jobs[command["job_id"]]
        if job.executor is not None:
            # Two scheduling passes raced on the same pending job (e.g. the
            # inline post-submit attempt and the periodic fallback pass);
            # whichever assign_job committed first wins, this one is a no-op
            # rather than a double resource deduction.
            raise ValueError(f"job {job.id} is already assigned to {job.executor.id}")
        executor = self.executors[command["executor_id"]]
        executor.deduct_resources(job)
        job.executor = executor.model_copy()
        return job

    def _apply_update_job_status(self, command: dict) -> Job:
        job = self.jobs[command["job_id"]]
        if job.status in TERMINAL_STATUSES:
            # Terminal is final: an executor's late/duplicate status report
            # (e.g. a RUNNING report that arrives after an abort already
            # marked the job FAILED) must not resurrect or re-release it.
            return job
        status = command["status"]
        timestamp = datetime.fromisoformat(command["timestamp"])

        job.status = status
        if status == JobStatus.RUNNING.value:
            job.started_at = timestamp
        elif status in TERMINAL_STATUSES:
            job.finished_at = timestamp
            self._release_executor_resources(job)
        return job

    def _apply_abort_job(self, command: dict) -> Job:
        job = self.jobs[command["job_id"]]
        if job.status not in (JobStatus.PENDING.value, JobStatus.RUNNING.value):
            raise ValueError(f"cannot abort job {job.id} in status {job.status!r}")
        job.status = JobStatus.FAILED.value
        job.finished_at = datetime.fromisoformat(command["timestamp"])
        self._release_executor_resources(job)
        return job

    def _release_executor_resources(self, job: Job) -> None:
        if job.executor is None:
            return
        executor = self.executors.get(job.executor.id)
        if executor is None:
            return  # executor was deregistered (or crashed) while the job was running
        executor.add_resources(job)

    # -- read-only queries used by the API layer and bin-packing ---------

    def list_jobs(self) -> list[Job]:
        return list(self.jobs.values())

    def get_job(self, job_id: str) -> Optional[Job]:
        return self.jobs.get(job_id)

    def get_pending_jobs(self) -> list[Job]:
        return [
            job
            for job in self.jobs.values()
            if job.status == JobStatus.PENDING.value and job.executor is None
        ]

    def list_executors(self, data_center: Optional[str] = None) -> list[Executor]:
        executors = list(self.executors.values())
        if data_center:
            executors = [e for e in executors if e.data_center == data_center]
        return executors

    def get_executor(self, executor_id: str) -> Optional[Executor]:
        return self.executors.get(executor_id)
