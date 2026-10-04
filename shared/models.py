"""Shared data models for Job and Executor across scheduler and executor services.

These are plain Pydantic models with no persistence of their own. The
scheduler's replicated state machine (scheduler-api-server/state_machine.py)
is the single source of truth, and it stays consistent across scheduler
replicas via the Raft log (scheduler-api-server/raft/), not via a shared
database.
"""

from enum import Enum
from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobDefinitionInput(BaseModel):
    """Job definition submitted by a client."""

    docker_image: str
    command: List[str]
    cpu_cores: int
    memory_gb: int
    gpu_count: int = 0
    gpu_type: Optional[str] = None
    data_center: Optional[str] = None


class Executor(BaseModel):
    """Executor instance state.

    Used by the bin-packing algorithm: given a job definition and the list of
    known executors, bin_packing_best_worker() picks the best fit.
    """

    id: str
    ip_address: str
    available_cpu_cores: int
    available_memory_gb: int
    available_gpu_count: int
    gpu_type: Optional[str] = None
    data_center: str

    def deduct_resources(self, job) -> None:
        self.available_cpu_cores -= job.cpu_cores
        self.available_memory_gb -= job.memory_gb
        self.available_gpu_count -= job.gpu_count

    def add_resources(self, job) -> None:
        self.available_cpu_cores += job.cpu_cores
        self.available_memory_gb += job.memory_gb
        self.available_gpu_count += job.gpu_count


class Job(BaseModel):
    """Job state, as tracked by the scheduler's replicated state machine."""

    id: str

    # Job definition fields
    docker_image: str
    command: List[str]
    cpu_cores: int
    memory_gb: int
    gpu_type: Optional[str] = None
    gpu_count: int = 0
    data_center: Optional[str] = None

    # Job execution fields
    executor: Optional[Executor] = None
    enqueued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    status: str = JobStatus.PENDING.value
    progress: int = 0

    def get_status_time(self) -> Optional[datetime]:
        """Get the timestamp for the current status."""
        if self.status in (JobStatus.SUCCEEDED.value, JobStatus.FAILED.value):
            return self.finished_at
        elif self.status == JobStatus.RUNNING.value:
            return self.started_at
        elif self.status == JobStatus.PENDING.value:
            return self.enqueued_at
        return None
