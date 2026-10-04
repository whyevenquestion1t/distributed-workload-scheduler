"""Executor service.

Introspects local resources, registers with the scheduler cluster, and runs
whatever jobs get dispatched to it. The executor holds no authoritative
state of its own - registration and every job status transition it reports
go through the scheduler's Raft log, so the cluster's view stays consistent
even if this executor or the current scheduler leader restarts.
"""

import asyncio
import os
import random
import socket
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException

from shared import JobDefinitionInput, JobStatus

### RESOURCES
DATA_CENTER_NAME = random.choice(["dc-1", "dc-2", "dc-3"])
GPU_NODE = os.getenv("GPU_NODE", "false").lower() == "true"
if GPU_NODE:
    gpu_type = random.choice(
        [
            "nvidia-A100",
            "nvidia-L4",
            "nvidia-B200",
            "nvidia-H100",
        ]
    )
    available_gpu_count = random.randint(1, 4)
else:
    gpu_type = None
    available_gpu_count = 0

my_ip_address = socket.gethostbyname(socket.gethostname())
available_cpu_cores = random.randint(4, 16)
available_memory_gb = random.randint(8, 32)

### SCHEDULER CLUSTER
# Any of these may be a follower; the scheduler redirects writes to its
# current leader and httpx follows that redirect automatically.
SCHEDULER_URLS = [
    url.strip()
    for url in os.getenv(
        "SCHEDULER_URLS",
        "http://scheduler-1:5001,http://scheduler-2:5001,http://scheduler-3:5001",
    ).split(",")
    if url.strip()
]

### STATE
executor_id: Optional[str] = None
tasks: Dict[str, asyncio.Task] = {}  # job_id -> task


async def _request(method: str, path: str, **kwargs) -> httpx.Response:
    """Call the scheduler cluster, trying each known node in turn.

    A follower responds 503 with a leader hint rather than redirecting -
    this executor and a host-side client can't necessarily reach the same
    address for a given node, so there's no address we could safely follow.
    Instead we just try our own next known node; with 3 nodes that's cheap,
    and it converges on the real leader either way.
    """
    last_exc: Optional[Exception] = None
    for base_url in SCHEDULER_URLS:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.request(method, f"{base_url}{path}", **kwargs)
                response.raise_for_status()
                return response
        except httpx.HTTPError as exc:
            last_exc = exc
    raise last_exc


async def report_job_status(job_id: str, status: str) -> None:
    """Tell the scheduler a job started or finished running."""
    try:
        await _request(
            "POST",
            f"/jobs/{job_id}/status",
            json={"status": status, "timestamp": datetime.now().isoformat()},
        )
    except httpx.HTTPError as exc:
        print(f"⚠️  Failed to report job {job_id} status={status}: {exc}")


async def run_simulated_job(job_id: str, job_input: JobDefinitionInput) -> None:
    await report_job_status(job_id, JobStatus.RUNNING.value)

    if job_input.gpu_count > 0:
        print(f"Job {job_id} - Running on GPU")
        print(f"nvidia-smi: mounting GPU {job_input.gpu_type}")
        await asyncio.sleep(5)

    print(f"Job {job_id} - Running")
    print(f"Pulling image {job_input.docker_image}")
    print(f"Running command {job_input.command}")
    time_to_run = 15
    while time_to_run > 0:
        await asyncio.sleep(1)
        time_to_run -= 1

    print(f"Job {job_id} - Finished")
    await report_job_status(job_id, JobStatus.SUCCEEDED.value)


## Lifespan function: https://fastapi.tiangolo.com/advanced/events/#lifespan-function
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Register with the scheduler cluster on startup, deregister on shutdown."""
    global executor_id

    response = await _request(
        "POST",
        "/executors",
        json={
            "ip_address": my_ip_address,
            "available_cpu_cores": available_cpu_cores,
            "available_memory_gb": available_memory_gb,
            "available_gpu_count": available_gpu_count,
            "gpu_type": gpu_type,
            "data_center": DATA_CENTER_NAME,
        },
    )
    executor_id = response.json()["id"]
    print(f"✓ Executor registered with ID: {executor_id}")

    yield

    try:
        await _request("DELETE", f"/executors/{executor_id}")
        print(f"✓ Executor {executor_id} deregistered")
    except httpx.HTTPError as exc:
        print(f"✗ Failed to deregister executor: {exc}")


app = FastAPI(
    title="Distributed Workload Scheduler - Executor",
    description="Runs jobs dispatched by the scheduler cluster.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Welcome endpoint"""
    return {
        "message": "Welcome to the Executor service!",
        "endpoints": {
            "health": "GET /health",
        },
    }


@app.post("/job/{job_id}")
async def run_job(job_id: str, job_input: JobDefinitionInput):
    """Run job in a separate task to allow concurrent execution"""
    if job_id in tasks:
        raise HTTPException(status_code=400, detail="Job is already running")

    async def execute_job():
        try:
            await run_simulated_job(job_id, job_input)
        except asyncio.CancelledError:
            print(f"✗ Job {job_id} was cancelled")
            await report_job_status(job_id, JobStatus.FAILED.value)
            raise  # Re-raise to properly complete the cancellation
        except Exception as e:
            print(f"✗ Job {job_id} failed: {e}")
            await report_job_status(job_id, JobStatus.FAILED.value)
        finally:
            tasks.pop(job_id, None)

    # Start job in background, give the task a name to identify it later, for cancelling the job later on.
    task = asyncio.create_task(execute_job())
    tasks[job_id] = task

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": "Job started in background",
        "task_id": task.get_name(),
    }


@app.delete("/job/{job_id}")
async def abort_job(job_id: str):
    """Abort a running job"""
    task = tasks.get(job_id)
    if task is None:
        raise HTTPException(status_code=404, detail="No running task found for this job")

    # Request cancellation
    task.cancel()

    # Wait for the task to complete its cleanup (with timeout)
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.CancelledError:
        # Expected - task was successfully cancelled
        pass
    except asyncio.TimeoutError:
        print(f"⚠️  Task {job_id} cancellation timed out")

    print(f"Job {job_id} - Aborted")

    return {
        "status": "aborted",
        "job_id": job_id,
        "message": "Job cancelled successfully",
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}


if __name__ == "__main__":
    uvicorn.run("executor:app", host="0.0.0.0", port=5002)
