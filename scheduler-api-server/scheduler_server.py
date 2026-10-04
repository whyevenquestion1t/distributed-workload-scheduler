"""Scheduler API server.

Every scheduler replica runs one of these. They form a Raft cluster (see
raft/) that replicates a single, consistent view of jobs and executors
(state_machine.py). Only the current Raft leader accepts reads and writes; a
follower responds with HTTP 503 and a `leader_hint` node id, and the caller
retries against a different address from its own list of known scheduler
nodes. Serving reads from the leader only avoids needing a separate
ReadIndex protocol to get a linearizable view - the tradeoff is that
followers can't serve reads, which is fine for a cluster this size.

Run three of these (see ../docker-compose.yml) with NODE_ID and PEERS set,
e.g.:
    NODE_ID=scheduler-1 \
    PEERS="scheduler-1@http://scheduler-1:5001,scheduler-2@http://scheduler-2:5001,scheduler-3@http://scheduler-3:5001" \
    python scheduler_server.py
"""

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from bin_packing import bin_packing_best_job, bin_packing_best_worker
from raft.node import RaftNode
from raft.storage import FileStorage
from raft.transport import HttpTransport
from raft.types import (
    AppendEntriesArgs,
    LogEntry,
    NotLeaderError,
    RequestVoteArgs,
)
from shared import Executor, Job, JobDefinitionInput, JobStatus
from state_machine import SchedulerStateMachine

EXECUTOR_PORT = 5002


def _parse_peers(raw: str) -> dict[str, str]:
    nodes: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        node_id, url = pair.split("@", 1)
        nodes[node_id.strip()] = url.strip().rstrip("/")
    return nodes


NODE_ID = os.environ["NODE_ID"]
ALL_NODES = _parse_peers(os.environ["PEERS"])
if NODE_ID not in ALL_NODES:
    raise RuntimeError(f"NODE_ID={NODE_ID!r} must be one of the entries in PEERS={ALL_NODES!r}")

PEER_IDS = [node_id for node_id in ALL_NODES if node_id != NODE_ID]
PEER_ADDRS = {node_id: url for node_id, url in ALL_NODES.items() if node_id != NODE_ID}

RAFT_DATA_DIR = os.getenv("RAFT_DATA_DIR", "./raft-data")

state_machine = SchedulerStateMachine()
storage = FileStorage(os.path.join(RAFT_DATA_DIR, f"{NODE_ID}.json"))
transport = HttpTransport(PEER_ADDRS)
raft_node = RaftNode(
    node_id=NODE_ID,
    peer_ids=PEER_IDS,
    storage=storage,
    transport=transport,
    apply_callback=state_machine.apply,
    election_timeout_range=(0.5, 1.0),
    heartbeat_interval=0.1,
)

# Set whenever a job is submitted or frees up an executor, so the scheduling
# pass runs promptly instead of waiting for its periodic fallback tick.
_scheduling_event = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await raft_node.start()
    scheduling_task = asyncio.create_task(_scheduling_loop())
    yield
    scheduling_task.cancel()
    await raft_node.stop()
    await transport.close()


class PrettyJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        import json

        return json.dumps(content, ensure_ascii=False, indent=2, separators=(", ", ": ")).encode(
            "utf-8"
        )


app = FastAPI(
    title="Distributed Workload Scheduler",
    description="Raft-replicated scheduler API for a cluster of job executors.",
    version="1.0.0",
    lifespan=lifespan,
    default_response_class=PrettyJSONResponse,
)


def not_leader_response() -> JSONResponse:
    """Told a caller isn't talking to the leader.

    Deliberately not an HTTP redirect: this node's peer addresses are
    whatever PEERS says (Docker-internal hostnames in the compose setup),
    which isn't necessarily an address the *caller* can reach - a host-side
    curl and an in-cluster executor need different addresses for the same
    node. So instead of guessing, hand back the leader's node id and let the
    caller retry against its own list of known scheduler addresses, which it
    already needed to have in order to reach us in the first place.
    """
    return JSONResponse(
        status_code=503,
        content={"detail": "not the leader", "leader_hint": raft_node.leader_hint},
    )


async def _dispatch_to_executor(job: Job, job_input: JobDefinitionInput) -> None:
    """Tell the assigned executor to run the job; mark it failed if that fails."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(
                f"http://{job.executor.ip_address}:{EXECUTOR_PORT}/job/{job.id}",
                json=job_input.model_dump(),
            )
            response.raise_for_status()
    except httpx.HTTPError as exc:
        try:
            await raft_node.propose(
                {
                    "type": "update_job_status",
                    "job_id": job.id,
                    "status": JobStatus.FAILED.value,
                    "timestamp": datetime.now().isoformat(),
                }
            )
        except NotLeaderError:
            pass  # whoever's leader now will pick this up on its next scheduling pass
        raise HTTPException(status_code=500, detail=f"failed to start job on executor: {exc}")


def _job_input_for(job: Job) -> JobDefinitionInput:
    return JobDefinitionInput(
        docker_image=job.docker_image,
        command=job.command,
        cpu_cores=job.cpu_cores,
        memory_gb=job.memory_gb,
        gpu_type=job.gpu_type,
        gpu_count=job.gpu_count,
        data_center=job.data_center,
    )


async def _scheduling_loop() -> None:
    """Event-driven scheduling pass: fires on submit/completion, with a
    periodic fallback so a freshly elected leader reconciles any jobs left
    pending by a predecessor that lost leadership mid-assignment."""
    while True:
        try:
            await asyncio.wait_for(_scheduling_event.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        _scheduling_event.clear()
        if raft_node.is_leader():
            try:
                await _run_scheduling_pass()
            except Exception as exc:
                # A bug in one pass shouldn't take down the loop for the
                # life of the process - log it and let the next trigger
                # (or the 2s fallback) try again.
                print(f"✗ scheduling pass failed: {exc}")


async def _run_scheduling_pass() -> None:
    pending_jobs = state_machine.get_pending_jobs()
    if not pending_jobs:
        return
    executors = state_machine.list_executors()
    for job, executor in bin_packing_best_job(pending_jobs, executors):
        try:
            assigned_job: Job = await raft_node.propose(
                {"type": "assign_job", "job_id": job.id, "executor_id": executor.id}
            )
        except NotLeaderError:
            return
        except ValueError:
            # Already assigned by a racing pass (e.g. submit_job's inline
            # attempt) - someone else is dispatching it, move on.
            continue
        try:
            await _dispatch_to_executor(assigned_job, _job_input_for(assigned_job))
        except HTTPException:
            pass  # already marked failed and resources released by _dispatch_to_executor


@app.get("/")
def root():
    return {
        "message": "Distributed Workload Scheduler",
        "node_id": NODE_ID,
        "raft": raft_node.status(),
        "endpoints": {
            "submit_job": "POST /jobs",
            "get_job": "GET /jobs/{job_id}",
            "list_jobs": "GET /jobs",
            "abort_job": "DELETE /jobs/{job_id}",
            "list_executors": "GET /executors",
        },
    }


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}


@app.get("/raft/status")
def raft_status():
    return raft_node.status()


class RequestVoteBody(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class AppendEntriesBody(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[dict] = []
    leader_commit: int = 0


@app.post("/raft/request_vote")
async def raft_request_vote(body: RequestVoteBody):
    result = await raft_node.handle_request_vote(
        RequestVoteArgs(
            term=body.term,
            candidate_id=body.candidate_id,
            last_log_index=body.last_log_index,
            last_log_term=body.last_log_term,
        )
    )
    return {"term": result.term, "vote_granted": result.vote_granted}


@app.post("/raft/append_entries")
async def raft_append_entries(body: AppendEntriesBody):
    result = await raft_node.handle_append_entries(
        AppendEntriesArgs(
            term=body.term,
            leader_id=body.leader_id,
            prev_log_index=body.prev_log_index,
            prev_log_term=body.prev_log_term,
            entries=[LogEntry.from_dict(e) for e in body.entries],
            leader_commit=body.leader_commit,
        )
    )
    return {
        "term": result.term,
        "success": result.success,
        "conflict_index": result.conflict_index,
        "conflict_term": result.conflict_term,
    }


class RegisterExecutorBody(BaseModel):
    ip_address: str
    available_cpu_cores: int
    available_memory_gb: int
    available_gpu_count: int
    gpu_type: Optional[str] = None
    data_center: str


@app.post("/executors", status_code=201)
async def register_executor(body: RegisterExecutorBody):
    if not raft_node.is_leader():
        return not_leader_response()
    executor_id = str(uuid.uuid4())
    try:
        executor: Executor = await raft_node.propose(
            {"type": "register_executor", "id": executor_id, **body.model_dump()}
        )
    except NotLeaderError:
        return not_leader_response()
    return executor


@app.delete("/executors/{executor_id}")
async def deregister_executor(executor_id: str):
    if not raft_node.is_leader():
        return not_leader_response()
    try:
        await raft_node.propose({"type": "deregister_executor", "id": executor_id})
    except NotLeaderError:
        return not_leader_response()
    return {"status": "deregistered", "executor_id": executor_id}


@app.get("/executors", response_model=List[Executor])
def list_executors():
    if not raft_node.is_leader():
        return not_leader_response()
    return state_machine.list_executors()


@app.post("/jobs", status_code=201)
async def submit_job(job_input: JobDefinitionInput):
    if not raft_node.is_leader():
        return not_leader_response()

    job_id = str(uuid.uuid4())
    command = {
        "type": "submit_job",
        "id": job_id,
        "docker_image": job_input.docker_image,
        "command": job_input.command,
        "cpu_cores": job_input.cpu_cores,
        "memory_gb": job_input.memory_gb,
        "gpu_count": job_input.gpu_count,
        "gpu_type": job_input.gpu_type,
        "data_center": job_input.data_center,
        "enqueued_at": datetime.now().isoformat(),
    }
    try:
        job: Job = await raft_node.propose(command)

        executor = bin_packing_best_worker(
            job_input, state_machine.list_executors(job_input.data_center)
        )
        if executor is not None:
            try:
                job = await raft_node.propose(
                    {"type": "assign_job", "job_id": job.id, "executor_id": executor.id}
                )
            except ValueError:
                # The background scheduling pass's fallback tick raced us
                # and already assigned this job - nothing left to dispatch.
                job = None
            if job is not None:
                await _dispatch_to_executor(job, job_input)
    except NotLeaderError:
        return not_leader_response()

    return state_machine.get_job(job_id)


@app.get("/jobs", response_model=List[Job])
def list_jobs():
    if not raft_node.is_leader():
        return not_leader_response()
    return state_machine.list_jobs()


@app.get("/jobs/{job_id}", response_model=Job)
def get_job(job_id: str):
    if not raft_node.is_leader():
        return not_leader_response()
    job = state_machine.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job with ID '{job_id}' not found")
    return job


@app.delete("/jobs/{job_id}")
async def abort_job(job_id: str):
    if not raft_node.is_leader():
        return not_leader_response()

    job = state_machine.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job with ID '{job_id}' not found")
    if job.status not in (JobStatus.PENDING.value, JobStatus.RUNNING.value):
        raise HTTPException(
            status_code=400,
            detail=f"Cannot abort job with status '{job.status}'. Only PENDING or RUNNING jobs can be aborted.",
        )

    if job.executor is not None:
        # Once a job has an executor it's already been dispatched there (we
        # always dispatch synchronously right after assign_job commits), so
        # it may be running even if we haven't yet received its "running"
        # status callback - checking job.status == RUNNING here would miss
        # that window and declare it aborted without actually stopping it.
        # Ask the executor to cancel, and let its own callback (POST
        # /jobs/{id}/status) land the FAILED transition; proposing
        # "abort_job" here too would race with that callback.
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.delete(
                    f"http://{job.executor.ip_address}:{EXECUTOR_PORT}/job/{job_id}"
                )
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502, detail=f"failed to reach executor to abort job: {exc}"
            )
        return {"status": "abort requested", "job_id": job_id}

    try:
        job = await raft_node.propose(
            {"type": "abort_job", "job_id": job_id, "timestamp": datetime.now().isoformat()}
        )
    except NotLeaderError:
        return not_leader_response()

    _scheduling_event.set()
    return job


class JobStatusUpdate(BaseModel):
    status: str
    timestamp: Optional[str] = None


@app.post("/jobs/{job_id}/status")
async def report_job_status(job_id: str, update: JobStatusUpdate):
    """Called by executors to report that a job started running or finished."""
    if not raft_node.is_leader():
        return not_leader_response()
    if update.status not in {s.value for s in JobStatus}:
        raise HTTPException(status_code=400, detail=f"unknown status '{update.status}'")

    timestamp = update.timestamp or datetime.now().isoformat()
    try:
        job: Job = await raft_node.propose(
            {
                "type": "update_job_status",
                "job_id": job_id,
                "status": update.status,
                "timestamp": timestamp,
            }
        )
    except NotLeaderError:
        return not_leader_response()
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Job with ID '{job_id}' not found")

    if update.status in (JobStatus.SUCCEEDED.value, JobStatus.FAILED.value):
        _scheduling_event.set()
    return job


if __name__ == "__main__":
    port = int(ALL_NODES[NODE_ID].rsplit(":", 1)[-1])
    uvicorn.run(app, host="0.0.0.0", port=port)
