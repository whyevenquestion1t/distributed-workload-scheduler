# Distributed Workload Scheduler

A small Nomad-style job scheduler: clients submit containerized jobs with
resource requirements (CPU, memory, GPU), a bin-packing algorithm matches
each job to the best-fitting executor, and executors run the jobs and report
back.

The scheduler itself runs as a 3-node cluster replicated with a from-scratch
**Raft** implementation, so the cluster keeps a single consistent view of
jobs and executors even if one scheduler node goes down.

## Architecture

```
.
├── shared/                     # Models shared by scheduler and executor
│   └── models.py               # Job, Executor, JobStatus, JobDefinitionInput
├── scheduler-api-server/
│   ├── raft/                   # Raft consensus core (transport-agnostic)
│   │   ├── node.py             # Leader election + log replication
│   │   ├── storage.py          # Persistent term/vote/log (file or in-memory)
│   │   ├── transport.py        # HTTP transport + in-memory transport for tests
│   │   └── types.py            # RPC message types
│   ├── state_machine.py        # The replicated state: jobs + executors
│   ├── bin_packing.py          # Best-fit / reverse best-fit matching
│   └── scheduler_server.py     # FastAPI app; one process per Raft node
├── executor/
│   └── executor.py             # Registers with the cluster, runs dispatched jobs
├── tests/raft/                 # Raft correctness tests (no network, no Docker)
└── docker-compose.yml          # 3 scheduler replicas + CPU/GPU executors
```

### Why Raft

A single scheduler instance is a single point of failure, and bolting
consistency onto multiple instances via a shared external store (e.g. one
Redis) just moves the single point of failure into that store. Instead, the
three scheduler replicas themselves form a Raft cluster: every state change
(a job submitted, an executor registered, a job assigned, a status update)
is proposed as a log entry, replicated to a majority, and only then applied
to each replica's in-memory state machine, in the same order everywhere.
Executors and clients always end up talking to whichever replica is
currently the leader. A follower responds `503` with `{"leader_hint": "<node id>"}`
rather than redirecting — a Docker-internal caller (an executor) and a
host-side caller (`curl`, `e2e-test.sh`) generally can't reach the same
address for a given node, so the node can't construct a redirect URL that
works for every caller. Instead, callers keep their own list of scheduler
addresses and just retry the next one on a 503; with 3 nodes that converges
on the leader immediately.

Implemented, following the Raft paper (Ongaro & Ousterhout):
- Randomized-timeout leader election, with the standard safety rules
  (grant a vote only if the candidate's log is at least as up to date).
- Log replication with `prevLogIndex`/`prevLogTerm` consistency checks and
  conflicting-entry truncation, plus the `conflictIndex` optimization so a
  lagging follower catches up in one round trip instead of backing off by
  one entry at a time.
- The leader-completeness safety rule: a leader only commits an entry by
  counting replicas directly for entries from its *own* term; it appends a
  no-op entry on election so earlier-term entries from the previous leader
  commit promptly instead of being stuck behind a quiet cluster.
- Persistent `currentTerm` / `votedFor` / log, flushed to disk (write to a
  temp file, `fsync`, atomic rename) before a node replies to any RPC.

Explicitly out of scope (reasonable for a small, fixed-size demo cluster,
not for a long-lived production one): log compaction / snapshots, and
cluster membership changes (the peer set is fixed at startup).

### Consistency model

Every scheduler replica runs the same deterministic state machine
(`state_machine.py`) over the same replicated log, so once an entry is
committed, every replica that applies it computes the identical result. To
keep that determinism intact, nothing in the state machine reads the clock,
generates an id, or makes a random choice — the leader computes those
(job id, timestamps) once, before proposing, and they travel inside the log
entry.

Both reads and writes are served only by the current leader (followers
respond 503). That trades off the ability to scale reads across followers
for a linearizable view without needing a separate ReadIndex protocol — a
reasonable tradeoff at this cluster size.

### Scheduling flow

1. Client `POST /jobs` → leader proposes `submit_job`, then immediately
   tries to place it with `bin_packing_best_worker` (best-fit on CPU/memory,
   weighted toward GPU efficiency when a GPU is requested).
2. If a fit is found: leader proposes `assign_job` (deducts the executor's
   resources in the state machine) and dispatches the job to that executor
   over HTTP. If dispatch fails, the job is proposed back to `failed` and
   its resources released.
3. The executor reports `running` when it starts and `succeeded`/`failed`
   when it finishes (`POST /jobs/{id}/status`); the leader proposes
   `update_job_status`, which releases the executor's resources on a
   terminal status and wakes the scheduling loop.
4. The scheduling loop (event-driven, with a 2s fallback tick in case a
   leader change drops an in-flight assignment) runs reverse bin-packing
   (`bin_packing_best_job`) over all pending jobs and free executors
   whenever it wakes, so freed-up capacity gets reused immediately.

Not implemented: executor failure detection. If an executor crashes mid-job,
the scheduler has no heartbeat on it and the job stays `running` forever —
only explicit deregistration (clean shutdown) is handled. Addressing this
would mean adding executor heartbeats/leases, which felt like a separate
feature from "make the scheduler's own state consistent."

## Running the tests

The Raft core is tested without Docker, sockets, or real time delays beyond
short asyncio sleeps — an in-memory transport lets tests simulate dropped
messages and network partitions directly.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt -r scheduler-api-server/requirements.txt
pytest
```

Covers: a single leader is elected and the cluster agrees on the term;
a proposed command replicates to every node; proposing on a follower raises
`NotLeaderError` with the correct leader hint; a partitioned follower
catches up once the partition heals; an isolated leader can't commit
anything and steps down once it reconnects to a cluster that elected a new
leader without it; and a long sequence of proposals applies in the same
order on every node.

## Running the cluster

### Docker (recommended)

```bash
docker compose up --build --scale cpu-executor=4 --scale gpu-executor=2
```

This starts 3 scheduler replicas (`scheduler-1/2/3`, reachable on host ports
`5001`/`5002`/`5003`) plus CPU and GPU executors that register with the
cluster on startup. Check `/raft/status` on any port to see who's currently
leader; the other two will answer API calls with a 503 and a leader hint.

Or run the full flow (start services, submit a mix of CPU/GPU jobs, wait for
completion, print a summary):

```bash
./e2e-test.sh [NUM_CPU_JOBS] [NUM_GPU_JOBS] [NUM_CPU_WORKERS] [NUM_GPU_WORKERS]
# e.g.
./e2e-test.sh 20 5 2 4
```

### Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install -r scheduler-api-server/requirements.txt -r executor/requirements.txt

# Terminal 1-3: one scheduler replica each
NODE_ID=scheduler-1 PEERS="scheduler-1@http://localhost:5001,scheduler-2@http://localhost:5002,scheduler-3@http://localhost:5003" \
  python scheduler-api-server/scheduler_server.py
NODE_ID=scheduler-2 PEERS="scheduler-1@http://localhost:5001,scheduler-2@http://localhost:5002,scheduler-3@http://localhost:5003" \
  python scheduler-api-server/scheduler_server.py
NODE_ID=scheduler-3 PEERS="scheduler-1@http://localhost:5001,scheduler-2@http://localhost:5002,scheduler-3@http://localhost:5003" \
  python scheduler-api-server/scheduler_server.py

# Terminal 4: an executor
SCHEDULER_URLS="http://localhost:5001,http://localhost:5002,http://localhost:5003" \
  python executor/executor.py
```

## API usage

These examples assume `localhost:5001` happens to be the leader; if it
isn't, you'll get `{"detail": "not the leader", "leader_hint": "scheduler-2"}`
back with a 503 — try `:5002` or `:5003` instead (`e2e-test.sh`'s
`scheduler_request` helper does this automatically).

**Submit a job** (`command` is a list of strings, like a Docker `CMD`):
```bash
curl -s -X POST http://localhost:5001/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "docker_image": "python:3.10",
    "command": ["python", "-c", "print(\"hello\")"],
    "cpu_cores": 2,
    "memory_gb": 4,
    "gpu_count": 1,
    "gpu_type": "nvidia-A100"
  }'
```

```bash
curl -s http://localhost:5001/jobs            # list all jobs
curl -s http://localhost:5001/jobs/<job_id>    # get one job
curl -s -X DELETE http://localhost:5001/jobs/<job_id>  # abort a job
curl -s http://localhost:5001/executors        # list registered executors
curl -s http://localhost:5001/raft/status      # this node's Raft role/term/commit index
```

## Design notes

- **Bin-packing as the core abstraction.** Scheduling a job is "does this
  job fit in this bin (executor), and if several fit, which one wastes the
  least capacity." The reverse direction — given a newly freed executor,
  which of the pending jobs is the best fit — is the same scoring function
  run the other way, which is what makes freed-up capacity get reused
  without a separate code path.
- **Event-driven, not polled.** Scheduling runs when something relevant
  happens (a job is submitted, a job finishes) via an `asyncio.Event`, not
  on a fixed interval — the periodic fallback tick exists only to reconcile
  state after a leader change, not as the primary trigger.
