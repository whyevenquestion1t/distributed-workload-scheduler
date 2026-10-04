"""Core Raft consensus state machine (leader election + log replication).

Implements the rules from Figure 2 of "In Search of an Understandable
Consensus Algorithm" (Ongaro & Ousterhout). A RaftNode knows nothing about
schedulers, jobs or executors: it replicates an opaque `command` to every
node in the cluster, in the same order, and calls `apply_callback(command)`
exactly once per node, in log order, once a command is safely committed to a
majority. The caller's apply_callback is what turns that into actual state
(see scheduler-api-server/state_machine.py).

Deliberately out of scope: log compaction/snapshots and cluster membership
changes. The log grows unboundedly and the peer set is fixed at startup,
which is fine for a demo-sized cluster but would need addressing for a
long-lived production deployment.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Optional

from raft.storage import Storage
from raft.transport import Transport
from raft.types import (
    AppendEntriesArgs,
    AppendEntriesResult,
    LogEntry,
    NotLeaderError,
    RequestVoteArgs,
    RequestVoteResult,
    Role,
)

NOOP = {"__noop__": True}


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        storage: Storage,
        transport: Transport,
        apply_callback: Callable[[Any], Any],
        election_timeout_range: tuple[float, float] = (0.5, 1.0),
        heartbeat_interval: float = 0.1,
    ) -> None:
        self.node_id = node_id
        self.peer_ids = list(peer_ids)
        self.storage = storage
        self.transport = transport
        self.apply_callback = apply_callback
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval

        state = storage.load()
        self.current_term = state.current_term
        self.voted_for = state.voted_for
        self.role = Role.FOLLOWER
        self.leader_hint: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self._pending: dict[int, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._election_deadline = 0.0
        self._reset_election_deadline()
        self._running = False
        self._tasks: list[asyncio.Task] = []

    # -- public API -----------------------------------------------------

    def is_leader(self) -> bool:
        return self.role == Role.LEADER

    def status(self) -> dict:
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.current_term,
            "leader_hint": self.leader_hint,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": self.storage.last_index(),
        }

    async def start(self) -> None:
        self._running = True
        self._tasks = [
            asyncio.create_task(self._election_timer_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._reject_pending_futures()

    async def propose(self, command: Any, timeout: float = 5.0) -> Any:
        """Replicate `command` to a majority and return the apply() result.

        Raises NotLeaderError if this node isn't the leader (callers should
        redirect to `leader_hint`), or asyncio.TimeoutError if the entry
        doesn't commit within `timeout` seconds (e.g. no majority available).
        """
        async with self._lock:
            if self.role != Role.LEADER:
                raise NotLeaderError(self.leader_hint)
            index = self.storage.last_index() + 1
            entry = LogEntry(index=index, term=self.current_term, command=command)
            self.storage.append_entries([entry])
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._pending[index] = fut
            self._advance_commit_index()  # resolves immediately in single-node clusters

        asyncio.create_task(self._replicate_round())
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            async with self._lock:
                self._pending.pop(index, None)

    # -- RPC handlers (called by the HTTP layer or InMemoryTransport) ---

    async def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteResult:
        async with self._lock:
            if args.term < self.current_term:
                return RequestVoteResult(term=self.current_term, vote_granted=False)
            if args.term > self.current_term:
                self._become_follower(args.term, leader_hint=None)

            log_ok = args.last_log_term > self.storage.last_term() or (
                args.last_log_term == self.storage.last_term()
                and args.last_log_index >= self.storage.last_index()
            )
            can_vote = self.voted_for in (None, args.candidate_id)

            if can_vote and log_ok:
                self.voted_for = args.candidate_id
                self.storage.save_term_and_vote(self.current_term, self.voted_for)
                self._reset_election_deadline()
                return RequestVoteResult(term=self.current_term, vote_granted=True)
            return RequestVoteResult(term=self.current_term, vote_granted=False)

    async def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesResult:
        async with self._lock:
            if args.term < self.current_term:
                return AppendEntriesResult(term=self.current_term, success=False)

            if args.term > self.current_term or self.role != Role.FOLLOWER:
                self._become_follower(args.term, leader_hint=args.leader_id)
            self.leader_hint = args.leader_id
            self._reset_election_deadline()

            if args.prev_log_index > 0:
                prev_entry = self.storage.get_entry(args.prev_log_index)
                if prev_entry is None:
                    return AppendEntriesResult(
                        term=self.current_term,
                        success=False,
                        conflict_index=self.storage.last_index() + 1,
                    )
                if prev_entry.term != args.prev_log_term:
                    conflict_term = prev_entry.term
                    conflict_index = args.prev_log_index
                    while True:
                        earlier = self.storage.get_entry(conflict_index - 1)
                        if earlier is None or earlier.term != conflict_term:
                            break
                        conflict_index -= 1
                    self.storage.truncate_from(args.prev_log_index)
                    return AppendEntriesResult(
                        term=self.current_term,
                        success=False,
                        conflict_index=conflict_index,
                        conflict_term=conflict_term,
                    )

            insert_index = args.prev_log_index + 1
            for i, entry in enumerate(args.entries):
                idx = insert_index + i
                existing = self.storage.get_entry(idx)
                if existing is not None and existing.term != entry.term:
                    self.storage.truncate_from(idx)
                    existing = None
                if existing is None:
                    self.storage.append_entries(args.entries[i:])
                    break

            if args.leader_commit > self.commit_index:
                self.commit_index = min(args.leader_commit, self.storage.last_index())
                self._apply_committed()

            return AppendEntriesResult(term=self.current_term, success=True)

    # -- election ---------------------------------------------------------

    async def _election_timer_loop(self) -> None:
        while self._running:
            await asyncio.sleep(0.02)
            start = False
            async with self._lock:
                if self.role == Role.LEADER:
                    continue
                if time.monotonic() < self._election_deadline:
                    continue
                start = True
            if start:
                await self._start_election()

    async def _start_election(self) -> None:
        async with self._lock:
            if self.role == Role.LEADER:
                return
            self.role = Role.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.storage.save_term_and_vote(self.current_term, self.voted_for)
            self._reset_election_deadline()
            term = self.current_term
            args = RequestVoteArgs(
                term=term,
                candidate_id=self.node_id,
                last_log_index=self.storage.last_index(),
                last_log_term=self.storage.last_term(),
            )
            peers = list(self.peer_ids)
            votes = 1  # vote for self

        results = await asyncio.gather(
            *[self.transport.send_request_vote(p, args) for p in peers],
            return_exceptions=True,
        )

        became_leader = False
        async with self._lock:
            if self.role != Role.CANDIDATE or self.current_term != term:
                return  # world moved on while we were waiting for votes
            for result in results:
                if result is None or isinstance(result, Exception):
                    continue
                if result.term > self.current_term:
                    self._become_follower(result.term, leader_hint=None)
                    return
                if result.vote_granted:
                    votes += 1
            if votes >= self._majority():
                self._become_leader()
                became_leader = True

        if became_leader:
            asyncio.create_task(self._replicate_round())

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_hint = self.node_id
        last_index = self.storage.last_index()
        self.next_index = {p: last_index + 1 for p in self.peer_ids}
        self.match_index = {p: 0 for p in self.peer_ids}
        # A no-op entry in the new term lets earlier-term entries commit
        # promptly (Raft can only count replicas to commit its own term's
        # entries directly; this bumps commit_index for the backlog too).
        entry = LogEntry(index=last_index + 1, term=self.current_term, command=NOOP)
        self.storage.append_entries([entry])
        self._advance_commit_index()

    def _become_follower(self, term: int, leader_hint: Optional[str]) -> None:
        was_leader = self.role == Role.LEADER
        self.role = Role.FOLLOWER
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.storage.save_term_and_vote(self.current_term, self.voted_for)
        if leader_hint is not None:
            self.leader_hint = leader_hint
        self._reset_election_deadline()
        if was_leader:
            self._reject_pending_futures()

    def _reset_election_deadline(self) -> None:
        self._election_deadline = time.monotonic() + random.uniform(
            *self.election_timeout_range
        )

    def _majority(self) -> int:
        return (len(self.peer_ids) + 1) // 2 + 1

    def _reject_pending_futures(self) -> None:
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(NotLeaderError(self.leader_hint))

    # -- replication (leader side) ---------------------------------------

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            async with self._lock:
                is_leader = self.role == Role.LEADER
            if is_leader:
                await self._replicate_round()

    async def _replicate_round(self) -> None:
        await asyncio.gather(
            *[self._send_append_entries_to(p) for p in self.peer_ids],
            return_exceptions=True,
        )

    async def _send_append_entries_to(self, peer_id: str) -> None:
        async with self._lock:
            if self.role != Role.LEADER:
                return
            term = self.current_term
            next_idx = self.next_index.get(peer_id, self.storage.last_index() + 1)
            prev_log_index = next_idx - 1
            prev_entry = self.storage.get_entry(prev_log_index) if prev_log_index > 0 else None
            prev_log_term = prev_entry.term if prev_entry else 0
            entries = self.storage.get_entries_from(next_idx)
            commit_index = self.commit_index

        args = AppendEntriesArgs(
            term=term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=commit_index,
        )
        result = await self.transport.send_append_entries(peer_id, args)
        if result is None:
            return  # unreachable peer this round; next heartbeat will retry

        async with self._lock:
            if self.role != Role.LEADER or self.current_term != term:
                return  # stale response, world moved on
            if result.term > self.current_term:
                self._become_follower(result.term, leader_hint=None)
                return
            if result.success:
                new_match = prev_log_index + len(entries)
                if new_match > self.match_index.get(peer_id, 0):
                    self.match_index[peer_id] = new_match
                    self.next_index[peer_id] = new_match + 1
                self._advance_commit_index()
            elif result.conflict_index is not None:
                self.next_index[peer_id] = max(1, result.conflict_index)
            else:
                self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)

    def _advance_commit_index(self) -> None:
        last_index = self.storage.last_index()
        for n in range(last_index, self.commit_index, -1):
            entry = self.storage.get_entry(n)
            if entry is None or entry.term != self.current_term:
                continue
            replicated_count = 1 + sum(
                1 for p in self.peer_ids if self.match_index.get(p, 0) >= n
            )
            if replicated_count >= self._majority():
                self.commit_index = n
                self._apply_committed()
                return

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            idx = self.last_applied + 1
            entry = self.storage.get_entry(idx)
            assert entry is not None, f"missing committed log entry at index {idx}"
            try:
                result: Any = None if entry.command == NOOP else self.apply_callback(entry.command)
                error = None
            except Exception as exc:  # the state machine rejected the command
                result = None
                error = exc
            self.last_applied = idx
            fut = self._pending.pop(idx, None)
            if fut is not None and not fut.done():
                if error is not None:
                    fut.set_exception(error)
                else:
                    fut.set_result(result)
