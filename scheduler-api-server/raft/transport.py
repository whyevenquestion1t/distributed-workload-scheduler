"""Transport abstraction for Raft RPCs.

RaftNode talks to its peers only through this interface, which is what lets
the consensus core be tested in-process (instant, deterministic, and able to
simulate dropped/partitioned nodes) while running over real HTTP in Docker.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Optional

import httpx

from raft.types import (
    AppendEntriesArgs,
    AppendEntriesResult,
    LogEntry,
    RequestVoteArgs,
    RequestVoteResult,
)


class Transport(ABC):
    @abstractmethod
    async def send_request_vote(
        self, peer_id: str, args: RequestVoteArgs
    ) -> Optional[RequestVoteResult]:
        """Return None if the peer could not be reached."""

    @abstractmethod
    async def send_append_entries(
        self, peer_id: str, args: AppendEntriesArgs
    ) -> Optional[AppendEntriesResult]:
        """Return None if the peer could not be reached."""


class InMemoryTransport(Transport):
    """Routes RPCs directly to other RaftNode instances in the same process.

    `isolated` lets tests simulate a network partition: any RPC where either
    the sender or the recipient is in the set is dropped, in both
    directions, which is what makes it possible to test things like "the old
    leader can't win an election or commit anything while cut off."
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, "RaftNode"] = {}  # noqa: F821 (circular import avoided)
        self.isolated: set[str] = set()
        self.latency: float = 0.0

    def register(self, node_id: str, node) -> None:
        self.nodes[node_id] = node

    def partition(self, node_id: str) -> None:
        self.isolated.add(node_id)

    def heal(self, node_id: str) -> None:
        self.isolated.discard(node_id)

    async def send_request_vote(
        self, peer_id: str, args: RequestVoteArgs
    ) -> Optional[RequestVoteResult]:
        if peer_id in self.isolated or args.candidate_id in self.isolated:
            return None
        if self.latency:
            await asyncio.sleep(self.latency)
        peer = self.nodes.get(peer_id)
        if peer is None:
            return None
        return await peer.handle_request_vote(args)

    async def send_append_entries(
        self, peer_id: str, args: AppendEntriesArgs
    ) -> Optional[AppendEntriesResult]:
        if peer_id in self.isolated or args.leader_id in self.isolated:
            return None
        if self.latency:
            await asyncio.sleep(self.latency)
        peer = self.nodes.get(peer_id)
        if peer is None:
            return None
        return await peer.handle_append_entries(args)


class HttpTransport(Transport):
    """Sends Raft RPCs to peers over HTTP, used in the Docker deployment.

    `peer_addrs` maps node_id -> base URL, e.g. {"node-2": "http://scheduler-2:5001"}.
    """

    def __init__(self, peer_addrs: Dict[str, str], timeout: float = 0.3) -> None:
        self._peer_addrs = peer_addrs
        self._client = httpx.AsyncClient(timeout=timeout)

    async def send_request_vote(
        self, peer_id: str, args: RequestVoteArgs
    ) -> Optional[RequestVoteResult]:
        try:
            resp = await self._client.post(
                f"{self._peer_addrs[peer_id]}/raft/request_vote",
                json={
                    "term": args.term,
                    "candidate_id": args.candidate_id,
                    "last_log_index": args.last_log_index,
                    "last_log_term": args.last_log_term,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return RequestVoteResult(term=data["term"], vote_granted=data["vote_granted"])
        except (httpx.HTTPError, KeyError):
            return None

    async def send_append_entries(
        self, peer_id: str, args: AppendEntriesArgs
    ) -> Optional[AppendEntriesResult]:
        try:
            resp = await self._client.post(
                f"{self._peer_addrs[peer_id]}/raft/append_entries",
                json={
                    "term": args.term,
                    "leader_id": args.leader_id,
                    "prev_log_index": args.prev_log_index,
                    "prev_log_term": args.prev_log_term,
                    "entries": [e.to_dict() for e in args.entries],
                    "leader_commit": args.leader_commit,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return AppendEntriesResult(
                term=data["term"],
                success=data["success"],
                conflict_index=data.get("conflict_index"),
                conflict_term=data.get("conflict_term"),
            )
        except (httpx.HTTPError, KeyError):
            return None

    async def close(self) -> None:
        await self._client.aclose()
