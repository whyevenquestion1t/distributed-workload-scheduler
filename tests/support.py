"""Test harness for exercising the Raft core without any networking or I/O."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from raft.node import RaftNode
from raft.storage import InMemoryStorage
from raft.transport import InMemoryTransport


class Cluster:
    """An N-node Raft cluster wired together with InMemoryTransport.

    Each node's apply_callback appends committed commands to `self.applied[node_id]`,
    so tests can assert on what got replicated and in what order.
    """

    def __init__(
        self,
        n: int,
        heartbeat_interval: float = 0.02,
        election_timeout_range: tuple[float, float] = (0.1, 0.2),
        apply_callback_factory: Callable[[str], Callable[[Any], Any]] | None = None,
    ) -> None:
        self.transport = InMemoryTransport()
        self.applied: dict[str, list[Any]] = {}
        self.nodes: dict[str, RaftNode] = {}

        node_ids = [f"n{i}" for i in range(n)]
        for node_id in node_ids:
            peers = [p for p in node_ids if p != node_id]
            self.applied[node_id] = []

            if apply_callback_factory is not None:
                apply_callback = apply_callback_factory(node_id)
            else:
                log = self.applied[node_id]

                def apply_callback(command, log=log):
                    log.append(command)
                    return command

            node = RaftNode(
                node_id=node_id,
                peer_ids=peers,
                storage=InMemoryStorage(),
                transport=self.transport,
                apply_callback=apply_callback,
                election_timeout_range=election_timeout_range,
                heartbeat_interval=heartbeat_interval,
            )
            self.nodes[node_id] = node
            self.transport.register(node_id, node)

    async def start(self) -> None:
        for node in self.nodes.values():
            await node.start()

    async def stop(self) -> None:
        for node in self.nodes.values():
            await node.stop()

    def leaders(self) -> list[RaftNode]:
        return [n for n in self.nodes.values() if n.is_leader()]

    def followers(self) -> list[RaftNode]:
        return [n for n in self.nodes.values() if not n.is_leader()]

    async def wait_for_leader(self, timeout: float = 2.0) -> RaftNode:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            leaders = self.leaders()
            if len(leaders) == 1:
                return leaders[0]
            await asyncio.sleep(0.02)
        raise AssertionError(
            f"no single leader elected within {timeout}s; leaders={self.leaders()}"
        )

    async def wait_until(self, predicate: Callable[[], bool], timeout: float = 2.0) -> None:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.02)
        raise AssertionError(f"condition not met within {timeout}s")
