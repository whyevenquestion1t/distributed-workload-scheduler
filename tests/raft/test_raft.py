"""Correctness tests for the Raft core, run entirely in-process via InMemoryTransport.

These exercise the properties Raft is supposed to guarantee: a single leader
per term, replication of committed entries to every node, followers catching
up after lagging, and an isolated leader being unable to make progress (and
stepping down once it rejoins a cluster that has moved on without it).
"""

import asyncio

import pytest

from raft.types import NotLeaderError
from support import Cluster


@pytest.mark.asyncio
async def test_single_leader_elected_among_three():
    cluster = Cluster(3)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        terms = {n.current_term for n in cluster.nodes.values()}
        assert len(terms) == 1, "all nodes should agree on the current term"
        assert len(cluster.leaders()) == 1
        assert leader.leader_hint == leader.node_id
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_proposed_command_replicates_to_all_followers():
    cluster = Cluster(5)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        result = await leader.propose({"op": "submit_job", "id": "job-1"})
        assert result == {"op": "submit_job", "id": "job-1"}

        await cluster.wait_until(
            lambda: all(cluster.applied[n] == [{"op": "submit_job", "id": "job-1"}] for n in cluster.nodes)
        )
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_propose_on_follower_raises_not_leader_with_hint():
    cluster = Cluster(3)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        follower = next(n for n in cluster.nodes.values() if n is not leader)
        with pytest.raises(NotLeaderError) as excinfo:
            await follower.propose({"op": "noop"})
        assert excinfo.value.leader_hint == leader.node_id
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_lagging_follower_catches_up_after_partition_heals():
    cluster = Cluster(3)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        lagger_id = next(nid for nid, n in cluster.nodes.items() if n is not leader)

        cluster.transport.partition(lagger_id)
        for i in range(5):
            await leader.propose({"op": "submit_job", "id": f"job-{i}"})

        assert cluster.applied[lagger_id] == []

        cluster.transport.heal(lagger_id)
        await cluster.wait_until(lambda: len(cluster.applied[lagger_id]) == 5)
        assert cluster.applied[lagger_id] == cluster.applied[leader.node_id]
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_isolated_leader_cannot_commit_and_steps_down():
    cluster = Cluster(3, election_timeout_range=(0.1, 0.2))
    await cluster.start()
    try:
        old_leader = await cluster.wait_for_leader()
        old_leader_id = old_leader.node_id

        # Cut the leader off from the other two nodes entirely.
        cluster.transport.partition(old_leader_id)

        # It can still append to its own log, but can never reach a majority.
        with pytest.raises(asyncio.TimeoutError):
            await old_leader.propose({"op": "should_never_commit"}, timeout=0.5)

        # The remaining two nodes should elect a new leader among themselves.
        remaining = [n for nid, n in cluster.nodes.items() if nid != old_leader_id]
        deadline = asyncio.get_event_loop().time() + 2.0
        new_leader = None
        while asyncio.get_event_loop().time() < deadline:
            leaders = [n for n in remaining if n.is_leader()]
            if len(leaders) == 1:
                new_leader = leaders[0]
                break
            await asyncio.sleep(0.02)
        assert new_leader is not None, "majority partition should elect a new leader"
        assert new_leader.node_id != old_leader_id

        result = await new_leader.propose({"op": "submit_job", "id": "job-after-split"})
        assert result["id"] == "job-after-split"

        # Heal the partition: the stale leader must step down and adopt the new term.
        cluster.transport.heal(old_leader_id)
        await cluster.wait_until(lambda: not old_leader.is_leader())
        await cluster.wait_until(
            lambda: cluster.applied[old_leader_id][-1]["id"] == "job-after-split"
        )
        assert old_leader.current_term >= new_leader.current_term
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_many_sequential_proposals_apply_in_order_everywhere():
    cluster = Cluster(3)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        for i in range(20):
            await leader.propose({"op": "submit_job", "id": f"job-{i}"})

        await cluster.wait_until(lambda: all(len(log) == 20 for log in cluster.applied.values()))
        expected = [{"op": "submit_job", "id": f"job-{i}"} for i in range(20)]
        for node_id, log in cluster.applied.items():
            assert log == expected, f"node {node_id} diverged from expected log order"
    finally:
        await cluster.stop()
