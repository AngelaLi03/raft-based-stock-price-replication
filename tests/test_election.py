"""
Tests for Raft election logic.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from raft.types import PeerInfo, RaftState
from raft.election import ElectionManager


@pytest.fixture
def mock_peers():
    """Create mock peer list."""
    return [
        PeerInfo("node2", "localhost", 50052, 50052, 50062),
        PeerInfo("node3", "localhost", 50053, 50053, 50063)
    ]


@pytest.fixture
def election_manager(mock_peers):
    """Create election manager with mock callbacks."""
    request_vote_callback = AsyncMock(return_value=MagicMock(vote_granted=True))
    become_leader_callback = AsyncMock()
    become_follower_callback = AsyncMock()
    
    manager = ElectionManager(
        node_id="node1",
        peers=mock_peers,
        request_vote_callback=request_vote_callback,
        become_leader_callback=become_leader_callback,
        become_follower_callback=become_follower_callback
    )
    
    return manager


@pytest.mark.asyncio
async def test_election_manager_initialization(election_manager):
    """Test election manager initialization."""
    assert election_manager.node_id == "node1"
    assert election_manager.state == RaftState.FOLLOWER
    assert election_manager.current_term == 0
    assert election_manager.voted_for is None
    assert len(election_manager.peers) == 2


@pytest.mark.asyncio
async def test_vote_request_handling(election_manager):
    """Test handling of vote requests."""
    # Test granting vote
    vote_granted = election_manager.handle_vote_request(
        term=1,
        candidate_id="node2",
        last_log_index=0,
        last_log_term=0
    )
    
    assert vote_granted is True
    assert election_manager.current_term == 1
    assert election_manager.voted_for == "node2"
    assert election_manager.state == RaftState.FOLLOWER
    
    # Test denying vote (already voted)
    vote_granted = election_manager.handle_vote_request(
        term=1,
        candidate_id="node3",
        last_log_index=0,
        last_log_term=0
    )
    
    assert vote_granted is False
    assert election_manager.voted_for == "node2"  # Still voted for node2


@pytest.mark.asyncio
async def test_append_entries_handling(election_manager):
    """Test handling of append entries (heartbeats)."""
    # Test accepting heartbeat
    success = election_manager.handle_append_entries(
        term=1,
        leader_id="node2",
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0
    )
    
    assert success is True
    assert election_manager.current_term == 1
    assert election_manager.state == RaftState.FOLLOWER


@pytest.mark.asyncio
async def test_election_timeout_start_stop(election_manager):
    """Test election timeout start/stop."""
    # Start timeout
    election_manager.start_election_timeout()
    assert election_manager.election_timeout_task is not None
    
    # Stop timeout
    election_manager.stop_election_timeout()
    assert election_manager.election_timeout_task is None


@pytest.mark.asyncio
async def test_heartbeat_start_stop(election_manager):
    """Test heartbeat start/stop."""
    # Start heartbeat
    election_manager.start_heartbeat()
    assert election_manager.heartbeat_task is not None
    
    # Stop heartbeat
    election_manager.stop_heartbeat()
    assert election_manager.heartbeat_task is None


@pytest.mark.asyncio
async def test_become_leader(election_manager):
    """Test becoming leader."""
    # Mock the callbacks
    election_manager.become_leader_callback = AsyncMock()
    election_manager.become_follower_callback = AsyncMock()
    
    # Start election and become leader
    election_manager.current_term = 1
    election_manager.voted_for = "node1"
    election_manager.votes_received = {"node1": True, "node2": True}  # Majority
    
    await election_manager._become_leader()
    
    assert election_manager.state == RaftState.LEADER
    assert election_manager.election_timeout_task is None
    assert election_manager.heartbeat_task is not None
    election_manager.become_leader_callback.assert_called_once()


@pytest.mark.asyncio
async def test_become_follower(election_manager):
    """Test becoming follower."""
    # Mock the callbacks
    election_manager.become_leader_callback = AsyncMock()
    election_manager.become_follower_callback = AsyncMock()
    
    # Become follower
    await election_manager._become_follower()
    
    assert election_manager.state == RaftState.FOLLOWER
    assert election_manager.voted_for is None
    assert election_manager.heartbeat_task is None
    assert election_manager.election_timeout_task is not None
    election_manager.become_follower_callback.assert_called_once()


def test_get_state_info(election_manager):
    """Test getting state information."""
    info = election_manager.get_state_info()

    assert info["node_id"] == "node1"
    assert info["state"] == "follower"
    assert info["current_term"] == 0
    assert info["voted_for"] is None


@pytest.mark.asyncio
async def test_start_election_records_election_duration(election_manager):
    """_start_election() should record raft_election_duration_ms on the real
    PrometheusMetrics instance once the election resolves (win or lose) -
    not just call record_election() as a mock. ElectionManager doesn't get a
    real PrometheusMetrics instance by default (that's normally wired up in
    RaftNode.__init__ via init_prometheus_metrics), so initialize one here,
    matching tests/test_performance_metrics.py's test_election_recording."""
    from raft.prometheus_metrics import init_prometheus_metrics, get_prometheus_metrics
    init_prometheus_metrics("node1", port=0)
    metrics = get_prometheus_metrics()

    await election_manager._start_election()

    assert metrics.elections_total.labels(node_id="node1")._value.get() == 1
    # Histogram objects expose _sum directly but not a _count attribute.
    # Internally each _buckets[i] holds a non-cumulative per-bucket count
    # (cumulative summation only happens at collect()/export time), so the
    # total observation count is the sum across all of them, regardless of
    # which specific bucket a given observation landed in.
    election_duration = metrics.election_duration_ms.labels(node_id="node1")
    observation_count = sum(bucket.get() for bucket in election_duration._buckets)
    assert observation_count == 1
    assert election_duration._sum.get() >= 0


@pytest.mark.asyncio
async def test_start_election_calls_become_candidate_callback(mock_peers):
    """become_candidate_callback should be awaited as soon as the state
    transitions to CANDIDATE, before votes are requested - this is what
    lets raft_node_role reach 1 on the dashboard."""
    request_vote_callback = AsyncMock(return_value=MagicMock(vote_granted=True))
    become_leader_callback = AsyncMock()
    become_follower_callback = AsyncMock()
    become_candidate_callback = AsyncMock()

    manager = ElectionManager(
        node_id="node1",
        peers=mock_peers,
        request_vote_callback=request_vote_callback,
        become_leader_callback=become_leader_callback,
        become_follower_callback=become_follower_callback,
        become_candidate_callback=become_candidate_callback
    )

    await manager._start_election()

    become_candidate_callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_election_without_candidate_callback_does_not_raise(election_manager):
    """The election_manager fixture constructs ElectionManager without a
    become_candidate_callback (default None) - confirms existing
    construction sites that don't pass one keep working unchanged."""
    assert election_manager.become_candidate_callback is None

    # Should not raise (e.g. no "NoneType is not callable").
    await election_manager._start_election()

    assert election_manager.state == RaftState.LEADER
