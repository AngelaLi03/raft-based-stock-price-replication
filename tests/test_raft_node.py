"""
Tests for Raft node.
"""

import pytest
import asyncio
import tempfile
import os
from unittest.mock import AsyncMock, MagicMock

from raft.types import PeerInfo, RaftState
from raft.node import RaftNode


@pytest.fixture
def mock_peers():
    """Create mock peer list."""
    return [
        PeerInfo("node2", "localhost", 50052, 50052, 50062),
        PeerInfo("node3", "localhost", 50053, 50053, 50063)
    ]


@pytest.fixture
def temp_data_dir():
    """Create temporary data directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def raft_node(mock_peers, temp_data_dir):
    """Create Raft node."""
    node = RaftNode(
        node_id="node1",
        peers=mock_peers,
        data_dir=temp_data_dir
    )
    return node


@pytest.mark.asyncio
async def test_raft_node_initialization(raft_node):
    """Test Raft node initialization."""
    assert raft_node.node_id == "node1"
    assert len(raft_node.peers) == 2
    assert raft_node.state == RaftState.FOLLOWER
    assert raft_node.commit_index == 0
    assert raft_node.last_applied == 0


@pytest.mark.asyncio
async def test_raft_node_start_stop(raft_node):
    """Test starting and stopping Raft node."""
    # Start node
    await raft_node.start()
    
    # Check that election timeout is started
    assert raft_node.election_manager.election_timeout_task is not None
    
    # Stop node
    await raft_node.stop()
    
    # Check that election timeout is stopped
    assert raft_node.election_manager.election_timeout_task is None


@pytest.mark.asyncio
async def test_handle_request_vote(raft_node):
    """Test handling request vote."""
    # Mock the storage
    raft_node.storage.set_current_term = MagicMock()
    raft_node.storage.set_voted_for = MagicMock()
    
    # Create mock request
    mock_request = MagicMock()
    mock_request.term = 1
    mock_request.candidate_id = "node2"
    mock_request.last_log_index = 0
    mock_request.last_log_term = 0
    
    # Handle request
    response = await raft_node.handle_request_vote(mock_request)
    
    assert response.term == 1
    assert response.vote_granted is True
    assert raft_node.election_manager.current_term == 1
    assert raft_node.election_manager.voted_for == "node2"


@pytest.mark.asyncio
async def test_handle_append_entries(raft_node):
    """Test handling append entries."""
    # Mock the storage
    raft_node.storage.set_current_term = MagicMock()
    
    # Create mock request
    mock_request = MagicMock()
    mock_request.term = 1
    mock_request.leader_id = "node2"
    mock_request.prev_log_index = 0
    mock_request.prev_log_term = 0
    mock_request.entries = []
    mock_request.leader_commit = 0
    
    # Handle request
    response = await raft_node.handle_append_entries(mock_request)
    
    assert response.term == 1
    assert response.success is True
    assert raft_node.election_manager.current_term == 1


@pytest.mark.asyncio
async def test_get_cluster_info(raft_node):
    """Test getting cluster info."""
    info = await raft_node.get_cluster_info()
    
    assert info["node_id"] == "node1"
    assert info["role"] == "follower"
    assert info["term"] == 0
    assert info["leader_id"] is None
    assert "node1" in info["members"]
    assert "node2" in info["members"]
    assert "node3" in info["members"]


@pytest.mark.asyncio
async def test_put_price_not_leader(raft_node):
    """Test put price when not leader."""
    result = await raft_node.put_price("AAPL", 150.0, 1234567890)
    
    assert result["ok"] is False
    assert result["error_message"] == "Not leader"
    assert result["leader_hint"] is None


@pytest.mark.asyncio
async def test_put_price_as_leader(raft_node):
    """Test put price when leader."""
    # Make node leader
    raft_node.state = RaftState.LEADER
    
    result = await raft_node.put_price("AAPL", 150.0, 1234567890)
    
    assert result["ok"] is True
    assert result["leader_hint"] == "node1"
    assert result["error_message"] is None


@pytest.mark.asyncio
async def test_get_price(raft_node):
    """Test get price."""
    result = await raft_node.get_price("AAPL")
    
    assert result["found"] is False
    assert result["error_message"] == "Price not found"
    assert result["ticker_price"] is None


def test_get_state_info(raft_node):
    """Test getting state info."""
    info = raft_node.get_state_info()
    
    assert info["node_id"] == "node1"
    assert info["state"] == "follower"
    assert info["current_term"] == 0
    assert info["voted_for"] is None
    assert info["commit_index"] == 0
    assert info["last_applied"] == 0
    assert info["log_length"] == 0
    assert len(info["peers"]) == 2
