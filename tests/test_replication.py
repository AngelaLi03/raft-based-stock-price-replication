"""
Tests for Raft log replication and commit logic.
"""

import pytest
import asyncio
import tempfile
import os
from unittest.mock import AsyncMock, MagicMock

from raft.types import PeerInfo, RaftState, LogEntry
from raft.node import RaftNode
from kv.state_machine import serialize_put_command


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
async def test_log_replication_leader(raft_node):
    """Test log replication from leader to followers."""
    # Make node leader
    raft_node.state = RaftState.LEADER
    raft_node.election_manager.current_term = 1
    
    # Initialize leader state
    for peer in raft_node.peers:
        raft_node.next_index[peer.node_id] = 1
        raft_node.match_index[peer.node_id] = 0
    
    # Mock the append entries callback
    raft_node._append_entries_to_peer = AsyncMock(return_value=MagicMock(success=True, match_index=1))
    
    # Create a log entry
    command_bytes = serialize_put_command("AAPL", 150.0, 1234567890)
    entry = LogEntry(index=1, term=1, command_bytes=command_bytes)
    
    # Test replication
    success = await raft_node._replicate_to_peers([entry])
    
    assert success is True
    assert raft_node.next_index["node2"] == 2
    assert raft_node.next_index["node3"] == 2
    assert raft_node.match_index["node2"] == 1
    assert raft_node.match_index["node3"] == 1


@pytest.mark.asyncio
async def test_commit_index_update(raft_node):
    """Test commit index update based on majority replication."""
    # Make node leader
    raft_node.state = RaftState.LEADER
    raft_node.election_manager.current_term = 1
    raft_node.commit_index = 0
    
    # Set up match indices for majority
    raft_node.match_index["node2"] = 2
    raft_node.match_index["node3"] = 1
    
    # Mock storage
    raft_node.storage.get_last_log_index = MagicMock(return_value=2)
    raft_node.storage.get_log_entry = MagicMock(return_value=LogEntry(index=2, term=1, command_bytes=b"test"))
    raft_node.storage.set_commit_index = MagicMock()
    
    # Mock apply method
    raft_node._apply_committed_entries = AsyncMock()
    
    # Test commit index update
    await raft_node._update_commit_index()
    
    # Should commit index 2 (majority has it)
    assert raft_node.commit_index == 2
    raft_node.storage.set_commit_index.assert_called_with(2)


@pytest.mark.asyncio
async def test_log_matching_property(raft_node):
    """Test log matching property validation."""
    # Add some entries to storage
    entry1 = LogEntry(index=1, term=1, command_bytes=b"cmd1")
    entry2 = LogEntry(index=2, term=1, command_bytes=b"cmd2")
    raft_node.storage.append_entries([entry1, entry2])
    
    # Test matching at index 1, term 1
    assert raft_node._check_log_matching(1, 1) is True
    
    # Test mismatch at index 1, term 2
    assert raft_node._check_log_matching(1, 2) is False
    
    # Test matching at index 0 (always true)
    assert raft_node._check_log_matching(0, 0) is True
    
    # Test index beyond log length
    assert raft_node._check_log_matching(3, 1) is False


@pytest.mark.asyncio
async def test_append_entries_with_log_entries(raft_node):
    """Test AppendEntries with actual log entries."""
    # Set up node state
    raft_node.election_manager.current_term = 1
    raft_node.commit_index = 0
    
    # Create mock request
    mock_request = MagicMock()
    mock_request.term = 1
    mock_request.leader_id = "node2"
    mock_request.prev_log_index = 0
    mock_request.prev_log_term = 0
    mock_request.leader_commit = 0
    
    # Create mock entries
    mock_entry = MagicMock()
    mock_entry.index = 1
    mock_entry.term = 1
    mock_entry.command_bytes = serialize_put_command("AAPL", 150.0, 1234567890)
    mock_request.entries = [mock_entry]
    
    # Mock storage methods
    raft_node.storage.set_current_term = MagicMock()
    raft_node.storage.append_entries = MagicMock()
    raft_node.storage.set_commit_index = MagicMock()
    
    # Mock log entries to return one entry after append
    def mock_get_log_entries():
        return [MagicMock()]  # Return one entry
    raft_node.storage.get_log_entries = MagicMock(side_effect=mock_get_log_entries)
    
    # Mock apply method
    raft_node._apply_committed_entries = AsyncMock()
    
    # Handle request
    response = await raft_node.handle_append_entries(mock_request)
    
    assert response.success is True
    assert response.term == 1
    assert response.match_index == 1  # Should be 1 after appending one entry
    raft_node.storage.append_entries.assert_called_once()


@pytest.mark.asyncio
async def test_append_entries_log_truncation(raft_node):
    """Test log truncation when entries don't match."""
    # Add existing entries
    entry1 = LogEntry(index=1, term=1, command_bytes=b"cmd1")
    entry2 = LogEntry(index=2, term=1, command_bytes=b"cmd2")
    raft_node.storage.append_entries([entry1, entry2])
    
    # Set up node state
    raft_node.election_manager.current_term = 1
    
    # Create mock request that will cause truncation
    mock_request = MagicMock()
    mock_request.term = 1
    mock_request.leader_id = "node2"
    mock_request.prev_log_index = 1  # Will truncate from index 2
    mock_request.prev_log_term = 1
    mock_request.leader_commit = 0
    
    # Create new entry
    mock_entry = MagicMock()
    mock_entry.index = 2
    mock_entry.term = 2  # Different term
    mock_entry.command_bytes = b"new_cmd"
    mock_request.entries = [mock_entry]
    
    # Mock storage methods
    raft_node.storage.set_current_term = MagicMock()
    raft_node.storage.truncate_log_from = MagicMock()
    raft_node.storage.append_entries = MagicMock()
    raft_node.storage.set_commit_index = MagicMock()
    
    # Mock apply method
    raft_node._apply_committed_entries = AsyncMock()
    
    # Handle request
    response = await raft_node.handle_append_entries(mock_request)
    
    assert response.success is True
    raft_node.storage.truncate_log_from.assert_called_with(2)
    raft_node.storage.append_entries.assert_called_once()


@pytest.mark.asyncio
async def test_put_price_replication(raft_node):
    """Test PutPrice with log replication."""
    # Make node leader
    raft_node.state = RaftState.LEADER
    raft_node.election_manager.current_term = 1
    
    # Initialize leader state
    for peer in raft_node.peers:
        raft_node.next_index[peer.node_id] = 1
        raft_node.match_index[peer.node_id] = 0
    
    # Mock replication
    raft_node._replicate_to_peers = AsyncMock(return_value=True)
    raft_node._update_commit_index = AsyncMock()
    
    # Mock storage
    raft_node.storage.get_last_log_index = MagicMock(return_value=0)
    raft_node.storage.append_entries = MagicMock()
    
    # Test PutPrice
    result = await raft_node.put_price("AAPL", 150.0, 1234567890)
    
    assert result["ok"] is True
    assert result["leader_hint"] == "node1"
    assert result["error_message"] is None
    
    # Verify replication was called
    raft_node._replicate_to_peers.assert_called_once()
    raft_node._update_commit_index.assert_called_once()


@pytest.mark.asyncio
async def test_put_price_not_leader(raft_node):
    """Test PutPrice when not leader."""
    # Make node follower
    raft_node.state = RaftState.FOLLOWER
    
    # Test PutPrice
    result = await raft_node.put_price("AAPL", 150.0, 1234567890)
    
    assert result["ok"] is False
    assert result["error_message"] == "Not leader"


@pytest.mark.asyncio
async def test_get_price_from_state_machine(raft_node):
    """Test GetPrice reading from state machine."""
    # Add entry to state machine
    from kv.state_machine import TickerPrice
    ticker_price = TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    raft_node.kv_state_machine.store["AAPL"] = ticker_price
    
    # Test GetPrice
    result = await raft_node.get_price("AAPL")
    
    assert result["found"] is True
    assert result["ticker_price"]["symbol"] == "AAPL"
    assert result["ticker_price"]["price"] == 150.0
    assert result["ticker_price"]["timestamp"] == 1234567890


@pytest.mark.asyncio
async def test_get_price_not_found(raft_node):
    """Test GetPrice when price not found."""
    # Test GetPrice
    result = await raft_node.get_price("UNKNOWN")
    
    assert result["found"] is False
    assert result["ticker_price"] is None
    assert result["error_message"] == "Price not found"


@pytest.mark.asyncio
async def test_apply_committed_entries(raft_node):
    """Test applying committed entries to state machine."""
    # Set up state
    raft_node.commit_index = 2
    raft_node.last_applied = 0
    
    # Add entries to storage
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    raft_node.storage.append_entries([entry1, entry2])
    
    # Mock storage methods
    raft_node.storage.get_log_entry = MagicMock(side_effect=[entry1, entry2])
    
    # Test applying entries
    await raft_node._apply_committed_entries()
    
    assert raft_node.last_applied == 2
    assert "AAPL" in raft_node.kv_state_machine.store
    assert "NVDA" in raft_node.kv_state_machine.store
    assert raft_node.kv_state_machine.store["AAPL"].price == 150.0
    assert raft_node.kv_state_machine.store["NVDA"].price == 800.0
