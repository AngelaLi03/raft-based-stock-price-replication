"""
Tests for crash recovery and persistence functionality.
"""

import pytest
import asyncio
import tempfile
import os
import json
from unittest.mock import AsyncMock, MagicMock

from raft.types import PeerInfo, RaftState, LogEntry
from raft.node import RaftNode
from raft.storage import RaftStorage
from kv.state_machine import KVStateMachine, serialize_put_command


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
async def test_crash_recovery_with_unapplied_entries(raft_node):
    """Test crash recovery when there are unapplied committed entries."""
    # Set up state with unapplied entries
    raft_node.commit_index = 3
    raft_node.last_applied = 1
    
    # Add some log entries
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    entry3 = LogEntry(index=3, term=1, command_bytes=serialize_put_command("MSFT", 300.0, 1234567892))
    
    raft_node.storage.append_entries([entry1, entry2, entry3])
    raft_node.storage.set_commit_index(3)
    raft_node.storage.set_last_applied(1)
    
    # Mock storage methods
    raft_node.storage.get_log_entry = MagicMock(side_effect=[entry2, entry3])
    
    # Test crash recovery
    await raft_node._recover_from_crash()
    
    # Verify that entries 2 and 3 were applied
    assert raft_node.last_applied == 3
    assert "NVDA" in raft_node.kv_state_machine.store
    assert "MSFT" in raft_node.kv_state_machine.store
    assert raft_node.kv_state_machine.store["NVDA"].price == 800.0
    assert raft_node.kv_state_machine.store["MSFT"].price == 300.0


@pytest.mark.asyncio
async def test_crash_recovery_no_unapplied_entries(raft_node):
    """Test crash recovery when there are no unapplied entries."""
    # Set up state with no unapplied entries
    raft_node.commit_index = 2
    raft_node.last_applied = 2
    
    # Add some log entries
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    
    raft_node.storage.append_entries([entry1, entry2])
    raft_node.storage.set_commit_index(2)
    raft_node.storage.set_last_applied(2)
    
    # Test crash recovery
    await raft_node._recover_from_crash()
    
    # Verify that last_applied didn't change
    assert raft_node.last_applied == 2


@pytest.mark.asyncio
async def test_durable_storage_atomic_write(temp_data_dir):
    """Test that storage uses atomic writes."""
    storage = RaftStorage(temp_data_dir, "test_node")
    
    # Add some entries
    entry1 = LogEntry(index=1, term=1, command_bytes=b"test1")
    entry2 = LogEntry(index=2, term=1, command_bytes=b"test2")
    
    storage.append_entries([entry1, entry2])
    storage.set_current_term(5)
    storage.set_voted_for("node2")
    storage.set_commit_index(2)
    storage.set_last_applied(2)
    
    # Verify files exist
    assert os.path.exists(storage.log_file)
    assert os.path.exists(storage.meta_file)
    
    # Verify content
    with open(storage.log_file, 'r') as f:
        log_data = json.load(f)
        assert len(log_data) == 2
        assert log_data[0]["index"] == 1
        assert log_data[1]["index"] == 2
    
    with open(storage.meta_file, 'r') as f:
        meta_data = json.load(f)
        assert meta_data["current_term"] == 5
        assert meta_data["voted_for"] == "node2"
        assert meta_data["commit_index"] == 2
        assert meta_data["last_applied"] == 2


@pytest.mark.asyncio
async def test_storage_persistence_and_reload(temp_data_dir):
    """Test that storage persists and reloads correctly."""
    # Create first storage instance
    storage1 = RaftStorage(temp_data_dir, "test_node")
    
    # Add some data
    entry1 = LogEntry(index=1, term=1, command_bytes=b"test1")
    entry2 = LogEntry(index=2, term=1, command_bytes=b"test2")
    
    storage1.append_entries([entry1, entry2])
    storage1.set_current_term(3)
    storage1.set_voted_for("node2")
    storage1.set_commit_index(2)
    storage1.set_last_applied(1)
    
    # Create second storage instance (simulating restart)
    storage2 = RaftStorage(temp_data_dir, "test_node")
    
    # Verify data was reloaded
    assert storage2.get_current_term() == 3
    assert storage2.get_voted_for() == "node2"
    assert storage2.get_commit_index() == 2
    assert storage2.get_last_applied() == 1
    
    log_entries = storage2.get_log_entries()
    assert len(log_entries) == 2
    assert log_entries[0].index == 1
    assert log_entries[1].index == 2


@pytest.mark.asyncio
async def test_kv_state_machine_idempotent_apply(temp_data_dir):
    """Test that KV state machine apply is idempotent."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create a log entry
    entry = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    
    # Apply the same entry multiple times
    await kv.apply_command(entry)
    await kv.apply_command(entry)
    await kv.apply_command(entry)
    
    # Verify only one entry was applied
    assert len(kv.store) == 1
    assert "AAPL" in kv.store
    assert kv.store["AAPL"].price == 150.0
    assert kv.last_applied_index == 1
    
    await kv.stop()


@pytest.mark.asyncio
async def test_kv_state_machine_replay_log_entries(temp_data_dir):
    """Test replaying log entries for recovery."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create multiple log entries
    entries = [
        LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890)),
        LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891)),
        LogEntry(index=3, term=1, command_bytes=serialize_put_command("MSFT", 300.0, 1234567892))
    ]
    
    # Replay entries
    await kv.replay_log_entries(entries)
    
    # Verify all entries were applied
    assert len(kv.store) == 3
    assert "AAPL" in kv.store
    assert "NVDA" in kv.store
    assert "MSFT" in kv.store
    assert kv.last_applied_index == 3
    
    await kv.stop()


@pytest.mark.asyncio
async def test_kv_state_machine_persistence(temp_data_dir):
    """Test KV state machine persistence and recovery."""
    # Create first KV instance
    kv1 = KVStateMachine(temp_data_dir)
    await kv1.start()
    
    # Add some data
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    
    await kv1.apply_command(entry1)
    await kv1.apply_command(entry2)
    
    await kv1.stop()
    
    # Create second KV instance (simulating restart)
    kv2 = KVStateMachine(temp_data_dir)
    await kv2.start()
    
    # Verify data was recovered
    assert len(kv2.store) == 2
    assert "AAPL" in kv2.store
    assert "NVDA" in kv2.store
    assert kv2.store["AAPL"].price == 150.0
    assert kv2.store["NVDA"].price == 800.0
    assert kv2.last_applied_index == 2
    
    await kv2.stop()


@pytest.mark.asyncio
async def test_raft_node_start_with_recovery(raft_node):
    """Test Raft node startup with crash recovery."""
    # Set up persistent state
    raft_node.storage.set_current_term(2)
    raft_node.storage.set_voted_for("node2")
    raft_node.storage.set_commit_index(2)
    raft_node.storage.set_last_applied(1)
    
    # Add log entries
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    
    raft_node.storage.append_entries([entry1, entry2])
    
    # Mock storage methods
    raft_node.storage.get_log_entry = MagicMock(side_effect=[entry2])
    
    # Start node (should trigger recovery)
    await raft_node.start()
    
    # Verify recovery
    assert raft_node.election_manager.current_term == 2
    assert raft_node.election_manager.voted_for == "node2"
    assert raft_node.commit_index == 2
    assert raft_node.last_applied == 2
    
    # Verify KV state machine has the data
    assert "NVDA" in raft_node.kv_state_machine.store
    assert raft_node.kv_state_machine.store["NVDA"].price == 800.0
    
    await raft_node.stop()


@pytest.mark.asyncio
async def test_follower_catch_up_scenario(raft_node):
    """Test follower catch-up after being behind."""
    # Set up follower state (behind leader)
    raft_node.state = RaftState.FOLLOWER
    raft_node.election_manager.current_term = 1
    raft_node.commit_index = 1
    raft_node.last_applied = 1
    
    # Add some existing entries
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    raft_node.storage.append_entries([entry1])
    
    # Simulate AppendEntries from leader with new entries
    from raft.proto import raft_pb2
    
    mock_request = MagicMock()
    mock_request.term = 1
    mock_request.leader_id = "node2"
    mock_request.prev_log_index = 1
    mock_request.prev_log_term = 1
    mock_request.leader_commit = 3
    
    # Create new entries
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    entry3 = LogEntry(index=3, term=1, command_bytes=serialize_put_command("MSFT", 300.0, 1234567892))
    
    mock_entry2 = MagicMock()
    mock_entry2.index = 2
    mock_entry2.term = 1
    mock_entry2.command_bytes = serialize_put_command("NVDA", 800.0, 1234567891)
    
    mock_entry3 = MagicMock()
    mock_entry3.index = 3
    mock_entry3.term = 1
    mock_entry3.command_bytes = serialize_put_command("MSFT", 300.0, 1234567892)
    
    mock_request.entries = [mock_entry2, mock_entry3]
    
    # Mock storage methods
    raft_node.storage.get_log_entry = MagicMock(return_value=entry1)
    raft_node.storage.append_entries = MagicMock()
    raft_node.storage.set_commit_index = MagicMock()
    raft_node.storage.set_current_term = MagicMock()
    
    # Mock apply method
    raft_node._apply_committed_entries = AsyncMock()
    
    # Handle AppendEntries (catch-up)
    response = await raft_node.handle_append_entries(mock_request)
    
    # Verify response
    assert response.success is True
    assert response.term == 1
    
    # Verify storage was updated
    raft_node.storage.append_entries.assert_called_once()
    # The commit index should be set to min(leader_commit, log_length)
    # Since log_length is 1 (only entry1), commit_index should be 1, not 3
    raft_node.storage.set_commit_index.assert_called_with(1)
    
    # Verify apply was called
    raft_node._apply_committed_entries.assert_called_once()


@pytest.mark.asyncio
async def test_log_truncation_during_catch_up(raft_node):
    """Test log truncation during follower catch-up."""
    # Set up follower with conflicting log
    raft_node.state = RaftState.FOLLOWER
    raft_node.election_manager.current_term = 1
    
    # Add conflicting entries
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("CONFLICT", 999.0, 1234567891))
    
    raft_node.storage.append_entries([entry1, entry2])
    
    # Simulate AppendEntries that requires truncation
    from raft.proto import raft_pb2
    
    mock_request = MagicMock()
    mock_request.term = 1
    mock_request.leader_id = "node2"
    mock_request.prev_log_index = 1
    mock_request.prev_log_term = 1
    mock_request.leader_commit = 2
    
    # New entry to replace conflicting one
    entry2_new = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    
    mock_entry2 = MagicMock()
    mock_entry2.index = 2
    mock_entry2.term = 1
    mock_entry2.command_bytes = serialize_put_command("NVDA", 800.0, 1234567891)
    
    mock_request.entries = [mock_entry2]
    
    # Mock storage methods
    raft_node.storage.get_log_entry = MagicMock(return_value=entry1)
    raft_node.storage.truncate_log_from = MagicMock()
    raft_node.storage.append_entries = MagicMock()
    raft_node.storage.set_commit_index = MagicMock()
    raft_node.storage.set_current_term = MagicMock()
    
    # Mock apply method
    raft_node._apply_committed_entries = AsyncMock()
    
    # Handle AppendEntries
    response = await raft_node.handle_append_entries(mock_request)
    
    # Verify truncation was called
    raft_node.storage.truncate_log_from.assert_called_with(2)
    
    # Verify new entry was appended
    raft_node.storage.append_entries.assert_called_once()
    
    # Verify response
    assert response.success is True
