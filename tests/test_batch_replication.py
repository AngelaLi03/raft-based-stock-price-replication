"""
Tests for batch replication functionality.
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from typing import List

from raft.node import RaftNode
from raft.types import PeerInfo, LogEntry, RaftState
from kv.state_machine import serialize_put_command


class TestBatchReplication:
    """Test batch replication functionality."""
    
    @pytest.fixture
    def mock_peers(self):
        """Create mock peer list."""
        return [
            PeerInfo("node2", "localhost", 50062, 50052, 50062),
            PeerInfo("node3", "localhost", 50063, 50053, 50063)
        ]
    
    @pytest.fixture
    def raft_node(self, mock_peers, tmp_path):
        """Create RaftNode instance for testing."""
        return RaftNode(
            node_id="node1",
            peers=mock_peers,
            data_dir=str(tmp_path),
            batch_size=3,  # Small batch size for testing
            flush_interval=100  # 100ms flush interval
        )
    
    @pytest.mark.asyncio
    async def test_batch_accumulation(self, raft_node):
        """Test that entries are accumulated in batch."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Add entries to batch
        for i in range(2):  # Less than batch size
            await raft_node._add_to_batch(LogEntry(
                index=i + 1,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            ))
        
        # Should not have flushed yet
        assert len(raft_node.pending_entries) == 2
        raft_node._replicate_to_peers.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_batch_flush_on_size(self, raft_node):
        """Test that batch flushes when size limit is reached."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Add entries up to batch size
        for i in range(3):  # Exactly batch size
            await raft_node._add_to_batch(LogEntry(
                index=i + 1,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            ))
        
        # Should have flushed
        assert len(raft_node.pending_entries) == 0
        raft_node._replicate_to_peers.assert_called_once()
        assert len(raft_node._replicate_to_peers.call_args[0][0]) == 3
    
    @pytest.mark.asyncio
    async def test_batch_flush_interval(self, raft_node):
        """Test that batch flushes on time interval."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Add one entry
        await raft_node._add_to_batch(LogEntry(
            index=1,
            term=1,
            command_bytes=serialize_put_command("key0", 100.0, int(time.time() * 1000))
        ))
        
        # Start batch flush task
        flush_task = asyncio.create_task(raft_node._batch_flush_loop())
        
        # Wait for flush interval
        await asyncio.sleep(0.15)  # 150ms, longer than 100ms interval
        
        # Cancel task
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        
        # Should have flushed
        raft_node._replicate_to_peers.assert_called()
    
    @pytest.mark.asyncio
    async def test_batch_replication_success(self, raft_node):
        """Test successful batch replication."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock successful replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Add entries and flush
        entries = []
        for i in range(3):
            entry = LogEntry(
                index=i + 1,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            )
            entries.append(entry)
            await raft_node._add_to_batch(entry)
        
        # Verify replication was called with correct entries
        raft_node._replicate_to_peers.assert_called_once()
        replicated_entries = raft_node._replicate_to_peers.call_args[0][0]
        assert len(replicated_entries) == 3
        assert replicated_entries == entries
        
        # Verify commit index update
        raft_node._update_commit_index.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_batch_replication_failure(self, raft_node):
        """Test batch replication failure handling."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage read, but use the real truncate_log_from so we verify
        # the actual method (not a mock) is called correctly and that it
        # only removes the failed batch, not prior committed entries.
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        raft_node.storage.truncate_log_from = Mock(wraps=raft_node.storage.truncate_log_from)

        # Mock failed replication
        raft_node._replicate_to_peers = AsyncMock(return_value=False)

        # Seed the log with one already-committed entry that must survive
        # the failed batch's truncation.
        committed_entry = LogEntry(
            index=1, term=1,
            command_bytes=serialize_put_command("PRIOR", 1.0, int(time.time() * 1000))
        )
        raft_node.storage.log = [committed_entry]

        # Add entries and flush
        entries = []
        for i in range(3):
            entry = LogEntry(
                index=i + 2,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            )
            entries.append(entry)
            await raft_node._add_to_batch(entry)

        # Verify truncation was called once, from the first failed entry's index
        raft_node.storage.truncate_log_from.assert_called_once_with(entries[0].index)

        # And that the prior committed entry was NOT removed
        assert raft_node.storage.log == [committed_entry]
    
    @pytest.mark.asyncio
    async def test_put_price_uses_batching(self, raft_node):
        """Test that put_price uses batching instead of immediate replication."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock batch operations - put_price now awaits the future _add_to_batch
        # returns (resolved once the entry's batch actually flushes), so the
        # mock must return an already-resolved future for this success case.
        flushed_future = asyncio.get_event_loop().create_future()
        flushed_future.set_result(True)
        raft_node._add_to_batch = AsyncMock(return_value=flushed_future)

        # Call put_price
        result = await raft_node.put_price("AAPL", 150.0, int(time.time() * 1000))
        
        # Should succeed
        assert result["ok"] is True
        assert result["leader_hint"] == "node1"
        
        # Should have added to batch
        raft_node._add_to_batch.assert_called_once()
        
        # Should have appended to local log
        raft_node.storage.append_entries.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_batch_size_configuration(self):
        """Test that batch size can be configured."""
        peers = [PeerInfo("node2", "localhost", 50062, 50052, 50062)]
        
        # Test custom batch size
        node = RaftNode(
            node_id="node1",
            peers=peers,
            data_dir="/tmp",
            batch_size=5,
            flush_interval=200
        )
        
        assert node.batch_size == 5
        assert node.flush_interval == 200
    
    @pytest.mark.asyncio
    async def test_environment_variable_configuration(self, monkeypatch):
        """Test configuration via environment variables."""
        monkeypatch.setenv("RAFT_BATCH_SIZE", "7")
        monkeypatch.setenv("RAFT_FLUSH_INTERVAL_MS", "300")
        
        peers = [PeerInfo("node2", "localhost", 50062, 50052, 50062)]
        
        node = RaftNode(
            node_id="node1",
            peers=peers,
            data_dir="/tmp"
        )
        
        assert node.batch_size == 7
        assert node.flush_interval == 300
    
    @pytest.mark.asyncio
    async def test_batch_flush_task_lifecycle(self, raft_node):
        """Test batch flush task start/stop lifecycle."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        
        # Start batch flush task
        await raft_node._start_batch_flush_task()
        assert raft_node.batch_flush_task is not None
        assert not raft_node.batch_flush_task.done()
        
        # Stop batch flush task
        await raft_node._stop_batch_flush_task()
        assert raft_node.batch_flush_task.done()
    
    @pytest.mark.asyncio
    async def test_batch_flush_empty_pending(self, raft_node):
        """Test that flush does nothing when no pending entries."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        
        # Mock replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        
        # Flush with no pending entries
        await raft_node._flush_pending_entries()
        
        # Should not call replication
        raft_node._replicate_to_peers.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_concurrent_batch_operations(self, raft_node):
        """Test concurrent batch operations are thread-safe."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Add entries concurrently
        async def add_entry(i):
            await raft_node._add_to_batch(LogEntry(
                index=i + 1,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            ))
        
        # Run concurrent operations
        tasks = [add_entry(i) for i in range(10)]
        await asyncio.gather(*tasks)
        
        # Should have flushed multiple times
        assert raft_node._replicate_to_peers.call_count > 0
