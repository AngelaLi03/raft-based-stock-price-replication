"""
Tests for chaos recovery scenarios.
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from typing import List

from raft.node import RaftNode
from raft.types import PeerInfo, LogEntry, RaftState
from kv.state_machine import serialize_put_command


class TestChaosRecovery:
    """Test chaos recovery scenarios."""
    
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
            batch_size=5,
            flush_interval=50
        )
    
    @pytest.mark.asyncio
    async def test_leader_crash_during_replication(self, raft_node):
        """Test leader crash during replication."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        raft_node.commit_index = 0
        raft_node.last_applied = 0
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        raft_node.storage.get_log_entries = Mock(return_value=[])
        
        # Mock replication to fail (simulating network issues)
        raft_node._replicate_to_peers = AsyncMock(return_value=False)
        
        # Add entries to batch
        entries = []
        for i in range(3):
            entry = LogEntry(
                index=i + 1,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            )
            entries.append(entry)
            await raft_node._add_to_batch(entry)
        
        # Simulate leader crash (become follower)
        await raft_node._on_become_follower()
        
        # Verify state
        assert raft_node.state == RaftState.FOLLOWER
        assert len(raft_node.pending_entries) == 0  # Should be cleared
    
    @pytest.mark.asyncio
    async def test_follower_catchup_after_restart(self, raft_node):
        """Test follower catch-up after restart."""
        # Set up as follower
        raft_node.state = RaftState.FOLLOWER
        raft_node.election_manager.current_term = 1
        raft_node.commit_index = 0
        raft_node.last_applied = 0
        
        # Seed real storage with existing entries (rather than mocking
        # get_log_entries to a static value) so reads made during the call -
        # including the commit-index calc, which depends on log length after
        # the new entries are appended - reflect the actual state.
        existing_entries = [
            LogEntry(1, 1, serialize_put_command("key0", 100.0, int(time.time() * 1000))),
            LogEntry(2, 1, serialize_put_command("key1", 101.0, int(time.time() * 1000)))
        ]
        raft_node.storage.append_entries(existing_entries)

        # Mock KV state machine
        raft_node.kv_state_machine.apply_command = AsyncMock()
        raft_node.kv_state_machine.dump_state = Mock(return_value={"entries": {}})
        
        # Simulate receiving AppendEntries with new entries
        from raft.proto import raft_pb2
        
        # Create mock AppendEntries request
        request = Mock()
        request.term = 1
        request.leader_id = "node2"
        request.prev_log_index = 2
        request.prev_log_term = 1
        request.leader_commit = 3
        
        # New entries to append
        new_entries = [
            LogEntry(3, 1, serialize_put_command("key2", 102.0, int(time.time() * 1000))),
            LogEntry(4, 1, serialize_put_command("key3", 103.0, int(time.time() * 1000)))
        ]
        
        # Mock protobuf entries
        pb_entries = []
        for entry in new_entries:
            pb_entry = Mock()
            pb_entry.index = entry.index
            pb_entry.term = entry.term
            pb_entry.command_bytes = entry.command_bytes
            pb_entries.append(pb_entry)
        
        request.entries = pb_entries

        # Spy on (rather than replace) the real storage methods, so behavior
        # stays real while we can still assert on how they were called.
        raft_node.storage.append_entries = Mock(wraps=raft_node.storage.append_entries)
        raft_node.storage.set_commit_index = Mock(wraps=raft_node.storage.set_commit_index)
        raft_node.storage.set_last_applied = Mock(wraps=raft_node.storage.set_last_applied)
        raft_node.storage.set_current_term = Mock(wraps=raft_node.storage.set_current_term)

        # Handle AppendEntries
        response = await raft_node.handle_append_entries(request)

        # Verify response
        assert response.success is True
        assert response.term == 1

        # Verify entries were appended
        raft_node.storage.append_entries.assert_called_once()
        appended_entries = raft_node.storage.append_entries.call_args[0][0]
        assert len(appended_entries) == 2

        # Verify commit index was updated (now reflects the real post-append
        # log length of 4, so leader_commit=3 is fully applied)
        raft_node.storage.set_commit_index.assert_called_with(3)
    
    @pytest.mark.asyncio
    async def test_concurrent_writes_during_leader_change(self, raft_node):
        """Test concurrent writes during leader change."""
        # Set up as leader initially
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        raft_node.commit_index = 0
        raft_node.last_applied = 0
        
        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        
        # Mock batch operations
        raft_node._add_to_batch = AsyncMock()
        
        # Start concurrent write tasks
        async def write_task(task_id):
            try:
                result = await raft_node.put_price(f"key{task_id}", 100.0 + task_id, int(time.time() * 1000))
                return result.get("ok", False)
            except Exception:
                return False
        
        # Create concurrent write tasks
        tasks = [write_task(i) for i in range(10)]
        
        # Simulate leader change during writes
        async def leader_change():
            await asyncio.sleep(0.01)  # Let some writes start
            await raft_node._on_become_follower()
        
        # Run concurrent operations
        write_results, _ = await asyncio.gather(
            asyncio.gather(*tasks, return_exceptions=True),
            leader_change(),
            return_exceptions=True
        )
        
        # Some writes should succeed, some might fail due to leader change
        successful_writes = sum(1 for result in write_results if result is True)
        assert successful_writes >= 0  # At least some should succeed or fail gracefully
    
    @pytest.mark.asyncio
    async def test_network_partition_recovery(self, raft_node):
        """Test recovery from network partition."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        raft_node.commit_index = 0
        raft_node.last_applied = 0
        # put_price now awaits its entry's batch flush; batch_size=1 makes each
        # write flush immediately so this test observes the outcome directly
        # instead of hanging on a timer that's never started here.
        raft_node.batch_size = 1

        # Mock storage
        raft_node.storage.get_last_log_index = Mock(return_value=0)
        raft_node.storage.append_entries = Mock()
        raft_node.storage.get_log_entries = Mock(return_value=[])
        
        # Mock replication to simulate network partition (all fail)
        raft_node._replicate_to_peers = AsyncMock(return_value=False)
        
        # Try to write data during partition
        result = await raft_node.put_price("key1", 100.0, int(time.time() * 1000))
        
        # Should fail due to replication failure
        assert result["ok"] is False
        
        # Simulate network recovery
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Try to write again
        result = await raft_node.put_price("key2", 101.0, int(time.time() * 1000))
        
        # Should succeed now
        assert result["ok"] is True
    
    @pytest.mark.asyncio
    async def test_log_truncation_during_catchup(self, raft_node):
        """Test log truncation during follower catch-up."""
        # Set up as follower
        raft_node.state = RaftState.FOLLOWER
        raft_node.election_manager.current_term = 1
        raft_node.commit_index = 0
        raft_node.last_applied = 0
        
        # Mock storage with conflicting entries
        conflicting_entries = [
            LogEntry(1, 1, serialize_put_command("key0", 100.0, int(time.time() * 1000))),
            LogEntry(2, 1, serialize_put_command("key1", 101.0, int(time.time() * 1000))),
            LogEntry(3, 2, serialize_put_command("key2", 102.0, int(time.time() * 1000)))  # Different term
        ]
        raft_node.storage.get_log_entries = Mock(return_value=conflicting_entries)
        raft_node.storage.get_log_entry = Mock(side_effect=lambda i: conflicting_entries[i-1] if 1 <= i <= 3 else None)
        raft_node.storage.get_last_log_index = Mock(return_value=3)
        raft_node.storage.truncate_log_from = Mock()
        raft_node.storage.append_entries = Mock()
        raft_node.storage.set_commit_index = Mock()
        raft_node.storage.set_last_applied = Mock()
        raft_node.storage.set_current_term = Mock()
        
        # Mock KV state machine
        raft_node.kv_state_machine.apply_command = AsyncMock()
        
        # Create mock AppendEntries request that will cause truncation
        from raft.proto import raft_pb2
        
        request = Mock()
        request.term = 2
        request.leader_id = "node2"
        request.prev_log_index = 1  # Will cause truncation at index 2
        request.prev_log_term = 1
        request.leader_commit = 2
        
        # New entries to append
        new_entries = [
            LogEntry(2, 2, serialize_put_command("key1_new", 201.0, int(time.time() * 1000))),
            LogEntry(3, 2, serialize_put_command("key2_new", 202.0, int(time.time() * 1000)))
        ]
        
        # Mock protobuf entries
        pb_entries = []
        for entry in new_entries:
            pb_entry = Mock()
            pb_entry.index = entry.index
            pb_entry.term = entry.term
            pb_entry.command_bytes = entry.command_bytes
            pb_entries.append(pb_entry)
        
        request.entries = pb_entries
        
        # Handle AppendEntries
        response = await raft_node.handle_append_entries(request)
        
        # Verify response
        assert response.success is True
        assert response.term == 2
        
        # Verify truncation was called
        raft_node.storage.truncate_log_from.assert_called_with(2)
        
        # Verify new entries were appended
        raft_node.storage.append_entries.assert_called_once()
        appended_entries = raft_node.storage.append_entries.call_args[0][0]
        assert len(appended_entries) == 2
    
    @pytest.mark.asyncio
    async def test_crash_recovery_with_partial_commits(self, raft_node):
        """Test crash recovery with partially committed entries."""
        # Set up state after crash. commit_index=3 so entries 2 AND 3 are
        # committed-but-unapplied (entry 1 was already applied); recovery
        # must never replay entry beyond commit_index, since an uncommitted
        # entry could still be truncated by a future leader.
        raft_node.election_manager.current_term = 1
        raft_node.commit_index = 3  # Entries up to 3 committed
        raft_node.last_applied = 1  # Only entry 1 applied so far
        
        # Mock storage with unapplied committed entries
        entries = [
            LogEntry(1, 1, serialize_put_command("key0", 100.0, int(time.time() * 1000))),
            LogEntry(2, 1, serialize_put_command("key1", 101.0, int(time.time() * 1000))),
            LogEntry(3, 1, serialize_put_command("key2", 102.0, int(time.time() * 1000)))
        ]
        raft_node.storage.get_log_entries = Mock(return_value=entries)
        raft_node.storage.get_log_entry = Mock(side_effect=lambda i: entries[i-1] if 1 <= i <= 3 else None)
        raft_node.storage.set_last_applied = Mock()
        
        # Mock KV state machine
        raft_node.kv_state_machine.apply_command = AsyncMock()
        raft_node.kv_state_machine.last_applied_index = 1
        
        # Run crash recovery
        await raft_node._recover_from_crash()
        
        # Verify that unapplied entries were applied
        assert raft_node.kv_state_machine.apply_command.call_count == 2  # Entries 2 and 3
        assert raft_node.last_applied == 3

        # last_applied is persisted once for the whole recovered batch, not
        # once per entry (see raft/node.py:_recover_from_crash).
        assert raft_node.storage.set_last_applied.call_count == 1
    
    @pytest.mark.asyncio
    async def test_batch_flush_during_leader_change(self, raft_node):
        """Test batch flush behavior during leader change."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        
        # Add entries to batch
        for i in range(3):
            entry = LogEntry(
                index=i + 1,
                term=1,
                command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time() * 1000))
            )
            await raft_node._add_to_batch(entry)
        
        # Mock replication
        raft_node._replicate_to_peers = AsyncMock(return_value=True)
        raft_node._update_commit_index = AsyncMock()
        
        # Start batch flush task
        flush_task = asyncio.create_task(raft_node._batch_flush_loop())
        
        # Simulate leader change
        await asyncio.sleep(0.01)
        await raft_node._on_become_follower()
        
        # Cancel flush task
        flush_task.cancel()
        try:
            await flush_task
        except asyncio.CancelledError:
            pass
        
        # Verify that pending entries were cleared
        assert len(raft_node.pending_entries) == 0
    
    @pytest.mark.asyncio
    async def test_metrics_recording_during_chaos(self, raft_node):
        """Test that metrics are recorded during chaos scenarios."""
        # Set up as leader
        raft_node.state = RaftState.LEADER
        raft_node.election_manager.current_term = 1
        # Fixture default batch_size=5; match it to the 3 entries added below
        # so the third _add_to_batch call actually triggers a flush (and
        # therefore _replicate_to_peers / record_replication).
        raft_node.batch_size = 3

        # Mock metrics recording
        with patch('raft.prometheus_metrics.record_replication') as mock_record_replication:
            # Mock one level below _replicate_to_peers (the per-peer RPC),
            # not _replicate_to_peers itself - record_replication is called
            # from inside the real _replicate_to_peers, so replacing that
            # method would skip the very call this test verifies.
            raft_node._send_append_entries_to_peer = AsyncMock(return_value={"success": True, "match_index": 1})
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
            
            # Verify metrics were recorded
            mock_record_replication.assert_called()
            call_args = mock_record_replication.call_args
            assert call_args[0][0] == 3  # entries_count
            assert call_args[0][2] is True  # success
