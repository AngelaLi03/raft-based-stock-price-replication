"""
Tests for Raft log snapshotting/compaction: storage-level compaction and
snapshot persistence, RaftNode's threshold-triggered snapshotting and
crash recovery from a snapshot, and the InstallSnapshot RPC (both sides).
"""

import pytest
import tempfile
import time
from unittest.mock import AsyncMock, MagicMock, Mock

from raft.storage import RaftStorage
from raft.node import RaftNode
from raft.types import PeerInfo, LogEntry, RaftState
from kv.state_machine import serialize_put_command


@pytest.fixture
def mock_peers():
    return [
        PeerInfo("node2", "localhost", 50062, 50052, 50062),
        PeerInfo("node3", "localhost", 50063, 50053, 50063)
    ]


@pytest.fixture
def temp_data_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


class TestStorageSnapshotting:
    """Storage-level compaction and snapshot persistence."""

    def _entries(self, n, start=1):
        return [
            LogEntry(index=i, term=1, command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time())))
            for i in range(start, start + n)
        ]

    def test_save_snapshot_compacts_log(self, temp_data_dir):
        storage = RaftStorage(temp_data_dir, "node1")
        storage.append_entries(self._entries(5))  # indices 1-5

        storage.save_snapshot(3, last_included_term=1, kv_state={"AAPL": {"symbol": "AAPL", "price": 1.0, "timestamp": 1}})

        # Entries 1-3 compacted away; 4-5 remain
        assert len(storage.get_log_entries()) == 2
        assert storage.get_log_entry(3) is None
        assert storage.get_log_entry(4) is not None
        assert storage.get_log_entry(4).index == 4

        # Absolute log length is unaffected by compaction
        assert storage.get_last_log_index() == 5
        assert storage.last_included_index == 3
        assert storage.last_included_term == 1

    def test_get_last_log_term_falls_back_to_snapshot_when_log_empty(self, temp_data_dir):
        storage = RaftStorage(temp_data_dir, "node1")
        storage.append_entries(self._entries(3))

        storage.save_snapshot(3, last_included_term=2, kv_state={})

        # Everything got compacted away - term should come from the snapshot
        assert len(storage.get_log_entries()) == 0
        assert storage.get_last_log_term() == 2
        assert storage.get_last_log_index() == 3

    def test_load_snapshot_roundtrip(self, temp_data_dir):
        storage = RaftStorage(temp_data_dir, "node1")
        storage.append_entries(self._entries(2))
        kv_state = {"AAPL": {"symbol": "AAPL", "price": 150.0, "timestamp": 123}}

        storage.save_snapshot(2, last_included_term=1, kv_state=kv_state)

        loaded = storage.load_snapshot()
        assert loaded["last_included_index"] == 2
        assert loaded["last_included_term"] == 1
        assert loaded["kv_state"] == kv_state

    def test_truncate_refuses_into_snapshot(self, temp_data_dir):
        storage = RaftStorage(temp_data_dir, "node1")
        storage.append_entries(self._entries(5))
        storage.save_snapshot(3, last_included_term=1, kv_state={})

        # Attempting to truncate at/before the snapshot boundary must be a
        # no-op - that history is only committed, durable data now.
        storage.truncate_log_from(2)

        assert len(storage.get_log_entries()) == 2  # entries 4, 5 still there
        assert storage.last_included_index == 3

    def test_snapshot_and_compacted_log_persist_across_restart(self, temp_data_dir):
        storage1 = RaftStorage(temp_data_dir, "node1")
        storage1.append_entries(self._entries(5))
        storage1.save_snapshot(3, last_included_term=1, kv_state={"AAPL": {"symbol": "AAPL", "price": 1.0, "timestamp": 1}})

        # Simulate a restart: fresh RaftStorage instance over the same dir
        storage2 = RaftStorage(temp_data_dir, "node1")

        assert storage2.last_included_index == 3
        assert storage2.last_included_term == 1
        assert storage2.get_last_log_index() == 5
        assert len(storage2.get_log_entries()) == 2
        assert storage2.get_log_entry(4).index == 4

    def test_save_snapshot_is_noop_if_not_newer(self, temp_data_dir):
        storage = RaftStorage(temp_data_dir, "node1")
        storage.append_entries(self._entries(5))
        storage.save_snapshot(3, last_included_term=1, kv_state={})

        # A second, non-advancing snapshot must not touch state
        storage.save_snapshot(2, last_included_term=1, kv_state={"should": "be ignored"})

        assert storage.last_included_index == 3
        loaded = storage.load_snapshot()
        assert loaded["last_included_index"] == 3


class TestNodeSnapshotting:
    """RaftNode-level snapshot triggering and crash recovery."""

    @pytest.mark.asyncio
    async def test_snapshot_triggered_at_threshold(self, mock_peers, temp_data_dir):
        node = RaftNode(node_id="node1", peers=mock_peers, data_dir=temp_data_dir)
        node.snapshot_threshold = 3
        node.election_manager.current_term = 1
        node.commit_index = 0
        node.last_applied = 0

        entries = [
            LogEntry(index=i, term=1, command_bytes=serialize_put_command(f"key{i}", 100.0, int(time.time())))
            for i in range(1, 4)
        ]
        node.storage.append_entries(entries)
        node.commit_index = 3

        await node._apply_committed_entries()

        assert node.storage.last_included_index == 3
        assert len(node.storage.get_log_entries()) == 0  # fully compacted
        assert node.storage.load_snapshot() is not None

    @pytest.mark.asyncio
    async def test_recovery_restores_from_snapshot_without_replaying_compacted_log(self, mock_peers, temp_data_dir):
        # First "session": write entries, force a snapshot, then stop.
        node1 = RaftNode(node_id="node1", peers=mock_peers, data_dir=temp_data_dir)
        node1.snapshot_threshold = 3
        node1.election_manager.current_term = 1

        entries = [
            LogEntry(index=i, term=1, command_bytes=serialize_put_command(f"key{i}", 100.0 + i, int(time.time())))
            for i in range(1, 4)
        ]
        node1.storage.append_entries(entries)
        node1.commit_index = 3
        await node1.kv_state_machine.start()
        await node1._apply_committed_entries()
        await node1.kv_state_machine.stop()

        assert node1.storage.last_included_index == 3
        assert len(node1.storage.get_log_entries()) == 0

        # Second "session": fresh RaftNode over the same data_dir. The
        # compacted entries no longer exist in the log at all - recovery
        # must come entirely from the snapshot.
        node2 = RaftNode(node_id="node1", peers=mock_peers, data_dir=temp_data_dir)
        await node2.start()

        assert node2.last_applied == 3
        assert node2.commit_index == 3
        assert node2.kv_state_machine.get("key1") is not None
        assert node2.kv_state_machine.get("key3") is not None

        await node2.stop()


class TestInstallSnapshotRPC:
    """InstallSnapshot RPC: leader triggers it, follower applies it."""

    @pytest.mark.asyncio
    async def test_leader_sends_install_snapshot_when_peer_behind_compacted_log(self, mock_peers, temp_data_dir):
        node = RaftNode(node_id="node1", peers=mock_peers, data_dir=temp_data_dir)
        node.state = RaftState.LEADER
        node.election_manager.current_term = 1

        # Simulate having already compacted past index 5, but this peer
        # thinks it still needs index 2 - normal AppendEntries can't help.
        node.storage.last_included_index = 5
        node.next_index["node2"] = 2
        node.match_index["node2"] = 0

        node._send_install_snapshot_to_peer = AsyncMock()

        await node._send_heartbeat_to_peer(mock_peers[0])

        node._send_install_snapshot_to_peer.assert_called_once_with(mock_peers[0])

    @pytest.mark.asyncio
    async def test_follower_applies_install_snapshot(self, mock_peers, temp_data_dir):
        import json
        from raft.proto import raft_pb2

        node = RaftNode(node_id="node2", peers=mock_peers, data_dir=temp_data_dir)
        await node.kv_state_machine.start()

        # Follower has some stale local entries that should be wiped by the
        # snapshot install rather than reconciled entry-by-entry.
        node.storage.append_entries([
            LogEntry(index=1, term=1, command_bytes=serialize_put_command("STALE", 1.0, 1)),
        ])

        kv_state = {"AAPL": {"symbol": "AAPL", "price": 150.0, "timestamp": 123}}
        request = MagicMock()
        request.term = 1
        request.leader_id = "node1"
        request.last_included_index = 10
        request.last_included_term = 1
        request.data = json.dumps(kv_state).encode('utf-8')

        response = await node.handle_install_snapshot(request)

        assert isinstance(response, raft_pb2.InstallSnapshotResponse)
        assert node.storage.last_included_index == 10
        assert node.commit_index == 10
        assert node.last_applied == 10
        assert node.kv_state_machine.get("AAPL").price == 150.0
        assert node.kv_state_machine.get("STALE") is None  # wiped, not merged
        assert len(node.storage.get_log_entries()) == 0

        await node.kv_state_machine.stop()

    @pytest.mark.asyncio
    async def test_install_snapshot_rejects_stale_term(self, mock_peers, temp_data_dir):
        from raft.proto import raft_pb2

        node = RaftNode(node_id="node2", peers=mock_peers, data_dir=temp_data_dir)
        node.election_manager.current_term = 5

        request = MagicMock()
        request.term = 2  # stale
        request.leader_id = "node1"
        request.last_included_index = 10
        request.last_included_term = 1
        request.data = b'{}'

        response = await node.handle_install_snapshot(request)

        assert response.term == 5
        assert node.storage.last_included_index == 0  # untouched


class TestSnapshotBoundaryLogMatching:
    """
    Regression coverage for a real bug found via live testing: the entry at
    exactly the snapshot boundary (last_included_index) isn't reachable via
    get_log_entry (only entries *after* it remain in the log), so both the
    log-matching check and the prev_log_term a leader sends need their own
    handling of that boundary index specifically - otherwise a follower's
    very first AppendEntries after receiving a snapshot always fails log
    matching, next_index gets decremented back onto the boundary, and the
    leader re-sends InstallSnapshot again next heartbeat. Forever. Live
    symptom: leader logs showed "Installed snapshot on nodeX" repeating in
    a tight loop for every peer, and every write failed with "Replication
    failed" because no follower could ever actually accept a real entry
    again once it had received one snapshot.
    """

    def test_check_log_matching_succeeds_exactly_at_snapshot_boundary(self, mock_peers, temp_data_dir):
        node = RaftNode(node_id="node2", peers=mock_peers, data_dir=temp_data_dir)
        # Simulate a follower that just received a snapshot covering up to
        # index 10 (term 3) and therefore has an empty log - exactly the
        # state right after handle_install_snapshot.
        node.storage.last_included_index = 10
        node.storage.last_included_term = 3

        # The leader's very next AppendEntries after installing that
        # snapshot uses prev_log_index=10, prev_log_term=3 (the boundary
        # itself) - this must match.
        assert node._check_log_matching(10, 3) is True
        # A mismatched term at the boundary must still correctly fail.
        assert node._check_log_matching(10, 99) is False

    def test_get_term_at_index_returns_snapshot_term_at_boundary(self, temp_data_dir):
        from raft.storage import RaftStorage
        storage = RaftStorage(temp_data_dir, "node1")
        storage.append_entries([
            LogEntry(index=1, term=1, command_bytes=b"a"),
            LogEntry(index=2, term=1, command_bytes=b"b"),
        ])
        storage.save_snapshot(2, last_included_term=7, kv_state={})

        # Index 2 is now the snapshot boundary - not in self.log anymore,
        # but get_term_at_index must still resolve its real term (7), not 0.
        assert storage.get_term_at_index(2) == 7
        assert storage.get_term_at_index(0) == 0

    @pytest.mark.asyncio
    async def test_leader_sends_correct_prev_log_term_at_snapshot_boundary(self, mock_peers, temp_data_dir):
        node = RaftNode(node_id="node1", peers=mock_peers, data_dir=temp_data_dir)
        node.state = RaftState.LEADER
        node.election_manager.current_term = 5
        node.storage.last_included_index = 10
        node.storage.last_included_term = 3
        # This peer just got caught up via InstallSnapshot to exactly the
        # boundary - next_index is one past it, so prev_log_index will be
        # the boundary itself.
        node.next_index["node2"] = 11
        node.match_index["node2"] = 10

        captured = {}

        async def fake_append_entries_to_peer(peer_id, peer, term, prev_log_index, prev_log_term, entries, leader_commit):
            captured["prev_log_index"] = prev_log_index
            captured["prev_log_term"] = prev_log_term
            return MagicMock(success=True, match_index=10)

        node._append_entries_to_peer = fake_append_entries_to_peer

        await node._send_heartbeat_to_peer(mock_peers[0])

        assert captured["prev_log_index"] == 10
        # Before the fix this was always 0 (get_log_entry(10) returns None
        # at the boundary), which would make the follower reject the RPC.
        assert captured["prev_log_term"] == 3
