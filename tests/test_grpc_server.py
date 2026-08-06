"""
Tests for the gRPC service layer (server/grpc_server.py).

This layer had zero test coverage before - it's "just" translation between
RaftNode dicts and protobuf messages, but that translation is exactly where
a real bug slipped through: prometheus_client's Counter/Gauge values are
always Python floats internally, and assigning a float into a protobuf
uint64 field raises a TypeError. No unit test caught it because nothing
exercised DumpState with metrics that had actually been recorded (as
opposed to the always-zero, never-updated old collector) - only a live
`kvctl.py dump-state` call after real writes did.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from server.grpc_server import ClientService
from client.proto import client_pb2


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.mark.asyncio
async def test_dump_state_converts_float_metrics_to_protobuf_ints(mock_context):
    """
    Regression test: PrometheusMetrics.get_metrics_dict() returns *_total
    fields as Python floats (prometheus_client's internal storage type),
    even though the protobuf RaftMetrics message declares them uint64.
    Assigning a float there must not raise.
    """
    raft_node = MagicMock()
    raft_node.dump_state = AsyncMock(return_value={
        "ok": True,
        "node_id": "node1",
        "current_term": 5,
        "state": "leader",
        "commit_index": 3,
        "last_applied": 3,
        "log_length": 3,
        "kv_entries": 2,
        "kv_store": {
            "AAPL": {"price": 150.0, "timestamp": 1234567890},
        },
        # Realistic shape of PrometheusMetrics.get_metrics_dict(): every
        # *_total field is a float, not an int.
        "metrics": {
            "elections_total": 1.0,
            "commits_total": 3.0,
            "entries_replicated_total": 3.0,
            "replication_failures_total": 0.0,
            "crash_recoveries_total": 0.0,
            "replay_entries_total": 0.0,
            "storage_writes_total": 6.0,
            "storage_reads_total": 0.0,
            "commands_applied_total": 3.0,
            "kv_entries_total": 2.0,
        }
    })

    service = ClientService(raft_node)
    request = client_pb2.DumpStateRequest()

    response = await service.DumpState(request, mock_context)

    assert response.ok is True
    assert response.metrics.elections_total == 1
    assert response.metrics.commits_total == 3
    assert response.metrics.entries_replicated_total == 3
    assert response.metrics.storage_writes_total == 6
    assert response.metrics.kv_entries_total == 2
    # context.set_code should never have been called - that only happens
    # on the exception path this bug used to trigger.
    mock_context.set_code.assert_not_called()


@pytest.mark.asyncio
async def test_dump_state_with_no_metrics(mock_context):
    """DumpState must still work if metrics were never initialized."""
    raft_node = MagicMock()
    raft_node.dump_state = AsyncMock(return_value={
        "ok": True,
        "node_id": "node1",
        "current_term": 0,
        "state": "follower",
        "commit_index": 0,
        "last_applied": 0,
        "log_length": 0,
        "kv_entries": 0,
        "kv_store": {},
        "metrics": None,
    })

    service = ClientService(raft_node)
    response = await service.DumpState(client_pb2.DumpStateRequest(), mock_context)

    assert response.ok is True
    assert response.node_id == "node1"
