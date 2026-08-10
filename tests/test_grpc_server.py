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
from unittest.mock import AsyncMock, MagicMock, patch

from server.grpc_server import ClientService, GrpcServer
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


@pytest.mark.asyncio
async def test_start_wires_raft_node_into_metrics_server():
    """Regression test for the wiring that makes /ready meaningful at all.

    GrpcServer.start() calls start_metrics_server(port, raft_node=self.raft_node).
    Nothing else asserts this kwarg is actually passed through - if it were
    accidentally dropped, MetricsServer would have no RaftNode to read state
    from and /ready would presumably 503 (or error) forever on every pod,
    the exact worst-case failure this whole feature exists to avoid, while
    the rest of the test suite (which only exercises MetricsServer.handle_ready
    directly with a manually-constructed instance) would keep passing.
    """
    raft_node = MagicMock()
    raft_node.node_id = "raft-node-0"

    server = GrpcServer(raft_node, raft_port=50051, client_port=51051)

    # resolve_metrics_port and start_metrics_server are imported locally
    # inside GrpcServer.start(), so they must be patched at their source
    # modules (`from X import Y` re-resolves Y from X's namespace at call
    # time) rather than as attributes of server.grpc_server.
    with patch("raft.types.resolve_metrics_port", return_value=8000), \
         patch("server.metrics_server.start_metrics_server", new=AsyncMock()) as mock_start:
        # Avoid actually binding real gRPC servers/ports.
        with patch("server.grpc_server.grpc.aio.server") as mock_grpc_server:
            fake_server = MagicMock()
            fake_server.start = AsyncMock()
            mock_grpc_server.return_value = fake_server

            await server.start()

    mock_start.assert_awaited_once()
    _, kwargs = mock_start.call_args
    assert kwargs["raft_node"] is raft_node
