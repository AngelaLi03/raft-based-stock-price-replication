"""
Tests for server/metrics_server.py's HTTP handlers.

Regression coverage for a bug that could only ever be triggered once the
port-collision bug (see README Known Issues) was fixed and this server
could actually bind and receive a real request: aiohttp's web.Response
rejects a charset embedded inside content_type when text= is also given.
"""

import pytest
from unittest.mock import MagicMock, patch

from server.metrics_server import MetricsServer


@pytest.mark.asyncio
async def test_handle_metrics_with_recorded_metrics():
    server = MetricsServer(port=0)
    mock_metrics = MagicMock()
    mock_metrics.get_metrics.return_value = "raft_commits_total 3.0\n"

    with patch("raft.prometheus_metrics.get_prometheus_metrics", return_value=mock_metrics):
        response = await server.handle_metrics(MagicMock())

    assert response.status == 200
    assert "charset" in response.content_type.lower() or response.charset is not None
    assert b"raft_commits_total" in response.body


@pytest.mark.asyncio
async def test_handle_metrics_with_no_metrics_initialized():
    server = MetricsServer(port=0)

    with patch("raft.prometheus_metrics.get_prometheus_metrics", return_value=None):
        response = await server.handle_metrics(MagicMock())

    assert response.status == 200
    assert b"No metrics available" in response.body


@pytest.mark.asyncio
async def test_handle_health():
    server = MetricsServer(port=0)
    response = await server.handle_health(MagicMock())

    assert response.status == 200
    assert response.body == b"OK"


@pytest.mark.asyncio
async def test_handle_ready_returns_503_without_a_node_reference():
    """No node wired in means we can't assert readiness - fail closed."""
    server = MetricsServer(port=0)

    response = await server.handle_ready(MagicMock())

    assert response.status == 503


@pytest.mark.asyncio
async def test_handle_ready_returns_503_while_still_a_candidate():
    """Mid-election, the node can't usefully serve traffic yet."""
    from raft.types import RaftState

    node = MagicMock()
    node.state = RaftState.CANDIDATE
    server = MetricsServer(port=0, raft_node=node)

    response = await server.handle_ready(MagicMock())

    assert response.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("state_name", ["FOLLOWER", "LEADER"])
async def test_handle_ready_returns_200_once_role_is_settled(state_name):
    """A settled follower serves reads; a leader serves writes. Both ready."""
    from raft.types import RaftState

    node = MagicMock()
    node.state = getattr(RaftState, state_name)
    server = MetricsServer(port=0, raft_node=node)

    response = await server.handle_ready(MagicMock())

    assert response.status == 200
    assert state_name.lower().encode() in response.body.lower()


@pytest.mark.asyncio
async def test_health_stays_unconditional_ok():
    """Liveness must NOT become role-aware - a candidate is alive, just not
    ready, and restarting it mid-election would be actively harmful."""
    from raft.types import RaftState

    node = MagicMock()
    node.state = RaftState.CANDIDATE
    server = MetricsServer(port=0, raft_node=node)

    response = await server.handle_health(MagicMock())

    assert response.status == 200
    assert response.body == b"OK"
