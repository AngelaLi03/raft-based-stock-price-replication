"""
Tests for scripts/kvctl.py's RaftClient.
"""

import asyncio
import pytest

from scripts.kvctl import RaftClient


@pytest.mark.asyncio
async def test_get_cluster_info_times_out_on_unresponsive_peer():
    """A node that accepts the TCP connection but never responds (e.g.
    mid-restart, or - as observed live - a node struggling under an
    election storm) must not hang the caller forever. This is exactly what
    made chaos_test.py itself hang for 25+ minutes: _get_leader() loops
    over every node's get_cluster_info() with no per-call timeout, so one
    unresponsive node blocks the whole loop indefinitely."""
    async def _accept_and_never_respond(reader, writer):
        await asyncio.sleep(3600)

    server = await asyncio.start_server(_accept_and_never_respond, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = RaftClient("127.0.0.1", port)

    try:
        result = await asyncio.wait_for(client.get_cluster_info(), timeout=5.0)
        assert result is None
    finally:
        server.close()
        await server.wait_closed()
