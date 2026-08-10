"""
HTTP server for Prometheus metrics endpoint.
"""

import logging
from aiohttp import web, web_request, web_response
from typing import Optional

logger = logging.getLogger(__name__)


class MetricsServer:
    """HTTP server for serving Prometheus metrics."""
    
    def __init__(self, port: int = 8000, raft_node=None):
        self.port = port
        # Optional so the Compose path and unit tests can construct this
        # without a node; /ready fails closed (503) when it's absent.
        self.raft_node = raft_node
        self.app = web.Application()
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.app.router.add_get('/health', self.handle_health)
        self.app.router.add_get('/ready', self.handle_ready)
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
    
    async def start(self) -> None:
        """Start the metrics server."""
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            logger.info(f"Metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop the metrics server."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info("Metrics server stopped")
    
    async def handle_metrics(self, request: web_request.Request) -> web_response.Response:
        """Handle /metrics endpoint.

        content_type intentionally omits ';charset=utf-8' - aiohttp's
        Response rejects a charset embedded in content_type when text= is
        also given (it wants to own that via a separate charset= kwarg, and
        raises "charset must not be in content_type argument" otherwise). It
        appends ';charset=utf-8' itself by default, producing the same final
        header Prometheus expects. This only ever got exercised once this
        server could actually bind its port - see README Known Issues.
        """
        try:
            from raft.prometheus_metrics import get_prometheus_metrics
            metrics = get_prometheus_metrics()
            
            if metrics:
                return web.Response(
                    text=metrics.get_metrics(),
                    content_type='text/plain; version=0.0.4'
                )
            else:
                return web.Response(
                    text="# No metrics available\n",
                    content_type='text/plain; version=0.0.4'
                )
        except Exception as e:
            logger.error(f"Error serving metrics: {e}")
            return web.Response(
                text=f"# Error: {e}\n",
                content_type='text/plain; version=0.0.4',
                status=500
            )
    
    async def handle_health(self, request: web_request.Request) -> web_response.Response:
        """Handle /health endpoint."""
        return web.Response(
            text="OK",
            content_type='text/plain'
        )

    async def handle_ready(self, request: web_request.Request) -> web_response.Response:
        """Handle /ready endpoint - the Kubernetes readiness probe.

        Distinct from /health (liveness) on purpose. Liveness answers "is
        this process alive"; readiness answers "should this pod receive
        traffic". A node that hasn't finished its first election has no
        useful answer for anyone: it doesn't know who the leader is, so it
        can neither serve writes nor vouch for its reads. Once it settles
        into a role - follower or leader - it's ready.

        Deliberately NOT checking commit_index freshness: that would need
        the cluster's true commit_index to compare against, which is a
        meaningfully harder problem and isn't what readiness is for here.
        """
        from raft.types import RaftState

        if self.raft_node is None:
            return web.Response(status=503, text="not ready: no node reference")

        state = getattr(self.raft_node, "state", None)
        if state in (RaftState.FOLLOWER, RaftState.LEADER):
            return web.Response(status=200, text=f"ready: {state.value}")

        state_name = state.value if isinstance(state, RaftState) else "unknown"
        return web.Response(status=503, text=f"not ready: {state_name}")


# Global metrics server instance
_metrics_server: Optional[MetricsServer] = None


async def start_metrics_server(port: int = 8000, raft_node=None) -> None:
    """Start the global metrics server.

    raft_node is optional and only used to answer /ready; passing it is what
    makes the readiness probe meaningful rather than always-503.
    """
    global _metrics_server
    _metrics_server = MetricsServer(port, raft_node=raft_node)
    await _metrics_server.start()


async def stop_metrics_server() -> None:
    """Stop the global metrics server."""
    global _metrics_server
    if _metrics_server:
        await _metrics_server.stop()
        _metrics_server = None


def get_metrics_server() -> Optional[MetricsServer]:
    """Get the global metrics server instance."""
    return _metrics_server
