"""
HTTP server for Prometheus metrics endpoint.
"""

import asyncio
import logging
from aiohttp import web, web_request, web_response
from typing import Optional

logger = logging.getLogger(__name__)


class MetricsServer:
    """HTTP server for serving Prometheus metrics."""
    
    def __init__(self, port: int = 8000):
        self.port = port
        self.app = web.Application()
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.app.router.add_get('/health', self.handle_health)
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


# Global metrics server instance
_metrics_server: Optional[MetricsServer] = None


async def start_metrics_server(port: int = 8000) -> None:
    """Start the global metrics server."""
    global _metrics_server
    _metrics_server = MetricsServer(port)
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
