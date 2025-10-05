"""
Main server entry point.
"""

import asyncio
import signal
import logging
from typing import Optional

from raft.node import RaftNode
from server.grpc_server import GrpcServer
from server.cluster_boot import get_cluster_config, setup_logging

logger = logging.getLogger(__name__)


class RaftServer:
    """Main server that coordinates Raft node and gRPC services."""
    
    def __init__(self):
        """Initialize server."""
        self.raft_node: Optional[RaftNode] = None
        self.grpc_server: Optional[GrpcServer] = None
        self.running = False
    
    async def start(self) -> None:
        """Start the server."""
        logger.info("Starting Raft server...")
        
        # Get cluster configuration
        config = get_cluster_config()
        
        # Create Raft node
        self.raft_node = RaftNode(
            node_id=config["node_id"],
            peers=config["peers"],
            data_dir=config["data_dir"]
        )
        
        # Create gRPC server
        self.grpc_server = GrpcServer(
            raft_node=self.raft_node,
            raft_port=config["raft_port"],
            client_port=config["client_port"]
        )
        
        # Start services
        await self.grpc_server.start()
        await self.raft_node.start()
        
        self.running = True
        logger.info(f"Raft server started successfully (node_id={config['node_id']})")
    
    async def stop(self) -> None:
        """Stop the server."""
        if not self.running:
            return
        
        logger.info("Stopping Raft server...")
        
        # Stop services
        if self.raft_node:
            await self.raft_node.stop()
        
        if self.grpc_server:
            await self.grpc_server.stop()
        
        self.running = False
        logger.info("Raft server stopped")


async def main():
    """Main entry point."""
    # Setup logging
    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level)
    
    # Create and start server
    server = RaftServer()
    
    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(server.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await server.start()
        
        # Wait for termination
        if server.grpc_server:
            await server.grpc_server.wait_for_termination()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise
    finally:
        await server.stop()


if __name__ == "__main__":
    import os
    asyncio.run(main())
