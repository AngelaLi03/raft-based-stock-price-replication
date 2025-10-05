"""
Cluster bootstrapping and configuration.
"""

import os
import logging
from typing import List, Dict, Any

from raft.types import PeerInfo

logger = logging.getLogger(__name__)


def parse_peer_list(peer_list_str: str) -> List[Dict[str, Any]]:
    """
    Parse peer list from environment variable.
    
    Expected format: "node1:localhost:50051:50061,node2:localhost:50052:50062,node3:localhost:50053:50063"
    
    Args:
        peer_list_str: Comma-separated list of peer specifications
        
    Returns:
        List of peer dictionaries
    """
    peers = []
    
    if not peer_list_str:
        return peers
    
    for peer_spec in peer_list_str.split(','):
        parts = peer_spec.strip().split(':')
        if len(parts) != 4:
            logger.warning(f"Invalid peer specification: {peer_spec}")
            continue
        
        node_id, host, raft_port, client_port = parts
        peers.append({
            "node_id": node_id,
            "host": host,
            "raft_port": int(raft_port),
            "client_port": int(client_port)
        })
    
    return peers


def get_cluster_config() -> Dict[str, Any]:
    """
    Get cluster configuration from environment variables.
    
    Returns:
        Dictionary with cluster configuration
    """
    # Get node identity
    node_id = os.getenv("NODE_ID")
    if not node_id:
        raise ValueError("NODE_ID environment variable is required")
    
    # Get peer list
    peer_list_str = os.getenv("PEER_LIST", "")
    peer_specs = parse_peer_list(peer_list_str)
    
    # Convert to PeerInfo objects, excluding self
    peers = []
    for spec in peer_specs:
        if spec["node_id"] != node_id:
            peer = PeerInfo(
                node_id=spec["node_id"],
                host=spec["host"],
                port=spec["client_port"],  # For backward compatibility
                raft_port=spec["raft_port"],
                client_port=spec["client_port"]
            )
            peers.append(peer)
    
    # Get ports for this node
    raft_port = int(os.getenv("RAFT_PORT", "50051"))
    client_port = int(os.getenv("CLIENT_PORT", "50061"))
    
    # Get data directory
    data_dir = os.getenv("DATA_DIR", "./data")
    
    config = {
        "node_id": node_id,
        "peers": peers,
        "raft_port": raft_port,
        "client_port": client_port,
        "data_dir": data_dir
    }
    
    logger.info(f"Cluster config: node_id={node_id}, peers={len(peers)}, "
               f"raft_port={raft_port}, client_port={client_port}")
    
    return config


def setup_logging(level: str = "INFO") -> None:
    """
    Setup structured logging.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set specific logger levels
    logging.getLogger("grpc").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    logger.info(f"Logging configured at {level} level")
