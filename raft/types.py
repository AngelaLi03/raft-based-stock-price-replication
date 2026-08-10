"""
Raft types, enums, and constants.
"""

import logging
import os
import re
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any

logger = logging.getLogger(__name__)


class RaftState(Enum):
    """Raft node states."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A single log entry in the Raft log."""
    index: int
    term: int
    command_bytes: bytes
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "index": self.index,
            "term": self.term,
            "command_bytes": self.command_bytes.hex()  # Convert bytes to hex string
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        """Create from dictionary."""
        return cls(
            index=data["index"],
            term=data["term"],
            command_bytes=bytes.fromhex(data["command_bytes"])
        )


@dataclass
class PeerInfo:
    """Information about a peer node."""
    node_id: str
    host: str
    port: int
    raft_port: int
    client_port: int
    
    @property
    def raft_address(self) -> str:
        """Get the Raft gRPC address."""
        return f"{self.host}:{self.raft_port}"
    
    @property
    def client_address(self) -> str:
        """Get the Client gRPC address."""
        return f"{self.host}:{self.client_port}"


# Raft constants
ELECTION_TIMEOUT_MIN = 150  # milliseconds
ELECTION_TIMEOUT_MAX = 300  # milliseconds
HEARTBEAT_INTERVAL = 75     # milliseconds

# Deadline for a single outbound peer RPC (RequestVote/AppendEntries/
# InstallSnapshot). Without this, a peer that accepts the TCP connection but
# never responds (e.g. mid-restart) hangs the call forever - and since
# _start_election/_heartbeat_loop gather() every peer's RPC and wait for all
# of them, one stuck peer freezes that node's entire election or heartbeat
# loop, not just replication to that one peer.
RPC_TIMEOUT_SECONDS = 2.0

# Batching configuration
DEFAULT_BATCH_SIZE = 10     # entries per batch
DEFAULT_FLUSH_INTERVAL = 50 # milliseconds
MAX_BATCH_SIZE = 100        # maximum entries per batch

# Snapshotting: take a snapshot (and compact the log) every N newly-applied
# committed entries.
DEFAULT_SNAPSHOT_THRESHOLD = 50

# Default ports
DEFAULT_RAFT_PORT = 50051
DEFAULT_CLIENT_PORT = 50061

# Logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def metrics_port_for_node_id(node_id: str, base: int = 8000) -> int:
    """Derive this node's metrics port from its ID.

    Handles both naming schemes this project uses: Docker Compose's
    "node1"/"node15" and Kubernetes StatefulSet's "raft-node-0". Takes the
    trailing integer in the ID, so "node15" -> 15 and "raft-node-0" -> 0.

    Never raises: an ID with no trailing digits falls back to offset 1.
    The previous implementation, `int(node_id.replace('node', ''))`, raised
    ValueError on StatefulSet-style IDs - which crashed RaftNode outright
    and silently disabled the metrics server (and with it the readiness
    probe) in GrpcServer.
    """
    match = re.search(r'(\d+)$', node_id)
    return base + (int(match.group(1)) if match else 1)


def resolve_metrics_port(node_id: str) -> int:
    """Resolve this node's metrics port.

    An explicit METRICS_PORT env var wins - this is what the Kubernetes
    manifests set, since every pod shares one pod template and therefore
    must bind the same port. Falls back to the per-node port derived from
    node_id otherwise - the Docker Compose scheme, where each node already
    has its own unique port and no such env var is set.

    Never raises: a malformed METRICS_PORT (non-integer) logs a warning and
    falls through to the per-node computation instead of propagating
    ValueError, matching the "never raises" guarantee metrics_port_for_node_id
    already documents. Without this, a bad env var would crash RaftNode.__init__
    outright (the call site in raft/node.py only catches ImportError) -
    the same "loud failure from an unexpected place" shape as the node-ID
    parsing bug, just for a different malformed input.
    """
    override = os.environ.get("METRICS_PORT")
    if override:
        try:
            return int(override)
        except ValueError:
            logger.warning(
                "Ignoring malformed METRICS_PORT=%r; falling back to "
                "per-node computation for node_id=%r", override, node_id
            )
    return metrics_port_for_node_id(node_id)
