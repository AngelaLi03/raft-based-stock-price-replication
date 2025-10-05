"""
Raft types, enums, and constants.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import time


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

# Default ports
DEFAULT_RAFT_PORT = 50051
DEFAULT_CLIENT_PORT = 50061

# Logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
