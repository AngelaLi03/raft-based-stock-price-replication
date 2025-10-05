"""
Main Raft node implementation.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any

from .types import RaftState, PeerInfo, LogEntry
from .storage import RaftStorage
from .election import ElectionManager

logger = logging.getLogger(__name__)


class RaftNode:
    """Main Raft node implementation."""
    
    def __init__(self, node_id: str, peers: List[PeerInfo], data_dir: str = "./data"):
        """
        Initialize Raft node.
        
        Args:
            node_id: Unique node identifier
            peers: List of peer nodes
            data_dir: Directory for persistent storage
        """
        self.node_id = node_id
        self.peers = peers
        self.data_dir = data_dir
        
        # Initialize storage
        self.storage = RaftStorage(data_dir, node_id)
        
        # Initialize election manager
        self.election_manager = ElectionManager(
            node_id=node_id,
            peers=peers,
            request_vote_callback=self._request_vote_from_peer,
            become_leader_callback=self._on_become_leader,
            become_follower_callback=self._on_become_follower
        )
        
        # Current state
        self.state = RaftState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        
        # gRPC server references (set by server)
        self.raft_server = None
        self.client_server = None
        
        logger.info(f"Raft node {node_id} initialized with {len(peers)} peers")
    
    async def start(self) -> None:
        """Start the Raft node."""
        logger.info(f"Starting Raft node {self.node_id}")
        
        # Load persistent state
        self.election_manager.current_term = self.storage.get_current_term()
        self.election_manager.voted_for = self.storage.get_voted_for()
        
        # Start as follower with election timeout
        self.election_manager.start_election_timeout()
        
        logger.info(f"Raft node {self.node_id} started as {self.state.value}")
    
    async def stop(self) -> None:
        """Stop the Raft node."""
        logger.info(f"Stopping Raft node {self.node_id}")
        
        # Stop election timeout and heartbeats
        self.election_manager.stop_election_timeout()
        self.election_manager.stop_heartbeat()
        
        logger.info(f"Raft node {self.node_id} stopped")
    
    async def _request_vote_from_peer(self, peer_id: str, peer: PeerInfo, 
                                    term: int, last_log_index: int, last_log_term: int) -> Any:
        """
        Request vote from a peer via gRPC.
        This is called by the election manager.
        """
        if not self.raft_server:
            logger.warning("Raft server not available for vote request")
            return None
        
        try:
            # Import here to avoid circular imports
            from raft.proto import raft_pb2, raft_pb2_grpc
            
            # Create gRPC channel and stub
            import grpc
            channel = grpc.aio.insecure_channel(peer.raft_address)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            # Create request
            request = raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
            
            # Send request
            response = await stub.RequestVote(request)
            
            # Close channel
            await channel.close()
            
            return response
            
        except Exception as e:
            logger.warning(f"Failed to request vote from {peer_id}: {e}")
            return None
    
    async def _append_entries_to_peer(self, peer_id: str, peer: PeerInfo,
                                    term: int, prev_log_index: int, prev_log_term: int,
                                    entries: List[LogEntry], leader_commit: int) -> Any:
        """
        Send append entries to a peer via gRPC.
        This is called by the election manager for heartbeats.
        """
        if not self.raft_server:
            logger.warning("Raft server not available for append entries")
            return None
        
        try:
            # Import here to avoid circular imports
            from raft.proto import raft_pb2, raft_pb2_grpc
            
            # Create gRPC channel and stub
            import grpc
            channel = grpc.aio.insecure_channel(peer.raft_address)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            # Convert log entries to protobuf
            pb_entries = []
            for entry in entries:
                pb_entry = raft_pb2.LogEntry(
                    index=entry.index,
                    term=entry.term,
                    command_bytes=entry.command_bytes
                )
                pb_entries.append(pb_entry)
            
            # Create request
            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=pb_entries,
                leader_commit=leader_commit
            )
            
            # Send request
            response = await stub.AppendEntries(request)
            
            # Close channel
            await channel.close()
            
            return response
            
        except Exception as e:
            logger.warning(f"Failed to send append entries to {peer_id}: {e}")
            return None
    
    async def _on_become_leader(self) -> None:
        """Called when this node becomes leader."""
        self.state = RaftState.LEADER
        logger.info(f"Node {self.node_id} became leader for term {self.election_manager.current_term}")
    
    async def _on_become_follower(self) -> None:
        """Called when this node becomes follower."""
        self.state = RaftState.FOLLOWER
        logger.info(f"Node {self.node_id} became follower")
    
    # gRPC service methods (called by server)
    
    async def handle_request_vote(self, request) -> Any:
        """Handle incoming RequestVote RPC."""
        from raft.proto import raft_pb2
        
        logger.debug(f"Handling vote request from {request.candidate_id}")
        
        # Delegate to election manager
        vote_granted = self.election_manager.handle_vote_request(
            request.term,
            request.candidate_id,
            request.last_log_index,
            request.last_log_term
        )
        
        # Update storage
        self.storage.set_current_term(self.election_manager.current_term)
        self.storage.set_voted_for(self.election_manager.voted_for)
        
        # Create response
        response = raft_pb2.RequestVoteResponse(
            term=self.election_manager.current_term,
            vote_granted=vote_granted
        )
        
        logger.debug(f"Vote response: granted={vote_granted}")
        return response
    
    async def handle_append_entries(self, request) -> Any:
        """Handle incoming AppendEntries RPC."""
        from raft.proto import raft_pb2
        
        logger.debug(f"Handling append entries from {request.leader_id}")
        
        # Convert protobuf entries to LogEntry objects
        entries = []
        for pb_entry in request.entries:
            entry = LogEntry(
                index=pb_entry.index,
                term=pb_entry.term,
                command_bytes=pb_entry.command_bytes
            )
            entries.append(entry)
        
        # Delegate to election manager
        success = self.election_manager.handle_append_entries(
            request.term,
            request.leader_id,
            request.prev_log_index,
            request.prev_log_term,
            entries,
            request.leader_commit
        )
        
        # Update storage
        self.storage.set_current_term(self.election_manager.current_term)
        
        # TODO: Handle log entries and commit index
        
        # Create response
        response = raft_pb2.AppendEntriesResponse(
            term=self.election_manager.current_term,
            success=success,
            match_index=len(self.storage.get_log_entries())  # TODO: Proper match index
        )
        
        logger.debug(f"Append entries response: success={success}")
        return response
    
    # Client API methods
    
    async def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information for client API."""
        return {
            "leader_id": self.node_id if self.state == RaftState.LEADER else None,
            "term": self.election_manager.current_term,
            "members": [peer.node_id for peer in self.peers] + [self.node_id],
            "node_id": self.node_id,
            "role": self.state.value
        }
    
    async def put_price(self, symbol: str, price: float, timestamp: int) -> Dict[str, Any]:
        """
        Handle PutPrice request.
        For Week 1, just return NOT_LEADER unless we're the leader.
        """
        if self.state != RaftState.LEADER:
            # Find current leader (for now, just return None)
            leader_hint = None
            for peer in self.peers:
                # TODO: Implement proper leader discovery
                pass
            
            return {
                "ok": False,
                "leader_hint": leader_hint,
                "error_message": "Not leader"
            }
        
        # TODO: In Week 2, implement actual log replication
        logger.info(f"Leader received PutPrice: {symbol}={price}")
        
        return {
            "ok": True,
            "leader_hint": self.node_id,
            "error_message": None
        }
    
    async def get_price(self, symbol: str) -> Dict[str, Any]:
        """
        Handle GetPrice request.
        For Week 1, just return not found.
        """
        # TODO: In Week 2, implement actual KV store lookup
        return {
            "ticker_price": None,
            "found": False,
            "error_message": "Price not found"
        }
    
    def get_state_info(self) -> Dict[str, Any]:
        """Get detailed state information for debugging."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_term": self.election_manager.current_term,
            "voted_for": self.election_manager.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "log_length": len(self.storage.get_log_entries()),
            "peers": [peer.node_id for peer in self.peers]
        }
