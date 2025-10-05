"""
Raft election logic and timeout handling.
"""

import asyncio
import random
import logging
from typing import Dict, List, Optional, Callable

from .types import RaftState, PeerInfo, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL

logger = logging.getLogger(__name__)


class ElectionManager:
    """Manages Raft election timeouts and candidate behavior."""
    
    def __init__(self, node_id: str, peers: List[PeerInfo], 
                 request_vote_callback: Callable,
                 become_leader_callback: Callable,
                 become_follower_callback: Callable):
        """
        Initialize election manager.
        
        Args:
            node_id: This node's ID
            peers: List of peer nodes
            request_vote_callback: Function to call when requesting votes
            become_leader_callback: Function to call when becoming leader
            become_follower_callback: Function to call when becoming follower
        """
        self.node_id = node_id
        self.peers = {peer.node_id: peer for peer in peers}
        self.request_vote_callback = request_vote_callback
        self.become_leader_callback = become_leader_callback
        self.become_follower_callback = become_follower_callback
        
        # Election state
        self.election_timeout_task: Optional[asyncio.Task] = None
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.votes_received: Dict[str, bool] = {}
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.state = RaftState.FOLLOWER
        
        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        logger.info(f"Election manager initialized for node {node_id}")
    
    def start_election_timeout(self) -> None:
        """Start or restart the election timeout."""
        if self.election_timeout_task:
            self.election_timeout_task.cancel()
        
        # Randomize election timeout
        timeout_ms = random.randint(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        logger.debug(f"Starting election timeout: {timeout_ms}ms")
        
        self.election_timeout_task = asyncio.create_task(
            self._election_timeout(timeout_ms / 1000.0)
        )
    
    def stop_election_timeout(self) -> None:
        """Stop the election timeout."""
        if self.election_timeout_task:
            self.election_timeout_task.cancel()
            self.election_timeout_task = None
    
    def start_heartbeat(self) -> None:
        """Start sending heartbeats (leader only)."""
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        logger.debug("Starting heartbeat")
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    def stop_heartbeat(self) -> None:
        """Stop sending heartbeats."""
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None
    
    async def _election_timeout(self, timeout_seconds: float) -> None:
        """Handle election timeout - become candidate and start election."""
        try:
            await asyncio.sleep(timeout_seconds)
            
            if self.state == RaftState.FOLLOWER:
                logger.info("Election timeout - becoming candidate")
                await self._start_election()
                
        except asyncio.CancelledError:
            logger.debug("Election timeout cancelled")
    
    async def _start_election(self) -> None:
        """Start a new election."""
        # Increment term and become candidate
        self.current_term += 1
        self.voted_for = self.node_id
        self.state = RaftState.CANDIDATE
        self.votes_received = {self.node_id: True}  # Vote for self
        
        logger.info(f"Starting election for term {self.current_term}")
        
        # Request votes from all peers
        vote_tasks = []
        for peer_id, peer in self.peers.items():
            task = asyncio.create_task(self._request_vote(peer_id, peer))
            vote_tasks.append(task)
        
        # Wait for all vote requests to complete
        if vote_tasks:
            await asyncio.gather(*vote_tasks, return_exceptions=True)
        
        # Check if we won the election
        total_votes = len(self.votes_received)
        votes_for_me = sum(1 for voted in self.votes_received.values() if voted)
        majority = (len(self.peers) + 1) // 2 + 1  # +1 for self
        
        logger.info(f"Election results: {votes_for_me}/{total_votes} votes, need {majority}")
        
        if votes_for_me >= majority:
            logger.info("Won election - becoming leader")
            await self._become_leader()
        else:
            logger.info("Lost election - becoming follower")
            await self._become_follower()
    
    async def _request_vote(self, peer_id: str, peer: PeerInfo) -> None:
        """Request vote from a peer."""
        try:
            # Get last log index and term (for now, use dummy values)
            last_log_index = 0  # TODO: Get from storage
            last_log_term = 0   # TODO: Get from storage
            
            logger.debug(f"Requesting vote from {peer_id}")
            
            # Call the request vote callback (implemented by RaftNode)
            response = await self.request_vote_callback(
                peer_id, peer, self.current_term, last_log_index, last_log_term
            )
            
            if response and response.vote_granted:
                self.votes_received[peer_id] = True
                logger.debug(f"Received vote from {peer_id}")
            else:
                self.votes_received[peer_id] = False
                logger.debug(f"Vote denied by {peer_id}")
                
        except Exception as e:
            logger.warning(f"Failed to request vote from {peer_id}: {e}")
            self.votes_received[peer_id] = False
    
    async def _become_leader(self) -> None:
        """Become leader and start heartbeats."""
        self.state = RaftState.LEADER
        self.stop_election_timeout()
        
        # Initialize leader state
        next_log_index = 1  # TODO: Get from storage
        for peer_id in self.peers:
            self.next_index[peer_id] = next_log_index
            self.match_index[peer_id] = 0
        
        # Start sending heartbeats
        self.start_heartbeat()
        
        # Notify the main node
        await self.become_leader_callback()
    
    async def _become_follower(self) -> None:
        """Become follower."""
        self.state = RaftState.FOLLOWER
        self.voted_for = None
        self.stop_heartbeat()
        
        # Restart election timeout
        self.start_election_timeout()
        
        # Notify the main node
        await self.become_follower_callback()
    
    async def _heartbeat_loop(self) -> None:
        """Send heartbeats to all followers."""
        while self.state == RaftState.LEADER:
            try:
                # Send heartbeats to all peers
                heartbeat_tasks = []
                for peer_id, peer in self.peers.items():
                    task = asyncio.create_task(self._send_heartbeat(peer_id, peer))
                    heartbeat_tasks.append(task)
                
                if heartbeat_tasks:
                    await asyncio.gather(*heartbeat_tasks, return_exceptions=True)
                
                # Wait for next heartbeat
                await asyncio.sleep(HEARTBEAT_INTERVAL / 1000.0)
                
            except asyncio.CancelledError:
                logger.debug("Heartbeat loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL / 1000.0)
    
    async def _send_heartbeat(self, peer_id: str, peer: PeerInfo) -> None:
        """Send heartbeat to a peer."""
        try:
            logger.debug(f"Sending heartbeat to {peer_id}")
            
            # This will be handled by the RaftNode's heartbeat mechanism
            # The actual AppendEntries call is made by the RaftNode
            
        except Exception as e:
            logger.warning(f"Failed to send heartbeat to {peer_id}: {e}")
    
    def handle_vote_request(self, term: int, candidate_id: str, 
                          last_log_index: int, last_log_term: int) -> bool:
        """
        Handle incoming vote request.
        
        Returns:
            True if vote is granted, False otherwise
        """
        logger.debug(f"Received vote request from {candidate_id} for term {term}")
        
        # If term is higher, update our term and become follower
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            if self.state != RaftState.FOLLOWER:
                asyncio.create_task(self._become_follower())
        
        # Grant vote if:
        # 1. We haven't voted for anyone in this term, AND
        # 2. Candidate's log is at least as up-to-date as ours
        if (self.voted_for is None or self.voted_for == candidate_id) and term >= self.current_term:
            # TODO: Check log up-to-date condition
            self.voted_for = candidate_id
            logger.info(f"Granted vote to {candidate_id} for term {term}")
            return True
        
        logger.debug(f"Denied vote to {candidate_id}")
        return False
    
    def handle_append_entries(self, term: int, leader_id: str, 
                            prev_log_index: int, prev_log_term: int,
                            entries: List, leader_commit: int) -> bool:
        """
        Handle incoming append entries (heartbeat or log replication).
        
        Returns:
            True if entries are accepted, False otherwise
        """
        logger.debug(f"Received append entries from {leader_id} for term {term}")
        
        # If term is higher, update our term and become follower
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            if self.state != RaftState.FOLLOWER:
                asyncio.create_task(self._become_follower())
        
        # If we're not follower, become follower
        if self.state != RaftState.FOLLOWER:
            asyncio.create_task(self._become_follower())
        
        # Reset election timeout since we heard from leader
        self.start_election_timeout()
        
        # TODO: Implement proper log matching logic
        # For now, always accept heartbeats (empty entries)
        if not entries:
            logger.debug("Accepted heartbeat")
            return True
        
        # TODO: Implement log entry validation
        logger.debug("Accepted log entries")
        return True
    
    def get_state_info(self) -> dict:
        """Get current state information for debugging."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "votes_received": self.votes_received,
            "next_index": self.next_index,
            "match_index": self.match_index
        }
