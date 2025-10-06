"""
Main Raft node implementation.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any

from .types import RaftState, PeerInfo, LogEntry
from .storage import RaftStorage
from .election import ElectionManager
from kv.state_machine import KVStateMachine, serialize_put_command, serialize_batch_put_command

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
        
        # Initialize KV state machine
        self.kv_state_machine = KVStateMachine(data_dir)
        
        # Initialize metrics
        try:
            from raft.metrics import init_metrics
            init_metrics(node_id)
        except ImportError:
            pass  # Metrics not available
        
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
        
        # Leader state for replication
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # gRPC server references (set by server)
        self.raft_server = None
        self.client_server = None
        
        logger.info(f"Raft node {node_id} initialized with {len(peers)} peers")
    
    async def start(self) -> None:
        """Start the Raft node with crash recovery."""
        logger.info(f"Starting Raft node {self.node_id}")
        
        # Load persistent state
        self.election_manager.current_term = self.storage.get_current_term()
        self.election_manager.voted_for = self.storage.get_voted_for()
        self.commit_index = self.storage.get_commit_index()
        self.last_applied = self.storage.get_last_applied()
        
        # Ensure commit index doesn't exceed log length (in case of log truncation)
        log_length = len(self.storage.get_log_entries())
        if self.commit_index > log_length:
            logger.warning(f"Commit index {self.commit_index} exceeds log length {log_length}, adjusting")
            self.commit_index = log_length
            self.storage.set_commit_index(self.commit_index)
        
        # Ensure last_applied doesn't exceed commit_index
        if self.last_applied > self.commit_index:
            logger.warning(f"Last applied {self.last_applied} exceeds commit index {self.commit_index}, adjusting")
            self.last_applied = self.commit_index
            self.storage.set_last_applied(self.last_applied)
        
        logger.info(f"Recovered state: term={self.election_manager.current_term}, commit_index={self.commit_index}, last_applied={self.last_applied}")
        
        # Start KV state machine
        await self.kv_state_machine.start()
        
        # Synchronize KV state machine's last_applied_index with Raft node's last_applied
        self.kv_state_machine.last_applied_index = self.last_applied
        
        # Replay any unapplied committed entries for crash recovery
        await self._recover_from_crash()
        
        # Start as follower with election timeout
        self.election_manager.start_election_timeout()
        
        logger.info(f"Raft node {self.node_id} started as {self.state.value} (recovery complete)")
    
    async def stop(self) -> None:
        """Stop the Raft node."""
        logger.info(f"Stopping Raft node {self.node_id}")
        
        # Stop election timeout and heartbeats
        self.election_manager.stop_election_timeout()
        self.election_manager.stop_heartbeat()
        
        # Stop KV state machine
        await self.kv_state_machine.stop()
        
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
        
        # Initialize leader state
        next_log_index = self.storage.get_last_log_index() + 1
        for peer in self.peers:
            self.next_index[peer.node_id] = next_log_index
            self.match_index[peer.node_id] = 0
        
        # Start heartbeat task
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        logger.info(f"Node {self.node_id} became leader for term {self.election_manager.current_term}")
    
    async def _on_become_follower(self) -> None:
        """Called when this node becomes follower."""
        self.state = RaftState.FOLLOWER
        
        # Stop heartbeat task
        if hasattr(self, 'heartbeat_task') and self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Node {self.node_id} became follower")
    
    async def _recover_from_crash(self) -> None:
        """Recover from crash by replaying unapplied committed entries."""
        logger.info(f"Starting crash recovery: last_applied={self.last_applied}, commit_index={self.commit_index}")
        
        # Start timing recovery
        import time
        start_time = time.time()
        
        if self.last_applied < self.commit_index:
            # Get all unapplied committed entries
            unapplied_entries = []
            for i in range(self.last_applied + 1, self.commit_index + 1):
                entry = self.storage.get_log_entry(i)
                if entry:
                    unapplied_entries.append(entry)
            
            if unapplied_entries:
                logger.info(f"Replaying {len(unapplied_entries)} unapplied entries for crash recovery")
                await self.kv_state_machine.replay_log_entries(unapplied_entries, self.last_applied + 1)
                
                # Update last_applied to match commit_index
                self.last_applied = self.commit_index
                self.storage.set_last_applied(self.last_applied)
                
                # Record metrics
                duration_ms = (time.time() - start_time) * 1000
                try:
                    from raft.metrics import record_crash_recovery
                    record_crash_recovery(len(unapplied_entries), duration_ms)
                except ImportError:
                    pass
                
                logger.info(f"Crash recovery complete: last_applied={self.last_applied}")
        else:
            logger.info("No unapplied entries found, recovery not needed")
    
    async def _apply_committed_entries(self) -> None:
        """Apply all committed entries that haven't been applied yet."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.storage.get_log_entry(self.last_applied)
            if entry:
                await self.kv_state_machine.apply_command(entry)
                logger.debug(f"Applied entry {self.last_applied} to state machine")
            
            # Persist last_applied after each entry
            self.storage.set_last_applied(self.last_applied)
    
    async def _replicate_to_peers(self, entries: List[LogEntry]) -> bool:
        """
        Replicate entries to all peers and wait for majority acknowledgment.
        
        Args:
            entries: List of log entries to replicate
            
        Returns:
            True if replicated to majority, False otherwise
        """
        if not entries:
            return True
        
        # Send AppendEntries to all peers
        replication_tasks = []
        for peer in self.peers:
            task = asyncio.create_task(self._send_append_entries_to_peer(peer, entries))
            replication_tasks.append(task)
        
        # Wait for all replication attempts
        results = await asyncio.gather(*replication_tasks, return_exceptions=True)
        
        # Count successful replications
        successful_replications = 1  # Count self
        for result in results:
            if isinstance(result, dict) and result.get("success", False):
                successful_replications += 1
        
        # Check if we have majority
        majority = (len(self.peers) + 1) // 2 + 1  # +1 for self
        return successful_replications >= majority
    
    async def _send_append_entries_to_peer(self, peer: PeerInfo, entries: List[LogEntry]) -> Dict[str, Any]:
        """
        Send AppendEntries to a specific peer.
        
        Args:
            peer: Peer to send to
            entries: Log entries to send
            
        Returns:
            Result dictionary with success status
        """
        try:
            # Get previous log entry info
            prev_log_index = self.next_index[peer.node_id] - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_entry = self.storage.get_log_entry(prev_log_index)
                if prev_entry:
                    prev_log_term = prev_entry.term
            
            # Send AppendEntries RPC
            response = await self._append_entries_to_peer(
                peer.node_id, peer, 
                self.election_manager.current_term,
                prev_log_index, prev_log_term,
                entries, self.commit_index
            )
            
            if response and response.success:
                # Update next_index and match_index
                self.next_index[peer.node_id] = len(entries) + self.next_index[peer.node_id]
                self.match_index[peer.node_id] = self.next_index[peer.node_id] - 1
                return {"success": True, "match_index": response.match_index}
            else:
                # Decrement next_index for retry
                if self.next_index[peer.node_id] > 1:
                    self.next_index[peer.node_id] -= 1
                return {"success": False}
                
        except Exception as e:
            logger.warning(f"Failed to replicate to {peer.node_id}: {e}")
            return {"success": False}
    
    async def _update_commit_index(self) -> None:
        """Update commit index based on match_index from followers."""
        if self.state != RaftState.LEADER:
            return
        
        # Find the highest index that's replicated on majority
        match_indices = [self.storage.get_last_log_index()]  # Include self
        for peer in self.peers:
            match_indices.append(self.match_index[peer.node_id])
        
        match_indices.sort(reverse=True)
        majority_index = match_indices[(len(self.peers) + 1) // 2]
        
        # Only commit entries from current term
        if majority_index > self.commit_index:
            entry = self.storage.get_log_entry(majority_index)
            if entry and entry.term == self.election_manager.current_term:
                self.commit_index = majority_index
                self.storage.set_commit_index(self.commit_index)
                logger.info(f"Updated commit index to {self.commit_index}")
                
                # Apply newly committed entries
                await self._apply_committed_entries()
    
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
        
        # Check term
        if request.term < self.election_manager.current_term:
            response = raft_pb2.AppendEntriesResponse(
                term=self.election_manager.current_term,
                success=False,
                match_index=0
            )
            return response
        
        # Update term and become follower if needed
        if request.term > self.election_manager.current_term:
            self.election_manager.current_term = request.term
            self.election_manager.voted_for = None
            if self.state != RaftState.FOLLOWER:
                await self._on_become_follower()
        
        # Reset election timeout
        self.election_manager.start_election_timeout()
        
        # Check log matching property
        if not self._check_log_matching(request.prev_log_index, request.prev_log_term):
            response = raft_pb2.AppendEntriesResponse(
                term=self.election_manager.current_term,
                success=False,
                match_index=0
            )
            return response
        
        # Convert protobuf entries to LogEntry objects
        entries = []
        for pb_entry in request.entries:
            entry = LogEntry(
                index=pb_entry.index,
                term=pb_entry.term,
                command_bytes=pb_entry.command_bytes
            )
            entries.append(entry)
        
        # Apply log entries
        if entries:
            # Truncate log if necessary (log matching property)
            if request.prev_log_index < len(self.storage.get_log_entries()):
                logger.info(f"Truncating log from index {request.prev_log_index + 1} for catch-up")
                self.storage.truncate_log_from(request.prev_log_index + 1)
            
            # Append new entries
            self.storage.append_entries(entries)
            logger.info(f"Appended {len(entries)} entries to log (catch-up)")
        
        # Update commit index
        if request.leader_commit > self.commit_index:
            old_commit_index = self.commit_index
            self.commit_index = min(request.leader_commit, len(self.storage.get_log_entries()))
            self.storage.set_commit_index(self.commit_index)
            
            if self.commit_index > old_commit_index:
                logger.info(f"Updated commit index from {old_commit_index} to {self.commit_index}")
            
            # Apply newly committed entries
            await self._apply_committed_entries()
        
        # Update storage
        self.storage.set_current_term(self.election_manager.current_term)
        
        # Create response
        current_log_length = len(self.storage.get_log_entries())
        response = raft_pb2.AppendEntriesResponse(
            term=self.election_manager.current_term,
            success=True,
            match_index=current_log_length
        )
        
        logger.debug(f"Append entries response: success=True, match_index={response.match_index}")
        return response
    
    def _check_log_matching(self, prev_log_index: int, prev_log_term: int) -> bool:
        """
        Check if the log matches at the given index and term.
        
        Args:
            prev_log_index: Index to check
            prev_log_term: Expected term at that index
            
        Returns:
            True if log matches, False otherwise
        """
        if prev_log_index == 0:
            return True
        
        if prev_log_index > len(self.storage.get_log_entries()):
            return False
        
        entry = self.storage.get_log_entry(prev_log_index)
        if not entry:
            return False
        
        return entry.term == prev_log_term
    
    async def _heartbeat_loop(self) -> None:
        """Send heartbeats to all followers."""
        from .types import HEARTBEAT_INTERVAL
        
        while self.state == RaftState.LEADER:
            try:
                # Send heartbeats to all peers
                heartbeat_tasks = []
                for peer in self.peers:
                    task = asyncio.create_task(self._send_heartbeat_to_peer(peer))
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
    
    async def _send_heartbeat_to_peer(self, peer: PeerInfo) -> None:
        """Send heartbeat and catch up missing entries to a peer."""
        try:
            # Get previous log entry info
            prev_log_index = self.next_index[peer.node_id] - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_entry = self.storage.get_log_entry(prev_log_index)
                if prev_entry:
                    prev_log_term = prev_entry.term
            
            # Get entries to send (for catch-up)
            entries_to_send = []
            current_log_length = len(self.storage.get_log_entries())
            if self.next_index[peer.node_id] <= current_log_length:
                # Send missing entries for catch-up
                for i in range(self.next_index[peer.node_id], current_log_length + 1):
                    entry = self.storage.get_log_entry(i)
                    if entry:
                        entries_to_send.append(entry)
            
            # Send AppendEntries (with entries if catch-up needed, empty if heartbeat)
            response = await self._append_entries_to_peer(
                peer.node_id, peer,
                self.election_manager.current_term,
                prev_log_index, prev_log_term,
                entries_to_send,  # Send missing entries for catch-up
                self.commit_index
            )
            
            if response and response.success:
                # Update next_index and match_index
                if entries_to_send:
                    self.next_index[peer.node_id] = len(entries_to_send) + self.next_index[peer.node_id]
                    self.match_index[peer.node_id] = self.next_index[peer.node_id] - 1
                    logger.info(f"Sent {len(entries_to_send)} entries to {peer.node_id} for catch-up")
                else:
                    self.match_index[peer.node_id] = prev_log_index
                    logger.debug(f"Sent heartbeat to {peer.node_id}")
            else:
                # Decrement next_index for retry
                if self.next_index[peer.node_id] > 1:
                    self.next_index[peer.node_id] -= 1
                logger.debug(f"AppendEntries failed to {peer.node_id}, retrying with lower index")
                
        except Exception as e:
            logger.warning(f"Failed to send heartbeat to {peer.node_id}: {e}")
    
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
        Only leader accepts writes and replicates them.
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
        
        try:
            # Serialize command
            command_bytes = serialize_put_command(symbol, price, timestamp)
            
            # Create log entry
            log_index = self.storage.get_last_log_index() + 1
            entry = LogEntry(
                index=log_index,
                term=self.election_manager.current_term,
                command_bytes=command_bytes
            )
            
            # Append to local log
            self.storage.append_entries([entry])
            logger.info(f"Leader appended PutPrice: {symbol}={price} at index {log_index}")
            
            # Replicate to followers
            success = await self._replicate_to_peers([entry])
            
            if success:
                # Update commit index and apply
                await self._update_commit_index()
                
                return {
                    "ok": True,
                    "leader_hint": self.node_id,
                    "error_message": None
                }
            else:
                # Replication failed - remove the entry from log
                logger.warning(f"Replication failed for entry {log_index}, removing from log")
                self.storage.truncate_log(log_index - 1)
                
                return {
                    "ok": False,
                    "leader_hint": self.node_id,
                    "error_message": "Failed to replicate to majority"
                }
                
        except Exception as e:
            logger.error(f"Error in PutPrice: {e}")
            return {
                "ok": False,
                "leader_hint": self.node_id,
                "error_message": str(e)
            }
    
    async def get_price(self, symbol: str) -> Dict[str, Any]:
        """
        Handle GetPrice request.
        Read from committed state.
        """
        try:
            ticker_price = self.kv_state_machine.get(symbol)
            
            if ticker_price:
                return {
                    "ticker_price": {
                        "symbol": ticker_price.symbol,
                        "price": ticker_price.price,
                        "timestamp": ticker_price.timestamp
                    },
                    "found": True,
                    "error_message": None
                }
            else:
                return {
                    "ticker_price": None,
                    "found": False,
                    "error_message": "Price not found"
                }
                
        except Exception as e:
            logger.error(f"Error in GetPrice: {e}")
            return {
                "ticker_price": None,
                "found": False,
                "error_message": str(e)
            }
    
    async def dump_state(self) -> Dict[str, Any]:
        """Dump local node state for debugging and monitoring."""
        try:
            # Get KV store contents
            kv_store = self.kv_state_machine.dump_state()
            
            # Get metrics if available
            metrics = None
            try:
                from raft.metrics import get_metrics
                metrics_collector = get_metrics()
                if metrics_collector:
                    metrics = metrics_collector.get_metrics()
            except ImportError:
                pass  # Metrics not available
            
            return {
                "ok": True,
                "node_id": self.node_id,
                "current_term": self.election_manager.current_term,
                "state": self.state.value,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": len(self.storage.get_log_entries()),
                "kv_entries": len(kv_store.get("entries", {})),
                "kv_store": kv_store.get("entries", {}),
                "metrics": metrics
            }
            
        except Exception as e:
            logger.error(f"Error dumping state: {e}")
            return {
                "ok": False,
                "error_message": str(e)
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
