"""
Main Raft node implementation.
"""

import asyncio
import logging
import os
from typing import List, Optional, Dict, Any

from .types import RaftState, PeerInfo, LogEntry
from .storage import RaftStorage
from .election import ElectionManager
from kv.state_machine import KVStateMachine, TickerPrice, serialize_put_command, serialize_batch_put_command

logger = logging.getLogger(__name__)


class RaftNode:
    """Main Raft node implementation."""
    
    def __init__(self, node_id: str, peers: List[PeerInfo], data_dir: str = "./data", 
                 batch_size: int = None, flush_interval: int = None):
        """
        Initialize Raft node.
        
        Args:
            node_id: Unique node identifier
            peers: List of peer nodes
            data_dir: Directory for persistent storage
            batch_size: Number of entries per batch (default from types)
            flush_interval: Flush interval in milliseconds (default from types)
        """
        from .types import DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL, DEFAULT_SNAPSHOT_THRESHOLD

        self.node_id = node_id
        self.peers = peers
        self.data_dir = data_dir

        # Batching configuration
        self.batch_size = batch_size or int(os.environ.get('RAFT_BATCH_SIZE', DEFAULT_BATCH_SIZE))
        self.flush_interval = flush_interval or int(os.environ.get('RAFT_FLUSH_INTERVAL_MS', DEFAULT_FLUSH_INTERVAL))

        # Snapshot every N newly-applied committed entries
        self.snapshot_threshold = int(os.environ.get('RAFT_SNAPSHOT_THRESHOLD', DEFAULT_SNAPSHOT_THRESHOLD))
        
        # Initialize storage
        self.storage = RaftStorage(data_dir, node_id)
        
        # Initialize KV state machine
        self.kv_state_machine = KVStateMachine(data_dir)
        
        # Initialize metrics
        try:
            from raft.prometheus_metrics import init_prometheus_metrics
            # Use different ports for each node to avoid conflicts
            metrics_port = 8000 + int(node_id.replace('node', ''))
            init_prometheus_metrics(node_id, metrics_port)
        except ImportError:
            logger.warning("Prometheus metrics not available")
        
        # Initialize structured logging
        try:
            from raft.structured_logging import get_structured_logger, setup_structured_logging
            setup_structured_logging(node_id, use_json=True, log_level="INFO")
            self.structured_logger = get_structured_logger("raft.node", node_id)
        except ImportError:
            logger.warning("Structured logging not available")
            self.structured_logger = None
        
        # Initialize election manager
        self.election_manager = ElectionManager(
            node_id=node_id,
            peers=peers,
            request_vote_callback=self._request_vote_from_peer,
            become_leader_callback=self._on_become_leader,
            become_follower_callback=self._on_become_follower,
            get_last_log_info_callback=self._get_last_log_info
        )
        
        # Current state
        self.state = RaftState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state for replication
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Batching state
        self.pending_entries: List[LogEntry] = []
        self._pending_futures: Dict[int, asyncio.Future] = {}  # log index -> future resolved on flush
        self.batch_flush_task: Optional[asyncio.Task] = None
        self.batch_lock = asyncio.Lock()
        
        # gRPC server references (set by server)
        self.raft_server = None
        self.client_server = None
        
        logger.info(f"Raft node {node_id} initialized with {len(peers)} peers (batch_size={self.batch_size}, flush_interval={self.flush_interval}ms)")
    
    async def start(self) -> None:
        """Start the Raft node with crash recovery."""
        logger.info(f"Starting Raft node {self.node_id}")
        
        # Load persistent state
        self.election_manager.current_term = self.storage.get_current_term()
        self.election_manager.voted_for = self.storage.get_voted_for()
        self.commit_index = self.storage.get_commit_index()
        self.last_applied = self.storage.get_last_applied()

        # A snapshot's contents are, by construction, committed and applied -
        # never let commit_index/last_applied fall behind what it covers
        # (e.g. on a fresh data dir restored from a copied snapshot file).
        if self.storage.last_included_index > self.commit_index:
            self.commit_index = self.storage.last_included_index
            self.storage.set_commit_index(self.commit_index)
        if self.storage.last_included_index > self.last_applied:
            self.last_applied = self.storage.last_included_index

        # Ensure commit index doesn't exceed the log's real length (which is
        # last_included_index + len(log) once a snapshot has compacted the
        # log - NOT just len(log), which would undercount and wrongly roll
        # back commit_index on a node with an active snapshot).
        last_log_index = self.storage.get_last_log_index()
        if self.commit_index > last_log_index:
            logger.warning(f"Commit index {self.commit_index} exceeds log length {last_log_index}, adjusting")
            self.commit_index = last_log_index
            self.storage.set_commit_index(self.commit_index)

        # Ensure last_applied doesn't exceed commit_index
        if self.last_applied > self.commit_index:
            logger.warning(f"Last applied {self.last_applied} exceeds commit index {self.commit_index}, adjusting")
            self.last_applied = self.commit_index
            self.storage.set_last_applied(self.last_applied)

        logger.info(f"Recovered state: term={self.election_manager.current_term}, commit_index={self.commit_index}, last_applied={self.last_applied}")

        # Start KV state machine (loads its own periodically-persisted state)
        await self.kv_state_machine.start()

        # If a Raft snapshot exists and covers more than what the KV state
        # machine's own (independently-scheduled) persistence last captured,
        # it's the more authoritative source - restore from it instead.
        raft_snapshot = self.storage.load_snapshot()
        if raft_snapshot and raft_snapshot["last_included_index"] > self.kv_state_machine.last_applied_index:
            self.kv_state_machine.restore_from_snapshot(
                raft_snapshot["kv_state"], raft_snapshot["last_included_index"]
            )

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
    
    def _get_last_log_info(self) -> "tuple[int, int]":
        """Return (last_log_index, last_log_term) for this node's own log."""
        return self.storage.get_last_log_index(), self.storage.get_last_log_term()

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

    async def _send_install_snapshot_to_peer(self, peer: PeerInfo) -> None:
        """
        Send the full snapshot to a peer whose next_index has fallen behind
        our compacted log - normal AppendEntries can't catch it up since
        those entries no longer exist locally, only in the snapshot.
        """
        snapshot = self.storage.load_snapshot()
        if not snapshot:
            logger.warning(f"No snapshot available to install on {peer.node_id}")
            return

        try:
            import json
            from raft.proto import raft_pb2, raft_pb2_grpc
            import grpc

            channel = grpc.aio.insecure_channel(peer.raft_address)
            stub = raft_pb2_grpc.RaftServiceStub(channel)

            request = raft_pb2.InstallSnapshotRequest(
                term=self.election_manager.current_term,
                leader_id=self.node_id,
                last_included_index=snapshot["last_included_index"],
                last_included_term=snapshot["last_included_term"],
                data=json.dumps(snapshot["kv_state"]).encode('utf-8')
            )

            response = await stub.InstallSnapshot(request)
            await channel.close()

            if not response:
                return

            if response.term > self.election_manager.current_term:
                self.election_manager.current_term = response.term
                self.election_manager.voted_for = None
                await self._on_become_follower()
                return

            # Follower is now caught up through the snapshot boundary.
            self.next_index[peer.node_id] = snapshot["last_included_index"] + 1
            self.match_index[peer.node_id] = snapshot["last_included_index"]
            logger.info(f"Installed snapshot on {peer.node_id} up to index {snapshot['last_included_index']}")

        except Exception as e:
            logger.warning(f"Failed to install snapshot on {peer.node_id}: {e}")

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
        
        # Start batch flush task
        await self._start_batch_flush_task()
        
        # Record leader change
        try:
            from raft.prometheus_metrics import record_leader_change, update_batch_size
            record_leader_change()
            update_batch_size(self.batch_size)
        except ImportError:
            pass
        
        # Structured logging
        if self.structured_logger:
            self.structured_logger.log_leader_change(
                term=self.election_manager.current_term,
                role=self.state.value,
                commit_index=self.commit_index,
                last_applied=self.last_applied,
                log_length=self.storage.get_last_log_index(),
                old_role="follower"
            )
        
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
        
        # Stop batch flush task
        await self._stop_batch_flush_task()

        # Any writes still queued never got replicated as leader - fail them
        # explicitly rather than leaving callers awaiting put_price() hanging.
        async with self.batch_lock:
            self.pending_entries.clear()
            for future in self._pending_futures.values():
                if not future.done():
                    future.set_result(False)
            self._pending_futures.clear()

        logger.info(f"Node {self.node_id} became follower")
    
    # Batching methods
    
    async def _start_batch_flush_task(self) -> None:
        """Start the batch flush task for periodic flushing."""
        if self.batch_flush_task and not self.batch_flush_task.done():
            self.batch_flush_task.cancel()
        
        self.batch_flush_task = asyncio.create_task(self._batch_flush_loop())
    
    async def _stop_batch_flush_task(self) -> None:
        """Stop the batch flush task."""
        if self.batch_flush_task and not self.batch_flush_task.done():
            self.batch_flush_task.cancel()
            try:
                await self.batch_flush_task
            except asyncio.CancelledError:
                pass
    
    async def _batch_flush_loop(self) -> None:
        """Periodically flush pending entries."""
        while self.state == RaftState.LEADER:
            try:
                await asyncio.sleep(self.flush_interval / 1000.0)
                await self._flush_pending_entries()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batch flush loop: {e}")
                await asyncio.sleep(self.flush_interval / 1000.0)
    
    async def _add_to_batch(self, entry: LogEntry) -> asyncio.Future:
        """
        Add entry to pending batch and flush if needed.

        Returns a future that resolves to True/False once this entry's batch
        is actually flushed and replicated (or fails to be), so callers like
        put_price can report the real outcome instead of an optimistic ack.
        """
        future = asyncio.get_running_loop().create_future()
        async with self.batch_lock:
            self.pending_entries.append(entry)
            self._pending_futures[entry.index] = future

            # Flush if batch is full
            should_flush = len(self.pending_entries) >= self.batch_size

        if should_flush:
            await self._flush_pending_entries()

        return future

    async def _flush_pending_entries(self) -> None:
        """Flush all pending entries to followers."""
        async with self.batch_lock:
            if not self.pending_entries:
                return

            entries_to_flush = self.pending_entries.copy()
            self.pending_entries.clear()
            futures_to_resolve = [self._pending_futures.pop(e.index, None) for e in entries_to_flush]

        if entries_to_flush:
            logger.info(f"Flushing batch of {len(entries_to_flush)} entries")
            
            # Record batch flush
            try:
                from raft.prometheus_metrics import record_batch_flush
                record_batch_flush()
            except ImportError:
                pass
            
            # Time the batch flush
            import time
            start_time = time.time()
            success = await self._replicate_to_peers(entries_to_flush)
            duration_ms = (time.time() - start_time) * 1000
            
            # Structured logging
            if self.structured_logger:
                self.structured_logger.log_batch_flush(
                    term=self.election_manager.current_term,
                    role=self.state.value,
                    commit_index=self.commit_index,
                    last_applied=self.last_applied,
                    log_length=self.storage.get_last_log_index(),
                    batch_size=self.batch_size,
                    entries_count=len(entries_to_flush),
                    duration_ms=duration_ms
                )
            
            if success:
                # Update commit index and apply
                await self._update_commit_index()
                logger.info(f"Successfully replicated batch of {len(entries_to_flush)} entries")
            else:
                # Replication failed - remove entries from log, preserving everything
                # before this batch (entries_to_flush are contiguous, appended together).
                logger.warning(f"Batch replication failed for {len(entries_to_flush)} entries, removing from log")
                self.storage.truncate_log_from(entries_to_flush[0].index)

            # Let anyone awaiting these specific entries (e.g. put_price) know the outcome.
            for future in futures_to_resolve:
                if future is not None and not future.done():
                    future.set_result(success)
    
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
                    from raft.prometheus_metrics import record_crash_recovery
                    record_crash_recovery(len(unapplied_entries), duration_ms)
                except ImportError:
                    pass
                
                logger.info(f"Crash recovery complete: last_applied={self.last_applied}")
        else:
            logger.info("No unapplied entries found, recovery not needed")
    
    async def _apply_committed_entries(self) -> None:
        """Apply all committed entries that haven't been applied yet."""
        applied_count = 0
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.storage.get_log_entry(self.last_applied)
            if entry:
                await self.kv_state_machine.apply_command(entry)
                logger.debug(f"Applied entry {self.last_applied} to state machine")
                applied_count += 1
                
                # Record command applied
                try:
                    from raft.prometheus_metrics import record_command_applied
                    record_command_applied()
                except ImportError:
                    pass
            
            # Persist last_applied after each entry
            self.storage.set_last_applied(self.last_applied)
        
        # Update KV entries count
        if applied_count > 0:
            try:
                from raft.prometheus_metrics import update_kv_entries
                kv_entries = len(self.kv_state_machine.dump_state().get("entries", {}))
                update_kv_entries(kv_entries)
            except ImportError:
                pass

        # Compact the log once enough entries have accumulated since the
        # last snapshot, so it doesn't grow unbounded.
        if self.last_applied - self.storage.last_included_index >= self.snapshot_threshold:
            await self._take_snapshot()

    async def _take_snapshot(self) -> None:
        """Snapshot the state machine at last_applied and compact the log up to it."""
        snapshot_index = self.last_applied
        entry = self.storage.get_log_entry(snapshot_index)
        snapshot_term = entry.term if entry else self.storage.get_last_log_term()

        kv_state = self.kv_state_machine.get_snapshot_data()
        self.storage.save_snapshot(snapshot_index, snapshot_term, kv_state)

        try:
            from raft.prometheus_metrics import record_snapshot
            record_snapshot(snapshot_index)
        except ImportError:
            pass

        logger.info(f"Took snapshot at index {snapshot_index} (term {snapshot_term})")

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
        
        # Start timing replication
        import time
        start_time = time.time()
        
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
        
        # Calculate duration and record metrics
        duration_ms = (time.time() - start_time) * 1000
        success = successful_replications >= ((len(self.peers) + 1) // 2 + 1)  # +1 for self
        
        try:
            from raft.prometheus_metrics import record_replication
            record_replication(len(entries), duration_ms, success)
        except ImportError:
            pass
        
        # Structured logging
        if self.structured_logger:
            self.structured_logger.log_replication(
                term=self.election_manager.current_term,
                role=self.state.value,
                commit_index=self.commit_index,
                last_applied=self.last_applied,
                log_length=self.storage.get_last_log_index(),
                entries_count=len(entries),
                duration_ms=duration_ms,
                success=success
            )
        
        return success
    
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
            # get_term_at_index (not get_log_entry) - prev_log_index can
            # legitimately land exactly on the snapshot boundary, which
            # get_log_entry can't see (see RaftStorage.get_term_at_index).
            prev_log_term = self.storage.get_term_at_index(prev_log_index)
            
            # Send AppendEntries RPC
            response = await self._append_entries_to_peer(
                peer.node_id, peer, 
                self.election_manager.current_term,
                prev_log_index, prev_log_term,
                entries, self.commit_index
            )
            
            # Record AppendEntries sent
            try:
                from raft.prometheus_metrics import record_appendentries_sent
                record_appendentries_sent(peer.node_id)
            except ImportError:
                pass
            
            if response and response.success:
                # Update next_index/match_index using an absolute value
                # derived from *this RPC's own* prev_log_index, and only
                # ever move it forward (max, never regress). Concurrent
                # flushes can both be in flight for the same peer at once
                # (batch_lock only protects the pending_entries snapshot,
                # not the network round trip - see _flush_pending_entries),
                # so a second, already-superseded success response landing
                # after a newer one must not blindly add its own len(entries)
                # on top of whatever next_index currently holds - that
                # double-counts and overshoots past what the follower
                # actually has, which then makes every subsequent RPC fail
                # log matching. Found live: leader sending prev_log_index=15
                # while every follower only had 10 entries.
                confirmed_next_index = prev_log_index + len(entries) + 1
                self.next_index[peer.node_id] = max(self.next_index[peer.node_id], confirmed_next_index)
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
        
        # Only commit entries from current term. get_term_at_index (not
        # get_log_entry) - majority_index can legitimately land exactly on
        # the snapshot boundary, since a peer caught up via InstallSnapshot
        # has its match_index set to exactly that boundary (see
        # _send_install_snapshot_to_peer), and get_log_entry can't see it.
        if majority_index > self.commit_index:
            majority_term = self.storage.get_term_at_index(majority_index)
            if majority_term == self.election_manager.current_term:
                old_commit_index = self.commit_index
                self.commit_index = majority_index
                self.storage.set_commit_index(self.commit_index)
                logger.info(f"Updated commit index to {self.commit_index}")
                
                # Record commit metrics
                try:
                    from raft.prometheus_metrics import record_commit
                    import time
                    # Simple timing for commit operation
                    commit_duration = 1.0  # Approximate
                    record_commit(self.commit_index - old_commit_index, commit_duration)
                except ImportError:
                    pass
                
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
            logger.warning(
                f"Log matching failed from {request.leader_id}: "
                f"prev_log_index={request.prev_log_index}, prev_log_term={request.prev_log_term}, "
                f"my last_log_index={self.storage.get_last_log_index()}, "
                f"my term_at(prev_log_index)={self.storage.get_term_at_index(request.prev_log_index)}, "
                f"my last_included_index={self.storage.last_included_index}"
            )
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
            if request.prev_log_index < self.storage.get_last_log_index():
                logger.info(f"Truncating log from index {request.prev_log_index + 1} for catch-up")
                self.storage.truncate_log_from(request.prev_log_index + 1)
            
            # Append new entries
            self.storage.append_entries(entries)
            logger.info(f"Appended {len(entries)} entries to log (catch-up)")
        
        # Update commit index
        if request.leader_commit > self.commit_index:
            old_commit_index = self.commit_index
            self.commit_index = min(request.leader_commit, self.storage.get_last_log_index())
            self.storage.set_commit_index(self.commit_index)
            
            if self.commit_index > old_commit_index:
                logger.info(f"Updated commit index from {old_commit_index} to {self.commit_index}")
            
            # Apply newly committed entries
            await self._apply_committed_entries()
        
        # Update storage
        self.storage.set_current_term(self.election_manager.current_term)
        
        # Create response
        current_log_length = self.storage.get_last_log_index()
        response = raft_pb2.AppendEntriesResponse(
            term=self.election_manager.current_term,
            success=True,
            match_index=current_log_length
        )
        
        logger.debug(f"Append entries response: success=True, match_index={response.match_index}")
        return response

    async def handle_install_snapshot(self, request) -> Any:
        """
        Handle incoming InstallSnapshot RPC (follower side). The leader sends
        this instead of AppendEntries when we've fallen far enough behind
        that it no longer has the entries we'd need for normal catch-up.
        """
        import json
        from raft.proto import raft_pb2

        logger.info(f"Handling InstallSnapshot from {request.leader_id} up to index {request.last_included_index}")

        if request.term < self.election_manager.current_term:
            return raft_pb2.InstallSnapshotResponse(term=self.election_manager.current_term)

        if request.term > self.election_manager.current_term:
            self.election_manager.current_term = request.term
            self.election_manager.voted_for = None
            if self.state != RaftState.FOLLOWER:
                await self._on_become_follower()

        self.election_manager.start_election_timeout()

        kv_state = json.loads(request.data.decode('utf-8'))

        # Install the snapshot, then discard our entire local log rather
        # than trying to preserve a matching suffix: anything we had is
        # either already covered by the snapshot or may conflict with the
        # leader's timeline, and replacing wholesale is simpler and still
        # correct (the leader will just resend anything genuinely needed).
        self.storage.save_snapshot(request.last_included_index, request.last_included_term, kv_state)
        self.storage.truncate_log_from(request.last_included_index + 1)

        self.kv_state_machine.restore_from_snapshot(kv_state, request.last_included_index)

        self.commit_index = max(self.commit_index, request.last_included_index)
        self.last_applied = request.last_included_index
        self.storage.set_commit_index(self.commit_index)
        self.storage.set_last_applied(self.last_applied)
        self.storage.set_current_term(self.election_manager.current_term)

        logger.info(f"Installed snapshot up to index {request.last_included_index} (term {request.last_included_term})")

        return raft_pb2.InstallSnapshotResponse(term=self.election_manager.current_term)

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

        if prev_log_index > self.storage.get_last_log_index():
            return False

        # The snapshot boundary itself is a valid match point - it's exactly
        # what a follower just installed via InstallSnapshot - but it isn't
        # addressable via get_log_entry (only entries after it remain in the
        # log), so it needs its own check here. Without this, the leader's
        # very first AppendEntries after a successful snapshot install
        # always fails log matching (prev_log_index lands exactly on the
        # boundary), next_index gets decremented back onto the boundary, and
        # the leader re-sends InstallSnapshot again next heartbeat - forever.
        if prev_log_index == self.storage.last_included_index:
            return prev_log_term == self.storage.last_included_term

        entry = self.storage.get_log_entry(prev_log_index)
        if not entry:
            return False
        
        return entry.term == prev_log_term
    
    async def _heartbeat_loop(self) -> None:
        """Send heartbeats to all followers."""
        from .types import HEARTBEAT_INTERVAL
        
        while self.state == RaftState.LEADER:
            try:
                # Update node state metrics
                try:
                    from raft.prometheus_metrics import update_node_state
                    log_length = self.storage.get_last_log_index()
                    update_node_state(
                        self.election_manager.current_term,
                        self.commit_index,
                        self.last_applied,
                        log_length
                    )
                except ImportError:
                    pass
                
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
            # If this peer needs entries we've already compacted away, a
            # normal AppendEntries can't catch it up (those entries no
            # longer exist in our log) - send the snapshot instead.
            if self.next_index[peer.node_id] <= self.storage.last_included_index:
                await self._send_install_snapshot_to_peer(peer)
                return

            # Get previous log entry info
            prev_log_index = self.next_index[peer.node_id] - 1
            # get_term_at_index (not get_log_entry) - prev_log_index can
            # legitimately land exactly on the snapshot boundary, which
            # get_log_entry can't see (see RaftStorage.get_term_at_index).
            prev_log_term = self.storage.get_term_at_index(prev_log_index)
            
            # Get entries to send (for catch-up)
            entries_to_send = []
            current_log_length = self.storage.get_last_log_index()
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
                # Update next_index/match_index using an absolute value
                # derived from *this RPC's own* prev_log_index, and only
                # ever move it forward - see the matching comment in
                # _send_append_entries_to_peer for why (this path races
                # against that one for the very same peer).
                if entries_to_send:
                    confirmed_next_index = prev_log_index + len(entries_to_send) + 1
                    self.next_index[peer.node_id] = max(self.next_index[peer.node_id], confirmed_next_index)
                    self.match_index[peer.node_id] = self.next_index[peer.node_id] - 1
                    logger.info(f"Sent {len(entries_to_send)} entries to {peer.node_id} for catch-up")
                    # A follower catching up here (rather than via the
                    # synchronous batch-flush path - e.g. right after a
                    # leader failover, when followers reconcile next_index
                    # over several heartbeats) can push the cluster past
                    # majority for entries the original flush's own
                    # majority check missed. Re-check now, or that commit
                    # only becomes visible whenever some *future* write
                    # happens to re-trigger it - a real liveness gap,
                    # found via live 15-node failover testing.
                    await self._update_commit_index()
                else:
                    # Same reasoning as above: only move match_index forward,
                    # never regress it based on a possibly-stale prev_log_index.
                    self.match_index[peer.node_id] = max(self.match_index[peer.node_id], prev_log_index)
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
            
            # Add to batch for replication, and wait for that specific entry's
            # batch to actually flush so we report the real outcome rather
            # than an optimistic ack (batching still gives concurrent writers
            # throughput - they just each wait on their own future).
            flush_future = await self._add_to_batch(entry)
            replicated = await flush_future

            return {
                "ok": replicated,
                "leader_hint": self.node_id,
                "error_message": None if replicated else "Replication failed"
            }

        except Exception as e:
            logger.error(f"Error in PutPrice: {e}")
            return {
                "ok": False,
                "leader_hint": self.node_id,
                "error_message": str(e)
            }

    async def batch_put_price(self, ticker_prices: List[TickerPrice]) -> Dict[str, Any]:
        """
        Handle BatchPutPrice request.
        Only leader accepts writes. The whole batch is appended and
        replicated as a single log entry (BATCH_PUT command) rather than
        one entry per price - kv/state_machine.py already applies a
        BATCH_PUT atomically to all included symbols.
        """
        if self.state != RaftState.LEADER:
            return {
                "ok": False,
                "leader_hint": None,
                "error_message": "Not leader"
            }

        try:
            command_bytes = serialize_batch_put_command(ticker_prices)

            log_index = self.storage.get_last_log_index() + 1
            entry = LogEntry(
                index=log_index,
                term=self.election_manager.current_term,
                command_bytes=command_bytes
            )

            self.storage.append_entries([entry])
            logger.info(f"Leader appended BatchPutPrice: {len(ticker_prices)} entries at index {log_index}")

            # Same real-confirmation behavior as put_price - see fix #3 in
            # README's "Fixed this session": don't ack until the batch
            # containing this entry has actually flushed and replicated.
            flush_future = await self._add_to_batch(entry)
            replicated = await flush_future

            return {
                "ok": replicated,
                "leader_hint": self.node_id,
                "error_message": None if replicated else "Replication failed"
            }

        except Exception as e:
            logger.error(f"Error in BatchPutPrice: {e}")
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
            
            # Get metrics if available. Reads from the live Prometheus
            # collector (the same one raft/prometheus_metrics.py updates on
            # every election/replication/commit/etc.) - previously this read
            # from the old raft/metrics.py collector, which nothing actually
            # updated, so dump-state always showed zeros under real traffic.
            metrics = None
            try:
                from raft.prometheus_metrics import get_prometheus_metrics
                prometheus_metrics = get_prometheus_metrics()
                if prometheus_metrics:
                    metrics = prometheus_metrics.get_metrics_dict()
            except ImportError:
                pass  # Metrics not available
            
            return {
                "ok": True,
                "node_id": self.node_id,
                "current_term": self.election_manager.current_term,
                "state": self.state.value,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": self.storage.get_last_log_index(),
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
            "log_length": self.storage.get_last_log_index(),
            "peers": [peer.node_id for peer in self.peers]
        }
