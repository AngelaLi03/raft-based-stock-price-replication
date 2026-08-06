"""
Prometheus metrics integration for Raft cluster observability.
"""

import time
import logging
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import CollectorRegistry

logger = logging.getLogger(__name__)


class PrometheusMetrics:
    """Prometheus metrics collector for Raft cluster."""
    
    def __init__(self, node_id: str, port: int = 8000):
        self.node_id = node_id
        self.port = port
        
        # Create a custom registry for this node
        self.registry = CollectorRegistry()
        
        # Election metrics
        self.elections_total = Counter(
            'raft_elections_total',
            'Total number of elections',
            ['node_id'],
            registry=self.registry
        )
        
        self.election_duration_ms = Histogram(
            'raft_election_duration_ms',
            'Election duration in milliseconds',
            ['node_id'],
            buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
            registry=self.registry
        )
        
        # Replication metrics
        self.appendentries_sent_total = Counter(
            'raft_appendentries_sent_total',
            'Total AppendEntries RPCs sent',
            ['node_id', 'peer_id'],
            registry=self.registry
        )
        
        self.entries_replicated_total = Counter(
            'raft_entries_replicated_total',
            'Total log entries replicated',
            ['node_id'],
            registry=self.registry
        )
        
        self.replication_latency_ms = Histogram(
            'raft_replication_latency_ms',
            'Replication latency in milliseconds',
            ['node_id'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
            registry=self.registry
        )
        
        self.replication_failures_total = Counter(
            'raft_replication_failures_total',
            'Total replication failures',
            ['node_id'],
            registry=self.registry
        )
        
        # Commit metrics
        self.commits_total = Counter(
            'raft_commits_total',
            'Total commits',
            ['node_id'],
            registry=self.registry
        )
        
        self.commit_latency_ms = Histogram(
            'raft_commit_latency_ms',
            'Commit latency in milliseconds',
            ['node_id'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
            registry=self.registry
        )
        
        # Leader changes
        self.leader_changes_total = Counter(
            'raft_leader_changes_total',
            'Total leader changes',
            ['node_id'],
            registry=self.registry
        )
        
        # Recovery metrics
        self.crash_recoveries_total = Counter(
            'raft_crash_recoveries_total',
            'Total crash recoveries',
            ['node_id'],
            registry=self.registry
        )
        
        self.replay_entries_total = Counter(
            'raft_replay_entries_total',
            'Total entries replayed during recovery',
            ['node_id'],
            registry=self.registry
        )
        
        self.snapshot_load_time_ms = Histogram(
            'raft_snapshot_load_time_ms',
            'Snapshot load time in milliseconds',
            ['node_id'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
            registry=self.registry
        )
        
        self.catchup_latency_ms = Histogram(
            'raft_catchup_latency_ms',
            'Follower catch-up latency in milliseconds',
            ['node_id'],
            buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
            registry=self.registry
        )
        
        # Storage metrics
        self.storage_writes_total = Counter(
            'raft_storage_writes_total',
            'Total storage writes',
            ['node_id'],
            registry=self.registry
        )
        
        self.storage_reads_total = Counter(
            'raft_storage_reads_total',
            'Total storage reads',
            ['node_id'],
            registry=self.registry
        )
        
        # State machine metrics
        self.commands_applied_total = Counter(
            'raft_commands_applied_total',
            'Total commands applied to state machine',
            ['node_id'],
            registry=self.registry
        )
        
        self.kv_entries_total = Gauge(
            'raft_kv_entries_total',
            'Total KV store entries',
            ['node_id'],
            registry=self.registry
        )
        
        # Node state metrics
        self.current_term = Gauge(
            'raft_current_term',
            'Current term',
            ['node_id'],
            registry=self.registry
        )
        
        self.commit_index = Gauge(
            'raft_commit_index',
            'Current commit index',
            ['node_id'],
            registry=self.registry
        )
        
        self.last_applied = Gauge(
            'raft_last_applied',
            'Last applied index',
            ['node_id'],
            registry=self.registry
        )
        
        self.log_length = Gauge(
            'raft_log_length',
            'Current log length',
            ['node_id'],
            registry=self.registry
        )
        
        # Batch metrics
        self.batch_size = Gauge(
            'raft_batch_size',
            'Current batch size',
            ['node_id'],
            registry=self.registry
        )
        
        self.batch_flushes_total = Counter(
            'raft_batch_flushes_total',
            'Total batch flushes',
            ['node_id'],
            registry=self.registry
        )

        # Snapshot metrics
        self.snapshots_total = Counter(
            'raft_snapshots_total',
            'Total snapshots taken',
            ['node_id'],
            registry=self.registry
        )

        self.snapshot_last_index = Gauge(
            'raft_snapshot_last_index',
            'Index covered by the most recent snapshot',
            ['node_id'],
            registry=self.registry
        )

        # No HTTP server started here - server/metrics_server.py serves this
        # registry's /metrics (plus /health) on self.port instead. Starting
        # one here too used to collide with that server binding the same
        # port right after (the second bind failed silently, taking
        # /health down with it even though /metrics still worked via
        # whichever server won the race).
    
    def record_election(self, duration_ms: float) -> None:
        """Record an election event."""
        self.elections_total.labels(node_id=self.node_id).inc()
        self.election_duration_ms.labels(node_id=self.node_id).observe(duration_ms)
        logger.info(f"Election completed in {duration_ms:.2f}ms")
    
    def record_appendentries_sent(self, peer_id: str) -> None:
        """Record an AppendEntries RPC sent."""
        self.appendentries_sent_total.labels(node_id=self.node_id, peer_id=peer_id).inc()
    
    def record_replication(self, entries_count: int, duration_ms: float, success: bool = True) -> None:
        """Record a replication event."""
        self.entries_replicated_total.labels(node_id=self.node_id).inc(entries_count)
        self.replication_latency_ms.labels(node_id=self.node_id).observe(duration_ms)
        
        if not success:
            self.replication_failures_total.labels(node_id=self.node_id).inc()
            
        logger.info(f"Replicated {entries_count} entries in {duration_ms:.2f}ms (success={success})")
    
    def record_commit(self, entries_count: int, duration_ms: float) -> None:
        """Record a commit event."""
        self.commits_total.labels(node_id=self.node_id).inc(entries_count)
        self.commit_latency_ms.labels(node_id=self.node_id).observe(duration_ms)
        logger.info(f"Committed {entries_count} entries in {duration_ms:.2f}ms")
    
    def record_leader_change(self) -> None:
        """Record a leader change event."""
        self.leader_changes_total.labels(node_id=self.node_id).inc()
    
    def record_crash_recovery(self, replay_entries: int, duration_ms: float) -> None:
        """Record a crash recovery event."""
        self.crash_recoveries_total.labels(node_id=self.node_id).inc()
        self.replay_entries_total.labels(node_id=self.node_id).inc(replay_entries)
        self.snapshot_load_time_ms.labels(node_id=self.node_id).observe(duration_ms)
        logger.info(f"Crash recovery: replayed {replay_entries} entries in {duration_ms:.2f}ms")
    
    def record_catchup(self, entries_count: int, duration_ms: float) -> None:
        """Record a follower catch-up event."""
        self.catchup_latency_ms.labels(node_id=self.node_id).observe(duration_ms)
        logger.info(f"Follower catch-up: {entries_count} entries in {duration_ms:.2f}ms")
    
    def record_storage_write(self) -> None:
        """Record a storage write operation."""
        self.storage_writes_total.labels(node_id=self.node_id).inc()
    
    def record_storage_read(self) -> None:
        """Record a storage read operation."""
        self.storage_reads_total.labels(node_id=self.node_id).inc()
    
    def record_command_applied(self) -> None:
        """Record a command application to state machine."""
        self.commands_applied_total.labels(node_id=self.node_id).inc()
    
    def update_kv_entries(self, count: int) -> None:
        """Update KV store entries count."""
        self.kv_entries_total.labels(node_id=self.node_id).set(count)
    
    def update_node_state(self, term: int, commit_index: int, last_applied: int, log_length: int) -> None:
        """Update node state metrics."""
        self.current_term.labels(node_id=self.node_id).set(term)
        self.commit_index.labels(node_id=self.node_id).set(commit_index)
        self.last_applied.labels(node_id=self.node_id).set(last_applied)
        self.log_length.labels(node_id=self.node_id).set(log_length)
    
    def update_batch_size(self, size: int) -> None:
        """Update batch size metric."""
        self.batch_size.labels(node_id=self.node_id).set(size)
    
    def record_batch_flush(self) -> None:
        """Record a batch flush event."""
        self.batch_flushes_total.labels(node_id=self.node_id).inc()

    def record_snapshot(self, last_included_index: int) -> None:
        """Record a snapshot event."""
        self.snapshots_total.labels(node_id=self.node_id).inc()
        self.snapshot_last_index.labels(node_id=self.node_id).set(last_included_index)
    
    def get_metrics(self) -> str:
        """Get metrics in Prometheus format."""
        return generate_latest(self.registry).decode('utf-8')
    
    def get_metrics_dict(self) -> Dict[str, Any]:
        """Get metrics as dictionary for JSON responses."""
        return {
            "node_id": self.node_id,
            "timestamp": int(time.time()),
            "elections_total": self.elections_total.labels(node_id=self.node_id)._value.get(),
            "commits_total": self.commits_total.labels(node_id=self.node_id)._value.get(),
            "entries_replicated_total": self.entries_replicated_total.labels(node_id=self.node_id)._value.get(),
            "replication_failures_total": self.replication_failures_total.labels(node_id=self.node_id)._value.get(),
            "crash_recoveries_total": self.crash_recoveries_total.labels(node_id=self.node_id)._value.get(),
            "replay_entries_total": self.replay_entries_total.labels(node_id=self.node_id)._value.get(),
            "storage_writes_total": self.storage_writes_total.labels(node_id=self.node_id)._value.get(),
            "commands_applied_total": self.commands_applied_total.labels(node_id=self.node_id)._value.get(),
            "kv_entries_total": self.kv_entries_total.labels(node_id=self.node_id)._value.get(),
            "current_term": self.current_term.labels(node_id=self.node_id)._value.get(),
            "commit_index": self.commit_index.labels(node_id=self.node_id)._value.get(),
            "last_applied": self.last_applied.labels(node_id=self.node_id)._value.get(),
            "log_length": self.log_length.labels(node_id=self.node_id)._value.get(),
            "batch_size": self.batch_size.labels(node_id=self.node_id)._value.get(),
            "batch_flushes_total": self.batch_flushes_total.labels(node_id=self.node_id)._value.get()
        }


# Global metrics instance
_prometheus_metrics: Optional[PrometheusMetrics] = None


def init_prometheus_metrics(node_id: str, port: int = 8000) -> None:
    """Initialize Prometheus metrics."""
    global _prometheus_metrics
    _prometheus_metrics = PrometheusMetrics(node_id, port)
    logger.info(f"Prometheus metrics initialized for node {node_id} on port {port}")


def get_prometheus_metrics() -> Optional[PrometheusMetrics]:
    """Get the Prometheus metrics instance."""
    return _prometheus_metrics


def record_election(duration_ms: float) -> None:
    """Record an election event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_election(duration_ms)


def record_appendentries_sent(peer_id: str) -> None:
    """Record an AppendEntries RPC sent."""
    if _prometheus_metrics:
        _prometheus_metrics.record_appendentries_sent(peer_id)


def record_replication(entries_count: int, duration_ms: float, success: bool = True) -> None:
    """Record a replication event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_replication(entries_count, duration_ms, success)


def record_commit(entries_count: int, duration_ms: float) -> None:
    """Record a commit event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_commit(entries_count, duration_ms)


def record_leader_change() -> None:
    """Record a leader change event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_leader_change()


def record_crash_recovery(replay_entries: int, duration_ms: float) -> None:
    """Record a crash recovery event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_crash_recovery(replay_entries, duration_ms)


def record_catchup(entries_count: int, duration_ms: float) -> None:
    """Record a follower catch-up event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_catchup(entries_count, duration_ms)


def record_storage_write() -> None:
    """Record a storage write operation."""
    if _prometheus_metrics:
        _prometheus_metrics.record_storage_write()


def record_storage_read() -> None:
    """Record a storage read operation."""
    if _prometheus_metrics:
        _prometheus_metrics.record_storage_read()


def record_command_applied() -> None:
    """Record a command application to state machine."""
    if _prometheus_metrics:
        _prometheus_metrics.record_command_applied()


def update_kv_entries(count: int) -> None:
    """Update KV store entries count."""
    if _prometheus_metrics:
        _prometheus_metrics.update_kv_entries(count)


def update_node_state(term: int, commit_index: int, last_applied: int, log_length: int) -> None:
    """Update node state metrics."""
    if _prometheus_metrics:
        _prometheus_metrics.update_node_state(term, commit_index, last_applied, log_length)


def update_batch_size(size: int) -> None:
    """Update batch size metric."""
    if _prometheus_metrics:
        _prometheus_metrics.update_batch_size(size)


def record_batch_flush() -> None:
    """Record a batch flush event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_batch_flush()


def record_snapshot(last_included_index: int) -> None:
    """Record a snapshot event."""
    if _prometheus_metrics:
        _prometheus_metrics.record_snapshot(last_included_index)
