"""
Metrics collection for Raft cluster observability.
"""

import time
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RaftMetrics:
    """Raft cluster metrics."""
    
    # Election metrics
    elections_total: int = 0
    election_duration_ms: float = 0.0
    
    # Replication metrics
    entries_replicated_total: int = 0
    replication_latency_ms: float = 0.0
    replication_failures_total: int = 0
    
    # Commit metrics
    commits_total: int = 0
    commit_latency_ms: float = 0.0
    
    # Recovery metrics
    crash_recoveries_total: int = 0
    replay_entries_total: int = 0
    snapshot_load_time_ms: float = 0.0
    catchup_latency_ms: float = 0.0
    
    # Storage metrics
    log_entries_total: int = 0
    storage_writes_total: int = 0
    storage_reads_total: int = 0
    
    # State machine metrics
    commands_applied_total: int = 0
    kv_entries_total: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "elections_total": self.elections_total,
            "election_duration_ms": self.election_duration_ms,
            "entries_replicated_total": self.entries_replicated_total,
            "replication_latency_ms": self.replication_latency_ms,
            "replication_failures_total": self.replication_failures_total,
            "commits_total": self.commits_total,
            "commit_latency_ms": self.commit_latency_ms,
            "crash_recoveries_total": self.crash_recoveries_total,
            "replay_entries_total": self.replay_entries_total,
            "snapshot_load_time_ms": self.snapshot_load_time_ms,
            "catchup_latency_ms": self.catchup_latency_ms,
            "log_entries_total": self.log_entries_total,
            "storage_writes_total": self.storage_writes_total,
            "storage_reads_total": self.storage_reads_total,
            "commands_applied_total": self.commands_applied_total,
            "kv_entries_total": self.kv_entries_total
        }


class MetricsCollector:
    """Collects and tracks Raft metrics."""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.metrics = RaftMetrics()
        self.start_times: Dict[str, float] = {}
        
    def start_timer(self, operation: str) -> None:
        """Start timing an operation."""
        self.start_times[operation] = time.time()
        
    def end_timer(self, operation: str) -> float:
        """End timing an operation and return duration in milliseconds."""
        if operation not in self.start_times:
            return 0.0
            
        duration_ms = (time.time() - self.start_times[operation]) * 1000
        del self.start_times[operation]
        return duration_ms
    
    def record_election(self, duration_ms: float) -> None:
        """Record an election event."""
        self.metrics.elections_total += 1
        self.metrics.election_duration_ms = duration_ms
        logger.info(f"Election completed in {duration_ms:.2f}ms")
    
    def record_replication(self, entries_count: int, duration_ms: float, success: bool = True) -> None:
        """Record a replication event."""
        self.metrics.entries_replicated_total += entries_count
        self.metrics.replication_latency_ms = duration_ms
        
        if not success:
            self.metrics.replication_failures_total += 1
            
        logger.info(f"Replicated {entries_count} entries in {duration_ms:.2f}ms (success={success})")
    
    def record_commit(self, entries_count: int, duration_ms: float) -> None:
        """Record a commit event."""
        self.metrics.commits_total += entries_count
        self.metrics.commit_latency_ms = duration_ms
        logger.info(f"Committed {entries_count} entries in {duration_ms:.2f}ms")
    
    def record_crash_recovery(self, replay_entries: int, duration_ms: float) -> None:
        """Record a crash recovery event."""
        self.metrics.crash_recoveries_total += 1
        self.metrics.replay_entries_total += replay_entries
        self.metrics.snapshot_load_time_ms = duration_ms
        logger.info(f"Crash recovery: replayed {replay_entries} entries in {duration_ms:.2f}ms")
    
    def record_catchup(self, entries_count: int, duration_ms: float) -> None:
        """Record a follower catch-up event."""
        self.metrics.catchup_latency_ms = duration_ms
        logger.info(f"Follower catch-up: {entries_count} entries in {duration_ms:.2f}ms")
    
    def record_storage_write(self) -> None:
        """Record a storage write operation."""
        self.metrics.storage_writes_total += 1
    
    def record_storage_read(self) -> None:
        """Record a storage read operation."""
        self.metrics.storage_reads_total += 1
    
    def record_log_entry(self) -> None:
        """Record a log entry addition."""
        self.metrics.log_entries_total += 1
    
    def record_command_applied(self) -> None:
        """Record a command application to state machine."""
        self.metrics.commands_applied_total += 1
    
    def record_kv_entry(self) -> None:
        """Record a KV store entry."""
        self.metrics.kv_entries_total += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        metrics_dict = self.metrics.to_dict()
        metrics_dict["node_id"] = self.node_id
        metrics_dict["timestamp"] = int(time.time())
        return metrics_dict
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics = RaftMetrics()
        self.start_times.clear()
        logger.info("Metrics reset")


# Global metrics collector instance
_metrics_collector: Optional[MetricsCollector] = None


def init_metrics(node_id: str) -> None:
    """Initialize global metrics collector."""
    global _metrics_collector
    _metrics_collector = MetricsCollector(node_id)
    logger.info(f"Metrics initialized for node {node_id}")


def get_metrics() -> Optional[MetricsCollector]:
    """Get the global metrics collector."""
    return _metrics_collector


def record_election(duration_ms: float) -> None:
    """Record an election event."""
    if _metrics_collector:
        _metrics_collector.record_election(duration_ms)


def record_replication(entries_count: int, duration_ms: float, success: bool = True) -> None:
    """Record a replication event."""
    if _metrics_collector:
        _metrics_collector.record_replication(entries_count, duration_ms, success)


def record_commit(entries_count: int, duration_ms: float) -> None:
    """Record a commit event."""
    if _metrics_collector:
        _metrics_collector.record_commit(entries_count, duration_ms)


def record_crash_recovery(replay_entries: int, duration_ms: float) -> None:
    """Record a crash recovery event."""
    if _metrics_collector:
        _metrics_collector.record_crash_recovery(replay_entries, duration_ms)


def record_catchup(entries_count: int, duration_ms: float) -> None:
    """Record a follower catch-up event."""
    if _metrics_collector:
        _metrics_collector.record_catchup(entries_count, duration_ms)


def record_storage_write() -> None:
    """Record a storage write operation."""
    if _metrics_collector:
        _metrics_collector.record_storage_write()


def record_storage_read() -> None:
    """Record a storage read operation."""
    if _metrics_collector:
        _metrics_collector.record_storage_read()


def record_log_entry() -> None:
    """Record a log entry addition."""
    if _metrics_collector:
        _metrics_collector.record_log_entry()


def record_command_applied() -> None:
    """Record a command application to state machine."""
    if _metrics_collector:
        _metrics_collector.record_command_applied()


def record_kv_entry() -> None:
    """Record a KV store entry."""
    if _metrics_collector:
        _metrics_collector.record_kv_entry()


def start_timer(operation: str) -> None:
    """Start timing an operation."""
    if _metrics_collector:
        _metrics_collector.start_timer(operation)


def end_timer(operation: str) -> float:
    """End timing an operation and return duration in milliseconds."""
    if _metrics_collector:
        return _metrics_collector.end_timer(operation)
    return 0.0
