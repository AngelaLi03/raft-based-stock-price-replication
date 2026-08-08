"""
Tests for performance metrics functionality.
"""

import pytest
import time
from unittest.mock import Mock, patch
from prometheus_client import CollectorRegistry

from raft.prometheus_metrics import PrometheusMetrics, init_prometheus_metrics, get_prometheus_metrics


class TestPrometheusMetrics:
    """Test Prometheus metrics functionality."""
    
    def test_metrics_initialization(self):
        """Test metrics initialization."""
        metrics = PrometheusMetrics("test_node", port=0)  # Port 0 to avoid conflicts
        
        assert metrics.node_id == "test_node"
        assert metrics.port == 0
        assert metrics.registry is not None
        
        # Check that metrics are registered
        assert metrics.elections_total is not None
        assert metrics.commits_total is not None
        assert metrics.entries_replicated_total is not None
    
    def test_election_recording(self):
        """Test election event recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record election
        metrics.record_election(150.5)
        
        # Check that metric was recorded. election_duration_ms is a Histogram,
        # which tracks _sum/_count/buckets rather than a single _value.
        assert metrics.elections_total.labels(node_id="test_node")._value.get() == 1
        assert metrics.election_duration_ms.labels(node_id="test_node")._sum.get() == 150.5
    
    def test_replication_recording(self):
        """Test replication event recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record replication
        metrics.record_replication(5, 25.3, success=True)
        
        # Check that metrics were recorded (replication_latency_ms is a Histogram)
        assert metrics.entries_replicated_total.labels(node_id="test_node")._value.get() == 5
        assert metrics.replication_latency_ms.labels(node_id="test_node")._sum.get() == 25.3
        assert metrics.replication_failures_total.labels(node_id="test_node")._value.get() == 0
    
    def test_replication_failure_recording(self):
        """Test replication failure recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record failed replication
        metrics.record_replication(3, 100.0, success=False)
        
        # Check that failure was recorded
        assert metrics.entries_replicated_total.labels(node_id="test_node")._value.get() == 3
        assert metrics.replication_failures_total.labels(node_id="test_node")._value.get() == 1
    
    def test_commit_recording(self):
        """Test commit event recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record commit
        metrics.record_commit(10, 5.2)
        
        # Check that metrics were recorded (commit_latency_ms is a Histogram)
        assert metrics.commits_total.labels(node_id="test_node")._value.get() == 10
        assert metrics.commit_latency_ms.labels(node_id="test_node")._sum.get() == 5.2
    
    def test_leader_change_recording(self):
        """Test leader change recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record leader change
        metrics.record_leader_change()
        
        # Check that metric was recorded
        assert metrics.leader_changes_total.labels(node_id="test_node")._value.get() == 1
    
    def test_crash_recovery_recording(self):
        """Test crash recovery recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record crash recovery
        metrics.record_crash_recovery(15, 200.0)
        
        # Check that metrics were recorded (snapshot_load_time_ms is a Histogram)
        assert metrics.crash_recoveries_total.labels(node_id="test_node")._value.get() == 1
        assert metrics.replay_entries_total.labels(node_id="test_node")._value.get() == 15
        assert metrics.snapshot_load_time_ms.labels(node_id="test_node")._sum.get() == 200.0
    
    def test_catchup_recording(self):
        """Test follower catch-up recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record catch-up
        metrics.record_catchup(8, 50.0)
        
        # Check that metric was recorded (catchup_latency_ms is a Histogram)
        assert metrics.catchup_latency_ms.labels(node_id="test_node")._sum.get() == 50.0
    
    def test_storage_operations_recording(self):
        """Test storage operations recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record storage operations
        metrics.record_storage_write()
        metrics.record_storage_read()
        metrics.record_storage_write()
        
        # Check that metrics were recorded
        assert metrics.storage_writes_total.labels(node_id="test_node")._value.get() == 2
        assert metrics.storage_reads_total.labels(node_id="test_node")._value.get() == 1
    
    def test_command_applied_recording(self):
        """Test command application recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record command applications
        for _ in range(5):
            metrics.record_command_applied()
        
        # Check that metric was recorded
        assert metrics.commands_applied_total.labels(node_id="test_node")._value.get() == 5
    
    def test_kv_entries_update(self):
        """Test KV entries count update."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Update KV entries count
        metrics.update_kv_entries(42)
        
        # Check that metric was updated
        assert metrics.kv_entries_total.labels(node_id="test_node")._value.get() == 42
    
    def test_node_state_update(self):
        """Test node state metrics update."""
        metrics = PrometheusMetrics("test_node", port=0)

        # Update node state
        metrics.update_node_state(term=5, commit_index=100, last_applied=95, log_length=105)

        # Check that metrics were updated
        assert metrics.current_term.labels(node_id="test_node")._value.get() == 5
        assert metrics.commit_index.labels(node_id="test_node")._value.get() == 100
        assert metrics.last_applied.labels(node_id="test_node")._value.get() == 95
        assert metrics.log_length.labels(node_id="test_node")._value.get() == 105

    def test_node_role_update(self):
        """Test node role metric update."""
        metrics = PrometheusMetrics("test_node", port=0)

        metrics.update_node_role("follower")
        assert metrics.node_role.labels(node_id="test_node")._value.get() == 0

        metrics.update_node_role("candidate")
        assert metrics.node_role.labels(node_id="test_node")._value.get() == 1

        metrics.update_node_role("leader")
        assert metrics.node_role.labels(node_id="test_node")._value.get() == 2

    def test_batch_metrics(self):
        """Test batch-related metrics."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Update batch size
        metrics.update_batch_size(10)
        assert metrics.batch_size.labels(node_id="test_node")._value.get() == 10
        
        # Record batch flush
        metrics.record_batch_flush()
        assert metrics.batch_flushes_total.labels(node_id="test_node")._value.get() == 1
    
    def test_appendentries_sent_recording(self):
        """Test AppendEntries sent recording."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record AppendEntries sent to different peers
        metrics.record_appendentries_sent("node2")
        metrics.record_appendentries_sent("node3")
        metrics.record_appendentries_sent("node2")
        
        # Check that metrics were recorded
        assert metrics.appendentries_sent_total.labels(node_id="test_node", peer_id="node2")._value.get() == 2
        assert metrics.appendentries_sent_total.labels(node_id="test_node", peer_id="node3")._value.get() == 1
    
    def test_metrics_export(self):
        """Test metrics export functionality."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record some metrics
        metrics.record_election(100.0)
        metrics.record_commit(5, 10.0)
        
        # Get metrics in Prometheus format
        metrics_text = metrics.get_metrics()
        
        # Should contain our metrics
        assert "raft_elections_total" in metrics_text
        assert "raft_commits_total" in metrics_text
        assert "test_node" in metrics_text
    
    def test_metrics_dict_export(self):
        """Test metrics dictionary export."""
        metrics = PrometheusMetrics("test_node", port=0)
        
        # Record some metrics
        metrics.record_election(100.0)
        metrics.record_commit(5, 10.0)
        
        # Get metrics as dictionary
        metrics_dict = metrics.get_metrics_dict()
        
        # Should contain expected fields
        assert "node_id" in metrics_dict
        assert "timestamp" in metrics_dict
        assert "elections_total" in metrics_dict
        assert "commits_total" in metrics_dict
        
        assert metrics_dict["node_id"] == "test_node"
        assert metrics_dict["elections_total"] == 1
        assert metrics_dict["commits_total"] == 5
    
    def test_global_metrics_functions(self):
        """Test global metrics functions."""
        # Initialize metrics
        init_prometheus_metrics("test_node", port=0)
        
        # Get metrics instance
        metrics = get_prometheus_metrics()
        assert metrics is not None
        assert metrics.node_id == "test_node"
        
        # Test global functions
        from raft.prometheus_metrics import (
            record_election, record_replication, record_commit,
            record_leader_change, record_crash_recovery, record_catchup,
            record_storage_write, record_command_applied, update_kv_entries,
            update_node_state, update_node_role, update_batch_size, record_batch_flush
        )
        
        # These should not raise exceptions
        record_election(50.0)
        record_replication(3, 25.0, True)
        record_commit(2, 5.0)
        record_leader_change()
        record_crash_recovery(10, 100.0)
        record_catchup(5, 30.0)
        record_storage_write()
        record_command_applied()
        update_kv_entries(20)
        update_node_state(1, 10, 8, 12)
        update_node_role("leader")
        update_batch_size(5)
        record_batch_flush()
    
    def test_metrics_without_initialization(self):
        """Test metrics functions when not initialized."""
        # Clear global metrics
        import raft.prometheus_metrics
        raft.prometheus_metrics._prometheus_metrics = None
        
        from raft.prometheus_metrics import record_election
        
        # Should not raise exception
        record_election(50.0)
    
    def test_multiple_metrics_instances(self):
        """Test multiple metrics instances don't conflict."""
        metrics1 = PrometheusMetrics("node1", port=0)
        metrics2 = PrometheusMetrics("node2", port=0)
        
        # Record metrics on different instances
        metrics1.record_election(100.0)
        metrics2.record_election(200.0)
        
        # Should be separate
        assert metrics1.elections_total.labels(node_id="node1")._value.get() == 1
        assert metrics2.elections_total.labels(node_id="node2")._value.get() == 1
        
        # Other node should have 0
        assert metrics1.elections_total.labels(node_id="node2")._value.get() == 0
        assert metrics2.elections_total.labels(node_id="node1")._value.get() == 0
    
    def test_metrics_registry_isolation(self):
        """Test that different metrics instances use separate registries."""
        metrics1 = PrometheusMetrics("node1", port=0)
        metrics2 = PrometheusMetrics("node2", port=0)

        # Should have different registries
        assert metrics1.registry is not metrics2.registry

        # Record something node-specific - without any recorded samples,
        # exported text is just HELP/TYPE metadata (identical for both,
        # since both define the same metric families), so "isolation"
        # is only observable once each has its own label values.
        metrics1.record_election(100.0)
        metrics2.record_election(200.0)

        # Should be able to export separately
        text1 = metrics1.get_metrics()
        text2 = metrics2.get_metrics()

        assert text1 != text2
        assert "node1" in text1
        assert "node2" in text2
        assert "node2" not in text1
        assert "node1" not in text2
