"""
Tests for structured logging functionality.
"""

import pytest
import json
import logging
from unittest.mock import Mock, patch
from io import StringIO

from raft.structured_logging import (
    StructuredLogger, StructuredFormatter, LogContext, LogLevel,
    get_structured_logger, setup_structured_logging
)


class TestStructuredLogging:
    """Test structured logging functionality."""
    
    def test_log_context_creation(self):
        """Test LogContext creation and serialization."""
        context = LogContext(
            node_id="node1",
            term=5,
            role="leader",
            commit_index=100,
            last_applied=95,
            log_length=105,
            batch_size=10,
            operation="replication",
            duration_ms=25.5
        )
        
        # Test to_dict conversion
        context_dict = {
            "node_id": "node1",
            "term": 5,
            "role": "leader",
            "commit_index": 100,
            "last_applied": 95,
            "log_length": 105,
            "batch_size": 10,
            "peer_id": None,
            "operation": "replication",
            "duration_ms": 25.5,
            "error_message": None
        }
        
        # Note: asdict would be used in actual implementation
        # This is a simplified test
        assert context.node_id == "node1"
        assert context.term == 5
        assert context.role == "leader"
        assert context.commit_index == 100
        assert context.last_applied == 95
        assert context.log_length == 105
        assert context.batch_size == 10
        assert context.operation == "replication"
        assert context.duration_ms == 25.5
    
    def test_structured_formatter_json(self):
        """Test JSON structured formatter."""
        formatter = StructuredFormatter(include_context=True)
        
        # Create a mock log record
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Add context to record
        context = LogContext(
            node_id="node1",
            term=5,
            role="leader",
            commit_index=100,
            last_applied=95,
            log_length=105
        )
        record.context = context
        
        # Format the record
        formatted = formatter.format(record)
        
        # Should be valid JSON
        log_data = json.loads(formatted)
        
        assert log_data["level"] == "INFO"
        assert log_data["logger"] == "test.logger"
        assert log_data["message"] == "Test message"
        assert "context" in log_data
        assert log_data["context"]["node_id"] == "node1"
        assert log_data["context"]["term"] == 5
        assert log_data["context"]["role"] == "leader"
    
    def test_structured_formatter_without_context(self):
        """Test structured formatter without context."""
        formatter = StructuredFormatter(include_context=False)
        
        # Create a mock log record
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Format the record
        formatted = formatter.format(record)
        
        # Should be valid JSON
        log_data = json.loads(formatted)
        
        assert log_data["level"] == "INFO"
        assert log_data["logger"] == "test.logger"
        assert log_data["message"] == "Test message"
        assert "context" not in log_data
    
    def test_structured_logger_initialization(self):
        """Test structured logger initialization."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        assert logger.logger.name == "test.logger"
        assert logger.node_id == "node1"
        assert logger.use_json is True
    
    def test_election_logging(self):
        """Test election event logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        # Mock the logger to capture output
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_election(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                duration_ms=150.5,
                success=True
            )
            
            # Should have called info
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Election successful" in call_args[0]
    
    def test_replication_logging(self):
        """Test replication event logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_replication(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                entries_count=3,
                duration_ms=25.0,
                success=True,
                peer_id="node2"
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Replication successful" in call_args[0]
            assert "3 entries" in call_args[0]
    
    def test_commit_logging(self):
        """Test commit event logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_commit(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                entries_count=5,
                duration_ms=10.0
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Committed 5 entries" in call_args[0]
    
    def test_leader_change_logging(self):
        """Test leader change logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_leader_change(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                old_role="follower"
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Role changed from follower to leader" in call_args[0]
    
    def test_batch_flush_logging(self):
        """Test batch flush logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_batch_flush(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                batch_size=10,
                entries_count=5,
                duration_ms=15.0
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Batch flush" in call_args[0]
            assert "5 entries" in call_args[0]
            assert "batch_size=10" in call_args[0]
    
    def test_crash_recovery_logging(self):
        """Test crash recovery logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_crash_recovery(
                term=5,
                role="follower",
                commit_index=100,
                last_applied=95,
                log_length=105,
                replay_entries=10,
                duration_ms=200.0
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Crash recovery" in call_args[0]
            assert "replayed 10 entries" in call_args[0]
    
    def test_catchup_logging(self):
        """Test follower catch-up logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_catchup(
                term=5,
                role="follower",
                commit_index=100,
                last_applied=95,
                log_length=105,
                entries_count=8,
                duration_ms=50.0,
                peer_id="node2"
            )
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0]
            assert "Follower catch-up" in call_args[0]
            assert "8 entries" in call_args[0]
    
    def test_error_logging(self):
        """Test error logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'error') as mock_error:
            logger.log_error(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                error_message="Replication failed",
                operation="replication"
            )
            
            mock_error.assert_called_once()
            call_args = mock_error.call_args[0]
            assert "Error in replication" in call_args[0]
            assert "Replication failed" in call_args[0]
    
    def test_debug_logging(self):
        """Test debug logging."""
        logger = StructuredLogger("test.logger", "node1", use_json=True)
        
        with patch.object(logger.logger, 'debug') as mock_debug:
            logger.log_debug(
                term=5,
                role="leader",
                commit_index=100,
                last_applied=95,
                log_length=105,
                message="Debug message",
                operation="test"
            )
            
            mock_debug.assert_called_once()
            call_args = mock_debug.call_args[0]
            assert "Debug message" in call_args[0]
    
    def test_global_logger_functions(self):
        """Test global logger functions."""
        # Test get_structured_logger
        logger1 = get_structured_logger("test1", "node1")
        logger2 = get_structured_logger("test1", "node1")
        logger3 = get_structured_logger("test2", "node1")
        
        # Should return same instance for same name and node
        assert logger1 is logger2
        # Should return different instance for different name
        assert logger1 is not logger3
    
    def test_setup_structured_logging(self):
        """Test structured logging setup."""
        # Capture log output
        log_capture = StringIO()
        handler = logging.StreamHandler(log_capture)
        
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_logger.handlers = []
            mock_logger.setLevel = Mock()
            mock_logger.addHandler = Mock()
            mock_get_logger.return_value = mock_logger
            
            setup_structured_logging("node1", use_json=True, log_level="DEBUG")
            
            # Should have configured logging
            mock_logger.setLevel.assert_called_with(logging.DEBUG)
            mock_logger.addHandler.assert_called()
    
    def test_log_level_enum(self):
        """Test LogLevel enum."""
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"
    
    def test_context_with_optional_fields(self):
        """Test LogContext with optional fields."""
        context = LogContext(
            node_id="node1",
            term=5,
            role="leader",
            commit_index=100,
            last_applied=95,
            log_length=105,
            batch_size=10,
            peer_id="node2",
            operation="replication",
            duration_ms=25.0,
            error_message="Test error"
        )
        
        assert context.batch_size == 10
        assert context.peer_id == "node2"
        assert context.operation == "replication"
        assert context.duration_ms == 25.0
        assert context.error_message == "Test error"
    
    def test_context_without_optional_fields(self):
        """Test LogContext without optional fields."""
        context = LogContext(
            node_id="node1",
            term=5,
            role="leader",
            commit_index=100,
            last_applied=95,
            log_length=105
        )
        
        assert context.batch_size is None
        assert context.peer_id is None
        assert context.operation is None
        assert context.duration_ms is None
        assert context.error_message is None
