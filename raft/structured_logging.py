"""
Structured logging utilities for Raft cluster observability.
"""

import logging
import json
import time
from typing import Dict, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class LogLevel(Enum):
    """Log levels for structured logging."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogContext:
    """Context information for structured logging."""
    node_id: str
    term: int
    role: str
    commit_index: int
    last_applied: int
    log_length: int
    batch_size: Optional[int] = None
    peer_id: Optional[str] = None
    operation: Optional[str] = None
    duration_ms: Optional[float] = None
    error_message: Optional[str] = None


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logs."""
    
    def __init__(self, include_context: bool = True):
        """
        Initialize structured formatter.
        
        Args:
            include_context: Whether to include context information
        """
        self.include_context = include_context
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_entry = {
            "timestamp": time.time(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add context if available
        if self.include_context and hasattr(record, 'context'):
            context_dict = asdict(record.context) if hasattr(record.context, '__dataclass_fields__') else record.context
            log_entry["context"] = context_dict
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 
                          'msecs', 'relativeCreated', 'thread', 'threadName', 
                          'processName', 'process', 'getMessage', 'exc_info', 
                          'exc_text', 'stack_info', 'context']:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)


class StructuredLogger:
    """Structured logger for Raft operations."""
    
    def __init__(self, name: str, node_id: str, use_json: bool = True):
        """
        Initialize structured logger.
        
        Args:
            name: Logger name
            node_id: Node identifier
            use_json: Whether to use JSON formatting
        """
        self.logger = logging.getLogger(name)
        self.node_id = node_id
        self.use_json = use_json
        
        # Set up formatter
        if use_json:
            formatter = StructuredFormatter(include_context=True)
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(context)s] - %(message)s'
            )
        
        # Set up handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _create_context(self, term: int, role: str, commit_index: int, 
                       last_applied: int, log_length: int, **kwargs) -> LogContext:
        """Create log context from parameters."""
        return LogContext(
            node_id=self.node_id,
            term=term,
            role=role,
            commit_index=commit_index,
            last_applied=last_applied,
            log_length=log_length,
            **kwargs
        )
    
    def _log_with_context(self, level: LogLevel, message: str, context: LogContext, **kwargs):
        """Log message with context."""
        extra = {'context': context}
        extra.update(kwargs)
        
        if level == LogLevel.DEBUG:
            self.logger.debug(message, extra=extra)
        elif level == LogLevel.INFO:
            self.logger.info(message, extra=extra)
        elif level == LogLevel.WARNING:
            self.logger.warning(message, extra=extra)
        elif level == LogLevel.ERROR:
            self.logger.error(message, extra=extra)
        elif level == LogLevel.CRITICAL:
            self.logger.critical(message, extra=extra)
    
    def log_election(self, term: int, role: str, commit_index: int, last_applied: int, 
                    log_length: int, duration_ms: float, success: bool = True):
        """Log election event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="election", duration_ms=duration_ms
        )
        
        status = "successful" if success else "failed"
        message = f"Election {status} in {duration_ms:.2f}ms"
        
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_replication(self, term: int, role: str, commit_index: int, last_applied: int,
                       log_length: int, entries_count: int, duration_ms: float, 
                       success: bool = True, peer_id: str = None):
        """Log replication event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="replication", duration_ms=duration_ms,
            peer_id=peer_id
        )
        
        status = "successful" if success else "failed"
        message = f"Replication {status}: {entries_count} entries in {duration_ms:.2f}ms"
        
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_commit(self, term: int, role: str, commit_index: int, last_applied: int,
                  log_length: int, entries_count: int, duration_ms: float):
        """Log commit event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="commit", duration_ms=duration_ms
        )
        
        message = f"Committed {entries_count} entries in {duration_ms:.2f}ms"
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_leader_change(self, term: int, role: str, commit_index: int, last_applied: int,
                         log_length: int, old_role: str = None):
        """Log leader change event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="leader_change"
        )
        
        if old_role:
            message = f"Role changed from {old_role} to {role} in term {term}"
        else:
            message = f"Became {role} in term {term}"
        
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_batch_flush(self, term: int, role: str, commit_index: int, last_applied: int,
                       log_length: int, batch_size: int, entries_count: int, duration_ms: float):
        """Log batch flush event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="batch_flush", batch_size=batch_size, duration_ms=duration_ms
        )
        
        message = f"Batch flush: {entries_count} entries (batch_size={batch_size}) in {duration_ms:.2f}ms"
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_crash_recovery(self, term: int, role: str, commit_index: int, last_applied: int,
                          log_length: int, replay_entries: int, duration_ms: float):
        """Log crash recovery event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="crash_recovery", duration_ms=duration_ms
        )
        
        message = f"Crash recovery: replayed {replay_entries} entries in {duration_ms:.2f}ms"
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_catchup(self, term: int, role: str, commit_index: int, last_applied: int,
                   log_length: int, entries_count: int, duration_ms: float, peer_id: str = None):
        """Log follower catch-up event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation="catchup", duration_ms=duration_ms, peer_id=peer_id
        )
        
        message = f"Follower catch-up: {entries_count} entries in {duration_ms:.2f}ms"
        self._log_with_context(LogLevel.INFO, message, context)
    
    def log_error(self, term: int, role: str, commit_index: int, last_applied: int,
                 log_length: int, error_message: str, operation: str = None):
        """Log error event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation=operation, error_message=error_message
        )
        
        message = f"Error in {operation or 'operation'}: {error_message}"
        self._log_with_context(LogLevel.ERROR, message, context)
    
    def log_debug(self, term: int, role: str, commit_index: int, last_applied: int,
                 log_length: int, message: str, operation: str = None, **kwargs):
        """Log debug event."""
        context = self._create_context(
            term=term, role=role, commit_index=commit_index,
            last_applied=last_applied, log_length=log_length,
            operation=operation, **kwargs
        )
        
        self._log_with_context(LogLevel.DEBUG, message, context)


# Global structured logger instances
_structured_loggers: Dict[str, StructuredLogger] = {}


def get_structured_logger(name: str, node_id: str, use_json: bool = True) -> StructuredLogger:
    """Get or create a structured logger instance."""
    key = f"{name}_{node_id}"
    if key not in _structured_loggers:
        _structured_loggers[key] = StructuredLogger(name, node_id, use_json)
    return _structured_loggers[key]


def setup_structured_logging(node_id: str, use_json: bool = True, log_level: str = "INFO"):
    """Setup structured logging for a node."""
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add structured handler
    handler = logging.StreamHandler()
    if use_json:
        formatter = StructuredFormatter(include_context=True)
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(context)s] - %(message)s'
        )
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    
    # Get logger for this module
    logger = logging.getLogger(__name__)
    logger.info(f"Structured logging configured for node {node_id} (JSON={use_json})")
