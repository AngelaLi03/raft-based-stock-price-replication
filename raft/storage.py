"""
Durable storage abstraction for Raft log.
MVP implementation using simple file-based storage.
"""

import json
import os
from typing import List, Optional
from dataclasses import asdict
import logging

from .types import LogEntry

logger = logging.getLogger(__name__)


class RaftStorage:
    """Simple file-based storage for Raft log entries."""
    
    def __init__(self, data_dir: str, node_id: str):
        """
        Initialize storage.
        
        Args:
            data_dir: Directory to store log files
            node_id: Unique node identifier
        """
        self.data_dir = data_dir
        self.node_id = node_id
        self.log_file = os.path.join(data_dir, f"{node_id}_raft_log.json")
        self.meta_file = os.path.join(data_dir, f"{node_id}_raft_meta.json")
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Load existing data
        self._load_metadata()
        self._load_log()
    
    def _load_metadata(self) -> None:
        """Load persistent metadata (term, voted_for, etc.)."""
        self.current_term = 0
        self.voted_for: Optional[str] = None
        
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, 'r') as f:
                    meta = json.load(f)
                    self.current_term = meta.get("current_term", 0)
                    self.voted_for = meta.get("voted_for")
                    logger.info(f"Loaded metadata: term={self.current_term}, voted_for={self.voted_for}")
            except Exception as e:
                logger.warning(f"Failed to load metadata: {e}")
    
    def _load_log(self) -> None:
        """Load log entries from disk."""
        self.log: List[LogEntry] = []
        
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'r') as f:
                    log_data = json.load(f)
                    self.log = [LogEntry.from_dict(entry) for entry in log_data]
                    logger.info(f"Loaded {len(self.log)} log entries")
            except Exception as e:
                logger.warning(f"Failed to load log: {e}")
    
    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        try:
            meta = {
                "current_term": self.current_term,
                "voted_for": self.voted_for
            }
            with open(self.meta_file, 'w') as f:
                json.dump(meta, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def _save_log(self) -> None:
        """Save log entries to disk."""
        try:
            log_data = [entry.to_dict() for entry in self.log]
            with open(self.log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save log: {e}")
    
    def get_current_term(self) -> int:
        """Get the current term."""
        return self.current_term
    
    def set_current_term(self, term: int) -> None:
        """Set the current term and persist it."""
        self.current_term = term
        self._save_metadata()
        logger.debug(f"Updated term to {term}")
    
    def get_voted_for(self) -> Optional[str]:
        """Get the candidate voted for in current term."""
        return self.voted_for
    
    def set_voted_for(self, candidate_id: Optional[str]) -> None:
        """Set the candidate voted for and persist it."""
        self.voted_for = candidate_id
        self._save_metadata()
        logger.debug(f"Voted for {candidate_id}")
    
    def get_log_entries(self) -> List[LogEntry]:
        """Get all log entries."""
        return self.log.copy()
    
    def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """Get log entry at specific index (1-based)."""
        if 1 <= index <= len(self.log):
            return self.log[index - 1]
        return None
    
    def get_last_log_index(self) -> int:
        """Get the index of the last log entry (0 if empty)."""
        return len(self.log)
    
    def get_last_log_term(self) -> int:
        """Get the term of the last log entry (0 if empty)."""
        if self.log:
            return self.log[-1].term
        return 0
    
    def append_entries(self, entries: List[LogEntry]) -> None:
        """Append new entries to the log."""
        self.log.extend(entries)
        self._save_log()
        logger.debug(f"Appended {len(entries)} entries to log")
    
    def truncate_log_from(self, index: int) -> None:
        """Truncate log from the given index (1-based)."""
        if index <= len(self.log):
            self.log = self.log[:index - 1]
            self._save_log()
            logger.debug(f"Truncated log from index {index}")
    
    def get_commit_index(self) -> int:
        """Get the commit index (for now, just return last log index)."""
        return len(self.log)
    
    def set_commit_index(self, commit_index: int) -> None:
        """Set the commit index (for now, just log it)."""
        logger.debug(f"Commit index set to {commit_index}")
