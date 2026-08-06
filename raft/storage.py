"""
Durable storage abstraction for Raft log.
Production implementation with atomic writes, fsync, and crash recovery.
"""

import json
import os
import tempfile
import asyncio
from typing import List, Optional
from dataclasses import asdict
import logging
import time

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
        self.snapshot_file = os.path.join(data_dir, f"{node_id}_snapshot.json")

        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)

        # Snapshot coverage: entries with index <= last_included_index are
        # compacted away and only available via the snapshot itself, not
        # self.log. Must be loaded before _load_log() so index<->position
        # math (see get_log_entry/truncate_log_from) is consistent from the
        # start - self.log only ever holds entries *after* the snapshot.
        self.last_included_index = 0
        self.last_included_term = 0
        self._load_snapshot_metadata()

        # Load existing data
        self._load_metadata()
        self._load_log()
    
    def _load_metadata(self) -> None:
        """Load persistent metadata (term, voted_for, commit_index, last_applied)."""
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, 'r') as f:
                    meta = json.load(f)
                    self.current_term = meta.get("current_term", 0)
                    self.voted_for = meta.get("voted_for")
                    self.commit_index = meta.get("commit_index", 0)
                    self.last_applied = meta.get("last_applied", 0)
                    logger.info(f"Loaded metadata: term={self.current_term}, voted_for={self.voted_for}, commit_index={self.commit_index}, last_applied={self.last_applied}")
            except Exception as e:
                logger.warning(f"Failed to load metadata: {e}")
                self.current_term = 0
                self.voted_for = None
                self.commit_index = 0
                self.last_applied = 0
    
    def _load_snapshot_metadata(self) -> None:
        """Load just last_included_index/term from the snapshot file, if any."""
        if os.path.exists(self.snapshot_file):
            try:
                with open(self.snapshot_file, 'r') as f:
                    snap = json.load(f)
                    self.last_included_index = snap.get("last_included_index", 0)
                    self.last_included_term = snap.get("last_included_term", 0)
                    logger.info(
                        f"Found snapshot covering up to index {self.last_included_index} "
                        f"(term {self.last_included_term})"
                    )
            except Exception as e:
                logger.warning(f"Failed to load snapshot metadata: {e}")

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
        """Save metadata to disk with atomic write and fsync."""
        try:
            meta = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "timestamp": int(time.time())
            }
            self._atomic_write(self.meta_file, json.dumps(meta, indent=2))
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
    
    def _save_log(self) -> None:
        """Save log entries to disk with atomic write and fsync."""
        try:
            log_data = [entry.to_dict() for entry in self.log]
            self._atomic_write(self.log_file, json.dumps(log_data, indent=2))
        except Exception as e:
            logger.error(f"Failed to save log: {e}")
    
    def _atomic_write(self, filepath: str, content: str) -> None:
        """
        Atomically write content to file with fsync for durability.
        
        Args:
            filepath: Target file path
            content: Content to write
        """
        # Write to temporary file first
        temp_file = filepath + ".tmp"
        try:
            with open(temp_file, 'w') as f:
                f.write(content)
                f.flush()  # Ensure data is written to OS buffer
                os.fsync(f.fileno())  # Force data to disk
            
            # Atomic rename
            os.rename(temp_file, filepath)
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_file):
                os.unlink(temp_file)
            raise e
    
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
        """
        Get log entry at specific absolute index (1-based across the whole
        log, including any compacted prefix). Returns None both for
        out-of-range indices and for indices already covered by a snapshot -
        callers needing snapshot-covered data should read the snapshot
        instead (see get_last_log_info / load_snapshot).
        """
        pos = index - self.last_included_index - 1
        if 0 <= pos < len(self.log):
            return self.log[pos]
        return None

    def get_last_log_index(self) -> int:
        """Get the index of the last log entry (0 if empty and no snapshot)."""
        return self.last_included_index + len(self.log)

    def get_last_log_term(self) -> int:
        """Get the term of the last log entry, falling back to the snapshot's
        last_included_term if the log is empty (e.g. right after a snapshot
        compacted everything, or on a freshly restored node)."""
        if self.log:
            return self.log[-1].term
        return self.last_included_term

    def append_entries(self, entries: List[LogEntry]) -> None:
        """Append new entries to the log."""
        self.log.extend(entries)
        self._save_log()
        logger.debug(f"Appended {len(entries)} entries to log")

    def truncate_log_from(self, index: int) -> None:
        """Truncate log from the given absolute index (1-based)."""
        pos = index - self.last_included_index - 1
        if pos < 0:
            logger.error(
                f"Refusing to truncate from index {index}: already covered by "
                f"snapshot up to index {self.last_included_index}"
            )
            return
        if pos <= len(self.log):
            self.log = self.log[:pos]
            self._save_log()
            logger.debug(f"Truncated log from index {index}")

    def save_snapshot(self, last_included_index: int, last_included_term: int,
                       kv_state: dict) -> None:
        """
        Persist a snapshot covering all entries up to last_included_index,
        then compact the log by discarding those entries (they're now fully
        represented by the snapshot).
        """
        if last_included_index <= self.last_included_index:
            logger.debug(
                f"Skipping snapshot at {last_included_index}: already covered "
                f"up to {self.last_included_index}"
            )
            return

        snapshot = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "kv_state": kv_state,
            "timestamp": int(time.time())
        }
        self._atomic_write(self.snapshot_file, json.dumps(snapshot))

        # Compact: keep only entries not yet covered by the new snapshot.
        # Filtering on each entry's own .index (rather than positional slicing)
        # is correct regardless of the previous snapshot offset.
        self.log = [e for e in self.log if e.index > last_included_index]
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self._save_log()

        logger.info(
            f"Snapshot saved at index {last_included_index} (term {last_included_term}), "
            f"log compacted to {len(self.log)} entries"
        )

    def load_snapshot(self) -> Optional[dict]:
        """Load the persisted snapshot, if any, for state machine restore."""
        if not os.path.exists(self.snapshot_file):
            return None
        try:
            with open(self.snapshot_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load snapshot: {e}")
            return None
    
    def get_commit_index(self) -> int:
        """Get the commit index."""
        return self.commit_index
    
    def set_commit_index(self, commit_index: int) -> None:
        """Set the commit index and persist it."""
        self.commit_index = commit_index
        self._save_metadata()
        logger.debug(f"Commit index set to {commit_index}")
    
    def get_last_applied(self) -> int:
        """Get the last applied index."""
        return self.last_applied
    
    def set_last_applied(self, last_applied: int) -> None:
        """Set the last applied index and persist it."""
        self.last_applied = last_applied
        self._save_metadata()
        logger.debug(f"Last applied index set to {last_applied}")
