"""
Key-Value state machine for applying committed Raft log entries.
"""

import json
import logging
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import asyncio

from client.proto import client_pb2

logger = logging.getLogger(__name__)


@dataclass
class TickerPrice:
    """Stock price data structure."""
    symbol: str
    price: float
    timestamp: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "price": self.price,
            "timestamp": self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TickerPrice":
        """Create from dictionary."""
        return cls(
            symbol=data["symbol"],
            price=data["price"],
            timestamp=data["timestamp"]
        )


@dataclass
class Command:
    """Command structure for log entries."""
    type: str  # "PUT", "BATCH_PUT"
    data: Any  # TickerPrice or List[TickerPrice]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        if self.type == "PUT":
            return {
                "type": self.type,
                "data": self.data.to_dict()
            }
        elif self.type == "BATCH_PUT":
            return {
                "type": self.type,
                "data": [item.to_dict() for item in self.data]
            }
        else:
            raise ValueError(f"Unknown command type: {self.type}")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Command":
        """Create from dictionary."""
        if data["type"] == "PUT":
            return cls(
                type=data["type"],
                data=TickerPrice.from_dict(data["data"])
            )
        elif data["type"] == "BATCH_PUT":
            return cls(
                type=data["type"],
                data=[TickerPrice.from_dict(item) for item in data["data"]]
            )
        else:
            raise ValueError(f"Unknown command type: {data['type']}")


class KVStateMachine:
    """Key-Value state machine that applies committed Raft log entries."""
    
    def __init__(self, data_dir: str = "./data", persistence_interval: int = 5):
        """
        Initialize KV state machine.
        
        Args:
            data_dir: Directory for persistent storage
            persistence_interval: Seconds between persistence operations
        """
        self.data_dir = data_dir
        self.persistence_interval = persistence_interval
        
        # In-memory store: symbol -> TickerPrice
        self.store: Dict[str, TickerPrice] = {}
        
        # Persistence
        self.state_file = f"{data_dir}/kv_state.json"
        self.last_applied_index = 0
        
        # Background persistence task
        self.persistence_task: Optional[asyncio.Task] = None
        
        logger.info(f"KV state machine initialized with persistence interval {persistence_interval}s")
    
    async def start(self) -> None:
        """Start the state machine and load persisted state."""
        # Load persisted state if it exists
        await self._load_state()
        
        # Start background persistence
        self.persistence_task = asyncio.create_task(self._persistence_loop())
        
        logger.info(f"KV state machine started, loaded {len(self.store)} entries")
    
    async def stop(self) -> None:
        """Stop the state machine and save state."""
        if self.persistence_task:
            self.persistence_task.cancel()
            try:
                await self.persistence_task
            except asyncio.CancelledError:
                pass
        
        # Final save
        await self._save_state()
        logger.info("KV state machine stopped")
    
    async def apply_command(self, log_entry) -> None:
        """
        Apply a committed log entry to the state machine.
        
        Args:
            log_entry: LogEntry from Raft log
        """
        try:
            # Deserialize command
            command_data = json.loads(log_entry.command_bytes.decode('utf-8'))
            command = Command.from_dict(command_data)
            
            # Apply command
            if command.type == "PUT":
                ticker_price = command.data
                self.store[ticker_price.symbol] = ticker_price
                logger.debug(f"Applied PUT: {ticker_price.symbol}={ticker_price.price}")
                
            elif command.type == "BATCH_PUT":
                for ticker_price in command.data:
                    self.store[ticker_price.symbol] = ticker_price
                logger.debug(f"Applied BATCH_PUT: {len(command.data)} entries")
                
            else:
                logger.warning(f"Unknown command type: {command.type}")
            
            # Update last applied index
            self.last_applied_index = log_entry.index
            
        except Exception as e:
            logger.error(f"Failed to apply command at index {log_entry.index}: {e}")
            raise
    
    def get(self, key: str) -> Optional[TickerPrice]:
        """
        Get a value from the state machine.
        
        Args:
            key: Stock symbol
            
        Returns:
            TickerPrice if found, None otherwise
        """
        return self.store.get(key)
    
    def get_all(self) -> Dict[str, TickerPrice]:
        """Get all entries in the store."""
        return self.store.copy()
    
    def dump_state(self) -> Dict[str, Any]:
        """Dump current state for debugging."""
        return {
            "store_size": len(self.store),
            "last_applied_index": self.last_applied_index,
            "entries": {k: v.to_dict() for k, v in self.store.items()}
        }
    
    async def _load_state(self) -> None:
        """Load state from persistent storage."""
        try:
            import os
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.last_applied_index = data.get("last_applied_index", 0)
                    
                    # Load store entries
                    entries = data.get("entries", {})
                    for symbol, entry_data in entries.items():
                        self.store[symbol] = TickerPrice.from_dict(entry_data)
                    
                    logger.info(f"Loaded {len(self.store)} entries from persistent storage")
            else:
                logger.info("No persistent state found, starting fresh")
                
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            # Start fresh on error
            self.store = {}
            self.last_applied_index = 0
    
    async def _save_state(self) -> None:
        """Save state to persistent storage."""
        try:
            import os
            os.makedirs(self.data_dir, exist_ok=True)
            
            data = {
                "last_applied_index": self.last_applied_index,
                "entries": {k: v.to_dict() for k, v in self.store.items()},
                "timestamp": int(time.time())
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    async def _persistence_loop(self) -> None:
        """Background task to periodically save state."""
        while True:
            try:
                await asyncio.sleep(self.persistence_interval)
                await self._save_state()
                logger.debug("Periodic state save completed")
            except asyncio.CancelledError:
                logger.debug("Persistence loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in persistence loop: {e}")


# Utility functions for command serialization

def serialize_put_command(symbol: str, price: float, timestamp: int) -> bytes:
    """Serialize a PUT command."""
    command = Command(
        type="PUT",
        data=TickerPrice(symbol=symbol, price=price, timestamp=timestamp)
    )
    return json.dumps(command.to_dict()).encode('utf-8')


def serialize_batch_put_command(ticker_prices: List[TickerPrice]) -> bytes:
    """Serialize a BATCH_PUT command."""
    command = Command(
        type="BATCH_PUT",
        data=ticker_prices
    )
    return json.dumps(command.to_dict()).encode('utf-8')


def deserialize_command(command_bytes: bytes) -> Command:
    """Deserialize a command from bytes."""
    command_data = json.loads(command_bytes.decode('utf-8'))
    return Command.from_dict(command_data)
