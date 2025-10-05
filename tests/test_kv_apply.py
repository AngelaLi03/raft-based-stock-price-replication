"""
Tests for KV state machine operations.
"""

import pytest
import asyncio
import tempfile
import json
import time
from unittest.mock import patch

from kv.state_machine import KVStateMachine, TickerPrice, Command, serialize_put_command, serialize_batch_put_command


@pytest.fixture
def temp_data_dir():
    """Create temporary data directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def kv_state_machine(temp_data_dir):
    """Create KV state machine."""
    return KVStateMachine(temp_data_dir, persistence_interval=1)


@pytest.mark.asyncio
async def test_kv_state_machine_initialization(kv_state_machine):
    """Test KV state machine initialization."""
    assert len(kv_state_machine.store) == 0
    assert kv_state_machine.last_applied_index == 0
    assert kv_state_machine.persistence_interval == 1


@pytest.mark.asyncio
async def test_kv_state_machine_start_stop(kv_state_machine):
    """Test starting and stopping KV state machine."""
    await kv_state_machine.start()
    assert kv_state_machine.persistence_task is not None
    
    await kv_state_machine.stop()
    # Task might still exist but be cancelled
    assert kv_state_machine.persistence_task.done()


@pytest.mark.asyncio
async def test_apply_put_command(kv_state_machine):
    """Test applying a PUT command."""
    await kv_state_machine.start()
    
    # Create a mock log entry
    command_bytes = serialize_put_command("AAPL", 150.0, 1234567890)
    
    class MockLogEntry:
        def __init__(self, index, term, command_bytes):
            self.index = index
            self.term = term
            self.command_bytes = command_bytes
    
    entry = MockLogEntry(index=1, term=1, command_bytes=command_bytes)
    
    # Apply command
    await kv_state_machine.apply_command(entry)
    
    # Verify state
    assert kv_state_machine.last_applied_index == 1
    assert "AAPL" in kv_state_machine.store
    assert kv_state_machine.store["AAPL"].symbol == "AAPL"
    assert kv_state_machine.store["AAPL"].price == 150.0
    assert kv_state_machine.store["AAPL"].timestamp == 1234567890
    
    await kv_state_machine.stop()


@pytest.mark.asyncio
async def test_apply_batch_put_command(kv_state_machine):
    """Test applying a BATCH_PUT command."""
    await kv_state_machine.start()
    
    # Create batch command
    ticker_prices = [
        TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890),
        TickerPrice(symbol="NVDA", price=800.0, timestamp=1234567891)
    ]
    command_bytes = serialize_batch_put_command(ticker_prices)
    
    class MockLogEntry:
        def __init__(self, index, term, command_bytes):
            self.index = index
            self.term = term
            self.command_bytes = command_bytes
    
    entry = MockLogEntry(index=1, term=1, command_bytes=command_bytes)
    
    # Apply command
    await kv_state_machine.apply_command(entry)
    
    # Verify state
    assert kv_state_machine.last_applied_index == 1
    assert "AAPL" in kv_state_machine.store
    assert "NVDA" in kv_state_machine.store
    assert kv_state_machine.store["AAPL"].price == 150.0
    assert kv_state_machine.store["NVDA"].price == 800.0
    
    await kv_state_machine.stop()


@pytest.mark.asyncio
async def test_get_operations(kv_state_machine):
    """Test get operations."""
    await kv_state_machine.start()
    
    # Add some entries
    kv_state_machine.store["AAPL"] = TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    kv_state_machine.store["NVDA"] = TickerPrice(symbol="NVDA", price=800.0, timestamp=1234567891)
    
    # Test get
    aapl = kv_state_machine.get("AAPL")
    assert aapl is not None
    assert aapl.symbol == "AAPL"
    assert aapl.price == 150.0
    
    # Test get non-existent
    unknown = kv_state_machine.get("UNKNOWN")
    assert unknown is None
    
    # Test get_all
    all_entries = kv_state_machine.get_all()
    assert len(all_entries) == 2
    assert "AAPL" in all_entries
    assert "NVDA" in all_entries
    
    await kv_state_machine.stop()


@pytest.mark.asyncio
async def test_dump_state(kv_state_machine):
    """Test state dumping."""
    await kv_state_machine.start()
    
    # Add some entries
    kv_state_machine.store["AAPL"] = TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    kv_state_machine.last_applied_index = 1
    
    # Test dump state
    state = kv_state_machine.dump_state()
    
    assert state["store_size"] == 1
    assert state["last_applied_index"] == 1
    assert "AAPL" in state["entries"]
    assert state["entries"]["AAPL"]["price"] == 150.0
    
    await kv_state_machine.stop()


@pytest.mark.asyncio
async def test_persistence(kv_state_machine):
    """Test state persistence and loading."""
    # Start and add data
    await kv_state_machine.start()
    kv_state_machine.store["AAPL"] = TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    kv_state_machine.last_applied_index = 1
    
    # Stop to save state
    await kv_state_machine.stop()
    
    # Create new instance and load state
    new_kv = KVStateMachine(kv_state_machine.data_dir)
    await new_kv.start()
    
    # Verify state was loaded
    assert len(new_kv.store) == 1
    assert "AAPL" in new_kv.store
    assert new_kv.store["AAPL"].price == 150.0
    assert new_kv.last_applied_index == 1
    
    await new_kv.stop()


@pytest.mark.asyncio
async def test_command_serialization():
    """Test command serialization and deserialization."""
    # Test PUT command
    command_bytes = serialize_put_command("AAPL", 150.0, 1234567890)
    command_data = json.loads(command_bytes.decode('utf-8'))
    
    assert command_data["type"] == "PUT"
    assert command_data["data"]["symbol"] == "AAPL"
    assert command_data["data"]["price"] == 150.0
    assert command_data["data"]["timestamp"] == 1234567890
    
    # Test BATCH_PUT command
    ticker_prices = [
        TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890),
        TickerPrice(symbol="NVDA", price=800.0, timestamp=1234567891)
    ]
    batch_bytes = serialize_batch_put_command(ticker_prices)
    batch_data = json.loads(batch_bytes.decode('utf-8'))
    
    assert batch_data["type"] == "BATCH_PUT"
    assert len(batch_data["data"]) == 2
    assert batch_data["data"][0]["symbol"] == "AAPL"
    assert batch_data["data"][1]["symbol"] == "NVDA"


@pytest.mark.asyncio
async def test_ticker_price_serialization():
    """Test TickerPrice serialization."""
    ticker_price = TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    
    # Test to_dict
    data = ticker_price.to_dict()
    assert data["symbol"] == "AAPL"
    assert data["price"] == 150.0
    assert data["timestamp"] == 1234567890
    
    # Test from_dict
    new_ticker = TickerPrice.from_dict(data)
    assert new_ticker.symbol == "AAPL"
    assert new_ticker.price == 150.0
    assert new_ticker.timestamp == 1234567890


@pytest.mark.asyncio
async def test_command_serialization_roundtrip():
    """Test command serialization roundtrip."""
    # Test PUT command
    original_command = Command(
        type="PUT",
        data=TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    )
    
    command_bytes = json.dumps(original_command.to_dict()).encode('utf-8')
    command_data = json.loads(command_bytes.decode('utf-8'))
    restored_command = Command.from_dict(command_data)
    
    assert restored_command.type == "PUT"
    assert restored_command.data.symbol == "AAPL"
    assert restored_command.data.price == 150.0
    
    # Test BATCH_PUT command
    ticker_prices = [
        TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890),
        TickerPrice(symbol="NVDA", price=800.0, timestamp=1234567891)
    ]
    
    batch_command = Command(type="BATCH_PUT", data=ticker_prices)
    batch_bytes = json.dumps(batch_command.to_dict()).encode('utf-8')
    batch_data = json.loads(batch_bytes.decode('utf-8'))
    restored_batch = Command.from_dict(batch_data)
    
    assert restored_batch.type == "BATCH_PUT"
    assert len(restored_batch.data) == 2
    assert restored_batch.data[0].symbol == "AAPL"
    assert restored_batch.data[1].symbol == "NVDA"


@pytest.mark.asyncio
async def test_persistence_loop(kv_state_machine):
    """Test background persistence loop."""
    await kv_state_machine.start()
    
    # Add some data
    kv_state_machine.store["AAPL"] = TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890)
    
    # Wait for persistence
    await asyncio.sleep(1.5)  # Wait longer than persistence interval
    
    # Check if file was created
    import os
    assert os.path.exists(kv_state_machine.state_file)
    
    # Verify file contents
    with open(kv_state_machine.state_file, 'r') as f:
        data = json.load(f)
        assert "AAPL" in data["entries"]
        assert data["entries"]["AAPL"]["price"] == 150.0
    
    await kv_state_machine.stop()


@pytest.mark.asyncio
async def test_error_handling(kv_state_machine):
    """Test error handling in state machine."""
    await kv_state_machine.start()
    
    # Test invalid command
    class MockLogEntry:
        def __init__(self, index, term, command_bytes):
            self.index = index
            self.term = term
            self.command_bytes = command_bytes
    
    # Invalid JSON
    entry = MockLogEntry(index=1, term=1, command_bytes=b"invalid json")
    
    with pytest.raises(Exception):
        await kv_state_machine.apply_command(entry)
    
    await kv_state_machine.stop()
