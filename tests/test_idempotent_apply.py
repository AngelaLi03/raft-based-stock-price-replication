"""
Tests for idempotent command application.
"""

import pytest
import asyncio
import tempfile
from unittest.mock import MagicMock

from kv.state_machine import KVStateMachine, serialize_put_command, serialize_batch_put_command
from raft.types import LogEntry


@pytest.fixture
def temp_data_dir():
    """Create temporary data directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.mark.asyncio
async def test_put_command_idempotency(temp_data_dir):
    """Test that PUT commands are idempotent."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create a log entry
    entry = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    
    # Apply the same entry multiple times
    await kv.apply_command(entry)
    await kv.apply_command(entry)
    await kv.apply_command(entry)
    
    # Verify only one entry was applied
    assert len(kv.store) == 1
    assert "AAPL" in kv.store
    assert kv.store["AAPL"].price == 150.0
    assert kv.store["AAPL"].timestamp == 1234567890
    assert kv.last_applied_index == 1
    
    await kv.stop()


@pytest.mark.asyncio
async def test_batch_put_command_idempotency(temp_data_dir):
    """Test that BATCH_PUT commands are idempotent."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create a batch log entry
    from kv.state_machine import TickerPrice
    ticker_prices = [
        TickerPrice(symbol="AAPL", price=150.0, timestamp=1234567890),
        TickerPrice(symbol="NVDA", price=800.0, timestamp=1234567891)
    ]
    
    entry = LogEntry(index=1, term=1, command_bytes=serialize_batch_put_command(ticker_prices))
    
    # Apply the same entry multiple times
    await kv.apply_command(entry)
    await kv.apply_command(entry)
    await kv.apply_command(entry)
    
    # Verify only one batch was applied
    assert len(kv.store) == 2
    assert "AAPL" in kv.store
    assert "NVDA" in kv.store
    assert kv.store["AAPL"].price == 150.0
    assert kv.store["NVDA"].price == 800.0
    assert kv.last_applied_index == 1
    
    await kv.stop()


@pytest.mark.asyncio
async def test_apply_skips_already_applied_entries(temp_data_dir):
    """Test that apply skips entries that have already been applied."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Set last_applied_index to 2
    kv.last_applied_index = 2
    
    # Create entries with different indices
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891))
    entry3 = LogEntry(index=3, term=1, command_bytes=serialize_put_command("MSFT", 300.0, 1234567892))
    
    # Apply entries 1 and 2 (should be skipped)
    await kv.apply_command(entry1)
    await kv.apply_command(entry2)
    
    # Verify entries 1 and 2 were skipped
    assert len(kv.store) == 0
    assert kv.last_applied_index == 2
    
    # Apply entry 3 (should be applied)
    await kv.apply_command(entry3)
    
    # Verify entry 3 was applied
    assert len(kv.store) == 1
    assert "MSFT" in kv.store
    assert kv.store["MSFT"].price == 300.0
    assert kv.last_applied_index == 3
    
    await kv.stop()


@pytest.mark.asyncio
async def test_replay_log_entries_idempotency(temp_data_dir):
    """Test that replaying log entries is idempotent."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create multiple log entries
    entries = [
        LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890)),
        LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891)),
        LogEntry(index=3, term=1, command_bytes=serialize_put_command("MSFT", 300.0, 1234567892))
    ]
    
    # Replay entries multiple times
    await kv.replay_log_entries(entries)
    await kv.replay_log_entries(entries)
    await kv.replay_log_entries(entries)
    
    # Verify entries were only applied once
    assert len(kv.store) == 3
    assert "AAPL" in kv.store
    assert "NVDA" in kv.store
    assert "MSFT" in kv.store
    assert kv.last_applied_index == 3
    
    await kv.stop()


@pytest.mark.asyncio
async def test_replay_with_start_index(temp_data_dir):
    """Test replaying log entries with a start index."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Set last_applied_index to 1
    kv.last_applied_index = 1
    
    # Create multiple log entries
    entries = [
        LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890)),
        LogEntry(index=2, term=1, command_bytes=serialize_put_command("NVDA", 800.0, 1234567891)),
        LogEntry(index=3, term=1, command_bytes=serialize_put_command("MSFT", 300.0, 1234567892))
    ]
    
    # Replay starting from index 2
    await kv.replay_log_entries(entries, start_index=2)
    
    # Verify only entries 2 and 3 were applied
    assert len(kv.store) == 2
    assert "NVDA" in kv.store
    assert "MSFT" in kv.store
    assert "AAPL" not in kv.store
    assert kv.last_applied_index == 3
    
    await kv.stop()


@pytest.mark.asyncio
async def test_apply_with_overlapping_entries(temp_data_dir):
    """Test applying overlapping entries (same symbol, different values)."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create entries with same symbol but different values
    entry1 = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    entry2 = LogEntry(index=2, term=1, command_bytes=serialize_put_command("AAPL", 160.0, 1234567891))
    
    # Apply first entry
    await kv.apply_command(entry1)
    
    # Verify first value
    assert len(kv.store) == 1
    assert "AAPL" in kv.store
    assert kv.store["AAPL"].price == 150.0
    assert kv.last_applied_index == 1
    
    # Apply second entry (should update the value)
    await kv.apply_command(entry2)
    
    # Verify second value (should overwrite first)
    assert len(kv.store) == 1
    assert "AAPL" in kv.store
    assert kv.store["AAPL"].price == 160.0
    assert kv.store["AAPL"].timestamp == 1234567891
    assert kv.last_applied_index == 2
    
    await kv.stop()


@pytest.mark.asyncio
async def test_apply_with_mixed_command_types(temp_data_dir):
    """Test applying mixed command types (PUT and BATCH_PUT)."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create PUT entry
    put_entry = LogEntry(index=1, term=1, command_bytes=serialize_put_command("AAPL", 150.0, 1234567890))
    
    # Create BATCH_PUT entry
    from kv.state_machine import TickerPrice
    ticker_prices = [
        TickerPrice(symbol="NVDA", price=800.0, timestamp=1234567891),
        TickerPrice(symbol="MSFT", price=300.0, timestamp=1234567892)
    ]
    batch_entry = LogEntry(index=2, term=1, command_bytes=serialize_batch_put_command(ticker_prices))
    
    # Apply both entries
    await kv.apply_command(put_entry)
    await kv.apply_command(batch_entry)
    
    # Verify all entries were applied
    assert len(kv.store) == 3
    assert "AAPL" in kv.store
    assert "NVDA" in kv.store
    assert "MSFT" in kv.store
    assert kv.store["AAPL"].price == 150.0
    assert kv.store["NVDA"].price == 800.0
    assert kv.store["MSFT"].price == 300.0
    assert kv.last_applied_index == 2
    
    await kv.stop()


@pytest.mark.asyncio
async def test_apply_error_handling(temp_data_dir):
    """Test error handling during command application."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create invalid log entry
    invalid_entry = LogEntry(index=1, term=1, command_bytes=b"invalid json")
    
    # Apply invalid entry (should raise exception)
    with pytest.raises(Exception):
        await kv.apply_command(invalid_entry)
    
    # Verify state machine is still in valid state
    assert len(kv.store) == 0
    assert kv.last_applied_index == 0
    
    await kv.stop()


@pytest.mark.asyncio
async def test_apply_with_unknown_command_type(temp_data_dir):
    """Test applying unknown command types."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create entry with unknown command type
    unknown_command = {
        "type": "UNKNOWN_COMMAND",
        "data": {"test": "data"}
    }
    
    import json
    entry = LogEntry(index=1, term=1, command_bytes=json.dumps(unknown_command).encode('utf-8'))
    
    # Apply unknown command (should raise exception)
    with pytest.raises(ValueError, match="Unknown command type"):
        await kv.apply_command(entry)
    
    # Verify state machine is still in valid state
    assert len(kv.store) == 0
    assert kv.last_applied_index == 0  # Index should not be updated due to exception
    
    await kv.stop()


@pytest.mark.asyncio
async def test_apply_with_empty_batch(temp_data_dir):
    """Test applying empty batch commands."""
    kv = KVStateMachine(temp_data_dir)
    await kv.start()
    
    # Create empty batch entry
    empty_batch_entry = LogEntry(index=1, term=1, command_bytes=serialize_batch_put_command([]))
    
    # Apply empty batch
    await kv.apply_command(empty_batch_entry)
    
    # Verify no entries were added
    assert len(kv.store) == 0
    assert kv.last_applied_index == 1
    
    await kv.stop()
