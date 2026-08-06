# Raft-Based Stock Price Replication System - Interview Guide

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture & Components](#architecture--components)
3. [Complete Request Lifecycle](#complete-request-lifecycle)
4. [Performance Optimizations & Bottlenecks](#performance-optimizations--bottlenecks)
5. [Durability & Crash Recovery](#durability--crash-recovery)
6. [Leader Election & Failover](#leader-election--failover)
7. [Consistency Guarantees](#consistency-guarantees)
8. [Observability & Monitoring](#observability--monitoring)
9. [Key Design Decisions & Trade-offs](#key-design-decisions--trade-offs)
10. [Interview Talking Points](#interview-talking-points)

---

## System Overview

### What is This System?
A **production-ready distributed key-value store** for stock price data that uses the **Raft consensus algorithm** to ensure strong consistency, automatic failover, and data durability across a cluster of nodes.

### Core Problem It Solves
- **Distributed Data Replication**: How to keep stock prices consistent across multiple nodes
- **Fault Tolerance**: System must continue operating when nodes crash or network partitions occur
- **Strong Consistency**: All nodes must see the same data in the same order
- **Automatic Recovery**: System must automatically recover from failures without manual intervention

### Why Raft?
- **Understandable**: Simpler than Paxos, easier to reason about
- **Production-Proven**: Used by etcd, Consul, CockroachDB
- **Strong Consistency**: Linearizable reads and writes
- **Leader-Based**: Single leader simplifies consensus (vs. multi-leader approaches)

---

## Architecture & Components

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Layer                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │   CLI Tool  │  │  (Future)   │  │  (Future)   │     │
│  │  (kvctl.py) │  │  Dashboard  │  │  Ingestor  │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
└─────────────────────┬───────────────────────────────────┘
                      │ gRPC (Client API)
┌─────────────────────▼───────────────────────────────────┐
│              gRPC Server Layer                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │  ClientService (grpc_server.py)                 │  │
│  │  - PutPrice, GetPrice, GetClusterInfo, etc.     │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │  RaftService (grpc_server.py)                    │  │
│  │  - RequestVote, AppendEntries (internal RPCs)    │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Raft Node (raft/node.py)                   │
│  ┌──────────────────────────────────────────────────┐  │
│  │  ElectionManager (raft/election.py)              │  │
│  │  - Leader election, timeouts, vote handling     │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │  RaftStorage (raft/storage.py)                  │  │
│  │  - Persistent log, metadata (term, voted_for)   │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │  KVStateMachine (kv/state_machine.py)           │  │
│  │  - Applies committed log entries                │  │
│  │  - In-memory KV store with periodic snapshots  │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### 1. **RaftNode** (`raft/node.py`)
- **Orchestrator**: Coordinates all Raft components
- **State Management**: Tracks current state (FOLLOWER, CANDIDATE, LEADER)
- **Log Replication**: Manages replication to followers
- **Batching**: Batches writes for performance
- **Commit Logic**: Updates commit_index based on majority replication

#### 2. **ElectionManager** (`raft/election.py`)
- **Election Timeouts**: Randomized timeouts (150-300ms) to prevent split votes
- **Vote Requests**: Sends RequestVote RPCs to peers
- **State Transitions**: Manages transitions between FOLLOWER → CANDIDATE → LEADER
- **Heartbeats**: Sends periodic heartbeats when leader

#### 3. **RaftStorage** (`raft/storage.py`)
- **Persistent Log**: Stores all log entries on disk
- **Metadata Persistence**: Stores term, voted_for, commit_index, last_applied
- **Atomic Writes**: Uses temp files + rename + fsync for crash safety
- **Crash Recovery**: Loads state on startup

#### 4. **KVStateMachine** (`kv/state_machine.py`)
- **Command Application**: Applies committed log entries to in-memory store
- **Idempotency**: Safe to apply same entry multiple times
- **Persistence**: Periodic snapshots to disk
- **Replay Logic**: Replays unapplied entries on crash recovery

#### 5. **GrpcServer** (`server/grpc_server.py`)
- **Client API**: Exposes PutPrice, GetPrice, GetClusterInfo, DumpState
- **Raft RPCs**: Handles RequestVote, AppendEntries internally
- **Error Handling**: Returns leader hints when not leader
- **Metrics**: Exposes Prometheus metrics endpoints

---

## Complete Request Lifecycle

### Write Request Flow (PutPrice)

```
1. Client (kvctl.py)
   └─> gRPC PutPrice request to any node
       │
2. ClientService (grpc_server.py)
   └─> Checks if node is leader
       │
       ├─> If NOT leader:
       │   └─> Returns error with leader_hint
       │
       └─> If leader:
           │
3. RaftNode.put_price()
   ├─> Serialize command (symbol, price, timestamp) → JSON bytes
   ├─> Create LogEntry (index, term, command_bytes)
   ├─> Append to local log (RaftStorage.append_entries)
   │   └─> Persists to disk with atomic write + fsync
   ├─> Add to batch queue (_add_to_batch)
   │   └─> Returns success immediately (async replication)
   │
4. Batch Flush (periodic or when batch full)
   └─> _flush_pending_entries()
       │
       ├─> For each follower:
       │   └─> Send AppendEntries RPC (concurrently)
       │       ├─> Includes prev_log_index, prev_log_term
       │       ├─> Follower checks log matching property
       │       └─> If match: append entries, return success
       │
       ├─> Wait for majority acknowledgment
       │   └─> Count successful replications (including self)
       │
       └─> If majority reached:
           │
5. Update Commit Index
   └─> _update_commit_index()
       ├─> Find highest index replicated on majority
       ├─> Only commit entries from current term
       └─> Update commit_index
           │
6. Apply to State Machine
   └─> _apply_committed_entries()
       ├─> For each entry between last_applied and commit_index:
       │   └─> KVStateMachine.apply_command()
       │       ├─> Deserialize command
       │       ├─> Update in-memory store
       │       └─> Update last_applied_index
       └─> Persist last_applied to disk
```

### Read Request Flow (GetPrice)

```
1. Client (kvctl.py)
   └─> gRPC GetPrice request
       │
2. ClientService
   └─> RaftNode.get_price()
       │
3. KVStateMachine.get()
   └─> Read from in-memory store (committed data only)
       └─> Returns TickerPrice or None
```

**Key Point**: Reads are fast (< 10ms) because they read from in-memory state machine, which only contains committed data.

---

## Performance Optimizations & Bottlenecks

### Bottleneck #1: Sequential Replication
**Problem**: Sending AppendEntries to followers one-by-one is slow.

**Solution**: **Concurrent Replication**
- Send AppendEntries to all followers in parallel using `asyncio.gather()`
- Reduces replication latency from O(n) to O(1) where n = number of followers

```python
# From raft/node.py:_replicate_to_peers()
replication_tasks = []
for peer in self.peers:
    task = asyncio.create_task(self._send_append_entries_to_peer(peer, entries))
    replication_tasks.append(task)

results = await asyncio.gather(*replication_tasks, return_exceptions=True)
```

### Bottleneck #2: Per-Entry Network Overhead
**Problem**: Each write creates a separate AppendEntries RPC, causing high network overhead.

**Solution**: **Batching**
- Collect multiple writes into batches
- Flush when batch is full (`RAFT_BATCH_SIZE=10`) or timeout (`RAFT_FLUSH_INTERVAL_MS=50`)
- **2x+ throughput improvement** (from README)

```python
# From raft/node.py
async def _add_to_batch(self, entry: LogEntry) -> None:
    async with self.batch_lock:
        self.pending_entries.append(entry)
        if len(self.pending_entries) >= self.batch_size:
            await self._flush_pending_entries()
```

**Trade-off**: 
- **Latency**: Small batches = lower latency, larger batches = higher latency
- **Throughput**: Larger batches = higher throughput
- **Configurable**: Can tune via environment variables

### Bottleneck #3: Disk I/O for Every Write
**Problem**: fsync on every write is slow but necessary for durability.

**Solution**: **Optimized Persistence Strategy**
- **Raft Log**: Persist immediately (required for correctness)
- **State Machine**: Periodic snapshots (every 5 seconds) + atomic writes
- **Metadata**: Persist on every change (term, commit_index, etc.)

**Why This Works**:
- Raft log is the source of truth - must be durable
- State machine can be reconstructed from log, so periodic snapshots are acceptable
- Reduces fsync calls while maintaining durability

### Bottleneck #4: Follower Catch-Up
**Problem**: Slow followers lag behind and slow down commits.

**Solution**: **Sliding Window Replication**
- Track `next_index` and `match_index` for each follower
- On heartbeat, send missing entries for catch-up
- Automatically decrement `next_index` on failure (retry with earlier entries)

```python
# From raft/node.py:_send_heartbeat_to_peer()
entries_to_send = []
if self.next_index[peer.node_id] <= current_log_length:
    for i in range(self.next_index[peer.node_id], current_log_length + 1):
        entry = self.storage.get_log_entry(i)
        if entry:
            entries_to_send.append(entry)
```

### Performance Characteristics
- **Write Latency**: < 100ms (includes replication to majority)
- **Read Latency**: < 10ms (in-memory lookup)
- **Throughput**: 1000+ ops/second per cluster
- **Recovery Time**: < 5 seconds for follower catch-up

---

## Durability & Crash Recovery

### Durability Mechanisms

#### 1. **Atomic Writes with fsync**
```python
# From raft/storage.py:_atomic_write()
def _atomic_write(self, filepath: str, content: str) -> None:
    temp_file = filepath + ".tmp"
    with open(temp_file, 'w') as f:
        f.write(content)
        f.flush()      # Write to OS buffer
        os.fsync(f.fileno())  # Force to disk
    os.rename(temp_file, filepath)  # Atomic rename
```

**Why Important**:
- **Crash Safety**: If crash occurs during write, old file remains intact
- **Atomicity**: Rename is atomic on most filesystems
- **Durability**: fsync ensures data is on disk, not just in OS buffer

#### 2. **Persistent Raft Log**
- Every log entry is persisted immediately
- Log is append-only (never modified, only truncated on conflicts)
- Contains complete history of all operations

#### 3. **State Machine Snapshots**
- Periodic snapshots (every 5 seconds) of KV store
- Reduces recovery time (don't need to replay entire log)
- Atomic writes ensure snapshot consistency

#### 4. **Metadata Persistence**
- `current_term`, `voted_for`, `commit_index`, `last_applied` persisted on every change
- Critical for crash recovery correctness

### Crash Recovery Process

```
1. Node Restarts
   │
2. RaftStorage._load_metadata()
   └─> Load term, voted_for, commit_index, last_applied
   │
3. RaftStorage._load_log()
   └─> Load all log entries from disk
   │
4. KVStateMachine._load_state()
   └─> Load snapshot if available
   │
5. RaftNode._recover_from_crash()
   └─> Replay unapplied committed entries
       │
       ├─> Find entries between last_applied and commit_index
       ├─> Apply each entry to state machine
       └─> Update last_applied
   │
6. Start Election Timeout
   └─> Node becomes FOLLOWER and waits for leader
```

**Key Insight**: The system can recover from any crash because:
- Log is durable (source of truth)
- State machine can be reconstructed from log
- Metadata ensures we know what was committed vs. applied

---

## Leader Election & Failover

### Election Process

#### 1. **Election Timeout**
- Randomized timeout: 150-300ms (prevents split votes)
- If follower doesn't hear from leader, becomes candidate

```python
# From raft/election.py
timeout_ms = random.randint(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
```

#### 2. **Become Candidate**
- Increment term
- Vote for self
- Send RequestVote to all peers

#### 3. **Vote Granting Rules**
A node grants vote if:
- **Haven't voted** in this term (or voted for same candidate)
- **Candidate's log is at least as up-to-date** (last_log_term and last_log_index check)
- **Term is >= current term**

#### 4. **Election Outcome**
- **Win**: Receive majority votes → become leader
- **Lose**: Receive AppendEntries from new leader → become follower
- **Split Vote**: Timeout → start new election with higher term

### Failover Scenario

```
1. Leader Crashes
   │
2. Followers Don't Receive Heartbeats
   └─> Election timeout expires (150-300ms)
   │
3. First Follower to Timeout Becomes Candidate
   └─> Sends RequestVote RPCs
   │
4. Majority Grants Votes
   └─> New leader elected (1-2 seconds total)
   │
5. New Leader Starts Heartbeats
   └─> Followers reset election timeout
   │
6. New Leader Replicates Pending Entries
   └─> Catches up any missing entries
```

**Failover Time**: 1-2 seconds (election timeout + vote gathering)

---

## Consistency Guarantees

### Strong Consistency (Linearizability)

**Definition**: Operations appear to execute atomically at some point between invocation and response.

**How Raft Achieves This**:

1. **Single Leader**: Only leader accepts writes
2. **Majority Commit**: Entry is committed only when replicated to majority
3. **Log Matching Property**: Follower logs match leader's log up to match_index
4. **State Machine Application**: All nodes apply same entries in same order

### Log Matching Property

**Raft Guarantee**: If two logs contain an entry with the same index and term, then:
- The logs are identical in all preceding entries
- All entries before this index are the same

**How It's Enforced**:
```python
# From raft/node.py:_check_log_matching()
def _check_log_matching(self, prev_log_index: int, prev_log_term: int) -> bool:
    if prev_log_index == 0:
        return True
    entry = self.storage.get_log_entry(prev_log_index)
    return entry and entry.term == prev_log_term
```

If log doesn't match:
- Leader truncates follower's log from mismatch point
- Leader sends all entries from that point forward
- Follower replaces conflicting entries

### Read Consistency

**Current Implementation**: Reads from committed state (strongly consistent)

**Future Optimization**: Could implement:
- **Read-only queries** that don't require leader (with stale read option)
- **Lease-based reads** for better performance

---

## Observability & Monitoring

### Prometheus Metrics

**Exposed on**: `http://localhost:8001/metrics` (per node)

**Key Metrics**:
- `raft_elections_total`: Number of leader elections
- `raft_commits_total`: Number of committed entries
- `raft_replication_latency_ms`: Time to replicate entries
- `raft_crash_recoveries_total`: Recovery operations
- `raft_batch_flushes_total`: Batch flush operations
- `kv_commands_applied_total`: State machine operations

**Why Important**: 
- **Debugging**: Identify performance bottlenecks
- **Alerting**: Detect leader churn, high latency
- **Capacity Planning**: Understand throughput patterns

### Structured Logging

**Format**: JSON with rich context
```json
{
  "timestamp": 1704067200.123,
  "level": "INFO",
  "logger": "raft.node",
  "message": "Batch flush: 5 entries in 15.23ms",
  "context": {
    "node_id": "node1",
    "term": 42,
    "role": "leader",
    "commit_index": 150,
    "batch_size": 10,
    "duration_ms": 15.23
  }
}
```

**Benefits**:
- **Searchable**: Easy to filter by node, term, operation
- **Context-Rich**: Every log includes relevant state
- **Production-Ready**: Can be ingested by log aggregation systems

### State Inspection

**Command**: `kvctl.py dump-state`

**Shows**:
- Node metadata (ID, term, state, commit_index)
- Complete KV store contents
- Performance metrics
- Log statistics

**Use Cases**:
- **Debugging**: Understand node state during issues
- **Verification**: Confirm data consistency across nodes
- **Monitoring**: Track system health

---

## Key Design Decisions & Trade-offs

### 1. **Leader-Only Writes**
**Decision**: Only leader accepts writes

**Pros**:
- Simpler consistency model
- No write conflicts
- Strong consistency guaranteed

**Cons**:
- Leader becomes bottleneck
- Clients must find leader (or get redirected)

**Trade-off**: Chose simplicity and correctness over write availability

### 2. **Immediate Log Persistence**
**Decision**: Persist log entry immediately on leader

**Pros**:
- Crash safety (no data loss)
- Required for Raft correctness

**Cons**:
- Higher latency (fsync is slow)
- More disk I/O

**Trade-off**: Durability over performance (correctness requirement)

### 3. **Periodic State Machine Snapshots**
**Decision**: Snapshot every 5 seconds, not on every write

**Pros**:
- Faster recovery (don't replay entire log)
- Reduced disk I/O

**Cons**:
- Potential data loss if crash between snapshots (but log is source of truth)

**Trade-off**: Performance over immediate persistence (log can reconstruct state)

### 4. **Batching Configuration**
**Decision**: Configurable batch size and flush interval

**Pros**:
- Tunable for workload (latency vs. throughput)
- Environment variable configuration

**Cons**:
- Requires tuning for optimal performance
- Defaults may not suit all workloads

**Trade-off**: Flexibility over one-size-fits-all

### 5. **gRPC for Communication**
**Decision**: Use gRPC instead of HTTP/REST

**Pros**:
- Type-safe (Protobuf)
- Efficient binary protocol
- Built-in streaming support
- Language-agnostic

**Cons**:
- More complex than REST
- Requires code generation

**Trade-off**: Performance and type safety over simplicity

---

## Interview Talking Points

### "Tell me about your Raft project"

**30-Second Summary**:
"I built a distributed key-value store for stock prices using the Raft consensus algorithm. It ensures strong consistency across 3 nodes, automatically handles leader failures, and persists all data to disk. I implemented batching for performance, comprehensive crash recovery, and Prometheus metrics for observability."

### "What was the biggest challenge?"

**Answer Options**:

1. **Performance Optimization**:
   "The biggest challenge was balancing consistency and performance. Initially, each write required waiting for majority replication, which was slow. I solved this by implementing batching - collecting multiple writes and replicating them together, which improved throughput by 2x while maintaining consistency guarantees."

2. **Crash Recovery**:
   "Ensuring correct crash recovery was challenging. I had to implement atomic writes with fsync, track commit_index vs. last_applied, and replay unapplied entries on startup. The key insight was that the log is the source of truth - the state machine can always be reconstructed from it."

3. **Concurrent Replication**:
   "Initially, I was replicating to followers sequentially, which was slow. I optimized this by sending AppendEntries RPCs concurrently using asyncio.gather(), reducing replication latency from O(n) to O(1)."

### "How does your system handle network partitions?"

**Answer**:
"Raft handles partitions through the majority rule. In a 3-node cluster, we need 2 nodes for a majority. If a partition splits the cluster:
- **Partition with majority (2 nodes)**: Continues operating, elects new leader
- **Partition with minority (1 node)**: Cannot commit new entries (no majority), but can serve reads from committed data

This ensures strong consistency - we never have conflicting writes. When the partition heals, the minority node catches up by receiving AppendEntries from the leader."

### "What would you improve?"

**Answer Options**:

1. **Leader Discovery**:
   "Currently, if a client connects to a non-leader, it gets an error with a leader hint, but the client must manually retry. I'd implement automatic leader discovery - the client could query any node to find the current leader."

2. **Read Scaling**:
   "Reads currently go through the leader. I could implement lease-based reads that allow followers to serve reads without contacting the leader, improving read throughput."

3. **Snapshot Compression**:
   "The state machine snapshots are stored as JSON. I could compress them or use a more efficient format to reduce disk usage and recovery time."

4. **Dynamic Membership**:
   "Currently, cluster membership is static. I'd add support for adding/removing nodes dynamically, which requires implementing the Raft membership change protocol."

### "How do you ensure data consistency?"

**Answer**:
"I ensure consistency through multiple mechanisms:

1. **Single Leader**: Only the leader accepts writes, preventing conflicts
2. **Majority Commit**: Entries are committed only when replicated to majority
3. **Log Matching Property**: Follower logs are guaranteed to match leader's log up to match_index
4. **Atomic Writes**: All disk writes use atomic operations (temp file + rename + fsync)
5. **State Machine Application**: All nodes apply the same entries in the same order

Together, these guarantee linearizability - operations appear to execute atomically."

### "What metrics do you track?"

**Answer**:
"I track comprehensive metrics via Prometheus:

- **Election metrics**: Number of elections, election duration
- **Replication metrics**: Entries replicated, replication latency, failures
- **Commit metrics**: Commits total, commit latency
- **Recovery metrics**: Crash recoveries, replay entries, snapshot load time
- **Storage metrics**: Log entries, storage reads/writes
- **State machine metrics**: Commands applied, KV entries

These metrics help identify bottlenecks, detect leader churn, and understand system performance under load."

### "How does batching work?"

**Answer**:
"Batching improves throughput by reducing network overhead. When a write comes in:
1. It's added to a pending batch queue
2. The batch flushes when either:
   - It reaches the configured size (default 10 entries)
   - Or the flush interval expires (default 50ms)
3. All entries in the batch are replicated together in a single AppendEntries RPC
4. The batch is committed when majority acknowledges

This reduces the number of RPCs from N (one per write) to N/batch_size, significantly improving throughput. The trade-off is slightly higher latency for individual writes, but this is configurable."

---

## Quick Reference: Key Numbers

- **Nodes**: 3-node cluster (tolerates 1 failure)
- **Election Timeout**: 150-300ms (randomized)
- **Heartbeat Interval**: 75ms
- **Batch Size**: 10 entries (configurable)
- **Flush Interval**: 50ms (configurable)
- **Write Latency**: < 100ms
- **Read Latency**: < 10ms
- **Throughput**: 1000+ ops/second
- **Failover Time**: 1-2 seconds
- **Recovery Time**: < 5 seconds

---

## Conclusion

This system demonstrates:
- **Deep understanding** of distributed systems concepts
- **Production-ready** implementation with durability, recovery, and observability
- **Performance optimization** through batching and concurrent replication
- **Comprehensive testing** with 71 tests covering various scenarios
- **Real-world considerations** like metrics, logging, and chaos testing

**Key Strengths for Interview**:
1. Can explain every design decision
2. Understands trade-offs (consistency vs. performance, durability vs. speed)
3. Implemented optimizations to solve real bottlenecks
4. Built observability for production debugging
5. Handles edge cases (crashes, partitions, concurrent writes)
