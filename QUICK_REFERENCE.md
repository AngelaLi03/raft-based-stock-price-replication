# Quick Reference - Interview Prep

## 🎯 30-Second Elevator Pitch

"I built a distributed key-value store for stock prices using Raft consensus. It ensures strong consistency across 3 nodes, handles leader failures automatically, and persists all data. I optimized it with batching (2x throughput), implemented comprehensive crash recovery, and added Prometheus metrics for observability."

## 📊 System at a Glance

| Aspect | Details |
|--------|---------|
| **Algorithm** | Raft Consensus |
| **Nodes** | 3-node cluster (tolerates 1 failure) |
| **Consistency** | Strong (Linearizable) |
| **Write Latency** | < 100ms |
| **Read Latency** | < 10ms |
| **Throughput** | 1000+ ops/sec |
| **Failover Time** | 1-2 seconds |
| **Recovery Time** | < 5 seconds |

## 🔄 Request Flow (Write)

```
Client → gRPC → ClientService → RaftNode.put_price()
  → Serialize command → Create LogEntry
  → Append to local log (persist to disk)
  → Add to batch queue → Return success
  → [Background] Flush batch when full/timeout
  → Replicate to followers (concurrent)
  → Wait for majority → Update commit_index
  → Apply to state machine → Done
```

## 🔄 Request Flow (Read)

```
Client → gRPC → ClientService → RaftNode.get_price()
  → KVStateMachine.get() → In-memory lookup → Return
```

## 🚀 Key Optimizations

1. **Batching**: Collect 10 writes, replicate together (2x throughput)
2. **Concurrent Replication**: Send to all followers in parallel (O(1) latency)
3. **Periodic Snapshots**: State machine snapshots every 5s (faster recovery)
4. **Sliding Window**: Automatic follower catch-up on heartbeats

## 🛡️ Durability Mechanisms

1. **Atomic Writes**: Temp file → fsync → atomic rename
2. **Immediate Log Persistence**: Every log entry persisted immediately
3. **Periodic State Snapshots**: Every 5 seconds
4. **Metadata Persistence**: term, voted_for, commit_index, last_applied

## 🔧 Crash Recovery

1. Load metadata (term, commit_index, last_applied)
2. Load log entries
3. Load state machine snapshot
4. Replay unapplied committed entries
5. Start as follower

## 👑 Leader Election

1. **Timeout**: 150-300ms (randomized)
2. **Become Candidate**: Increment term, vote for self
3. **Request Votes**: Send to all peers
4. **Grant Vote If**: Haven't voted AND candidate's log is up-to-date
5. **Win**: Majority votes → become leader
6. **Failover Time**: 1-2 seconds

## ✅ Consistency Guarantees

- **Single Leader**: Only leader accepts writes
- **Majority Commit**: Entry committed when replicated to majority
- **Log Matching**: Follower logs match leader up to match_index
- **Same Order**: All nodes apply same entries in same order

## 📈 Metrics Tracked

- Elections, commits, replication latency
- Crash recoveries, batch flushes
- Storage reads/writes, commands applied

## 🎤 Common Interview Questions

### "What was the biggest challenge?"
**Answer**: "Balancing consistency and performance. I solved it with batching - collecting multiple writes and replicating them together, improving throughput 2x while maintaining strong consistency."

### "How does it handle network partitions?"
**Answer**: "Raft uses majority rule. In a 3-node cluster, we need 2 for majority. The partition with majority continues operating; the minority cannot commit new entries but can serve reads. This prevents conflicting writes."

### "What would you improve?"
**Answer**: "1) Automatic leader discovery for clients, 2) Lease-based reads for better read scaling, 3) Dynamic membership for adding/removing nodes."

### "How do you ensure consistency?"
**Answer**: "Single leader for writes, majority commit rule, log matching property enforced, atomic disk writes, and all nodes apply same entries in same order."

## 🔑 Key Design Decisions

| Decision | Rationale |
|----------|----------|
| Leader-only writes | Simpler consistency, no conflicts |
| Immediate log persistence | Required for correctness |
| Periodic state snapshots | Performance (log can reconstruct) |
| Configurable batching | Tune for workload (latency vs throughput) |
| gRPC over REST | Type safety, efficiency, streaming |

## 📝 Code Locations

- **Main Raft Logic**: `raft/node.py`
- **Election**: `raft/election.py`
- **Storage**: `raft/storage.py`
- **State Machine**: `kv/state_machine.py`
- **gRPC Services**: `server/grpc_server.py`
- **CLI Tool**: `scripts/kvctl.py`

## 🧪 Testing Coverage

- **71 tests** covering:
  - Leader election scenarios
  - Log replication
  - Crash recovery
  - Persistence
  - Batch replication
  - Chaos testing (leader crash, follower catch-up, concurrent writes)
  - Idempotent state machine application

## 💡 Key Insights to Remember

1. **Log is source of truth** - State machine can always be reconstructed
2. **Majority = consistency** - Need majority to commit, prevents conflicts
3. **Batching = performance** - Reduces network overhead significantly
4. **Atomic writes = safety** - Temp file + fsync + rename prevents corruption
5. **Concurrent replication = speed** - Parallel AppendEntries reduces latency

## 🎯 Amazon-Specific Talking Points

- **Scalability**:** "The system can scale horizontally by adding more nodes. Batching ensures we maintain performance as cluster size grows."
- **Reliability**:** "Comprehensive crash recovery ensures zero data loss. Atomic writes and fsync guarantee durability even during power failures."
- **Observability**:** "Prometheus metrics and structured logging enable production debugging. We can identify bottlenecks and detect issues before they impact users."
- **Consistency**:** "Strong consistency is critical for financial data. Raft's linearizability guarantees ensure all nodes see the same data in the same order."

---

**Full Details**: See `INTERVIEW_GUIDE.md` for comprehensive explanations.
