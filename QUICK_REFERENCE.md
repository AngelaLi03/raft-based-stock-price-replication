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

- Elections, election duration, commits, replication latency
- Node role (follower/candidate/leader), current term
- Crash recoveries, batch flushes
- Storage reads/writes, commands applied

## 📊 Monitoring Stack (Grafana + Prometheus)

- **Separate compose file** (`docker-compose.monitoring.yml`), provisioned entirely as code — datasource, dashboard, alerts, no manual UI setup
- **Prometheus** `:9090` scrapes all nodes; **Grafana** `:3000` (`admin`/`admin`)
- **6-panel dashboard**, 5s refresh: role per node, term per node, leader changes, election duration, per-follower commit lag, quorum health
- **2 alerts**, visual-only: no leader >10s, leader flapping >2 changes/60s
- **Key gotcha**: `count(x == 2)` returns *no data* (not 0) on zero matches — used `sum(x == bool 2)` instead so the no-leader alert can actually fire on the case it exists for
- **`scripts/dashboard_demo.py`**: narrated live demo — failover, quorum loss, flapping — watchable on the dashboard in real time

## ☸️ Kubernetes Deployment

- **Generated** by `scripts/gen_k8s_manifests.py --nodes 5 > ops/k8s/raft-cluster.yaml` (mirrors `gen_docker_compose.py`) — 5 manifests: headless Service (`raft`, per-pod DNS), ClusterIP Service (`raft-client`), ConfigMap (`PEER_LIST`), PodDisruptionBudget (`raft-pdb`), StatefulSet (`raft-node`)
- **Key commands**: `kind create cluster --name raft` → `docker build -f ops/Dockerfile -t raft-node:latest .` → `kind load docker-image raft-node:latest --name raft` (required — kind nodes can't see the host Docker daemon; `imagePullPolicy: Never` means skipping this = `ErrImageNeverPull`) → `kubectl apply -f ops/k8s/raft-cluster.yaml`
- **Quorum/PDB relationship**: `minAvailable` is generated as `quorum(N)` — the same `(N//2)+1` formula Raft's own commit logic uses. For 5 nodes: `minAvailable: 3`. Live-verified: a 3rd back-to-back pod eviction was refused — `Cannot evict pod as it would violate the pod's disruption budget` — and a full rolling restart never dropped ready pods below 4/5
- **StatefulSet, not Deployment**: pods need stable ordinal names (`raft-node-0`..`raft-node-4`) and per-pod PVCs (`volumeClaimTemplates`) that a restarted pod reattaches — verified live, a deleted pod came back with the same name and its prior term/log/KV data intact
- **Identity via downward API**: `POD_NAME` comes from `fieldRef: metadata.name`, not hardcoded — one pod template is shared by all N pods, unlike Compose's N separate service blocks
- **N=5 local default** (Compose defaults to 15)

## 🌪️ Chaos-Testing Bugs Found (Live 15-Node Cluster)

- **No RPC had a `timeout=`** → a peer accepting-but-not-responding (mid-restart) hung the caller forever → since election/heartbeat loops `gather()` every peer's RPC before proceeding, one stuck peer froze the *entire* loop, not just that peer. A candidate stuck 6+ minutes; a leader's heartbeat loop went silent mid-term. Fixed: `RPC_TIMEOUT_SECONDS = 2.0` on every outbound RPC, client and server.
- **Heartbeat loop cadence wasn't flat 75ms** → it awaited every peer via `gather()` before sleeping, so one down peer's failure latency delayed heartbeats to *every* peer. With `ELECTION_TIMEOUT_MIN` only 2× the heartbeat interval, this triggered a real election storm (term +100 in minutes) with one node down. Fixed: fire-and-forget each peer's heartbeat instead of awaiting before sleep.
- **A write could ack `ok=True` and then vanish** → commit-index update silently no-ops once no longer leader, but the confirmation future was resolved from the replication-ACK count alone, not from whether commit actually happened. Stepping down in that window let an entry look "successful" while never being committed anywhere — the next leader could freely overwrite it. Fixed: check `state == LEADER` after the commit-index update before reporting success.
- **Key pattern**: all three assumed a failing peer always raises an exception. A mock can't reproduce "accepts the connection, then says nothing" — only a real container mid-restart can, which is exactly why these needed the live cluster, not more unit tests, to find.

## 🎤 Common Interview Questions

### "What was the biggest challenge?"
**Answer**: "Balancing consistency and performance. I solved it with batching - collecting multiple writes and replicating them together, improving throughput 2x while maintaining strong consistency."

### "How does it handle network partitions?"
**Answer**: "Raft uses majority rule. In a 3-node cluster, we need 2 for majority. The partition with majority continues operating; the minority cannot commit new entries but can serve reads. This prevents conflicting writes."

### "What would you improve?"
**Answer**: "1) Automatic leader discovery for clients, 2) Lease-based reads for better read scaling, 3) Dynamic membership for adding/removing nodes."

### "How do you ensure consistency?"
**Answer**: "Single leader for writes, majority commit rule, log matching property enforced, atomic disk writes, and all nodes apply same entries in same order."

### "How do you monitor it?"
**Answer**: "Grafana + Prometheus, provisioned as code. Found a real bug testing the alerting itself: `count(role==2)` returns no data (not 0) when no leader exists, which Grafana treats as a separate non-firing state — fixed with `sum(role == bool 2)` so it always returns a real number. I don't trust an alert until I've actually stopped a node and watched it fire and resolve."

### "How do you deploy a consensus system without downtime?"
**Answer**: "The deployment layer has to know the same quorum math the algorithm does. My PodDisruptionBudget's `minAvailable` is generated as `quorum(N)` — same formula Raft uses to commit — so a rolling update or node drain can never be allowed to drop below majority. Live-verified: a 3rd eviction was refused by the API server directly, and a full rolling restart never dropped ready pods below 4 of 5."

### "What's a bug unit tests couldn't have caught?"
**Answer**: "None of my gRPC calls had a timeout, so a container mid-restart — TCP connected, not yet responding — hung the caller forever instead of erroring, and since my election/heartbeat loops wait on every peer via `gather()`, one stuck peer froze the whole loop. Fixed with a 2-second RPC deadline everywhere. A mock can't reproduce that failure mode — it either returns or raises — so this only showed up running `chaos_test.py` against real Docker containers."

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

- **194 tests** covering:
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
- **Observability**:** "Prometheus metrics and structured logging enable production debugging, and a Grafana dashboard + alert rules (provisioned as code, live-verified by actually triggering them) mean issues get noticed automatically, not just debuggable after the fact."
- **Consistency**:** "Strong consistency is critical for financial data. Raft's linearizability guarantees ensure all nodes see the same data in the same order."

---

**Full Details**: See `INTERVIEW_GUIDE.md` for comprehensive explanations.
