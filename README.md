# Raft-based Stock Price Replication System

A distributed key-value store that implements the [Raft consensus algorithm](https://raft.github.io/raft.pdf) from scratch, specialized for replicating stock ticker prices across a cluster (15 nodes by default, configurable). Built with Python, asyncio, and gRPC.

## Why this project is meaningful

Raft is the algorithm behind the consistency guarantees of systems like etcd (Kubernetes' backing store), CockroachDB, and Consul. Building it from scratch — rather than using an off-the-shelf library — is what makes this project worth having on a resume or talking through in an interview: it demonstrates hands-on understanding of leader election, log replication, quorum-based commitment, crash recovery, and the subtle correctness properties (log matching, leader completeness) that distributed consensus depends on. The stock-price domain is just a vehicle: a plausible, understandable use case (financial data that must not be lost or corrupted, even during node failures) for exercising the consensus layer.

## Tech Stack

- **Python 3.11**, `asyncio` for concurrency
- **gRPC** (`grpc.aio`) for both internal Raft RPCs (RequestVote, AppendEntries) and the external client API
- **Protocol Buffers** for wire format / service definitions (`proto/raft.proto`, `proto/client.proto`)
- **Docker Compose** for local cluster orchestration (15-node by default, generated — see `scripts/gen_docker_compose.py`)
- **Prometheus client** for metrics export (added in the in-progress Week 4 work)
- **pytest** + `pytest-asyncio` for the test suite (148 tests, ~3,900 lines across 14 files)

## Concurrency Model

The gRPC servers run on `grpc.aio` (asyncio), **not** a threaded `grpc.server` + `ThreadPoolExecutor`. This is a deliberate choice, not a gap: Raft's correctness depends on state mutations (`term`, `voted_for`, the log, `commit_index`) being effectively serialized between the points where a decision is made and where it's persisted. Asyncio's single-threaded event loop gives that for free — control only switches at `await` points — while `asyncio.gather` still fans out vote requests and `AppendEntries` calls to all peers concurrently for the I/O-bound parts (waiting on the network). A genuinely multithreaded server would need explicit locks around every one of those state mutations to get the same safety back, with no real throughput win for a workload this I/O-bound. If you're describing this project's concurrency, "concurrent, asyncio-based gRPC server — elections, heartbeats, and client requests handled via non-blocking coroutines with parallel RPC fan-out" is accurate; "multithreaded server" is not.

## Core Theory Implemented

- **Leader election**: randomized election timeouts (150–300ms), term-based voting, majority quorum
- **Log replication**: leader-driven `AppendEntries`, log matching / conflict truncation, `next_index`/`match_index` tracking per follower
- **Commit rule**: an entry commits only once replicated to a majority *and* it's from the leader's current term (the classic Raft safety subtlety)
- **Durability**: log entries and metadata (term, voted_for, commit_index) are fsync'd to disk with atomic temp-file-then-rename writes
- **Crash recovery**: on restart, a node reloads persisted state and replays committed-but-unapplied log entries into the state machine before rejoining the cluster
- **Follower catch-up**: lagging followers receive missing entries piggybacked on heartbeats
- **Batching**: writes are buffered and flushed as a batch (by size or timer) rather than replicated one-by-one; `put_price` awaits its own entry's flush outcome, so batching doesn't sacrifice the "the client is told the truth" guarantee for throughput
- **Log snapshotting / compaction** (Raft §7): once `last_applied` has advanced `RAFT_SNAPSHOT_THRESHOLD` entries past the last snapshot, the leader checkpoints the state machine and compacts the log up to that point. A leader whose compacted log no longer contains what a lagging follower needs sends the checkpoint wholesale via `InstallSnapshot` instead of `AppendEntries`. Without this, the log grows without bound for the life of the cluster.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Applications                      │
│              (scripts/kvctl.py — CLI only, today)               │
└─────────────────────┬───────────────────────────────────────────┘
                      │ gRPC Client API
┌─────────────────────▼───────────────────────────────────────────┐
│                 Raft Cluster (15 nodes, default)                │
│  ┌─────────────┐    ┌─────────────┐          ┌─────────────┐   │
│  │   Node 1    │◄──►│   Node 2    │◄── ... ─►│   Node 15   │   │
│  │             │    │             │          │             │   │
│  │ ┌─────────┐ │    │ ┌─────────┐ │          │ ┌─────────┐ │   │
│  │ │RaftNode │ │    │ │RaftNode │ │          │ │RaftNode │ │   │
│  │ └─────────┘ │    │ └─────────┘ │          │ └─────────┘ │   │
│  │ ┌─────────┐ │    │ ┌─────────┐ │          │ ┌─────────┐ │   │
│  │ │KV Store │ │    │ │KV Store │ │          │ │KV Store │ │   │
│  │ └─────────┘ │    │ └─────────┘ │          │ └─────────┘ │   │
│  │ ┌─────────┐ │    │ ┌─────────┐ │          │ ┌─────────┐ │   │
│  │ │Storage  │ │    │ │Storage  │ │          │ │Storage  │ │   │
│  │ └─────────┘ │    │ └─────────┘ │          │ └─────────┘ │   │
│  └─────────────┘    └─────────────┘          └─────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

Node count is generated, not hand-maintained — see `scripts/gen_docker_compose.py` (`--nodes N`). Every peer fans out RPCs concurrently via `asyncio.gather`, so wall-clock cost per heartbeat/election round is dominated by the slowest peer, not the peer count.

## Current Status (as of 2026-08-05)

**Git history only covers Weeks 1–3** (skeleton → Docker → leader election → log replication → durability/crash recovery/follower catch-up, 5 commits, last one "Fixed issue with follower catch-up"). Everything below described as "Week 4" — batching, Prometheus metrics, structured logging, chaos testing, benchmarking, snapshotting, and the 15-node scale-out — exists only as **uncommitted working-tree changes**, never committed. This README itself was previously uncommitted/aspirational and has been rewritten to match the actual code.

### Solid and working
- Leader election (now with the log up-to-date safety check — see "Fixed this session" #1 below), heartbeats, log replication, majority commit
- Durable, fsync'd log + metadata storage with atomic writes
- Log snapshotting/compaction (`RAFT_SNAPSHOT_THRESHOLD`) with `InstallSnapshot` RPC for catching up followers whose needed entries have been compacted away
- Crash recovery (replays unapplied committed entries on startup, or restores directly from a snapshot if the entries it would need were compacted)
- Follower catch-up via heartbeat piggybacking
- Batching with real per-write confirmation: `put_price` awaits its own entry's flush outcome rather than acking optimistically
- KV state machine (`kv/state_machine.py`) — in-memory store with periodic snapshotting, idempotent command application
- `PutPrice` / `BatchPutPrice` / `GetPrice` / `GetClusterInfo` — implemented and replicated correctly
- 15-node cluster (`scripts/gen_docker_compose.py --nodes N`), verified live via `docker compose up` — see "15-Node Verification" below
- **Full test suite passes**: 148 tests (129 pre-existing/fixed + 11 for snapshotting + 3 for BatchPutPrice + 5 for the gRPC/metrics-server layer, which had zero coverage before this session), verified by actually running `pytest` after installing `requirements.txt` into a working venv — not just read for plausibility

### Fixed this session
1. **Leader-election safety gap** (was the single highest-priority correctness bug): vote granting now checks that the candidate's log is at least as up-to-date as the voter's before granting (`raft/election.py:_is_log_up_to_date`, implementing Raft §5.4.1), and candidates advertise their real `last_log_index`/`last_log_term` (from `RaftNode._get_last_log_info`, backed by storage) instead of hardcoded zeros.
2. **`node.py` called `self.storage.truncate_log(...)`, a method that didn't exist** — now calls the real `truncate_log_from(entries_to_flush[0].index)`, which also fixes an off-by-one that would have deleted the last committed entry before a failed batch, not just the failed batch itself.
3. **`put_price` acked writes before replication was confirmed**: under batching, a write could return `ok=True` and then genuinely fail to replicate — found via `test_network_partition_recovery`. `put_price` now awaits a per-entry future that resolves once its batch actually flushes, so the client is told the truth; concurrent writers still batch together for throughput.
4. **`_on_become_follower` never cleared `pending_entries`** on step-down, which (after fix #3) would have left any caller awaiting a queued write's future hanging forever. Now resolves them to `False` and clears the queue.
5. **Prometheus ports weren't published** in `docker-compose.yml`/`Dockerfile` — the generated compose file now maps each node's metrics port to the host.
6. **Concurrency claim corrected** — see "Concurrency Model" above.
7. Regenerated the committed `_pb2.py` files, which were built with a `protoc` far newer than the pinned `grpcio-tools==1.59.0` in `requirements.txt` and failed to import at all under the pinned toolchain.
8. **8 real bugs in the "Week 4" test files themselves**, found while getting the suite to actually pass rather than just read as plausible: stale mocks that didn't reflect state changes made during the call under test (`test_follower_catchup_after_restart`), mocking the very method whose internal side effect was being asserted on (`test_metrics_recording_during_chaos`), wrong expected counts not matching the test's own setup (`test_crash_recovery_with_partial_commits`), `Histogram` metrics asserted via the `Counter`/`Gauge`-only `._value` API instead of `._sum` (5 assertions across `test_performance_metrics.py`), and an "isolation" test that compared two registries' exports before either had any recorded data (so they were trivially identical).
9. **Commit index could get stuck after a leader failover** — found via live testing on the 15-node cluster, not by unit tests. `_update_commit_index()` was only ever called from the batch-flush path; a follower that instead caught up via the heartbeat path (the normal case right after a new leader takes over and has to reconcile `next_index` with everyone) never triggered a re-check. A write could return `ok=True`, replicate everywhere, and then sit uncommitted on the leader — invisible to some followers' reads — until some *later*, unrelated write happened to re-trigger the check and retroactively commit it. Reproduced live: killed the leader, wrote through the new one, and `GetPrice` on the farthest follower returned "not found" for a write the leader had already acked. Fixed by calling `_update_commit_index()` after heartbeat-driven catch-up too (`raft/node.py:_send_heartbeat_to_peer`); re-verified live afterward — commit index now advances immediately in the same scenario.
10. **`BatchPutPrice` was a non-functional stub**: the gRPC handler returned `ok=True` without touching the request payload — batch writes silently did nothing, even though the KV state machine's `BATCH_PUT` apply logic was already correct and ready. Implemented `RaftNode.batch_put_price()` (mirrors `put_price`, but serializes the whole batch as a single log entry via `serialize_batch_put_command` rather than one entry per price), wired it through `ClientService.BatchPutPrice`, and added the `kvctl.py batch-put-price "SYM:PRICE,SYM:PRICE"` subcommand the README's own Quick Start had been implying worked all along.
11. **Metrics were split across two disconnected systems, and the redundant one caused a port collision**: `raft/prometheus_metrics.py` started its own HTTP server on the metrics port *and* `server/metrics_server.py` (aiohttp) tried to bind the same port right after — the second bind failed silently, taking `/health` down with it. Meanwhile `dump-state` read from the old `raft/metrics.py` collector, which nothing had updated since the Prometheus-based rewrite, so it always showed 0s under real traffic. Fixed by removing the redundant `start_http_server()` call (`metrics_server.py` already serves this registry's `/metrics` plus `/health` on the same port — one server, not two), pointing `dump_state()` at the live `PrometheusMetrics.get_metrics_dict()` instead, and deleting `raft/metrics.py` entirely now that nothing referenced it. Verified live: `dump-state` shows real, non-zero counts after real writes.
12. **Two more bugs the port collision had been hiding**, both only surfaced once `metrics_server.py` could actually bind and receive real requests for the first time: (a) `dump-state`'s protobuf conversion crashed with `'float' object cannot be interpreted as an integer` — `prometheus_client`'s `Counter`/`Gauge` values are always Python floats internally regardless of what was recorded, and assigning one into a protobuf `uint64` field raises. Fixed with explicit `int(...)` on every `*_total` field in `server/grpc_server.py`'s `DumpState`. (b) `/metrics` returned HTTP 500 — aiohttp's `web.Response` rejects a charset embedded inside `content_type` when `text=` is also given (`'charset must not be in content_type argument'`); fixed by dropping the embedded `;charset=utf-8` and letting aiohttp append it automatically. Neither had any test coverage before this session — added `tests/test_grpc_server.py` and `tests/test_metrics_server.py`, the first tests either file has ever had.

### Known issues still open
1. **No leader-hint on redirect**: `put_price()`/`batch_put_price()` never set `leader_hint` when returning "not leader" (`node.py`, a no-op `# TODO` loop) — clients can't be automatically redirected to the current leader.
2. **`scripts/benchmark.py` can't run**: imports `from client.client import RaftClient`, but that module doesn't exist — the only real client implementation lives inside `scripts/kvctl.py`.
3. **`scripts/chaos_test.py` always targets `node1`**: its node-name parsing logic (`_stop_node`/`_start_node`) reduces any `localhost:PORT` address to an empty string and falls back to a hardcoded default, so it doesn't actually stop the node it claims to. It also calls a `close()` method that `RaftClient` doesn't have, which will raise during cleanup.
4. Minor: `MAX_BATCH_SIZE` is defined in `raft/types.py` but never referenced; `ElectionManager.handle_append_entries` (`election.py:248`) is dead code — the real path is `RaftNode.handle_append_entries`. `InstallSnapshot`'s follower-side implementation always replaces the whole local log rather than trying to preserve a matching suffix (correct, per the Raft paper's simpler allowed option — just not the most bandwidth-efficient one).
5. **Leader's own `current_term` isn't persisted to storage when it wins an election** — only followers persist term via `handle_request_vote`/`handle_append_entries`. A candidate's in-memory term bump (`ElectionManager._start_election`) and the transition to leader (`RaftNode._on_become_leader`) never call `storage.set_current_term(...)`. Found while diagnosing the commit-index bug above (a leader's on-disk metadata showed a stale term while its live state was already ahead) — didn't end up being the cause of that bug, but it's a real durability gap: if a leader crashes right after winning an election, it can come back up believing an earlier term than it was actually in.

## 15-Node Verification

Actually built and ran the 15-node cluster (`docker compose up --build`) rather than just validating the generated config, since config validity doesn't prove the fan-out actually works:

- **Election**: fresh cluster reliably elects a single leader within a few hundred ms of startup (some split-vote rounds are normal and expected with 15 simultaneous candidates — Raft's randomized timeouts resolve them within a handful of rounds).
- **Replication**: wrote through the leader, read the same value back correctly from a nearby follower and the most distant one (node1 → node15).
- **Failover**: `docker stop`'d the leader mid-cluster; a new leader was elected from the remaining 14 within ~2s, and all previously-committed data was intact on it.
- **This is exactly how the commit-index bug above was found** — it didn't show up in any unit test (which mock past the exact interaction between the heartbeat loop and the batch-flush path), only in a real failover under real network timing. Re-verified after the fix: killed the leader again, wrote through the new one, and confirmed commit_index advanced immediately (not retroactively) on the farthest follower.

Note: `docker compose down` (without `-v`) leaves named volumes in place — reusing them mixes old persisted Raft state (term, log) with a differently-sized cluster and produces genuinely confusing symptoms (election terms in the hundreds, no leader converging). Use `docker compose down -v` for a clean slate.

## Local Development Setup

The checked-in `.venv` doesn't have the project's dependencies installed, and if your system Python is 3.13+, `grpcio==1.59.0` (pinned in `requirements.txt`) has no prebuilt wheel for it — use 3.11 explicitly.

```bash
python3.11 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# Regenerate the protobuf stubs with the pinned grpcio-tools version. The
# committed raft/proto/*_pb2.py and client/proto/*_pb2.py files can go stale
# if they were last generated with a newer protoc than what's pinned here -
# if you see `ImportError: cannot import name 'runtime_version'`, this is why.
bash scripts/gen_protos.sh
```

## Running the Test Suite

```bash
PYTHONPATH=. pytest tests/ -v
```

148 tests, ~4.5s, all passing as of this writing. Run a single file with `pytest tests/test_election.py -v`, or `-k <pattern>` to filter by name.

## Running the Cluster

```bash
# 1. Generate the compose file for however many nodes you want (15 by
#    default; regenerate whenever you change the count, not just at 15).
python3 scripts/gen_docker_compose.py --nodes 15 > ops/docker-compose.yml

# 2. Build and start
cd ops
docker compose up --build --detach
cd ..

# 3. Give leader election a couple seconds, then check who won. Followers
#    only report their own role (not who the leader is - see Known Issue
#    #1), so check a few nodes. Client ports run 51051-51065 for a 15-node
#    cluster (node N's client port is 51050+N).
PYTHONPATH=. python3 scripts/kvctl.py cluster-info --host localhost --port 51051

# or scan all of them for whichever one says "Role: leader":
for p in $(seq 51051 51065); do
  echo "port $p: $(PYTHONPATH=. python3 scripts/kvctl.py cluster-info --host localhost --port $p 2>&1 | grep Role)"
done

# 4. Basic read/write (use whichever port came back as leader above -
#    writes to a follower are rejected)
PYTHONPATH=. python3 scripts/kvctl.py put-price AAPL 150.0 --host localhost --port <leader_port>
PYTHONPATH=. python3 scripts/kvctl.py get-price AAPL --host localhost --port <leader_port>

# 5. Confirm replication landed on a different node
PYTHONPATH=. python3 scripts/kvctl.py get-price AAPL --host localhost --port 51065

# 6. Inspect full node state (log length, commit index, KV contents)
PYTHONPATH=. python3 scripts/kvctl.py dump-state --host localhost --port <leader_port>
```

`batch-put-price` is implemented: `kvctl.py batch-put-price "AAPL:150.0,NVDA:800.0" --port <leader_port>`.

### Tearing down

```bash
cd ops
docker compose down -v   # -v also removes the named data volumes
```

Use `-v`. Plain `docker compose down` leaves the per-node volumes behind, and reusing them on a next `up` mixes old persisted Raft state (term, log) into a fresh cluster — see the note under "15-Node Verification" above for what that actually looks like when it goes wrong.

### Manual failover test

Reproduces the check that found the commit-index liveness bug (see "Fixed this session" #9) — worth running after any change that touches replication or leader transitions:

```bash
# 1. Find the leader (see step 3 above), then write something through it
PYTHONPATH=. python3 scripts/kvctl.py put-price NVDA 890.25 --host localhost --port <leader_port>

# 2. Kill it
docker stop raft-node<N>   # N = the leader's node number

# 3. Wait ~2s, find the new leader among the rest, and check it immediately
#    (no extra sleep) - commit_index/last_applied should already reflect
#    everything the old leader had committed, and NVDA should read back
#    correctly from any follower right away, not just eventually.
PYTHONPATH=. python3 scripts/kvctl.py dump-state --host localhost --port <new_leader_port>
PYTHONPATH=. python3 scripts/kvctl.py get-price NVDA --host localhost --port 51065
```

## API Reference

### Client Service (external, `proto/client.proto`)
- `PutPrice(symbol, price, timestamp)` → `{ok, leader_hint}` — working; `leader_hint` is currently always empty (Known Issue #1)
- `BatchPutPrice(entries[])` → `{ok, leader_hint}` — working, replicated as a single log entry
- `GetPrice(symbol)` → `TickerPrice` — working
- `GetClusterInfo()` → `{leader_id, term, members[], node_id, role}` — working
- `DumpState()` → `{node_state, kv_store, metrics}` — working, reads live metrics from the Prometheus collector

### Raft Service (internal, `proto/raft.proto`)
- `RequestVote(term, candidate_id, last_log_index, last_log_term)` → `{term, vote_granted}`
- `AppendEntries(term, leader_id, prev_log_index, prev_log_term, entries[], leader_commit)` → `{term, success, match_index}`
- `InstallSnapshot(term, leader_id, last_included_index, last_included_term, data)` → `{term}` — sent instead of `AppendEntries` when a follower's `next_index` has fallen behind the leader's compacted log

## Project Structure

```
.
├── proto/                     # Protobuf service definitions
│   ├── raft.proto             # Raft internal RPCs
│   └── client.proto           # Client-facing RPCs
├── raft/                      # Core Raft implementation
│   ├── node.py                # Main RaftNode orchestrator (replication, batching, commit, snapshotting)
│   ├── election.py            # Leader election / heartbeat timers
│   ├── storage.py             # Durable log + metadata storage, snapshot save/load, compaction
│   ├── types.py                # Enums, dataclasses, constants
│   ├── prometheus_metrics.py  # Metrics (single source of truth - dump-state reads this too)
│   └── structured_logging.py  # JSON structured logging (uncommitted)
├── kv/                        # Key-value state machine
│   └── state_machine.py       # KV store, snapshot export/restore, command application (types live here too)
├── server/                    # gRPC service implementations
│   ├── grpc_server.py         # RaftService (incl. InstallSnapshot) + ClientService (PutPrice, BatchPutPrice, GetPrice, ...)
│   ├── cluster_boot.py        # Peer list parsing, logging setup
│   ├── main.py                 # Server entry point
│   └── metrics_server.py      # aiohttp metrics/health server (uncommitted; port-collision bug)
├── scripts/
│   ├── kvctl.py                # CLI tool (cluster-info, put-price, get-price, dump-state)
│   ├── gen_protos.sh           # Protobuf codegen
│   ├── gen_docker_compose.py   # Generates ops/docker-compose.yml for N nodes
│   ├── benchmark.py            # Load benchmark (uncommitted; broken import, see Known Issues)
│   └── chaos_test.py           # Container-level chaos scenarios (uncommitted; node-targeting bug)
├── tests/                      # 14 files, ~3,900 lines, 148 tests, all passing
├── ops/                        # docker-compose.yml (generated, 15 nodes), Dockerfile
└── README.md
```

See "Local Development Setup" and "Running the Test Suite" / "Running the Cluster" above for how to get this running.

## Configuration

### Environment Variables
- `NODE_ID`, `PEER_LIST`, `RAFT_PORT`, `CLIENT_PORT`, `DATA_DIR`, `LOG_LEVEL`
- `RAFT_BATCH_SIZE` (default 10), `RAFT_FLUSH_INTERVAL_MS` (default 50) — batching config, uncommitted
- `RAFT_SNAPSHOT_THRESHOLD` (default 50) — take a snapshot and compact the log every N newly-applied committed entries

### Raft Parameters
- Election timeout: 150–300ms (randomized)
- Heartbeat interval: 75ms
- Majority requirement: majority of the cluster (8 of 15, by default)

## What's Left to Finish Week 4 (before adding new features)

1. ~~Fix the log-up-to-date vote check~~ — done
2. ~~Fix `truncate_log` → `truncate_log_from` call site~~ — done
3. ~~Fix `put_price` acking writes before replication was confirmed~~ — done
4. ~~Expose metrics ports in `docker-compose.yml`/`Dockerfile`~~ — done
5. ~~Install `requirements.txt` into a working venv and actually run the full test suite to confirm claims~~ — done, 140/140 passing
6. ~~Scale to a 15-node cluster and verify election/heartbeat/replication still hold at that fan-out~~ — done
7. ~~Implement log snapshotting/compaction so the log doesn't grow unbounded~~ — done
8. ~~Implement `BatchPutPrice` server-side + `kvctl.py batch-put-price`~~ — done
9. ~~Resolve the metrics port collision and unify the two metrics collectors so `dump-state` reflects reality~~ — done
10. Fix `scripts/benchmark.py`'s broken import and `scripts/chaos_test.py`'s node-targeting bug (Known Issues #2, #3)
11. ~~Commit all of the above as real, reviewed commits~~ — done (Week 4 is now in git history)

## Future Roadmap (not started)

- **Live Data Integration**: real-time price ingestion (`ingestor/feeder.py`), external API integration (Yahoo Finance / Alpha Vantage), automatic leader discovery/redirect, batch ingestion
- **Visualization & Deployment**: FastAPI + Chart.js dashboard showing live price charts and cluster/leader-election status, Grafana integration for metrics, CI/CD

## Additional Resources

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Raft Visualization](https://raft.github.io/)
- [gRPC Documentation](https://grpc.io/docs/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)

## License

MIT License

---

**Status**: Weeks 1–3 solid and committed. Week 4 (batching + observability) implemented but uncommitted, with several known bugs to fix before it's genuinely done. Weeks 5–7 not started.
