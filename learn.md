# Learn: Building a Raft-Based Distributed KV Store From Zero

This doc assumes you know how to write Python and have used a database, but have **never built a distributed system**. By the end, you should understand every mechanism in this codebase well enough to rebuild it yourself, file by file, in the same order it was actually built.

It's organized top-down: the *problem* first, then the *tools*, then *how the pieces fit together*, then a *step-by-step build order* that mirrors how this project actually grew (Week 1 → Week 4 → the bug-fixing session that came after). Read it in order the first time.

---

## Part 1 — The Problem This Project Solves

### Why isn't one server enough?

Imagine you run the price feed for a trading app on a single server. Two things go wrong eventually:

1. **The server crashes.** Hardware fails, the process OOMs, someone `rm -rf`s the wrong thing. While it's down, your app is down. If the disk itself dies, you've also lost whatever wasn't backed up.
2. **The server can't take the traffic.** One machine has a ceiling on how many requests/second it can serve.

The obvious fix is "run more than one server." But the moment you have two servers holding the *same* data, you've created a new problem: **how do you keep them agreeing with each other?**

### Why "just copy the data" doesn't work

Say you have 3 servers, and a client writes `AAPL = 150`. Naively, you'd send that write to all 3. But:

- What if the write reaches servers 1 and 2, but the network hiccups and server 3 never gets it? Now server 3 is silently wrong.
- What if two clients write to different servers at the *same instant* — client A writes `AAPL=150` to server 1, client B writes `AAPL=151` to server 2? Which one is "the real" value? There's no shared clock you can trust to say "150 came first."
- What if a server crashes *in the middle* of writing to disk? Is the write half-applied? Lost? Duplicated on restart?

This is the general problem of **distributed consensus**: getting multiple computers, connected by a network that can lose, delay, or reorder messages, to agree on a single, ordered sequence of operations — even when some of those computers crash. It's provably impossible to solve *perfectly* (see: the FLP impossibility result, and the CAP theorem, if you want to fall down that rabbit hole later) — but algorithms like **Paxos** and **Raft** solve it well enough for real systems by making specific, well-understood trade-offs.

### Why Raft (and not Paxos)

Paxos is the original, more famous algorithm — and notoriously hard to actually implement correctly; even its own paper admits it's hard to understand. Raft (2014, Ongaro & Ousterhout, ["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)) was explicitly designed to be *teachable* by decomposing the problem into three mostly-independent sub-problems:

1. **Leader election** — agree on who's in charge right now.
2. **Log replication** — the leader is the only one who accepts writes, and streams them to everyone else in a strict order.
3. **Safety** — a set of rules that, if followed, guarantee the whole system never disagrees about what was actually committed, even across crashes and leader changes.

This project implements all three from scratch — no consensus library, just the raw algorithm — because that's what makes it worth having on a resume: anyone can call `etcd.Put()`; building the thing `etcd` uses under the hood is a different level of understanding.

### What Raft actually guarantees

At any moment: **at most one leader** exists (never zero — the algorithm keeps re-electing — and never two, which would let two nodes both accept conflicting writes). A write is only considered **committed** — safe, durable, will never be lost or rolled back — once a **majority** of nodes have it in their log. Once committed, it survives the crash of any *minority* of nodes.

That word "majority" is the whole trick. With 15 nodes, majority = 8. Any two majorities out of 15 must share at least one node in common (8 + 8 = 16 > 15). That overlapping node is what makes it *impossible* for two different values to both get "committed" for the same slot — whichever one is being decided, the overlap node would have to agree to both, which the algorithm's rules forbid.

---

## Part 2 — Tech Stack, and *Why* Each Piece

### Python + `asyncio`

Raft nodes spend almost all their time *waiting* — waiting for a peer's RPC response, waiting for the next heartbeat tick, waiting for a disk write to finish. This is an **I/O-bound** workload, not a CPU-bound one. `asyncio` is Python's model for handling many concurrent waits on a *single thread*, using `async`/`await` to say "pause here until this network call comes back, and let something else run meanwhile."

Analogy: one waiter (the event loop) can take orders from 10 tables while food is cooking (network calls in flight) — they just can't do two things in the exact same instant, they interleave. Contrast with hiring 10 separate waiters (OS threads) who *can* act simultaneously, but now need to coordinate so they don't both try to ring up the same table's bill at once (a lock).

That interleaving-not-simultaneous property is exactly why this project uses asyncio instead of real OS threads for its gRPC server: **Raft's correctness depends on state like `term`, `commit_index`, and the log being mutated one step at a time.** With asyncio, code only "hands off control" at an `await` — so a block of code with no `await` in it runs atomically, with no other coroutine able to interleave. A single-threaded event loop gets you that for free. A real multithreaded server would need an explicit lock around every one of those mutations to get the same guarantee back — for a workload that's I/O-bound anyway, that's pure downside with no real throughput win (see `README.md`'s "Concurrency Model" section for the fuller version of this argument).

`asyncio.gather()` is the key primitive used throughout: "fire off these N coroutines, run them all concurrently, and give me all the results once they're done." That's how a leader sends `AppendEntries` to 14 peers "at the same time" without needing 14 threads.

### gRPC

**RPC** (Remote Procedure Call) means calling a function on another machine and getting a response, structured like calling a local function. **gRPC** is Google's RPC framework: you define a service contract (what functions exist, what their inputs/outputs look like) in a `.proto` file, and gRPC generates client and server code in whatever language you want from that single contract.

Why gRPC instead of a REST/JSON API? Three reasons that matter here specifically:
- **Strongly-typed contract.** `RequestVoteRequest` is a real Python class with real fields, not a dict you hope has the right keys.
- **HTTP/2 under the hood** — supports multiplexing and is lower-overhead than repeatedly opening HTTP/1.1 connections, which matters when you're sending a heartbeat to every peer every 75ms.
- **Codegen.** You never hand-write the request/response marshalling code — `scripts/gen_protos.sh` generates it from `proto/*.proto` for you.

This project runs **two separate gRPC services per node**, on two separate ports:
- `RaftService` (port `50051+`) — internal, node-to-node only: `RequestVote`, `AppendEntries`, `InstallSnapshot`.
- `ClientService` (port `51051+`) — external, what `kvctl.py` talks to: `PutPrice`, `GetPrice`, `BatchPutPrice`, `GetClusterInfo`, `DumpState`.

Splitting them is a deliberate design choice: it means you could firewall the Raft port off from the outside world while still exposing the client port, and it keeps "cluster machinery" and "public API" conceptually and physically separate.

### Protocol Buffers ("protobuf")

Protobuf is the serialization format gRPC uses — the actual bytes that go over the wire. You write a schema once (`proto/raft.proto`, `proto/client.proto`), and `protoc` (the protobuf compiler, invoked via `grpc_tools` in `scripts/gen_protos.sh`) generates:
- `*_pb2.py` — the actual message classes (`RequestVoteRequest`, `LogEntry`, etc.)
- `*_pb2_grpc.py` — the client stub and server base class for each service

Why not just JSON? Protobuf messages are smaller on the wire (binary, not text), faster to serialize/deserialize, and — most importantly for a project like this — the schema is enforced. You can't accidentally send a `RequestVote` with a typo'd field name and have it silently do nothing; it won't compile.

One sharp edge worth knowing about (because it bit this project): the generated `_pb2.py` files are tied to the exact version of `protoc`/`grpcio-tools` that generated them. If someone regenerates them with a newer toolchain than what's pinned in `requirements.txt`, the older runtime can fail to even *import* them (`ImportError: cannot import name 'runtime_version'`). Always regenerate with the pinned version (`bash scripts/gen_protos.sh` after `pip install -r requirements.txt`), not whatever `protoc` happens to be on your PATH.

### Docker + Docker Compose

A **container** is a lightweight, isolated bundle of your app plus its exact dependencies — not a full VM, but isolated enough that "works on my machine" mostly stops being a problem. `ops/Dockerfile` describes how to build one image containing this project: install Python deps, generate the protobuf stubs, copy the code.

You can't test a *distributed* system by running one process. You need N independent processes that genuinely can't see each other's memory and only talk over a real network — otherwise you're not actually testing the distributed parts. **Docker Compose** (`ops/docker-compose.yml`) declares N services, each running the same image with a different `NODE_ID`/ports, all attached to a shared virtual network (`raft-network`) where they can resolve each other by service name (`node1`, `node2`, ...) — no manual IP management needed.

Because this project scales to a configurable node count, the compose file is **generated**, not hand-written — see `scripts/gen_docker_compose.py`. More on why in Part 4.

### pytest + pytest-asyncio

Standard Python testing, plus the `pytest-asyncio` plugin, which is required because most of this codebase's functions are `async def` — plain pytest doesn't know how to run and await a coroutine as a test.

### Prometheus client

**Observability**: the ability to answer "what is this system doing right now" without SSHing into 15 machines and grepping logs. Prometheus is a metrics system built around three shapes of data, all used in `raft/prometheus_metrics.py`:
- **Counter** — a number that only goes up (`raft_elections_total`, `raft_commits_total`).
- **Gauge** — a number that goes up or down (`raft_commit_index`, `raft_log_length`).
- **Histogram** — a distribution of observed values, e.g. `raft_replication_latency_ms`, so you can ask "what's the p99 replication latency" not just "what was the last one."

---

## Part 3 — Project Anatomy: How the Pieces Talk to Each Other

```
proto/          the wire contract (source of truth for RPC shapes)
raft/           the actual consensus algorithm
kv/             the "database" the algorithm replicates commands into
server/         glues raft/ + kv/ to gRPC
scripts/        CLI tools, codegen, cluster generation
ops/            Docker + Docker Compose
tests/          pytest suite
```

Trace a single write end-to-end to see how they connect — this is the single most important mental model in the whole project:

1. You run `kvctl.py put-price AAPL 150.0 --port 51057`. `kvctl.py` (`scripts/kvctl.py`) opens a gRPC channel to that node's `ClientService` and calls `PutPrice`.
2. `server/grpc_server.py`'s `ClientService.PutPrice` receives it and calls `raft_node.put_price(...)`.
3. `RaftNode.put_price()` (`raft/node.py`) first checks: **am I the leader?** If not, it rejects the write (only the leader accepts writes in Raft — this is what keeps the system from diverging).
4. If leader: it serializes the command (`kv/state_machine.py`'s `serialize_put_command`, which just JSON-encodes `{"type": "PUT", "data": {...}}` into bytes — Raft itself doesn't know or care what the bytes mean), wraps it in a `LogEntry`, and **appends it to its own local durable log** (`raft/storage.py`).
5. It's added to a batch queue (`_add_to_batch`) rather than replicated immediately — see Part 4's batching section for why.
6. When the batch flushes, `_replicate_to_peers` fires an `AppendEntries` RPC at every other node **concurrently** (`asyncio.gather`).
7. Each follower's `RaftNode.handle_append_entries` checks the **log matching property** (does my log agree with yours up to the entry right before this one?), appends the entry if so, and replies success.
8. Once the leader has heard "success" from a **majority** (itself + enough followers), it advances its `commit_index` and applies the entry to its own KV state machine (`kv/state_machine.py`'s `apply_command`, which actually does `store["AAPL"] = TickerPrice(150.0, ...)`).
9. The future that `put_price` was waiting on gets resolved, and the client finally gets its real `ok=True`/`False` back.
10. Followers find out the entry is committed on their **next** heartbeat (which carries the leader's `commit_index`), at which point they apply it to their own KV state machines too.

A **read** (`GetPrice`) is much simpler and doesn't go through consensus at all — it just reads directly out of the local `KVStateMachine`'s in-memory dict. That's a real, documented trade-off: a follower that hasn't caught up yet can serve a stale read. (A "fixed" version of this codebase could route reads through the leader or add a read-index protocol; this project doesn't, and that's worth knowing rather than assuming.)

---

## Part 4 — Building It, Step by Step

This section mirrors how the project actually grew. Each step only makes sense once the previous one works — build (and test) in this order.

### Step 0: The contract, before any logic

Before writing a single line of Raft logic, `proto/raft.proto` and `proto/client.proto` were written first. This isn't arbitrary — both "sides" of every conversation this system will ever have (node ↔ node, client ↔ node) need to agree on shapes *before* you can write code that uses them. Run `scripts/gen_protos.sh` to turn these into importable Python classes. Everything downstream imports from `raft.proto` / `client.proto` (the generated `_pb2` modules), never invents its own ad-hoc message format.

### Step 1: Leader Election

Files: `raft/types.py`, `raft/election.py`.

**`raft/types.py`** defines the vocabulary everything else uses:
- `RaftState` — an enum: `FOLLOWER`, `CANDIDATE`, `LEADER`. Every node is in exactly one of these at any time.
- `LogEntry` — `(index, term, command_bytes)`. `index` is the entry's position in the log (1-based). `term` is *when* (in Raft's logical clock) the entry was created — critical for detecting stale/conflicting data later.
- `PeerInfo` — how to reach another node (host, raft port, client port).
- Timing constants: `ELECTION_TIMEOUT_MIN/MAX = 150/300ms`, `HEARTBEAT_INTERVAL = 75ms`.

**A "term"** is a monotonically increasing integer — think of it as an epoch number. Every node tracks "the current term I know about." Whenever a node sees a message (vote request, heartbeat) from a *higher* term than its own, it immediately adopts that term and steps down to follower — this is how the whole cluster converges on "whose in-progress leadership counts" after any disruption. A message from a *lower* term is always stale and rejected.

**Election flow** (`raft/election.py`, `ElectionManager`):
1. Every follower runs a timer, randomized between 150–300ms (`start_election_timeout`). **The randomization is the whole trick here** — if every node used the exact same timeout, they'd all become candidates in the same instant after every leader loss, split the vote forever, and never converge. Jitter means *someone* almost always fires first and gets a head start collecting votes before others even become candidates.
2. If the timer fires with no heartbeat received, the node becomes a `CANDIDATE`: increments its term, votes for itself, and requests votes from every peer concurrently.
3. Each peer decides whether to grant the vote (`handle_vote_request`) based on two rules: (a) it hasn't already voted for someone else *this term*, and (b) — **this is the one that was missing in the original implementation and caused a real safety bug, see Part 6** — the candidate's log must be at least as up-to-date as the voter's own log. Skipping rule (b) would let a node with a stale, incomplete log get elected leader and overwrite already-committed data on everyone else.
4. If the candidate gets votes from a majority (including itself), it becomes leader and immediately starts sending heartbeats — an empty `AppendEntries` sent to every peer every 75ms, both to assert "I'm still alive, don't start an election" and to piggyback any log catch-up a peer needs.

**Why AppendEntries doubles as the heartbeat**: rather than a separate "ping" RPC, an `AppendEntries` with zero entries *is* the heartbeat. Any AppendEntries — heartbeat or not — resets the receiver's election timer, since it proves a leader is alive.

### Step 2: Log Replication & the State Machine

Files: `raft/storage.py`, `kv/state_machine.py`, the replication path in `raft/node.py`.

**`raft/storage.py`** (`RaftStorage`) is where the log and metadata (`current_term`, `voted_for`, `commit_index`, `last_applied`) actually live on disk, as two JSON files per node. Every mutation is written with an **atomic-write pattern**: write to `<file>.tmp`, `fsync()` it (force the OS to actually flush to physical disk, not just an in-memory page cache that a power loss could wipe), then `os.rename()` the temp file over the real one. `rename` is atomic at the filesystem level — there is no possible crash point that leaves you with a half-written file. Without this, a crash mid-write could corrupt the log.

**`kv/state_machine.py`** (`KVStateMachine`) is the "database" — a plain Python dict, `symbol -> TickerPrice`. Here's the conceptual leap that makes Raft general-purpose: **Raft itself doesn't know what a `PUT AAPL 150` means.** It only replicates an ordered sequence of opaque byte blobs (`command_bytes`). Each node independently applies that *same sequence, in the same order*, to its own local state machine. Because state machines are deterministic (same input → same output, always), applying the same ordered sequence on every node guarantees they all converge to identical state — without ever needing to compare their actual data directly. This is why you could swap `kv/state_machine.py` for a completely different "database" (a graph, a counter, anything) without touching a single line of `raft/`.

**Idempotency matters here**: a command might get applied more than once (crash recovery replay, a retried RPC). `apply_command` tracks `last_applied_index` and skips anything already applied — applying the same `PUT` twice must be a no-op, not a bug.

**The log matching property** is what makes replication efficient. Claim: if two logs have an entry with the same `(index, term)`, then *every entry before that index* is guaranteed identical between the two logs too. This means a follower only has to check **one** entry — `prev_log_index`/`prev_log_term`, sent with every `AppendEntries` — to know whether its whole log prefix still agrees with the leader's. If it doesn't match, the follower rejects, and the leader retries with an earlier `prev_log_index` (`next_index` decrements by one and retries — see `_send_append_entries_to_peer`) until it finds the point where they last agreed, then overwrites everything after that with its own (correct, majority-backed) version. The leader is always assumed right for this purpose: anything the follower had beyond the divergence point was, by definition, never committed (committed data survives leader changes; if it had been committed, log matching guarantees no leader could have a conflicting version there).

**Advancing `commit_index`** (`_update_commit_index`): the leader tracks `match_index[peer]` — the highest log index it *knows* each follower has durably stored (updated whenever an `AppendEntries` to that peer succeeds). Sort all the `match_index` values (plus the leader's own) descending; the value at the *majority* position is the highest index a majority is guaranteed to hold — that becomes the new `commit_index`. One extra safety rule: **only commit entries from the leader's current term** — an older-term entry can be "provisionally" replicated to a majority via a newer leader's catch-up traffic without that leader ever having proven it can commit *its own* term's entries; Raft's authors show committing across terms this way can, in rare interleavings, be unsafe. Once `commit_index` advances, `_apply_committed_entries` pushes the newly-committed entries into the KV state machine.

**Crash recovery** (`_recover_from_crash`): on startup, a node reloads its persisted `term`/`commit_index`/`last_applied` from `raft/storage.py`, then replays only the entries between `last_applied` and `commit_index` into the KV state machine — not the whole log, just the gap. This is safe specifically *because* of idempotent apply and because `commit_index` was itself only ever advanced once a majority already had that data.

### Step 3: Wiring It to gRPC

Files: `server/grpc_server.py`, `server/cluster_boot.py`, `server/main.py`.

This layer is intentionally thin. `RaftService` and `ClientService` (`server/grpc_server.py`) are gRPC servicer classes whose methods do almost nothing but call the matching `RaftNode` method and translate the result into a protobuf response. All the actual logic lives in `raft/node.py` — the gRPC layer's only job is "receive bytes off the wire, call the right function, put bytes back on the wire." `server/cluster_boot.py` parses the `PEER_LIST` environment variable (`node_id:host:raft_port:client_port,...`) into `PeerInfo` objects — this is how a node learns who its peers are without hardcoding IPs.

### Step 4: Docker Compose — Simulating a Real Cluster

Files: `ops/Dockerfile`, `ops/docker-compose.yml`, `scripts/gen_docker_compose.py`.

Once a single node works, you need *N* of them talking over a real network to actually exercise the algorithm (a single process can't have a network partition with itself). `ops/docker-compose.yml` declares one service per node, each with a unique `NODE_ID`, unique host ports, and the *same* `PEER_LIST` (so every node knows about every other node, including itself — the code filters itself out).

This file is **generated**, not hand-maintained (`scripts/gen_docker_compose.py --nodes N`), for a concrete reason: hand-writing 15 near-identical blocks is exactly how port-numbering mistakes creep in (this actually happened during this project's own development — see Part 6). Two subtleties the generator has to get right that aren't obvious until you hit them:
- **Port ranges must not overlap.** Raft ports, client ports, and metrics ports each need their own non-overlapping numeric range wide enough for however many nodes you might ever run.
- **Only one service should carry a `build:` stanza.** If all N services declare their own `build:` for the *same* image tag, Docker's parallel build tooling (buildx) can race and fail with "image already exists." One service builds it; the rest just reference `image: raft-node:latest`.

### Step 5: Performance & Observability (Batching, Metrics, Logging)

Files: `raft/prometheus_metrics.py`, `raft/structured_logging.py`, the batching methods in `raft/node.py`.

**Batching**: replicating one `AppendEntries` per write is wasteful — the network round-trip cost dominates for small individual writes. Instead, writes accumulate in `pending_entries` and flush together, either once `batch_size` entries have queued up, or every `flush_interval` milliseconds, whichever comes first (`_add_to_batch` / `_flush_pending_entries` / `_batch_flush_loop`). The subtlety (and the bug this project actually shipped with initially — see Part 6): batching must not let a client believe a write succeeded before you actually know that. The fix used here is an `asyncio.Future` per pending entry: `put_price` gets a future back from `_add_to_batch`, and `await`s it; whenever that entry's batch actually flushes, the future resolves to the real success/failure. Concurrent writers still batch together for throughput — they just each individually wait on their own outcome rather than the caller not waiting at all.

**Metrics & structured logging** exist purely for observability, not correctness — but they matter for the same reason production systems need them: you can't debug a live 15-node cluster by adding `print()` statements and restarting everything.

### Step 6: Log Snapshotting / Compaction

Files: the snapshot methods in `raft/storage.py`, `kv/state_machine.py`'s `get_snapshot_data`/`restore_from_snapshot`, `_take_snapshot` and `InstallSnapshot` handling in `raft/node.py`, the RPC itself in `proto/raft.proto`.

**Why this has to exist eventually**: the log only ever grows. Every write appends an entry, nothing ever removes one (until this feature). Left alone, disk usage grows without bound, and — worse — every crash recovery has to replay a longer and longer history.

**The fix, in Raft terms (§7 of the paper)**: periodically, once enough new entries have been applied since the last checkpoint (`RAFT_SNAPSHOT_THRESHOLD`, default 50), a node saves a full checkpoint of its state machine (`kv_state_machine.get_snapshot_data()` — just the whole `store` dict) to disk, records the `(last_included_index, last_included_term)` it covers, and then **discards** every log entry up to that point. The log entries before the snapshot are gone — the snapshot itself is now the only record of what they did.

This creates a new problem: what if a follower is *so* far behind that the entries it needs to catch up have already been discarded on the leader? Normal `AppendEntries` catch-up can't work — those entries simply don't exist anymore. The answer is a new RPC, **`InstallSnapshot`**: the leader sends the *entire checkpoint* to that follower in one shot, the follower wholesale-replaces its own state and log with it, and both sides are now caught up from the snapshot boundary forward. (This implementation always replaces the follower's whole log rather than trying to preserve a matching suffix — the simpler of the two behaviors the Raft paper allows; see Known Issues in the README for the bandwidth trade-off that implies.)

### Step 7: Scaling Up and Finding What Only Shows Up At Scale

This is covered in depth in Part 6 and Part 7 — the short version: generating a 15-node cluster and config-validating it is not the same as *running* it. Only running it live surfaced a real concurrency bug that no unit test caught.

### Step 8: Closing the Loop — Batch Updates (`BatchPutPrice`)

`BatchPutPrice` was actually the *first* RPC defined in the proto contract (Step 0) but the *last* one actually implemented — it sat as a stub returning `ok=True` without touching its payload for the entire life of the project until this step. It's worth its own section because "batch update" is a genuinely useful, genuinely misunderstood idea, and because this codebase has **two different, easily-confused kinds of batching** operating at two different layers.

**What a batch update actually is**: instead of calling `PutPrice` once per symbol — `PutPrice(AAPL, 150)`, then `PutPrice(NVDA, 800)`, two separate client calls — you call `BatchPutPrice([("AAPL", 150), ("NVDA", 800)])` **once**, and both writes are committed to the log **together, as a single log entry, atomically**: either both end up committed, or neither does (there's no state where a crash mid-flight leaves you with `AAPL` written and `NVDA` not, the way there could be if you'd made two independent `PutPrice` calls and the process died between them).

**The two layers of batching — don't conflate them:**

| | **Command batching** (`BatchPutPrice`) | **Network batching** (`_add_to_batch`/`_flush_pending_entries`, Step 5) |
|---|---|---|
| What it groups | Multiple *key-value writes* into one *log entry* | Multiple *log entries* into one round of *AppendEntries RPCs* |
| Who triggers it | The client, explicitly, by calling `BatchPutPrice` instead of N `PutPrice` calls | The system, automatically, for *every* write regardless of which RPC created it |
| Unit being reduced | Log entries (disk writes, replay-on-recovery cost) | Network round trips |
| Visible to the client? | Yes — it's an API you choose to use | No — happens transparently whether you called `PutPrice` or `BatchPutPrice` |

They compose, and it's worth seeing concretely how: say 10 separate `BatchPutPrice` calls arrive at the leader within the same `flush_interval` window (50ms by default), each with 5 ticker prices. Command batching means that's **10 log entries** (not 50 — each call's 5 prices collapse into 1 entry). Network batching then means those 10 log entries get sent to every follower in **1 `AppendEntries` RPC**, not 10. Two independent optimizations, stacking: 50 logical writes → 10 log entries → 1 network round trip per follower.

**Advantages of command batching specifically** (beyond what network batching already gives you):
- **Atomicity.** A batch is one log entry with one index — it commits or doesn't as a unit. Ten separate `PutPrice` calls have no such guarantee between each other, even if network batching happens to ship them together (network batching's atomicity, from `_flush_pending_entries`'s success/failure handling, is *per-entry*, not across the whole flushed group).
- **Smaller log, faster recovery.** 1 entry instead of N means 1/N the disk writes for this data, and — since crash recovery replays entries one at a time — proportionally less to replay after a restart.
- **Lower overhead per logical write.** Every log entry carries fixed costs (an index, a term, a `fsync`'d disk write, a place in the majority-commit accounting). Batching amortizes that fixed cost across N writes instead of paying it N times.

**How it's actually wired, end to end** (mirrors `put_price` almost exactly — that's deliberate, see Part 5):

1. **Client**: `kvctl.py batch-put-price "AAPL:150.0,NVDA:800.0"` parses the comma-separated pairs and calls the `BatchPutPrice` gRPC method with a `repeated TickerPrice` field (the message shape — `proto/client.proto`'s `BatchPutPriceRequest` — was defined back in Step 0 and never needed to change).
2. **gRPC layer** (`server/grpc_server.py`'s `ClientService.BatchPutPrice`): converts the protobuf `TickerPrice` messages into `kv.state_machine.TickerPrice` dataclass instances, and calls `raft_node.batch_put_price(ticker_prices)`. This layer still does nothing but translate — same principle as Step 3.
3. **`RaftNode.batch_put_price()`** (`raft/node.py`): checks leadership (same as `put_price`), then calls `serialize_batch_put_command(ticker_prices)` — **not** `serialize_put_command` N times — which wraps the *entire list* into one `Command(type="BATCH_PUT", data=[...])`, JSON-encodes it, and hands back **one** blob of bytes. That blob becomes the `command_bytes` of a **single** `LogEntry`, appended to the log **once**, and handed to `_add_to_batch` (the network-batching queue from Step 5) exactly like any other entry — `batch_put_price` doesn't need to know or care that network batching exists; it just produces one entry and lets the existing machinery replicate it, `await`ing the same per-entry future `put_price` uses for real confirmation.
4. **State machine** (`kv/state_machine.py`'s `apply_command`): this is the part that required *no new code at all* — `apply_command` already had a `BATCH_PUT` branch (`elif command.type == "BATCH_PUT": for ticker_price in command.data: self.store[ticker_price.symbol] = ticker_price`) from the very first version of this file. It was written to handle this case from the start; it just had no way to ever *receive* a `BATCH_PUT` command, because nothing upstream of it ever produced one. **This is worth sitting with**: the bug wasn't "we don't know how to do batch updates" — the hard part (state machine semantics) was done. The gap was purely plumbing — the gRPC handler and the `RaftNode` method that should have called this were never written. A good reminder that "is the feature done" and "does every layer in the pipeline actually connect" are different questions.

---

## Part 5 — Design Decisions, Distilled

| Decision | Alternative considered | Why this one |
|---|---|---|
| asyncio, single event loop | Threaded `grpc.server` + locks | Raft's state mutations need implicit serialization; asyncio gives that for free between `await` points. Threads would need explicit locking for the same guarantee, with no throughput win on an I/O-bound workload. |
| gRPC + protobuf | REST + JSON | Typed contract, codegen, HTTP/2 multiplexing — matters when heartbeating N peers every 75ms. |
| Two gRPC services (Raft vs Client) on separate ports | One combined service | Keeps internal cluster machinery separable (e.g. firewallable) from the public API. |
| Log is opaque bytes, state machine is separate | Raft directly manipulating the KV dict | Decouples consensus from what's being replicated — the KV store could be swapped for anything. |
| Atomic write (tmp file + fsync + rename) | Direct in-place writes | Guarantees no crash can leave a half-written file; `rename` is atomic at the OS level. |
| Batched writes with per-entry futures | Synchronous per-write replication | Throughput under concurrent load, without sacrificing "the client is told the truth" about whether their specific write actually landed. |
| Snapshot + InstallSnapshot | Let the log grow forever | Bounded disk usage and bounded crash-recovery replay time. |
| Generated `docker-compose.yml` | Hand-written | Eliminates copy-paste port/peer-list mistakes at any node count; regenerable if the count changes. |
| Reads served from local state, no read-index protocol | Route all reads through the leader | Simpler, faster reads — at the cost of followers being able to serve stale data. A conscious, documented trade-off, not an oversight. |
| `BatchPutPrice` = 1 log entry for N writes | N separate `PutPrice` calls (relying only on network batching to group them) | Atomicity across the whole group, smaller log, and no dependence on N writes happening to land in the same flush window. |

---

## Part 6 — Bugs We Hit, and What Each One Teaches

Real bugs found (and fixed) while getting this project to actually pass its own tests and actually run live. Each one maps to a specific Raft concept — understanding *why* they were bugs is arguably more valuable than the fix itself.

### 1. Vote granted without checking the candidate's log (safety bug)

**Symptom**: `ElectionManager.handle_vote_request` granted a vote to any candidate it hadn't already voted for, full stop — it never checked whether the candidate's log was actually up to date. Candidates also always advertised `last_log_index = last_log_term = 0`, regardless of their real log, so even *had* the check existed, it would've been comparing against fake data.

**Why it matters**: this directly violates Raft's **leader completeness property** — the guarantee that a newly elected leader is guaranteed to already contain every entry committed in all previous terms. Without it, a node with a stale, incomplete log could win an election (if it happened to ask for votes before the up-to-date nodes did) and then, as leader, start overwriting other nodes' correct, committed history with its own incomplete version. This is the single most safety-critical fix in the whole session — data loss, not just unavailability.

**Fix**: candidates now advertise their *real* `(last_log_index, last_log_term)` (`RaftNode._get_last_log_info`, backed by real storage), and voters compare it against their own before granting a vote (`_is_log_up_to_date` — implements Raft §5.4.1: a candidate's log is at least as up-to-date if its last entry has a strictly higher term, or the same term with index ≥ the voter's).

### 2. `truncate_log` — a method that didn't exist, with an off-by-one hiding behind it

**Symptom**: on a failed batch replication, the code called `self.storage.truncate_log(entry.index - 1)` — but `RaftStorage` only ever defined `truncate_log_from(index)`. This would crash the first time it actually ran in production. It didn't get caught by the existing test because the test *mocked out the exact method being called*, so it never exercised the real (broken) code path.

**Why it matters**: this is a good example of a test that passes for the wrong reason — it proves the mock was called, not that the real method exists or does the right thing.

**Fix**: call the real method, `truncate_log_from(entries_to_flush[0].index)` — which also fixed a genuine logic bug: the original code's index math would have deleted the *last successfully committed entry before the batch*, not just the failed batch itself.

### 3. `put_price` returned `ok=True` before replication was actually confirmed

**Symptom**: once batching was introduced, `put_price` appended locally, queued the entry for later batched replication, and returned success **immediately** — without knowing whether replication would actually succeed. Caught by `test_network_partition_recovery`: simulate replication always failing, and the client still got told `ok=True`.

**Why it matters**: this silently breaks the whole point of Raft. A client-visible "success" is supposed to mean "this is durable, majority-committed data" — if it can lie, none of the consistency guarantees this system is built to provide actually hold in practice.

**Fix**: `_add_to_batch` now returns an `asyncio.Future`; `put_price` `await`s it, and the future only resolves once that specific entry's batch has actually flushed (successfully or not). Concurrent callers still batch together server-side for throughput — each just waits on their own outcome.

### 4. Stepping down mid-batch left queued futures unresolved (a deadlock risk)

**Symptom**: once fix #3 existed, a new failure mode appeared: if a leader stepped down (`_on_become_follower`) while writes were still queued in `pending_entries`, nothing ever resolved those entries' futures — any caller `await`ing one of them would hang **forever**.

**Why it matters**: this is a classic async-programming trap — introducing a "wait for X" primitive means you now also have to guarantee X is *always eventually resolved*, on every code path, including error/abort paths, not just the happy path.

**Fix**: `_on_become_follower` now explicitly resolves any still-pending futures to `False` and clears the queue.

### 5. Eight bugs, in the tests themselves

Found while making the (previously never-run) test suite actually pass. As a group, they teach the same lesson: **a passing test only proves what it actually exercises.**
- Stale mocks that returned a fixed value regardless of what the code under test actually did during the call (`test_follower_catchup_after_restart` mocked `get_log_entries()` to a static list, so the test never noticed the real commit-index math was reading a real, growing log).
- A test that mocked the *exact method* whose internal side effect (a metrics call) it was trying to verify — guaranteeing that call could never happen (`test_metrics_recording_during_chaos`).
- Expected values that didn't match the test's own setup (`test_crash_recovery_with_partial_commits` set `commit_index=2` but asserted 2 entries got applied — only 1 was actually committed by that setup).
- `Histogram` metrics (which track a distribution, not a single value) asserted via the `Counter`/`Gauge`-only `._value` API instead of `._sum`.
- An "isolation" test comparing two Prometheus registries' text exports *before either had recorded anything* — of course they were identical; nothing had happened yet.

### 6. Commit index could get permanently stuck after a leader failover — found only by *running* the thing

This is the most instructive bug in the project, because unit tests structurally couldn't have caught it.

**Symptom**: kill the leader of a live 15-node cluster, let a new leader get elected, write through it, and `GetPrice` on a distant follower returned "not found" — for data the new leader had already told the client was written successfully.

**Root cause**: `_update_commit_index()` — the function that checks whether a majority now has some entry and advances `commit_index` accordingly — was only ever called from *one* place: right after a batch flush succeeds. But right after a leader failover, followers don't all catch up through the batch-flush path — many of them catch up through the **heartbeat loop** instead (`_send_heartbeat_to_peer`), which reconciles each follower's `next_index` independently, on its own schedule, as part of normal heartbeating. That path updated `match_index` correctly, but never re-triggered `_update_commit_index()`. So: a write could genuinely reach a majority — just not *all through the same code path within the same instant* — and the leader would never notice the majority had actually been reached, until some *unrelated, later* write happened to trigger a fresh check (which would then retroactively "discover" the earlier majority all at once).

**Why unit tests couldn't have found it**: this bug lives entirely in the *interaction* between two independently-running async loops (the heartbeat loop and the flush path) under real timing, right after a real leader transition. Every unit test that exercises replication mocks the network layer, so it can't reproduce "some followers caught up via one code path, others via a different one, at slightly different real wall-clock times." You need real processes, a real network, and a real failure injected mid-flight to see it.

**Fix**: also call `_update_commit_index()` after a successful heartbeat-driven catch-up, not just after a batch flush. Verified by literally repeating the failure recipe against the running cluster after the fix, and confirming `commit_index` now advances immediately instead of waiting for a subsequent unrelated write.

### 7. Metrics: a chain of four bugs, each one hidden behind the last

This one is worth studying not for any single bug in it, but for the *shape* of the whole episode: fixing bug (a) was what made bug (b) possible to even notice; fixing (b) is what let (c) happen for the first time ever; fixing (c) is what finally let (d) run. None of these were introduced by fixing the previous one — all four had existed since the code was written. They were just standing in a line, each one hidden behind the failure of the one in front of it. This is a distinct lesson from bug #6 above: that one was about *timing and interleaving*; this one is about how **a silently-failing code path doesn't just fail — it hides every bug that lives downstream of it, indefinitely, until something forces that path open.**

Starting point: two known, already-documented issues — `raft/prometheus_metrics.py` and `server/metrics_server.py` both tried to bind the same metrics port, and `dump-state` always showed 0 for every counter. Two bugs, one investigation session, but they turned into four.

**(a) The port collision**

- **Symptom**: node logs showed `Failed to start metrics server: [Errno 98] address already in use`, and `metrics_server.py`'s `/health` endpoint was unreachable.
- **Root cause**: both files computed the metrics port the same way (`8000 + node number`), and both tried to bind it — `raft/prometheus_metrics.py`'s `PrometheusMetrics.__init__` calls `prometheus_client`'s own `start_http_server()`, and separately, `server/grpc_server.py` starts `metrics_server.py`'s aiohttp server on the same port a moment later. Since `RaftNode` (which constructs `PrometheusMetrics`) is built before the gRPC server starts, the first one always won the race and the second always lost, silently.
- **How I got to the fix**: read both files in full rather than guessing. `metrics_server.py`'s `/metrics` handler already called `raft.prometheus_metrics.get_prometheus_metrics().get_metrics()` — the exact same registry `prometheus_client`'s built-in server was exposing — plus it uniquely served `/health`, which the built-in server doesn't provide at all. That made the two servers not "two independent things that happen to collide" but "one server doing a strict superset of what the other does, and losing the race to bind."
- **Fix**: delete the redundant `start_http_server()` call. One server, not two.

**(b) `dump-state` reading a collector nothing updates**

- **Symptom**: `kvctl.py dump-state` always printed 0 for elections/commits/replicated-entries, even right after real writes.
- **Root cause**: `RaftNode.dump_state()` read from `raft/metrics.py`'s old `MetricsCollector` — a *second, separate* metrics system, written before the project migrated to `raft/prometheus_metrics.py`, and never removed. Nothing had called that old collector's `record_*` functions since the migration; it was permanently frozen at all-zero.
- **How I got to the fix**: `grep -rn "raft\.metrics"` across the whole codebase turned up exactly two call sites, both in `node.py`, confirming the old module was otherwise dead. Compared `PrometheusMetrics.get_metrics_dict()`'s output keys against what `dump_state()` and `kvctl.py`'s display code expected — the field names already matched exactly, so this was a drop-in swap, not a rewrite.
- **Fix**: point `dump_state()` (and the one other live call site, in `_recover_from_crash`) at `raft/prometheus_metrics.py` instead. Once that was the only consumer, `raft/metrics.py` had zero remaining references anywhere in the codebase — deleted the whole file rather than leave dead code sitting there to confuse the next person (exactly the situation bug (a)/(b) themselves came from).

**(c) `dump-state` immediately crashing anyway, but only once it started reading real data**

- **Symptom**: with (a) and (b) both fixed, rebuilt the cluster, made a real write, and called `dump-state` for the first time against a node with non-zero metrics — and it crashed: `'float' object cannot be interpreted as an integer`, `grpc_status:13`.
- **Root cause**: `prometheus_client`'s `Counter`/`Gauge` types store their value internally as a Python `float` *no matter what you increment them by* — `get_metrics_dict()`'s `._value.get()` calls therefore always return floats. But `client.proto`'s `RaftMetrics` message declares fields like `elections_total` as `uint64`, and protobuf's setter is strict: unlike, say, JSON, it refuses to silently coerce a float into an integer field. This code path had *never once executed with a non-zero value* before — the old dead collector in bug (b) was always all-zero, and `0`/`0.0` happen to both satisfy protobuf's int fields without erroring, so the bug had no way to surface until real data flowed through it for the first time.
- **How I got to the fix**: the traceback pointed at the exact `client_pb2.RaftMetrics(...)` construction call in `grpc_server.py`; comparing the proto schema's field types against what the metrics dict actually contained made the mismatch obvious.
- **Fix**: wrap every `*_total` field in `int(...)` at the point of protobuf construction (the `_ms` latency fields stay as floats — those really are `double` in the proto).

**(d) `/metrics` returning HTTP 500, but only once it could actually be reached**

- **Symptom**: with (a)-(c) fixed, `curl http://.../metrics` against a live node returned a 500.
- **Root cause**: `metrics_server.py`'s handlers built the Content-Type header as one compound string, `'text/plain; version=0.0.4; charset=utf-8'`, and passed it as `content_type=` alongside `text=` to `aiohttp.web.Response`. Newer aiohttp versions explicitly forbid a `charset=` substring inside `content_type` when `text=` is also given — aiohttp wants to own charset negotiation itself, via a separate keyword, and raises `ValueError: charset must not be in content_type argument` rather than silently accepting the redundant value. Like (c), this line of code had existed since the file was written, but the aiohttp server had never once successfully bound *and served a real request* before (a) was fixed, so this had never actually run.
- **How I got to the fix**: read the full container log traceback, which named the exact line and aiohttp's own source location raising the `ValueError` — no guessing required, just reading what was already being reported.
- **Fix**: drop the embedded `;charset=utf-8` from the content-type string, keeping `;version=0.0.4`. aiohttp appends `;charset=utf-8` itself by default when `text=` is given, producing the identical final header without hitting the restriction.

**Final result**: all four fixed, then re-verified live end to end against the rebuilt 15-node cluster — a real write followed by `dump-state` now shows correct non-zero counts instead of crashing, `curl /health` returns `200 OK`, and `curl /metrics` returns real Prometheus-format text instead of a 500. Neither `server/grpc_server.py` nor `server/metrics_server.py` had *any* test coverage before this — added `tests/test_grpc_server.py` (covering exactly the float→protobuf conversion that broke) and `tests/test_metrics_server.py` (covering exactly the content-type construction that broke), bringing the suite to 148 tests, all passing.

**The general lesson**: when you fix a bug that was causing something to fail *silently* (a crash that's swallowed, a server that never starts, a code path that's always fed zeros), don't assume you're done once that one symptom goes away. Ask what that failure was *protecting you from seeing*. The right move after a fix like this is the same one that found bugs (c) and (d): go run the now-unblocked path for real, with real data, and see what happens.

### 8. The snapshot boundary was a hole nobody had walked through yet — and it broke *every* write, permanently

Found while going back to re-verify the `benchmark.py`/`chaos_test.py` fixes from bug-chain #7 — the most severe bug in this whole project's history, in the sense that once triggered, it didn't just cause an occasional wrong answer, it made **every single write fail, forever, with no way to recover short of a code fix.**

**Symptom**: a live 15-node cluster that had been running a while (long enough for `RAFT_SNAPSHOT_THRESHOLD`, default 50, to be crossed — not a rare edge case, just normal usage) stopped accepting writes entirely. Every `put_price` returned "Replication failed." The leader's logs showed the same message looping forever: `"Installed snapshot on nodeX"`, for every peer, over and over.

**Root cause**: recall from Step 6 (Part 4) that snapshotting *compacts* the log — entries at or before the snapshot boundary are deleted from `self.log`, recoverable only from the snapshot file itself, not via `get_log_entry()`. That's correct and intentional. The bug is that **two other pieces of code silently assumed `get_log_entry()` could see everything**, including the boundary index itself:

1. `_check_log_matching()` (the follower-side check that decides whether to accept an `AppendEntries`) called `get_log_entry(prev_log_index)`. If `prev_log_index` happened to be *exactly* the snapshot boundary — which it always is on the very first `AppendEntries` a follower receives right after installing a snapshot, since `next_index` gets set to `last_included_index + 1` — `get_log_entry` returns `None` (that entry is compacted, not addressable that way), and the check returned `False`. Rejected, even though the follower's log genuinely *does* match at that point (it just installed exactly that snapshot).
2. The leader's own computation of `prev_log_term` (what term to claim that boundary entry is from) had the identical bug: `get_log_entry(prev_log_index)` returns `None`, so `prev_log_term` silently defaulted to `0` instead of the real term.

Put those together: the leader installs a snapshot on a lagging follower, then immediately sends it the next `AppendEntries` — which the follower's own `_check_log_matching` rejects, because neither side's code knew how to talk about the boundary index correctly. The leader interprets the rejection as "this follower is even further behind than I thought," decrements `next_index` back down — right onto the snapshot boundary again — which re-triggers `InstallSnapshot` on the *next* heartbeat. Forever. A follower that's ever received one snapshot can never accept a normal `AppendEntries` again.

**Why this had never been caught**: every earlier live test in this project (BatchPutPrice, metrics verification, the first `chaos_test.py` attempt) involved a small number of writes — comfortably under the 50-entry snapshot threshold. Nobody had actually exercised `InstallSnapshot`'s *aftermath* live before. The unit tests for snapshotting (Part 4, Step 6) tested that a snapshot gets installed correctly — they never went on to test "and then what happens on the *next* `AppendEntries` after that." A gap in the tests' own coverage precisely because nobody had reason to suspect there was more to test.

**Fix**: added `RaftStorage.get_term_at_index(index)` — a small helper that returns `last_included_term` when `index` is exactly the snapshot boundary, and only falls through to `get_log_entry` otherwise. Applied it at all three places that had silently assumed `get_log_entry` was complete: the follower's log-matching check, and *two* separate places on the leader side that compute `prev_log_term` (the batch-flush path and the heartbeat path both do this independently — same bug, duplicated). A fourth, related spot (`_update_commit_index`'s check that a majority-replicated entry is from the current term) had the identical blind spot for a different reason — a follower caught up via `InstallSnapshot` has its `match_index` set to exactly the boundary — fixed the same way.

**The teaching point**: "the entry at this index" is not a uniform concept once you've introduced compaction. Some code (deciding what to send, what to accept, what to consider committed) needs to reason about indices that exist *conceptually* — as part of the cluster's history — even once they're no longer *physically* present in the in-memory log. Anywhere your code does `get_log_entry(some_index)` and treats `None` as "that index doesn't exist," you have to ask: doesn't exist, or *isn't stored here anymore*? Those are different facts with different correct handling, and only one, `get_term_at_index`, was written to know the difference in this codebase.

### 9. A second bug hiding behind the first: `next_index` could overshoot what a follower actually has

Found immediately after fixing bug #8, while re-running the exact benchmark that had first exposed it — the error rate dropped from 50% to a lower but still-nonzero number, which was the tell that something *else* was also wrong.

**Symptom**: with #8 fixed, writes mostly succeeded, but concurrent writes under load (`benchmark.py --concurrency 5`) still failed intermittently. Temporary diagnostic logging (added specifically to chase this down — see the note on debuggability below) showed the leader sending `AppendEntries` with `prev_log_index=15`, while every single follower's real log only had 10 entries. The leader's bookkeeping had drifted five entries ahead of reality.

**Root cause**: `_flush_pending_entries` (Part 4, Step 5) takes a lock only long enough to snapshot and clear the pending-entries queue — the actual network replication happens *after* releasing that lock. That means two separate flushes for the *same follower* can genuinely have RPCs in flight to it at the same time. The old success-handling code was:

```python
self.next_index[peer.node_id] = len(entries) + self.next_index[peer.node_id]
```

This reads `next_index` *at response time*, not at request time. If flush A's response is processed first (correctly advancing `next_index` from 6 to 11), and flush B's response — for a *different*, earlier-computed batch that also started from `next_index=6` — is processed *after*, B's handler reads the *already-updated* value (11) and adds its own 5 on top: `next_index = 5 + 11 = 16`. Double-counted. The leader now believes the follower has 5 more entries than it actually does — exactly the `prev_log_index=15` vs. real `10` seen live.

**Fix**: compute the update as an *absolute* value derived from that specific RPC's own `prev_log_index` (`prev_log_index + len(entries) + 1`), and only ever move `next_index`/`match_index` forward — `max(current, new)`, never a plain assignment. A stale, late-arriving success response can then never regress or over-advance the leader's view of reality; the freshest correct value always wins, regardless of response arrival order. Applied to both places this pattern existed (the batch-flush path and the heartbeat-catch-up path — the same duplication-of-logic issue as bug #8).

**Confirming the fix, and confirming the test would have caught the bug**: the regression test (`test_next_index_does_not_overshoot_on_concurrent_stale_success`) uses `asyncio.gather` on two calls with a real `await asyncio.sleep(...)` inside the mocked RPC — deliberately forcing both calls to read the stale `next_index` before either one's response lands, which is exactly what made this a race in production. A version of this test written with plain sequential `await`s (call A, wait for it to fully finish, *then* call B) would not have caught the bug at all — B would simply see A's already-correct update and never manifest the race. This is worth remembering as a general rule for testing races: if your test's calls are actually sequential under the hood, you have proven nothing about what happens when they're not.

**On the leftover residual**: after this fix, a small error rate (3–10%) remained under sustained high concurrency (10+ writers) — but diagnostic logging showed a *different*, much smaller pattern (`prev_log_index` off by exactly 1, not 5), which is normal, self-healing Raft behavior under load (a follower transiently one entry behind a fast-moving leader, correctly rejected, correctly retried next round) rather than a bug. The distinction that matters: is data ever corrupted or wrongly visible? No — confirmed by directly reading back what a "failed" write's keys held on multiple nodes: nothing, cleanly. The system was never unsafe, only occasionally unavailable for a specific write attempt under heavy load, which the client-facing code already reports honestly (`ok=False`) rather than lying about. Knowing when to stop chasing a finding — once you've confirmed it's safe, and the remaining signal looks like the system's own retry/backoff protocol working as designed rather than a new defect — is as much a skill as finding the bug in the first place.

**On debuggability, again**: this bug was hard to find precisely because `_check_log_matching`'s rejection was silent — it told the caller "no" and nothing else. The single biggest unlock in finding both bug #8 and #9 was adding one `logger.warning(...)` line at the rejection point, printing exactly what the leader claimed vs. what the follower actually had. That line is now a permanent part of the codebase, not a debugging scaffold that got removed — the same lesson bug-chain #7 already taught (Part 6, entry #7): a check that fails without saying why is a check that will cost you hours the next time something goes wrong behind it.

---

## Part 7 — Testing Strategy: What Runs When, and What Each Layer Can (and Can't) Catch

Think of this as a pyramid, cheapest/fastest at the bottom, most expensive/realistic at the top. You need all of it — each layer catches a different *class* of bug.

### Layer 1 — Unit tests (`tests/*.py`, pytest, ~3.8s for all 157)

Run on every change, take seconds. Test one function's logic in isolation, with its dependencies (network, sometimes disk) mocked out — e.g. "given these `match_index` values, does `_update_commit_index` compute the right majority index?" or "does `handle_vote_request` correctly deny a vote when the term is stale?"

**What they're good at**: fast feedback on pure logic, edge cases (empty log, single node, exact majority boundary), regression protection once a bug is found (every bug fixed this session got a matching test — e.g. `tests/test_snapshotting.py` for the whole new feature).

**What they structurally can't catch**: anything that only emerges from the *timing and interleaving* of multiple real, independent processes — like bug #6 above. A mock responds instantly and deterministically; it can't accidentally reproduce a race between two concurrently-running loops.

**A trap to know about, since this project hit it repeatedly**: over-mocking. If you mock the exact thing you're trying to test, or mock a method to return a fixed value regardless of what actually happened, your test can pass while the real code is broken (see Part 6, bugs #2 and #5). Prefer mocking at the *boundary* (network calls, wall-clock time) and letting real logic run through real objects wherever practical — several fixes this session replaced `Mock()` with `Mock(wraps=real_method)` specifically to keep tests honest.

### Layer 2 — Live single-cluster testing (`docker compose up`, real network, real timing)

Slower (minutes, not seconds), but this is the only layer that exercises real concurrent processes, real (if fast) network latency, and real OS/asyncio scheduling. This is how bug #6 was actually found: build the real cluster, kill a real container, and watch what the *other* real containers actually do.

**What to check here, concretely** (see `README.md`'s "Running the Cluster" and "Manual failover test" sections for the exact commands): does a fresh cluster converge on exactly one leader within a reasonable time? Does a write through the leader show up correctly when read from a distant follower? After killing the leader, does a new one get elected, and is previously-committed data intact on it?

### Layer 3 — Chaos testing (deliberately injecting failure)

A distributed system's entire value proposition is *surviving failure* — so failure is exactly what you have to test, not just the happy path. `scripts/chaos_test.py` exists for this (leader-crash-during-write, follower-catchup-after-restart, concurrent-writes-during-leader-change) — though as documented in the README's Known Issues, it currently has a real bug of its own (it always targets `node1` regardless of which node it means to target) and needs fixing before it's trustworthy. The manual failover recipe in the README is the reliable stand-in for now.

### Why this matters as a takeaway, not just a fact about this project

If you only ever run Layer 1, you will ship bugs like #6 with 100% green tests and total confidence. The unit test suite passing is necessary, not sufficient, evidence that a distributed system works. This project's own history is the proof: 140/140 passing, and there was still a bug that made committed writes invisible to some followers indefinitely. Nothing about "tests pass" implies "the distributed algorithm is correct under real timing" — only running the real thing, and deliberately breaking it, can tell you that.

---

## Part 8 — Reconstructing This From Zero: A Build Order Checklist

If you were rebuilding this from scratch, this is the order that actually works — each step is checkpointed with what to verify before moving to the next, because building step *N+1* on top of a broken step *N* just compounds the confusion when something fails.

1. **Write the proto contracts** (`proto/raft.proto`, `proto/client.proto`). Generate stubs (`scripts/gen_protos.sh`). *Checkpoint: the generated `_pb2.py` files import cleanly.*
2. **Define the vocabulary** (`raft/types.py`): `RaftState`, `LogEntry`, `PeerInfo`, timing constants.
3. **Build leader election in isolation** (`raft/election.py`), with the network layer mocked. Get a single node to time out and become a candidate. Get two mock "peers" to grant/deny votes correctly, *including the log-up-to-date check from day one* (don't repeat this project's bug — build it in from the start now that you know why it matters). *Checkpoint: `tests/test_election.py`-equivalent — one node reliably becomes leader given enough mock votes; a node with a shorter/older log never wins against one with a longer/newer log.*
4. **Build durable storage** (`raft/storage.py`): the log, metadata, atomic writes. *Checkpoint: kill the process mid-write (or just don't call the save at all) and confirm a restart never sees a corrupted file — only the last successfully-committed atomic write.*
5. **Build the state machine** (`kv/state_machine.py`), completely ignorant of Raft — just "apply this command to this dict, idempotently." *Checkpoint: applying the same command twice is a no-op.*
6. **Wire election + storage + state machine together into `RaftNode`** (`raft/node.py`): the replication path (`AppendEntries` send/handle), commit-index advancement, crash recovery. *Checkpoint: with the network still mocked, a 3-node in-process cluster (`tests/test_replication.py`-equivalent) replicates a write to majority and applies it on all three; a follower with a conflicting log entry gets it correctly overwritten by the leader's version.*
7. **Wire it to real gRPC** (`server/grpc_server.py`, `server/cluster_boot.py`, `server/main.py`). *Checkpoint: run 3 processes on localhost with different ports (no Docker yet), and drive them with a simple client script.*
8. **Containerize and compose** (`ops/Dockerfile`, generated `ops/docker-compose.yml`). *Checkpoint: `docker compose up`, a leader gets elected, `kvctl.py put-price`/`get-price` works end-to-end across real containers.*
9. **Add (network) batching** — and build the future-based confirmation in from the start this time, now that you've read Part 6 bug #3. *Checkpoint: a `put_price` call genuinely waits for and reports the real replication outcome, verified by a test that makes replication fail and checking the client sees `ok=False`.*
10. **Add command batching (`BatchPutPrice`)** while network batching is still fresh in mind — they're easy to conflate, so build them close together and make sure your own mental model keeps them straight (see Part 4 Step 8's table). Since the state machine's `apply_command` should already handle a multi-write command type generically (build that in at step 5, not as an afterthought), this step should mostly be plumbing: a serialize function that takes a list, one `RaftNode` method, one gRPC handler. *Checkpoint: a single `BatchPutPrice` call with N entries produces exactly one log entry (assert on log length before/after, not just that the data landed), and all N writes are visible after that one entry commits.*
11. **Add snapshotting/compaction** (`InstallSnapshot`, `raft/storage.py`'s snapshot methods). *Checkpoint: force a snapshot at a tiny threshold, confirm the log actually shrinks, restart a node and confirm it recovers correctly from the snapshot rather than the (now-gone) old log entries.*
12. **Add metrics/structured logging** — last, because they're observability, not correctness; nothing else should depend on them existing.
13. **Scale up and actually run it live.** Generate a bigger cluster. Kill the leader. Write through the new one. Read from the farthest node. If something's wrong, it'll show up here, in a way no unit test could have told you — go back and re-read Part 7 if that surprises you.
14. **Write the chaos tests last**, once you trust the happy path enough to be confident that a chaos test failure means something real broke, not that your test harness is unreliable.

If you can walk through all 14 steps, verify each checkpoint, and understand *why* each one had to come before the next — you understand this project well enough to have built it.
