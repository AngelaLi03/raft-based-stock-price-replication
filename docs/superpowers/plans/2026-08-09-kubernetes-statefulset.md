# Kubernetes StatefulSet Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the existing Raft cluster on Kubernetes as a StatefulSet with stable per-pod identity, per-pod persistent volumes, a real Raft-aware readiness probe, and a PodDisruptionBudget that provably prevents a rollout from breaking quorum.

**Architecture:** A new `scripts/gen_k8s_manifests.py` generates five manifests into `ops/k8s/` (StatefulSet, headless Service, ClusterIP Service, PodDisruptionBudget, ConfigMap), mirroring the existing `gen_docker_compose.py` pattern. Three small application changes make the code work under StatefulSet naming: a naming-scheme-independent metrics-port derivation, `NODE_ID` derivation from the Kubernetes downward API, and a new `/ready` endpoint reflecting actual Raft state. Docker Compose remains fully supported and untouched in behavior.

**Tech Stack:** Kubernetes (StatefulSet, headless Service, PodDisruptionBudget, PersistentVolumeClaim templates, downward API), kind (Kubernetes-in-Docker), kubectl, Python 3.11, aiohttp.

## Global Constraints

- **Docker Compose is not replaced.** It remains fully supported. Every change must keep the Compose path working identically — verified by the existing test suite plus an explicit Compose smoke check.
- **Local verification uses N=5, not 15.** `--nodes N` stays parametric (mirroring `gen_docker_compose.py`), but local kind verification uses 5 pods. Quorum for N=5 is 3.
- **PodDisruptionBudget `minAvailable` is exactly `(N // 2) + 1`** — the Raft quorum count. For N=5 that is 3.
- **Peer discovery stays static**, generated at manifest-creation time into a ConfigMap. No k8s API queries, no DNS SRV lookups at runtime.
- **The peer-list wire format does not change**: `node_id:host:raft_port:client_port`, parsed by the existing `parse_peer_list()` in `server/cluster_boot.py`, which needs no modification.
- **Under a StatefulSet every pod uses the same ports** (raft `50051`, client `51051`, metrics `8001`) and differs by hostname. This is the opposite of the Compose scheme, where ports differ per node.
- **No Helm, no cloud deployment, no Ingress, no HorizontalPodAutoscaler.** Each is a deliberate non-goal — see the spec's "Non-goals" for the reasoning (notably: autoscaling a Raft cluster is actively wrong without the joint-consensus membership-change protocol, which this project does not implement).
- Full `pytest` suite (169 tests as of this plan's writing) must stay green throughout every task.
- `ruff check .` must stay clean throughout — CI enforces `test` and `lint` as required checks on `main`.
- This plan executes in an isolated git worktree/branch, never on `main` directly. Commit normally inside the worktree. **Pushing to `origin` requires explicit human go-ahead** — standing rule for this repo.

---

### Task 1: Make the metrics-port derivation independent of the node-ID naming scheme

**Files:**
- Modify: `raft/node.py:52-58`
- Modify: `server/grpc_server.py:335-342`
- Create: `tests/test_node_id_parsing.py`

**Interfaces:**
- Produces: `raft.types.metrics_port_for_node_id(node_id: str, base: int = 8000) -> int` — a single shared helper used by both call sites. Returns `base + N` where `N` is the trailing integer in `node_id`, or `base + 1` when the ID contains no trailing integer.

**Why this task is first and blocking:** both existing call sites derive the metrics port with `int(node_id.replace('node', ''))`. That works for Compose IDs (`node1` → `1`) and breaks for StatefulSet IDs — `'raft-node-0'.replace('node','')` is `'raft--0'`, and `int('raft--0')` raises `ValueError`. The two failure modes are different and both bad:

- `raft/node.py:52-58` catches only `ImportError`, so the `ValueError` propagates and **the node crashes at construction** → `CrashLoopBackOff`.
- `server/grpc_server.py:335-342` catches broad `Exception`, so it **silently logs a warning and never starts the metrics server**. Since Task 3 serves `/ready` from that same server, pods would never become Ready and the StatefulSet rollout would hang forever with no obvious cause.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_node_id_parsing.py`:

```python
"""
Tests for deriving a per-node metrics port from a node ID.

The original implementation was `int(node_id.replace('node', ''))`, which
works for Compose-style IDs ("node1") and raises ValueError for
StatefulSet-style IDs ("raft-node-0" -> "raft--0"). Under Kubernetes that
meant RaftNode.__init__ crashed outright (it caught only ImportError) and
GrpcServer silently skipped starting the metrics server (it caught broad
Exception) - which would have left readiness probes unanswered forever.
"""

import pytest

from raft.types import metrics_port_for_node_id


@pytest.mark.parametrize("node_id,expected", [
    ("node1", 8001),
    ("node9", 8009),
    ("node15", 8015),
])
def test_compose_style_ids_keep_their_existing_ports(node_id, expected):
    """The Compose path must be completely unchanged by this fix."""
    assert metrics_port_for_node_id(node_id) == expected


@pytest.mark.parametrize("node_id,expected", [
    ("raft-node-0", 8000),
    ("raft-node-1", 8001),
    ("raft-node-4", 8004),
])
def test_statefulset_style_ids_parse_instead_of_raising(node_id, expected):
    """StatefulSet pod names are `<statefulset>-<ordinal>`; the trailing
    ordinal is the number we want, and it must not raise."""
    assert metrics_port_for_node_id(node_id) == expected


def test_id_with_no_trailing_number_falls_back_instead_of_raising():
    """Never raise - a bad port is recoverable, a crash at construction is not."""
    assert metrics_port_for_node_id("leader") == 8001


def test_base_is_configurable():
    assert metrics_port_for_node_id("node3", base=9000) == 9003
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_node_id_parsing.py -v`
Expected: FAIL — `ImportError: cannot import name 'metrics_port_for_node_id' from 'raft.types'`

- [ ] **Step 3: Add the helper to `raft/types.py`**

Append to `raft/types.py` (it already imports `re`? it does not — add `import re` alongside the existing imports at the top of the file):

```python
def metrics_port_for_node_id(node_id: str, base: int = 8000) -> int:
    """Derive this node's metrics port from its ID.

    Handles both naming schemes this project uses: Docker Compose's
    "node1"/"node15" and Kubernetes StatefulSet's "raft-node-0". Takes the
    trailing integer in the ID, so "node15" -> 15 and "raft-node-0" -> 0.

    Never raises: an ID with no trailing digits falls back to offset 1.
    The previous implementation, `int(node_id.replace('node', ''))`, raised
    ValueError on StatefulSet-style IDs - which crashed RaftNode outright
    and silently disabled the metrics server (and with it the readiness
    probe) in GrpcServer.
    """
    match = re.search(r'(\d+)$', node_id)
    return base + (int(match.group(1)) if match else 1)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_node_id_parsing.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Use the helper at both call sites**

In `raft/node.py`, replace the metrics-init block (currently around lines 52-58):

```python
        # Initialize metrics
        try:
            from raft.prometheus_metrics import init_prometheus_metrics
            # Use different ports for each node to avoid conflicts
            metrics_port = 8000 + int(node_id.replace('node', ''))
            init_prometheus_metrics(node_id, metrics_port)
        except ImportError:
            logger.warning("Prometheus metrics not available")
```

with:

```python
        # Initialize metrics
        try:
            from raft.prometheus_metrics import init_prometheus_metrics
            from .types import metrics_port_for_node_id
            # Use different ports for each node to avoid conflicts
            metrics_port = metrics_port_for_node_id(node_id)
            init_prometheus_metrics(node_id, metrics_port)
        except ImportError:
            logger.warning("Prometheus metrics not available")
```

In `server/grpc_server.py`, replace the metrics-server block (currently around lines 335-342):

```python
        # Start metrics server
        try:
            from server.metrics_server import start_metrics_server
            metrics_port = 8000 + int(self.raft_node.node_id.replace('node', ''))
            await start_metrics_server(metrics_port)
            logger.info(f"Metrics server started on port {metrics_port}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")
```

with:

```python
        # Start metrics server
        try:
            from server.metrics_server import start_metrics_server
            from raft.types import metrics_port_for_node_id
            metrics_port = metrics_port_for_node_id(self.raft_node.node_id)
            await start_metrics_server(metrics_port)
            logger.info(f"Metrics server started on port {metrics_port}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")
```

- [ ] **Step 6: Verify nothing regressed**

```bash
PYTHONPATH=. .venv311/bin/python3 -m pytest tests/ -q
.venv311/bin/ruff check .
```

Expected: 173 passed (169 existing + 4 new parametrized cases expand to 8 test instances — report the real number you observe rather than assuming this one), `All checks passed!`

- [ ] **Step 7: Commit**

```bash
git add raft/types.py raft/node.py server/grpc_server.py tests/test_node_id_parsing.py
git commit -m "Derive metrics port from node ID without assuming the Compose naming scheme

int(node_id.replace('node','')) works for 'node1' but raises ValueError
for StatefulSet names like 'raft-node-0' ('raft--0'). Two different
failure modes, both bad: RaftNode.__init__ caught only ImportError so it
crashed outright, and GrpcServer caught broad Exception so it silently
never started the metrics server - which serves the readiness probe, so
pods would have hung un-Ready forever with no obvious cause."
```

---

### Task 2: Derive `NODE_ID` from the Kubernetes downward API

**Files:**
- Modify: `server/cluster_boot.py:47-92` (`get_cluster_config`)
- Create: `tests/test_cluster_boot.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `get_cluster_config()` now resolves the node's identity from `NODE_ID` if set, else from `POD_NAME`, and raises `ValueError` if neither is set. `parse_peer_list()` is unchanged.

**Why:** Compose sets `NODE_ID` explicitly per service because each service has its own spec. A StatefulSet's pods all share one pod template, so `NODE_ID` cannot be hardcoded — each pod gets its identity from the downward API, which exposes `metadata.name` (e.g. `raft-node-0`) as an env var. `server/cluster_boot.py` currently has no test coverage at all, so this task adds the first tests for it.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_cluster_boot.py`:

```python
"""
Tests for server/cluster_boot.py's environment-driven cluster configuration.

This module had no test coverage before the Kubernetes work. The identity
resolution matters because Docker Compose sets NODE_ID explicitly per
service, while a StatefulSet's pods share one pod template and must derive
identity from the downward API (metadata.name -> POD_NAME).
"""

import pytest

from server.cluster_boot import get_cluster_config, parse_peer_list


def test_explicit_node_id_wins(monkeypatch):
    """The Compose path must be unchanged: NODE_ID is used as-is."""
    monkeypatch.setenv("NODE_ID", "node2")
    monkeypatch.setenv("POD_NAME", "raft-node-9")
    monkeypatch.setenv("PEER_LIST", "node1:node1:50051:51051,node2:node2:50052:51052")

    config = get_cluster_config()

    assert config["node_id"] == "node2"


def test_falls_back_to_pod_name_when_node_id_absent(monkeypatch):
    """The Kubernetes path: identity comes from the downward API."""
    monkeypatch.delenv("NODE_ID", raising=False)
    monkeypatch.setenv("POD_NAME", "raft-node-0")
    monkeypatch.setenv(
        "PEER_LIST",
        "raft-node-0:raft-node-0.raft:50051:51051,"
        "raft-node-1:raft-node-1.raft:50051:51051",
    )

    config = get_cluster_config()

    assert config["node_id"] == "raft-node-0"


def test_self_is_excluded_from_peers_under_statefulset_naming(monkeypatch):
    """A node must never treat itself as a peer - the quorum math breaks."""
    monkeypatch.delenv("NODE_ID", raising=False)
    monkeypatch.setenv("POD_NAME", "raft-node-0")
    monkeypatch.setenv(
        "PEER_LIST",
        "raft-node-0:raft-node-0.raft:50051:51051,"
        "raft-node-1:raft-node-1.raft:50051:51051,"
        "raft-node-2:raft-node-2.raft:50051:51051",
    )

    config = get_cluster_config()

    peer_ids = [p.node_id for p in config["peers"]]
    assert "raft-node-0" not in peer_ids
    assert peer_ids == ["raft-node-1", "raft-node-2"]


def test_raises_when_neither_node_id_nor_pod_name_is_set(monkeypatch):
    monkeypatch.delenv("NODE_ID", raising=False)
    monkeypatch.delenv("POD_NAME", raising=False)

    with pytest.raises(ValueError):
        get_cluster_config()


def test_parse_peer_list_handles_statefulset_dns_names():
    """Same ports on every peer, differing by hostname - the inverse of the
    Compose scheme, and the existing parser must handle it unchanged."""
    peers = parse_peer_list(
        "raft-node-0:raft-node-0.raft:50051:51051,"
        "raft-node-1:raft-node-1.raft:50051:51051"
    )

    assert peers == [
        {"node_id": "raft-node-0", "host": "raft-node-0.raft",
         "raft_port": 50051, "client_port": 51051},
        {"node_id": "raft-node-1", "host": "raft-node-1.raft",
         "raft_port": 50051, "client_port": 51051},
    ]
```

- [ ] **Step 2: Run the tests to verify the right ones fail**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_cluster_boot.py -v`
Expected: `test_explicit_node_id_wins`, `test_raises_when_neither...` and `test_parse_peer_list_handles_statefulset_dns_names` PASS already (existing behavior); `test_falls_back_to_pod_name_when_node_id_absent` and `test_self_is_excluded_from_peers_under_statefulset_naming` FAIL with `ValueError: NODE_ID environment variable is required`.

- [ ] **Step 3: Add the fallback**

In `server/cluster_boot.py`'s `get_cluster_config()`, replace:

```python
    # Get node identity
    node_id = os.getenv("NODE_ID")
    if not node_id:
        raise ValueError("NODE_ID environment variable is required")
```

with:

```python
    # Get node identity. Docker Compose sets NODE_ID explicitly per service.
    # A Kubernetes StatefulSet can't - all its pods share one pod template -
    # so each pod supplies its own name via the downward API instead
    # (metadata.name, e.g. "raft-node-0"), which is stable across restarts.
    node_id = os.getenv("NODE_ID") or os.getenv("POD_NAME")
    if not node_id:
        raise ValueError(
            "Node identity is required: set NODE_ID (Docker Compose) or "
            "POD_NAME via the downward API (Kubernetes)"
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_cluster_boot.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Verify nothing regressed**

```bash
PYTHONPATH=. .venv311/bin/python3 -m pytest tests/ -q
.venv311/bin/ruff check .
```

Expected: all tests pass, `All checks passed!`

- [ ] **Step 6: Commit**

```bash
git add server/cluster_boot.py tests/test_cluster_boot.py
git commit -m "Derive node identity from POD_NAME when NODE_ID isn't set

Compose sets NODE_ID per service; a StatefulSet's pods share one pod
template and must get identity from the downward API instead. Adds the
first test coverage this module has ever had, including that a node
never lists itself as a peer under StatefulSet naming."
```

---

### Task 3: Raft-aware `/ready` readiness endpoint

**Files:**
- Modify: `server/metrics_server.py` (whole file: `MetricsServer.__init__`, new handler, `start_metrics_server`)
- Modify: `server/grpc_server.py:335-342` (pass the node into `start_metrics_server`)
- Modify: `tests/test_metrics_server.py`

**Interfaces:**
- Consumes: `metrics_port_for_node_id` from Task 1 (already wired into `grpc_server.py`).
- Produces: `MetricsServer(port: int, raft_node=None)` — optional node reference; `MetricsServer.handle_ready(request)`; `start_metrics_server(port: int = 8000, raft_node=None)`.

**Why:** `/health` returns `"OK"` unconditionally — a fine liveness probe ("is the process alive"), but useless as a readiness probe ("should this pod receive traffic"). `/ready` returns 200 once the node has a settled role (`FOLLOWER` or `LEADER`), and 503 during the startup window before the first election resolves. `MetricsServer` currently has no view of Raft state, so it needs an optional node reference.

Note `RaftNode.state` is a `RaftState` enum whose `.value` is the lowercase string (`"follower"`, `"candidate"`, `"leader"`) — see `raft/types.py`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_metrics_server.py`:

```python
@pytest.mark.asyncio
async def test_handle_ready_returns_503_without_a_node_reference():
    """No node wired in means we can't assert readiness - fail closed."""
    server = MetricsServer(port=0)

    response = await server.handle_ready(MagicMock())

    assert response.status == 503


@pytest.mark.asyncio
async def test_handle_ready_returns_503_while_still_a_candidate():
    """Mid-election, the node can't usefully serve traffic yet."""
    from raft.types import RaftState

    node = MagicMock()
    node.state = RaftState.CANDIDATE
    server = MetricsServer(port=0, raft_node=node)

    response = await server.handle_ready(MagicMock())

    assert response.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("state_name", ["FOLLOWER", "LEADER"])
async def test_handle_ready_returns_200_once_role_is_settled(state_name):
    """A settled follower serves reads; a leader serves writes. Both ready."""
    from raft.types import RaftState

    node = MagicMock()
    node.state = getattr(RaftState, state_name)
    server = MetricsServer(port=0, raft_node=node)

    response = await server.handle_ready(MagicMock())

    assert response.status == 200
    assert state_name.lower().encode() in response.body.lower()


@pytest.mark.asyncio
async def test_health_stays_unconditional_ok():
    """Liveness must NOT become role-aware - a candidate is alive, just not
    ready, and restarting it mid-election would be actively harmful."""
    from raft.types import RaftState

    node = MagicMock()
    node.state = RaftState.CANDIDATE
    server = MetricsServer(port=0, raft_node=node)

    response = await server.handle_health(MagicMock())

    assert response.status == 200
    assert response.body == b"OK"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_metrics_server.py -v`
Expected: the four new tests FAIL — `TypeError: __init__() got an unexpected keyword argument 'raft_node'` / `AttributeError: 'MetricsServer' object has no attribute 'handle_ready'`. The three pre-existing tests still PASS.

- [ ] **Step 3: Add the node reference and the `/ready` route**

In `server/metrics_server.py`, replace `MetricsServer.__init__`:

```python
    def __init__(self, port: int = 8000):
        self.port = port
        self.app = web.Application()
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.app.router.add_get('/health', self.handle_health)
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
```

with:

```python
    def __init__(self, port: int = 8000, raft_node=None):
        self.port = port
        # Optional so the Compose path and unit tests can construct this
        # without a node; /ready fails closed (503) when it's absent.
        self.raft_node = raft_node
        self.app = web.Application()
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.app.router.add_get('/health', self.handle_health)
        self.app.router.add_get('/ready', self.handle_ready)
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
```

- [ ] **Step 4: Add the `handle_ready` handler**

In `server/metrics_server.py`, immediately after the existing `handle_health` method, add:

```python
    async def handle_ready(self, request: web_request.Request) -> web_response.Response:
        """Handle /ready endpoint - the Kubernetes readiness probe.

        Distinct from /health (liveness) on purpose. Liveness answers "is
        this process alive"; readiness answers "should this pod receive
        traffic". A node that hasn't finished its first election has no
        useful answer for anyone: it doesn't know who the leader is, so it
        can neither serve writes nor vouch for its reads. Once it settles
        into a role - follower or leader - it's ready.

        Deliberately NOT checking commit_index freshness: that would need
        the cluster's true commit_index to compare against, which is a
        meaningfully harder problem and isn't what readiness is for here.
        """
        from raft.types import RaftState

        if self.raft_node is None:
            return web.Response(status=503, text="not ready: no node reference")

        state = getattr(self.raft_node, "state", None)
        if state in (RaftState.FOLLOWER, RaftState.LEADER):
            return web.Response(status=200, text=f"ready: {state.value}")

        state_name = state.value if isinstance(state, RaftState) else "unknown"
        return web.Response(status=503, text=f"not ready: {state_name}")
```

- [ ] **Step 5: Thread the node through `start_metrics_server`**

In `server/metrics_server.py`, replace:

```python
async def start_metrics_server(port: int = 8000) -> None:
    """Start the global metrics server."""
    global _metrics_server
    _metrics_server = MetricsServer(port)
    await _metrics_server.start()
```

with:

```python
async def start_metrics_server(port: int = 8000, raft_node=None) -> None:
    """Start the global metrics server.

    raft_node is optional and only used to answer /ready; passing it is what
    makes the readiness probe meaningful rather than always-503.
    """
    global _metrics_server
    _metrics_server = MetricsServer(port, raft_node=raft_node)
    await _metrics_server.start()
```

Then in `server/grpc_server.py`, change the one call site so the node is actually passed:

```python
            await start_metrics_server(metrics_port)
```

to:

```python
            await start_metrics_server(metrics_port, raft_node=self.raft_node)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_metrics_server.py -v`
Expected: PASS (8 test instances — 3 pre-existing + 5 new, since one is parametrized over two states)

- [ ] **Step 7: Verify nothing regressed**

```bash
PYTHONPATH=. .venv311/bin/python3 -m pytest tests/ -q
.venv311/bin/ruff check .
```

Expected: all tests pass, `All checks passed!`

- [ ] **Step 8: Commit**

```bash
git add server/metrics_server.py server/grpc_server.py tests/test_metrics_server.py
git commit -m "Add Raft-aware /ready endpoint for the Kubernetes readiness probe

/health stays an unconditional-OK liveness check (a candidate is alive,
just not useful yet - restarting it mid-election would be harmful).
/ready is 200 only once the node has settled into follower or leader,
503 during the pre-election startup window, and fails closed when no
node reference is wired in."
```

---

### Task 4: Manifest generator

**Files:**
- Create: `scripts/gen_k8s_manifests.py`
- Create: `ops/k8s/raft-cluster.yaml` (generated output, committed)
- Modify: `tests/test_scripts_importable.py`

**Interfaces:**
- Consumes: the `NODE_ID`/`POD_NAME` behavior from Task 2 and the `/ready` endpoint from Task 3 — the generated manifests depend on both existing.
- Produces: `scripts/gen_k8s_manifests.py` with `build_peer_list(num_nodes: int) -> str`, `quorum(num_nodes: int) -> int`, and `render(num_nodes: int) -> str`.

**Why generated rather than hand-written:** the same reason `ops/docker-compose.yml` is generated — hand-maintaining N near-identical blocks is exactly how the port and peer-list mistakes documented in this project's history crept in.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_scripts_importable.py`:

```python
def test_gen_k8s_manifests_module_imports():
    import scripts.gen_k8s_manifests as gen
    assert hasattr(gen, "render")
    assert hasattr(gen, "build_peer_list")
    assert hasattr(gen, "quorum")


def test_k8s_quorum_is_strict_majority():
    """The PodDisruptionBudget's whole purpose is encoding this number - if
    it's wrong, a rolling update can silently take the cluster below quorum."""
    from scripts.gen_k8s_manifests import quorum
    assert quorum(1) == 1
    assert quorum(3) == 2
    assert quorum(5) == 3
    assert quorum(15) == 8


def test_k8s_peer_list_uses_headless_dns_and_uniform_ports():
    """Inverse of the Compose scheme: every pod shares the same ports and
    differs by hostname, which only resolves via the headless Service."""
    from scripts.gen_k8s_manifests import build_peer_list
    peer_list = build_peer_list(3)

    assert peer_list == (
        "raft-node-0:raft-node-0.raft:50051:51051,"
        "raft-node-1:raft-node-1.raft:50051:51051,"
        "raft-node-2:raft-node-2.raft:50051:51051"
    )


def test_k8s_render_emits_all_five_manifests_and_correct_pdb():
    import yaml
    from scripts.gen_k8s_manifests import render

    docs = [d for d in yaml.safe_load_all(render(5)) if d]
    kinds = [d["kind"] for d in docs]

    assert kinds.count("StatefulSet") == 1
    assert kinds.count("Service") == 2          # headless + ClusterIP
    assert kinds.count("PodDisruptionBudget") == 1
    assert kinds.count("ConfigMap") == 1

    pdb = next(d for d in docs if d["kind"] == "PodDisruptionBudget")
    assert pdb["spec"]["minAvailable"] == 3     # quorum of 5

    sts = next(d for d in docs if d["kind"] == "StatefulSet")
    assert sts["spec"]["replicas"] == 5
    assert sts["spec"]["serviceName"] == "raft"
    # Identity must come from the downward API, not a hardcoded NODE_ID.
    env_names = [e["name"] for e in sts["spec"]["template"]["spec"]["containers"][0]["env"]]
    assert "POD_NAME" in env_names
    # Each pod needs its own volume, not a shared one.
    assert sts["spec"]["volumeClaimTemplates"][0]["metadata"]["name"] == "data"


def test_k8s_probes_use_the_right_endpoints():
    """Liveness on /health, readiness on /ready - swapping them would make
    k8s restart pods mid-election instead of just withholding traffic."""
    import yaml
    from scripts.gen_k8s_manifests import render

    docs = [d for d in yaml.safe_load_all(render(3)) if d]
    sts = next(d for d in docs if d["kind"] == "StatefulSet")
    container = sts["spec"]["template"]["spec"]["containers"][0]

    assert container["livenessProbe"]["httpGet"]["path"] == "/health"
    assert container["readinessProbe"]["httpGet"]["path"] == "/ready"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_scripts_importable.py -v`
Expected: the five new tests FAIL — `ModuleNotFoundError: No module named 'scripts.gen_k8s_manifests'`. Pre-existing tests still pass.

- [ ] **Step 3: Confirm PyYAML is available for the tests**

The new tests import `yaml`. Check whether it's already a dependency:

```bash
grep -i "yaml" requirements.txt || echo "NOT PRESENT"
.venv311/bin/python3 -c "import yaml; print(yaml.__version__)"
```

If it imports fine but isn't in `requirements.txt`, add `PyYAML==6.0.1` to `requirements.txt` — CI installs only from `requirements.txt`, so a test importing an undeclared dependency passes locally and fails in CI. If it's already listed, change nothing.

- [ ] **Step 4: Write the generator**

Create `scripts/gen_k8s_manifests.py`:

```python
#!/usr/bin/env python3
"""
Generate ops/k8s/raft-cluster.yaml for an N-node Raft cluster.

Generated rather than hand-written for the same reason ops/docker-compose.yml
is (see scripts/gen_docker_compose.py): hand-maintaining N near-identical
blocks is exactly how peer-list mistakes creep in. Regenerate whenever the
node count changes:

    python3 scripts/gen_k8s_manifests.py --nodes 5 > ops/k8s/raft-cluster.yaml

Note the addressing difference from Docker Compose. Under Compose every node
has a UNIQUE port and shares the host. Under a StatefulSet every pod uses the
SAME ports and differs by HOSTNAME, resolvable only because of the headless
Service (clusterIP: None) - a normal Service would load-balance across pods,
which is useless for peer-to-peer consensus traffic.
"""

import argparse

# Uniform across every pod - pods are isolated from each other by network
# namespace, so unlike the Compose setup there's no port collision to avoid.
RAFT_PORT = 50051
CLIENT_PORT = 51051
METRICS_PORT = 8001

STATEFULSET_NAME = "raft-node"
HEADLESS_SERVICE_NAME = "raft"
CLIENT_SERVICE_NAME = "raft-client"
IMAGE = "raft-node:latest"


def quorum(num_nodes: int) -> int:
    """Strict majority - the number of nodes Raft needs to make progress."""
    return (num_nodes // 2) + 1


def build_peer_list(num_nodes: int) -> str:
    """Each pod's stable DNS name is <pod>.<headless-service>."""
    peers = []
    for i in range(num_nodes):
        node_id = f"{STATEFULSET_NAME}-{i}"
        host = f"{node_id}.{HEADLESS_SERVICE_NAME}"
        peers.append(f"{node_id}:{host}:{RAFT_PORT}:{CLIENT_PORT}")
    return ",".join(peers)


def render(num_nodes: int) -> str:
    peer_list = build_peer_list(num_nodes)
    min_available = quorum(num_nodes)

    return f"""---
# Headless Service: gives each pod an individually-resolvable DNS name
# (raft-node-0.raft, raft-node-1.raft, ...). clusterIP: None is what makes
# this headless; a normal Service would load-balance across pods, which is
# useless for peer-to-peer consensus traffic.
apiVersion: v1
kind: Service
metadata:
  name: {HEADLESS_SERVICE_NAME}
  labels:
    app: raft
spec:
  clusterIP: None
  selector:
    app: raft
  ports:
    - name: raft
      port: {RAFT_PORT}
    - name: client
      port: {CLIENT_PORT}
    - name: metrics
      port: {METRICS_PORT}
---
# Regular ClusterIP Service: one stable address for clients (kvctl), so
# callers don't have to pick a pod.
apiVersion: v1
kind: Service
metadata:
  name: {CLIENT_SERVICE_NAME}
  labels:
    app: raft
spec:
  selector:
    app: raft
  ports:
    - name: client
      port: {CLIENT_PORT}
      targetPort: {CLIENT_PORT}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: raft-config
  labels:
    app: raft
data:
  PEER_LIST: "{peer_list}"
---
# PodDisruptionBudget: this is where Raft's quorum requirement becomes a
# deployment-level guarantee. minAvailable is the strict majority
# ({min_available} of {num_nodes}), so Kubernetes will refuse a voluntary
# disruption (rolling update, node drain) that would drop the cluster below
# the number of nodes it needs to elect a leader and commit entries.
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: raft-pdb
spec:
  minAvailable: {min_available}
  selector:
    matchLabels:
      app: raft
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {STATEFULSET_NAME}
  labels:
    app: raft
spec:
  serviceName: {HEADLESS_SERVICE_NAME}
  replicas: {num_nodes}
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app: raft
  template:
    metadata:
      labels:
        app: raft
    spec:
      terminationGracePeriodSeconds: 10
      containers:
        - name: raft-node
          image: {IMAGE}
          # kind loads images into its nodes directly (kind load
          # docker-image); Never stops kubelet trying to pull from a
          # registry that doesn't have this image.
          imagePullPolicy: Never
          ports:
            - name: raft
              containerPort: {RAFT_PORT}
            - name: client
              containerPort: {CLIENT_PORT}
            - name: metrics
              containerPort: {METRICS_PORT}
          env:
            # Identity from the downward API: every pod shares this one pod
            # template, so NODE_ID can't be hardcoded the way Compose does
            # it. metadata.name is the stable pod name (raft-node-0, ...).
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: PEER_LIST
              valueFrom:
                configMapKeyRef:
                  name: raft-config
                  key: PEER_LIST
            - name: RAFT_PORT
              value: "{RAFT_PORT}"
            - name: CLIENT_PORT
              value: "{CLIENT_PORT}"
            - name: DATA_DIR
              value: /app/data
            - name: LOG_LEVEL
              value: INFO
          # Liveness: is the process alive. Unconditional OK - a node
          # mid-election is alive and must NOT be restarted.
          livenessProbe:
            httpGet:
              path: /health
              port: {METRICS_PORT}
            initialDelaySeconds: 5
            periodSeconds: 10
          # Readiness: should this pod receive traffic. 503 until the node
          # settles into follower or leader.
          readinessProbe:
            httpGet:
              path: /ready
              port: {METRICS_PORT}
            initialDelaySeconds: 2
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /app/data
  # Per-pod persistent storage. This is the StatefulSet's other half of
  # "stable identity": raft-node-2 restarting reattaches THIS volume and
  # recovers its own log and term, rather than coming back empty.
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=5,
                        help="Number of nodes in the cluster (default: 5)")
    args = parser.parse_args()

    if args.nodes < 1:
        raise SystemExit("--nodes must be >= 1")

    print(render(args.nodes), end="")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=. .venv311/bin/python3 -m pytest tests/test_scripts_importable.py -v`
Expected: PASS (all, including the five new)

- [ ] **Step 6: Generate the manifests and validate them client-side**

```bash
mkdir -p ops/k8s
PYTHONPATH=. .venv311/bin/python3 scripts/gen_k8s_manifests.py --nodes 5 > ops/k8s/raft-cluster.yaml
kubectl apply --dry-run=client -f ops/k8s/raft-cluster.yaml
```

Expected: five lines of `... created (dry run)`. This validates the manifests against real Kubernetes API schemas without needing a cluster — it catches field-name typos and API-version mistakes that a YAML parser alone would not.

- [ ] **Step 7: Verify nothing regressed**

```bash
PYTHONPATH=. .venv311/bin/python3 -m pytest tests/ -q
.venv311/bin/ruff check .
```

Expected: all tests pass, `All checks passed!`

- [ ] **Step 8: Commit**

```bash
git add scripts/gen_k8s_manifests.py ops/k8s/raft-cluster.yaml tests/test_scripts_importable.py requirements.txt
git commit -m "Add Kubernetes manifest generator (StatefulSet, Services, PDB, ConfigMap)

Generated rather than hand-written, same as ops/docker-compose.yml.
Addressing inverts the Compose scheme: every pod shares the same ports
and differs by hostname, which only resolves because of the headless
Service. The PodDisruptionBudget's minAvailable is the strict majority,
making Raft's quorum requirement a deployment-level guarantee."
```

---

### Task 5: Live verification against a real kind cluster

**Files:** none created or modified unless a bug is found — this task verifies Tasks 1-4 against reality.

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: verified-working manifests, plus any bug fixes the live run surfaces.

**Why this task exists:** this project's entire methodology is that passing tests do not prove a distributed system works — every serious bug in its history (the commit-index liveness bug, the InstallSnapshot boundary loop, the `sed -i ''` portability bug) was found by running the real thing. Steps 6 and 7 are the ones that matter; the rest is setup that makes them possible.

**If any step fails:** that is a real finding, not an obstacle to route around. Diagnose it (`kubectl describe pod`, `kubectl logs`), fix the root cause, commit the fix with an explanation, and re-verify. Report what you found — a bug caught here is the most valuable output of this whole plan.

- [ ] **Step 1: Install kind and create a cluster**

```bash
brew install kind
kind create cluster --name raft
kubectl config current-context   # expect: kind-raft
kubectl get nodes                # expect: one Ready node
```

- [ ] **Step 2: Build the image and load it into kind**

```bash
docker build -f ops/Dockerfile -t raft-node:latest .
kind load docker-image raft-node:latest --name raft
```

`kind load` is required and easy to forget: kind's nodes run their own container runtime and cannot see the host Docker daemon's images. Skipping it produces `ErrImageNeverPull` (because the manifests set `imagePullPolicy: Never`).

- [ ] **Step 3: Apply the manifests**

```bash
kubectl apply -f ops/k8s/raft-cluster.yaml
kubectl get pods -w
```

Expected: five pods `raft-node-0` … `raft-node-4` reach `Running`, then `READY 1/1`. Press Ctrl-C once all five are ready.

- [ ] **Step 4: Confirm readiness genuinely lags startup**

This is the check that proves `/ready` reflects real Raft state rather than being another always-OK endpoint. Immediately after applying (or after deleting a pod), watch the READY column:

```bash
kubectl delete pod raft-node-3
kubectl get pods -w
```

Expected: `raft-node-3` shows `Running` with `READY 0/1` for a moment before flipping to `1/1`. If it is `1/1` the instant it is `Running`, the readiness probe is not doing its job — investigate before continuing.

- [ ] **Step 5: Confirm a single leader and working replication**

```bash
# Find the leader by asking each pod its role.
for i in 0 1 2 3 4; do
  echo -n "raft-node-$i: "
  kubectl exec raft-node-$i -- python3 scripts/kvctl.py cluster-info \
    --host localhost --port 51051 2>/dev/null | grep Role || echo "(no answer)"
done

# Write through the leader (substitute the pod that reported Role: leader).
kubectl exec raft-node-<LEADER> -- python3 scripts/kvctl.py put-price K8S 42.0 \
  --host localhost --port 51051

# Read it back from a DIFFERENT pod - proves replication over pod DNS.
kubectl exec raft-node-0 -- python3 scripts/kvctl.py get-price K8S \
  --host localhost --port 51051
```

Expected: exactly one pod reports `Role: leader`; the write succeeds; the read from a different pod returns `42.0`.

- [ ] **Step 6: Verify stable identity and volume reattachment across restart**

This is the entire distinction between a StatefulSet and a Deployment.

```bash
# Note the pre-delete state of a specific pod.
kubectl exec raft-node-2 -- python3 scripts/kvctl.py dump-state \
  --host localhost --port 51051

kubectl delete pod raft-node-2

# Wait for it to come back, then check it again.
kubectl wait --for=condition=Ready pod/raft-node-2 --timeout=90s
kubectl exec raft-node-2 -- python3 scripts/kvctl.py get-price K8S \
  --host localhost --port 51051
```

Expected: the pod returns with the **same name** `raft-node-2` (not a random suffix), and `K8S=42.0` is still readable from it — proving it reattached its own PVC and recovered its prior Raft state rather than starting empty. Confirm the PVC was reused rather than recreated:

```bash
kubectl get pvc
```

Expected: five PVCs named `data-raft-node-0` … `data-raft-node-4`, with `AGE` values older than the just-restarted pod.

- [ ] **Step 7: Verify the PodDisruptionBudget actually blocks a quorum-breaking eviction**

A PDB that has never been tested against a real eviction attempt is just a YAML file that looks correct.

```bash
kubectl get pdb raft-pdb
```

Expected: `MIN AVAILABLE 3`, `ALLOWED DISRUPTIONS 2` (5 pods, quorum 3 → at most 2 may go down voluntarily).

Now attempt to violate it. Evictions — not deletions — are what a PDB governs, so use the eviction API directly:

```bash
# Evicting 2 pods should be allowed (5 - 2 = 3, still quorum).
kubectl delete pod raft-node-4 --dry-run=server
for p in raft-node-3 raft-node-4; do
  kubectl create -f - <<EOF
apiVersion: policy/v1
kind: Eviction
metadata:
  name: $p
  namespace: default
EOF
done

# A THIRD eviction must be REFUSED - it would leave only 2 of 5, below quorum.
kubectl create -f - <<EOF
apiVersion: policy/v1
kind: Eviction
metadata:
  name: raft-node-2
  namespace: default
EOF
```

Expected: the first two evictions succeed; the third fails with an error containing `Cannot evict pod as it would violate the pod's disruption budget`. **That refusal is the deliverable of this entire task** — record the exact message for the documentation in Task 6.

Then let the cluster recover:

```bash
kubectl wait --for=condition=Ready pod --all --timeout=120s
kubectl get pods
```

- [ ] **Step 8: Verify a rolling restart preserves quorum throughout**

```bash
kubectl rollout restart statefulset/raft-node
kubectl rollout status statefulset/raft-node --timeout=300s
```

While it runs, in a second terminal, watch that ready pods never drop below 3:

```bash
while true; do
  echo "$(date +%T) ready=$(kubectl get pods -l app=raft -o json \
    | python3 -c 'import json,sys; pods=json.load(sys.stdin)["items"]; print(sum(1 for p in pods for c in p.get("status",{}).get("conditions",[]) if c["type"]=="Ready" and c["status"]=="True"))')"
  sleep 2
done
```

Expected: the count never falls below 3. Record the minimum observed value for Task 6's documentation.

- [ ] **Step 9: Confirm the Docker Compose path still works**

The Global Constraints require Compose to remain fully supported. Tasks 1-3 touched shared code, so verify it directly rather than assuming:

```bash
kubectl delete -f ops/k8s/raft-cluster.yaml   # free the ports first
cd ops && docker compose up --build --detach && cd ..
sleep 15
PYTHONPATH=. .venv311/bin/python3 scripts/kvctl.py cluster-info --host localhost --port 51051
PYTHONPATH=. .venv311/bin/python3 scripts/kvctl.py put-price COMPOSE 1.0 --host localhost --port <leader_port>
curl -s localhost:8001/health && echo
curl -s -o /dev/null -w "%{http_code}\n" localhost:8001/ready
cd ops && docker compose down -v && cd ..
```

Expected: a leader is elected, the write succeeds, `/health` returns `OK`, and `/ready` returns `200` — confirming Task 1's port fix and Task 3's new endpoint both work under Compose naming too.

- [ ] **Step 10: Run the full suite one more time and commit any fixes**

```bash
PYTHONPATH=. .venv311/bin/python3 -m pytest tests/ -q
.venv311/bin/ruff check .
```

If Steps 1-9 surfaced bugs, each fix should already be committed individually with a message explaining root cause. If nothing needed fixing, there is nothing to commit for this task — say so explicitly in the report rather than inventing a commit.

---

### Task 6: Documentation

**Files:**
- Modify: `README.md`
- Modify: `learn.md`
- Modify: `INTERVIEW_GUIDE.md`
- Modify: `QUICK_REFERENCE.md`

**Interfaces:**
- Consumes: the real findings and observed output from Task 5. This task must document what actually happened, including the exact PDB refusal message and the minimum ready-pod count observed during the rolling restart — not a restatement of this plan's predictions.

**Why:** this project's established convention is that every feature updates all four Markdown documents with real findings, and that every bug found gets documented with root cause and fix. That convention is the reason these docs are worth reading.

- [ ] **Step 1: Add a "Running on Kubernetes" section to `README.md`**

Insert after the existing "### Manual failover test" subsection and before "## Monitoring & Alerting". Write it parallel in style to the existing "Running the Cluster" section (dense, exact commands, real numbers). It must cover:

- Installing kind and creating the cluster (`brew install kind`, `kind create cluster --name raft`)
- Building and loading the image (`kind load docker-image raft-node:latest --name raft`) — and **why** that step is required (kind nodes can't see the host Docker daemon; `imagePullPolicy: Never` means skipping it yields `ErrImageNeverPull`)
- Generating manifests (`python3 scripts/gen_k8s_manifests.py --nodes 5 > ops/k8s/raft-cluster.yaml`) and applying them
- Verifying leader election and replication with the `kubectl exec` commands from Task 5 Step 5
- Tearing down (`kubectl delete -f ops/k8s/raft-cluster.yaml`, `kind delete cluster --name raft`) — and noting that PVCs must be deleted explicitly (`kubectl delete pvc -l app=raft`) since StatefulSet PVCs deliberately outlive their pods, the same class of gotcha as `docker compose down` without `-v` (already documented in this README)
- A note that N=5 is the local default while Compose defaults to 15, and that quorum for 5 is 3

- [ ] **Step 2: Update `README.md`'s Project Structure tree**

Add, in the correct tree positions:

```
│   ├── gen_k8s_manifests.py    # Generates ops/k8s/raft-cluster.yaml for N nodes
```

under `scripts/`, and:

```
├── ops/k8s/                    # Generated Kubernetes manifests (StatefulSet, Services, PDB, ConfigMap)
```

near the existing `ops/` entries.

- [ ] **Step 3: Update `README.md`'s "Fixed this session" and Future Roadmap**

Add a new numbered entry (continuing the existing sequence — check the current highest number rather than assuming) for the node-ID parsing bug from Task 1. It must state: the two call sites, the two *different* failure modes (`raft/node.py` caught only `ImportError` so it crashed outright; `server/grpc_server.py` caught broad `Exception` so it silently skipped starting the metrics server), why the silent one was worse (that server hosts `/ready`, so pods would hang un-Ready forever with no obvious cause), and the fix.

Add any further entries for bugs found during Task 5's live verification.

Then remove Kubernetes from the "Future Roadmap (not started)" section, since it is now done.

- [ ] **Step 4: Add a new Part to `learn.md`**

Append a new Part (check the current highest Part number and continue the sequence) covering the Compose→Kubernetes progression, in the same teaching voice as the existing Parts. It must explain, for a reader who has never used Kubernetes:

- **StatefulSet vs. Deployment** — why a consensus cluster needs stable, ordinal pod identity and cannot use interchangeable pods
- **Headless Services** — what `clusterIP: None` actually does, and why peer-to-peer traffic needs per-pod DNS rather than a load-balanced VIP
- **`volumeClaimTemplates`** — how per-pod PVCs map onto the durable per-node Raft log this project already had, and why a restarted pod reattaching its own volume is what makes crash recovery work under k8s
- **PodDisruptionBudget** — that it is the deployment-layer expression of Raft's quorum rule, with the observed `minAvailable: 3` of 5, and the real refusal message from Task 5 Step 7
- **Liveness vs. readiness** — why `/health` must stay unconditional (restarting a node mid-election is actively harmful) while `/ready` must be role-aware
- **The downward API** — why identity can't be hardcoded when N pods share one pod template

Also add a "Bugs We Hit" entry in the existing Part 6 numbering for the node-ID parsing bug, framed around its real lesson: a string-manipulation assumption about an identifier's *format* silently baked a deployment topology into the code, and the broad `except Exception` turned a crash into an invisible failure — the same silent-failure theme as the existing metrics bug chain.

- [ ] **Step 5: Update `INTERVIEW_GUIDE.md`**

Add a Kubernetes deployment section covering the manifests and what each is for, plus at least one new Q&A. The strongest talking point is the PDB/quorum relationship — an interviewer question like *"How do you deploy a consensus system without downtime?"* has a genuinely good answer here: the deployment layer has to know the quorum math, or a routine rolling update silently becomes an outage.

- [ ] **Step 6: Update `QUICK_REFERENCE.md`**

Add a Kubernetes section (manifests, key commands, the quorum/PDB relationship) and one short Q&A, matching the file's existing terse bulleted style.

- [ ] **Step 7: Verify the docs are accurate**

Re-read each new section against what was actually built and observed. Specifically confirm: every command shown was actually run in Task 5, the PDB numbers match the real `kubectl get pdb` output, and no claim describes behavior that wasn't verified. Then check the test count cited anywhere in `README.md` still matches reality:

```bash
PYTHONPATH=. .venv311/bin/python3 -m pytest tests/ --collect-only -q | tail -1
grep -n "tests" README.md | grep -E "[0-9]{3} tests"
```

Update any stale counts. (This repo has been bitten twice by stale test counts propagating between branches — verify rather than assume.)

- [ ] **Step 8: Commit**

```bash
git add README.md learn.md INTERVIEW_GUIDE.md QUICK_REFERENCE.md
git commit -m "Document the Kubernetes StatefulSet migration

README gets a Running on Kubernetes section, learn.md a new Part on the
Compose->k8s progression (StatefulSet vs Deployment, headless Services,
volumeClaimTemplates, PDB as the deployment-layer expression of quorum,
liveness vs readiness), and both interview docs get k8s sections with the
PDB/quorum talking point. Includes the node-ID parsing bug with root
cause, per this project's convention of documenting every real bug."
```
