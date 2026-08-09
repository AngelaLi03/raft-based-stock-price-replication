# Grafana Dashboards + Alerting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the 15-node Raft cluster a live Grafana "Cluster Health" dashboard (leader/role per node, leader changes, per-follower commit lag, election duration, quorum health) plus two alert rules (no leader, leader flapping), all provisioned as code on top of the existing Prometheus metrics.

**Architecture:** A new, separate `ops/docker-compose.monitoring.yml` runs Prometheus + Grafana, joining the existing (now externally-addressable) `raft-network` so Prometheus can scrape all 15 nodes by service name. Grafana's datasource, dashboard, and alert rules are all mounted config files, not manual UI setup. Two real gaps in the existing metrics code are fixed first: no node ever reports its Raft *role* as a metric, and only the *leader* periodically refreshes its state gauges — followers never do.

**Tech Stack:** Prometheus (official `prom/prometheus` image), Grafana (official `grafana/grafana` image), Docker Compose, `prometheus_client` (already a dependency), `asyncio` for the new periodic metrics tick.

## Global Constraints

- Config-as-code only — no manual clicking in the Grafana UI to reproduce datasource/dashboard/alert setup (from spec's Goals).
- Alerting is visual-only in v1 — no external contact point/notification channel (Slack/webhook/email) (spec's Non-goals).
- One dashboard for v1 ("Cluster Health") — no second detail dashboard (spec's Non-goals).
- Docker Compose only — no Kubernetes in this plan (spec's Non-goals).
- This plan executes in an isolated git worktree/branch (never on `main` directly). Inside that worktree, commit normally at the end of each task — standard practice for this repo's isolated-worktree work. `main` stays untouched until the finished branch is explicitly reviewed and merged.
- Full `pytest` suite (157 existing tests + new ones) must stay green throughout.

---

### Task 1: Make `raft-network` externally addressable

**Files:**
- Modify: `scripts/gen_docker_compose.py:84-86`
- Modify: `ops/docker-compose.yml` (regenerated output, not hand-edited)

**Interfaces:**
- Produces: a Docker network literally named `raft-network` (not Compose-project-prefixed), which Task 4's `ops/docker-compose.monitoring.yml` will reference via `external: true`.

- [ ] **Step 1: Add an explicit `name:` to the generated network block**

In `scripts/gen_docker_compose.py`, the network block is currently generated without a `name:` field, so Docker Compose prefixes the actual network name with the Compose project name (e.g. `ops_raft-network` or similar, depending on invocation directory) — which a separate compose file can't reliably guess to attach to as `external: true`. Fix at lines 84-86:

```python
    lines.append("networks:")
    lines.append("  raft-network:")
    lines.append("    driver: bridge")
    lines.append("    name: raft-network")
```

- [ ] **Step 2: Regenerate `ops/docker-compose.yml`**

```bash
python3 scripts/gen_docker_compose.py --nodes 15 > ops/docker-compose.yml
```

- [ ] **Step 3: Verify the generated config resolves correctly**

```bash
docker compose -f ops/docker-compose.yml config | grep -A2 "^networks:"
```

Expected output includes `name: raft-network` under the `raft-network:` key. This doesn't require Docker to be running containers — `config` just resolves and prints the merged YAML.

- [ ] **Step 4: Confirm the full test suite is unaffected**

```bash
PYTHONPATH=. pytest tests/ -v
```

Expected: all existing tests still pass (this change touches no Python logic under test, only generated YAML).

- [ ] **Step 5: Commit**

```bash
git add scripts/gen_docker_compose.py ops/docker-compose.yml
git commit -m "Give raft-network an explicit name so it's externally addressable

Without an explicit name:, Compose prefixes the network with the
project name, making it unguessable for a separate compose file to
join via external: true. Needed for the upcoming Prometheus/Grafana
monitoring stack to scrape nodes by service name."
```

---

### Task 2: Add the `raft_node_role` Prometheus gauge

**Files:**
- Modify: `raft/prometheus_metrics.py`
- Test: `tests/test_performance_metrics.py`

**Interfaces:**
- Produces: `PrometheusMetrics.update_node_role(role: str) -> None` (instance method) and module-level `update_node_role(role: str) -> None` (mirrors the existing `update_node_state` pair at lines 279-284 and 408-411). Accepts `"follower"`, `"candidate"`, or `"leader"` (the exact string values of `RaftState.FOLLOWER.value` / `.CANDIDATE.value` / `.LEADER.value` from `raft/types.py`), maps to `0`/`1`/`2` on a `raft_node_role` Gauge labeled by `node_id`.
- Consumes: nothing new — same `CollectorRegistry`/`Gauge` pattern already used for `current_term` etc. at `raft/prometheus_metrics.py:156-161`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_performance_metrics.py`, in `class TestPrometheusMetrics`, right after `test_node_state_update` (after line 152):

```python
    def test_node_role_update(self):
        """Test node role metric update."""
        metrics = PrometheusMetrics("test_node", port=0)

        metrics.update_node_role("follower")
        assert metrics.node_role.labels(node_id="test_node")._value.get() == 0

        metrics.update_node_role("candidate")
        assert metrics.node_role.labels(node_id="test_node")._value.get() == 1

        metrics.update_node_role("leader")
        assert metrics.node_role.labels(node_id="test_node")._value.get() == 2
```

Also add `update_node_role` to the import list and call in `test_global_metrics_functions` (currently lines 216-246): add `update_node_role` to the `from raft.prometheus_metrics import (...)` block, and add `update_node_role("leader")` alongside the other "should not raise exceptions" calls.

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=. pytest tests/test_performance_metrics.py::TestPrometheusMetrics::test_node_role_update -v
```

Expected: FAIL with `AttributeError: 'PrometheusMetrics' object has no attribute 'node_role'`.

- [ ] **Step 3: Add the gauge and methods**

In `raft/prometheus_metrics.py`, add the gauge right after `self.log_length` (after line 182, before the `# Batch metrics` comment at line 184):

```python
        self.node_role = Gauge(
            'raft_node_role',
            'Current node role (0=follower, 1=candidate, 2=leader)',
            ['node_id'],
            registry=self.registry
        )
```

Add the instance method right after `update_node_state` (after line 284, before `update_batch_size`):

```python
    def update_node_role(self, role: str) -> None:
        """Update node role metric (0=follower, 1=candidate, 2=leader)."""
        role_values = {"follower": 0, "candidate": 1, "leader": 2}
        self.node_role.labels(node_id=self.node_id).set(role_values[role])
```

Add the module-level wrapper right after `update_node_state` (after line 411, before `update_batch_size`):

```python
def update_node_role(role: str) -> None:
    """Update node role metric."""
    if _prometheus_metrics:
        _prometheus_metrics.update_node_role(role)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
PYTHONPATH=. pytest tests/test_performance_metrics.py -v
```

Expected: all tests in the file PASS, including the new `test_node_role_update`.

- [ ] **Step 5: Commit**

```bash
git add raft/prometheus_metrics.py tests/test_performance_metrics.py
git commit -m "Add raft_node_role gauge

No metric currently exposes which role (follower/candidate/leader) a
node is in, which the upcoming Grafana dashboard's 'current role per
node' panel and both alert rules need."
```

---

### Task 3: Make every node (not just the leader) report role and state metrics

**Files:**
- Modify: `raft/node.py`
- Test: `tests/test_raft_node.py`

**Interfaces:**
- Consumes: `update_node_state(term, commit_index, last_applied, log_length)` and `update_node_role(role)` from `raft/prometheus_metrics.py` (Task 2).
- Produces: `RaftNode._record_state_metrics() -> None` (synchronous, directly testable without waiting on any loop), `RaftNode._metrics_tick_loop() -> None` (async, runs for the node's whole lifetime independent of role), `RaftNode.metrics_tick_task: Optional[asyncio.Task]` (new instance attribute, mirrors `self.batch_flush_task`).

**Problem this closes:** `update_node_state(...)` is currently only called from `_heartbeat_loop()` (`raft/node.py:925-961`), which only runs `while self.state == RaftState.LEADER` — followers never call it, so a follower's `commit_index`/`current_term`/`last_applied`/`log_length` gauges never update. This would leave the dashboard's "Per-Follower Commit Lag" panel with no data.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_raft_node.py`, after `test_get_state_info` (end of file, after line 188):

```python
@pytest.mark.asyncio
async def test_record_state_metrics_sets_role_and_state_gauges(raft_node):
    """_record_state_metrics should update both the role gauge and the
    term/commit/applied/log-length gauges from the node's current state,
    regardless of whether this node is a leader or follower."""
    raft_node.state = RaftState.LEADER
    raft_node.election_manager.current_term = 3
    raft_node.commit_index = 5
    raft_node.last_applied = 5
    raft_node.storage.get_last_log_index = MagicMock(return_value=5)

    raft_node._record_state_metrics()

    from raft.prometheus_metrics import get_prometheus_metrics
    metrics = get_prometheus_metrics()
    assert metrics.node_role.labels(node_id="node1")._value.get() == 2
    assert metrics.current_term.labels(node_id="node1")._value.get() == 3
    assert metrics.commit_index.labels(node_id="node1")._value.get() == 5
    assert metrics.last_applied.labels(node_id="node1")._value.get() == 5
    assert metrics.log_length.labels(node_id="node1")._value.get() == 5


@pytest.mark.asyncio
async def test_record_state_metrics_reports_follower_role(raft_node):
    """A follower (the default state, never set to LEADER) must still report
    role=0 and its own state gauges - this is the gap being fixed."""
    raft_node.election_manager.current_term = 2
    raft_node.commit_index = 7
    raft_node.last_applied = 6
    raft_node.storage.get_last_log_index = MagicMock(return_value=7)

    raft_node._record_state_metrics()

    from raft.prometheus_metrics import get_prometheus_metrics
    metrics = get_prometheus_metrics()
    assert metrics.node_role.labels(node_id="node1")._value.get() == 0
    assert metrics.commit_index.labels(node_id="node1")._value.get() == 7


@pytest.mark.asyncio
async def test_metrics_tick_task_starts_on_start_and_cancels_on_stop(raft_node):
    """The periodic tick must run independent of role (started in start(),
    not tied to becoming leader) and must be cleanly cancelled on stop()."""
    await raft_node.start()

    assert raft_node.metrics_tick_task is not None
    assert not raft_node.metrics_tick_task.done()

    await raft_node.stop()

    assert raft_node.metrics_tick_task.cancelled()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=. pytest tests/test_raft_node.py -v -k "state_metrics or metrics_tick"
```

Expected: all three FAIL — `_record_state_metrics` and `metrics_tick_task` don't exist yet.

- [ ] **Step 3: Add `metrics_tick_task` attribute in `__init__`**

In `raft/node.py`, in `__init__`, add next to the other task attributes (after line 92, `self.batch_flush_task: Optional[asyncio.Task] = None`):

```python
        self.metrics_tick_task: Optional[asyncio.Task] = None
```

- [ ] **Step 4: Add `_record_state_metrics` and `_metrics_tick_loop`**

Add these two new methods in `raft/node.py` right before `_heartbeat_loop` (before line 925):

```python
    def _record_state_metrics(self) -> None:
        """Refresh this node's role and state gauges from current in-memory state."""
        try:
            from raft.prometheus_metrics import update_node_state, update_node_role
            update_node_state(
                self.election_manager.current_term,
                self.commit_index,
                self.last_applied,
                self.storage.get_last_log_index()
            )
            update_node_role(self.state.value)
        except ImportError:
            pass

    async def _metrics_tick_loop(self) -> None:
        """Periodically refresh state metrics for this node regardless of
        role - unlike the heartbeat loop, this runs on followers too."""
        while True:
            try:
                self._record_state_metrics()
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics tick loop: {e}")
                await asyncio.sleep(2.0)

```

- [ ] **Step 5: Remove the now-redundant metrics update from `_heartbeat_loop`**

The leader-only update in `_heartbeat_loop` (lines 930-942) is superseded by the new role-independent tick loop. Replace:

```python
        while self.state == RaftState.LEADER:
            try:
                # Update node state metrics
                try:
                    from raft.prometheus_metrics import update_node_state
                    log_length = self.storage.get_last_log_index()
                    update_node_state(
                        self.election_manager.current_term,
                        self.commit_index,
                        self.last_applied,
                        log_length
                    )
                except ImportError:
                    pass
                
                # Send heartbeats to all peers
```

with:

```python
        while self.state == RaftState.LEADER:
            try:
                # Send heartbeats to all peers
```

- [ ] **Step 6: Call `_record_state_metrics()` immediately on role transitions**

For instant dashboard feedback on leader change (not waiting up to 2s for the next tick), add a call at the end of `_on_become_leader` (after line 351's `logger.info(...)`, still inside the method) and at the end of `_on_become_follower` (after line 377's `logger.info(...)`, still inside the method):

In `_on_become_leader`, after:
```python
        logger.info(f"Node {self.node_id} became leader for term {self.election_manager.current_term}")
```
add:
```python
        self._record_state_metrics()
```

In `_on_become_follower`, after:
```python
        logger.info(f"Node {self.node_id} became follower")
```
add:
```python
        self._record_state_metrics()
```

- [ ] **Step 7: Start the tick task in `start()` and cancel it in `stop()`**

In `raft/node.py`, in `start()`, add right before the closing `logger.info(f"Raft node {self.node_id} started as {self.state.value} (recovery complete)")` line (currently line 159):

```python
        self.metrics_tick_task = asyncio.create_task(self._metrics_tick_loop())

```

In `stop()`, add right after `self.election_manager.stop_heartbeat()` (currently line 167), before `# Stop KV state machine`:

```python
        if self.metrics_tick_task:
            self.metrics_tick_task.cancel()
            try:
                await self.metrics_tick_task
            except asyncio.CancelledError:
                pass

```

- [ ] **Step 8: Run tests to verify they pass**

```bash
PYTHONPATH=. pytest tests/test_raft_node.py -v
```

Expected: all tests PASS, including the 3 new ones.

- [ ] **Step 9: Run the full suite**

```bash
PYTHONPATH=. pytest tests/ -v
```

Expected: all 160 tests (157 existing + 1 from Task 2 + these 3 minus any renumbering) pass.

- [ ] **Step 10: Commit**

```bash
git add raft/node.py tests/test_raft_node.py
git commit -m "Report role/state metrics from every node, not just the leader

update_node_state() was only ever called from the leader-only
heartbeat loop, so followers never reported commit_index/current_term/
last_applied/log_length - the upcoming 'per-follower commit lag'
dashboard panel would have had no data. Added a role-independent
periodic tick plus immediate updates on role transitions."
```

---

### Task 4: Prometheus service + scrape config

**Files:**
- Create: `ops/monitoring/prometheus/prometheus.yml`
- Create: `ops/docker-compose.monitoring.yml`

**Interfaces:**
- Consumes: `raft-network` (Task 1), each node's `/metrics` endpoint on `nodeN:800N` (already exists, unchanged).
- Produces: a running Prometheus reachable at `localhost:9090`, scraping all 15 nodes under job name `raft-nodes` — this exact job name is required by the Quorum Health panel's `up{job="raft-nodes"}` query in Task 6.

- [ ] **Step 1: Write the Prometheus scrape config**

Create `ops/monitoring/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  - job_name: raft-nodes
    static_configs:
      - targets:
          - node1:8001
          - node2:8002
          - node3:8003
          - node4:8004
          - node5:8005
          - node6:8006
          - node7:8007
          - node8:8008
          - node9:8009
          - node10:8010
          - node11:8011
          - node12:8012
          - node13:8013
          - node14:8014
          - node15:8015
```

- [ ] **Step 2: Write the monitoring compose file**

Create `ops/docker-compose.monitoring.yml`:

```yaml
services:
  prometheus:
    image: prom/prometheus:v2.53.0
    container_name: raft-prometheus
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    networks:
      - raft-network
    restart: unless-stopped

volumes:
  prometheus_data:

networks:
  raft-network:
    external: true
```

- [ ] **Step 3: Bring up the cluster, then the monitoring stack**

```bash
cd ops
docker compose up --build --detach
docker compose -f docker-compose.monitoring.yml up --detach
cd ..
```

- [ ] **Step 4: Verify all 15 targets are being scraped successfully**

```bash
sleep 10
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import json, sys
data = json.load(sys.stdin)
targets = data['data']['activeTargets']
print(f'{len(targets)} targets')
for t in targets:
    print(t['labels']['instance'], t['health'])
"
```

Expected: `15 targets`, all showing `up`.

- [ ] **Step 5: Commit**

```bash
git add ops/monitoring/prometheus/prometheus.yml ops/docker-compose.monitoring.yml
git commit -m "Add Prometheus service scraping all 15 nodes

Separate docker-compose.monitoring.yml joining the cluster's
raft-network via external: true, so the cluster and observability
stack can be started/stopped independently."
```

---

### Task 5: Grafana service + Prometheus datasource

**Files:**
- Create: `ops/monitoring/grafana/provisioning/datasources/prometheus.yml`
- Modify: `ops/docker-compose.monitoring.yml`

**Interfaces:**
- Consumes: the `prometheus` service from Task 4 (referenced by Docker service name `prometheus:9090`).
- Produces: a running Grafana at `localhost:3000` (`admin`/`admin`) with a Prometheus datasource pre-registered under `uid: prometheus` — Task 6's dashboard JSON and Task 7's alert rules both reference this exact `uid`.

- [ ] **Step 1: Write the datasource provisioning file**

Create `ops/monitoring/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [ ] **Step 2: Add the Grafana service to the monitoring compose file**

In `ops/docker-compose.monitoring.yml`, add a `grafana` service after `prometheus` (before the `volumes:` section):

```yaml
  grafana:
    image: grafana/grafana:11.1.0
    container_name: raft-grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - grafana_data:/var/lib/grafana
    ports:
      - "3000:3000"
    networks:
      - raft-network
    depends_on:
      - prometheus
    restart: unless-stopped
```

Add `grafana_data:` alongside `prometheus_data:` in the `volumes:` section, so the full file reads:

```yaml
services:
  prometheus:
    image: prom/prometheus:v2.53.0
    container_name: raft-prometheus
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    networks:
      - raft-network
    restart: unless-stopped

  grafana:
    image: grafana/grafana:11.1.0
    container_name: raft-grafana
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - grafana_data:/var/lib/grafana
    ports:
      - "3000:3000"
    networks:
      - raft-network
    depends_on:
      - prometheus
    restart: unless-stopped

volumes:
  prometheus_data:
  grafana_data:

networks:
  raft-network:
    external: true
```

- [ ] **Step 3: Bring up the updated monitoring stack**

```bash
cd ops
docker compose -f docker-compose.monitoring.yml up --detach
cd ..
```

- [ ] **Step 4: Verify Grafana is up and the datasource auto-registered**

```bash
sleep 5
curl -s -u admin:admin http://localhost:3000/api/datasources | python3 -m json.tool
```

Expected: a JSON array with one entry, `"name": "Prometheus"`, `"uid": "prometheus"`, `"type": "prometheus"`, `"url": "http://prometheus:9090"`.

If this 404s or connection-refuses, check `docker logs raft-grafana` for provisioning errors before treating it as a networking problem — same debugging order this project has used for every prior infra issue (config error first, network second).

- [ ] **Step 5: Commit**

```bash
git add ops/monitoring/grafana/provisioning/datasources/prometheus.yml ops/docker-compose.monitoring.yml
git commit -m "Add Grafana service with provisioned Prometheus datasource

Datasource is config-as-code (uid: prometheus, fixed so later
dashboard/alert provisioning can reference it reliably) rather than
manual UI setup."
```

---

### Task 6: "Cluster Health" dashboard

**Files:**
- Create: `ops/monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Create: `ops/monitoring/grafana/provisioning/dashboards/cluster-health.json`

**Interfaces:**
- Consumes: the `prometheus` datasource `uid: prometheus` (Task 5), the `raft_node_role` metric (Task 2/3), and all pre-existing metrics (`raft_current_term`, `raft_leader_changes_total`, `raft_election_duration_ms`, `raft_commit_index`) from `raft/prometheus_metrics.py`.
- Produces: a dashboard at `localhost:3000/d/raft-cluster-health` with 6 panels, auto-loaded on Grafana startup.

- [ ] **Step 1: Write the dashboard provisioning pointer**

Create `ops/monitoring/grafana/provisioning/dashboards/dashboards.yml`:

```yaml
apiVersion: 1

providers:
  - name: raft-dashboards
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
```

- [ ] **Step 2: Write the dashboard JSON**

Create `ops/monitoring/grafana/provisioning/dashboards/cluster-health.json`:

```json
{
  "title": "Cluster Health",
  "uid": "raft-cluster-health",
  "schemaVersion": 39,
  "version": 1,
  "editable": true,
  "refresh": "5s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "Current Role per Node",
      "type": "stat",
      "gridPos": { "h": 6, "w": 24, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "raft_node_role", "legendFormat": "{{node_id}}", "refId": "A" }
      ],
      "fieldConfig": {
        "defaults": {
          "mappings": [
            {
              "type": "value",
              "options": {
                "0": { "text": "follower", "color": "blue" },
                "1": { "text": "candidate", "color": "yellow" },
                "2": { "text": "leader", "color": "green" }
              }
            }
          ]
        }
      }
    },
    {
      "id": 2,
      "title": "Current Term per Node",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 6 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "raft_current_term", "legendFormat": "{{node_id}}", "refId": "A" }
      ]
    },
    {
      "id": 3,
      "title": "Leader Changes Over Time",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 6 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "increase(raft_leader_changes_total[$__interval])", "legendFormat": "{{node_id}}", "refId": "A" }
      ]
    },
    {
      "id": 4,
      "title": "Election Duration (ms, avg)",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 14 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "rate(raft_election_duration_ms_sum[$__interval]) / rate(raft_election_duration_ms_count[$__interval])",
          "legendFormat": "{{node_id}}",
          "refId": "A"
        }
      ]
    },
    {
      "id": 5,
      "title": "Per-Follower Commit Lag",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 14 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "max(raft_commit_index) - raft_commit_index", "legendFormat": "{{node_id}}", "refId": "A" }
      ]
    },
    {
      "id": 6,
      "title": "Quorum Health (reachable nodes, need 8/15)",
      "type": "stat",
      "gridPos": { "h": 6, "w": 24, "x": 0, "y": 22 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "expr": "count(up{job=\"raft-nodes\"} == 1)", "legendFormat": "reachable nodes", "refId": "A" }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              { "color": "red", "value": null },
              { "color": "green", "value": 8 }
            ]
          }
        }
      }
    }
  ]
}
```

- [ ] **Step 3: Restart Grafana to pick up the new provisioning files**

```bash
cd ops
docker compose -f docker-compose.monitoring.yml restart grafana
cd ..
sleep 5
```

- [ ] **Step 4: Verify the dashboard loaded with all 6 panels**

```bash
curl -s -u admin:admin http://localhost:3000/api/dashboards/uid/raft-cluster-health | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('panel count:', len(d['dashboard']['panels']))
"
```

Expected: `panel count: 6`. If this 404s, check `docker logs raft-grafana` for a dashboard-provisioning parse error (most likely cause: invalid JSON) before assuming the mount path is wrong.

- [ ] **Step 5: Verify the dashboard is showing real, live data end-to-end**

This is the check that actually proves the whole pipeline works (node → metric → scrape → dashboard query), not just that the JSON parsed:

```bash
curl -s 'http://localhost:9090/api/v1/query?query=raft_node_role' | python3 -c "
import json, sys
data = json.load(sys.stdin)['data']['result']
print(f'{len(data)} nodes reporting role')
leaders = [r for r in data if r['value'][1] == '2']
print(f'{len(leaders)} node(s) reporting leader')
"
```

Expected: `15 nodes reporting role` and `1 node(s) reporting leader`. If it shows `0 node(s) reporting leader`, give the cluster a few more seconds to elect (or check that Task 3's changes actually deployed — rebuild the node image if you edited `raft/node.py` after the cluster was last built).

Then open `http://localhost:3000/d/raft-cluster-health` in a browser and visually confirm all 6 panels render non-empty data.

- [ ] **Step 6: Live-verify the demo scenario — kill the leader, watch the dashboard reflect recovery**

This is the spec's "money-shot" check: confirm the role/term data actually changes when a real failover happens, not just that it's non-zero at rest.

```bash
# Find current leader's term and node id
BEFORE=$(curl -s 'http://localhost:9090/api/v1/query?query=raft_node_role==2' | python3 -c "
import json, sys
r = json.load(sys.stdin)['data']['result'][0]
print(r['metric']['node_id'])
")
echo "Current leader: $BEFORE"

# Kill it
cd ops
docker compose stop $BEFORE
cd ..
sleep 5

# Confirm a *different* node is now leader and term advanced
curl -s 'http://localhost:9090/api/v1/query?query=raft_node_role==2' | python3 -c "
import json, sys
r = json.load(sys.stdin)['data']['result']
print('new leader(s):', [x['metric']['node_id'] for x in r])
"
curl -s 'http://localhost:9090/api/v1/query?query=raft_current_term' | python3 -c "
import json, sys
r = json.load(sys.stdin)['data']['result']
print('terms:', {x['metric']['node_id']: x['value'][1] for x in r})
"

# Restart the stopped node so the cluster is back to 15/15 for later tasks
cd ops
docker compose start $BEFORE
cd ..
```

Expected: exactly one new leader, different from `$BEFORE`, and all reporting nodes on a higher term than before the kill. On the dashboard itself, the "Leader Changes Over Time" panel should show a spike at the same moment.

- [ ] **Step 7: Commit**

```bash
git add ops/monitoring/grafana/provisioning/dashboards/dashboards.yml ops/monitoring/grafana/provisioning/dashboards/cluster-health.json
git commit -m "Add Cluster Health dashboard (role, term, leader changes, commit lag, quorum)

Provisioned as code so it auto-loads on Grafana startup rather than
being clicked together manually."
```

---

### Task 7: Alert rules + live verification

**Files:**
- Create: `ops/monitoring/grafana/provisioning/alerting/rules.yml`

**Interfaces:**
- Consumes: `raft_node_role` and `raft_leader_changes_total` metrics via the `prometheus` datasource `uid: prometheus` (Task 5).
- Produces: two firing/resolving alert rules visible in Grafana's Alerting UI and API — no external delivery.

- [ ] **Step 1: Write the alert rules provisioning file**

Create `ops/monitoring/grafana/provisioning/alerting/rules.yml`:

```yaml
apiVersion: 1

groups:
  - orgId: 1
    name: raft-no-leader-alerts
    folder: Raft Cluster
    interval: 5s
    rules:
      - uid: raft-no-leader
        title: No leader elected
        condition: C
        for: 10s
        labels:
          severity: critical
        annotations:
          summary: "No node in the cluster currently reports role=leader"
        data:
          - refId: A
            datasourceUid: prometheus
            relativeTimeRange:
              from: 60
              to: 0
            model:
              expr: count(raft_node_role == 2)
              instant: true
              refId: A
          - refId: B
            datasourceUid: __expr__
            model:
              type: reduce
              expression: A
              reducer: last
              refId: B
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              expression: B
              conditions:
                - evaluator:
                    type: lt
                    params: [1]
              refId: C

  - orgId: 1
    name: raft-leader-flapping-alerts
    folder: Raft Cluster
    interval: 15s
    rules:
      - uid: raft-leader-flapping
        title: Leader changing too frequently
        condition: C
        for: 0s
        labels:
          severity: warning
        annotations:
          summary: "More than 2 leader changes in the last 60s - possible instability"
        data:
          - refId: A
            datasourceUid: prometheus
            relativeTimeRange:
              from: 60
              to: 0
            model:
              expr: sum(increase(raft_leader_changes_total[60s]))
              instant: true
              refId: A
          - refId: B
            datasourceUid: __expr__
            model:
              type: reduce
              expression: A
              reducer: last
              refId: B
          - refId: C
            datasourceUid: __expr__
            model:
              type: threshold
              expression: B
              conditions:
                - evaluator:
                    type: gt
                    params: [2]
              refId: C
```

- [ ] **Step 2: Restart Grafana to pick up the alert rules**

```bash
cd ops
docker compose -f docker-compose.monitoring.yml restart grafana
cd ..
sleep 5
```

- [ ] **Step 3: Verify both rules loaded without provisioning errors**

```bash
docker logs raft-grafana 2>&1 | grep -i "alert" | grep -i "error"
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules | python3 -c "
import json, sys
rules = json.load(sys.stdin)
print([r['title'] for r in rules])
"
```

Expected: no error lines from the first command; `['No leader elected', 'Leader changing too frequently']` from the second. If Grafana rejects the schema, the log line will name the exact field it didn't like — fix that field and restart rather than guessing.

- [ ] **Step 4: Live-verify the no-leader alert actually fires**

Stop enough nodes to break quorum (8+ of 15) and hold it broken for >10s:

```bash
cd ops
for i in 1 2 3 4 5 6 7 8; do docker compose stop node$i; done
cd ..
sleep 20
curl -s -u admin:admin http://localhost:3000/api/v1/provisioning/alert-rules/raft-no-leader | python3 -c "
import json, sys
print(json.load(sys.stdin))
" 2>&1 | head -5
curl -s -u admin:admin 'http://localhost:3000/api/alertmanager/grafana/api/v2/alerts' | python3 -c "
import json, sys
alerts = json.load(sys.stdin)
print([(a['labels'].get('alertname'), a['status']['state']) for a in alerts])
"
```

Expected: the second command shows `('No leader elected', 'active')` (Grafana's alert-state naming may show `alerting` rather than `active` depending on version — check for the rule appearing with a firing-type state, not `normal`).

- [ ] **Step 5: Restore quorum and confirm the alert resolves**

```bash
cd ops
for i in 1 2 3 4 5 6 7 8; do docker compose start node$i; done
cd ..
sleep 15
curl -s -u admin:admin 'http://localhost:3000/api/alertmanager/grafana/api/v2/alerts' | python3 -c "
import json, sys
alerts = json.load(sys.stdin)
print([(a['labels'].get('alertname'), a['status']['state']) for a in alerts])
"
```

Expected: either no entry for "No leader elected", or state `resolved`/absent from the active list.

- [ ] **Step 6: Live-verify the flapping alert fires**

```bash
cd ops
LEADER=$(for p in $(seq 51051 51065); do
  PYTHONPATH=.. python3 ../scripts/kvctl.py cluster-info --host localhost --port $p 2>&1 | grep -q "Role: leader" && echo $p && break
done)
for i in 1 2 3; do
  docker compose stop node1
  sleep 3
  docker compose start node1
  sleep 3
done
cd ..
sleep 20
curl -s -u admin:admin 'http://localhost:3000/api/alertmanager/grafana/api/v2/alerts' | python3 -c "
import json, sys
alerts = json.load(sys.stdin)
print([(a['labels'].get('alertname'), a['status']['state']) for a in alerts])
"
```

Expected: `('Leader changing too frequently', 'alerting')` (or equivalent firing state) appears — note this only fires reliably if `node1` isn't itself always the leader every restart; if it doesn't trigger, repeat against whichever node the cluster-info scan above identified as current leader instead of hardcoding `node1`.

- [ ] **Step 7: Full regression check — bring the cluster back to a clean, healthy state and confirm all tests still pass**

```bash
cd ops
docker compose down -v
docker compose -f docker-compose.monitoring.yml down -v
docker compose up --build --detach
docker compose -f docker-compose.monitoring.yml up --detach
cd ..
PYTHONPATH=. pytest tests/ -v
```

Expected: cluster comes up healthy, full test suite passes.

- [ ] **Step 8: Commit**

```bash
git add ops/monitoring/grafana/provisioning/alerting/rules.yml
git commit -m "Add no-leader and leader-flapping alert rules, live-verified

Visual-only in Grafana's Alerting UI for v1 (no external contact
point). Live-verified both rules actually fire and resolve on the
real 15-node cluster, not just that they parse."
```

---

### Task 8: Document the monitoring stack in README.md

**Files:**
- Modify: `README.md`

**Interfaces:**
- None (documentation only) — this task wraps up the feature by describing what Tasks 1-7 built.

- [ ] **Step 1: Add a "Monitoring & Alerting" section**

Insert a new section in `README.md` right after the "### Manual failover test" section ends and before "## API Reference" (currently the blank line after line 207, before line 209 `## API Reference`):

```markdown
## Monitoring & Alerting

Prometheus + Grafana run as a separate compose stack (`ops/docker-compose.monitoring.yml`) on top of the cluster's existing per-node metrics, joining the cluster's `raft-network` so Prometheus can scrape all 15 nodes by service name. Everything - datasource, dashboard, alert rules - is provisioned as code under `ops/monitoring/`, not clicked together manually.

```bash
# Bring up the cluster first (see "Running the Cluster" above), then:
cd ops
docker compose -f docker-compose.monitoring.yml up --detach
cd ..
```

- **Prometheus**: `http://localhost:9090` — scrapes all 15 nodes' `/metrics` under job `raft-nodes`.
- **Grafana**: `http://localhost:3000` (`admin`/`admin`) — "Cluster Health" dashboard auto-loads with 6 panels: current role per node, current term per node, leader changes over time, election duration, per-follower commit lag, and quorum health.
- **Alerts** (visual-only in Grafana's Alerting UI, no external delivery in v1): no leader for >10s, and leader changing more than twice in 60s (flapping).

The dashboard's 5s refresh is fast enough to watch a live failover: kill the current leader's container and watch the role panel flip, the term increment, and the leader-changes panel spike within a couple of refresh cycles.

Tear down with `docker compose -f docker-compose.monitoring.yml down -v` (before or after the main cluster's own teardown).
```

- [ ] **Step 2: Update "Project Structure" to include `ops/monitoring/`**

In the `## Project Structure` tree (around line 251), change:

```
├── ops/                        # docker-compose.yml (generated, 15 nodes), Dockerfile
```

to:

```
├── ops/                        # docker-compose.yml (generated, 15 nodes), Dockerfile
│   ├── docker-compose.monitoring.yml  # Prometheus + Grafana, joins raft-network
│   └── monitoring/             # Prometheus scrape config, Grafana datasource/dashboard/alert provisioning
```

- [ ] **Step 3: Remove the now-done item from "Future Roadmap"**

Change (around line 286):

```
- **Visualization & Deployment**: FastAPI + Chart.js dashboard showing live price charts and cluster/leader-election status, Grafana integration for metrics, CI/CD
```

to:

```
- **Visualization & Deployment**: FastAPI + Chart.js dashboard showing live price charts and cluster/leader-election status, CI/CD
```

(Grafana integration is done — described in the new "Monitoring & Alerting" section above — so it's removed from the not-started roadmap rather than struck through, matching how this section only ever listed not-yet-started items.)

- [ ] **Step 4: Update the test count if it changed**

Check the actual current count and update the two places it's cited (line 75's "Solid and working" bullet, and line 142's "Running the Test Suite" section) if it no longer says the right number:

```bash
PYTHONPATH=. pytest tests/ --collect-only -q | tail -1
```

Update both occurrences of the old count to the new one, and extend the parenthetical breakdown on line 75 with `+ 1 for the node-role metric + 3 for role/state metrics on followers` (adjust wording to match the actual final count from Task 2/3).

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Document the Grafana/Prometheus monitoring stack"
```

- [ ] **Step 6: Final full verification**

```bash
PYTHONPATH=. pytest tests/ -v
docker compose -f ops/docker-compose.yml ps
docker compose -f ops/docker-compose.monitoring.yml ps
```

Expected: all tests pass, all 15 node containers + prometheus + grafana show `Up`/`running`.
