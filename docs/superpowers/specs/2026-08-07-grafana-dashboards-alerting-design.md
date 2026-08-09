# Grafana Dashboards + Alerting — Design

**Status:** Approved
**Date:** 2026-08-07
**Scope:** First of five sub-projects under the "AI-engineer + production/infra layer" initiative (on-call diagnostic agent, chaos-driven eval harness, Kubernetes migration, CI/CD pipeline, Grafana dashboards + alerting). This spec covers Grafana dashboards + alerting only.

## Problem

The cluster already exposes rich Prometheus metrics per node (elections, replication, commits, snapshots, storage, batching — see `raft/prometheus_metrics.py`) on ports 8001-8015, but nothing scrapes or visualizes them. There's no way to *see* cluster health, and no way to *demonstrate* it (e.g. "kill the leader, watch the cluster recover") without reading raw `/metrics` text or grepping logs.

## Goals

- Live dashboard showing cluster health: current leader/role per node, leader changes, per-follower commit lag, quorum health, election duration.
- Alerting on the two conditions that actually indicate trouble: no leader, and leader flapping.
- Everything provisioned as code and checked into git — no manual UI clicking to reproduce the setup.

## Non-goals (cut for this spec, can be added later without redesign)

- A second "Storage/Replication detail" dashboard (replication latency histograms, snapshot activity, batch flush sizes).
- Alerting on follower commit lag or node-scrape-failure (visible on the dashboard, but not wired to fire alerts in v1).
- Alert delivery to an external channel (Slack, webhook, email) — v1 is visual-only, within Grafana's own Alerting UI.
- Kubernetes deployment — this is docker-compose only, matching how the rest of the cluster runs today.

## Architecture

### Networking

`ops/docker-compose.yml` currently relies on Compose's implicit default network, which isn't addressable from outside that file. It gains an explicit named network (`raft-net`) that all 15 node services join.

`ops/docker-compose.monitoring.yml` is a **separate** compose file (not merged into the main one) so the cluster and the observability stack can be started/stopped independently. It declares `raft-net` as `external: true` and joins it, so Prometheus can scrape nodes by service name (`node1:8001`, …, `node15:8015`) rather than depending on host-mapped ports.

Startup order: cluster first, then monitoring —
```
docker compose -f ops/docker-compose.yml up -d
docker compose -f ops/docker-compose.monitoring.yml up -d
```

### Services

- **Prometheus** — scrapes all 15 nodes' `/metrics` endpoints on a single job.
- **Grafana** — one datasource (Prometheus), one dashboard, alert rules — all provisioned on container startup via mounted config, not manual setup.

### Config-as-code layout

```
ops/monitoring/
  prometheus/
    prometheus.yml                                 # scrape job, 15 static targets
  grafana/
    provisioning/
      datasources/prometheus.yml                    # auto-registers Prometheus datasource
      dashboards/dashboards.yml                      # tells Grafana where to load dashboard JSON from
      dashboards/cluster-health.json                 # the actual dashboard definition
      alerting/rules.yml                             # the two alert rules
```

## Required application code changes

Two gaps in the existing metrics code block this design and must be fixed first — not just Grafana/Prometheus config:

### 1. No node-role metric exists

There is currently no way to query "who is the leader right now" from Prometheus. Add a `raft_node_role` gauge (`0=follower, 1=candidate, 2=leader`), labeled by `node_id`, to `raft/prometheus_metrics.py`. Set it in `raft/node.py` at every state transition:
- `become_leader` ([node.py:318](../../../raft/node.py#L318))
- step-down-to-follower ([node.py:355](../../../raft/node.py#L355))
- the candidate transition

This feeds both the "current role per node" dashboard panel and the no-leader / flapping alert rules.

### 2. Followers never report state metrics

`update_node_state()` (sets `current_term`, `commit_index`, `last_applied`, `log_length` gauges) is only called from `_heartbeat_loop()` ([node.py:933](../../../raft/node.py#L933)), which only runs `while self.state == RaftState.LEADER`. Followers currently emit none of these metrics, which would leave the "per-follower commit lag" panel with no data.

Fix: call `update_node_state(...)` regardless of role — extend `_apply_committed_entries()` ([node.py:520](../../../raft/node.py#L520)) to call it after applying entries, plus a lightweight periodic call so metrics stay fresh even when idle (no new entries to apply). Follow the existing best-effort `try/except ImportError` pattern already used at every other metrics call site in this file — no new abstraction.

### Tests

Add regression tests matching the existing pattern (`tests/test_grpc_server.py`, `tests/test_metrics_server.py` test metrics recording the same way):
- Role gauge is set correctly on each of the three state transitions.
- Follower state gauges (`commit_index`, `current_term`, `last_applied`, `log_length`) update after applying committed entries, independent of role.

## Dashboard: "Cluster Health"

Single page, 5s refresh (fast enough to watch a live failover demo).

| Panel | Query | Purpose |
|---|---|---|
| Current role per node | `raft_node_role` (stat/table, colored by value) | See leader/followers at a glance |
| Current term per node | `raft_current_term` (time series) | Term should jump together across nodes on election |
| Leader changes over time | `increase(raft_leader_changes_total[$__interval])` | The demo money-shot signal |
| Election duration | `raft_election_duration_ms` (histogram) | How fast the cluster recovers |
| Per-follower commit lag | `max(raft_commit_index) - raft_commit_index` by `node_id` | Is a given follower falling behind |
| Quorum health | `count(up{job="raft-nodes"} == 1)` vs. majority threshold (8/15), stat panel red/green | Can the cluster still make progress |

## Alerting

Grafana unified alerting, provisioned as code (`ops/monitoring/grafana/provisioning/alerting/rules.yml`). Visual only — fires and shows in Grafana's Alerting page and turns the relevant panel red; no external contact point/notification channel in v1.

- **No leader**: `count(raft_node_role == 2) == 0`, evaluated every 5s, `for: 10s` before firing (avoids flapping "firing" state during a normal sub-second election).
- **Leader flapping**: `increase(raft_leader_changes_total[60s]) > 2`, evaluated every 15s.

## Verification plan

Live verification, matching how every other feature in this project has been verified — no substitute for actually watching it work:

1. Bring up the 15-node cluster, then the monitoring stack. Confirm Prometheus shows 15/15 targets `up`.
2. Confirm the Cluster Health dashboard renders live, non-zero data across all 6 panels.
3. Kill the current leader's container. Watch: role panel flips to the new leader, term increments across all nodes, "Leader changes" panel spikes, election-duration panel gets a new data point. This is the demo clip.
4. Force a longer outage (leader + enough followers to break quorum) to confirm the "No leader" alert transitions to `firing` after 10s, then resolves once quorum is restored.
5. Rapidly restart a node a few times to confirm the flapping alert fires.
6. `pytest` full suite still green after the `raft/node.py` / `raft/prometheus_metrics.py` changes.

## Rollout / git hygiene

Follows this repo's standing convention: implementation code (`ops/monitoring/**`, `raft/node.py`, `raft/prometheus_metrics.py`, new tests, README updates) gets committed and pushed; `learn.md` updates documenting this work stay local-only, added in a separate unpushed commit. Nothing is committed without explicit go-ahead.
