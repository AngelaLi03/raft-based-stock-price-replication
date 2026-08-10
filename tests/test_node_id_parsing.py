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

from raft.types import metrics_port_for_node_id, resolve_metrics_port


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


def test_metrics_port_env_override_wins_regardless_of_node_id(monkeypatch):
    """This is what the Kubernetes manifests rely on: every pod shares one
    pod template and therefore must bind the same metrics port, so an
    explicit METRICS_PORT env var must override the per-node computation
    no matter what node_id says."""
    monkeypatch.setenv("METRICS_PORT", "8001")
    assert resolve_metrics_port("raft-node-0") == 8001
    assert resolve_metrics_port("raft-node-4") == 8001
    assert resolve_metrics_port("node9") == 8001


def test_metrics_port_falls_back_to_per_node_id_when_env_unset(monkeypatch):
    """Docker Compose sets no METRICS_PORT, so this must be a no-op change
    for Compose - the existing per-node computation still applies."""
    monkeypatch.delenv("METRICS_PORT", raising=False)
    assert resolve_metrics_port("node1") == metrics_port_for_node_id("node1")
    assert resolve_metrics_port("raft-node-3") == metrics_port_for_node_id("raft-node-3")


def test_malformed_metrics_port_falls_back_instead_of_raising(monkeypatch):
    """A malformed METRICS_PORT must not raise ValueError out of
    resolve_metrics_port. At the raft/node.py call site this sits inside a
    try/except that only catches ImportError, so an uncaught ValueError here
    would crash RaftNode.__init__ outright - CrashLoopBackOff under
    Kubernetes. metrics_port_for_node_id() is documented as never raising;
    resolve_metrics_port() must keep that guarantee."""
    monkeypatch.setenv("METRICS_PORT", "not-a-port")
    assert resolve_metrics_port("raft-node-2") == metrics_port_for_node_id("raft-node-2")
    assert resolve_metrics_port("node9") == metrics_port_for_node_id("node9")
