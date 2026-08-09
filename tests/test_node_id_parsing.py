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
