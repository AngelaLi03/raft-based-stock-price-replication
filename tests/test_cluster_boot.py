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
