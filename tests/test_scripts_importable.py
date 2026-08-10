"""
Smoke tests for scripts/benchmark.py and scripts/chaos_test.py.

Neither is exercised by the rest of the test suite (they're docker/subprocess-
driven ops tooling, not pure logic), so nothing caught `benchmark.py` importing
a `client.client.RaftClient` that didn't exist - it just silently couldn't run,
for the entire life of the project, until someone tried it live. These tests
don't replace live verification (see README's chaos-test/benchmark sections),
but they do guarantee the module actually imports and its key classes/functions
exist with the shape callers expect - cheap, fast, and exactly what would have
caught the original bug in CI without needing Docker at all.
"""

import inspect


def test_benchmark_module_imports():
    import scripts.benchmark as benchmark
    assert hasattr(benchmark, "RaftClient")
    assert hasattr(benchmark, "BenchmarkRunner")


def test_benchmark_runner_uses_real_client():
    """Regression test for the client.client import bug: RaftClient must be
    the real implementation (scripts.kvctl.RaftClient), not a module that
    doesn't exist."""
    from scripts.benchmark import RaftClient
    from scripts.kvctl import RaftClient as KvctlRaftClient
    assert RaftClient is KvctlRaftClient


def test_benchmark_client_has_close_method():
    """BenchmarkRunner.cleanup() awaits client.close() for every client."""
    from scripts.benchmark import RaftClient
    assert hasattr(RaftClient, "close")
    assert inspect.iscoroutinefunction(RaftClient.close)


def test_chaos_test_module_imports():
    import scripts.chaos_test as chaos_test
    assert hasattr(chaos_test, "RaftClient")
    assert hasattr(chaos_test, "ChaosTester")


def test_chaos_tester_keys_nodes_by_id_not_address():
    """Regression test for the node-targeting bug: cluster_nodes must map
    node_id -> address (so _stop_node/_start_node can use node_id directly
    as the docker-compose service name), not be a bare list of addresses
    that then has to be parsed back into a node identity."""
    from scripts.chaos_test import ChaosTester, DEFAULT_CLUSTER_NODES

    tester = ChaosTester({"node1": "localhost:51051", "node2": "localhost:51052"})
    assert set(tester.cluster_nodes.keys()) == {"node1", "node2"}
    assert set(tester.clients.keys()) == {"node1", "node2"}

    # Defaults should cover the standard 15-node cluster, keyed the same way.
    assert DEFAULT_CLUSTER_NODES["node1"] == "localhost:51051"
    assert DEFAULT_CLUSTER_NODES["node15"] == "localhost:51065"
    assert len(DEFAULT_CLUSTER_NODES) == 15


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
