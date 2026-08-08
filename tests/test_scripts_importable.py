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
