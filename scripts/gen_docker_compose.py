#!/usr/bin/env python3
"""
Generate ops/docker-compose.yml for an N-node Raft cluster.

Hand-writing one near-identical service block per node doesn't scale past a
handful of nodes (and is exactly how copy-paste port/peer-list mistakes creep
in). Regenerate whenever the node count changes:

    python3 scripts/gen_docker_compose.py --nodes 15 > ops/docker-compose.yml
"""

import argparse

# Widely separated bases so the per-node ranges never collide with each
# other regardless of node count (the original 3-node file used bases only
# 10 apart - 50051 and 50061 - which silently collides once there are more
# than 10 nodes: node11's raft port 50061 == node1's client port).
RAFT_PORT_BASE = 50051
CLIENT_PORT_BASE = 51051
METRICS_PORT_BASE = 8001


def build_peer_list(num_nodes: int) -> str:
    peers = []
    for i in range(1, num_nodes + 1):
        node_id = f"node{i}"
        peers.append(f"{node_id}:{node_id}:{RAFT_PORT_BASE + i - 1}:{CLIENT_PORT_BASE + i - 1}")
    return ",".join(peers)


def render(num_nodes: int) -> str:
    peer_list = build_peer_list(num_nodes)
    lines = ["services:"]

    for i in range(1, num_nodes + 1):
        node_id = f"node{i}"
        raft_port = RAFT_PORT_BASE + i - 1
        client_port = CLIENT_PORT_BASE + i - 1
        metrics_port = METRICS_PORT_BASE + i - 1

        # Only the first service carries a `build:` stanza. All services
        # share the same image tag, so if every one of them also declared
        # its own `build:`, `docker compose up --build` would fan them out
        # as N *concurrent* buildx targets all producing the same final
        # tag - which collides ("image already exists") rather than
        # reusing a cached build. One real build, N services referencing
        # its result, is both correct and far faster to build.
        build_stanza = ""
        if i == 1:
            build_stanza = """    build:
      context: ..
      dockerfile: ops/Dockerfile
"""

        lines.append(f"""  {node_id}:
    image: raft-node:latest
{build_stanza}    container_name: raft-{node_id}
    environment:
      - NODE_ID={node_id}
      - PEER_LIST={peer_list}
      - RAFT_PORT={raft_port}
      - CLIENT_PORT={client_port}
      - DATA_DIR=/app/data
      - LOG_LEVEL=INFO
      - RAFT_BATCH_SIZE=${{RAFT_BATCH_SIZE:-10}}
      - RAFT_FLUSH_INTERVAL_MS=${{RAFT_FLUSH_INTERVAL_MS:-50}}
      - RAFT_SNAPSHOT_THRESHOLD=${{RAFT_SNAPSHOT_THRESHOLD:-50}}
    ports:
      - "{raft_port}:{raft_port}"  # Raft port
      - "{client_port}:{client_port}"  # Client port
      - "{metrics_port}:{metrics_port}"  # Prometheus metrics
    volumes:
      - {node_id}_data:/app/data
    networks:
      - raft-network
    restart: unless-stopped
""")

    lines.append("volumes:")
    for i in range(1, num_nodes + 1):
        lines.append(f"  node{i}_data:")
    lines.append("")

    lines.append("networks:")
    lines.append("  raft-network:")
    lines.append("    driver: bridge")

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=15, help="Number of nodes in the cluster (default: 15)")
    args = parser.parse_args()

    if args.nodes < 1:
        raise SystemExit("--nodes must be >= 1")

    # Guard against the exact class of bug this script used to have: port
    # ranges silently overlapping once node count grows.
    ranges = [
        (RAFT_PORT_BASE, RAFT_PORT_BASE + args.nodes - 1),
        (CLIENT_PORT_BASE, CLIENT_PORT_BASE + args.nodes - 1),
        (METRICS_PORT_BASE, METRICS_PORT_BASE + args.nodes - 1),
    ]
    for (a_start, a_end), (b_start, b_end) in [(ranges[0], ranges[1]), (ranges[0], ranges[2]), (ranges[1], ranges[2])]:
        if a_start <= b_end and b_start <= a_end:
            raise SystemExit(f"Port range overlap for --nodes={args.nodes}: "
                              f"({a_start}-{a_end}) overlaps ({b_start}-{b_end})")

    print(render(args.nodes), end="")


if __name__ == "__main__":
    main()
