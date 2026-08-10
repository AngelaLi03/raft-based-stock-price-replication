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
  # Without this, only endpoints backed by READY pods get DNS records.
  # Readiness here depends on Raft election state (see readinessProbe
  # below), and node.py opens a fresh gRPC channel per RPC rather than
  # holding long-lived connections - so if this were unset, a node stuck
  # CANDIDATE past the readiness probe's failure window would drop out of
  # its own peers' DNS, and could never receive the votes needed to become
  # FOLLOWER/LEADER and regain readiness. Peer-to-peer discovery must be
  # unconditional; only client traffic (raft-client Service, below) should
  # stay readiness-gated.
  publishNotReadyAddresses: true
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
# deployment-level guarantee for EVICTIONS - node drain, cluster-autoscaler,
# descheduler, manual `kubectl evict`. minAvailable is the strict majority
# ({min_available} of {num_nodes}), so Kubernetes will refuse a voluntary
# eviction that would drop the cluster below the number of nodes it needs to
# elect a leader and commit entries. Note this does NOT cover rolling
# updates: the StatefulSet deletes pods directly via its own RollingUpdate
# strategy, which never goes through the Eviction API the PDB guards. What
# keeps a routine `kubectl rollout restart` safe is the StatefulSet's
# default one-ordinal-at-a-time update behavior below, not this PDB - the
# PDB is the backstop for the abnormal case (drain, bad rollout config
# forcing extra parallelism), not the mechanism behind routine rollouts.
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
            - name: METRICS_PORT
              value: "{METRICS_PORT}"
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
