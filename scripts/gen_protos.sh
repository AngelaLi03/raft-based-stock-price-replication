#!/bin/bash

# Generate Python gRPC stubs from protobuf definitions
# This script should be run from the project root

set -e

echo "Generating Python gRPC stubs..."

# Create output directories
mkdir -p raft/proto
mkdir -p client/proto

# Generate Python files from protobuf definitions
python3 -m grpc_tools.protoc \
    --proto_path=proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/raft.proto

python3 -m grpc_tools.protoc \
    --proto_path=proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/client.proto

# Move generated files to appropriate locations
mv raft_pb2.py raft/proto/
mv raft_pb2_grpc.py raft/proto/
mv client_pb2.py client/proto/
mv client_pb2_grpc.py client/proto/

# Fix import statements in generated files
sed -i '' 's/import raft_pb2 as raft__pb2/from . import raft_pb2 as raft__pb2/g' raft/proto/raft_pb2_grpc.py
sed -i '' 's/import client_pb2 as client__pb2/from . import client_pb2 as client__pb2/g' client/proto/client_pb2_grpc.py

# Create __init__.py files
touch raft/proto/__init__.py
touch client/proto/__init__.py

echo "Generated files:"
echo "  - raft/proto/raft_pb2.py"
echo "  - raft/proto/raft_pb2_grpc.py"
echo "  - client/proto/client_pb2.py"
echo "  - client/proto/client_pb2_grpc.py"
echo ""
echo "Protobuf generation complete!"
