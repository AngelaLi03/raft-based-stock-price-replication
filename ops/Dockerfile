FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Generate protobuf files
RUN python -m grpc_tools.protoc --proto_path=proto --python_out=. --grpc_python_out=. proto/raft.proto && \
    python -m grpc_tools.protoc --proto_path=proto --python_out=. --grpc_python_out=. proto/client.proto && \
    mkdir -p raft/proto client/proto && \
    mv raft_pb2.py raft/proto/ && \
    mv raft_pb2_grpc.py raft/proto/ && \
    mv client_pb2.py client/proto/ && \
    mv client_pb2_grpc.py client/proto/ && \
    touch raft/proto/__init__.py client/proto/__init__.py && \
    sed -i 's/import raft_pb2 as raft__pb2/from . import raft_pb2 as raft__pb2/' raft/proto/raft_pb2_grpc.py && \
    sed -i 's/import client_pb2 as client__pb2/from . import client_pb2 as client__pb2/' client/proto/client_pb2_grpc.py

# Create data directory
RUN mkdir -p /app/data

# Expose ports
EXPOSE 50051 50061

# Set default environment variables
ENV LOG_LEVEL=INFO
ENV DATA_DIR=/app/data
ENV PYTHONPATH=/app

# Run the server
CMD ["python", "server/main.py"]
