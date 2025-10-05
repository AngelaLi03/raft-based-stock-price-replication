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
RUN bash scripts/gen_protos.sh

# Create data directory
RUN mkdir -p /app/data

# Expose ports
EXPOSE 50051 50061

# Set default environment variables
ENV LOG_LEVEL=INFO
ENV DATA_DIR=/app/data

# Run the server
CMD ["python", "server/main.py"]
