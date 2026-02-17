#!/bin/bash
# Setup script for stalled stream integration tests
# This starts MinIO and Toxiproxy in Docker

set -e

echo "🚀 Setting up test environment for stalled stream tests..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Start MinIO
echo "📦 Starting MinIO..."
docker rm -f test-minio 2>/dev/null || true
docker run -d \
    --name test-minio \
    -p 9000:9000 \
    -p 9001:9001 \
    -e "MINIO_ROOT_USER=minio123" \
    -e "MINIO_ROOT_PASSWORD=minio123" \
    minio/minio server /data --console-address ":9001"

# Wait for MinIO to be ready
echo "⏳ Waiting for MinIO to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "✅ MinIO is ready!"
        break
    fi
    echo "   Attempt $i/30..."
    sleep 1
done

# Start Toxiproxy
echo "🧪 Starting Toxiproxy..."
docker rm -f test-toxiproxy 2>/dev/null || true
docker run -d \
    --name test-toxiproxy \
    -p 8474:8474 \
    -p 9002:9002 \
    -p 9003:9003 \
    shopify/toxiproxy

# Wait for Toxiproxy to be ready
echo "⏳ Waiting for Toxiproxy to be ready..."
for i in {1..10}; do
    if curl -s http://localhost:8474/version > /dev/null 2>&1; then
        echo "✅ Toxiproxy is ready!"
        break
    fi
    echo "   Attempt $i/10..."
    sleep 1
done

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Services running:"
echo "  • MinIO:       http://localhost:9000 (console: http://localhost:9001)"
echo "  • Toxiproxy:   http://localhost:8474"
echo ""
echo "Now run:"
echo "  cargo test test_stalled_stream --test test_stalled_stream -- --ignored --nocapture"
echo ""
echo "To stop services:"
echo "  docker stop test-minio test-toxiproxy"
echo "  docker rm test-minio test-toxiproxy"
