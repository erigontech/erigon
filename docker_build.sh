#!/bin/bash

# Alternative Docker-based build for Olym3 Testnet Season 3
# This avoids Go runtime conflicts by using a clean environment

echo "🐳 Building Erigon with Olym3 Testnet Season 3 using Docker..."

# Create a simple Dockerfile for building
cat > Dockerfile.build << 'EOF'
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev linux-headers

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build Erigon with Olym3 Testnet Season 3 support
RUN make erigon

# Final stage
FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/build/bin/erigon .

# Expose ports
EXPOSE 8545 8546 30303 30304 42069

# Default command
CMD ["./erigon", "--chain=olym3-testnet-s3", "--datadir=olym3-data", "--http.api=eth,erigon,web3,net,debug,trace,txpool"]
EOF

echo "🔨 Building Docker image..."
docker build -f Dockerfile.build -t erigon-olym3:latest .

if [ $? -eq 0 ]; then
    echo "✅ Docker build successful!"
    echo ""
    echo "🚀 To run Olym3 Testnet Season 3:"
    echo "   docker run -p 8545:8545 -p 30303:30303 -v olym3-data:/root/olym3-data erigon-olym3:latest"
    echo ""
    echo "🔍 To check chain ID:"
    echo "   curl -X POST -H \"Content-Type: application/json\" --data '{\"jsonrpc\": \"2.0\", \"method\": \"eth_chainId\", \"params\": [], \"id\":1}' localhost:8545"
    echo ""
    echo "📊 Expected result: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0x3e803\"} (256003 in hex)"
else
    echo "❌ Docker build failed."
    echo "💡 Make sure Docker is installed and running."
fi

# Cleanup
rm -f Dockerfile.build
