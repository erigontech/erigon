#!/bin/bash

# Quick fix script for GCP environment
# This script handles common build issues on GCP

echo "🚀 Quick fix for Olym3 Testnet Season 3 on GCP..."

# Update system packages
echo "📦 Updating system packages..."
apt update -y
apt upgrade -y

# Install/update Go if needed
echo "🔧 Checking Go installation..."
if ! command -v go &> /dev/null; then
    echo "Installing Go..."
    apt install -y golang-go
fi

# Install build essentials
echo "🛠️ Installing build essentials..."
apt install -y build-essential git make gcc

# Check Go version
echo "📋 Current Go version:"
go version

# Set Go environment
export GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

# Navigate to project directory
cd /home/erigon/erigon

# Pull latest changes
echo "📥 Pulling latest changes..."
git pull origin main

# Clean everything
echo "🧹 Cleaning build environment..."
make clean 2>/dev/null || true
go clean -cache 2>/dev/null || true
rm -rf build/bin/erigon 2>/dev/null || true

# Create build directory
mkdir -p build/bin

# Try the simple build
echo "🔨 Attempting simple build..."
./simple_build.sh

# If that fails, try Docker
if [ ! -f "build/bin/erigon" ]; then
    echo "🐳 Trying Docker build..."
    ./docker_build.sh
fi

# Final check
if [ -f "build/bin/erigon" ]; then
    echo "✅ Build successful!"
    echo "📊 Binary info:"
    ls -la build/bin/erigon
    echo ""
    echo "🎯 To run Olym3 Testnet Season 3:"
    echo "   ./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool"
    echo ""
    echo "🔍 To test chain ID:"
    echo "   curl -X POST -H \"Content-Type: application/json\" --data '{\"jsonrpc\": \"2.0\", \"method\": \"eth_chainId\", \"params\": [], \"id\":1}' localhost:8545"
else
    echo "❌ Build failed. Please check the error messages above."
    echo ""
    echo "🔍 Debug information:"
    echo "Go version: $(go version)"
    echo "Disk space: $(df -h /)"
    echo "Memory: $(free -h)"
    echo "Go env: $(go env)"
fi
