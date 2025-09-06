#!/bin/bash

# Install Go 1.21 to fix Swiss table conflicts
# This script installs a compatible Go version

echo "🔧 Installing Go 1.21 to fix Swiss table conflicts..."

# Check current Go version
echo "📋 Current Go version:"
go version 2>/dev/null || echo "Go not installed"

# Download Go 1.21.5
echo "📥 Downloading Go 1.21.5..."
cd /tmp
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz

if [ ! -f "go1.21.5.linux-amd64.tar.gz" ]; then
    echo "❌ Failed to download Go 1.21.5"
    exit 1
fi

# Remove old Go installation
echo "🗑️ Removing old Go installation..."
rm -rf /usr/local/go

# Install new Go
echo "🔨 Installing Go 1.21.5..."
tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz

# Set environment variables
echo "🔧 Setting up Go environment..."
export PATH=/usr/local/go/bin:$PATH
export GOPATH=/root/go
export PATH=$PATH:$GOPATH/bin

# Add to bashrc for persistence
echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
echo 'export GOPATH=/root/go' >> ~/.bashrc
echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.bashrc

# Verify installation
echo "✅ Go 1.21.5 installed successfully!"
go version

# Clean up
rm -f go1.21.5.linux-amd64.tar.gz

echo ""
echo "🎯 Now try building Erigon again:"
echo "   cd /home/erigon/erigon"
echo "   ./fix_swiss_table.sh"
echo ""
echo "Or try the simple build:"
echo "   ./simple_build.sh"
