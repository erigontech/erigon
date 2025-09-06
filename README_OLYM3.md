# Olym3 Testnet Season 3 - Build Instructions

## 🚨 Build Issues & Solutions

### Problem: Swiss Table Runtime Conflicts
The error you encountered is due to Go runtime conflicts with Swiss table implementation:
```
mapiterinit redeclared in this block
mapiternext redeclared in this block
```

### 🔧 Solution 1: Use Fix Script (Recommended)
```bash
# Run the fix script
./fix_build.sh
```

### 🐳 Solution 2: Docker Build (Alternative)
```bash
# Use Docker to avoid Go runtime conflicts
./docker_build.sh
```

### 🛠️ Solution 3: Manual Fix
```bash
# Clean everything
make clean
go clean -cache

# Set environment variables
export GOFLAGS="-buildvcs=false"
export CGO_ENABLED=1

# Try building with specific tags
cd cmd/erigon
go build -tags "noswiss" -o ../../build/bin/erigon
cd ../..
```

## 🚀 Running Olym3 Testnet Season 3

### Basic Run
```bash
./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data
```

### With RPC API
```bash
./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --http.api=eth,erigon,web3,net,debug,trace,txpool
```

### With Mining
```bash
./build/bin/erigon --chain=olym3-testnet-s3 --datadir=olym3-data --mine --http.api=eth,erigon,web3,net,debug,trace,txpool
```

## 🔍 Verification

### Check Chain ID
```bash
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id":1}' localhost:8545
```

**Expected Result**: `{"jsonrpc":"2.0","id":1,"result":"0x3e803"}` (256003 in hex)

### Check Network Info
```bash
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc": "2.0", "method": "net_version", "params": [], "id":1}' localhost:8545
```

**Expected Result**: `{"jsonrpc":"2.0","id":1,"result":"256003"}`

## 📊 Olym3 Testnet Season 3 Specifications

- **Chain ID**: 256003 (0x3e803)
- **Network Name**: olym3-testnet-s3
- **Consensus**: Ethash → Proof of Stake
- **Genesis Accounts**: 10 test accounts with 1000 ETH each
- **Gas Limit**: 60,000,000
- **Fork Schedule**: All EIPs enabled from block 0

## 🎯 Test Accounts

The following accounts are pre-funded with 1000 ETH each:
- `0x1234567890123456789012345678901234567890`
- `0x2345678901234567890123456789012345678901`
- `0x3456789012345678901234567890123456789012`
- `0x4567890123456789012345678901234567890123`
- `0x5678901234567890123456789012345678901234`
- `0x6789012345678901234567890123456789012345`
- `0x7890123456789012345678901234567890123456`
- `0x8901234567890123456789012345678901234567`
- `0x9012345678901234567890123456789012345678`
- `0x0123456789012345678901234567890123456789`

## 🐛 Troubleshooting

### If build still fails:
1. **Update Go**: Ensure you're using Go 1.21+ or 1.22+
2. **Clean cache**: `go clean -cache -modcache`
3. **Use Docker**: Run `./docker_build.sh`
4. **Check dependencies**: Ensure all build tools are installed

### If runtime fails:
1. **Check ports**: Ensure ports 8545, 30303 are available
2. **Check permissions**: Ensure write access to datadir
3. **Check logs**: Look for specific error messages in console output

## 📝 Notes

- This testnet is designed for development and testing
- All forks are enabled from block 0 for easy testing
- The network uses Proof of Stake consensus
- DNS discovery is configured for `all.olym3-testnet-s3.ethdisco.net`
