# Launch bal-devnet-2 (Erigon + Lighthouse)

Run Erigon and Lighthouse for the bal-devnet-2 ethpandaops devnet (EIP-7928 Block Access Lists).

## Quick Start

```bash
# 1. Build erigon from bal-devnet-2 branch
cd ~/mark/hive/clients/erigon/erigon && make erigon

# 2. Initialize (IMPORTANT: --datadir must come BEFORE genesis file)
./build/bin/erigon init --datadir ~/mark/bal-devnet-2/erigon-data ~/mark/bal-devnet-2/genesis.json

# 3. Start erigon first (creates JWT secret)
bash ~/mark/bal-devnet-2/start-erigon.sh

# 4. Start lighthouse second (reads JWT from erigon)
bash ~/mark/bal-devnet-2/start-lighthouse.sh
```

## Reinitializing (Clean Restart)

When you need to wipe the datadir and start fresh:

```bash
# Stop both
pkill -f "erigon.*bal-devnet-2"; docker stop bal-devnet-2-lighthouse

# Clean EVERYTHING in the datadir except jwt.hex and nodekey
rm -rf ~/mark/bal-devnet-2/erigon-data/{chaindata,snapshots,txpool,nodes,temp,bal,caplin,migrations,downloader,LOCK,logs}

# CRITICAL: --datadir flag MUST come BEFORE the genesis file path!
# Wrong: erigon init genesis.json --datadir=path  (silently uses default path!)
# Right: erigon init --datadir path genesis.json
./build/bin/erigon init --datadir ~/mark/bal-devnet-2/erigon-data ~/mark/bal-devnet-2/genesis.json

# Verify init output says "Writing custom genesis block" (NOT "Writing main-net genesis block")
# Verify chain config shows ChainID: 7033429093 and Glamsterdam on startup
```

### Salt files

After a clean wipe, the `snapshots/salt-state.txt` and `snapshots/salt-blocks.txt` files are
recreated automatically by `erigon init`. If erigon errors with "salt not found on ReloadSalt",
create them manually:

```bash
python3 -c "import os; open('$HOME/mark/bal-devnet-2/erigon-data/snapshots/salt-state.txt', 'wb').write(os.urandom(4))"
python3 -c "import os; open('$HOME/mark/bal-devnet-2/erigon-data/snapshots/salt-blocks.txt', 'wb').write(os.urandom(4))"
```

## Stopping

```bash
pkill -f "erigon.*bal-devnet-2"
docker stop bal-devnet-2-lighthouse
```

## Checking Status

```bash
# Erigon block number
curl -s http://127.0.0.1:8645 -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# Lighthouse head slot
curl -s http://127.0.0.1:5152/eth/v1/beacon/headers/head | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print('Head slot:', d['data']['header']['message']['slot'])"

# Check erigon logs for errors
grep -E "WARN|ERR" ~/mark/bal-devnet-2/erigon-data/logs/erigon.log | tail -20

# Check for gas or BAL mismatches
grep -E "gas mismatch|BAL mismatch" ~/mark/bal-devnet-2/erigon-data/logs/erigon.log | tail -10
```

## Port Assignments (offset to avoid conflicts)

| Service | Port | Flag |
|---------|------|------|
| Erigon HTTP RPC | 8645 | `--http.port` |
| Erigon Auth RPC | 8651 | `--authrpc.port` |
| Erigon WebSocket | 8646 | `--ws.port` |
| Erigon P2P | 30403 | `--port` |
| Erigon gRPC | 9190 | `--private.api.addr` |
| Erigon Torrent | 42169 | `--torrent.port` |
| Erigon pprof | 6160 | `--pprof.port` |
| Erigon metrics | 6161 | `--metrics.port` |
| Erigon MCP | 8653 | `--mcp.port` |
| Lighthouse HTTP | 5152 | `--http-port` |
| Lighthouse P2P | 9100 | `--port` |
| Lighthouse metrics | 5264 | `--metrics-port` |

## Network Details

| Parameter | Value |
|-----------|-------|
| Chain ID | 7033429093 |
| Genesis timestamp | 1770388190 |
| Amsterdam timestamp | 1770400508 (epoch 32) |
| Seconds per slot | 12 |
| Gas limit | 60,000,000 |
| Lighthouse image | `ethpandaops/lighthouse:bal-devnet-2-65bb283` |
| RPC endpoint | https://rpc.bal-devnet-2.ethpandaops.io |
| Checkpoint sync | https://checkpoint-sync.bal-devnet-2.ethpandaops.io |
| Explorer | https://explorer.bal-devnet-2.ethpandaops.io |
| Faucet | https://faucet.bal-devnet-2.ethpandaops.io |

## Environment Variables

```bash
ERIGON_EXEC3_PARALLEL=true   # Enable parallel execution
ERIGON_ASSERT=true           # Enable assertions
ERIGON_EXEC3_WORKERS=12      # Number of parallel workers
LOG_HASH_MISMATCH_REASON=true # Detailed hash mismatch logging
```

## Troubleshooting

### "Unsupported fork" errors from engine API
This means the chain config is wrong. Verify:
1. The erigon init wrote to the CORRECT datadir (check `Opening Database label=chaindata path=...` in init output)
2. The chain config shows `Glamsterdam: 2026-02-06` (not mainnet dates)
3. If wrong, re-init with `--datadir` flag BEFORE the genesis file path

### No P2P peers
The devnet nodes may be at max peer capacity. Wait or restart; peer slots open periodically. Check with:
```bash
grep "GoodPeers\|peers=" ~/mark/bal-devnet-2/erigon-data/logs/erigon.log | tail -5
```

### Gas mismatch at block N (parallel execution)
Past bugs fixed:
- **Block 214**: Fixed in versionedio.go (caching all paths except CodePath when readStorage=nil)
- **Block 8177**: Fixed in versionedio.go (StoragePath MVReadResultNone checks IncarnationPath)
- **Block 10113**: Fixed in intra_block_state.go (Empty() was calling accountRead(&emptyAccount) for non-existent accounts, overwriting the nil recorded by versionedRead, causing createObject to be skipped for SELFDESTRUCT beneficiaries)

If a new gas mismatch appears, check the erigon logs for the specific block/tx causing it. The root cause is typically a missing version map write that causes stale reads during parallel validation.

## Config Files

All in `~/mark/bal-devnet-2/`:
- `genesis.json` - EL genesis (from ethpandaops)
- `testnet-config/config.yaml` - CL beacon config
- `testnet-config/genesis.ssz` - CL genesis state
- `start-erigon.sh` - Erigon start script
- `start-lighthouse.sh` - Lighthouse start script (Docker)
- `erigon-data/` - Erigon datadir
- `lighthouse-data/` - Lighthouse datadir (Docker volume)

## Geth Reference Implementation

The geth bal-devnet-2 source is at: https://github.com/ethereum/go-ethereum/tree/bal-devnet-2

Key EIP-7928 implementation files in geth:
- `core/block_access_list_tracer.go` - BAL tracer hooks
- `core/types/bal/bal.go` - BAL builder and types
- `core/state_processor.go` - BAL integration in block processing
- `core/state/bal_reader.go` - BAL reader for parallel execution

Key differences from erigon:
- Geth uses `receipt.GasUsed = MaxUsedGas` (pre-refund) for Amsterdam blocks
- Erigon uses `receipt.GasUsed = ReceiptGasUsed` (post-refund) - potential mismatch
- Both use pre-refund gas for block header GasUsed (EIP-7778)
