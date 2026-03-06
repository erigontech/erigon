---
name: chaintoml-3node-test
description: "Run a 3-node chain.toml P2P discovery test. Two seeders publish chain.toml via ENR, one leecher discovers it via --snap.p2p-manifest. Tests decentralized snapshot distribution (issue #19657)."
allowed-tools: Bash, Read, Write
allowed-prompts:
  - tool: Bash
    prompt: create, write, and delete files in the system default temp directory
---

# 3-Node chain.toml P2P Discovery Test

Tests the decentralized snapshot distribution flow where:
- **Node A & B (seeders)**: Publish chain.toml via ENR (local .torrent files + preverified registry)
- **Node C (leecher)**: Runs with `--snap.p2p-manifest`, discovers chain.toml from peers, downloads snapshots from public network

## Design Principles

### chain.toml Content
Seeders publish a chain.toml that merges two sources:
- **Local .torrent files**: files the seeder has on disk (available)
- **Preverified registry (snapcfg)**: files known to be authoritative from preverified.toml

This means seeders don't need to have all snapshot files downloaded — they just need to know about them. The leecher discovers the manifest via P2P, then downloads actual snapshot files from the public torrent/webseed network.

### OtterSync Wait Mechanism
When `--snap.p2p-manifest` is enabled, OtterSync blocks until the first chain.toml discovery completes. This prevents it from running with an empty preverified registry. The signal flows through:
1. `Downloader.EnableP2PManifest()` creates a `manifestReady` channel
2. `acquireChainToml()` closes the channel after first successful apply
3. `DownloadAndIndexSnapshotsIfNeed()` waits on `cfg.manifestReady` before proceeding

### Start Order Independence
Nodes can start in any order. The discovery loop runs on a 30s ticker and retries until peers with chain-toml ENR entries are found.

## Prerequisites

- Erigon binary built from `feat/decentralized-snapshots` branch
- `--http.api=admin,eth,erigon,engine` on all nodes (for admin_addPeer)
- Seeders need existing datadirs with snapshots (or will download from public network on first run)
- Leecher starts with empty datadir

## Port Assignments

Three instances use port offsets +100, +200, +300:

| Port | Node A (+100) | Node B (+200) | Node C (+300) |
|------|---------------|---------------|---------------|
| `--private.api.addr` | 127.0.0.1:9190 | 127.0.0.1:9290 | 127.0.0.1:9390 |
| `--http.port` | 8645 | 8745 | 8845 |
| `--authrpc.port` | 8651 | 8751 | 8851 |
| `--ws.port` | 8646 | 8746 | 8846 |
| `--torrent.port` | 42169 | 42269 | 42369 |
| `--port` (devp2p) | 30403 | 30503 | 30603 |
| `--p2p.allowed-ports` | 30403-30407 | 30503-30507 | 30603-30607 |
| `--caplin.discovery.port` | 4100 | 4200 | 4300 |
| `--caplin.discovery.tcpport` | 4101 | 4201 | 4301 |
| `--sentinel.port` | 7877 | 7977 | 8077 |
| `--beacon.api.port` | 5655 | 5755 | 5855 |
| `--mcp.port` | 8653 | 8753 | 8853 |

## Workflow

### Step 1: Build

```bash
make erigon
```

### Step 2: Create datadirs

For Hoodi (recommended for testing — small chain, ~80GB snapshots):
```bash
mkdir -p /erigon/erigon-hoodi-nodeA /erigon/erigon-hoodi-nodeB /erigon/erigon-hoodi-nodeC
```

### Step 3: Start all 3 nodes

Nodes can start in any order. Use `--chain=hoodi` for all.

**Node A (seeder, +100 offset):**
```bash
nohup ./build/bin/erigon \
  --datadir=/erigon/erigon-hoodi-nodeA \
  --chain=hoodi \
  --http.api=admin,eth,erigon,engine \
  --private.api.addr=127.0.0.1:9190 \
  --http.port=8645 --authrpc.port=8651 --ws.port=8646 \
  --torrent.port=42169 --port=30403 \
  --p2p.allowed-ports=30403,30404,30405,30406,30407 \
  --caplin.discovery.port=4100 --caplin.discovery.tcpport=4101 \
  --sentinel.port=7877 --beacon.api.port=5655 --mcp.port=8653 \
  --log.console.verbosity=3 \
  > /tmp/hoodi-nodeA.log 2>&1 &
```

**Node B (seeder, +200 offset):**
```bash
nohup ./build/bin/erigon \
  --datadir=/erigon/erigon-hoodi-nodeB \
  --chain=hoodi \
  --http.api=admin,eth,erigon,engine \
  --private.api.addr=127.0.0.1:9290 \
  --http.port=8745 --authrpc.port=8751 --ws.port=8746 \
  --torrent.port=42269 --port=30503 \
  --p2p.allowed-ports=30503,30504,30505,30506,30507 \
  --caplin.discovery.port=4200 --caplin.discovery.tcpport=4201 \
  --sentinel.port=7977 --beacon.api.port=5755 --mcp.port=8753 \
  --log.console.verbosity=3 \
  > /tmp/hoodi-nodeB.log 2>&1 &
```

**Node C (leecher, +300 offset, --snap.p2p-manifest):**
```bash
nohup ./build/bin/erigon \
  --datadir=/erigon/erigon-hoodi-nodeC \
  --chain=hoodi \
  --snap.p2p-manifest \
  --http.api=admin,eth,erigon,engine \
  --private.api.addr=127.0.0.1:9390 \
  --http.port=8845 --authrpc.port=8851 --ws.port=8846 \
  --torrent.port=42369 --port=30603 \
  --p2p.allowed-ports=30603,30604,30605,30606,30607 \
  --caplin.discovery.port=4300 --caplin.discovery.tcpport=4301 \
  --sentinel.port=8077 --beacon.api.port=5855 --mcp.port=8853 \
  --log.console.verbosity=3 \
  > /tmp/hoodi-nodeC.log 2>&1 &
```

### Step 4: Connect peers

Wait ~30s for nodes to initialize, then:
```bash
ENODE_A=$(curl -s -X POST http://127.0.0.1:8645 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' \
  | python3 -c "import sys,json; e=json.load(sys.stdin)['result']['enode']; print(e[:e.rfind(':')+1].replace(e.split('@')[1].split(':')[0],'127.0.0.1') + e.split(':')[-1])")

ENODE_B=$(curl -s -X POST http://127.0.0.1:8745 \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' \
  | python3 -c "import sys,json; e=json.load(sys.stdin)['result']['enode']; print(e[:e.rfind(':')+1].replace(e.split('@')[1].split(':')[0],'127.0.0.1') + e.split(':')[-1])")

curl -s -X POST http://127.0.0.1:8845 \
  -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"admin_addPeer\",\"params\":[\"$ENODE_A\"],\"id\":1}"

curl -s -X POST http://127.0.0.1:8845 \
  -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"method\":\"admin_addPeer\",\"params\":[\"$ENODE_B\"],\"id\":1}"
```

### Step 5: Verify

Monitor Node C's logs:
```bash
grep -E 'chaintoml|bt-peers|Waiting|manifest ready|OtterSync|Syncing' \
  /erigon/erigon-hoodi-nodeC/logs/erigon.log | tail -20
```

### Step 6: Cleanup

```bash
pgrep -f 'erigon-hoodi-node' | xargs kill 2>/dev/null
# Optionally remove datadirs:
# rm -rf /erigon/erigon-hoodi-node{A,B,C}
```

## Success Criteria

All of these must be observed in order:

| # | Log pattern (Node C) | Meaning |
|---|---------------------|---------|
| 1 | `[1/6 OtterSync] Waiting for P2P manifest discovery...` | OtterSync correctly blocks |
| 2 | `[chaintoml] applied discovered entries new=NNNN` | chain.toml received and applied (NNNN should match seeder's preverified count) |
| 3 | `[1/6 OtterSync] P2P manifest ready, proceeding with download` | OtterSync unblocks |
| 4 | `[Downloader] Syncing header-chain files=N/138` | Headers downloading from public network |
| 5 | `[Downloader] Syncing remaining snapshots files=N/441` | State downloading (441 = full preverified set for Hoodi) |
| 6 | `[1/6 OtterSync] Downloader completed remaining snapshots` | All snapshots downloaded |
| 7 | `[4/6 Execution] executed block NNNN` | Executing blocks from snapshot frontier |
| 8 | `statusV2Handler headSlot=NNNN` | Node reaches chain tip |

**Final check**: All 3 nodes report the same `finalizedRoot` and `headSlot` in their `statusV2Handler` logs.

## Known Issues and Fixes

### Permission denied on chain.toml save
The torrent client creates downloaded files as read-only (0o444). `SaveChainToml` now `chmod`s the file before writing.

### chain.toml only contains .torrent files
`GenerateChainToml` reads `.torrent` files on disk, which doesn't include state files (downloaded by hash without .torrent). Fixed by merging the preverified registry into chain.toml during `PublishChainToml`.

### OtterSync races with discovery
If OtterSync runs before chain.toml is discovered, it completes with an empty file list. Fixed by adding a `manifestReady` channel that blocks OtterSync until discovery succeeds.

### frozenTx=0 in ENR
Currently `PublishLocalChainToml` is called with `frozenTx=0`. This is cosmetic — the discovery still works since it picks any peer with a chain-toml ENR entry. Will be fixed when frozenTx is properly tracked.

## Key Flags

| Flag | Purpose |
|------|---------|
| `--snap.p2p-manifest` | Enable P2P manifest discovery (leecher mode) |
| `--http.api=admin,...` | Enable admin RPC namespace for peer management |
| `--chain=hoodi` | Use Hoodi testnet (recommended: small chain, fast sync) |
