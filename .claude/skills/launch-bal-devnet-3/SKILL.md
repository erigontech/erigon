---
name: launch-bal-devnet-3
description: Launch erigon + Lighthouse on the bal-devnet-3 ethpandaops devnet (EIP-7928 Block Access Lists). Manages start/stop of both EL and CL clients with proper port offsets and JWT auth.
allowed-tools: Bash, Read, Write, Edit, Glob
allowed-prompts:
  - tool: Bash
    prompt: start, stop, and manage erigon and lighthouse processes for bal-devnet-3
---

# Launch bal-devnet-3 (EIP-7928 BAL Devnet)

Run erigon (EL) + Lighthouse (CL) on the bal-devnet-3 ethpandaops devnet for testing EIP-7928 Block Access Lists.

## Network Details

| Parameter | Value |
|-----------|-------|
| Chain ID | 7098917910 |
| Genesis timestamp | 1775658600 |
| Amsterdam timestamp | 1775662086 (epoch 9) |
| Lighthouse image | `ethpandaops/lighthouse:bal-devnet-3-65bb283` |
| Lighthouse version | v8.0.1 (commit 65bb283, branch bal-devnet-3) |
| Explorer (Dora) | https://dora.bal-devnet-3.ethpandaops.io |
| Faucet | https://faucet.bal-devnet-3.ethpandaops.io |
| RPC | https://rpc.bal-devnet-3.ethpandaops.io |
| Checkpoint sync | https://checkpoint-sync.bal-devnet-3.ethpandaops.io |

## Working Directory

Ask the user where they want the working directory. Default suggestion: `~/bal-devnet-3/`.
Use `$WORKDIR` throughout to refer to the chosen path.

```
$WORKDIR/
├── genesis.json              # EL genesis
├── config.yaml               # CL beacon config
├── genesis.ssz               # CL genesis state
├── testnet-config/           # Lighthouse testnet dir (config.yaml + genesis.ssz + deploy files)
├── start-erigon.sh           # Erigon start script (run FIRST)
├── start-lighthouse.sh       # Lighthouse start script (run SECOND)
├── stop.sh                   # Stop both erigon + Lighthouse
├── clean.sh                  # Stop, wipe data, re-init genesis
├── erigon-data/              # Erigon datadir (contains jwt.hex)
├── lighthouse-data/          # Lighthouse datadir
├── erigon-console.log        # Erigon stdout/stderr
└── lighthouse-console.log    # Lighthouse stdout/stderr
```

## Port Assignments (offset +100)

| Service | Port | Protocol |
|---------|------|----------|
| Erigon HTTP RPC | 8645 | TCP |
| Erigon Engine API (authrpc) | 8651 | TCP |
| Erigon WebSocket | 8646 | TCP |
| Erigon P2P | 30403 | TCP+UDP |
| Erigon gRPC | 9190 | TCP |
| Erigon Torrent | 42169 | TCP+UDP |
| Erigon pprof | 6160 | TCP |
| Erigon metrics | 6161 | TCP |
| Lighthouse P2P | 9100 | TCP+UDP |
| Lighthouse QUIC | 9101 | UDP |
| Lighthouse HTTP API | 5152 | TCP |
| Lighthouse metrics | 5264 | TCP |

## Workflow

### Step 1: Check Prerequisites

1. Verify erigon binary exists at `./build/bin/erigon`. If not, invoke `/erigon-build`.
2. Verify the Lighthouse Docker image is available:
   ```bash
   docker image inspect ethpandaops/lighthouse:bal-devnet-3-65bb283 > /dev/null 2>&1
   ```
   If not, pull it:
   ```bash
   docker pull ethpandaops/lighthouse:bal-devnet-3-65bb283
   ```
3. Verify config files exist in `$WORKDIR` (genesis.json, testnet-config/).
   If not, download them:
   ```bash
   mkdir -p $WORKDIR/testnet-config
   curl -sL -o $WORKDIR/genesis.json https://config.bal-devnet-3.ethpandaops.io/el/genesis.json
   curl -sL -o $WORKDIR/testnet-config/config.yaml https://config.bal-devnet-3.ethpandaops.io/cl/config.yaml
   curl -sL -o $WORKDIR/testnet-config/genesis.ssz https://config.bal-devnet-3.ethpandaops.io/cl/genesis.ssz
   echo "0" > $WORKDIR/testnet-config/deposit_contract_block.txt
   echo "0" > $WORKDIR/testnet-config/deploy_block.txt
   ```

### Step 2: Initialize Datadir (first run only)

If `$WORKDIR/erigon-data/chaindata` does not exist:
```bash
./build/bin/erigon init --datadir $WORKDIR/erigon-data $WORKDIR/genesis.json
```

### Step 3: Create Scripts (first run only)

If the start/stop/clean scripts don't exist yet, generate them. The scripts must use absolute paths based on `$WORKDIR`. Key details:

**start-erigon.sh** — Runs erigon with `--externalcl`. Must start FIRST (creates JWT secret).
- Env vars: `ERIGON_EXEC3_PARALLEL=true`, `ERIGON_ASSERT=true`, `ERIGON_EXEC3_WORKERS=12`, `LOG_HASH_MISMATCH_REASON=true`
- Flags: `--datadir=$WORKDIR/erigon-data`, `--externalcl`, `--networkid=7098917910`, all 15 EL bootnodes, erigon static peers, `--prune.mode=minimal`, all offset ports (see port table), `--http.api=eth,erigon,engine,debug`, `--pprof`, `--metrics`
- EL bootnodes: fetch from `https://config.bal-devnet-3.ethpandaops.io/api/v1/nodes/inventory` (extract enode URLs from `execution.enode` fields)

**start-lighthouse.sh** — Runs Lighthouse via Docker with `--network=host`. Must start SECOND.
- Checks JWT exists at `$WORKDIR/erigon-data/jwt.hex`
- Docker container name: `bal-devnet-3-lighthouse`
- Mounts: `$WORKDIR/testnet-config:/config:ro`, `$WORKDIR/lighthouse-data:/data`, JWT as `/jwt.hex:ro`
- Flags: `--testnet-dir=/config`, `--execution-endpoint=http://127.0.0.1:8651`, `--execution-jwt=/jwt.hex`, all 15 CL ENR bootnodes, offset ports, `--checkpoint-sync-url=https://checkpoint-sync.bal-devnet-3.ethpandaops.io`
- CL bootnodes: fetch from same inventory URL (extract ENR entries from `consensus.enr` fields)

**stop.sh** — Stops Lighthouse (`docker stop bal-devnet-3-lighthouse`) then erigon (`pkill -f "datadir.*bal-devnet-3/erigon-data"`).

**clean.sh** — Runs `stop.sh`, removes erigon chain data (chaindata, snapshots, txpool, nodes, temp) and lighthouse data, re-initializes genesis.

### Step 4: Start Erigon (FIRST)

Erigon must start first because it creates the JWT secret that Lighthouse needs.

```bash
cd $WORKDIR && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
```

Verify it started:
- Check `tail $WORKDIR/erigon-console.log` for startup messages
- Check JWT exists: `ls $WORKDIR/erigon-data/jwt.hex`
- Check port binding: `ss -tlnp | grep 8651`

### Step 5: Start Lighthouse (SECOND)

After erigon is running and JWT exists:

```bash
cd $WORKDIR && nohup bash start-lighthouse.sh > lighthouse-console.log 2>&1 &
```

Verify it started:
- Check `tail $WORKDIR/lighthouse-console.log` for "Lighthouse started"
- Look for "Loaded checkpoint block and state" (checkpoint sync)
- Look for `peers: "N"` showing peer connections

### Step 6: Monitor

```bash
# Erigon sync progress
tail -f $WORKDIR/erigon-console.log

# Lighthouse sync progress
tail -f $WORKDIR/lighthouse-console.log

# Check erigon block height via RPC
curl -s http://localhost:8645 -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | python3 -m json.tool

# Check lighthouse sync status
curl -s http://localhost:5152/eth/v1/node/syncing | python3 -m json.tool
```

### Step 7: Stop

```bash
bash $WORKDIR/stop.sh
```

This stops Lighthouse (via `docker stop`) then erigon (via `pkill`).

### Step 8: Clean (wipe data and re-init)

```bash
bash $WORKDIR/clean.sh
```

This runs `stop.sh`, removes erigon chain data (chaindata, snapshots, txpool, nodes, temp) and lighthouse data, then re-initializes genesis. After clean, start again with Steps 4-5.

## A/B Testing: BAL vs Non-BAL Parallel Execution

Compare parallel execution throughput with and without BAL scheduling optimization.

### Overview

- **Instance A (BAL)**: Default `bal-devnet-3` instance at `$WORKDIR` — uses BAL to pre-populate version maps and schedule transactions optimistically
- **Instance B (No-BAL)**: Second instance at `$WORKDIR-nobal` — sets `IGNORE_BAL=true` to force dependency-tracking scheduling path

Both instances sync the same chain, enabling direct throughput comparison.

### Instance B Port Assignments (offset +400)

| Service | Port | Protocol |
|---------|------|----------|
| Erigon HTTP RPC | 8945 | TCP |
| Erigon Engine API (authrpc) | 8951 | TCP |
| Erigon WebSocket | 8946 | TCP |
| Erigon P2P | 30703 | TCP+UDP |
| Erigon gRPC | 9490 | TCP |
| Erigon Torrent | 42469 | TCP+UDP |
| Erigon pprof | 6460 | TCP |
| Erigon metrics | 6461 | TCP |
| Lighthouse P2P | 9100 | TCP+UDP |
| Lighthouse QUIC | 9101 | UDP |
| Lighthouse HTTP API | 5352 | TCP |
| Lighthouse metrics | 5464 | TCP |

### Setup Instance B

1. Create working directory and copy config:
   ```bash
   NOBAL_DIR=${WORKDIR}-nobal
   mkdir -p $NOBAL_DIR/erigon-data $NOBAL_DIR/lighthouse-data
   cp -r $WORKDIR/testnet-config $NOBAL_DIR/
   cp $WORKDIR/genesis.json $NOBAL_DIR/
   ```

2. Build a binary with `IGNORE_BAL` support (requires the bal-devnet-3 branch that includes the `IGNORE_BAL` flag):
   ```bash
   # Build from bal-devnet-3 branch (or use existing binary if already up to date)
   make erigon
   ```

3. Initialize genesis:
   ```bash
   ./build/bin/erigon init --datadir=$NOBAL_DIR/erigon-data $NOBAL_DIR/genesis.json
   ```

4. Create start scripts — same as Instance A but with:
   - `export IGNORE_BAL=true` in `start-erigon.sh`
   - Port offsets from the table above
   - Docker container name: `bal-devnet-3-nobal-lighthouse`
   - `--execution-endpoint=http://127.0.0.1:8951` in Lighthouse
   - `--disable-enr-auto-update` in Lighthouse (second instance on same host)

5. Start Instance B (erigon first, then Lighthouse):
   ```bash
   cd $NOBAL_DIR && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
   # Wait for jwt.hex
   cd $NOBAL_DIR && nohup bash start-lighthouse.sh > lighthouse-console.log 2>&1 &
   ```

### Metrics to Compare

Once both instances reach chain tip, compare at-head execution metrics:

| Metric | Source | What it measures |
|--------|--------|------------------|
| `gas/s` | Execution log lines | Raw execution throughput |
| `repeat%` | Execution log lines | Speculative re-execution rate (lower = better dependency prediction) |
| `abort` | Execution log lines | Number of aborted transactions per batch |
| `invalid` | Execution log lines | Transactions invalidated by conflict detection |
| `blk/s` | Execution log lines | Block processing rate |

**Expected**: BAL instance should have lower `repeat%` and `abort` counts because BAL pre-populates the version map, reducing false conflicts. The `gas/s` difference shows the net throughput impact.

### Monitoring Script

```bash
# Side-by-side log comparison
echo "=== BAL (Instance A) ===" && \
grep -E "parallel (executed|done)" $WORKDIR/erigon-data/logs/erigon.log | tail -3 && \
echo "" && \
echo "=== No-BAL (Instance B) ===" && \
grep -E "parallel (executed|done)" ${WORKDIR}-nobal/erigon-data/logs/erigon.log | tail -3
```

### Cleanup

```bash
# Stop Instance B
docker stop bal-devnet-3-nobal-lighthouse 2>/dev/null
pkill -f "datadir.*bal-devnet-3-nobal/erigon-data"
# Optionally remove data
rm -rf ${WORKDIR}-nobal
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| JWT auth fails | Ensure erigon started first and `jwt.hex` exists. Lighthouse must mount the same file. |
| No EL peers | Check firewall allows port 30403. Try adding `--nat=extip:<your-ip>`. |
| No CL peers | Check firewall allows port 9100/9101. ENR bootnodes may have changed — re-fetch from inventory. |
| "Head is optimistic" | Normal during initial sync. Erigon is behind Lighthouse. Will resolve as erigon catches up. |
| Engine API timeout | Check erigon is running and authrpc port 8651 is accessible. |
| Port conflict | Check `ss -tlnp | grep <port>`. Kill conflicting process or use higher offset. |
