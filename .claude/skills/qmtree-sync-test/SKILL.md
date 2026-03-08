---
name: qmtree-sync-test
description: Run an erigon sync with --experimental.qmtree enabled to test proof-of-execution root computation. Supports start, monitor, stop, and clean operations. Uses a temporary datadir with port offsets to avoid conflicts.
allowed-tools: Bash, Read, Write, Edit, Glob
allowed-prompts:
  - tool: Bash
    prompt: create, write, and delete files in the system default temp directory and manage erigon processes
---

# qmtree Sync Test

Run erigon with `--experimental.qmtree` to verify that proof-of-execution (ExecHash + TransitionHash) and qmtree root computation work correctly during live block execution.

## Overview

This skill manages the full lifecycle:
1. **Build** erigon + integration binaries (if needed)
2. **Download** hoodi block snapshots (fast, ~20 min via torrent, skips state)
3. **Execute** blocks with `--experimental.qmtree` computing qmtree roots
4. **Monitor** for qmtree root log lines
5. **Stop** and optionally **clean** the datadir

## Two Modes

### Mode A: Full erigon (recommended for first run)

Downloads block snapshots via torrent, then executes from genesis with qmtree enabled. First run takes ~20 min for download, then execution proceeds at ~45 blocks/sec. Subsequent runs reuse cached snapshots.

### Mode B: Integration tool (for re-execution testing)

Uses `integration stage_exec --experimental.qmtree` against an existing datadir with block snapshots. Useful for re-running execution after code changes without re-downloading. Requires `--reset` to clear execution state, then `--sync.mode.chaintip` to execute blocks.

## Configuration

| Parameter | Default | Notes |
|-----------|---------|-------|
| Chain | `hoodi` | Small testnet, fast initial sync |
| Datadir | `/tmp/qmtree-sync-test` | Fixed path for easy reuse |
| Port offset | +200 | Avoids default and +100 (ephemeral) ports |
| Log verbosity | 3 (info) | Shows qmtree root lines |
| Block snapshots | ~21.7 GB | Downloaded once, reused across runs |

### Port Table (offset +200)

| CLI Flag | Port |
|----------|------|
| `--private.api.addr` | `127.0.0.1:9290` |
| `--http.port` | `8745` |
| `--authrpc.port` | `8751` |
| `--ws.port` | `8746` |
| `--torrent.port` | `42269` |
| `--port` | `30503` |
| `--p2p.allowed-ports` | `30503,30504,30505,30506,30507` |
| `--caplin.discovery.port` | `4200` |
| `--caplin.discovery.tcpport` | `4201` |
| `--sentinel.port` | `7977` |
| `--beacon.api.port` | `5755` |
| `--mcp.port` | `8753` |

## Workflow (Mode A — Full erigon)

### Step 1: Build (if needed)

Check if `./build/bin/erigon` exists. If not, run `make erigon integration`.

### Step 2: Check for Existing Instance

```bash
pgrep -f "datadir.*/tmp/qmtree-sync-test" 2>/dev/null
```

If a process is found, report it and ask the user whether to stop it first or just monitor.

### Step 3: Prepare Datadir

If `/tmp/qmtree-sync-test` does not exist, create it:
```bash
mkdir -p /tmp/qmtree-sync-test
```

### Step 4: Start Erigon

**Key flags:**
- `--snap.skip-state-snapshot-download` — downloads block snapshots only (headers, bodies, transactions), skips state. Reduces download from ~78 GB to ~21.7 GB.
- `--experimental.qmtree` — enables proof-of-execution hashing and qmtree root computation.
- **Do NOT use `--no-downloader`** — the torrent downloader is needed to fetch block snapshots.

Print the full command to the user, then run it in the background:

```bash
./build/bin/erigon \
  --datadir=/tmp/qmtree-sync-test \
  --chain=hoodi \
  --snap.skip-state-snapshot-download \
  --experimental.qmtree \
  --private.api.addr=127.0.0.1:9290 \
  --http.port=8745 \
  --authrpc.port=8751 \
  --ws.port=8746 \
  --torrent.port=42269 \
  --port=30503 \
  --p2p.allowed-ports=30503,30504,30505,30506,30507 \
  --caplin.discovery.port=4200 \
  --caplin.discovery.tcpport=4201 \
  --sentinel.port=7977 \
  --beacon.api.port=5755 \
  --mcp.port=8753 \
  --log.console.verbosity=3 \
  > /tmp/qmtree-sync-test/console.log 2>&1
```

Run with `run_in_background: true`.

### Step 5: Monitor Download Phase

On first run, the downloader fetches block snapshots in two phases:

1. **Header-chain** (~1.4 GB, ~90 seconds):
   ```
   [Downloader] Syncing header-chain  time-left=1m30s ...
   [OtterSync] Downloader completed header-chain
   ```

2. **Remaining snapshots** (~20 GB, ~20 minutes):
   ```
   [Downloader] Syncing remaining snapshots  time-left=18m ...
   [OtterSync] Downloader completed remaining snapshots
   ```

Monitor with:
```bash
grep -E "Syncing|completed" /tmp/qmtree-sync-test/console.log | tail -5
```

On subsequent runs with the same datadir, snapshots are cached and this phase completes instantly.

### Step 6: Monitor Execution + qmtree Roots

After snapshots are ready, execution starts automatically. Look for:

```
[Execution] serial starting  from=0 to=...
[Execution] qmtree root  block=0 root=0xf120748b... leaves=2
[Execution] qmtree root  block=1 root=0x446c6098... leaves=4
...
[Execution] qmtree root  block=1000 root=0xfd0215c2... leaves=11851
```

qmtree root lines appear for blocks 0-10 and every 1000th block.

```bash
grep "qmtree" /tmp/qmtree-sync-test/console.log
```

Or follow live:
```bash
tail -f /tmp/qmtree-sync-test/console.log | grep -E "qmtree|serial"
```

### Success Criteria

The test is successful when:
1. Erigon starts without errors
2. Block snapshots download completes (or are cached from prior run)
3. `serial starting` and `serial executed` lines appear (blocks executing)
4. `qmtree root` log lines appear with non-zero roots
5. Roots change between blocks (proving the tree is accumulating)
6. Leaf count grows (e.g., block 1000 has ~11,851 leaves)

### Expected Performance

| Metric | Value |
|--------|-------|
| Blocks/sec | ~45 |
| Txs/sec | ~250-300 |
| Block 1000 leaves | ~11,851 |
| Block 2000 leaves | ~19,090 |

### Step 7: Stop

```bash
pkill -f "datadir.*/tmp/qmtree-sync-test"
```

Verify the process is gone:
```bash
sleep 2; pgrep -f "datadir.*/tmp/qmtree-sync-test" || echo "Process stopped"
```

If processes remain, force kill:
```bash
kill -9 $(pgrep -f "datadir.*/tmp/qmtree-sync-test") 2>/dev/null
```

### Step 8: Clean

After stopping:
```bash
rm -rf /tmp/qmtree-sync-test
```

Report cleanup complete.

**Note:** To preserve downloaded snapshots for re-testing, keep the datadir and just reset execution state:
```bash
./build/bin/integration stage_exec --datadir=/tmp/qmtree-sync-test --chain=hoodi --reset
```

## Workflow (Mode B — Re-execution via integration)

Use this after code changes to re-execute blocks against already-downloaded snapshots.

### Step 1: Build

```bash
make erigon integration
```

### Step 2: Reset Execution State

```bash
./build/bin/integration stage_exec --datadir=/tmp/qmtree-sync-test --chain=hoodi --reset
```

### Step 3: Run Erigon with qmtree (same as Mode A Step 4)

After reset, start erigon with `--experimental.qmtree` and `--snap.skip-state-snapshot-download`. It will skip the download phase (snapshots already cached) and go straight to execution.

## Known Roots (hoodi, deterministic)

These roots are deterministic — same block data always produces the same root:

| Block | Root | Leaves |
|-------|------|--------|
| 0 | `0xf120748bf9db1bc21d88ad4ba7d734cbca47a850e6f0f22b02e50ab4382c1984` | 2 |
| 1 | `0x446c6098ea47c9e9f1f37b2794ae7bf63f2fd82ef21e125a94dc58b9842e17cb` | 4 |
| 10 | `0x32eaf9570c6ce7c06b6375f5a4271d278fc88d56b84a2f0769432fefaa76548a` | 25 |
| 1000 | `0xfd0215c2ab4d1281e5046a865b7621c6a4acfee063db65eed09be3bf54bf2d0d` | 11,851 |
| 2000 | `0x2b571f452bd28ba3e7ab168a7951ecebb30cb5fb6cbf9cdf18a038b15fd3e968` | 19,090 |

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No qmtree root lines | Check `--experimental.qmtree` is in the startup command log. Verify blocks are executing (look for `serial executed`). |
| Port conflict on start | Check `ss -tlnp \| grep <port>`. Another erigon instance may be running. Stop it or use different offset. |
| Crash on startup | Check `tail -50 /tmp/qmtree-sync-test/console.log`. Likely a build issue — rebuild with `make erigon`. |
| Download very slow | Torrent peers may be scarce. The webseed (`erigon34-v1-snapshots-hoodi.erigon.network`) provides baseline bandwidth. |
| Zero roots only | ExecHasher/TransitionHasher may not be attached. Verify `--experimental.qmtree` in startup banner. |
| Integration stage_exec hangs | Use Mode A (full erigon) instead. The integration tool can get stuck in pruning loops on fresh datadirs. |
