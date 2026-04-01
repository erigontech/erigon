---
name: launch-epbs-devnet
description: "Launch erigon with Caplin on an epbs devnet. Usage: /launch-epbs-devnet <N> (e.g. /launch-epbs-devnet 0 for epbs-devnet-0). Reads config from .claude/skills/devnet/configs/epbs-devnet-<N>.yaml."
allowed-tools: Bash, Read, Write, Edit, Glob
allowed-prompts:
  - tool: Bash
    prompt: start, stop, and manage erigon with caplin for epbs devnets
---

# Launch ePBS Devnet

Launch erigon with Caplin (embedded CL) on an ePBS ethpandaops devnet.

## Usage

```
/launch-epbs-devnet 0     # epbs-devnet-0
/launch-epbs-devnet 1     # epbs-devnet-1 (when available)
```

The argument is the devnet number N. If omitted, default to 0.

## Step 0: Load Config

Read the config file at:
```
/root/work/erigon/.claude/skills/devnet/configs/epbs-devnet-<N>.yaml
```

If the file doesn't exist, stop and tell the user to create a config first.

Extract all parameters from the YAML. Key fields:
- `name` — devnet name (e.g. `epbs-devnet-0`)
- `workdir` — working directory (e.g. `/root/epbs-devnet-0`)
- `chain_id` — network ID
- `branch` — expected git branch
- `checkpoint_sync` — checkpoint sync URL
- `config_base` — base URL for downloading config files
- All port fields (`el_http`, `el_authrpc`, `cl_beacon_api`, etc.)

Use these values throughout. Refer to them as `$NAME`, `$WORKDIR`, etc.

## Step 1: Check Prerequisites

1. Verify branch:
   ```bash
   git branch --show-current
   ```
   Warn if not on the expected `branch` from config.

2. Verify erigon binary exists at `./build/bin/erigon`. If not, invoke `/erigon-build`.

3. Verify config files exist in `$WORKDIR` (genesis.json, config.yaml, genesis.ssz).
   If not, download them:
   ```bash
   mkdir -p $WORKDIR
   curl -sL -o $WORKDIR/genesis.json ${config_base}/genesis.json
   curl -sL -o $WORKDIR/config.yaml ${config_base}/config.yaml
   curl -sL -o $WORKDIR/genesis.ssz ${config_base}/genesis.ssz
   ```

## Step 2: Initialize Datadir (first run only)

If `$WORKDIR/erigon-data/chaindata` does not exist:
```bash
./build/bin/erigon init --datadir $WORKDIR/erigon-data $WORKDIR/genesis.json
```

## Step 3: Create Scripts (first run only)

If `$WORKDIR/start-erigon.sh` doesn't exist, generate it using the config values:

**start-erigon.sh** — Single process (erigon + Caplin).
- Env vars from config `env` section
- EL flags: `--datadir`, `--networkid`, `--bootnodes`, `--staticpeers`, `--prune.mode=minimal`, HTTP/WS/auth ports from config, `--http.api=eth,erigon,engine,debug,net,web3`, pprof, metrics
- Caplin flags: `--caplin.custom-config`, `--caplin.custom-genesis`, `--caplin.checkpoint-sync-url`, `--sentinel.bootnodes`, `--sentinel.staticpeers` (same ENRs as bootnodes, for persistent CL connections), discovery ports, beacon API ports
- Do NOT use `--externalcl`
- Bootnodes: download from `${config_base}/enodes.txt` and `${config_base}/bootstrap_nodes.txt`

**stop.sh** — `pkill -f "datadir.*${NAME}/erigon-data"`

**clean.sh** — Runs stop.sh, removes chaindata/snapshots/txpool/nodes/temp/caplin, re-inits genesis.

## Step 4: Start

```bash
cd $WORKDIR && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
```

Verify:
- `tail $WORKDIR/erigon-console.log` for startup messages
- `ss -tlnp | grep ${el_http}` (EL RPC)
- `ss -tlnp | grep ${cl_beacon_api}` (Beacon API)

## Step 5: Monitor

```bash
tail -f $WORKDIR/erigon-console.log

curl -s http://localhost:${el_http} -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

curl -s http://localhost:${cl_beacon_api}/eth/v1/node/syncing
curl -s http://localhost:${cl_beacon_api}/eth/v1/node/peer_count
curl -s http://localhost:${cl_beacon_api}/eth/v1/beacon/headers/head
```

## Step 6: Stop

```bash
bash $WORKDIR/stop.sh
```

## Step 7: Clean (wipe data and re-init)

```bash
bash $WORKDIR/clean.sh
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No EL peers | Check firewall for EL P2P port. Re-fetch enodes. |
| No CL peers | Check firewall for CL discovery ports. Ensure `--sentinel.staticpeers` is set (same as bootnodes). Re-fetch ENRs. |
| Checkpoint sync fails | Check checkpoint sync URL reachability. |
| GLOAS fork issues | Ensure correct branch and freshly built binary. |
| Port conflict | `ss -tlnp \| grep <port>`. Kill conflicting process. |

## Refreshing Bootnodes

```bash
curl -sL ${config_base}/enodes.txt
curl -sL ${config_base}/bootstrap_nodes.txt
```

Or from live inventory:
```bash
curl -s ${inventory_api}
```
