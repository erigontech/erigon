---
name: launch-epbs-devnet-0
description: Launch erigon with Caplin (embedded CL) on the epbs-devnet-0 ethpandaops devnet (EIP-7732 ePBS / GLOAS). Manages start/stop with proper port offsets.
allowed-tools: Bash, Read, Write, Edit, Glob
allowed-prompts:
  - tool: Bash
    prompt: start, stop, and manage erigon with caplin for epbs-devnet-0
---

# Launch epbs-devnet-0 (EIP-7732 ePBS / GLOAS Devnet)

Run erigon with Caplin (embedded CL) on the epbs-devnet-0 ethpandaops devnet for testing EIP-7732 ePBS (enshrined Proposer-Builder Separation).

**Branch**: `feature/caplin_gloas` (must be checked out before building)

## Network Details

| Parameter | Value |
|-----------|-------|
| Chain ID | 7055777152 |
| Genesis timestamp | 1772634659 (2026-Mar-04 14:30:59 UTC) |
| Amsterdam timestamp | 1772636639 (genesis + 1980s, ~epoch 5) |
| GLOAS fork epoch | 5 |
| Fulu fork epoch | 0 (PeerDAS from genesis) |
| Electra fork epoch | 0 |
| Explorer | https://explorer.epbs-devnet-0.ethpandaops.io |
| Faucet | https://faucet.epbs-devnet-0.ethpandaops.io |
| Public RPC | https://rpc.epbs-devnet-0.ethpandaops.io |
| Checkpoint sync | https://checkpoint-sync.epbs-devnet-0.ethpandaops.io |
| Config repo | https://github.com/ethpandaops/epbs-devnets/tree/master/network-configs/devnet-0/metadata |

## Working Directory

Ask the user where they want the working directory. Default suggestion: `~/epbs-devnet-0/`.
Use `$WORKDIR` throughout to refer to the chosen path.

```
$WORKDIR/
├── genesis.json              # EL genesis
├── config.yaml               # CL beacon config (for Caplin)
├── genesis.ssz               # CL genesis state (for Caplin)
├── start-erigon.sh           # Erigon + Caplin start script
├── stop.sh                   # Stop erigon
├── clean.sh                  # Stop, wipe data, re-init genesis
├── erigon-data/              # Erigon datadir
├── erigon-console.log        # Erigon stdout/stderr
└── caplin-console.log        # Symlink or tail filter for CL logs
```

## Port Assignments (offset +200 to avoid conflicts with other devnets)

| Service | Port | Protocol |
|---------|------|----------|
| Erigon HTTP RPC | 8745 | TCP |
| Erigon Engine API (authrpc) | 8751 | TCP |
| Erigon WebSocket | 8746 | TCP |
| Erigon P2P | 30503 | TCP+UDP |
| Erigon gRPC | 9290 | TCP |
| Erigon Torrent | 42269 | TCP+UDP |
| Erigon pprof | 6260 | TCP |
| Erigon metrics | 6261 | TCP |
| Caplin Discovery (DISCV5) | 4200 | UDP |
| Caplin TCP | 4201 | TCP |
| Caplin Beacon API | 5755 | TCP |

## EL Bootnodes (enodes)

```
enode://0cbe293b282466ffcaf6b9d15c04bd98da960334bd67d904990de251f4ad6ab3bf5b13abbe68b3673ebd24e9bcb49810bee1bc2e3148395903e28c4326d27229@91.98.233.229:30303?discport=30303
enode://7075a92a904e56e5690b975983494d67359b904c6a4a4573bc58ad81c8471005dd04d9846c7e67a8f76d39f2482f39b653b8be2b99fd24a361e1f3eeff2132d7@178.104.16.27:30303?discport=30303
enode://ef94d742b42ed0873012167571ae7ec75f71fc92dedfc12c39babc85eb6a3fa8e7eec9c9a39c0de7e14dcfbe220c1d7c2ec794b7e3903f04953847aa188f8c60@65.21.245.12:30303?discport=30303
enode://72d2add0ace5db55ad43ab27db9203f9ca9ec4c0a2e16e6a4395b1934f0c7a91609527e4af6e40b693abffc28821a645ef2d98afb4b25168d5088c4a08fdb04f@195.201.39.213:30303?discport=30303
enode://5f4355bf5584f2dced36c6df17f1e794eeec1fd318496a237d091984b960366f78a1c3fb9d7386df38836901b5c1394a2b341cdc17d58bc5473f0153e3470cb9@128.140.69.39:30303?discport=30303
enode://fd17487a5500f7854c1c9602bc7eef7eec6947cc9da374064dfbfcd372f120a048f872de2d6c39a22bcda19f9af3e3f846712030f84d86f5994019a6296be8da@89.167.119.3:30303?discport=30303
enode://a318dc7c08c572d3601514d1629eeadc7343a73297e7d2e913d42b1c6e9a1b969684e012c976cb06c40f3709390fadc3dba1c23b202f1c966a7a81106daba0b8@91.107.234.234:30303?discport=30303
enode://7047865cf0733ab2c4a2b9311d43f2ede97d7aef2cc6e49229bd4fc61f95c36b67e431bd93a45a30af90c2fe8a7bfa19317083ae552cd46c5cb00180291a96aa@116.203.40.15:30303?discport=30303
enode://64e39c48d53b491dab9229c0a6ff8ca920d0f4d23b723defde94e540e906e276a97e53b0b8ef5b816a5cc2401725ade1526da1612546a3a9a1834c8150ba108b@178.104.5.127:30303?discport=30303
enode://fde263a4d86b468312e23aaab5558ebd9a5b53d27964a12acf1fc2a8b5466994d1dcdb0856b6c945ad7f1e3b562827ff6d3d4fd7b64da357bf8ae77403ec099f@89.167.115.65:30303?discport=30303
enode://e130f031893837e671656cab06cb3bd90e6bcb6d25ccece5b2335ba77ab3ba67de05a1808ef11c0ae9d9dfd014166d359bda4b3d8f69bf9203ec242b0f0c49a2@128.140.15.196:30303?discport=30303
enode://6c421083d0eb5bcc6d564e965d1d0b83a0bc6edc84673dc94da7f49817a6df44a4f5d8866cc87eca6c95f207b67f86f46e80d49386efe9fc9a9ed76e23708bb2@116.203.62.101:30303?discport=30303
```

## CL Bootnodes (ENRs)

```
enr:-MG4QMdgvRCEwpisZkDd9_FJqwKOSh8NUr7XlwimX5n-G3ROZ-T-GZM8iKR6SedO3dLQQ52Sc03RxSvGqOQlZQEG0q8Gh2F0dG5ldHOIAAAwAAAAAACDY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhFti6eWDbmZkhAAAAACJc2VjcDI1NmsxoQISe3F-1kWOj15uIzCY6Amj0lnZtu7SCsA63hXqStHzToN0Y3CCIyiDdWRwgiMo
enr:-QEauECUyUvfrPWptf-_pcsvTDui1ZfUa_iCe1H7w73SGbPbOnkTOhHMXfUd9p2aKVo7kgxV44sRbDB7MhonnYbumbbqCYdhdHRuZXRziAAAABgAAAAAg2NnY4GAhmNsaWVudNGKTGlnaHRob3VzZYU4LjEuMYRldGgykNvUtIGAiYdE__________-CaWSCdjSCaXCEsmgQG4NpcDaQKgEE-BwZj3EAAAAAAAAAAYNuZmSE29S0gYRxdWljgiMphXF1aWM2giMpiXNlY3AyNTZrMaECH5GVSllBpUR_I5UNncsmj0scOJAhOpD0QzkgqNcHN3CIc3luY25ldHMPg3RjcIIjKIR0Y3A2giMog3VkcIIjKIR1ZHA2giMo
enr:-QEauECBn82E0MbuupEFNgA2oIUyET_UiLYWLODm1QLYEdrFgACGtyhnR9E4ZMUqhh4ClWcGuIBwtqAL68-gKAxwH0_GCYdhdHRuZXRziAAAYAAAAAAAg2NnY4GAhmNsaWVudNGKTGlnaHRob3VzZYU4LjEuMYRldGgykNvUtIGAiYdE__________-CaWSCdjSCaXCEQRX1DINpcDaQKgEE-cAU3K8AAAAAAAAAAYNuZmSE29S0gYRxdWljgiMphXF1aWM2giMpiXNlY3AyNTZrMaEDpz7nEfcmoIFmCuXPP3PiDjZM2MlmYNx_5FJ33mx7SYyIc3luY25ldHMPg3RjcIIjKIR0Y3A2giMog3VkcIIjKIR1ZHA2giMo
enr:-QEauEBqrsuuWCenQRSla8nYzAaF4-347jeRMmFIrdDPPtPsaVmDEyPf5oYZAoCnCyqLmeTCG2wfnCTvaHLgKeBA-VTACYdhdHRuZXRziAAAAAAAwAAAg2NnY4GAhmNsaWVudNGKTGlnaHRob3VzZYU4LjEuMYRldGgykNvUtIGAiYdE__________-CaWSCdjSCaXCEw8kn1YNpcDaQKgEE-BwZ0f4AAAAAAAAAAYNuZmSE29S0gYRxdWljgiMphXF1aWM2giMpiXNlY3AyNTZrMaEDkSg2dXfySbaW9jF5XzxwmqURiAShKJ9gjLNVn_8Z1fWIc3luY25ldHMPg3RjcIIjKIR0Y3A2giMog3VkcIIjKIR1ZHA2giMo
enr:-PC4QKfdlFvygTmw5ylN5sThrJbFUTLus-QnI0eDzjneo9cCeZFAB86ZPJCAzzHU8nq5G0PHkNJ-Li8YKuCNS8OGRvYJh2F0dG5ldHOIAAAAAABgAACDY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhICMRSeDaXA2kCoBBPgcGVcgAAAAAAAAAAGDbmZkhAAAAACJc2VjcDI1NmsxoQIkDXpY-dT2HMMyReQlG-oehqFIsmOJX3YjgHlbvxscOIhzeW5jbmV0cw-DdGNwgiMohHRjcDaCIyqDdWRwgiMohHVkcDaCIyo
enr:-PC4QO_Js4SYPxxKaHiBw6lCqIUx69FoQRvu4U45ozdcwwLjcYWGK0-ZVvPrtX7u_qK39RVHF3s4jwndzDCw4y3WKgsJh2F0dG5ldHOIAAAAAACAAQCDY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhFmndwODaXA2kCoBBPnAFOy4AAAAAAAAAAGDbmZkhAAAAACJc2VjcDI1NmsxoQNuvD5NQ7j6vuvafukn0b2uUbynq14-sLZegdf3YYI6nIhzeW5jbmV0cw-DdGNwgiMohHRjcDaCIyqDdWRwgiMohHVkcDaCIyo
enr:-PC4QKi1VuqxcKZQSvse8Tfhb1vjMLhpznmO5E67IhQfqCOGFyFCdiMYqU0_LOykz4qjhIbdc-v37TqOoZpLtctBccoJh2F0dG5ldHOIAAAAAAADAACDY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhFtr6uqDaXA2kCoBBPgcHsYyAAAAAAAAAAGDbmZkhAAAAACJc2VjcDI1NmsxoQOJmG25i3P4vfdanNq6mU0B3NwcXcbt4lqhgwADY0ero4hzeW5jbmV0cw-DdGNwgiMohHRjcDaCIyqDdWRwgiMohHVkcDaCIyo
enr:-MG4QNz7N8P9LKophgQxj9vF2BLxkkseHSkJtuXVMditcIizRK5DMfi4MMfZu7428YqT1b-aOevMsFrS0V3g4P8pRRoGh2F0dG5ldHOIAADAAAAAAACDY2djMYRldGgykNvUtIGAiYdE__________-CaWSCdjSCaXCEdMsoD4lzZWNwMjU2azGhAnrlpFKTVH_ov-_Cvbte0Z0AkX1e5JyNlduZQY7d95QIiHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo
enr:-Nm4QPV3l3uqnP4dJIiqISqB7q0Kt2eNc-AYLFWCAaFAfY5RY_rF9SRVI3Om6lmA7J0CKFQ1x5QyGhMP0S5LAYQvu1-GAZy9sQHeh2F0dG5ldHOI__________-DY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhLJoBX-DbmZkhAAAAACEcXVpY4IyyIlzZWNwMjU2azGhAgH2WAu--Xjdl5evaNotunHP38ESVMJDKaYmooNRG0gViHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo
enr:-Nm4QCqn-e_leDYuQykCtKZMh9n8g43UL6_pOWTblneGudRTVqvf7vgbfAm7UdIShhG-z8pjhjLID9B-guDVMxW9kISGAZy9r6R3h2F0dG5ldHOI__________-DY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhFmnc0GDbmZkhAAAAACEcXVpY4IyyIlzZWNwMjU2azGhAmM7lP-weDISCqZDhQpckwA31sTReCo0c5nfe7brda9XiHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo
enr:-Nm4QDNzTZA-xprggpC_jlyOLdcMFeN-d3y34l8edLxOlr4BHkXWLl_JKFqthGkt2JniJaXoDxVGeA5PZD-EeokU0IaGAZy9sZGJh2F0dG5ldHOI__________-DY2djgYCEZXRoMpDb1LSBgImHRP__________gmlkgnY0gmlwhICMD8SDbmZkhAAAAACEcXVpY4IyyIlzZWNwMjU2azGhAlieLPTzHMEFvSGycm4HPP2LF3nbQGsTBa0ec2sZ8PgGiHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo
enr:-Mq4QEkcoKnjKyIdox2pHhi7HiI7PfJHDFLJV97gmh3_zknIT7KPCbSgpNGCx7Cj8NhnzXWVn4kUF57l5ttnB8Zd9RMIh2F0dG5ldHOIAAAAgAEAAACDY2djMoRldGgykNvUtIGAiYdE__________-CaWSCdjSCaXCEdMs-ZYNuZmSEAAAAAIlzZWNwMjU2azGhA3SULrj3F0f6RHsYwI-19_dhgRyeWnP-UAqZ3kXE6gvFiHN5bmNuZXRzD4N0Y3CCIyiDdWRwgiMo
```

## Workflow

### Step 1: Check Prerequisites

1. Verify we are on the `feature/caplin_gloas` branch:
   ```bash
   cd /root/work/erigon && git branch --show-current
   ```
   If not on the correct branch, warn the user and ask if they want to switch.

2. Verify erigon binary exists at `./build/bin/erigon`. If not, invoke `/erigon-build`.

3. Verify config files exist in `$WORKDIR` (genesis.json, config.yaml, genesis.ssz).
   If not, download them:
   ```bash
   mkdir -p $WORKDIR
   curl -sL -o $WORKDIR/genesis.json https://raw.githubusercontent.com/ethpandaops/epbs-devnets/master/network-configs/devnet-0/metadata/genesis.json
   curl -sL -o $WORKDIR/config.yaml https://raw.githubusercontent.com/ethpandaops/epbs-devnets/master/network-configs/devnet-0/metadata/config.yaml
   curl -sL -o $WORKDIR/genesis.ssz https://raw.githubusercontent.com/ethpandaops/epbs-devnets/master/network-configs/devnet-0/metadata/genesis.ssz
   ```

### Step 2: Initialize Datadir (first run only)

If `$WORKDIR/erigon-data/chaindata` does not exist:
```bash
./build/bin/erigon init --datadir $WORKDIR/erigon-data $WORKDIR/genesis.json
```

### Step 3: Create Scripts (first run only)

If the start/stop/clean scripts don't exist yet, generate them. The scripts must use absolute paths based on `$WORKDIR`. Key details:

**start-erigon.sh** — Runs erigon with Caplin (embedded CL). Single process handles both EL and CL.
- Env vars: `ERIGON_EXEC3_PARALLEL=true`, `ERIGON_ASSERT=true`, `ERIGON_EXEC3_WORKERS=12`, `LOG_HASH_MISMATCH_REASON=true`
- EL flags:
  - `--datadir=$WORKDIR/erigon-data`
  - `--networkid=7055777152`
  - `--bootnodes=<all 12 EL enodes comma-separated>`
  - `--staticpeers=<all 12 EL enodes comma-separated>`
  - `--prune.mode=minimal`
  - `--http.addr=0.0.0.0 --http.port=8745 --http.vhosts=* --http.corsdomain=*`
  - `--http.api=eth,erigon,engine,debug,net,web3`
  - `--ws --ws.port=8746`
  - `--authrpc.addr=0.0.0.0 --authrpc.port=8751 --authrpc.vhosts=*`
  - `--port=30503`
  - `--private.api.addr=127.0.0.1:9290`
  - `--torrent.port=42269`
  - `--pprof --pprof.addr=0.0.0.0 --pprof.port=6260`
  - `--metrics --metrics.addr=0.0.0.0 --metrics.port=6261`
- Caplin flags (embedded CL):
  - `--caplin.custom-config=$WORKDIR/config.yaml`
  - `--caplin.custom-genesis=$WORKDIR/genesis.ssz`
  - `--caplin.checkpoint-sync-url=https://checkpoint-sync.epbs-devnet-0.ethpandaops.io`
  - `--sentinel.bootnodes=<all 12 CL ENRs comma-separated>`
  - `--caplin.discovery.addr=0.0.0.0`
  - `--caplin.discovery.port=4200`
  - `--caplin.discovery.tcpport=4201`
  - `--beacon.api=beacon,builder,config,debug,events,node,validator,lighthouse`
  - `--beacon.api.addr=0.0.0.0`
  - `--beacon.api.port=5755`
- Do NOT use `--externalcl` (we want the embedded Caplin CL)

**stop.sh** — Stops erigon (`pkill -f "datadir.*epbs-devnet-0/erigon-data"`).

**clean.sh** — Runs `stop.sh`, removes erigon chain data (chaindata, snapshots, txpool, nodes, temp, caplin) and re-initializes genesis.

### Step 4: Start Erigon + Caplin

```bash
cd $WORKDIR && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
```

Verify it started:
- Check `tail $WORKDIR/erigon-console.log` for startup messages
- Check port binding: `ss -tlnp | grep 8745` (HTTP RPC)
- Check port binding: `ss -tlnp | grep 5755` (Beacon API)
- Look for Caplin logs: `grep -i "caplin\|beacon\|checkpoint" $WORKDIR/erigon-console.log | tail -20`

### Step 5: Monitor

```bash
# Full log stream
tail -f $WORKDIR/erigon-console.log

# Check EL block height via RPC
curl -s http://localhost:8745 -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' | python3 -m json.tool

# Check CL sync status via Beacon API
curl -s http://localhost:5755/eth/v1/node/syncing | python3 -m json.tool

# Check CL peers
curl -s http://localhost:5755/eth/v1/node/peer_count | python3 -m json.tool

# Check CL head
curl -s http://localhost:5755/eth/v1/beacon/headers/head | python3 -m json.tool
```

### Step 6: Stop

```bash
bash $WORKDIR/stop.sh
```

### Step 7: Clean (wipe data and re-init)

```bash
bash $WORKDIR/clean.sh
```

This runs `stop.sh`, removes erigon data (chaindata, snapshots, txpool, nodes, temp, caplin), then re-initializes genesis. After clean, start again with Step 4.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No EL peers | Check firewall allows port 30503. Try adding `--nat=extip:<your-ip>`. Re-fetch enodes from inventory API. |
| No CL peers | Check firewall allows port 4200/4201. ENR bootnodes may have changed. |
| Checkpoint sync fails | Check that `https://checkpoint-sync.epbs-devnet-0.ethpandaops.io` is reachable. Try `curl -s https://checkpoint-sync.epbs-devnet-0.ethpandaops.io/eth/v1/beacon/states/finalized/finality_checkpoints`. |
| "Head is optimistic" | Normal during initial sync. CL is ahead of EL. Will resolve as EL catches up. |
| GLOAS fork issues | Ensure you are on `feature/caplin_gloas` branch and binary is freshly built. |
| Port conflict | Check `ss -tlnp | grep <port>`. Kill conflicting process or adjust port offset. |
| Stale bootnodes | Re-fetch from `https://config.epbs-devnet-0.ethpandaops.io/api/v1/nodes/inventory` |

## Refreshing Bootnodes

If peers are not connecting, fetch fresh bootnodes:

```bash
# EL enodes
curl -sL https://raw.githubusercontent.com/ethpandaops/epbs-devnets/master/network-configs/devnet-0/metadata/enodes.txt

# CL ENRs
curl -sL https://raw.githubusercontent.com/ethpandaops/epbs-devnets/master/network-configs/devnet-0/metadata/bootstrap_nodes.txt
```

Or from the live inventory API:
```bash
curl -s https://config.epbs-devnet-0.ethpandaops.io/api/v1/nodes/inventory
```
