---
name: launch-devnet
description: Launch erigon (EL) + a CL client on any ethpandaops devnet. Takes a devnet landing-page URL (e.g. https://bal-devnet-3.ethpandaops.io) and discovers everything else (chain id, fork schedule, EL/CL bootnodes, client images, ports) from the network's config service. Generates start/stop/clean scripts, monitors EL/CL progress, and investigates failures by comparing against the rest of the network rather than assuming the bug is in erigon.
allowed-tools: Bash, Read, Write, Edit, Glob, WebFetch
allowed-prompts:
  - tool: Bash
    prompt: start, stop, and manage erigon and CL client processes for an ethpandaops devnet
---

# Launch a generic ethpandaops devnet

Run erigon (EL) + a CL client on an arbitrary ethpandaops devnet. The user supplies only the landing-page URL; the skill discovers every parameter at runtime so the same flow works for `bal-devnet-N`, `fusaka-devnet-N`, `gloas-devnet-N`, `pectra-devnet-N`, etc.

## Inputs

The user provides:

1. **Devnet URL** — landing page like `https://bal-devnet-3.ethpandaops.io`. Required.
2. **Working directory** — defaults to `~/<devnet-name>/`. In auto mode, use the default.

Optional overrides (ask only if relevant or ambiguous):
- CL client choice (default `lighthouse`).
- CL image tag override (default = the tag the network publishes in its inventory).
- Port offset (default `+100`; bumped automatically if the offset ports are already bound).
- Extra erigon flags / env vars (e.g. `--prune.mode=...`, `ERIGON_EXEC3_PARALLEL=true`).

## Step 1 — Resolve devnet config

Derive `DEVNET` from the URL hostname (strip `.ethpandaops.io`). For `https://bal-devnet-3.ethpandaops.io` that is `bal-devnet-3`. Build:

- Landing page:    `https://${DEVNET}.ethpandaops.io`
- Config base:     `https://config.${DEVNET}.ethpandaops.io`
- Public RPC:      `https://rpc.${DEVNET}.ethpandaops.io`
- Public beacon:   `https://beacon.${DEVNET}.ethpandaops.io`
- Checkpoint sync: `https://checkpoint-sync.${DEVNET}.ethpandaops.io`
- Dora explorer:   `https://dora.${DEVNET}.ethpandaops.io`
- Forkmon:         `https://forkmon.${DEVNET}.ethpandaops.io`
- Spec notes:      linked from the landing page (typically `https://notes.ethereum.org/@ethpandaops/<DEVNET>`)

Optionally use `WebFetch` on the landing page to confirm the network exists and to grab the spec-notes link. Then download config artefacts:

```bash
mkdir -p $WORKDIR/testnet-config
CFG=https://config.${DEVNET}.ethpandaops.io
curl -fsSL ${CFG}/el/genesis.json    -o $WORKDIR/genesis.json
curl -fsSL ${CFG}/cl/config.yaml     -o $WORKDIR/testnet-config/config.yaml
curl -fsSL ${CFG}/cl/genesis.ssz     -o $WORKDIR/testnet-config/genesis.ssz
curl -fsSL ${CFG}/api/v1/nodes/inventory -o $WORKDIR/inventory.json
echo 0 > $WORKDIR/testnet-config/deploy_block.txt
echo 0 > $WORKDIR/testnet-config/deposit_contract_block.txt
```

If any of those 404, the derived name is wrong or the network uses a non-standard layout — ask the user for the correct config base URL. **Do not silently substitute defaults or stub data.**

## Step 2 — Extract network parameters

Parse `genesis.json`:
- `config.chainId` → `--networkid` for erigon
- All `config.*Time` fields → fork timestamps (record for log readability)

Parse `testnet-config/config.yaml`:
- `CONFIG_NAME`, `MIN_GENESIS_TIME`, `SECONDS_PER_SLOT`, `SLOTS_PER_EPOCH`
- Fork epoch fields (`*_FORK_EPOCH`) for scheduled forks

Parse `inventory.json` (`ethereum_pairs.<node>.{consensus,execution}`):
- `consensus.image` → available CL client images (lighthouse, lodestar, prysm, teku, nimbus). Pick the user's preferred one (default `lighthouse`).
- `execution.image` → available EL clients on the network. Keep this list — it's needed later when investigating failures.
- `consensus.enr` (all entries) → CL bootnodes
- `execution.enode` (all entries) → EL bootnodes
- `consensus.beacon_uri` and `execution.rpc_uri` → per-peer endpoints for cross-checking

Write a single human-readable summary to `$WORKDIR/devnet-info.txt`: chain id, key fork timestamps, # of bootnodes, chosen CL image, port offset, plus the public RPC/beacon URLs. Print the summary to the user and stop for confirmation if anything looks off (wrong chain id, missing forks, no peers in inventory).

## Step 3 — Working directory layout

```
$WORKDIR/
├── devnet-info.txt          # summary of what was fetched (chain id, forks, ports, ...)
├── genesis.json             # EL genesis
├── inventory.json           # raw inventory (consumed by start scripts and the failure flow)
├── testnet-config/          # CL testnet dir
│   ├── config.yaml
│   ├── genesis.ssz
│   ├── deploy_block.txt
│   └── deposit_contract_block.txt
├── start-erigon.sh          # FIRST: creates jwt.hex
├── start-cl.sh              # SECOND: connects to erigon over engine API
├── stop.sh                  # stops both
├── clean.sh                 # stop + wipe data + re-init
├── erigon-data/             # erigon datadir (jwt.hex lives here)
├── cl-data/                 # CL client datadir
├── erigon-console.log
└── cl-console.log
```

## Step 4 — Port assignments

Default offset is `+100` on top of erigon defaults (see [erigon-network-ports](../erigon-network-ports/SKILL.md) for the full list and CLI flags). Before generating scripts, check every candidate port:

```bash
for p in <each chosen port>; do
  ss -tlnp 2>/dev/null | awk -v P=":$p" '$4 ~ P {print P}'
done
```

If any port is already bound, bump the whole offset by +100 (try `+200`, `+300`, …) and re-check. Record the chosen offset in `devnet-info.txt`. Match the CL ports to the same offset family so the user only has one number to remember.

## Step 5 — Initialize erigon datadir

```bash
./build/bin/erigon init --datadir $WORKDIR/erigon-data $WORKDIR/genesis.json
```

If `./build/bin/erigon` is missing, invoke `/erigon-build` first.

## Step 6 — Generate `start-erigon.sh`

Required flags (substitute values discovered in Step 2):
- `--datadir=$WORKDIR/erigon-data`
- `--externalcl`
- `--networkid=<chainId>`
- `--bootnodes="<comma-joined enodes from inventory>"`
- `--staticpeers="<same enodes>"` — small devnets benefit from explicit static peers
- All offset ports (HTTP, WS, authrpc, P2P, p2p.allowed-ports, torrent, private.api.addr, pprof, metrics)
- `--http`, `--http.api=eth,erigon,engine,debug,net,trace,txpool,web3`, `--http.addr=127.0.0.1`
- `--ws`
- `--authrpc.addr=127.0.0.1`, `--authrpc.vhosts=*`
- `--pprof`, `--metrics` so the node is debuggable under load

Optional env vars to set in the script body (only when the user asks for them):
- `ERIGON_EXEC3_PARALLEL=true`, `ERIGON_EXEC3_WORKERS=12`
- `ERIGON_ASSERT=true`, `LOG_HASH_MISMATCH_REASON=true`

Do not bake in fork-specific or experimental flags unless the user requests them. The script should be a thin wrapper, not a config dump.

## Step 7 — Generate `start-cl.sh`

The script runs the chosen CL client via Docker with `--network=host`. Lighthouse is the default — recipe:

```bash
JWT=$WORKDIR/erigon-data/jwt.hex
test -f "$JWT"   # MUST exist; start-erigon.sh creates it

docker pull <CL image from inventory>
docker rm -f <DEVNET>-cl 2>/dev/null

docker run -d --name <DEVNET>-cl --network=host \
  -v $WORKDIR/testnet-config:/config:ro \
  -v $WORKDIR/cl-data:/data \
  -v "$JWT":/jwt.hex:ro \
  <CL image from inventory> \
  lighthouse bn \
    --testnet-dir=/config \
    --datadir=/data \
    --execution-endpoint=http://127.0.0.1:<authrpc port> \
    --execution-jwt=/jwt.hex \
    --boot-nodes="<comma-joined ENRs>" \
    --port=<CL P2P> --quic-port=<CL QUIC> \
    --http --http-port=<CL HTTP> --http-address=0.0.0.0 \
    --metrics --metrics-port=<CL metrics> \
    --checkpoint-sync-url=https://checkpoint-sync.${DEVNET}.ethpandaops.io \
    --disable-enr-auto-update \
    --suggested-fee-recipient=0x0000000000000000000000000000000000000000
```

For non-lighthouse CLs, look up the client's CLI flags in the ethpandaops repo or the client's docs before generating the script. Don't guess flag names — different clients spell things differently (e.g. lodestar uses `beacon`, prysm uses `--datadir=/data`, teku uses TOML config). When unsure, ask the user.

## Step 8 — Start, in order

Erigon must start first because it generates `jwt.hex`.

```bash
cd $WORKDIR && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
# Poll until both conditions are true (timeout ~60s):
#   - $WORKDIR/erigon-data/jwt.hex exists
#   - the authrpc port is bound by the erigon PID
cd $WORKDIR && nohup bash start-cl.sh > cl-console.log 2>&1 &
```

Verify before moving on:
- `tail -n 80 erigon-console.log` — no fatal errors, engine API listener up
- `ss -tlnp | grep <authrpc port>` — bound by erigon
- `tail -n 80 cl-console.log` — CL connected to engine API and starting checkpoint sync (or genesis sync if checkpoint endpoint is unavailable)

## Step 9 — Monitoring

Run these at intervals and report a single status line each pass:

```bash
HTTP=<erigon http port>; CL=<cl http port>
# Erigon head
EH=$(curl -s http://localhost:$HTTP -X POST -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))')
# Network head from public RPC
NH=$(curl -s https://rpc.${DEVNET}.ethpandaops.io -X POST -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))')
# Erigon peer count
EP=$(curl -s http://localhost:$HTTP -X POST -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}' \
  | python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"],16))')
# CL sync
CS=$(curl -s http://localhost:$CL/eth/v1/node/syncing)
echo "EL head=$EH (network=$NH, delta=$((NH-EH)))  EL peers=$EP  CL: $CS"
```

What to watch for:
- **EL head delta closing or stuck**: stuck after >2 min → suspect sync issue.
- **EL peers = 0 for >30s**: bootnodes unreachable or firewall blocking p2p; re-check static peers.
- **CL `is_syncing=true` with `sync_distance` not decreasing**: CL can't import payloads; investigate engine API errors next.
- **`erigon-console.log` "invalid payload" / "bad block"**: jump straight to the failure-investigation flow below.

## Step 10 — Stop and clean

`stop.sh`:
- `docker stop <DEVNET>-cl` (and `docker rm`)
- `pkill -f "datadir.*$WORKDIR/erigon-data"`

`clean.sh`:
- runs `stop.sh`
- removes `erigon-data/{chaindata,snapshots,txpool,nodes,temp}` and `cl-data/*`
- re-runs `erigon init`

## Investigating failures — finding the absolute truth

**Default assumption: nobody is right yet.** On a multi-client devnet, when erigon disagrees with another node, the other node may be the buggy one — or the spec itself is ambiguous. Always pin down the actual root cause before proposing a fix. Erigon may be doing the right thing per the spec while the rest of the network is wrong; or erigon may be wrong; or two factions of clients may disagree because the spec is unclear. All three happen on devnets — that is what devnets are for.

### A. Establish what the network is doing

For any disagreement, fetch the same data from at least **two other clients with different EL implementations** so you don't end up comparing erigon to a single peer that may itself be buggy. Use `inventory.json` to find direct endpoints:

```bash
# Pick two distinct EL clients (ideally different vendors, e.g. one geth-paired, one besu-paired)
jq -r '.ethereum_pairs | to_entries[] | select(.value.execution.client != "erigon") | "\(.value.execution.client) \(.value.execution.rpc_uri)"' $WORKDIR/inventory.json
```

Compare:
- `eth_getBlockByNumber` — block hash, state root, receipts root at the divergence height
- `eth_getBalance`, `eth_getTransactionCount`, `eth_getStorageAt` for accounts in the diverging block
- Beacon API `/eth/v2/beacon/blocks/<slot>` at the divergence slot
- Forkchoice store: `/eth/v1/debug/fork_choice` (CL)

Possible outcomes:
- **Two non-erigon clients agree, erigon disagrees** → most likely an erigon bug. Drill down in step B.
- **Clients split into factions** (e.g. geth + nethermind agree, erigon + reth agree) → spec ambiguity or a shared library issue. Note both factions, find the relevant EIP, and report.
- **Every client gives a different answer** → the network or genesis is broken. Check forkmon, the spec-notes page, and the devnet's Discord / GitHub before doing anything else.
- **Everyone agrees** including erigon → there isn't actually a divergence; re-check what the original symptom was.

### B. Drill down on a concrete divergence

When you have a specific block/slot where erigon disagrees:

1. From a non-erigon EL, fetch the block's transactions and receipts.
2. Re-run that single block under erigon with `LOG_HASH_MISMATCH_REASON=true` and find which account or storage slot differs.
3. Map the diff to a specific EVM op, hardfork rule, gas accounting change, or RLP encoding.
4. Cross-check against the actual EIP text. EIPs are linked from the devnet's notes page (look for "Network Spec"). Treat the EIP as authoritative — not other clients.
5. If our behaviour matches the spec and another client doesn't, write that up with quotes from the EIP and surface it. The bug is elsewhere.
6. If our behaviour does not match the spec, that is the erigon bug — file/share with the user.

Tools that help:
- `LOG_HASH_MISMATCH_REASON=true` — erigon prints the offending account/storage on state-root mismatch.
- `debug_traceTransaction` on both erigon and another EL (same tx, same block) — diff the traces.
- `debug_traceBlockByNumber` for whole-block diffs.

### C. Common false-positive signals

These look scary in logs but are usually fine:
- "Head is optimistic" during initial sync — erigon is just behind the CL.
- Engine API timeouts on the very first newPayload after genesis — checkpoint-sync race; resolves on its own.
- Single missed slot — proposers miss slots normally on small devnets.
- `peers: 0` for the first ~30 s before discv5 finds peers.

Don't escalate any of these without confirming they persist for >2 minutes.

### D. When (and how) to ask the user

Surface findings to the user when **all three** of these are true:
- Reproducible across a restart.
- You've identified the specific block/slot/account/storage that differs.
- You have at least one non-erigon EL agreeing on the alternative result, OR concrete spec text supporting one side.

Bad: "erigon failed to sync, please advise."
Good: "At block 12345, erigon's state root is `0xaaa…` but `geth` and `besu` agree on `0xbbb…`. The differing account is `0xabc…` — its balance is 1 wei lower in erigon. EIP-XXXX section 'Gas accounting' specifies `<quote>`, which matches `<which side>`. I think `<X>` is the bug; want me to bisect by transaction index, or look at the trace yourself?"

When you can't decide between two equally plausible root causes, present both with their evidence and ask which to chase first. Don't guess and don't pick the one that's easier to fix.

## See also

- [erigon-network-ports](../erigon-network-ports/SKILL.md) — full port reference for running multiple instances
- [erigon-build](../erigon-build/SKILL.md) — build the erigon binary
- [erigon-ephemeral](../erigon-ephemeral/SKILL.md) — throwaway nodes against an existing datadir
- [bal-devnet-ab-test](../bal-devnet-ab-test/SKILL.md) — A/B testing wrapper specific to BAL devnets
