# Caplin (Consensus Layer)

Caplin is Erigon's embedded Beacon Chain client implementing Ethereum's proof-of-stake consensus.

## Consensus Spec Conformance

All changes under `cl/` are consensus-critical. Agents must treat the upstream
Ethereum consensus specs as authoritative; local tests are not sufficient proof
of correctness.

Before changing or reviewing `cl/` code:

1. Read `cl/CLAUDE.md`.
2. Read the `CLAUDE.md` file for every touched consensus area that has one, for
   example:

- `cl/phase1/forkchoice/CLAUDE.md`
- `cl/transition/CLAUDE.md`
- `cl/phase1/core/state/CLAUDE.md`

3. Identify the upstream spec section for each changed behavior.
4. Check fork/version boundaries and preserve pre-fork behavior.
5. Mention the consulted `CLAUDE.md` files and spec sections in the final
   summary or review note.

## Directory Structure

| Directory | Purpose |
|-----------|---------|
| `beacon/` | Beacon API HTTP handlers |
| `phase1/forkchoice/` | LMD-GHOST fork choice |
| `phase1/execution_client/` | Engine API bridge |
| `phase1/core/state/` | Beacon state machine |
| `phase1/network/` | Gossip handlers |
| `cltypes/` | Consensus types (blocks, attestations) |
| `sentinel/` | libp2p P2P networking |
| `pool/` | Operations pools (attestations, slashings) |
| `validator/` | Attestation producer |

## Key Components

### Fork Choice (`phase1/forkchoice/`)
- `forkchoice.go` - LMD-GHOST implementation
- `on_block.go` - Block processing
- `on_attestation.go` - Attestation handling
- Tracks finality and justification checkpoints

### Engine API (`phase1/execution_client/`)
Bridge to execution layer:
- `NewPayload` - Receive blocks from consensus
- `ForkchoiceUpdated` - Set canonical head
- `GetPayload` - Request block for building

### Beacon State (`phase1/core/state/`)
- State machine with fork upgrades (Altair, Bellatrix, Capella, Deneb)
- Validator registry management
- Epoch processing

### Sentinel (`sentinel/`)
- libp2p-based P2P networking
- GossipSub for block/attestation propagation
- Peer scoring and discovery

## Beacon API (`beacon/handler/`)

REST API endpoints:
- Block production and validation
- Validator duties
- Chain state queries
- Node status

## Enable/Disable

```bash
# Caplin enabled by default (--internalcl)
./build/bin/erigon --datadir=./data

# Use external consensus client
./build/bin/erigon --externalcl --datadir=./data
```

## Archive Mode

```bash
# Enable historical state/block storage
./build/bin/erigon --caplin.archive --datadir=./data
```
