# Erigon Dev Chain

The `--chain=dev` mode runs Erigon as a single-process PoS node with an embedded
beacon chain (Caplin) and deterministic BLS validator. It uses the same fork choice,
block production, and state transition code as mainnet — no separate consensus client
is needed.

## Quick Start

```bash
make erigon
./build/bin/erigon --chain=dev --datadir=dev \
  --beacon.api=beacon,validator,node,config
```

This starts a node that:

- Creates a PoS genesis with 64 validators (Deneb fork from genesis)
- Runs the embedded Caplin consensus layer
- Proposes a block every 6 seconds (one per slot)
- Executes blocks through the standard EL pipeline

## Flags

| Flag                                        | Default  | Description                                                                      |
|---------------------------------------------|----------|----------------------------------------------------------------------------------|
| `--chain=dev`                               | —        | **Required.** Enables dev mode with PoS genesis.                                 |
| `--beacon.api=beacon,validator,node,config` | —        | **Required.** Enables the Beacon API endpoints used by the embedded validator.   |
| `--dev-validator-seed`                      | `devnet` | Deterministic seed for BLS key derivation. Change to run independent dev chains. |
| `--dev-validator-count`                     | `64`     | Number of genesis validators.                                                    |
| `--dev.slot-time`                           | `6`      | Seconds per slot (minimum: 2). Lower values produce blocks faster.               |
| `--datadir`                                 | `dev`    | Data directory. Each run creates fresh state (ephemeral mode).                   |

### Optional flags

| Flag                                                | Description                           |
|-----------------------------------------------------|---------------------------------------|
| `--http.api=eth,erigon,web3,net,debug,trace,txpool` | Enable JSON-RPC APIs for interaction. |
| `--http.port=8545`                                  | JSON-RPC port (default: 8545).        |
| `--beacon.api.port=5555`                            | Beacon API port (default: 5555).      |

## Example: Fast block production

For faster iteration during development, set a 2-second slot time:

```bash
./build/bin/erigon --chain=dev --datadir=dev \
  --beacon.api=beacon,validator,node,config \
  --dev.slot-time=2 \
  --http.api=eth,erigon,web3,net,debug,trace,txpool
```

## Interacting with the node

Query the chain via JSON-RPC:

```bash
# Get chain ID (dev chain = 0x539 / 1337)
curl -s -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' \
  localhost:8545

# Get latest block number
curl -s -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  localhost:8545

# Get block by number
curl -s -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":1}' \
  localhost:8545
```

Query the beacon chain via the Beacon API:

```bash
# Get current head slot
curl -s localhost:5555/eth/v1/beacon/headers/head | jq .data.header.message.slot

# Get genesis info
curl -s localhost:5555/eth/v1/beacon/genesis | jq .data
```

## Pre-funded account

The dev genesis pre-funds a deterministic signer account derived from the validator
seed. The address is logged at startup:

```
Using PoS dev mode  seed=devnet validators=64 signer=0x78eF752367584ee389aCB8824Ceec734456402b6
```

## How it works

1. **Genesis**: A PoS genesis is created with TTD=0 and all forks (Altair through
   Deneb) activated at epoch 0. The EL genesis has Shanghai+Cancun enabled from
   block 0. Validators are derived deterministically from the seed.

2. **Consensus**: Caplin runs the same fork choice and state transition as mainnet.
   The embedded validator checks proposer duties each slot and produces blocks via
   the standard Beacon API (`/eth/v3/validator/blocks/{slot}`).

3. **Execution**: Block payloads are assembled by the EL's `AssembleBlock`, including
   any pending transactions from the txpool. The CL signs and submits the block,
   which flows through `NewPayload` → `ForkChoiceUpdated` — identical to mainnet.

## Notes

- Dev mode uses the **minimal preset** (8 slots per epoch, 32-member sync committee)
  for faster epoch transitions. This matches the Ethereum spec's minimal preset used
  in consensus testing.
- P2P networking is disabled (`--nodiscover` is set automatically).
- Data is ephemeral by default — each run starts from a fresh genesis.
- The `--mine` flag from the old dev mode is no longer used. The `--dev.period` flag has been removed.
