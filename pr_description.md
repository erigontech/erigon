# engine, mining: derive slot duration from CL timestamps to fix devnet reorgs

## Problem

The `assertoor_regular_test` CI job intermittently fails on the `stability-check` test with:
- `check_consensus_reorgs`: "max reorgs per epoch exceeded (have: 2.125, want <= 2)"
- `check_consensus_attestation_stats`: attestation votes too low

### Root Cause

`Config.SecondsPerSlot()` returns a hardcoded 12 seconds for all Ethereum chains. The Kurtosis devnet test configures `SECONDS_PER_SLOT: 4` on the CL side, but the EL has no way to know this — the genesis JSON has no such field.

This causes two problems:

1. **Block builder timeout mismatch**: `NewBlockBuilder` uses `SecondsPerSlot()` (12s) as `maxBuildTimeSecs`. With 4-second slots, the builder polls the txpool for up to 12 seconds (3 full slots), logging "Stopping block builder due to max build time exceeded". By the time the block is ready, the CL has moved on, causing chain reorgs.

2. **Engine API wait timeout mismatch**: `waitForResponse` in `forkchoiceUpdated` and `getPayload` uses `SecondsPerSlot()` (12s), which is excessive for 4-second slots.

Evidence from failed CI run EL logs:
- 32 occurrences of "Stopping block builder due to max build time exceeded"
- Block 122 took 12.04 seconds to build
- Reorg distances escalated from 4 to 12
- 10911/14056 log lines (77%) were "Filtration" spam from the tight 50ms polling loop running for 12 seconds

## Fix

### 1. Derive actual slot duration from CL-provided timestamps (`execution/chain/chain_config.go`)

Added an `observedSecondsPerSlot` atomic field to `Config`. When set, `SecondsPerSlot()` returns this value instead of the hardcoded default. This is thread-safe and backward-compatible — existing mainnet/testnet behavior is unchanged since the observed value matches the hardcoded value (12s).

### 2. Set observed slot duration in `forkchoiceUpdated` (`execution/engineapi/engine_server.go`)

When the CL sends `PayloadAttributes` with a timestamp, we already have the parent header's timestamp from the head hash lookup. The difference `timestamp - headHeader.Time` gives the actual slot duration as communicated by the CL. We store this via `SetObservedSecondsPerSlot()`.

### 3. Reduce Filtration log spam (`execution/stagedsync/stage_mining_exec.go`)

Changed the "Filtration" log from `Info` to `Debug` level. This log is emitted every 50ms during txpool polling and produced 77% of all EL log output in devnet runs, making it difficult to find actual errors.

## Verification

Ran the full Kurtosis assertoor test suite 3 times locally. All 8 tests passed in every run:
- `all-opcodes-test`: SUCCESS
- `blob-transactions-test`: SUCCESS
- `dencun-opcodes-test`: SUCCESS
- `eoa-transactions-test`: SUCCESS
- `synchronized-check`: SUCCESS
- `validator-exit-test`: SUCCESS
- `block-proposal-check`: SUCCESS
- `stability-check`: SUCCESS (previously failing)

Block build times dropped from 12+ seconds to ~1.3 seconds. Zero persistent "Stopping block builder" warnings.
