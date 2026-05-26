---
name: erigondb-sync-integration-test-plan
description: Integration test plan for erigondb.toml settings resolution across 3 runtime scenarios (legacy, fresh+downloader, fresh+no-downloader)
allowed-tools: Bash, Read
user-invocable: false
---

# erigondb.toml Integration Test Plan

Verify that `erigondb.toml` settings are correctly resolved across three distinct runtime scenarios: legacy datadirs, fresh sync with downloader, and fresh sync without downloader.

Use this plan after any changes to the erigondb.toml resolution logic (creation, defaults, legacy detection, downloader delivery) to confirm all paths still work.

## Parameters

Two values must be provided by the caller:

| Parameter | Description | Example |
|-----------|-------------|---------|
| **chain** | Chain name passed to `--chain=` | `hoodi`, `mainnet` |
| **legacy datadir** | Path to an existing Erigon datadir that has `preverified.toml` but **no** `erigondb.toml` | `~/eth-nodes/erigon33-hoodi-stable` |

All scenarios use `--prune.mode=archive`.

## Pre-requisites

1. **Built binary** — invoke `/erigon-build` if `./build/bin/erigon` does not exist.
2. **Legacy datadir** — the user-provided path must contain `snapshots/preverified.toml` and must **not** contain `snapshots/erigondb.toml`.
3. **Port availability** — use `/erigon-network-ports` for reference. Each scenario uses a different port offset (+100, +200, +300) via `/erigon-ephemeral` conflict detection.

## Scenario A: Legacy datadir

**Goal**: Starting on a legacy datadir (has `preverified.toml`, no `erigondb.toml`) creates `erigondb.toml` with legacy settings (`step_size = 1562500`).

### Steps

1. Create an ephemeral clone of the user's legacy datadir using `/erigon-ephemeral` (Mode B: clone). This gives an isolated copy so the original is untouched.
2. Confirm pre-conditions on the clone:
   - `snapshots/preverified.toml` exists
   - `snapshots/erigondb.toml` does **not** exist
3. Start erigon with port offset **+100** (via `/erigon-ephemeral` port conflict detection):
   ```
   ./build/bin/erigon \
     --datadir=<clone-path> \
     --chain=<chain> \
     --prune.mode=archive \
     <port flags at +100 offset>
   ```
4. Wait for startup (~15s). Check logs for a message indicating legacy settings detection (e.g., `Creating erigondb.toml with LEGACY settings`).
5. Verify the generated file:
   ```bash
   cat <clone-path>/snapshots/erigondb.toml
   ```
   Expected: `step_size = 1562500` and `references_in_commitment_branches = true`
6. Kill the process and clean up the ephemeral datadir.

## Scenario B: Fresh sync with downloader

**Goal**: A fresh datadir (no `preverified.toml`, no `erigondb.toml`) starts with code defaults (`step_size = 1562500`), then the downloader delivers the network's `erigondb.toml` during the header-chain phase, which may have different settings (e.g., `step_size = 390625` for hoodi).

### Steps

1. Create an empty ephemeral datadir using `/erigon-ephemeral` (Mode A: empty).
2. Start erigon with port offset **+200**:
   ```
   ./build/bin/erigon \
     --datadir=<empty-path> \
     --chain=<chain> \
     --prune.mode=archive \
     <port flags at +200 offset>
   ```
3. Check early logs for a message indicating defaults are being used (e.g., `erigondb.toml not found, using defaults`) with `step_size=1562500`.
4. Wait for the header-chain download phase to complete (~30-120s depending on network). Watch logs for:
   - `[1/6 OtterSync] Downloader completed header-chain`
   - `Reading DB settings from existing erigondb.toml`
   - `erigondb stepSize changed, propagating` (if the network's settings differ from the code defaults)
5. Verify the delivered file:
   ```bash
   cat <empty-path>/snapshots/erigondb.toml
   ```
   The values come from the network's published `erigondb.toml` (check the chain's webseed for the canonical values).
6. Kill the process and clean up the ephemeral datadir.

## Scenario C: Fresh sync with `--no-downloader`

**Goal**: A fresh datadir with `--no-downloader` immediately writes `erigondb.toml` with code defaults (`step_size = 1562500`).

### Steps

1. Create an empty ephemeral datadir using `/erigon-ephemeral` (Mode A: empty).
2. Start erigon with port offset **+300** and `--no-downloader`:
   ```
   ./build/bin/erigon \
     --datadir=<empty-path> \
     --chain=<chain> \
     --prune.mode=archive \
     --no-downloader \
     <port flags at +300 offset>
   ```
3. Check logs for a message indicating defaults were written immediately (e.g., `Initializing erigondb.toml with DEFAULT settings`) with `step_size=1562500`.
4. Verify the file exists immediately:
   ```bash
   cat <empty-path>/snapshots/erigondb.toml
   ```
   Expected: `step_size = 1562500` and `references_in_commitment_branches = true` (code defaults, since no downloader to provide network settings)
5. Kill the process and clean up the ephemeral datadir.

## Success Criteria

| Scenario | Condition | Expected step_size | Expected references_in_commitment_branches | File timing |
|----------|-----------|-------------------|--------------------------------------------|-------------|
| A (legacy) | `Creating erigondb.toml with LEGACY settings` log | 1,562,500 | `true` (written explicitly) | Written immediately on startup |
| B (fresh+downloader) | `erigondb.toml not found, using defaults` then `erigondb stepSize changed, propagating` | Code default 1,562,500 then network value (chain-dependent) | Resolves to `true` in memory until the downloader delivers the file; whatever the network publishes thereafter (a producer-published plain snapshot set ships `false`) | After downloader delivers it |
| C (fresh+no-downloader) | `Initializing erigondb.toml with DEFAULT settings (nodownloader)` log | 1,562,500 | `true` (written explicitly) | Written immediately on startup |

### references_in_commitment_branches per scenario

The `references_in_commitment_branches` field governs whether new commitment merges write referenced (short-key) `.kv` files. It resolves identically to `step_size` across the three scenarios — absent field normalizes to `true` in memory, and a downloaded `erigondb.toml` is never rewritten (it is synced snapshot metadata, so a producer's published `false` survives). The resolution logic is unit-tested in `db/state/erigondb_settings_test.go` (`TestResolveErigonDBSettings*`); these runtime scenarios confirm the same behavior end-to-end.

- **A (legacy):** the generated file contains `references_in_commitment_branches = true`.
- **B (fresh+downloader):** no file until the downloader delivers it; the in-memory resolved value is `true`. After delivery the field reflects the network's published value (a plain-snapshot producer ships `false`).
- **C (fresh+no-downloader):** the immediately-written file contains `references_in_commitment_branches = true`.

## Producer workflow: publishing plain (unreferenced) snapshots

`references_in_commitment_branches` governs only *new* commitment merges; reads are version-aware and stay correct regardless of the flag (a `v2.0` commitment `.kv` whose range ≥ threshold is referenced/short-key; a `v2.1` file is plain). To publish a snapshot set whose commitment files carry no plain-key references:

1. In the producer datadir's `snapshots/erigondb.toml`, set `references_in_commitment_branches = false` **before** running merges.
2. Run the node / merge so commitment merges produce plain `v2.1` `.kv` files. Existing `v2.0` referenced files convert to plain lazily as they are merged — no upfront migration.
3. Ship the datadir's `erigondb.toml` with the snapshot set. Consumers read the file and inherit `false`; a downloaded `erigondb.toml` is never rewritten, so the producer's `false` survives.

This supersedes the build-time const flip (`awskii/36exp`): no binary divergence and no CLI flag. Default nodes (field absent → `true`) keep writing byte-compatible `v2.0` referenced files with no behavior change.

## Cleanup

After all scenarios complete, ensure all ephemeral datadirs and processes are cleaned up. Use `/erigon-ephemeral` Step 5 (cleanup) for each instance, or Step 6 (leftover detection) to find any stragglers.
