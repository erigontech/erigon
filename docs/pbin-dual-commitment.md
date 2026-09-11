# Dual commitment and binary-trie migration

Erigon can keep the hex Patricia trie and the EIP-8297 partitioned binary trie in lockstep while
changing which root is canonical. This is the migration mode for the devnet work described here;
it does not add snapshot publication, mid-window catch-up, or automatic finality-based freezing.

## Domains and roles

The implementation keeps two concerns separate:

- A commitment domain owns one trie algorithm for the lifetime of a datadir.
- The block timestamp determines whether that domain is canonical for that block.

`kv.CommitmentDomain` (`db/kv/tables.go`) is the existing commitment domain. In `hex+bin` mode,
`kv.CommitmentBinDomain` is the second domain and uses the `commitment-bin` tables and snapshot
filename prefix. The binary trie itself is implemented by `PBinPatriciaHashed`
(`execution/commitment/pbin_patricia_hashed.go`).

| Block | Hex domain | Binary domain |
|---|---|---|
| Before `binaryTrieTime` | Canonical and header-checked | Shadow, computed and recorded |
| At and after `binaryTrieTime` | Shadow, computed and recorded | Canonical and header-checked |
| After the hex freeze | Frozen and retained | Canonical and computed |

Both roots are computed from block 0 in `hex+bin` mode. The swap changes the header check and the
role of the roots, not the domains or their stored state. `Config.IsBinaryTrie` in
`execution/chain/chain_config.go` is the block-time predicate used by the committer and block-aware
RPC paths.

## Datadir modes

`trie_variant` in `erigondb.toml` is resolved by `ResolveErigonDBSettings` and
`reconcileTrieVariant` (`db/state/erigondb_settings.go`). It is fixed when the datadir is created.

| Mode | Chain configuration | Commitment storage |
|---|---|---|
| `hex` | No `binaryTrieTime` | Existing `kv.CommitmentDomain`, using the hex trie |
| `bin` | `binaryTrieTime` equals genesis time | Existing commitment domain, using the binary trie |
| `hex+bin` | `binaryTrieTime` is after genesis time | Hex in `kv.CommitmentDomain`; binary in `kv.CommitmentBinDomain` |

To initialize a new migration datadir, set `COMMITMENT_HEX_BIN=true` on the first invocation.
Without it, initialization refuses the post-genesis schedule and, unless `COMMITMENT_BIN` is set,
records no `trie_variant`, so a retry with the variable succeeds on the same datadir.
The genesis must schedule `amsterdamTime` no later than `binaryTrieTime`, and `binaryTrieTime`
must be after the genesis timestamp. For example, with a migration genesis at `./pbt-genesis.json`:

```sh
COMMITMENT_HEX_BIN=true ./build/bin/erigon init --datadir=./pbt-data ./pbt-genesis.json
./build/bin/erigon --datadir=./pbt-data
```

Initialization records `trie_variant = "hex+bin"` in `./pbt-data/snapshots/erigondb.toml`;
subsequent starts read the stored mode. Setting `COMMITMENT_BIN` alone selects binary-only storage
and does not enable a post-genesis migration. Select the mode before initializing the datadir;
changing an existing datadir's mode requires rebuilding it from genesis.

The binary hash suite is stored as `trie_hash`; it is meaningful only when the datadir contains a
binary trie. The domain schemas are defined in `db/state/statecfg/state_schema.go`. The binary domain
does not use references in commitment branches, history snapshots, or the hex branch cache.

The settings file also contains `frozen_at_txnum`, keyed by domain name. Changing a trie embedding
or hash suite is not an in-place migration: rebuild a binary datadir from genesis.

## Execution and reorganisation

`commitmentCalculator.computeDualFromUpdatesWithRole` (`execution/stagedsync/committer.go`) fans a
block's touched plain-key set into two folds. The hex arm uses the normal update mode; the binary arm
builds `ModeDirect` updates from the same keys and reads values from the state domains. Each arm has a
worker read transaction pinned to the parent view. Binary branch writes are held by
`BufferedPatriciaContext` (`execution/commitment/commitmentdb/buffered_context.go`) until both folds
join, then replayed on the calculator goroutine.

Dual mode uses this calculator for every block, including when parallel execution and BAL options
are disabled. The touched-key collector remains hex-owned across the swap. BAL compute-ahead reads
changed accounts, storage, and code from the BAL and unchanged values from the block's starting state.

The canonical arm checks the header root and reports `ErrWrongTrieRoot` on a mismatch. The shadow arm
records its root and does not invalidate the block if its fold fails; it is marked stopped for the
run, including later execution batches and context recreation. A shadow failure is therefore
observable without turning a migration comparison into a consensus failure.

Shadow roots are stored by `WriteShadowStateRoot` (`db/rawdb/accessors_shadow_root.go`) under
`dbutils.BlockBodyKey(number, hash)`. The block hash is part of the key, so competing blocks at one
height do not overwrite one another. The execute-stage and chain-pruning paths remove these records
with their block data.

Diffsets now carry a version and domain count in `serializeKeys` and `deserializeKeys`
(`db/state/changeset/state_changeset.go`). The reader accepts the old six-domain framing and leaves
new trailing domains empty when an older record is read. This lets an unwind across the flip restore
both domains without migration-specific rollback logic.

## Aggregation and files

The canonical domain is exposed by `Aggregator.CanonicalCommitmentDomain`
(`db/state/aggregator.go`) and is re-derived from the head block time when the aggregator opens. The
state minimax used for file building and merge alignment includes accounts, storage, code, and only
that canonical commitment domain through `kv.StateDomains` (`db/kv/tables.go`). A stopped, frozen, or
not-yet-built shadow domain therefore does not stall canonical file production.

Referenced hex branches retain a read view of their exact account and storage file ranges through
`aggregatorVisible` (`db/state/aggregator.go`). Those dependencies remain readable when newer merged
files replace the ranges used by ordinary state reads, including after the hex domain stops or freezes.

The binary domain has no hex branch-cache trunk and no inter-domain dependency. Its files use names
such as `v1.0-commitment-bin.0-1024.kv`; `ParseFileName` and the snapshot command name tables handle
the hyphenated type through `db/snaptype/files.go`.

## Freezing the hex domain

Freezing is explicit and operator-triggered:

```text
./build/bin/integration commitment freeze --datadir=./pbt-data --trie hex
```

The command requires aligned commitment domains after activation. It calls `Aggregator.FreezeDomain`
(`db/state/aggregator.go`) at the saved commitment transaction number and persists the result in
`erigondb.toml`. After the freeze, the hex domain's files remain
available but the committer does not fold it, `DomainPut` rejects writes to it, merges skip it, and an
unwind below the freeze point is rejected. The frozen state survives restart. There is no automatic
finality trigger in this migration implementation.

## Debug API

The private debug API in `rpc/jsonrpc/debug_api.go` exposes two migration observations:

- `debug_shadowStateRoot(blockHash)` returns the non-canonical root recorded for that block, or
  `null` when no shadow root is available.
- `debug_migrationProgress` returns `mode`, `activationTime`, `flipped`, and `shadowStopped`.
  `flipped` is derived from the current head timestamp and `binaryTrieTime`; it is not persisted.
  `shadowStopped` reads the stop marker of the current shadow domain, so a frozen hex domain or a
  head that execution has not reached yet does not report a stopped shadow.

These methods make it possible to compare the shadow window with another client and to distinguish a
stopped shadow fold from a canonical execution failure.

A shadow fold error stops that domain. Execution records the stop with
`WriteCommitmentDomainStopped` (`db/rawdb/accessors_shadow_root.go`) in the transaction that commits
the block; the aggregator restores it when it opens, so a restart skips the stopped domain instead of
refusing a torn datadir. `ResetExec` (`execution/stagedsync/rawdbreset/reset_stages.go`) clears it.

`debug_executionWitness` seeks only the selected trie's parent state. At the activation block, its
binary parent root comes from the parent's shadow-root record because the parent header still carries
the hex root. `buildWitnessResult` (`rpc/jsonrpc/debug_execution_witness.go`) reports an error if that
shadow-root record is unavailable.
The explicit commitment-history option enables history and history snapshots for both domains through
`EnableHistoricalCommitment` (`db/state/statecfg/state_schema.go`).

`eth_simulateV1` keeps both live tries current across simulated blocks and selects each returned
state root by the simulated timestamp. Frozen hex is excluded from post-activation folds.
Historical replay uses the selected domain in temporary storage with independent freeze settings;
`ComputeCustomCommitmentFromStateHistory` (`rpc/rpchelper/commitment.go`) leaves the source freeze
marker unchanged.

`eth_getProof` and `eth_getWitness` fold only the hex trie. On a `hex+bin` datadir they return
`ErrBinCommitmentUnsupported` for a block at or after `binaryTrieTime`.

## Compatibility

A datadir without `binaryTrieTime` remains hex-only: it registers the existing commitment domain,
keeps the existing genesis header, and can read legacy diffsets. A binary-only datadir remains a
single-domain binary trie. Only `hex+bin` adds the second commitment domain and the dual-fold path.

The binary tree's key, cell, node-hash, and witness encodings are documented in
`docs/pbin-encoding.md`; the domain split selects where those records live and which root is placed
in the header, but does not change those encodings.
