# Dual commitment

Erigon's migration mode keeps a v3 hex trie and a v3 binary trie current for the same executed
blocks. The hex trie is in `kv.CommitmentDomain`; the binary trie is in `kv.CommitmentBinDomain`.
The binary-only mode uses the existing commitment domain with the binary algorithm.

## Domains and roles

`canonicalCommitmentDomain` in `execution/stagedsync/committer.go` selects the block's canonical
domain from the chain schedule. It does not use the process-global variant flag. Before
`binaryTrieTime`, hex is canonical and binary is shadow. At and after that time, binary is
canonical and hex is shadow. A frozen hex domain remains readable but rejects writes and folds
below its recorded freeze point.

`NewSharedDomains` in `db/state/execctx/domain_shared.go` constructs the dual pair as
`VariantCommitmentV3` plus `VariantBinPatriciaTrie`. An explicit HPH configuration is rejected
for a dual datadir. `reconcileTrieVariant` in `db/state/erigondb_settings.go` also refuses a
hex+bin datadir without the v3 hex setting and refuses binary-only startup with v3 enabled.

## Feed and execution

The binary arm consumes `commitment.PBinFeed`, assembled by `BinFeedFromState` in
`execution/commitment/commitmentdb/pbin_feed.go`. The feed contains final account fields, final
code bytes when the address is in `codeKeys`, final changed slots, code-write and wipe status.
`TranslateFeed` in `execution/commitment/v3/pbt/feed.go` derives the sorted, unique operation list
for the row trie. Chunks are content-addressed, deduplicated per batch and retained after insertion.

There are three input sources:

- SharedDomains records dirty plain keys and code-domain touches, then clears the code-key set
  after it consumes or discards the touched keys.
- calcState records dirty plain keys, code writes and execution-derived wipes, including CREATE
  storage wipes, and reads through `asOfStateReader`.
- BAL reads account and code changes from `LoadFromBALUpTo`. BAL has no create marker, so its
  `wiped` set stays empty. CREATE over storage is a known BAL gap in both arms; on other blocks
  the BAL roots equal the non-BAL path.

The two arms use the same block feed and run concurrently. The binary arm uses
`ProcessParallel` in `execution/commitment/v3/pbt`; worker contexts are installed through
`SetTrieContextFactory`. Hex shadow writes use `BufferedPatriciaContext` and are replayed only
after both arms succeed. A shadow fold or replay failure stops that domain through
`recordStoppedCommitmentDomains` without invalidating the block. The step-boundary path resets
its block flags once after both arms complete.

## State and files

The binary state blob uses marker `0xb1` and row format `0x20`, validated by
`PBinValidateRowStateFormat` in `execution/commitment/pbin_state_format.go`. Legacy flags and the
removed `0x10` format are refused at datadir open with an instruction to rebuild the binary
commitment domain. A staged rebuild validates only its output files through the `newTemporalDB`
aggregator option.

The binary trie stores rows and fixed bucket-root records. `docs/pbin-encoding.md` describes the
key derivation, record bytes, root forms and fold. Rebuilds in `RebuildCommitmentFiles` in
`db/state/squeeze.go` translate the plain state through `BinFeedFromState`, sort by tree key for
each source range, cut bounded batches, and resume after the recorded completed key. Pending
commitment writes form the read overlay while a range is processed.

`erigondb.toml` records `trie_variant`, `trie_hash`, and per-domain freeze state. Changing the
embedding or selected binary hash suite requires rebuilding the binary datadir from genesis.

## Freeze and RPC

`integration commitment freeze` registers the v3 setting and reads each domain's state with the
variant-specific state key. It freezes the selected hex domain at its recorded transaction and
leaves the binary domain canonical when the schedule has flipped.

`debug_shadowStateRoot` reports the non-canonical root stored by
`rawdb.WriteShadowStateRoot`. `debug_executionWitness` refuses binary blocks with
`ErrBinCommitmentUnsupported`; `eth_getProof` and `eth_getWitness` have the same refusal for
binary blocks. Hex blocks before activation continue to serve the hex witness path.
