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
`VariantCommitmentV3` plus `VariantBinPatriciaTrie`. When it opens a hex+bin datadir,
`reconcileTrieVariant` in `db/state/erigondb_settings.go` enables v3-hex for the hex domain and
logs that choice, even when the process was started without the v3 flag. An explicit HPH
configuration is rejected for a dual datadir, and binary-only startup with v3 enabled remains
refused.

## Feed and execution

The execution binary arm consumes `commitment.PBinFeed`, assembled by `BinFeedFromState` in
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
removed `0x10` format are refused at datadir open with an instruction to run `commitment convert-pbt`.
The converter validates only its output files through the files-only aggregator.

The binary trie stores rows and fixed bucket-root records. `docs/pbin-encoding.md` describes the
key derivation, record bytes, root forms and fold. `integration commitment rebuild` remains the
hex rebuild and has no binary target. Binary migration uses `integration commitment convert-pbt`
and `integration commitment attach-pbt`; the operator procedure is in `docs/pbt-migration.md`.
`commitment convert-pbt` reads the files-only leaf stream from `db/state/pbt_leaf_stream.go`,
feeds the bounded range writer in `db/state/pbt_range_writer.go`, and writes a fresh binary target.
A hex datadir is converted with `commitment convert-pbt` into a fresh output datadir.
Use `--keep-hex` for a hex+bin output; omit it for a post-fork bin-only output. The converter
records the execution-committed bin root and conversion point, and validates the output before
writing its settings.

`erigondb.toml` records `trie_variant`, `trie_hash`, and per-domain freeze state. Changing the
embedding or selected binary hash suite requires running `commitment convert-pbt` into a fresh
binary datadir.

## Freeze and RPC

`integration commitment freeze` registers the v3 setting and reads each domain's state with the
variant-specific state key. It freezes the selected hex domain at its recorded transaction and
leaves the binary domain canonical when the schedule has flipped.

`debug_shadowStateRoot` reports the non-canonical root stored by
`rawdb.WriteShadowStateRoot`. `debug_executionWitness` refuses binary blocks with
`ErrBinCommitmentUnsupported`; `eth_getProof` and `eth_getWitness` have the same refusal for
binary blocks. With v3-hex, `debug_executionWitness` also refuses hex blocks because the v3 hex
trie does not provide the HPH witness implementation.
