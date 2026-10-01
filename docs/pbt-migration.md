# PBT migration

This procedure moves a stopped Erigon datadir from a v3 hex commitment to the
EIP-8297 binary commitment trie (PBT). The producer and the node must use the
same network state salt.

## Conversion and publication

Run conversion against a source that is not being written:

```sh
integration commitment convert-pbt \
  --datadir=<source-datadir> \
  --output.datadir=<published-datadir> \
  --keep-hex
```

`--keep-hex` writes the binary files and retains the v3 hex files, producing a
hex+bin datadir. Without it, conversion writes a post-fork bin-only datadir.
The output contains files and settings, not the source chaindata. Publish the
completed output together with its `erigondb.toml`.

The conversion point is `(B, S)`. `S` is the block's last transaction number,
as returned by `SeekCommitment`. The converter reads the source files only;
database rows newer than the files are not part of the conversion. It refuses
a source whose latest file contains writes after `S`. Provenance stamps are
file-granular, so wait for the next step when the last file extends past `S`.

The producer must use the network's `salt-state.txt`. A different state salt
invalidates the accessors, so attach refuses it. Attach never writes the
node's salts. `salt-blocks.txt` does not affect this check.

The converter verifies the written rows, the reference root, and the
checkpoint before it writes the output settings. If any check fails, remove
the incomplete output and publish only a successful conversion.

## Attach

Stop the node, then attach the published files:

```sh
integration commitment attach-pbt \
  --datadir=<node-datadir> \
  --from=<published-datadir>
```

`attachPBT` checks the step size, ranges, hash suite, conversion point, state
salt, both commitment domains, and every file type that it replaces. The
published set must contain the `.kv`, `.v`, `.ef`, and accessor files for the
affected ranges. It adopts the files through `S`, removes the node's state and
commitment files past `S`, runs `ResetExec`, and writes the conversion point
and hex+bin settings. It does not remove chaindata or block files.

A refused attach leaves the node unchanged. An interrupted attach leaves an
in-progress marker and the node refuses to start until the same
`attach-pbt --from` operation is rerun. Recovery removes the marker last.

## Dual window and automatic hex stop

During the dual window, execution keeps both commitment domains current. The
canonical domain is selected by the chain schedule: hex is canonical before
the binary fork and bin is canonical after it. The other domain is the
shadow. The role comes from `canonicalCommitmentDomain`, not from a process
global variant flag.

The hex shadow folds through the activation block plus the configured
`MaxReorgDepth` window. It stops on the next block through
`stopHexShadowAtWindow` and `recordStoppedCommitmentDomains`, in the same
transaction as that block's commitment. The stop survives restart, and an
unwind below the activation window is refused.

## Export

Export uses the block and transaction pin of the domain canonical at that
block:

```sh
erigon snapshots export-pbt \
  --datadir=<datadir> \
  --out=<export-dir>
```

The command writes `pbt-snapshot.bin`, the framed preimage file, and a meta
JSON file. The meta records the chain, block and hash, transaction number,
hash suite, state root, PBT root, section counts, both digests, and the
finalized flag. The artifact and preimage files are read back with the strict
readers and exact-set join before the meta is published. The streamed root
must match the bin root when the pinned datadir has one.

`export-preimages` uses the same pin and is the operator path when only the
preimage file is required. Export does not require restoring every shadow
domain; a lagging or frozen shadow is not the pin.

## Import (test-only)

The import command is for test and bootstrap use, not a live-node migration:

```sh
integration commitment import-pbt \
  --datadir=<datadir> \
  --snapshot=<pbt-snapshot.bin> \
  --preimages=<framed.bin> \
  --block=<canonical-block-hash>
```

The block must be canonical locally and at or after the binary fork. Import
validates the artifact, the exact preimage set, code reconstruction, and both
roots before `ResetExec`. It refuses a frozen target and a target whose files
run past the artifact transaction number before changing the datadir. After
validation it writes the state and bin commitment at the pinned transaction,
then flushes and checks the folded root.

## Code entry points

The command entry points are `cmdCommitmentConvertPBT`, `cmdCommitmentAttachPBT`,
`cmdCommitmentImportPBT`, and the `exportPBTCommand` in `cmd/utils/app`.
The conversion leaf stream is `db/state.ForEachPBinLeaf`; the range writer is
`db/state.NewPBinRangeWriter`, and output checks use `db/state.VerifyPBinDomain`.
Commitment checkpoints use `SharedDomains.SeekCommitment` and
`commitmentdb.SeekCommitments`. The dual execution roles are implemented by
`canonicalCommitmentDomain`; the automatic stop uses
`stopHexShadowAtWindow` and `recordStoppedCommitmentDomains`.
