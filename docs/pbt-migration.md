# PBT migration

This procedure moves a stopped Erigon datadir from a v3 hex commitment to the
EIP-8297 binary commitment trie (PBT). The producer and the node must use the
same network state salt and hash suite. Commands detect v3 commitment files;
`COMMITMENT_V3=true` may be set when a wrapper does not perform that detection
before opening the datadir.

## Conversion and publication

Run conversion against a source that is not being written:

```sh
integration commitment convert-pbt \
  --datadir=<source-datadir> --chain=<chain> \
  --output.datadir=<published-datadir> \
  --keep-hex \
  --experimental.bin-commitment.hash=<suite>
```

`--keep-hex` writes the binary files and retains the v3 hex files, producing a
hex+bin datadir. Without it, conversion writes a post-fork bin-only datadir.
The output contains files and settings, not the source chaindata. Publish the
completed output together with its `erigondb.toml`.

The conversion point is `(B, S)`. `S` is the latest commitment transaction
number in the source files, as read by `readPBinConversionPointFromFiles`; it
can be in the middle of a block. The converter reads the source files only;
database rows newer than the files are not part of the conversion. It refuses
a source whose latest file contains writes after `S`. Provenance stamps are
file-granular, so wait for the next step when the last file extends past `S`.

The producer and node must use the network's state salt and hash suite. Pass
`--experimental.bin-commitment.hash=<suite>` to both `convert-pbt` and
`attach-pbt`. The suite is a network choice; EIP-8297 does not make it final,
and its reference implementation uses BLAKE3. Attach refuses a suite mismatch
and a state-salt mismatch. Attach never writes the node's salts.

For a v3 hex datadir created without the current settings record, the complete
operator commands are:

```sh
COMMITMENT_V3=true integration commitment convert-pbt \
  --datadir=<source-datadir> --chain=<chain> --output.datadir=<published-datadir> --keep-hex \
  --experimental.bin-commitment.hash=<suite>

COMMITMENT_V3=true integration commitment attach-pbt \
  --datadir=<node-datadir> --chain=<chain> --from=<published-datadir> \
  --experimental.bin-commitment.hash=<suite>

COMMITMENT_V3=true erigon snapshots export-pbt \
  --datadir=<datadir> --chain=<chain> --out=<export-dir>

COMMITMENT_V3=true integration commitment import-pbt \
  --datadir=<datadir> --chain=<chain> --snapshot=<pbt-snapshot.bin> \
  --experimental.bin-commitment.hash=<suite>
```

The current commands detect v3 files themselves, so the environment variable
is an explicit setting rather than a requirement for this build.

The converter verifies the written rows, the reference root, and the
checkpoint before it writes the output settings. If any check fails, remove
the incomplete output and publish only a successful conversion.

## Attach

Stop the node, then attach the published files:

```sh
integration commitment attach-pbt \
  --datadir=<node-datadir> --chain=<chain> \
  --from=<published-datadir> \
  --experimental.bin-commitment.hash=<suite>
```

`attachPBT` checks the step size, ranges, hash suite, conversion point, state
salt, both commitment domains, and every file type that it replaces. For
accounts, storage and code, the published set must contain the `.kv`, `.bt`,
`.kvi` and `.kvei` files for the affected ranges. The converter publishes
commitment history and index files when the source has them; attach accepts
either set and adopts the published commitment files. It keeps the node's own
accounts, storage and code `.v`, `.ef`, `.vi` and `.efi` files through `S`,
removes files starting after `S`, and refuses a state-domain history or index
file that spans `S`, because it cannot be cut safely. It adopts the state and
commitment files through `S`, runs
`ResetExec`, and writes the conversion point and hex+bin settings. It does not
remove chaindata or block files. At a block-end conversion point before the
fork, attach writes the PBT root as the shadow root; after the fork it writes
the adopted hex root. If the post-fork hex root is unavailable, it writes no
shadow record.

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
block. To export at a selected block, stop execution with the integration
stage command, then run export:

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

`integration stage_exec --block=<B>` executes and commits block `B`. To recover
from a mid-block pin, run
`COMMITMENT_V3=true integration stage_exec --datadir=<datadir> --chain=<chain> --block=<next block end> --experimental.commitment-v3`,
then run
`COMMITMENT_V3=true erigon snapshots export-pbt --datadir=<datadir> --chain=<chain> --out=<export-dir>`.
Export has no block flag. `export-preimages` uses the same pin for a
preimage-only operation. Export does not require restoring every shadow domain;
a lagging or frozen shadow is not the pin.

## Import (test-only)

Import substitutes for conversion and attach in tests. Stop a v3 hex node at a
block end, export its snapshot, import that snapshot into the stopped node, and
continue in dual mode:

```sh
COMMITMENT_V3=true integration stage_exec \
  --datadir=<datadir> --chain=<chain> --block=<X> \
  --experimental.commitment-v3

COMMITMENT_V3=true erigon snapshots export-pbt \
  --datadir=<datadir> --chain=<chain> --out=<export-dir>

COMMITMENT_V3=true integration commitment import-pbt \
  --datadir=<datadir> --chain=<chain> --snapshot=<export-dir>/pbt-snapshot.bin \
  --experimental.bin-commitment.hash=<suite>

COMMITMENT_V3=true integration stage_exec \
  --datadir=<datadir> --chain=<chain> \
  --experimental.commitment-v3 --experimental.bin-commitment \
  --experimental.bin-commitment.hash=<suite>
```

The target must be a stopped, hex-only v3 datadir at `X`. The metadata must
match its chain, canonical block, last transaction, hex checkpoint, state root,
hash suite and snapshot digest. Import streams the artifact into staged
`commitment-bin` files, verifies the root and rows, then writes hex+bin settings
last. It changes no state-domain files and has no preimage or block-hash flag.
Any refusal leaves the datadir unchanged. A frozen block is read from block
snapshots through the block reader.

Import is a test substitute for conversion and attach. It checks the artifact
against its metadata and the rows it writes, but it does not compare every
artifact leaf with the node's prior state. An internally consistent artifact
whose leaves were changed is therefore outside its guarantees.

An unwind cannot cross the conversion point. `checkUnwindConversionPoint`
refuses a block or transaction target below the published point.

## Code entry points

The command entry points are `cmdCommitmentConvertPBT`, `cmdCommitmentAttachPBT`,
`cmdCommitmentImportPBT`, and the `exportPBTCommand` in `cmd/utils/app`.
The conversion leaf stream is `db/state.ForEachPBinLeaf`; the range writer is
`db/state.NewPBinRangeWriter`, and output checks use `db/state.VerifyPBinDomain`.
Commitment checkpoints use `SharedDomains.SeekCommitment` and
`commitmentdb.SeekCommitments`. The dual execution roles are implemented by
`canonicalCommitmentDomain`; the automatic stop uses
`stopHexShadowAtWindow` and `recordStoppedCommitmentDomains`.
