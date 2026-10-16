# Captured incompatible outbound fixture

This fixture is the five-block settlement window `[626..630]` exported from
`fixtures/scenarios/fresh-chain-canonical`. The source recording was captured
from `chiado-fresh-2026-07-13-registry-0x03858ac` at
`2026-07-13T13:32:39Z`; none of the block, witness, or `PostBatch` execution
data was synthesized for this test.

Block 630 contains five successful transactions in canonical Sync order:

1. a system `loadExecutionTable` transaction;
2. its user outbound transaction, carrying `10_000_000_000_000` wei;
3. a second system load;
4. its zero-value user outbound transaction; and
5. one inbound system transaction.

The recording's L2 manager emitted the five-field base
`CrossChainCallExecuted` event. The current `EEZL2` contract emits a distinct
six-field event containing `uint64 callGas`, with a different topic. The
fixture is retained to verify that stateless replay does not reinterpret the
incompatible event or permit an attestation by inventing a gas value.

The block, witness, and `PostBatch` artifacts were exported from
`fixtures/scenarios/fresh-chain-canonical`. `chain-config.json` was copied
separately from the configuration used by the captured deployment; its original
source filename and independently recorded SHA-256 remain in `oracle.json`.

The source recording has no directive selecting block 630, so this fixture is
derived from the `PostBatch` DA span of five blocks and loads those five
exported events directly; it is not claimed to be replayable with
`replay --window 630`. Blocks 626 through 629 are empty but remain necessary:
their real witnesses establish the batch anchor and the complete state-root
chain consumed by settlement.

`block-*.rlp.hex` stores the raw RLP as reviewable hex. The
`source_artifact_sha256` RLP values in `oracle.json` are over the decoded bytes;
the witness and `PostBatch` values cover exact exporter output, while the chain
configuration value covers its separately documented source. Checked-in JSON
files normalize one trailing newline, without changing their parsed contents,
so `fixture_file_sha256` separately covers the repository representations. The
empty L1 block hash and on-chain batch `blockNumber == 0` are intentional
properties of this recording's timeless profile.

This is a rejection vector, not a successful outbound conformance vector.

The fixture contains no credentials. These artifacts are contributed under the
repository's dual `MIT OR Apache-2.0` license.
