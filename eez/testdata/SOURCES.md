# EEZ test data

Vectors and recordings consumed by the `eez/...` tests. Every expected value was produced outside Erigon: by the EEZ Solidity contracts, by the reference client, or read back from a chain. Files are vendored unchanged unless stated.

## vectors/

| File | Origin | Produced by |
|---|---|---|
| `rolling_hash_vectors.json` | `eez-association/eez-rollup0` at `a270a06a92131d1987d49903a4174eef04ce959c`, `crates/eez-protocol/tests/fixtures/` | `contracts/test/RollingHashVectors.t.sol` against `eez-core-protocol` `855fe0602484750861b4c31502a1a91355d33ed6` |
| `public_inputs_hash_vectors.json` | same | `contracts/test/PublicInputsHashVectors.t.sol` against the same revision |

Both files carry `schemaVersion`, `protocolCommit` and `solidityOracle`; the loaders in `eez/internal/testfixtures` reject any other values. The inputs of the rolling-hash vectors are fixed by the Solidity oracle, not by the file: rollup updates `[(1, 0x…11), (2^64-1, 0x…22)]`, proxy entry hash `0x…33`, call hash `0x…44`, return data `0xaabbcc`, static results `(true, 0xaabbcc)` then `(false, 0xdeadbeef)`.

License: `MIT OR Apache-2.0` (eez-rollup0).

## SHA-256

| File | SHA-256 |
|---|---|
| `vectors/public_inputs_hash_vectors.json` | `8a787a8e307089a5be0ec4a56d8bd0e1ef4d8862eb58bb6b5086347ff2745472` |
| `vectors/rolling_hash_vectors.json` | `bc37e3a369c91dde689c586899a4f2cc49da50770032f24dfa5c313f5368b88d` |
