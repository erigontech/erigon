# EEZ test data

Vectors and recordings consumed by the `eez/...` tests. Every expected value was produced outside Erigon: by the EEZ Solidity contracts, by the reference client, or read back from a chain. Files are vendored unchanged unless stated.

## vectors/

| File | Origin | Produced by |
|---|---|---|
| `rolling_hash_vectors.json` | `eez-association/eez-rollup0` at `a270a06a92131d1987d49903a4174eef04ce959c`, `crates/eez-protocol/tests/fixtures/` | `contracts/test/RollingHashVectors.t.sol` against `eez-core-protocol` `855fe0602484750861b4c31502a1a91355d33ed6` |
| `public_inputs_hash_vectors.json` | same | `contracts/test/PublicInputsHashVectors.t.sol` against the same revision |

Both files carry `schemaVersion`, `protocolCommit` and `solidityOracle`; the loaders in `eez/internal/testfixtures` reject any other values. The inputs of the rolling-hash vectors are fixed by the Solidity oracle, not by the file: rollup updates `[(1, 0x…11), (2^64-1, 0x…22)]`, proxy entry hash `0x…33`, call hash `0x…44`, return data `0xaabbcc`, static results `(true, 0xaabbcc)` then `(false, 0xdeadbeef)`.

License: `MIT OR Apache-2.0` (eez-rollup0).

## windows/

Recorded L2 block windows with their execution witnesses, from `eez-association/eez-rollup0` at `a270a06a92131d1987d49903a4174eef04ce959c`, `crates/eez-prover-stateless/tests/fixtures/<name>/`. Each directory keeps the upstream `README.md` describing its recording. Blocks are consensus RLP (`debug_getRawBlock`), witnesses are reth `debug_executionWitness` results, `oracle.json` and `blocks.json` hold values read back from the chain or the reference signer.

| Window | L2 chain id | Content |
|---|---|---|
| `stateless-block-13` | 1 | one empty block; expected hash and state root are asserted by the upstream stateless tests |
| `stateless-checkpoint-2175` | 1 | one block with three transactions and per-transaction state roots; predates the native system transaction |
| `nonzero-outbound-630` | 1 | blocks 626 to 630 settled by one batch; the five-field `CrossChainCallExecuted` event makes it a rejection vector; `postbatch.json` uses a pre-855fe06 layout |
| `captured-devnet-window-84` | 6290 | blocks 79 to 84 settled by an anchor-only batch on a Kurtosis devnet; `postbatch.hex` is in the 855fe06 layout |

License: `MIT OR Apache-2.0` (eez-rollup0).

## chiado/

`anchor-23507949.json` is a settlement mined on the eez.dev Chiado testnet (L1 Gnosis Chiado, chain id 10200; L2 chain id 699069), read from `https://eez.dev/rpc/l1` on 2026-10-14. The file is curated, not a raw RPC dump:

- `transaction`: the settling transaction from `eth_getTransactionByBlockHashAndIndex`. It is a type-3 transaction carrying one blob; the blob is not recorded.
- `logs` and `receiptStatus`: from `eth_getTransactionReceipt`.
- `stateAtBlock`: `eth_call` results at block 23507949 for `signer()` on the proof system, `verificationKey(address)` and `threshold()` on the rollup manager, and `rollups(1)` on the registry.

The loader test rebuilds the transaction from its fields and requires the mined hash and the recovered sender.

`rollup-created-23413747.json` is the `RollupCreated` log registering rollup 1, from `eth_getLogs` on `https://rpc.chiadochain.net` on 2026-10-14; `https://gnosis-chiado-rpc.publicnode.com` returned the same log. `https://eez.dev/rpc/l1` no longer serves that range.

`header-23507949.rlp.hex` is the consensus RLP of the header of the settling block, rebuilt with Erigon's `types.Header` from `eth_getBlockByNumber` on `https://rpc.chiadochain.net` on 2026-10-14 and checked to hash to `l1BlockHash`; `https://gnosis-chiado-rpc.publicnode.com` returned the same header.

`blob-23507949-0.hex` is the blob the settlement carries, from `/eth/v1/beacon/blob_sidecars/25306518` on `https://rpc-gbc.chiadochain.net` on 2026-10-14. The file holds its first two field elements; the other 4094 are zero. The loader test recomputes the KZG commitment and requires the transaction's versioned hash.

## SHA-256

| File | SHA-256 |
|---|---|
| `chiado/anchor-23507949.json` | `9253ff31da86cbb86177d4058583a72223a5a7da162f6bd2d6d0187cd73a939d` |
| `chiado/blob-23507949-0.hex` | `384eaae16ad11854a0b1b83c2ff67542b7903b86ecd1ee189444699242b76f40` |
| `chiado/header-23507949.rlp.hex` | `b6c2ffc4aea6c879093cd6bf111dddf24807997d1b1b6bda485953950b22d373` |
| `chiado/rollup-created-23413747.json` | `986dbf338aa6ed9b8bae098578d5421665aa298caddc72095ce7aa0cca2a953d` |
| `vectors/public_inputs_hash_vectors.json` | `8a787a8e307089a5be0ec4a56d8bd0e1ef4d8862eb58bb6b5086347ff2745472` |
| `vectors/rolling_hash_vectors.json` | `bc37e3a369c91dde689c586899a4f2cc49da50770032f24dfa5c313f5368b88d` |
| `windows/captured-devnet-window-84/README.md` | `d356d9d06c28ad34f07d92a32e6931633ef6ef802fdec82e9f45a22bebb1ae6e` |
| `windows/captured-devnet-window-84/block-79.rlp.hex` | `a2f068c120cc0646ecd58b0febfaecdd0c1fbcefa52aaf5a3dd46ff1332879e3` |
| `windows/captured-devnet-window-84/block-80.rlp.hex` | `53ec098c1c770f05d6d373dc80fc4bf22083d607d0b3f6d13aafce5ac847eaa2` |
| `windows/captured-devnet-window-84/block-81.rlp.hex` | `6e82bfa5ebe75b2b23ead165d867acc729145874c288215b1e2c5324202c3723` |
| `windows/captured-devnet-window-84/block-82.rlp.hex` | `3bac34f3d7ce0b5f41128731da95209018c350ac56f56e4b95ff6f2097e2f5e2` |
| `windows/captured-devnet-window-84/block-83.rlp.hex` | `982efd014dcbd8c64f3cc78851f02b31f246812a9c3816ca6b95cb04ff66c2b6` |
| `windows/captured-devnet-window-84/block-84.rlp.hex` | `c3dfec8b3d0cccd5dbcb14677aabc96d211acbc33bfc4ba7b52a4e08de573397` |
| `windows/captured-devnet-window-84/blocks.json` | `9a664e98af8cdcce684fa267d5e0040dcfa94771aacafdb257926872d9c35e5c` |
| `windows/captured-devnet-window-84/chain-config.json` | `b087a71ea36c29953f7391c1cc085562b0eef4bba600e8b9ba5ac8cfd439be73` |
| `windows/captured-devnet-window-84/oracle.json` | `367962245f1a411c4e5c8ba8d94854c804d95875ce0a76a150197a5c4d470757` |
| `windows/captured-devnet-window-84/postbatch.hex` | `635b4311578179566f3f12bf028073b2795fc730c0759aa196d4664320d3634d` |
| `windows/captured-devnet-window-84/witness-79.json` | `8426e2288256ce76fd7c399c96c44221a968723399066c75feecb79061184086` |
| `windows/captured-devnet-window-84/witness-80.json` | `94b6dbfdc8694ff66515c10f389226d8fc986214811435040da88fc3ece480d4` |
| `windows/captured-devnet-window-84/witness-81.json` | `70a927cb648623d04aed4b63709728747656082f344ac49215de55fd685fa0fb` |
| `windows/captured-devnet-window-84/witness-82.json` | `db03d1e04377923faabbdcf0ab5a11b384f1b01f1c6d2ebb8b60b6fa25e0a095` |
| `windows/captured-devnet-window-84/witness-83.json` | `263c1f58d821d5e8830b69023052038e02d5fa6490ae88f6c7a42879e6bb41a2` |
| `windows/captured-devnet-window-84/witness-84.json` | `91af945cd242354dd3f1555cde11ca46ae415b4a60f1497d71366521dd45a0d0` |
| `windows/nonzero-outbound-630/README.md` | `bdd5648ae51a51e53161c86d7adc4ce1b14cc7f66ae2a82e927b325d010724ae` |
| `windows/nonzero-outbound-630/block-626.rlp.hex` | `b5efdd1b01cd7ea7c6e6d3d3f00c08ad022da0abf727fd29fdcb965d16a04ce9` |
| `windows/nonzero-outbound-630/block-627.rlp.hex` | `9d40bc3db9f7f02e6daceca6298171a5b10c1a0c98219a61f515213d87ae2401` |
| `windows/nonzero-outbound-630/block-628.rlp.hex` | `327edc4cafc094b7833c4b9be5e7c78c3e57629493912b0667b917254b3b7478` |
| `windows/nonzero-outbound-630/block-629.rlp.hex` | `6df2ac8f35d73ac40d2c86760b3e3998cd850eee40e6adb1b936d0446bcac895` |
| `windows/nonzero-outbound-630/block-630.rlp.hex` | `eff2ac965cceac48220e942248c6b2fbbd30a2c4ba47c14075858228ccd32d48` |
| `windows/nonzero-outbound-630/chain-config.json` | `324ea9532f8d6fe23f47d2aad7fb7063e97e8d2b2e6442b9f2ea1f067a74010a` |
| `windows/nonzero-outbound-630/oracle.json` | `93225bdd40bfd9f95c27c5ae7f4e699a785e5554652c8ccd0c11dfea7284a00b` |
| `windows/nonzero-outbound-630/postbatch.json` | `15988b7c9b9b4318908a77001ddbbc3b849fc01ae0a3dd89efb9d39294076081` |
| `windows/nonzero-outbound-630/witness-626.json` | `ae9fe2da966793ebf17c8fa7f100ebe914aeaafb75c313c5774b3d04ecea47db` |
| `windows/nonzero-outbound-630/witness-627.json` | `250d732f3b4473c04db60a9ded52940b7d63c85d9cab1e141f3af734db87e34f` |
| `windows/nonzero-outbound-630/witness-628.json` | `a2500c5eec5eedf8b1249e4562b6cc16a286973b0f1abda96fae07340831b57a` |
| `windows/nonzero-outbound-630/witness-629.json` | `0ded2d0ab5a2d613b8fee458912c62eaea402fab619a5bdbce10c767d7795464` |
| `windows/nonzero-outbound-630/witness-630.json` | `036529a75aba9d1d5bb12765ec8f023ae00d7008014cc6908f59ef6372dfaddf` |
| `windows/stateless-block-13/README.md` | `a8ed743e4d9ae3ebd7fcef0e77cfdb8879b3211b32c2683847a5af885cc44174` |
| `windows/stateless-block-13/block-13.rlp` | `453bed17bc61adf760335855eb93c727049fa63143d426c466e3bbf45adc31d7` |
| `windows/stateless-block-13/chain-config.json` | `324ea9532f8d6fe23f47d2aad7fb7063e97e8d2b2e6442b9f2ea1f067a74010a` |
| `windows/stateless-block-13/witness-13.json` | `5dc43bf664d222f333e72e211271adca779eee546a3e53c43af2f54ea884c9e1` |
| `windows/stateless-checkpoint-2175/README.md` | `73b9899c8757dd2a718c0171e12a1633f7653eefcff2fbbd420ecdb691eacd96` |
| `windows/stateless-checkpoint-2175/checkpoint-block-2175.rlp.hex` | `7fc805299f6f1214d14ebe597e37eb01917a511998f071f2c76569399ae36abe` |
| `windows/stateless-checkpoint-2175/checkpoint-chain-config.json` | `324ea9532f8d6fe23f47d2aad7fb7063e97e8d2b2e6442b9f2ea1f067a74010a` |
| `windows/stateless-checkpoint-2175/checkpoint-oracle-2175.json` | `f6fcf9fb12eab0eb0bd32ef676c7218fe0e697c36a7e704aad6deba423fbd45c` |
| `windows/stateless-checkpoint-2175/checkpoint-witness-2175.json` | `64f0017d4316ab2de04a45da27ebc819c0b2483b4acc82e40605ce7787dee533` |
