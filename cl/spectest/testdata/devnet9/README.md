# Devnet 9 parent-payload vectors

These two complete mainnet test cases are copied unchanged from the official
[consensus-specs v1.7.0-beta.0 mainnet release](https://github.com/ethereum/consensus-specs/releases/download/v1.7.0-beta.0/mainnet.tar.gz).
The archive SHA-256 is `0ef9c069293e2171dd75c5593faf7b97e32b9bcdce9285e870959938747c0774`.

[consensus-specs #5554](https://github.com/ethereum/consensus-specs/pull/5554),
required by devnet 9, makes parent-payload processing use the latest block header's
slot. The alpha.14 setup of these two payment tests changes only the bid slot;
the corrected setup also changes the header before advancing slots. Complete
vectors are needed because that advance also changes historical state/block roots.

The suite reads these two cases through `devnet9FixtureFS`. Every other case
continues to use the alpha.14 release pinned in `test-fixtures.json`. This is not
a beta.0 protocol upgrade. No expected state is generated or modified by Erigon.
The separate transition regressions retain deliberately unequal header/bid slots.

## File SHA-256

```
d817ee50009c0ce0c8f6da6433eb530dec4fe6ca89959f7223c38cc4430be3bb  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__older_than_previous_epoch/block.ssz_snappy
e9553ed5a02e0d17467f38900dccadbc8dc97de52a102e416d405770c7e2a4f2  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__older_than_previous_epoch/manifest.yaml
c6fb2a8e56019538de23482d90f7a5ede44506f9d6e8d9b7a56362fef3fcc1af  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__older_than_previous_epoch/post.ssz_snappy
635f4f796b10f922d4c75d253e7d504e2160a0833ff9197e899e848f16eeedc1  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__older_than_previous_epoch/pre.ssz_snappy
10cd9e428d13c5773aeef9e8b66b21c2379909f12f7a6228a956fa9e4eb7d443  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__settle_previous_epoch/block.ssz_snappy
aef827e309a34679d20c2759af36b2b86e99b129a71f0b494fc2182d0fa6fd4e  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__settle_previous_epoch/manifest.yaml
a60803165d83a69953e60638e46a396efc3dba719bb693169429b0854cf9124e  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__settle_previous_epoch/post.ssz_snappy
2bc4f1115fc55ef849923bc16a7c1a2f8fd9380e55fddabfef33f2dad68363d8  mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__settle_previous_epoch/pre.ssz_snappy
```
