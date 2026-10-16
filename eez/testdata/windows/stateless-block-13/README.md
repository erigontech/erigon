# Protocol-neutral Stateless fixture

This directory contains one block, its execution witness, and the chain rules
needed by the Stateless adapter tests. The tests use it to exercise block RLP
decoding, witness-backed execution, state-root validation, and adapter error
handling; none of those checks depends on the EEZ settlement ABI.

`block-13.rlp` contains the decoded binary RLP bytes directly, not hexadecimal
text. Its SHA-256 is
`453bed17bc61adf760335855eb93c727049fa63143d426c466e3bbf45adc31d7`.
The witness and chain configuration hashes are, respectively,
`5dc43bf664d222f333e72e211271adca779eee546a3e53c43af2f54ea884c9e1` and
`324ea9532f8d6fe23f47d2aad7fb7063e97e8d2b2e6442b9f2ea1f067a74010a`.
