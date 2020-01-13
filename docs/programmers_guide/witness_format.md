# Block Witness Format

Each block witness consists of a header followed by a list of operands.

There is no length of witness specified anywhere, the code expects to just reach `EOF`.

Witness: `[HEADER, OP1, OP2, ..., OPn-1, OPn, EOF]`


## Header:

Header contains only 1 byte with the current version `VERSION`, should be 1.

## Operands

Each operand starts with an opcode (see [`witness_operands.go`](../../trie/witness_operands.go) for exact values).

Then it might contain some data.

* `OpEmptyRoot` -- no data contained, encoded as `[ 0x06 ]`
* `OpHash` -- data constant-length hash `[32]byte`, encoded as `[ 0x03 hash_byte_1 ... hash_byte_32 ]`
* `OpBranch` -- data uint32, mask of existing children (e.g 0x03 will mean that children 0, 1 and 2 are present and the other ones are not); unit32 is encoded as CBOR `[ 0x02 CBOR(mask)...]`
* `OpCode` -- data is account code as a list of bytes encoded using CBOR; encoded as `[ 0x04 CBOR(code)... ]`
* `OpExtension` -- data is key encoded as a list of bytes, encoded using CBOR; encoded as `[ 0x01 CBOR(key)... ]`
* `OpLeaf` -- data is key and value, lists of bytes, encoded usng CBOR; the whole operand is `[ 0x00 CBOR(key)... CBOR(value)... ]`
* `OpAccountLeaf` -- data is account fields encoded as CBOR, the last fields is a mask determining if the account has a storage or any code associated with it; the whole operand is `[ 0x05 CBOR(key|[]byte)... flags /CBOR(nonce|uint64).../ /CBOR(balance|[]byte).../ ]`
  
    * *flags* - the lowest byte represents code, the 2nd lowest is storage, then nonce, balance. bits: `0b0000<balance><nonce><storage><code>`. If a flag for `balance` or `nonce` aren't set, the corresponding field is not encoded!


