# Block Witness Format

Each block witness consists of a header followed by a list of operands.

There is no length of witness specified anywhere, the code expects to just reach `EOF`.

Witness: `[HEADER, OP1, OP2, ..., OPn-1, OPn, EOF]`


## Header:

Header contains only 1 byte with the current version `VERSION`, should be 1.

## Operands

Each operand starts with an opcode (see [`witness_operands.go`](../../trie/witness_operands.go) for exact values).

Then it might contain some data.

### `OpEmptyRoot` 

format: `OpEmptyRoot` 

encoded as `[ 0x06 ]`

### `OpHash` 

format: `OpHash hash:[32]byte`

encoded as `[ 0x03 hash_byte_1 ... hash_byte_32 ]`

### `OpBranch`

format: `OpBranch mask:uint32`

*mask* defines which children are present 
(e.g. `0000000000001011` means that children 0, 1 and 3 are present and the other ones are not)

encoded as `[ 0x02 CBOR(mask)...]`

### `OpCode`

format: `OpCode code:[]byte`

encoded as `[ 0x04 CBOR(code)... ]`

### `OpExtension`
format: `OpExtension key:[]byte` 

encoded as `[ 0x01 CBOR(key)... ]`

### `OpLeaf`

format: `OpLeaf key:[]byte value:[]byte` 

encoded as `[ 0x00 CBOR(key)... CBOR(value)... ]`

### `OpAccountLeaf`

format: `OpAccountLeaf key:[]byte flags <nonce:uint64> <balance:[]byte>` 

encoded as `[ 0x05 CBOR(key|[]byte)... flags /CBOR(nonce|uint64).../ /CBOR(balance|[]byte).../ ]`
  
*flags* is a bitset encoded in a single bit.
* bit 0 defines if **code** is present; if set to 1, the next operand should be `OpCode` containing the code value;
* bit 1 defines if **storage** is present; if set to 1, the  operands following `OpAccountLeaf` will reconstruct a storage trie;
* bit 2 defines if **nonce** is not 0; if set to 0, *nonce* field is not encoded;
* bit 3 defines if **balance** is not 0; if set to 0, *balance* field is not encoded;
