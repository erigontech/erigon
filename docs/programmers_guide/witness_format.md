# Block Witness Format

Each block witness consists of a header followed by a list of operators.

There is no length of witness specified anywhere, the code expects to just reach `EOF`.

Witness: `[HEADER, OP1, OP2, ..., OPn-1, OPn, EOF]`

## Encoding

### Keys

key nibbles are encoded in a following way `[FLAGS NIBBLE1+NIBBLE2 NIBBLE3+NIBBLE4 NIBBLE5... ]`

*FLAGS*
* bit 0 -- 1 if the number of nibbles were odd
* bit 1 -- 1 if the nibbles end with 0x10

## Header

format: `version:byte`

encoded as `[ version ]`

the current version is 1.

## Operators

Each operator starts with an opcode (see [`witness_operators.go`](../../execution/trie/witness_operators.go) for exact values).

Then it might contain some data.

### `OpEmptyRoot` 

puts empty trie root to on the stack

format: `OpEmptyRoot` 

encoded as `[ 0x06 ]`

### `OpHash` 

puts a single hash on the stack

format: `OpHash hash:[32]byte`

encoded as `[ 0x03 hash_byte_1 ... hash_byte_32 ]`

### `OpBranch`

pops N values from the stack and adds them as the children; pushes the result to the stack; N is the number of 1's in the *mask* field.

format: `OpBranch mask:uint32`

*mask* defines which children are present 
(e.g. `0000000000001011` means that children 0, 1 and 3 are present and the other ones are not)

encoded as `[ 0x02 CBOR(mask)...]`

### `OpCode`

pushes a code to the stack

format: `OpCode code:[]byte`

encoded as `[ 0x04 CBOR(code)... ]`

### `OpExtension`

pushes an extension node with a specified key on the stack

format: `OpExtension key:[]byte` 

encoded as `[ 0x01 CBOR(key)... ]`

### `OpLeaf`

pushes a leaf with specified key and value to the stack

format: `OpLeaf key:[]byte value:[]byte` 

encoded as `[ 0x00 CBOR(key)... CBOR(value)... ]`

### `OpAccountLeaf`

pushes a leaf with specified parameters to the stack; if flags show, before that pops 1 or 2 values from the stack;

format: `OpAccountLeaf key:[]byte flags [nonce:uint64] [balance:[]byte]` 

encoded as `[ 0x05 CBOR(key|[]byte)... flags /CBOR(nonce).../ /CBOR(balance).../ ]`
  
*flags* is a bitset encoded in a single bit (see [`witness_operators_test.go`](../../execution/trie/witness_operators_test.go) to see flags in action).
* bit 0 defines if **code** is present; if set to 1 it assumes that either `OpCode` or `OpHash` already put something on the stack;
* bit 1 defines if **storage** is present; if set to 1, the operators preceding `OpAccountLeaf` will reconstruct a storage trie;
* bit 2 defines if **nonce** is not 0; if set to 0, *nonce* field is not encoded;
* bit 3 defines if **balance** is not 0; if set to 0, *balance* field is not encoded;
