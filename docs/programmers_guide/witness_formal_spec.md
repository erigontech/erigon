# Block Witness Formal Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://tools.ietf.org/html/rfc2119).

## Data Types

### Basic

`nil` - an empty value.

`Any` - any data type. MUST NOT be `nil`.

`Int` - an integer value. We treat the domain of integers as infinite,
the overflow behaviour or mapping to the actual data types is undefined
in this spec and should be up to implementation.

`Hash` - 32 byte value, representing a result of Keccak256 hashing.

`ByteArray` - a byte array of arbitrary size. MUST NOT be empty.

`()` - an empty array of arbitrary type.

`(Type1 Type1 Type1)` - an array of a type `Type1`.

### Nodes

```
type Node = HashNode{raw_hash:Hash}
          | ValueNode{raw_value:ByteArray}
          | AccountNode{nonce:Int balance:Int storage:nil|Node code:nil|CodeNode|HashNode}
          | LeafNode{key:ByteArray value:ValueNode|AccountNode}
          | ExtensionNode{key:ByteArray child:Node}
          | BranchNode{child0:nil|Node child1:nil|Node child3:nil|Node ... child15:nil|Node}
          | CodeNode{code:ByteArray}
```

### Witness

Witness MUST have at least 1 element.

```
type WitnessHeader = {version:Int}
type Witness = (Node|Instruction{code:Int parameter:Any...} ...)
```


## Execution Enviroment

The witness execution environment MUST contain the following 2 elements:

- **WitnessHeader** -- a header containing the version of the witness. The `version` MUST be 1.

- **Witness** -- a witness to be executed;

- **Substitution Rules** -- a list of all possible substitution rules.


## Execution process

Initially, the Witness MUST BE an array of `Instruction`s.

Then, as substitution rules are applied to the witness, some elements of the
array are replaces with `Node`s.

The execution continues until there are no substitution rules left to execute.

Here is how the execution code might look like in Go for building a single trie.

```go

witness := GetInitialWitness()
rules := GetSubstitutionRules()
numberOfRulesApplied := 1 // initial state

for rulesApplied {
    witness, numberOfRulesApplied := ApplyRules(witness, rules)
}

if len(witness) == 1 {
    trie.root = witness[0]
} else {
    panic("witness execution failed")
}

```

And here is an example of the execution process (we will use the set of rules
form the **Substitution Rules** section of this document):

Step 1. Witness: `(HASH h1 HASH h2 BRANCH 0b101 HASH h3 BRANCH 0b11)`

Step 2. Applying `HASH` substitution rules.
Witness: `(HashNode{h1} HashNode{h2} BRANCH 0b101 HashNode{h3} BRANCH 0b11)`

Step 3. Applying `BRANCH` substitution rules (only once, because `BRANCH 0b11`
doesn't pass its `GUARD` statements just yet).
Witness: `(BranchNode{0: HashNode{h1} 2:HashNode{h2}} HashNode{h3} BRANCH 0b11)`

Step 4: Applying `BRANCH` substitution rules again.
Witness: `(BranchNode{0: BranchNode{0: HashNode{h1} 2:HashNode{h2}} 1:HashNode{h3}})`

Step 5: No more rules are applicable, the execution ends successfully.


## End Criteria

The execution ends when there are no substitution rules applicable for this
witness.

### Building a single trie from the witness

If we are building a single trie from the witness, then the only SUCCESS
executon is when the following are true:

- The execution state MUST match the End Criteria
- There MUST be only one item left in the witness
- This item MUST be of the type `Node`
    
In that case, this last item will be the root of the built trie.

Every other end state is considered a FAILURE.


### Building a Forest 

We also can build a forest of tries with this approach, by adding a new
Instruction `NEW_TRIE` and adjusting the success criteria a bit:

- The execution state MUST match the End Criteria;
- The items that are left in the witness MUST follow this pattern: `(Node
    NEW_TRIE ... Node)`
- Each `Node` element will be a root of a trie.

Every other end state is considered a FAILURE.


## Instructions & Parameters

A single instruction is consists of substitution rules and parameters.

Each instruction MAY have one or more parameters.
The parameters values MUST be situated in the witness.
The parameter values MUST NOT be taken from the stack.

That makes it different from the helper function parameters that MAY come from the stack or MAY come from the witness.


## Substitution rules

A substitution rule consists of 2 parts: the criteria (on the left of the `|=>` sign) and the result (on the right of the `|=>` sign).
The criteria MUST consists of 0 or more `GUARD` statements and a pattern.

The result is a single `Node` statement that replaces the pattern in the
witness if it matches and the guards are passed.

Pattern matching is happening by the types.

The result MAY contain helper functions or might have in-line computation.

```
[GUARD <CONDITION> ...] [ NodeType(<node-var-name>), ... ] <INSTRUCTION>[(<params>)] |=>
Node(<HELPER_FUNCTION_OR_COMPUTATION>)
```

`NodeType` is one of the types of nodes to match. Can also be `Node` to match
any non-nil node.

There MUST be one and only one substitution rule applicable to the execution state. If an instruction has multiple substitution rules, the applicability is defined by the `GUARD` statements.
The substitution rule MAY have one or more GUARD statements.
The substitution rule MAY have one or more STACK statements before the instruction.
The substitution rule MUST have exactly one instruction.
The substitution rule MAY have parameters for the instruction.
The substitution rule MUST have at least one STACK statement after the arrow.

So, the minimal substitution rule is the one for the `HASH` instruction that pushes one hash to the stack:
```
HASH(hashValue) |=> HashNode{hashValue}
```

## GUARDs

Each substitution rule can have zero, one or multiple `GUARD` statements.
Each `GUARD` statement looks like this:

```
GUARD <CONDITION>
```

For a substitution rule to be applicable, the `<CONDITION>` in its `GUARD` statement MUST be true.

If a substitution rule has multiple `GUARD` statements, all of them MUST BE satisfied.

If there are no `GUARD` statements, the substitution rule is always applicable.

## Helper functions

Helper functions are functions that are used in GUARDs or substitution rules.

Helper functions MUST be pure.
Helper functions MUST have at least one argument.
Helper functions MAY have variadic parameters: `HELPER_EXAMPLE(arg1, arg2, list...)`.
Helper functions MAY contain recursion.

## Instructions

### `LEAF key raw_value`

**Substitution rules**

```
LEAF(key, raw_value) |=> LeafNode{key, ValueNode(raw_value)}
```

### `EXTENSION key`

**Substitution rules**

```
GUARD node != nil

Node(node) EXTENSION(key) |=> ExtensionNode{key, node}

```

### `HASH raw_hash`

Pushes a `HashNode` to stack.

**Substitution rules**

```
HASH(hash_value) |=> HashNode{hash_value}
```

### `CODE raw_code`

Pushes an nil node + the code hash to the stack.

```
CODE(raw_code) |=> CodeNode{raw_code}
```

### `ACCOUNT_LEAF key nonce balance has_code has_storage`

**Substitution rules**

```
GUARD has_code == true
GUARD has_storage == true

CodeNode(code) Node(storage_hash_node) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
LeafNode{key, AccountNode{nonce, balance, storage_root, code}}

---

GUARD has_code == true
GUARD has_storage == true

HashNode(code) Node(storage_hash_node) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
LeafNode{key, AccountNode{nonce, balance, storage_root, code}}

---

GUARD has_code == false
GUARD has_storage == true

Node(storage_root) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
LeafNode{key, AccountNode{nonce, balance, storage_root, nil}}

---

GUARD has_code == true
GUARD has_storage == false

CodeNode(code) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
LeafNode{key, AccountNode{nonce, balance, nil, nil, code}}

---

GUARD has_code == true
GUARD has_storage == false

HashNode(code) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
LeafNode{key, AccountNode{nonce, balance, nil, nil, code}}

---

GUARD has_code == false
GUARD has_storage == false

ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
LeafNode{key, AccountNode{nonce, balance, nil, nil, nil}}

```

### `NEW_TRIE`

Stops the witness execution.

No substitution rules

### `BRANCH mask`

This instruction pops `NBITSET(mask)` items from both node stack and hash stack (up to 16 for each one). Then it pushes a new branch node on the node stack that has children according to the stack; it also pushes a new hash to the hash stack.

**Substitution rules**
```

GUARD NBITSET(mask) == 2 

Node(n0) Node(n1) BRANCH(mask) |=> 
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1)}

---

GUARD NBITSET(mask) == 3

Node(n0) Node(n1) Node(n2) BRANCH(mask) |=> 
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1, n2)}

---

...

---

GUARD NBITSET(mask) == 16

Node(n0) Node(n1) ... Node(n15) BRANCH(mask) |=>
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1, ..., n15)}
```

## Helper functions

### `MAKE_VALUES_ARRAY`

returns an array of 16 elements, where values from `values` are set to the indices where `mask` has bits set to 1. Every other place has `nil` value there.

**Example**: `MAKE_VALUES_ARRAY(5, [a, b])` returns `[a, nil, b, nil, nil, ..., nil]` (binary representation of 5 is `0000000000000101`)

```
MAKE_VALUES_ARRAY(mask, values...) {
    return MAKE_VALUES_ARRAY(mask, 0, values)
}

MAKE_VALUES_ARRAY(mask, idx, values...) {
    if idx > 16 {
        return []
    }

    if BIT_TEST(mask, idx) {
        return PREPEND(FIRST(values), (MAKE_VALUES_ARRAY mask, INC(idx), REST(values)))
    } else {
        return PREPEND(nil, (MAKE_VALUES_ARRAY mask, INC(idx), values))
    }
}
```


### `RLP(value)`

returns the RLP encoding of a value


### `NBITSET(number)`

returns number of bits set in the binary representation of `number`.

### `BIT_TEST(number, n)`

`n` MUST NOT be negative.

returns `true` if bit `n` in `number` is set, `false` otherwise.

### `PREPEND(value, array)`

returns a new array with the `value` at index 0 and `array` values starting from index 1

### `INC(value)`

increments `value` by 1

### `FIRST(array)`

returns the first value in the specified array

### `REST(array)`

returns the array w/o the first item

### `KECCAK(bytes)`

returns a keccak-256 hash of `bytes`


## Serialization

The format for serialization of everything except hashes (that we know the
length of) is [CBOR](https://cbor.io). It is RFC-specified and concise.

For hashes we use the optimization of knowing the lengths, so we just read 32
bytes

### Block Witness Format

Each block witness consists of a header followed by a list of operators.

There is no length of witness specified anywhere, the code expects to just reach `EOF`.

Serialized Witness: `(HEADER, OP1, OP2, ..., OPn-1, OPn, EOF)`

#### Encoding

##### CBOR

The parts of the key that are encoded with CBOR are marked by the `CBOR` function.

##### Keys

Keys are also using custom encryption to make them more compact.

The nibbles of a key are encoded in a following way `[FLAGS NIBBLE1+NIBBLE2 NIBBLE3+NIBBLE4 NIBBLE5... ]`

*FLAGS*
* bit 0 -- 1 if the number of nibbles were odd
* bit 1 -- 1 if the nibbles end with 0x10 (the terminator byte)

This is shown later as `ENCODE_KEY` function.

#### Header

format: `version:byte`

encoded as `[ version ]`

the current version is 1.

#### Instructions 

Each instruction starts with an opcode (`uint`).

Then it might contain some data.

##### `HASH` 

format: `HASH hash:[32]byte`

encoded as `[ 0x03 hash_byte_1 ... hash_byte_32 ]`

##### `BRANCH`

format: `BRANCH mask:uint32`

*mask* defines which children are present 
(e.g. `0000000000001011` means that children 0, 1 and 3 are present and the other ones are not)

encoded as `[ 0x02 CBOR(mask)...]`

##### `CODE`

format: `CODE code:[]byte`

encoded as `[ 0x04 CBOR(code)... ]`

##### `EXTENSION`

format: `EXTENSION key:[]byte` 

encoded as `[ 0x01 CBOR(ENCODE_KEY(key))... ]`

##### `LEAF`

format: `LEAF key:[]byte value:[]byte` 

encoded as `[ 0x00 CBOR(ENCODE_KEY(key))... CBOR(value)... ]`

##### `ACCOUNT_LEAF`

format: `ACCOUNT_LEAF key:[]byte flags [nonce:uint64] [balance:[]byte]` 

encoded as `[ 0x05 CBOR(ENCODE_KEY(key))... flags /CBOR(nonce).../ /CBOR(balance).../ ]`

*flags* is a bitset encoded in a single bit (see [`witness_operators_test.go`](../../trie/witness_operators_test.go) to see flags in action).
* bit 0 defines if **code** is present; if set to 1 it assumes that either `OpCode` or `OpHash` already put something on the stack;
* bit 1 defines if **storage** is present; if set to 1, the operators preceeding `OpAccountLeaf` will reconstruct a storage trie;
* bit 2 defines if **nonce** is not 0; if set to 0, *nonce* field is not encoded;
* bit 3 defines if **balance** is not 0; if set to 0, *balance* field is not encoded;

##### `NEW_TRIE`

format: `NEW_TRIE`

encoded as `[ 0xBB ]`

