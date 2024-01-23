# Block Witness Formal Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://tools.ietf.org/html/rfc2119).

## Motivation

The witness format was picked to satisfy the following criteria.

**1. Witness Streaming w/o intermediate dynamic buffers**
It should be possible to basically stream-as-you-encode the the trie on one node
and recreate it at the same time by using a fixed allocated buffers. That helps
efficiently transfer and encode/decode witnesses.

The witness allows to walk through the trie and produce the witness as you go w/o buffering
send it straight to a network socket. A peer can then receive it from the socket
and it can start computing the hash of the state root straight away.

Also, it means that the memory consumption of witness processing itself will be
fixed and predictable, which helps for nodes with limited memory.


**2. Building Forests**
It should be possible to build a forest of tries from a single witness. It is
needed for two use-cases: 

- partial witnesses (like the ones that are used in
semi-stateless initial sync, when you already have some trie that you need to
extend with more data);

- splitting the witness into verifiable chunks (when we can build a trie piece
    by piece and verify root hashes). That is possible by first constructing
    a witness for the top of the trie (to verify the root hash) and then for
    subtries from top level to the bottom. At all times you will be able to
    verify a subtrie root hash.

**3. Arbitrary chunk sizes**
The witness format doesn't limit a chunk size, that makes it easy to experiment and find
the best size for efficient relaying properties.



## Use Cases

The format described in this document can be used to store and build
partial hexary Patricia Merkle Tries and forests in a linear way.

It can be used to store full or sparse tries as well.
It is also suitable to store subtries that can be used for semi-stateless
initial sync and other semi-stateless operations.


## Notation & Data types


### Basic data types

`nil` - an empty value.

`Bool` -a boolean value.

`Any` - any data type. MUST NOT be `nil`.

`Int` - an integer value. We treat the domain of integers as infinite,
the overflow behaviour or mapping to the actual data types is undefined
in this spec and should be up to implementation.

`Byte` - a single byte.

`Hash` - 32 byte value, representing a result of Keccak256 hashing.

### Composite data types

`()` - an empty array of arbitrary type.

`(Type...)` - an array of a type `Type`. MUST NOT be empty.

`{field:Type}` - a typed dictionary with(`field` is of type `Type`).


### Type Definitions

**Full type definition**
```
type HashNode = {raw_hash:Hash}
type CodeNode = {code:(Byte...)}
type Node = HashNode|CodeNode
```
this definition defines 3 types: `Node`, `HashNode` and `CodeNode`.

The defined type `Node` can be used to pattern-match both `CodeNode` and
`HashNode`.


This definition can also be writen in more compact way.

```
type Node = HashNode{raw_hash:Hash}|CodeNode{code:(Byte...)}
```

Both compact and full type definitons are equivalent to each other and
can be used interchangeably.


### Composite Types Destructuring

Sometimes, it is useful to show the actual contents of a complex type, like an
array or a dictionary. For that, a tecnique called [destructuring](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Destructuring_assignment) is used.

So, if we have a byte array: `(Byte...)` but we want to demonstrate it's
contents, we can show it as `(Header Instruction...)`.

This technique is used mostly to show the bytes layout in a serialized witness.


### Optional Values

Optional values are specified in square brackets.
`OpCode [parameters]` -- that mean that `parameters` might present and might
be absent.


## The Witness Format

A block witness is a binary data format that consists of the following logical
elements:

- witness header;

- list of instructions with parameters.


### The Logical Structure

Here, we will discuss a logical structure of the witness and its elements in
terms of types.

Note, that the keys names in the dictionaries aren't encoded in the binary
format and are serving the purpose to improve readability.

The header at the moment contains the versioning information.
```
type WitnessHeader = {version:Int}
```

The list of instructions is the key element for pattern-matching when building
a trie from the witness. 
We will use a simple naming convention to make them more salient: `BRANCH`, etc

The list of instructions is defined here.

```
type Instruction = LEAF{key:(Byte...) value:(Byte...)}
                 | EXTENSION{key:(Byte...) nonce:Int balance:Int has_code:Bool has_storage:Bool}
                 | HASH{raw_hash:Hash}
                 | CODE{raw_code:(Byte...)}
                 | ACCOUNT_LEAF{key:(Byte...)
                 | BRANCH{mask:Int}
                 | NEW_TRIE{}
                 | SMT_LEAF{nodeType:Byte address:(Byte...) /storageKey:(Byte...)/ value:(Byte...)}

type Witness = (INSTRUCTION...)

```

### The Bytes Layout

`(Header Instruction1 Instuction2... EOF)`

Each block witness consists of a header followed by a list of instructions.

There is no length of witness specified anywhere, the code expects to just reach `EOF`.

#### Endianness

All the data is interpreted as big-endian.

#### CBOR

The parts of the key that are encoded with CBOR are marked by the `CBOR` function.

#### Keys

Keys are also using custom encoding to make them more compact.

The nibbles of a key are encoded in a following way `(FLAGS NIBBLE1+NIBBLE2 NIBBLE3+NIBBLE4 NIBBLE5... )`

*FLAGS*
* bit 0 -- 1 if the number of nibbles were odd
* bit 1 -- 1 if the nibbles end with 0x10 (the terminator byte)

This is shown later as `ENCODE_KEY` function.


#### Header

A header is encoded in a single byte as `( version )`.

The current version MUST BE 1.

#### Instructions 

To distinguish between instuctions, they are serialized in the following way:
`(OpCode [parameters])`

```
type OpCode = Byte
```

The `OpCode` is a single byte that has a unique identifier of the instruction.
It defines how the next bytes are interpreted.

The parameter's type and value are interpeted based on it's position (e.g. if
`HASH` expects a 32-byte hash after the opcode, the parsing code should treat
the next 32 bytes as a hash).

We don't store any extra meta information for the values that have fixed
lengths. For the other types of data the length is encoded in one way or
another.

Here is how the instuctions are encoded:

* **`LEAF`** -> `( 0x00 CBOR(ENCODE_KEY(key))... CBOR(value)... )`.

* **`EXTENSION`** -> `( 0x01 CBOR(ENCODE_KEY(key))... )`.

* **`BRANCH`** -> `( 0x02 CBOR(mask)... )`; `mask` defines which children are present 
    (e.g. `0000000000001011` means that children 0, 1 and 3 are present and the other ones are not)

* **`HASH`** -> `( 0x03 hash_byte_1 ... hash_byte_32 )`

* **`CODE`** -> `( 0x04 CBOR(code)... )`

* **`ACCOUNT_LEAF`** -> `( 0x05 CBOR(ENCODE_KEY(key))... flags /CBOR(nonce).../ /CBOR(balance).../ )`
  `flags` is a bitset encoded in a single byte (bit endian):
    * bit 0 defines if **code** is present; if set to 1, then `has_code=true`;
    * bit 1 defines if **storage** is present; if set to 1, then `has_storage=true`;
    * bit 2 defines if **nonce** is not 0; if set to 0, *nonce* field is not encoded;
    * bit 3 defines if **balance** is not 0; if set to 0, *balance* field is not encoded;

* **`SMT_LEAF`** -> `( 0x07 nodeType CBOR(address) /CBOR(storageKey).../ CBOR(value)...)`
    * if `nodeType` == `0x03`, then an extra field `storageKey` is read; otherwise it is skipped


* **`NEW_TRIE`** -> `( 0xBB )`



### Witness -to-> Trie

Let's take a look on how to build a Merkle Trie from the witness.
the witness.

### Trie Nodes

To make the spec self-contained, here are all the possible trie node types with
their structure.

```
type Node = HashNode{raw_hash:Hash}
          | ValueNode{raw_value:(Byte...)}
          | AccountNode{nonce:Int balance:Int storage:nil|Node code:nil|CodeNode|HashNode}
          | LeafNode{key:(Byte...) value:ValueNode|AccountNode}
          | ExtensionNode{key:(Byte...) child:Node}
          | BranchNode{child0:nil|Node child1:nil|Node child3:nil|Node ... child15:nil|Node}
          | CodeNode{code:(Byte... )}
          | SmtLeafNode{nodeType:Byte address:(Byte...), storageKey:(Byte...), value:(Byte...)}
```

## Execution process

The execution process is based on **Substitution Rules** that are applied
repeatedly to a witness until there are no more applicable rules left.

In Go, this high-level algorithm can be shown as this.

```go
witness := GetInitialWitness()
rules := GetSubstitutionRules()
numberOfRulesApplied := 1 // initial state

for numberOfRulesApplied > 0 {
    witness, numberOfRulesApplied := ApplyRules(witness, rules)
}

if len(witness) == 1 {
    trie.root = witness[0]
} else {
    panic("witness execution failed")
}

```

#### Example

Let's look at some toy example on how this process works. 
After that, we define the set of rules that is enough to rebuild a proper sparse Patricia Merkle Trie.

Here is an example of a rule: `HASH{h1} |=> HashNode{h1}`. It replaces a single
`Instruction` with a single `Node`.

Applicability of a substitution rule is defined by a pattern to to the left of
`|=>`. In our case it requires an instruction `HASH` to be present.

Let's see the substitution rules in action.

Witness: `(HASH{h1} HASH{h2} HASH{h3})`

Let's apply this rule to the witness: `(HashNode{h1} HashNode{h2} HashNode{h3})`

That doesn't really look like a trie, does it?

Just a single rule is not enough to build a trie, we need some kind of rule to
define branches: `Node(n0) Node(n1) BRANCH |=> BranchNode{0: n0, 1: n1}`

This rule is a bit more complicated, not only it requires a `BRANCH`
instruction to be present, but to also have at least 2 `Node` instance to the
left of it.

Witness: `(HASH{h1} HASH{h2} BRANCH HASH{h3} BRANCH)`

Let's apply the rules to the witness:
1. Applying `HASH{h1} |=> HashNode{h1}` (the only applicable rule):
    `(HashNode{h1} HashNode{h2} BRANCH HashNode{h3} BRANCH)`

2. Applying `Node(n0) Node(n1) BRANCH |=> BranchNode{0: n0, 1: n1}`:
    `(BranchNode{0: HashNode{h1} 1: HashNode{h2}} HashNode{h3} BRANCH)`

3. Applying `Node(n0) Node(n1) BRANCH |=> BranchNode{0: n0, 1: n1}` again:
    `(BranchNode{0: BranchNode{0: HashNode{h1} 1: HashNode{h2}} 1:HashNode{h3}})`

4. No more rules can be applied, stop the execution.

Now we only have one element and it is a root of the rebuilt trie.

### Finiteness

Every rule MUST replace one `Instruction` (and sometimes a couple elements to the
left of it) with one and only one `Node`, so this process is always finite.

### Success criteria: a single trie

If we are building a single trie from the witness, then the only SUCCESS
execution is when the following are true:

- The execution state MUST match the End Criteria
- There MUST be only one item left in the witness
- This item MUST be one of these types: `LeafNode`, `ExtensionNode`, `BranchNode`
    
In that case, this last item will be the root of the built trie.

Every other end state is considered a FAILURE.


### Success criteria: a forest 

We also can build a forest of tries with this approach, by adding a new
Instruction `NEW_TRIE` and adjusting the success criteria a bit:

- The execution state MUST match the End Criteria;
- The items that are left in the witness MUST follow this pattern:
    `(LeafNode|ExtensionNode|BranchNode NEW_TRIE ... LeafNode|ExtensionNode|BranchNode)`
- Each `LeafNode|ExtensionNode|BranchNode` element root of a trie.

Every other end state is considered a FAILURE.


### Substitution rules in detail

The full syntax of a substitution rule is the following:

```
[GUARD <CONDITION> ...] [ NodeType(bound_variable1)... ] INSTRUCTION{(param1 ...)} |=>
Node(<HELPER_FUNCTION_OR_COMPUTATION>)
```

`NodeType` here can be either `Node` (if we want it to match any node) or
a specific type of a node if we need a more strict type match.


A substitution rule consists of 3 parts: 

`[GUARD] PATTERN |=> RESULT`

- to the left of the `|=>` sign:

    - optional `GUARD` statements;

    - the pattern to match against;

- result, to the right of the `|=>` sign.


#### `GUARD`s

Each substitution rule can have zero, one or multiple `GUARD` statements.
Each `GUARD` statement looks like this:

```
GUARD <CONDITION>
```

For a substitution rule to be applicable, the `<CONDITION>` in its `GUARD` statement MUST be true.

If a substitution rule has multiple `GUARD` statements, all of them MUST BE satisfied.

If there are no `GUARD` statements, the substitution rule's applicability is
only defined by the PATTERN.

Example:
```
 GUARD NBITSET(mask) == 2
|---- GUARD STATEMENT ---|

 Node(n0) Node(n1) BRANCH{mask} |=> 
 BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1)}
```

For the example rule to be applicable both facts MUST be true:

1. `mask` contains only 2 bits set to 1 (the rest are set to 0);

2. to the left of `BRANCH` instruction there is at least 2 `Node`s.

Fact (1) comes from the `GUARD` statement.


#### PATTERN

`[NodeType(boundVar1)... NodeType(boundVarN)] Instruction[(param1... paramN)]`

A pattern MUST contain a single instruction.
A pattern MAY contain one or more `Node`s to the left of the instruction to
match.
An instruction MAY have one or more parameters.

Pattern matching is happening by the types. `Node` type means any node is
matched, some specific node type will require a specific match.

Pattern can have bound variable names for both matched nodes and instruction
parameters (if present).

Match:

```
HASH{h0} HashNode{h1} HashNode{h2} BRANCH{0b11}
        |------------------- MATCH ------------|

HASH{h0} BranchNode{0: HashNode{h1} 1: HashNode{h2}}
        |----------- SUBSTITUTED -------------------|
```

No match (not enough nodes to the left of the instruction):

```
HASH h0 HASH h1 HashNode{h2} BRANCH{0b11}
```

#### Result

`NodeType(HELPER_FUNCTION(arguments))`

The result is a single `Node` statement that replaces the pattern in the
witness if it matches and the guards are passed.

The result MAY contain helper functions or might have in-line computation.
The result MUST have a specific node type. No generic `Node` is allowed.

Helper functions or inline computations might use bound variables from the
pattern. 

Example

```
                             
Node(n0) Node(n1) BRANCH{mask} |=>
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1)}
                             ^     ^-- ^--- BOUND NODES
                             |---- BOUND INSTRUCTION PARAM
          |------ HELPER CALL ------------|
|----------------- RESULT ------------------|

```

#### Helper functions

Helper functions are functions that are used in GUARDs or substitution rules.

Helper functions MUST be pure.
Helper functions MUST have at least one argument.
Helper functions MAY have variadic parameters: `HELPER_EXAMPLE(arg1, arg2, list...)`.
Helper functions MAY contain recursion.

### Substitution rules

Let's look at the full set of substitution rules that are enough to build
a trie from a witness.

Note, that there are no substitution rules for `NEW_TRIE`, and that makes it
possible to use it as a divider between trie when building a forest.

```

HASH{hash_value} |=> HashNode{hash_value}

---

CODE{raw_code} |=> CodeNode{raw_code}

---

LEAF{key, raw_value} |=> LeafNode{key, ValueNode{raw_value}}

---

SMT_LEAF{nodeType, address, storageKey, value} |=> SmtLeafNode{nodeType, address, storageKey, value}

---

Node(n) EXTENSION{key} |=> ExtensionNode{key, n}

---

GUARD has_code == true
GUARD has_storage == true

CodeNode(code) Node(storage_hash_node) ACCOUNT_LEAF{key, nonce, balance, has_code, has_storage} |=>
LeafNode{key, AccountNode{nonce, balance, storage_root, code}}

---

GUARD has_code == true
GUARD has_storage == true

HashNode(code) Node(storage_hash_node) ACCOUNT_LEAF{key, nonce, balance, has_code, has_storage} |=>
LeafNode{key, AccountNode{nonce, balance, storage_root, code}}

---

GUARD has_code == false
GUARD has_storage == true

Node(storage_root) ACCOUNT_LEAF{key, nonce, balance, has_code, has_storage} |=>
LeafNode{key, AccountNode{nonce, balance, storage_root, nil}}

---

GUARD has_code == true
GUARD has_storage == false

CodeNode(code) ACCOUNT_LEAF{key, nonce, balance, has_code, has_storage} |=>
LeafNode{key, AccountNode{nonce, balance, nil, code}}

---

GUARD has_code == true
GUARD has_storage == false

HashNode(code) ACCOUNT_LEAF{key, nonce, balance, has_code, has_storage} |=>
LeafNode{key, AccountNode{nonce, balance, nil, nil, code}}

---

GUARD has_code == false
GUARD has_storage == false

ACCOUNT_LEAF{key, nonce, balance, has_code, has_storage} |=>
LeafNode{key, AccountNode{nonce, balance, nil, nil, nil}}

---

GUARD NBITSET(mask) == 2

Node(n0) Node(n1) BRANCH{mask} |=> 
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1)}

---

GUARD NBITSET(mask) == 3

Node(n0) Node(n1) Node(n2) BRANCH{mask} |=> 
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1, n2)}

---

...

---

GUARD NBITSET(mask) == 16

Node(n0) Node(n1) ... Node(n15) BRANCH{mask} |=>
BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1, ..., n15)}
```

### Helper functions

These are all helper functions that we need to execute the rules.

#### `MAKE_VALUES_ARRAY mask values...`

`mask` should be at least 16 bits wide.

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


#### `NBITSET(number)`

returns number of bits set in the binary representation of `number`.

#### `BIT_TEST(number, n)`

returns `true` if bit `n` in `number` is set, `false` otherwise.

#### `PREPEND(value, array)`

returns a new array with the `value` at index 0 and `array` values starting from index 1

#### `INC(value)`

increments `value` by 1

#### `FIRST(array)`

returns the first value in the specified array

#### `REST(array)`

returns the array w/o the first item


## Validating The Witness

The witness MUST be rejected as invalid if any of these rules are violated.

1. When reading the binary data:
    - The `version` in the witness header MUST be 1;
    - The `opcode` MUST be one of `0x00`, `0x01`, `0x02`, `0x03`, `0x04`, `0x05`, `0xBB`;
    - Every fixed-length parameter MUST be read fully (e.g. if we need to read
        32-byte `Hash`, then there should be at least 32 bytes available to
        read).

2. When building a single trie/a forest:
    - When there are no rules applicable, the success criteria for a trie/forst MUST be met. It
        is important to add that on invalid inputs the situation when there are
        no rules applicable can happen way before every `INSTRUCTION` is
        replaced.


3. When checking the root hash:
    - The root hash of the built trie MUST be equal to what we expect.


## Implementer's guide

This section contains a few tips on actually implementing this spec.

#### Simple stack machine execution

One simpler implementation of these rules can be a stack machine, taking one
instruction from the left of the witness and applying rules. That allows to
rebuild a trie in a single pass.


#### Building hashes.

It might be useful to build hashes together with building the nodes so we can
execute and validate the trie in the same pass.


## Alternatives considered

### GetNodeData

TBD
