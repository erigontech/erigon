# Block Witness Formal Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://tools.ietf.org/html/rfc2119).

## Basic Data types

`nil` - an empty value.

`Any` - any data type. MUST NOT be `nil`.

`Int` - an integer value. We treat the domain of integers as infinite,
the overflow behaviour or mapping to the actual data types is undefined
in this spec and should be up to implementation.

`Hash` - 32 byte value, representing a result of Keccak256 hashing.

`ByteArray` - a byte array of arbitrary size. MUST NOT be empty.

`()` - an empty array of arbitrary type.

## Nodes

```
type Node = HashNode (raw_hash:nil|Hash)
          | ValueNode (raw_value:nil|ByteArray)
          | AccountNode (nonce:Int balance:Int storage:nil|Node storage_hash:nil|Hash code_hash:nil|Hash)
          | LeafNode (key:ByteArray value:ValueNode|AccountNode)
          | ExtensionNode (key:ByteArray child:Node)
          | BranchNode (child0:nil|Node child1:nil|Node child3:nil|Node ... child15:nil|Node)
```


# Execution Enviroment

The witness execution environment MUST contain the following 3 elements:

- **Witness** -- a witness to be executed;

- **Stack** -- a stack on which the witness is executed;

## The Stack

Witnesses are executed on a stack machine that builds tries/calculates hashes.

```
type Stack = () 
           | PREPEND(Node, Stack)
```

At the beginning of witness execution the stack MUST be empty.


## The Witness

Each witness is a queue of instructions.

```
type Parameters  = ()
                 | PREPEND(Any, Parameters)

type Instruction = OpCode
                 | (OpCode, Parameters)

type Witness     = ()
                 | PREPEND(Instuction, Witness)
```

In every execution cycle a single instruction gets dequeued and a matching substitution rule gets applied to the stack.

In the end, when there are no more instructions left, there MUST be only one item left on the stack.  

## Instructions & Parameters

A single instruction is consists of substitution rules and parameters.

Each instruction MAY have one or more parameters.
The parameters values MUST be situated in the witness.
The parameter values MUST NOT be taken from the stack.

That makes it different from the helper function parameters that MAY come from the stack or MAY come from the witness.


## Substitution rules

A substitution rule is applied to the stack. It replaces zero, one or multiple
stack items with a single one.

Here is an example substitution rule. The current instruction is named `INSTR1`.

```
STACK(node1) STACK(node2) INSTR1(params) |=> 
STACK(helper1(node1, node2))
```

This substitution rule replaces 2 stack items `node1` and `node2`
with a single stack item `helper1(node1, node2)`.

Where `helper1` and `helper2` are pure functions defined in pseudocode (see the example below).

```
helper1 (value1, value2) {
    return value1 + value2
}

```

The specification for a substitution rule

```
[GUARD <CONDITION> ...]

[ STACK(<node-var-name>), ... ] <INSTRUCTION>[(<params>)] |=>
STACK(node-value)
```

There MUST be one and only one substitution rule applicable to the execution state. If an instruction has multiple substitution rules, the applicability is defined by the `GUARD` statements.
The substitution rule MAY have one or more GUARD statements.
The substitution rule MAY have one or more STACK statements before the instruction.
The substitution rule MUST have exactly one instruction.
The substitution rule MAY have parameters for the instruction.
The substitution rule MUST have at least one STACK statement after the arrow.

So, the minimal substitution rule is the one for the `HASH` instruction that pushes one hash to the stack:
```
HASH(hashValue) |=> STACK(HashNode{hashValue})
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

## Execution flow 

Let's look at the example.

Our example witness would be `HASH h1; HASH h2; BRANCH 0b11`.

Initial state of the stack is ` <empty> `;

---

**1. Executing `HASH h1`**: push a `hashNode` to the stack.

Witness: `HASH h2; BRANCH 0b11`

Stack: `(HashNode{h1})`

---

**2. Executing `HASH h2`**: push one more `hashNode` to the stack.

Witness `BRANCH 0b11`

Stack: `(HashNode{h2}; HashNode{h1})`

---

**3. Executing `BRANCH 0b11`**: replace 2 items on the stack with a single `fullNode`.

Witness: ` <empty> `

Stack: `(BranchNode{0: HashNode{h2}, 1: HashNode{h1}})`

---

So our stack has exactly a single item and there are no more instructions in the witness, the execution is completed.

## Instructions

### `LEAF key raw_value`

**Substitution rules**

```
LEAF(key, raw_value) |=> 
STACK(LeafNode{key, ValueNode(raw_value)})
```

### `EXTENSION key`

**Substitution rules**

```
GUARD node != nil

STACK(node) EXTENSION(key) |=>
STACK(ExtensionNode{key, node})

```

### `HASH raw_hash`

Pushes a `HashNode` to stack.

**Substitution rules**

```
HASH(raw_hash) |=>
STACK(HashNode{raw_hash})
```

### `CODE raw_code`

Pushes an nil node + the code hash to the stack.

```
CODE(raw_code) |=>
STACK(HashNode{RLP(KECCAK(raw_code))})
```

### `ACCOUNT_LEAF key nonce balance has_code has_storage`

**Substitution rules**

```

GUARD has_code == true
GUARD has_storage == true
GUARD storage_root is HashNode

STACK(code_hash_node) STACK(storage_hash_node) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(LeafNode{key, AccountNode{nonce, balance, storage_root, storage_root.raw_hash, code_hash_node.raw_hash}})
--

GUARD has_code == true
GUARD has_storage == true
GUARD storage_root is not HashNode 

STACK(code_hash_node) STACK(storage_root) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(LeafNode{key, AccountNode{nonce, balance, storage_root, HASH_STORAGE(storage_root), code_hash_node.raw_hash}})

--

GUARD has_code == false
GUARD has_storage == true
GUARD storage_root is not HashNode

STACK(storage_root) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(LeafNode{key, AccountNode{nonce, balance, storage_root, HASH_STORAGE(storage_root), nil}})

---

GUARD has_code == false
GUARD has_storage == true
GUARD storage_root is HashNode

STACK(storage_root) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(LeafNode{key, AccountNode{nonce, balance, storage_root, storage_root.raw_hash, nil}})

--

GUARD has_code == true
GUARD has_storage == false

STACK(code_hash_node) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(LeafNode{key, AccountNode{nonce, balance, nil, nil, code_hash_node.raw_hash}})

--

GUARD has_code == false
GUARD has_storage == false

ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(LeafNode{key, AccountNode{nonce, balance, nil, nil, nil}})

```

### `EMPTY_ROOT`

Pushes an empty node + an empty hash to the stack

**Substitution rules**

```
EMPTY_ROOT |=>
STACK(HashNode{RLP(0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421)})
```

### `NEW_TRIE`

Stops the witness execution.

### `BRANCH mask`

This instruction pops `NBITSET(mask)` items from both node stack and hash stack (up to 16 for each one). Then it pushes a new branch node on the node stack that has children according to the stack; it also pushes a new hash to the hash stack.

**Substitution rules**
```

GUARD NBITSET(mask) == 2 

STACK(n0) STACK(n1) BRANCH(mask) |=> 
STACK(BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1)})

---

GUARD NBITSET(mask) == 3

STACK(n0) STACK(n1) STACK(n2) BRANCH(mask) |=> 
STACK(BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1, n2)})

---

...

---

GUARD NBITSET(mask) == 16

STACK(n0) STACK(n1) ... STACK(n15) BRANCH(mask) |=>
STACK(BranchNode{MAKE_VALUES_ARRAY(mask, n0, n1, ..., n15)})
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

### `LEN(bytes)`

returns the lengths of the byte array

### `HASH_STORAGE(root_node)`

hashes the trie starting from the root node, based on the yellow paper
