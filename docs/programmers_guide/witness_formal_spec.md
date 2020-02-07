# Block Witness Formal Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://tools.ietf.org/html/rfc2119).

## The Stack

Witnesses are executed on a stack machine that builds tries/calculates hashes.

The stack consists of stack items.
Each item is a pair: `(node, hash)`, where `node` is an object representing the Merkle trie node, and `hash` is 32 byte node hash value. The exact implementation details of `node`s and `hash`es are not the part of the spec.

Each witness is a queue of instructions.

In every execution cycle a single instruction gets dequeued and a matching substitution rule gets applied to the stack.

In the end, when there are no more instructions left, there MUST be only one item left on the stack.  

### Nodes

```
type node = nil
          | hashNode (raw_hash:byte[32])
          | valueNode (raw_value:byte[])
          | accountNode (nonce:uint balance:int storage:node storage_hash:hash code_hash:hash)
          | shortNode (key:byte[] value:node)
          | branchNode (child0:node child1:node child3:node ... child15:node)
```

## Substitution rules

Here is an example substitution rule. The current instruction is named `INSTR1`.

```
STACK(node1, hash1) STACK(node2, hash2) INSTR1(params) |=> 
STACK(helper1(node1, node2), helper2(hash1, hash2))
```

This substitution rule replaces 2 stack items `(node1, hash1)` and `(node2, hash2)`
with a single stack item `(helper1(node1, node2), helper2(hash1, hash2))`.

Where `helper1` and `helper2` are pure functions defined in pseudocode (see the example below).

```
helper1 (value1, value2) {
    return value1 + value2
}

helper2 (hash1, hash2) {
    return KECCAK(hash1 + hash2)
}
```

The specification for a substitution rule

```
[GUARD <CONDITION> ...]

[ STACK(<node-var-name>, <hash-var-name>), ... ] <INSTRUCTION>[(<params>)] |=>
STACK(node-value, hash-value), ...
```

There MUST be one and only one substitution rule applicable to the execution state. If an instruction has multiple substitution rules, the applicability is defined by the `GUARD` statements.
The substitution rule MAY have one or more GUARD statements.
The substitution rule MAY have one or more STACK statements before the instruction.
The substitution rule MUST have exactly one instruction.
The substitution rule MAY have parameters for the instruction.
The substitution rule MUST have at least one STACK statement after the arrow.

So, the minimal substitution rule is for the `HASH` instruction that pushes one hash to the stack:
```
HASH(hashValue) |=> STACK(hashNode(hashValue), hashValue)
```

## GUARDs

Each substitution rule can have zero, one or multiple `GUARD` statements.
Each `GUARD` statement looks like this:

```
GUARD <CONDITION>
```

That means that for the substitution rule to be applicable the `<CONDITION>` in the guard statement must be true.

If a substitution rule has multiple guard statements, all of the conditions specified there should be satisfied.

## `TRAP` statements

If no substitution rules are applicable for an instruction, then the execution MUST stop (`TRAP` instruction) and the partial results MUST NOT be used.

Implementation of the `TRAP` instruction is undefined, but it should stop the execution flow. (e.g. in Golang it might be a function returning an error or a panic).

## Instructions & Parameters

Each instruction MAY have one or more parameters.
The parameters values MUST be situated in the witness.
The parameter values MUST NOT be taken from the stack.

That makes it different from the helper function parameters that MAY come from the stack or MAY come from the witness.

## Helper functions

Helper functions are functions that are used in GUARDs or substitution rules.

Helper functions MUST be pure.
Helper functions MUST have at least one argument.
Helper functions MAY have variadic parameters: `HELPER_EXAMPLE(arg1, arg2, list...)`.
Helper functions MAY contain recursion.

## Data types

INTEGER - we treat the domain of integers as infinite, the overflow behaviour or mapping to the actual data types is undefined in this spec and should be dependent on implementation.

## Execution flow 

Let's look at the example.

Our example witness would be `HASH h1; HASH h2; BRANCH 0b11`.

Initial state of the stack is ` <empty> `;

---

**1. Executing `HASH h1`**: push a `hashNode` to the stack.

Witness: `HASH h2; BRANCH 0b11`

Stack: `(hashNode(h1), h1)`

---

**2. Executing `HASH h2`**: push one more `hashNode` to the stack.

Witness `BRANCH 0b11`

Stack: `(hashNode(h2), h2); (hashNode(h1), h1)`

---

**3. Executing `BRANCH 0b11`**: replace 2 items on the stack with a single `fullNode`.

Witness: ` <empty> `

Stack: `(branchNode{0: hashNode(h2), 1: hashNode(h1)}, KECCAK(h2+h1))`

---

So our stack has exactly a single item and there are no more instructions in the witness, the execution is completed.

## Modes

There are two modes of execution for this stack machine:

(1) **normal execution** -- the mode that constructs a trie;

(2) **hash only execution** -- the mode that calculates the root hashes of a trie without constructing the trie itself;

In the mode (2), a stack item  MUST only contain the node hash.

Stack item, mode (1): `(node, hash)`.

Stack item, mode (2): `(hash)`.

The exact implementation details are undefined in this spec.

## Instructions

### `LEAF key raw_value`

**Substitution rules**

```
LEAF(key, raw_value) |=> 
STACK(shortNode(key, valueNode(raw_value), VALUE_HASH(key, raw_value)))
```

### `EXTENSION key`

**Substitution rules**

```
GUARD node != nil

STACK(node, hash) EXTENSION(key) |=>
STACK(shortNode(key, node), hash)

---

GUARD node == nil

STACK(nil, hash) EXTENSION(key) |=>
STACK(shortNode(key, hashNode(hash)), hash)
```

### `HASH raw_hash`

Pushes a `hashNode` to stack.

**Substitution rules**

```
HASH(raw_hash) |=>
STACK(hashNode(raw_hash), raw_hash)
```

### `CODE raw_code`

Pushes an nil node + the code hash to the stack.

```
CODE(raw_code) |=>
STACK(nil, RLP(KECCAK(raw_code)))
```

### `ACCOUNT_LEAF key nonce balance has_code has_storage`

**Substitution rules**

```

GUARD has_code == true
GUARD storage_node == nil
GUARD has_storage == true

STACK(nil, code_hash) STACK(nil, storage_hash) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(shortNode(key, accountNode(account(nonce, balance, hashNode(storage_hash), storage_hash, code_hash))), ACC_HASH(key, account))
--

GUARD has_code == true
GUARD storage_node != nil
GUARD has_storage == true

STACK(nil, code_hash) STACK(storage_node, storage_hash) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(shortNode(key, accountNode(account(nonce, balance, storage_node, storage_hash, code_hash))), ACC_HASH(key, account))

--

GUARD has_code == false
GUARD storage_node != nil
GUARD has_storage == true

STACK(storage_node, storage_hash) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(shortNode(key, accountNode(account(nonce, balance, storage_node, storage_hash, nil))), ACC_HASH(key, account))

---

GUARD has_code == false
GUARD storage_node == nil
GUARD has_storage == true

STACK(nil, storage_hash) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(shortNode(key, accountNode(account(nonce, balance, storage_node, storage_hash, nil))), ACC_HASH(key, account))

--

GUARD has_code == true
GUARD has_storage == false

STACK(nil, code_hash) ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(shortNode(key, accountNode(account(nonce, balance, nil, nil, code_hash))), ACC_HASH(key, account))

--

GUARD has_code == false
GUARD has_storage == false

ACCOUNT_LEAF(key, nonce, balance, has_code, has_storage) |=>
STACK(shortNode(key, accountNode(account(nonce, balance, nil, nil, nil))), ACC_HASH(key, account))

```

### `EMPTY_ROOT`

Pushes an empty node + an empty hash to the stack

**Substitution rules**

```
EMPTY_ROOT |=>
STACK(nil, RLP(0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421))
```

### `NEW_TRIE`

Stops the witness execution.

### `BRANCH mask`

This instruction pops `NBITSET(mask)` items from both node stack and hash stack (up to 16 for each one). Then it pushes a new branch node on the node stack that has children according to the stack; it also pushes a new hash to the hash stack.

**Substitution rules**
```

GUARD NBITSET(mask) == 2 

STACK(n0, h0) STACK(n1, h1) BRANCH(mask) |=> 
STACK(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1)), keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1))))

---

GUARD NBITSET(mask) == 3

STACK(n0, h0) STACK(n1, h1) STACK(n2, h2) BRANCH(mask) |=> 
STACK(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1, n2)), keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1, n2))))

---

...

---

GUARD NBITSET(mask) == 16

STACK(n0, h0) STACK(n1, h1) ... STACK(n15, h15) BRANCH(mask) |=>
STACK(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1, ..., n15)), keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1, ..., n15))))
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