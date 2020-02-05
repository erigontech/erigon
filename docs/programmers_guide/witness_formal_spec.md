# Block Witness Formal Specification

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://tools.ietf.org/html/rfc2119).

## The Stack

Witnesses are executed on a stack machine that builds tries/calculates hashes.

The stack consists of pairs `(node, hash)`.

Each witness is a queue of instructions.
In every execution cycle one instruction gets dequeued and a matching substitution rule gets applied to the stack.

In the end, when there are no more instructions left, there should be only one item on the stack.  

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

## Guards & Traps

Each instruction and each helper function can have zero, one or multiple `guard` statements.
Each `guard` statement looks like this:

```
guard <CONDITION> else {
    TRAP
}
```

That means that for this instruction to be valid the `<CONDITION>` in the guard statement must be true.

If the condition is falsey, then the execution should stop (`TRAP` instruction). Implementation of the `TRAP` instruction is undefined, but it should stop the execution flow. (e.g. in Golang it might be a function returning an error or a panic).

If an instruction has multiple guard statements, all of the conditions specified there should be satisfied.

## Instructions & Parameters

Each instruction can have zero, one or multiple parameters. The values of these parameters are stored in the witness itself and decoded together with the instruction. They aren't taken from the stack.

That makes it different from the helper function parameters that can come from either stack or the witness itself.

## Helper functions

Helper functions are pure functions that are used in the substitution rules.
They can have zero, one or multiple arguments.

They also can have variadic parameters: `HELPER_EXAMPLE(arg1, arg2, list...)`.

The helper functions can be recursive but MUST be pure.

## Data types

In this document we treat numeric types (INT, FLOAT) as infinite and will not describe the overflow handling.

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

Stack: `(fullNode{0: hashNode(h2), 1: hashNode(h1)}, KECCAK(h2+h1))`

---

So our stack has exactly a single item and there are no more instructions in the witness, the execution is completed.

## Modes

There are two modes of execution for this stack machine:

(1) **normal execution** -- the mode that constructs a trie;

(2) **hash only execution** -- the mode that calculates the root hashe of a trie without constructing the tries itself;

In the mode (2), the first part of the pair `(node, hash)` is not used and always `nil`: `(nil, hash)`.

## Instructions

### `BRANCH mask`

This instruction pops `NBITSET(mask)` items from both node stack and hash stack (up to 16 for each one). Then it pushes a new branch node on the node stack that has children according to the stack; it also pushes a new hash to the hash stack.

**Guards**

```
guard NBITSET(mask) == LEN(values) else {
    TRAP
}
```

**Substitution rules**
```

STACK(n0, h0) STACK(n1, h1) BRANCH(mask) |=> 
STACK(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1)), keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1))))
---

STACK(n0, h0) STACK(n1, h1) STACK(n2, h2) BRANCH(mask) |=> 
STACK(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1, n2)), keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1, n2))))

---

...

---

STACK(n0, h0) STACK(n1, h1) ... STACK(n15, h15) BRANCH(mask) |=>
STACK(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1, ..., n15)), keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1, ..., n15))))
```

## Helper functions

### `MAKE_VALUES_ARRAY`

returns an array of 16 elements, where values from `values` are set to the indices where `mask` has bits set to 1. Every other place has `nil` value there.

**Example**: `MAKE_VALUES_ARRAY(5, [a, b])` returns `[a, nil, b, nil, nil, ..., nil]` (binary representation of 5 is `0000000000000101`)

```
MAKE_VALUES_ARRAY(mask uint16, values...) {
    return MAKE_VALUES_ARRAY(mask, 0, values)
}

MAKE_VALUES_ARRAY(mask uint16, idx uint, values...) {
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

### `NBITSET(number uint32)`

returns number of bits set in the binary representation of `number`.

### `BIT_TEST(number uint32, n uint)`

returns `true` if bit `n` in `number` is set, `false` otherwise.

### `PREPEND(value, array)`

returns a new array with the `value` at index 0 and `array` values starting from index 1

### `INC(value uint)`

increments `value` by 1