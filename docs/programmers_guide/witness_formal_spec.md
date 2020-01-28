# Block Witness Formal Specification

Block witness uses 2 stacks:
- node stack: `NODES`
- hashes stack: `HASHES`

That allows to calculate trie hashes w/o building the tries themselves.

## Instructions

### Substitution rules

a typical substitution rule shows what values on stacks are replaced (before an INSTRUCTION), to by which values (after an INSTRUCTION)

```
NODECONST(nodeValue), NODECONST(nodeValue2)   // pop nodes on the NODES stack
HASHCONST(hashValue),HASHCONST(hashValue2)    // pop nodes on the HASHES stack
INSTRUCTION(params) |=>                       // the current instruction with params
NODECONST(helperFunc(nodeValue, nodeValue2, params))    // push node(s) to the NODES stack
HASHCONST(heperFunc2(hashValue, hashValue2, params))    // push hash(es) to the HASHES stack
```

### `BRANCH mask`

This instruction pops `NBITSET(mask)` items from both node stack and hash stack (up to 16 for each one). Then it pushes a new branch node on the node stack that has children according to the stack; it also pushes a new hash to the hash stack.

**Substitution rules**
```
BRANCH(mask) |=>
NODECONST(branchNode())
HASHCONST(KECCAK())

---

NODECONST(n0)
HASHCONST(h0)
BRANCH(mask) |=>
NODECONST(branchNode(MAKE_VALUES_ARRAY(mask, n0))
HASHCONST(keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0)))

---

NODECONST(n0) NODECONST(n1)
HASHCONST(h0) HASHCONST(h1)
BRANCH(mask) |=>
NODECONST(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1))
HASHCONST(keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, n1)))

---

...

---

NODECONST(n0) NODECONST(n1) ... NODECONST(n15)
HASHCONST(h0) HASHCONST(h1) ... HASHCONST(h15)
BRANCH(mask) |=>
NODECONST(branchNode(MAKE_VALUES_ARRAY(mask, n0, n1, ..., n15))
HASHCONST(keccak(CONCAT(MAKE_VALUES_ARRAY(mask, h0, h1, ..., h15)))
```

## Helper functions

### `MAKE_VALUES_ARRAY`

returns an array of 16 elements, where values from `values` are set to the indices where `mask` has bits set to 1. Every other place has `nil` value there.

**Example**: `MAKE_VALUES_ARRAY(5, [a, b])` returns `[a, nil, b, nil, nil, ..., nil]` (binary representation of 5 is `0000000000000101`)

```
MAKE_VALUES_ARRAY(mask uint16, values...) {
    guard NBITSET(mask) == LEN(values) else {
        TRAP
    }
    return MAKE_VALUES_ARRAY(mask, 0, values)
}

MAKE_VALUES_ARRAY(mask uint16, idx uint, values...) {
    guard idx <= 16 else {
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

increments `value` by 1, undefined behaviour on overflows