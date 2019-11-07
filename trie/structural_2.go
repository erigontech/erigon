// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty off
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"fmt"
	"math/big"
	"math/bits"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"golang.org/x/crypto/sha3"
)

// Experimental code for separating data and structural information
// Each function corresponds to an opcode
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type structInfoReceiver interface {
	leaf(length int) error
	leafHash(length int) error
	accountLeaf(length int, fieldset uint32) error
	accountLeafHash(length int, fieldset uint32) error
	extension(key []byte) error
	extensionHash(key []byte) error
	branch(set uint16) error
	branchHash(set uint16) error
	hash(number int) error
}

// GenStructStep is one step of the algorithm that generates the structural information based on the sequence of keys.
// `fieldSet` parameter specifies whether the generated leaf should be a binary string (fieldSet==0), or
// an account (in that case the opcodes `ACCOUNTLEAF`/`ACCOUNTLEAFHASH` are emitted instead of `LEAF`/`LEAFHASH`).
// `hashOnly` parameter is the function that, called for a certain prefix, determines whether the trie node for that prefix needs to be
// compressed into just hash (if `true` is returned), or constructed (if `false` is returned). Usually the `hashOnly` function is
// implemented in such a way to guarantee that certain keys are always accessible in the resulting trie (see ResolveSet.HashOnly function).
// `recursive` parameter is set to true if the algorithm's step is invoked recursively, i.e. not after a freshly provided leaf.
// Recursive invocation is used to emit opcodes for non-leaf nodes.
// `prec`, `curr`, `succ` are three full keys or prefixes that are currently visible to the algorithm. By comparing these, the algorithm
// makes decisions about the local structure, i.e. the presense of the prefix groups.
// `e` parameter is the trie builder, which uses the structure information to assemble trie on the stack and compute its hash.
// `groups` parameter is the map of the stack. each element of the `groups` slice is a bitmask, one bit per element currently on the stack.
// Whenever a `BRANCH` or `BRANCHHASH` opcode is emitted, the set of digits is taken from the corresponding `groups` item, which is
// then removed from the slice. This signifies the usage of the number of the stack items by the `BRANCH` or `BRANCHHASH` opcode.
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
func GenStructStep(
	fieldSet uint32,
	hashOnly func(prefix []byte) bool,
	recursive bool,
	prec, curr, succ []byte,
	e structInfoReceiver,
	groups []uint16,
) ([]uint16, error) {
	if !recursive && len(prec) == 0 {
		prec = nil
	}
	// Calculate the prefix of the smallest prefix group containing curr
	precLen := prefixLen(prec, curr)
	succLen := prefixLen(succ, curr)
	var maxLen int
	if precLen > succLen {
		maxLen = precLen
	} else {
		maxLen = succLen
	}
	//fmt.Printf("prec: %x, curr: %x, succ: %x, maxLen %d, prefix: %x\n", prec, curr, succ, maxLen, prefix)
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	for maxLen >= len(groups) {
		groups = append(groups, 0)
	}
	groups[maxLen] |= (uint16(1) << extraDigit)
	//fmt.Printf("groups[%d] is now %b, len(groups) %d, prefix %x\n", maxLen, groups[maxLen], len(groups), prefix)
	remainderStart := maxLen
	if len(succ) > 0 || prec != nil {
		remainderStart++
	}
	remainderLen := len(curr) - remainderStart
	// Emit LEAF or EXTENSION based on the remainder
	if recursive {
		if remainderLen > 0 {
			if hashOnly(curr[:maxLen]) {
				e.extensionHash(curr[remainderStart : remainderStart+remainderLen])
			} else {
				e.extension(curr[remainderStart : remainderStart+remainderLen])
			}
		}
	} else {
		if hashOnly(curr[:maxLen]) {
			if fieldSet == 0 {
				if err := e.leafHash(remainderLen); err != nil {
					return nil, err
				}
			} else {
				if err := e.accountLeafHash(remainderLen, fieldSet); err != nil {
					return nil, err
				}
			}
		} else {
			if fieldSet == 0 {
				if err := e.leaf(remainderLen); err != nil {
					return nil, err
				}
			} else {
				if err := e.accountLeaf(remainderLen, fieldSet); err != nil {
					return nil, err
				}
			}
		}
	}
	// Check for the optional part
	if precLen <= succLen && len(succ) > 0 {
		return groups, nil
	}
	// Close the immediately encompassing prefix group, if needed
	if len(succ) > 0 || prec != nil {
		if hashOnly(curr[:maxLen]) {
			e.branchHash(groups[maxLen])
		} else {
			e.branch(groups[maxLen])
		}
	}
	groups = groups[:maxLen]
	// Check the end of recursion
	if precLen == 0 {
		return groups, nil
	}
	// Identify preceding key for the recursive invocation
	newCurr := curr[:precLen]
	var newPrec []byte
	for len(groups) > 0 && groups[len(groups)-1] == 0 {
		groups = groups[:len(groups)-1]
	}
	if len(groups) >= 1 {
		newPrec = curr[:len(groups)-1]
	}

	// Recursion
	return GenStructStep(fieldSet, hashOnly, true, newPrec, newCurr, succ, e, groups)
}

// BytesTape is an abstraction for an input tape that allows reading binary strings ([]byte) sequentially
// To be used for keys and binary string values
type BytesTape interface {
	// Returned slice is only valid until the next invocation of NextBytes()
	// i.e. the underlying array/slice may be shared between invocations
	Next() ([]byte, error)
}

// Uint64Tape is an abstraction for an input tape that allows reading unsigned 64-bit integers sequentially
// To be used for nonces of the accounts
type Uint64Tape interface {
	Next() (uint64, error)
}

// BigIntTape is an abstraction for an input tape that allows reading *big.Int values sequentially
// To be used for balances of the accounts
type BigIntTape interface {
	// Returned pointer is only valid until the next invocation of NextBitInt()
	// i.e. the underlying big.Int object may be shared between invocation
	Next() (*big.Int, error)
}

// HashTape is an abstraction for an input table that allows reading 32-byte hashes (common.Hash) sequentially
// To be used for intermediate hashes in the Patricia Merkle tree
type HashTape interface {
	Next() (common.Hash, error)
}

const hashStackStride = common.HashLength + 1 // + 1 byte for RLP encoding

// HashBuilder implements the interface `structInfoReceiver` and opcodes that the structural information of the trie
// is comprised of
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type HashBuilder struct {
	keyTape     BytesTape  // the source of key sequence
	valueTape   BytesTape  // the source of values (for values that are not accounts or contracts)
	nonceTape   Uint64Tape // the source of nonces for accounts and contracts (field 0)
	balanceTape BigIntTape // the source of balances for accounts and contracts (field 1)
	sSizeTape   Uint64Tape // the source of storage sizes for contracts (field 4)
	hashTape    HashTape   // the source of hashes
	codeTape    BytesTape  // the source of bytecodes

	hashStack []byte           // Stack of sub-slices, each 33 bytes each, containing RLP encodings of node hashes (or of nodes themselves, if shorter than 32 bytes)
	nodeStack []node           // Stack of nodes
	acc       accounts.Account // Working account instance (to avoid extra allocations)
	sha       keccakState      // Keccak primitive that can absorb data (Write), and get squeezed to the hash out (Read)
}

// NewHashBuilder creates a new HashBuilder
func NewHashBuilder() *HashBuilder {
	return &HashBuilder{
		sha: sha3.NewLegacyKeccak256().(keccakState),
	}
}

// SetKeyTape sets the key tape to be used by this builder (opcodes leaf, leafHash, accountLeaf, accountLeafHash)
func (hb *HashBuilder) SetKeyTape(keyTape BytesTape) {
	hb.keyTape = keyTape
}

// SetValueTape sets the value tape to be used by this builder (opcodes leaf and leafHash)
func (hb *HashBuilder) SetValueTape(valueTape BytesTape) {
	hb.valueTape = valueTape
}

// SetNonceTape sets the nonce tape to be used by this builder (opcodes accountLeaf, accountLeafHash)
func (hb *HashBuilder) SetNonceTape(nonceTape Uint64Tape) {
	hb.nonceTape = nonceTape
}

// SetBalanceTape sets the balance tape to be used by this builder (opcodes accountLeaf, accountLeafHash)
func (hb *HashBuilder) SetBalanceTape(balanceTape BigIntTape) {
	hb.balanceTape = balanceTape
}

// SetHashTape sets the hash tape to be used by this builder (opcode hash)
func (hb *HashBuilder) SetHashTape(hashTape HashTape) {
	hb.hashTape = hashTape
}

// SetSSizeTape sets the storage size tape to be used by this builder (opcodes accountLeaf, accountLeafHashs)
func (hb *HashBuilder) SetSSizeTape(sSizeTape Uint64Tape) {
	hb.sSizeTape = sSizeTape
}

// SetCodeTape sets the code tape to be used by this builder (opcode CODE)
func (hb *HashBuilder) SetCodeTape(codeTape BytesTape) {
	hb.codeTape = codeTape
}

// Reset makes the HashBuilder suitable for reuse
func (hb *HashBuilder) Reset() {
	hb.hashStack = hb.hashStack[:0]
	hb.nodeStack = hb.nodeStack[:0]
}

func (hb *HashBuilder) leaf(length int) error {
	//fmt.Printf("LEAF %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	val, err := hb.valueTape.Next()
	if err != nil {
		return err
	}
	s := &shortNode{Key: common.CopyBytes(key), Val: valueNode(common.CopyBytes(val))}
	hb.nodeStack = append(hb.nodeStack, s)
	return hb.leafHashWithKeyVal(key, val)
}

// To be called internally
func (hb *HashBuilder) leafHashWithKeyVal(key, val []byte) error {
	var hash [hashStackStride]byte // RLP representation of hash (or of un-hashed value if short)
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var lenPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0] // Odd: (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = rlp.EmptyStringCode + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	if len(val) > 1 || val[0] >= rlp.EmptyStringCode {
		vp = generateByteArrayLen(valPrefix[:], 0, len(val))
		vl = len(val)
	} else {
		vl = 1
	}
	totalLen := kp + kl + vp + vl
	pt := generateStructLen(lenPrefix[:], totalLen)
	if pt+totalLen < common.HashLength {
		// Embedded node
		pos := 0
		copy(hash[pos:], lenPrefix[:pt])
		pos += pt
		copy(hash[pos:], keyPrefix[:kp])
		pos += kp
		hash[pos] = compact0
		pos++
		for i := 1; i < compactLen; i++ {
			hash[pos] = key[ni]*16 + key[ni+1]
			pos++
			ni += 2
		}
		copy(hash[pos:], valPrefix[:vp])
		pos += vp
		copy(hash[pos:], val)
	} else {
		hb.sha.Reset()
		if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
			return err
		}
		var b [1]byte
		b[0] = compact0
		if _, err := hb.sha.Write(b[:]); err != nil {
			return err
		}
		for i := 1; i < compactLen; i++ {
			b[0] = key[ni]*16 + key[ni+1]
			if _, err := hb.sha.Write(b[:]); err != nil {
				return err
			}
			ni += 2
		}
		if _, err := hb.sha.Write(valPrefix[:vp]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(val); err != nil {
			return err
		}
		hash[0] = rlp.EmptyStringCode + common.HashLength
		if _, err := hb.sha.Read(hash[1:]); err != nil {
			return err
		}
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	if len(hb.hashStack) > hashStackStride*len(hb.nodeStack) {
		hb.nodeStack = append(hb.nodeStack, nil)
	}
	return nil
}

func (hb *HashBuilder) leafHash(length int) error {
	//fmt.Printf("LEAFHASH %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	val, err := hb.valueTape.Next()
	if err != nil {
		return err
	}
	return hb.leafHashWithKeyVal(key, val)
}

var EmptyCodeHash = crypto.Keccak256Hash(nil)

func (hb *HashBuilder) accountLeaf(length int, fieldSet uint32) error {
	//fmt.Printf("ACCOUNTLEAF %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	hb.acc.Root = EmptyRoot
	hb.acc.CodeHash = EmptyCodeHash
	hb.acc.Nonce = 0
	hb.acc.Balance.SetUint64(0)
	hb.acc.Initialised = true
	hb.acc.StorageSize = 0
	hb.acc.HasStorageSize = false
	if fieldSet&uint32(1) != 0 {
		if hb.acc.Nonce, err = hb.nonceTape.Next(); err != nil {
			return err
		}
	}
	if fieldSet&uint32(2) != 0 {
		var balance *big.Int
		if balance, err = hb.balanceTape.Next(); err != nil {
			return err
		}
		hb.acc.Balance.Set(balance)
	}
	popped := 0
	var root node
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		if hb.acc.Root != EmptyRoot {
			// Root is on top of the stack
			root = hb.nodeStack[len(hb.nodeStack)-popped-1]
			if root == nil {
				root = hashNode(common.CopyBytes(hb.acc.Root[:]))
			}
		}
		popped++
	}
	if fieldSet&uint32(8) != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		popped++
	}
	if fieldSet&uint32(16) != 0 {
		if hb.acc.StorageSize, err = hb.sSizeTape.Next(); err != nil {
			return err
		}
		hb.acc.HasStorageSize = hb.acc.StorageSize > 0
	}
	var accCopy accounts.Account
	accCopy.Copy(&hb.acc)
	s := &shortNode{Key: common.CopyBytes(key), Val: &accountNode{accCopy, root, true}}
	// this invocation will take care of the popping given number of items from both hash stack and node stack,
	// pushing resulting hash to the hash stack, and nil to the node stack
	if err = hb.accountLeafHashWithKey(key, popped); err != nil {
		return err
	}
	// Replace top of the stack
	hb.nodeStack[len(hb.nodeStack)-1] = s
	return nil
}

func (hb *HashBuilder) accountLeafHash(length int, fieldSet uint32) error {
	//fmt.Printf("ACCOUNTLEAFHASH %d\n", length)
	hex, err := hb.keyTape.Next()
	if err != nil {
		return err
	}
	key := hex[len(hex)-length:]
	hb.acc.Root = EmptyRoot
	hb.acc.CodeHash = EmptyCodeHash
	hb.acc.Nonce = 0
	hb.acc.Balance.SetUint64(0)
	hb.acc.Initialised = true
	hb.acc.StorageSize = 0
	hb.acc.HasStorageSize = false
	if fieldSet&uint32(1) != 0 {
		if hb.acc.Nonce, err = hb.nonceTape.Next(); err != nil {
			return err
		}
	}
	if fieldSet&uint32(2) != 0 {
		var balance *big.Int
		if balance, err = hb.balanceTape.Next(); err != nil {
			return err
		}
		hb.acc.Balance.Set(balance)
	}
	popped := 0
	if fieldSet&uint32(4) != 0 {
		copy(hb.acc.Root[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		popped++
	}
	if fieldSet&uint32(8) != 0 {
		copy(hb.acc.CodeHash[:], hb.hashStack[len(hb.hashStack)-popped*33-32:len(hb.hashStack)-popped*33])
		popped++
	}
	if fieldSet&uint32(16) != 0 {
		if hb.acc.StorageSize, err = hb.sSizeTape.Next(); err != nil {
			return err
		}
		hb.acc.HasStorageSize = hb.acc.StorageSize > 0
	}
	return hb.accountLeafHashWithKey(key, popped)
}

// To be called internally
func (hb *HashBuilder) accountLeafHashWithKey(key []byte, popped int) error {
	var hash [33]byte // RLP representation of hash (or un-hashes value)
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var lenPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 16 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = byte(128 + compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	valLen := hb.acc.EncodingLengthForHashing()
	valBuf := pool.GetBuffer(valLen)
	defer pool.PutBuffer(valBuf)
	hb.acc.EncodeForHashing(valBuf.B)
	val := valBuf.B
	if len(val) > 1 || val[0] >= 128 {
		vp = generateByteArrayLen(valPrefix[:], 0, len(val))
		vl = len(val)
	} else {
		vl = 1
	}
	totalLen := kp + kl + vp + vl
	pt := generateStructLen(lenPrefix[:], totalLen)
	if pt+totalLen < 32 {
		// Embedded node
		pos := 0
		copy(hash[pos:], lenPrefix[:pt])
		pos += pt
		copy(hash[pos:], keyPrefix[:kp])
		pos += kp
		hash[pos] = compact0
		pos++
		for i := 1; i < compactLen; i++ {
			hash[pos] = key[ni]*16 + key[ni+1]
			pos++
			ni += 2
		}
		copy(hash[pos:], valPrefix[:vp])
		pos += vp
		copy(hash[pos:], val)
	} else {
		hb.sha.Reset()
		if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
			return err
		}
		var b [1]byte
		b[0] = compact0
		if _, err := hb.sha.Write(b[:]); err != nil {
			return err
		}
		for i := 1; i < compactLen; i++ {
			b[0] = key[ni]*16 + key[ni+1]
			if _, err := hb.sha.Write(b[:]); err != nil {
				return err
			}
			ni += 2
		}
		if _, err := hb.sha.Write(valPrefix[:vp]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(val); err != nil {
			return err
		}
		hash[0] = byte(128 + 32)
		if _, err := hb.sha.Read(hash[1:]); err != nil {
			return err
		}
	}
	if popped > 0 {
		hb.hashStack = hb.hashStack[:len(hb.hashStack)-popped*33]
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-popped]
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	hb.nodeStack = append(hb.nodeStack, nil)
	return nil
}

func (hb *HashBuilder) extension(key []byte) error {
	//fmt.Printf("EXTENSION %x\n", key)
	nd := hb.nodeStack[len(hb.nodeStack)-1]
	switch n := nd.(type) {
	case nil:
		branchHash := common.CopyBytes(hb.hashStack[len(hb.hashStack)-common.HashLength:])
		hb.nodeStack[len(hb.nodeStack)-1] = &shortNode{Key: common.CopyBytes(key), Val: hashNode(branchHash)}
	case *fullNode:
		hb.nodeStack[len(hb.nodeStack)-1] = &shortNode{Key: common.CopyBytes(key), Val: n}
	default:
		return fmt.Errorf("wrong Val type for an extension: %T", nd)
	}
	return hb.extensionHash(key)
}

func (hb *HashBuilder) extensionHash(key []byte) error {
	//fmt.Printf("EXTENSIONHASH %x\n", key)
	branchHash := hb.hashStack[len(hb.hashStack)-hashStackStride:]
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var lenPrefix [4]byte
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	// https://github.com/ethereum/wiki/wiki/Patricia-Tree#specification-compact-encoding-of-hex-sequence-with-optional-terminator
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0] // Odd: (1<<4) + first nibble
			ni = 1
		}
	}
	if compactLen > 1 {
		keyPrefix[0] = rlp.EmptyStringCode + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	pt := generateStructLen(lenPrefix[:], totalLen)
	hb.sha.Reset()
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return err
	}
	if _, err := hb.sha.Write(keyPrefix[:kp]); err != nil {
		return err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := hb.sha.Write(b[:]); err != nil {
		return err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := hb.sha.Write(b[:]); err != nil {
			return err
		}
		ni += 2
	}
	if _, err := hb.sha.Write(branchHash); err != nil {
		return err
	}
	// Replace previous hash with the new one
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-common.HashLength:]); err != nil {
		return err
	}
	if _, ok := hb.nodeStack[len(hb.nodeStack)-1].(*fullNode); ok {
		return fmt.Errorf("extensionHash cannot be emitted when a node is on top of the stack")
	}
	return nil
}

func (hb *HashBuilder) branch(set uint16) error {
	//fmt.Printf("BRANCH %b\n", set)
	f := &fullNode{}
	digits := bits.OnesCount16(set)
	nodes := hb.nodeStack[len(hb.nodeStack)-digits:]
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if nodes[i] == nil {
				f.Children[digit] = hashNode(common.CopyBytes(hashes[hashStackStride*i+1 : hashStackStride*(i+1)]))
			} else {
				f.Children[digit] = nodes[i]
			}
			i++
		}
	}
	hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
	hb.nodeStack[len(hb.nodeStack)-1] = f
	if err := hb.branchHash(set); err != nil {
		return err
	}
	copy(f.flags.hash[:], hb.hashStack[len(hb.hashStack)-common.HashLength:])
	return nil
}

func (hb *HashBuilder) branchHash(set uint16) error {
	//fmt.Printf("BRANCHHASH %b\n", set)
	digits := bits.OnesCount16(set)
	hashes := hb.hashStack[len(hb.hashStack)-hashStackStride*digits:]
	// Calculate the size of the resulting RLP
	totalSize := 17 // These are 17 length prefixes
	var i int
	for digit := uint(0); digit < 16; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if hashes[hashStackStride*i] == rlp.EmptyStringCode+common.HashLength {
				totalSize += common.HashLength
			} else {
				// Embedded node
				totalSize += int(hashes[hashStackStride*i] - rlp.EmptyListCode)
			}
			i++
		}
	}
	hb.sha.Reset()
	var lenPrefix [4]byte
	pt := generateStructLen(lenPrefix[:], totalSize)
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return err
	}
	// Output children hashes or embedded RLPs
	i = 0
	var b [1]byte
	b[0] = rlp.EmptyStringCode
	for digit := uint(0); digit < 17; digit++ {
		if ((uint16(1) << digit) & set) != 0 {
			if hashes[hashStackStride*i] == byte(rlp.EmptyStringCode+common.HashLength) {
				if _, err := hb.sha.Write(hashes[hashStackStride*i : hashStackStride*i+hashStackStride]); err != nil {
					return err
				}
			} else {
				// Embedded node
				size := int(hashes[hashStackStride*i]) - rlp.EmptyListCode
				if _, err := hb.sha.Write(hashes[hashStackStride*i : hashStackStride*i+size+1]); err != nil {
					return err
				}
			}
			i++
		} else {
			if _, err := hb.sha.Write(b[:]); err != nil {
				return err
			}
		}
	}
	hb.hashStack = hb.hashStack[:len(hb.hashStack)-hashStackStride*digits+hashStackStride]
	hb.hashStack[len(hb.hashStack)-hashStackStride] = rlp.EmptyStringCode + common.HashLength
	if _, err := hb.sha.Read(hb.hashStack[len(hb.hashStack)-common.HashLength:]); err != nil {
		return err
	}
	if hashStackStride*len(hb.nodeStack) > len(hb.hashStack) {
		hb.nodeStack = hb.nodeStack[:len(hb.nodeStack)-digits+1]
		hb.nodeStack[len(hb.nodeStack)-1] = nil
	}
	return nil
}

func (hb *HashBuilder) hash(number int) error {
	for i := 0; i < number; i++ {
		hash, err := hb.hashTape.Next()
		if err != nil {
			return err
		}
		hb.hashStack = append(hb.hashStack, rlp.EmptyStringCode+common.HashLength)
		hb.hashStack = append(hb.hashStack, hash[:]...)
		hb.nodeStack = append(hb.nodeStack, nil)
	}
	return nil
}

func (hb *HashBuilder) code() ([]byte, common.Hash, error) {
	code, err := hb.codeTape.Next()
	if err != nil {
		return nil, common.Hash{}, err
	}
	code = common.CopyBytes(code)
	hb.nodeStack = append(hb.nodeStack, nil)
	hb.sha.Reset()
	if _, err := hb.sha.Write(code); err != nil {
		return nil, common.Hash{}, err
	}
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = rlp.EmptyStringCode + common.HashLength
	if _, err := hb.sha.Read(hash[1:]); err != nil {
		return nil, common.Hash{}, err
	}
	hb.hashStack = append(hb.hashStack, hash[:]...)
	return code, common.BytesToHash(hash[1:]), nil
}

func (hb *HashBuilder) emptyRoot() {
	hb.nodeStack = append(hb.nodeStack, nil)
	var hash [hashStackStride]byte // RLP representation of hash (or un-hashes value)
	hash[0] = rlp.EmptyStringCode + common.HashLength
	copy(hash[1:], EmptyRoot[:])
	hb.hashStack = append(hb.hashStack, hash[:]...)
}

func (hb *HashBuilder) RootHash() (common.Hash, error) {
	if !hb.hasRoot() {
		return common.Hash{}, fmt.Errorf("no root in the tree")
	}
	return hb.rootHash(), nil
}

func (hb *HashBuilder) rootHash() common.Hash {
	var hash common.Hash
	copy(hash[:], hb.hashStack[1:hashStackStride])
	return hash
}

func (hb *HashBuilder) root() node {
	return hb.nodeStack[0]
}

func (hb *HashBuilder) hasRoot() bool {
	return len(hb.nodeStack) > 0
}
