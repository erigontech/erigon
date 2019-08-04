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
	"bytes"
	"math/bits"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/valyala/bytebufferpool"

	"github.com/ledgerwatch/turbo-geth/common"
	"golang.org/x/crypto/sha3"
)

// Experimental code for separating data and structural information

// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type emitter interface {
	branch(digit int)
	hasher(digit int)
	leaf(length int)
	extension(key []byte)
	add(digit int)
	hash()
}

// step of the algoritm that converts sequence of keys into structural information
// DESCRIBED: docs/programmers_guide/guide.md#generating-the-structural-information-from-the-sequence-of-keys
func step(hashOnly func(prefix []byte) bool, recursive bool, prec, curr, succ []byte, e emitter, groups uint64) uint64 {
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
	//fmt.Printf("prec: %x, curr: %x, succ: %x, maxLen %d\n", prec, curr, succ, maxLen)
	var existed bool
	if succLen == precLen {
		// We don't know if this is the beginning of the new prefix group, or continuation of the existing one, so we check
		existed = groups&(uint64(1)<<uint(maxLen)) != 0
	} else {
		existed = precLen > succLen
	}
	if !existed {
		groups |= (uint64(1) << uint(maxLen))
	}
	remainderStart := maxLen
	if len(succ) > 0 || prec != nil {
		remainderStart++
	}
	remainderLen := len(curr) - remainderStart
	// Emit LEAF or EXTENSION based on the remainder
	if recursive {
		if remainderLen > 0 {
			e.extension(curr[remainderStart : remainderStart+remainderLen])
		}
	} else {
		e.leaf(remainderLen)
	}
	// Emit BRANCH or HASHER, or ADD
	// Add the digit immediately following the max common prefix and compute length of remainder length
	extraDigit := curr[maxLen]
	if existed {
		e.add(int(extraDigit))
	} else if len(succ) > 0 || prec != nil {
		if hashOnly(curr[:maxLen]) {
			e.hasher(int(extraDigit))
		} else {
			e.branch(int(extraDigit))
		}
	}
	// Check for the optional part
	if precLen <= succLen {
		return groups
	}
	// Close the immediately encompassing prefix group, if needed
	groups &^= (uint64(1) << uint(63-bits.LeadingZeros64(groups)))
	// Check the end of recursion
	if precLen == 0 {
		return groups
	}
	// Identify preceeding key for the recursive invocation
	newCurr := curr[:precLen]
	var newPrec []byte
	if groups != 0 {
		newPrec = curr[:63-bits.LeadingZeros64(groups)]
	}
	// Recursion
	return step(hashOnly, true, newPrec, newCurr, succ, e, groups)
}

type sortable [][]byte

func (s sortable) Len() int {
	return len(s)
}
func (s sortable) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}
func (s sortable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// ResolveSet encapsulates the set of keys that are required to be fully available, or resolved
// (by using `BRANCH` opcode instead of `HASHER`) after processing of the sequence of key-value
// pairs
// DESCRIBED: docs/programmers_guide/guide.md#converting-sequence-of-keys-and-value-into-a-multiproof
type ResolveSet struct {
	minLength int // Mininum length of prefixes for which `HashOnly` function can return `true`
	hexes     sortable
	inited    bool // Whether keys are sorted and "LTE" and "GT" indices set
	lteIndex  int  // Index of the "LTE" key in the keys slice. Next one is "GT"
}

// NewResolveSet creates new ResolveSet
func NewResolveSet(minLength int) *ResolveSet {
	return &ResolveSet{minLength: minLength}
}

// AddKey adds a new key to the set
func (rs *ResolveSet) AddKey(key []byte) {
	rs.hexes = append(rs.hexes, keybytesToHex(key))
}

func (rs *ResolveSet) AddHex(hex []byte) {
	rs.hexes = append(rs.hexes, hex)
}

func (rs *ResolveSet) ensureInited() {
	if rs.inited {
		return
	}
	sort.Sort(rs.hexes)
	rs.lteIndex = 0
	rs.inited = true
}

// HashOnly decides whether to emit `HASHER` or `BRANCH` for a given prefix, by
// checking if this is prefix of any of the keys added to the set
// Since keys in the set are sorted, and we expect that the prefixes will
// come in monotonically ascending order, we optimise for this, though
// the function would still work if the order is different
func (rs *ResolveSet) HashOnly(prefix []byte) bool {
	rs.ensureInited()
	if len(prefix) < rs.minLength {
		return false
	}
	// Adjust "GT" if necessary
	var gtAdjusted bool
	for rs.lteIndex < len(rs.hexes)-1 && bytes.Compare(rs.hexes[rs.lteIndex+1], prefix) <= 0 {
		rs.lteIndex++
		gtAdjusted = true
	}
	// Adjust "LTE" if necessary (normally will not be necessary)
	for !gtAdjusted && rs.lteIndex > 0 && bytes.Compare(rs.hexes[rs.lteIndex], prefix) > 0 {
		rs.lteIndex--
	}
	if rs.lteIndex < len(rs.hexes) && bytes.HasPrefix(rs.hexes[rs.lteIndex], prefix) {
		return false
	}
	if rs.lteIndex < len(rs.hexes)-1 && bytes.HasPrefix(rs.hexes[rs.lteIndex+1], prefix) {
		return false
	}
	return true
}

// HashBuilder impements the interface `emitter` and opcodes that the structural information of the trie
// is comprised of
// DESCRIBED: docs/programmers_guide/guide.md#separation-of-keys-and-the-structure
type HashBuilder struct {
	hexKey      bytes.Buffer // Next key-value pair to consume
	bufferStack []*bytes.Buffer
	digitStack  []int
	branchStack []*fullNode
	topKey      []byte
	topValue    *bytebufferpool.ByteBuffer
	topHash     common.Hash
	topBranch   *fullNode
	sha         keccakState
}

// NewHashBuilder creates new HashBuilder
func NewHashBuilder() *HashBuilder {
	return &HashBuilder{
		sha: sha3.NewLegacyKeccak256().(keccakState),
	}
}

func (hb *HashBuilder) Reset() {
	hb.hexKey.Reset()
	hb.digitStack = hb.digitStack[:0]
	hb.bufferStack = hb.bufferStack[:0]
	hb.branchStack = hb.branchStack[:0]
	hb.topKey = nil

	pool.PutBuffer(hb.topValue)
	hb.topValue = nil

	hb.topBranch = nil
}

// key is original key (not transformed into hex or compacted)
func (hb *HashBuilder) setKeyValue(skip int, key []byte, value *bytebufferpool.ByteBuffer) {
	// Transform key into hex representation
	hb.hexKey.Reset()
	i := 0
	for _, b := range key {
		if i >= skip {
			hb.hexKey.WriteByte(b / 16)
		}
		i++
		if i >= skip {
			hb.hexKey.WriteByte(b % 16)
		}
		i++
	}
	hb.hexKey.WriteByte(16)

	pool.PutBuffer(hb.topValue)
	hb.topValue = value
}

func (hb *HashBuilder) branch(digit int) {
	//fmt.Printf("BRANCH %d\n", digit)
	f := &fullNode{}
	f.flags.dirty = true
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			n := hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
			hb.branchStack[len(hb.branchStack)-1] = f
		} else {
			// Finalise existing hasher
			hn, _ := hb.finaliseHasher()
			f.Children[digit] = hashNode(hn[:])
			hb.branchStack = append(hb.branchStack, f)
		}
	} else {
		f.Children[digit] = hb.shortNode()
		hb.topKey = nil

		pool.PutBuffer(hb.topValue)
		hb.topValue = nil

		hb.topBranch = nil
		hb.branchStack = append(hb.branchStack, f)
	}
}

func generateStructLen(buffer []byte, l int) int {
	if l < 56 {
		buffer[0] = byte(192 + l)
		return 1
	}
	if l < 256 {
		// l can be encoded as 1 byte
		buffer[1] = byte(l)
		buffer[0] = byte(247 + 1)
		return 2
	}
	if l < 65536 {
		buffer[2] = byte(l & 255)
		buffer[1] = byte(l >> 8)
		buffer[0] = byte(247 + 2)
		return 3
	}
	buffer[3] = byte(l & 255)
	buffer[2] = byte((l >> 8) & 255)
	buffer[1] = byte(l >> 16)
	buffer[0] = byte(247 + 3)
	return 4
}

func (hb *HashBuilder) addLeafToHasher(buffer *bytes.Buffer) error {
	// Compute the total length of binary representation
	var keyPrefix [1]byte
	var valPrefix [4]byte
	var kp, vp, kl, vl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(hb.topKey) {
		compactLen = (len(hb.topKey)-1)/2 + 1
		if len(hb.topKey)&1 == 0 {
			compact0 = 48 + hb.topKey[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(hb.topKey)/2 + 1
		if len(hb.topKey)&1 == 1 {
			compact0 = 16 + hb.topKey[0] // Odd (1<<4) + first nibble
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
	var v []byte
	if hb.topValue != nil {
		if hb.topValue.Len() > 1 || hb.topValue.B[0] >= 128 {
			vp = generateByteArrayLen(valPrefix[:], 0, hb.topValue.Len())
			vl = hb.topValue.Len()
		} else {
			vl = 1
		}
		v = hb.topValue.B
	} else if hb.topBranch != nil {
		panic("")
	} else {
		valPrefix[0] = 128 + 32
		vp = 1
		vl = 32
		v = hb.topHash[:]
	}
	totalLen := kp + kl + vp + vl
	var lenPrefix [4]byte
	pt := generateStructLen(lenPrefix[:], totalLen)
	if pt+totalLen < 32 {
		// Embedded node
		buffer.Write(lenPrefix[:pt])
		buffer.Write(keyPrefix[:kp])
		if err := buffer.WriteByte(compact0); err != nil {
			return err
		}
		for i := 1; i < compactLen; i++ {
			if err := buffer.WriteByte(hb.topKey[ni]*16 + hb.topKey[ni+1]); err != nil {
				return err
			}
			ni += 2
		}
		buffer.Write(valPrefix[:vp])
		buffer.Write(v)
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
			b[0] = hb.topKey[ni]*16 + hb.topKey[ni+1]
			if _, err := hb.sha.Write(b[:]); err != nil {
				return err
			}
			ni += 2
		}
		if _, err := hb.sha.Write(valPrefix[:vp]); err != nil {
			return err
		}
		if _, err := hb.sha.Write(v); err != nil {
			return err
		}
		var hn common.Hash
		if _, err := hb.sha.Read(hn[:]); err != nil {
			return err
		}
		if err := buffer.WriteByte(128 + 32); err != nil {
			return err
		}
		if _, err := buffer.Write(hn[:]); err != nil {
			return err
		}
	}
	return nil
}

func (hb *HashBuilder) finaliseHasher() (common.Hash, error) {
	prevDigit := hb.digitStack[len(hb.digitStack)-1]
	hb.digitStack = hb.digitStack[:len(hb.digitStack)-1]
	prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
	hb.bufferStack = hb.bufferStack[:len(hb.bufferStack)-1]
	for i := prevDigit + 1; i < 17; i++ {
		prevBuffer.WriteByte(128)
	}
	var lenPrefix [4]byte
	pt := generateStructLen(lenPrefix[:], prevBuffer.Len())
	hb.sha.Reset()
	if _, err := hb.sha.Write(lenPrefix[:pt]); err != nil {
		return common.Hash{}, err
	} else {
		//fmt.Printf("%x", lenPrefix[:pt])
	}
	if _, err := hb.sha.Write(prevBuffer.Bytes()); err != nil {
		return common.Hash{}, err
	} else {
		//fmt.Printf("%x", prevBuffer.Bytes())
	}
	//fmt.Printf("\n")
	var hn common.Hash
	if _, err := hb.sha.Read(hn[:]); err != nil {
		return common.Hash{}, err
	}
	return hn, nil
}

func (hb *HashBuilder) hasher(digit int) {
	//fmt.Printf("HASHER %d\n", digit)
	var buffer bytes.Buffer
	for i := 0; i < digit; i++ {
		buffer.WriteByte(128) // Empty array
	}
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			panic("")
		} else {
			hn, _ := hb.finaliseHasher()
			buffer.WriteByte(128 + 32)
			buffer.Write(hn[:])
		}
	} else {
		if err := hb.addLeafToHasher(&buffer); err != nil {
			panic(err)
		}
	}
	hb.bufferStack = append(hb.bufferStack, &buffer)
	hb.topKey = nil

	pool.PutBuffer(hb.topValue)
	hb.topValue = nil

	hb.topBranch = nil
	hb.digitStack = append(hb.digitStack, digit)
}

func (hb *HashBuilder) leaf(length int) {
	hex := hb.hexKey.Bytes()
	//fmt.Printf("LEAF %d\n", length)
	hb.topKey = hex[len(hex)-length:]
	hb.topBranch = nil
}

func (hb *HashBuilder) extension(key []byte) {
	//fmt.Printf("EXTENSION %x\n", key)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
		hb.topBranch = f
	} else {
		hn, _ := hb.finaliseHasher()
		hb.topHash = hn
		hb.topBranch = nil
	}
	hb.topKey = key

	pool.PutBuffer(hb.topValue)
	hb.topValue = nil
}

func (hb *HashBuilder) shortNode() *shortNode {
	if hb.topValue != nil {
		return &shortNode{Key: hexToCompact(hb.topKey), Val: valueNode(common.CopyBytes(hb.topValue.B))}
	} else if hb.topBranch != nil {
		return &shortNode{Key: hexToCompact(hb.topKey), Val: hb.topBranch}
	}
	return &shortNode{Key: hexToCompact(hb.topKey), Val: hashNode(common.CopyBytes(hb.topHash[:]))}
}

func (hb *HashBuilder) add(digit int) {
	//fmt.Printf("ADD %d\n", digit)
	if len(hb.bufferStack) == 0 {
		f := hb.branchStack[len(hb.branchStack)-1]
		if hb.topKey == nil {
			n := f
			hb.branchStack = hb.branchStack[:len(hb.branchStack)-1]
			f = hb.branchStack[len(hb.branchStack)-1]
			f.Children[digit] = n
		} else {
			f.Children[digit] = hb.shortNode()
			hb.topKey = nil

			pool.PutBuffer(hb.topValue)
			hb.topValue = nil

			hb.topBranch = nil
		}
	} else {
		prevBuffer := hb.bufferStack[len(hb.bufferStack)-1]
		prevDigit := hb.digitStack[len(hb.digitStack)-1]
		if hb.topKey == nil {
			hn, _ := hb.finaliseHasher()
			if len(hb.bufferStack) > 0 {
				prevBuffer = hb.bufferStack[len(hb.bufferStack)-1]
				prevDigit = hb.digitStack[len(hb.digitStack)-1]
				for i := prevDigit + 1; i < digit; i++ {
					prevBuffer.WriteByte(128)
				}
				prevBuffer.WriteByte(128 + 32)
				prevBuffer.Write(hn[:])
				hb.digitStack[len(hb.digitStack)-1] = digit
			} else {
				f := hb.branchStack[len(hb.branchStack)-1]
				f.Children[digit] = hashNode(hn[:])
			}
		} else {
			for i := prevDigit + 1; i < digit; i++ {
				prevBuffer.WriteByte(128)
			}
			if err := hb.addLeafToHasher(prevBuffer); err != nil {
				panic(err)
			}
			hb.digitStack[len(hb.digitStack)-1] = digit
			hb.topKey = nil

			pool.PutBuffer(hb.topValue)
			hb.topValue = nil

			hb.topBranch = nil
		}
	}
}

func (hb *HashBuilder) hash() {
	//fmt.Printf("HASH\n")
}

func (hb *HashBuilder) root() node {
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return nil
			}
			return hb.branchStack[len(hb.branchStack)-1]
		}
		hn, _ := hb.finaliseHasher()
		return hashNode(hn[:])
	}
	return hb.shortNode()
}

func (hb *HashBuilder) rootHash() common.Hash {
	var hn common.Hash
	if hb.topKey == nil {
		if len(hb.bufferStack) == 0 {
			if len(hb.branchStack) == 0 {
				return EmptyRoot
			}
			h := newHasher(false)
			defer returnHasherToPool(h)
			h.hash(hb.branchStack[len(hb.branchStack)-1], true, hn[:])
		} else {
			hn, _ = hb.finaliseHasher()
			return hn
		}
	} else {
		h := newHasher(false)
		defer returnHasherToPool(h)
		h.hash(hb.shortNode(), true, hn[:])
	}
	return hn
}
