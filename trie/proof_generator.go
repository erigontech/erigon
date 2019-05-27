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

// Generation of block proofs for stateless clients

package trie

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ugorji/go/codec"
)

// Tape represents the sequence of values that is getting serialised using CBOR into a byte buffer
type Tape struct {
	buffer  bytes.Buffer     // Byte buffer where the CBOR-encoded values end up being written
	handle  codec.CborHandle // Object used to control the behavior of CBOR encoding
	encoder *codec.Encoder   // Values are supplied to this object (via its Encode function)
}

// init allocates a new encoder, binding it to the buffer and the handle
func (t *Tape) init() {
	t.encoder = codec.NewEncoder(&t.buffer, &t.handle)
}

// BlockWitnessBuilder accumulates data that can later be turned into a serialised
// version of the block witness
// All buffers are streams of CBOR-encoded items (not a CBOR array, but individual items back-to-back)
// `Keys` are binary strings
// `Values` are either binary strings or arrays of structures
// {nonce - integer, balance - integer, optionally [root hash - binary string, code hash - binary string]}
// `Hashes` are binary strings, all of size 32
// `Codes` are binary strings
// `Structure` are integers (for opcodes themselves), potentially followed by binary strings (key for EXTENSION) or
// integers (bitmaps for BRANCH or length of LEAF or number of hashes for HASH)
type BlockWitnessBuilder struct {
	Keys      Tape // Sequence of keys that are consumed by LEAF, LEAFHASH, CONTRACTLEAF, and CONTRACTLEAFHASH opcodes
	Values    Tape // Sequence of values that are consumed by LEAF, LEAFHASH, CONTRACTLEAF, and CONTRACTLEAFHASH opcodes
	Hashes    Tape // Sequence of hashes that are consumed by the HASH opcode
	Codes     Tape // Sequence of contract codes that are consumed by the CODE opcode
	Structure Tape // Sequence of opcodes and operands that define the structure of the witness
}

type Instruction uint8

const (
	OpLeaf Instruction = iota
	OpLeafHash
	OpExtension
	OpExtensionHash
	OpBranch
	OpBranchHash
	OpHash
	OpCode
	OpCodeHash
	OpContractLeaf
	OpContractLeafHash
	OpEmptyRoot
)

// NewBlockWitnessBuilder creates an initialised block witness builder ready for use
func NewBlockWitnessBuilder() *BlockWitnessBuilder {
	var bwb BlockWitnessBuilder
	bwb.Keys.init()
	bwb.Values.init()
	bwb.Hashes.init()
	bwb.Codes.init()
	bwb.Structure.init()
	return &bwb
}

// keyValue supplies the next key-value pair for the leaf tape
func (bwb *BlockWitnessBuilder) keyValue(key, value []byte) error {
	if err := bwb.Keys.encoder.Encode(key); err != nil {
		return err
	}
	if err := bwb.Values.encoder.Encode(value); err != nil {
		return err
	}
	return nil
}

func (bwb *BlockWitnessBuilder) leaf(length int) error {
	o := OpLeaf
	if err := bwb.Structure.encoder.Encode(&o); err != nil {
		return err
	}
	if err := bwb.Structure.encoder.Encode(&length); err != nil {
		return err
	}
	return nil
}

type BlockProof struct {
	Contracts  []common.Address
	CMasks     []uint16
	CHashes    []common.Hash
	CShortKeys [][]byte
	CValues    [][]byte
	Codes      [][]byte
	Masks      []uint16
	Hashes     []common.Hash
	ShortKeys  [][]byte
	Values     [][]byte
}

type ProofGenerator struct {
	touches        [][]byte
	proofMasks     map[string]uint32
	sMasks         map[string]map[string]uint32
	proofHashes    map[string][16]common.Hash
	sHashes        map[string]map[string][16]common.Hash
	soleHashes     map[string]common.Hash
	sSoleHashes    map[string]map[string]common.Hash
	createdProofs  map[string]struct{}
	sCreatedProofs map[string]map[string]struct{}
	proofShorts    map[string][]byte
	sShorts        map[string]map[string][]byte
	createdShorts  map[string]struct{}
	sCreatedShorts map[string]map[string]struct{}
	proofValues    map[string][]byte
	sValues        map[string]map[string][]byte
	proofCodes     map[common.Hash][]byte
	createdCodes   map[common.Hash][]byte
}

func NewProofGenerator() *ProofGenerator {
	return &ProofGenerator{
		proofMasks:     make(map[string]uint32),
		sMasks:         make(map[string]map[string]uint32),
		proofHashes:    make(map[string][16]common.Hash),
		sHashes:        make(map[string]map[string][16]common.Hash),
		soleHashes:     make(map[string]common.Hash),
		sSoleHashes:    make(map[string]map[string]common.Hash),
		createdProofs:  make(map[string]struct{}),
		sCreatedProofs: make(map[string]map[string]struct{}),
		proofShorts:    make(map[string][]byte),
		sShorts:        make(map[string]map[string][]byte),
		createdShorts:  make(map[string]struct{}),
		sCreatedShorts: make(map[string]map[string]struct{}),
		proofValues:    make(map[string][]byte),
		sValues:        make(map[string]map[string][]byte),
		proofCodes:     make(map[common.Hash][]byte),
		createdCodes:   make(map[common.Hash][]byte),
	}
}

func (pg *ProofGenerator) AddTouch(touch []byte) {
	pg.touches = append(pg.touches, touch)
}

func (pg *ProofGenerator) ExtractTouches() [][]byte {
	touches := pg.touches
	pg.touches = nil
	return touches
}

func (pg *ProofGenerator) extractProofs(prefix []byte, trace bool) (
	masks []uint16, hashes []common.Hash, shortKeys [][]byte, values [][]byte,
) {
	if trace {
		fmt.Printf("Extracting proofs for prefix %x\n", prefix)
		if prefix != nil {
			fmt.Printf("prefix hash: %x\n", crypto.Keccak256(prefix))
		}
	}
	var proofMasks map[string]uint32
	if prefix == nil {
		proofMasks = pg.proofMasks
	} else {
		var ok bool
		ps := string(prefix)
		proofMasks, ok = pg.sMasks[ps]
		if !ok {
			proofMasks = make(map[string]uint32)
		}
	}
	var proofHashes map[string][16]common.Hash
	if prefix == nil {
		proofHashes = pg.proofHashes
	} else {
		var ok bool
		ps := string(prefix)
		proofHashes, ok = pg.sHashes[ps]
		if !ok {
			proofHashes = make(map[string][16]common.Hash)
		}
	}
	var soleHashes map[string]common.Hash
	if prefix == nil {
		soleHashes = pg.soleHashes
	} else {
		var ok bool
		ps := string(prefix)
		soleHashes, ok = pg.sSoleHashes[ps]
		if !ok {
			soleHashes = make(map[string]common.Hash)
		}
	}
	var proofValues map[string][]byte
	if prefix == nil {
		proofValues = pg.proofValues
	} else {
		var ok bool
		ps := string(prefix)
		proofValues, ok = pg.sValues[ps]
		if !ok {
			proofValues = make(map[string][]byte)
		}
	}
	var proofShorts map[string][]byte
	if prefix == nil {
		proofShorts = pg.proofShorts
	} else {
		var ok bool
		ps := string(prefix)
		proofShorts, ok = pg.sShorts[ps]
		if !ok {
			proofShorts = make(map[string][]byte)
		}
	}
	// Collect all the strings
	keys := []string{}
	keySet := make(map[string]struct{})
	for key := range proofMasks {
		if _, ok := keySet[key]; !ok {
			keys = append(keys, key)
			keySet[key] = struct{}{}
		}
	}
	for key := range proofShorts {
		if _, ok := keySet[key]; !ok {
			keys = append(keys, key)
			keySet[key] = struct{}{}
		}
	}
	for key := range proofValues {
		if _, ok := keySet[key]; !ok {
			keys = append(keys, key)
			keySet[key] = struct{}{}
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if trace {
			fmt.Printf("%x\n", key)
		}
		if hashmask, ok := proofMasks[key]; ok {
			// Determine the downward mask
			var fullnodemask uint16
			var shortnodemask uint16
			for nibble := byte(0); nibble < 16; nibble++ {
				if _, ok2 := proofShorts[key+string(nibble)]; ok2 {
					shortnodemask |= (uint16(1) << nibble)
				}
				if _, ok3 := proofMasks[key+string(nibble)]; ok3 {
					fullnodemask |= (uint16(1) << nibble)
				}
			}
			h := proofHashes[key]
			for i := byte(0); i < 16; i++ {
				if (hashmask & (uint32(1) << i)) != 0 {
					hashes = append(hashes, h[i])
				}
			}
			if trace {
				fmt.Printf("%x: hash %16b, full %16b, short %16b\n", key, hashmask, fullnodemask, shortnodemask)
			}
			if len(masks) == 0 {
				masks = append(masks, 0)
			}
			masks = append(masks, uint16(hashmask))      // Hash mask
			masks = append(masks, uint16(fullnodemask))  // Fullnode mask
			masks = append(masks, uint16(shortnodemask)) // Short node mask
		}
		if short, ok := proofShorts[key]; ok {
			if trace {
				fmt.Printf("Short %x: %x\n", []byte(key), short)
			}
			var downmask uint16
			if _, ok2 := proofHashes[key+string(short)]; ok2 {
				downmask = 1
			} else if h, ok1 := soleHashes[key+string(short)]; ok1 {
				if trace {
					fmt.Printf("Sole hash: %x\n", h[:2])
				}
				hashes = append(hashes, h)
			}
			if trace {
				fmt.Printf("Down %16b\n", downmask)
			}
			if len(masks) == 0 {
				masks = append(masks, 1)
			}
			masks = append(masks, downmask)
			shortKeys = append(shortKeys, short)
		}
		if value, ok := proofValues[key]; ok {
			if trace {
				fmt.Printf("Value %x\n", value)
			}
			values = append(values, value)
		}
	}
	if trace {
		fmt.Printf("Masks:")
		for _, mask := range masks {
			fmt.Printf(" %16b", mask)
		}
		fmt.Printf("\n")
		fmt.Printf("Shorts:")
		for _, short := range shortKeys {
			fmt.Printf(" %x", short)
		}
		fmt.Printf("\n")
		fmt.Printf("Hashes:")
		for _, hash := range hashes {
			fmt.Printf(" %x", hash[:4])
		}
		fmt.Printf("\n")
		fmt.Printf("Values:")
		for _, value := range values {
			if value == nil {
				fmt.Printf(" nil")
			} else {
				fmt.Printf(" %x", value)
			}
		}
		fmt.Printf("\n")
	}
	return masks, hashes, shortKeys, values
}

func (pg *ProofGenerator) ExtractProofs(trace bool) BlockProof {
	// Collect prefixes
	prefixes := []string{}
	prefixSet := make(map[string]struct{})
	for prefix := range pg.sMasks {
		if _, ok := prefixSet[prefix]; !ok {
			prefixes = append(prefixes, prefix)
			prefixSet[prefix] = struct{}{}
		}
	}
	for prefix := range pg.sShorts {
		if _, ok := prefixSet[prefix]; !ok {
			prefixes = append(prefixes, prefix)
			prefixSet[prefix] = struct{}{}
		}
	}
	for prefix := range pg.sValues {
		if _, ok := prefixSet[prefix]; !ok {
			prefixes = append(prefixes, prefix)
			prefixSet[prefix] = struct{}{}
		}
	}
	sort.Strings(prefixes)
	var contracts []common.Address
	var cMasks []uint16
	var cHashes []common.Hash
	var cShortKeys [][]byte
	var cValues [][]byte
	for _, prefix := range prefixes {
		m, h, s, v := pg.extractProofs([]byte(prefix), trace)
		if len(m) > 0 || len(h) > 0 || len(s) > 0 || len(v) > 0 {
			contracts = append(contracts, common.BytesToAddress([]byte(prefix)))
			cMasks = append(cMasks, m...)
			cHashes = append(cHashes, h...)
			cShortKeys = append(cShortKeys, s...)
			cValues = append(cValues, v...)
		}
	}
	masks, hashes, shortKeys, values := pg.extractProofs(nil, trace)
	var codes [][]byte
	for _, code := range pg.proofCodes {
		codes = append(codes, code)
	}
	pg.proofMasks = make(map[string]uint32)
	pg.sMasks = make(map[string]map[string]uint32)
	pg.proofHashes = make(map[string][16]common.Hash)
	pg.sHashes = make(map[string]map[string][16]common.Hash)
	pg.soleHashes = make(map[string]common.Hash)
	pg.sSoleHashes = make(map[string]map[string]common.Hash)
	pg.proofShorts = make(map[string][]byte)
	pg.sShorts = make(map[string]map[string][]byte)
	pg.proofValues = make(map[string][]byte)
	pg.sValues = make(map[string]map[string][]byte)
	pg.proofCodes = make(map[common.Hash][]byte)
	pg.createdCodes = make(map[common.Hash][]byte)
	return BlockProof{contracts, cMasks, cHashes, cShortKeys, cValues, codes, masks, hashes, shortKeys, values}
}

func (pg *ProofGenerator) addProof(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {
	var proofShorts map[string][]byte
	if prefix == nil {
		proofShorts = pg.proofShorts
	} else {
		var ok bool
		proofShorts, ok = pg.sShorts[string(common.CopyBytes(prefix))]
		if !ok {
			proofShorts = make(map[string][]byte)
		}
	}
	k := make([]byte, pos)
	copy(k, key[:pos])
	for i := len(k); i >= 0; i-- {
		if i < len(k) {
			if short, ok := proofShorts[string(k[:i])]; ok && i+len(short) <= len(k) && bytes.Equal(short, k[i:i+len(short)]) {
				break
			}
		}
	}
	if prefix == nil {
		//fmt.Printf("addProof %x %x added\n", prefix, key[:pos])
	}
	var proofMasks map[string]uint32
	if prefix == nil {
		proofMasks = pg.proofMasks
	} else {
		var ok bool
		ps := string(prefix)
		proofMasks, ok = pg.sMasks[ps]
		if !ok {
			proofMasks = make(map[string]uint32)
			pg.sMasks[ps] = proofMasks
		}
	}
	var proofHashes map[string][16]common.Hash
	if prefix == nil {
		proofHashes = pg.proofHashes
	} else {
		var ok bool
		ps := string(prefix)
		proofHashes, ok = pg.sHashes[ps]
		if !ok {
			proofHashes = make(map[string][16]common.Hash)
			pg.sHashes[ps] = proofHashes
		}
	}
	ks := string(k)
	if m, ok := proofMasks[ks]; ok {
		intersection := m & mask
		//if mask != 0 {
		proofMasks[ks] = intersection
		//}
		h := proofHashes[ks]
		idx := 0
		for i := byte(0); i < 16; i++ {
			if intersection&(uint32(1)<<i) != 0 {
				h[i] = hashes[idx]
			} else {
				h[i] = common.Hash{}
			}
			if mask&(uint32(1)<<i) != 0 {
				idx++
			}
		}
		proofHashes[ks] = h
	} else {
		//if mask != 0 {
		proofMasks[ks] = mask
		//}
		var h [16]common.Hash
		idx := 0
		for i := byte(0); i < 16; i++ {
			if mask&(uint32(1)<<i) != 0 {
				h[i] = hashes[idx]
				idx++
			}
		}
		proofHashes[ks] = h
	}
}

func (pg *ProofGenerator) addSoleHash(prefix, key []byte, pos int, hash common.Hash) {
	var soleHashes map[string]common.Hash
	if prefix == nil {
		soleHashes = pg.soleHashes
	} else {
		var ok bool
		ps := string(prefix)
		soleHashes, ok = pg.sSoleHashes[ps]
		if !ok {
			soleHashes = make(map[string]common.Hash)
			pg.sSoleHashes[ps] = soleHashes
		}
	}
	k := make([]byte, pos)
	copy(k, key[:pos])
	ks := string(k)
	if _, ok := soleHashes[ks]; !ok {
		soleHashes[ks] = hash
	}
}

func (pg *ProofGenerator) addValue(prefix, key []byte, pos int, value []byte) {
	var proofShorts map[string][]byte
	if prefix == nil {
		proofShorts = pg.proofShorts
	} else {
		var ok bool
		ps := string(common.CopyBytes(prefix))
		proofShorts, ok = pg.sShorts[ps]
		if !ok {
			proofShorts = make(map[string][]byte)
		}
	}
	// Find corresponding short
	found := false
	for i := 0; i < pos; i++ {
		if short, ok := proofShorts[string(key[:i])]; ok && bytes.Equal(short, key[i:pos]) {
			found = true
			break
		}
	}
	if !found {
		return
	}
	var proofValues map[string][]byte
	if prefix == nil {
		proofValues = pg.proofValues
	} else {
		var ok bool
		ps := string(common.CopyBytes(prefix))
		proofValues, ok = pg.sValues[ps]
		if !ok {
			proofValues = make(map[string][]byte)
			pg.sValues[ps] = proofValues
		}
	}
	k := make([]byte, pos)
	copy(k, key[:pos])
	ks := string(k)
	if _, ok := proofValues[ks]; !ok {
		proofValues[ks] = common.CopyBytes(value)
	}
}

func (pg *ProofGenerator) addShort(prefix, key []byte, pos int, short []byte) {
	var proofShorts map[string][]byte
	if prefix == nil {
		proofShorts = pg.proofShorts
	} else {
		var ok bool
		ps := string(common.CopyBytes(prefix))
		proofShorts, ok = pg.sShorts[ps]
		if !ok {
			proofShorts = make(map[string][]byte)
			pg.sShorts[ps] = proofShorts
		}
	}
	k := make([]byte, pos)
	copy(k, key[:pos])
	ks := string(k)
	if _, ok := proofShorts[ks]; !ok {
		proofShorts[ks] = common.CopyBytes(short)
		return
	}
}

func (pg *ProofGenerator) ReadCode(codeHash common.Hash, code []byte) {
	if _, ok := pg.createdCodes[codeHash]; !ok {
		pg.proofCodes[codeHash] = code
	}
}

func (pg *ProofGenerator) CreateCode(codeHash common.Hash, code []byte) {
	if _, ok := pg.createdCodes[codeHash]; !ok {
		pg.createdCodes[codeHash] = code
	}
}

func constructFullNode(touchFunc func(hex []byte, del bool), ctime uint64,
	hex []byte,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	maskIdx, shortIdx, valueIdx, hashIdx *int,
	trace bool,
) *fullNode {
	pos := len(hex)
	hashmask := masks[*maskIdx]
	(*maskIdx)++
	fullnodemask := masks[*maskIdx]
	(*maskIdx)++
	shortnodemask := masks[*maskIdx]
	(*maskIdx)++
	if trace {
		fmt.Printf("%spos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b", strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask)
		fmt.Printf("%s, hashes:", strings.Repeat(" ", pos))
	}
	// Make a full node
	f := &fullNode{}
	f.flags.dirty = true
	touchFunc(hex, false)
	for nibble := byte(0); nibble < 16; nibble++ {
		if (hashmask & (uint16(1) << nibble)) != 0 {
			hash := hashes[*hashIdx]
			if trace {
				fmt.Printf(" %x", hash[:2])
			}
			f.Children[nibble] = hashNode(hash[:])
			(*hashIdx)++
		} else {
			f.Children[nibble] = nil
			if trace {
				fmt.Printf(" ....")
			}
		}
	}
	if trace {
		fmt.Printf("\n")
	}
	for nibble := byte(0); nibble < 16; nibble++ {
		if (fullnodemask & (uint16(1) << nibble)) != 0 {
			if trace {
				fmt.Printf("%sIn the loop at pos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b, nibble %x\n", strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask, nibble)
			}
			f.Children[nibble] = constructFullNode(touchFunc, ctime, concat(hex, nibble), masks, shortKeys, values, hashes, maskIdx, shortIdx, valueIdx, hashIdx, trace)
		} else if (shortnodemask & (uint16(1) << nibble)) != 0 {
			if trace {
				fmt.Printf("%sIn the loop at pos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b, nibble %x\n", strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask, nibble)
			}
			f.Children[nibble] = constructShortNode(touchFunc, ctime, concat(hex, nibble), masks, shortKeys, values, hashes, maskIdx, shortIdx, valueIdx, hashIdx, trace)
		}
	}
	return f
}

func constructShortNode(touchFunc func(hex []byte, del bool), ctime uint64,
	hex []byte,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	maskIdx, shortIdx, valueIdx, hashIdx *int,
	trace bool,
) *shortNode {
	pos := len(hex)
	downmask := masks[*maskIdx]
	(*maskIdx)++
	if trace {
		fmt.Printf("%spos: %d, down: %16b", strings.Repeat(" ", pos), pos, downmask)
	}
	// short node (leaf or extension)
	nKey := shortKeys[*shortIdx]
	(*shortIdx)++
	s := &shortNode{Key: common.CopyBytes(nKey)}
	if trace {
		fmt.Printf("\n")
	}
	if pos+len(nKey) == 65 {
		s.Val = valueNode(values[*valueIdx])
		(*valueIdx)++
	} else {
		if trace {
			fmt.Printf("%spos = %d, len(nKey) = %d, nKey = %x\n", strings.Repeat(" ", pos), pos, len(nKey), nKey)
		}
		if downmask == 0 || downmask == 4 {
			hash := hashes[*hashIdx]
			if trace {
				fmt.Printf("%shash: %x\n", strings.Repeat(" ", pos), hash[:2])
			}
			s.Val = hashNode(hash[:])
			(*hashIdx)++
		} else if downmask == 1 || downmask == 6 {
			s.Val = constructFullNode(touchFunc, ctime, concat(hex, nKey...), masks, shortKeys, values, hashes, maskIdx, shortIdx, valueIdx, hashIdx, trace)
		}
	}
	if s.Val == nil {
		fmt.Printf("s.Val is nil, pos %d, nKey %x, downmask %d\n", pos, nKey, downmask)
	}
	return s
}

func NewFromProofs(touchFunc func(hex []byte, del bool), ctime uint64,
	encodeToBytes bool,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	trace bool,
) (t *Trie, mIdx, hIdx, sIdx, vIdx int) {
	t = new(Trie)
	var maskIdx int
	var hashIdx int  // index in the hashes
	var shortIdx int // index in the shortKeys
	var valueIdx int // inde in the values
	if trace {
		fmt.Printf("\n")
	}
	firstMask := masks[0]
	maskIdx = 1
	if firstMask == 0 {
		t.root = constructFullNode(touchFunc, ctime, []byte{}, masks, shortKeys, values, hashes, &maskIdx, &shortIdx, &valueIdx, &hashIdx, trace)
	} else {
		t.root = constructShortNode(touchFunc, ctime, []byte{}, masks, shortKeys, values, hashes, &maskIdx, &shortIdx, &valueIdx, &hashIdx, trace)
	}
	return t, maskIdx, hashIdx, shortIdx, valueIdx
}

func ammendFullNode(timeFunc func(hex []byte) uint64, cuttime uint64, n node,
	hex []byte,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	maskIdx, shortIdx, valueIdx, hashIdx *int,
	aMasks []uint16,
	aShortKeys [][]byte,
	aValues [][]byte,
	aHashes []common.Hash,
	trace bool,
) ([]uint16, [][]byte, [][]byte, []common.Hash) {
	pos := len(hex)
	hashmask := masks[*maskIdx]
	(*maskIdx)++
	fullnodemask := masks[*maskIdx]
	(*maskIdx)++
	shortnodemask := masks[*maskIdx]
	(*maskIdx)++
	aHashMaxIdx := len(aMasks)
	aMasks = append(aMasks, 0)
	aFullnodemaskIdx := len(aMasks)
	aMasks = append(aMasks, 0)
	aShortnodemaskIdx := len(aMasks)
	aMasks = append(aMasks, 0)
	var aHashmask, aFullnodemask, aShortnodemask uint16
	if trace {
		fmt.Printf("%spos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b",
			strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask)
		fmt.Printf("%s, hashes:", strings.Repeat(" ", pos))
	}
	// Make a full node
	f, ok := n.(*fullNode)
	if !ok {
		if d, dok := n.(*duoNode); dok {
			f = d.fullCopy()
			ok = true
		}
	}
	if ok && trace {
		fmt.Printf("%sf.flags.t %d, cuttime %d\n", strings.Repeat(" ", pos), timeFunc(hex), cuttime)
	}
	if ok && timeFunc(hex) < cuttime {
		f = nil
		ok = false
	}
	for nibble := byte(0); nibble < 16; nibble++ {
		if (hashmask & (uint16(1) << nibble)) != 0 {
			hash := hashes[*hashIdx]
			(*hashIdx)++
			if trace {
				fmt.Printf(" %x", hash[:2])
			}
			if !ok {
				aHashes = append(aHashes, hash)
				aHashmask |= (uint16(1) << nibble)
			}
		} else {
			if trace {
				fmt.Printf(" ....")
			}
		}
	}
	if trace {
		fmt.Printf("\n")
	}
	for nibble := byte(0); nibble < 16; nibble++ {
		var child node
		if ok {
			child = f.Children[nibble]
		}
		if (fullnodemask & (uint16(1) << nibble)) != 0 {
			if trace {
				fmt.Printf("%sIn the loop at pos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b, nibble %x, fchild %T\n",
					strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask, nibble, child)
			}
			aMasks, aShortKeys, aValues, aHashes = ammendFullNode(timeFunc, cuttime, child, concat(hex, nibble), masks, shortKeys, values, hashes,
				maskIdx, shortIdx, valueIdx, hashIdx,
				aMasks, aShortKeys, aValues, aHashes, trace)
			aFullnodemask |= (uint16(1) << nibble)
		} else if (shortnodemask & (uint16(1) << nibble)) != 0 {
			if trace {
				fmt.Printf("%sIn the loop at pos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b, nibble %x, schild %T\n",
					strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask, nibble, child)
			}
			aMasks, aShortKeys, aValues, aHashes = ammendShortNode(timeFunc, cuttime, child, concat(hex, nibble), masks, shortKeys, values, hashes,
				maskIdx, shortIdx, valueIdx, hashIdx,
				aMasks, aShortKeys, aValues, aHashes, trace)
			aShortnodemask |= (uint16(1) << nibble)
		}
	}
	aMasks[aHashMaxIdx] = aHashmask
	aMasks[aFullnodemaskIdx] = aFullnodemask
	aMasks[aShortnodemaskIdx] = aShortnodemask

	return aMasks, aShortKeys, aValues, aHashes
}

func ammendShortNode(timeFunc func(hex []byte) uint64, cuttime uint64, n node,
	hex []byte,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	maskIdx, shortIdx, valueIdx, hashIdx *int,
	aMasks []uint16,
	aShortKeys [][]byte,
	aValues [][]byte,
	aHashes []common.Hash,
	trace bool,
) ([]uint16, [][]byte, [][]byte, []common.Hash) {
	pos := len(hex)
	downmask := masks[*maskIdx]
	(*maskIdx)++
	// short node (leaf or extension)
	nKey := shortKeys[*shortIdx]
	(*shortIdx)++
	if trace {
		fmt.Printf("%spos: %d, down: %16b, nKey %x", strings.Repeat(" ", pos), pos, downmask, nKey)
	}
	s, ok := n.(*shortNode)
	if trace {
		fmt.Printf("\n")
	}
	if pos+len(nKey) == 65 {
		value := values[*valueIdx]
		(*valueIdx)++
		if !ok {
			aMasks = append(aMasks, 2)
			aShortKeys = append(aShortKeys, nKey)
			aValues = append(aValues, value)
		} else {
			aMasks = append(aMasks, 3)
		}
	} else {
		if trace {
			fmt.Printf("%spos = %d, len(nKey) = %d, nKey = %x\n", strings.Repeat(" ", pos), pos, len(nKey), nKey)
		}
		if downmask == 0 {
			if trace {
				fmt.Printf("%shash: %x\n", strings.Repeat(" ", pos), hashes[*hashIdx][:2])
			}
			hash := hashes[*hashIdx]
			(*hashIdx)++
			if !ok {
				aMasks = append(aMasks, 4)
				aShortKeys = append(aShortKeys, nKey)
				aHashes = append(aHashes, hash)
			} else {
				aMasks = append(aMasks, 5)
			}
		} else {
			var val node
			if !ok {
				aMasks = append(aMasks, 6)
				aShortKeys = append(aShortKeys, nKey)
			} else {
				val = s.Val
				aMasks = append(aMasks, 7)
			}
			aMasks, aShortKeys, aValues, aHashes = ammendFullNode(timeFunc, cuttime,
				val, concat(hex, nKey...), masks, shortKeys, values, hashes,
				maskIdx, shortIdx, valueIdx, hashIdx,
				aMasks, aShortKeys, aValues, aHashes,
				trace)
		}
	}
	return aMasks, aShortKeys, aValues, aHashes
}

func (t *Trie) AmmendProofs(
	timeFunc func(hex []byte) uint64,
	cuttime uint64,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	aMasks []uint16,
	aShortKeys [][]byte,
	aValues [][]byte,
	aHashes []common.Hash,
	trace bool,
) (mIdx, hIdx, sIdx, vIdx int, aMasks_ []uint16, aShortKeys_ [][]byte, aValues_ [][]byte, aHashes_ []common.Hash) {
	var maskIdx int
	var hashIdx int  // index in the hashes
	var shortIdx int // index in the shortKeys
	var valueIdx int // inde in the values
	firstMask := masks[0]
	maskIdx = 1
	aMasks = append(aMasks, firstMask)
	if firstMask == 0 {
		aMasks_, aShortKeys_, aValues_, aHashes_ = ammendFullNode(timeFunc, cuttime, t.root, []byte{}, masks, shortKeys, values, hashes,
			&maskIdx, &shortIdx, &valueIdx, &hashIdx,
			aMasks, aShortKeys, aValues, aHashes, trace)
	} else {
		aMasks_, aShortKeys_, aValues_, aHashes_ = ammendShortNode(timeFunc, cuttime, t.root, []byte{}, masks, shortKeys, values, hashes,
			&maskIdx, &shortIdx, &valueIdx, &hashIdx,
			aMasks, aShortKeys, aValues, aHashes, trace)
	}
	return maskIdx, hashIdx, shortIdx, valueIdx, aMasks_, aShortKeys_, aValues_, aHashes_
}

func applyFullNode(h *hasher, touchFunc func(hex []byte, del bool), ctime uint64, n node,
	hex []byte,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	maskIdx, shortIdx, valueIdx, hashIdx *int,
	trace bool,
) *fullNode {
	pos := len(hex)
	hashmask := masks[*maskIdx]
	(*maskIdx)++
	fullnodemask := masks[*maskIdx]
	(*maskIdx)++
	shortnodemask := masks[*maskIdx]
	(*maskIdx)++
	if trace {
		fmt.Printf("%spos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b",
			strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask)
		fmt.Printf("%s, hashes:", strings.Repeat(" ", pos))
	}
	// Make a full node
	f, ok := n.(*fullNode)
	if !ok {
		if d, dok := n.(*duoNode); dok {
			f = d.fullCopy()
			ok = true
		} else {
			f = &fullNode{}
			f.flags.dirty = true
		}
	}
	touchFunc(hex, false)
	for nibble := byte(0); nibble < 16; nibble++ {
		if (hashmask & (uint16(1) << nibble)) != 0 {
			hash := hashes[*hashIdx]
			(*hashIdx)++
			if trace {
				fmt.Printf(" %x", hash[:2])
			}
			if !ok {
				f.Children[nibble] = hashNode(hash[:])
			}
		} else {
			if trace {
				fmt.Printf(" ....")
			}
		}
	}
	if trace {
		fmt.Printf("\n")
		if ok {
			fmt.Printf("%sKeep existing fullnode\n", strings.Repeat(" ", pos))
		}
	}
	for nibble := byte(0); nibble < 16; nibble++ {
		var child node
		if ok {
			child = f.Children[nibble]
		}
		if (fullnodemask & (uint16(1) << nibble)) != 0 {
			if trace {
				fmt.Printf("%sIn the loop at pos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b, nibble %x, child %T\n",
					strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask, nibble, child)
			}
			fn := applyFullNode(h, touchFunc, ctime, child, concat(hex, nibble), masks, shortKeys, values, hashes,
				maskIdx, shortIdx, valueIdx, hashIdx, trace)
			f.Children[nibble] = fn
		} else if (shortnodemask & (uint16(1) << nibble)) != 0 {
			if trace {
				fmt.Printf("%sIn the loop at pos: %d, hashes: %16b, fullnodes: %16b, shortnodes: %16b, nibble %x, child %T\n",
					strings.Repeat(" ", pos), pos, hashmask, fullnodemask, shortnodemask, nibble, child)
			}
			sn := applyShortNode(h, touchFunc, ctime, child, concat(hex, nibble), masks, shortKeys, values, hashes,
				maskIdx, shortIdx, valueIdx, hashIdx, trace)
			f.Children[nibble] = sn
		}
	}
	if f.flags.dirty {
		var hn common.Hash
		h.hash(f, pos == 0, hn[:])
	}
	return f
}

func applyShortNode(h *hasher, touchFunc func(hex []byte, del bool), ctime uint64, n node,
	hex []byte,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	maskIdx, shortIdx, valueIdx, hashIdx *int,
	trace bool,
) *shortNode {
	pos := len(hex)
	downmask := masks[*maskIdx]
	(*maskIdx)++
	// short node (leaf or extension)
	var s *shortNode
	var ok bool
	switch nt := n.(type) {
	case *shortNode:
		s = nt
		ok = true
	case *duoNode:
		touchFunc(hex, true) // duoNode turned into shortNode - delete from prunable set
	case *fullNode:
		touchFunc(hex, true) // fullNode turned into shortNode - delete from prunable set
	}
	var nKey []byte
	if (downmask <= 1) || downmask == 2 || downmask == 4 || downmask == 6 {
		nKey = shortKeys[*shortIdx]
		(*shortIdx)++
		if ok && !bytes.Equal(s.Key, nKey) {
			fmt.Printf("%s keys don't match: s.Key %x, nKey %x\n", strings.Repeat(" ", pos), s.Key, nKey)
		}
	}
	if !ok && ((downmask <= 1) || downmask == 2 || downmask == 4 || downmask == 6) {
		s = &shortNode{Key: common.CopyBytes(nKey)}
	}
	if trace {
		fmt.Printf("%spos: %d, down: %16b, nKey: %x", strings.Repeat(" ", pos), pos, downmask, nKey)
	}
	if trace {
		fmt.Printf("\n")
		if ok {
			fmt.Printf("%skeep existing short node %x\n", strings.Repeat(" ", pos), s.Key)
		}
	}
	switch downmask {
	case 0:
		if pos+len(nKey) == 65 {
			value := values[*valueIdx]
			(*valueIdx)++
			s.Val = valueNode(value)
		} else {
			hash := hashes[*hashIdx]
			(*hashIdx)++
			s.Val = hashNode(hash[:])
		}
	case 1:
		if pos+len(nKey) == 65 {
			value := values[*valueIdx]
			(*valueIdx)++
			s.Val = valueNode(value)
		} else {
			s.Val = applyFullNode(h, touchFunc, ctime, s.Val, concat(hex, nKey...), masks, shortKeys, values, hashes,
				maskIdx, shortIdx, valueIdx, hashIdx, trace)
		}
	case 2:
		value := values[*valueIdx]
		(*valueIdx)++
		s.Val = valueNode(value)
	case 3:
	case 4:
		if trace {
			fmt.Printf("%spos = %d, len(nKey) = %d, nKey = %x\n", strings.Repeat(" ", pos), pos, len(nKey), nKey)
		}
		hash := hashes[*hashIdx]
		(*hashIdx)++
		s.Val = hashNode(hash[:])
	case 5:
		if trace {
			fmt.Printf("%spos = %d, len(nKey) = %d, nKey = %x\n", strings.Repeat(" ", pos), pos, len(nKey), nKey)
		}
	case 6:
		s.Val = applyFullNode(h, touchFunc, ctime, nil, concat(hex, nKey...), masks, shortKeys, values, hashes,
			maskIdx, shortIdx, valueIdx, hashIdx, trace)
	case 7:
		s.Val = applyFullNode(h, touchFunc, ctime, s.Val, concat(hex, s.Key...), masks, shortKeys, values, hashes,
			maskIdx, shortIdx, valueIdx, hashIdx, trace)
	}
	return s
}

func (t *Trie) ApplyProof(
	ctime uint64,
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
	trace bool,
) (mIdx, hIdx, sIdx, vIdx int) {
	var maskIdx int
	var hashIdx int  // index in the hashes
	var shortIdx int // index in the shortKeys
	var valueIdx int // inde in the values
	firstMask := masks[0]
	maskIdx = 1
	if len(masks) == 1 {
		return maskIdx, hashIdx, shortIdx, valueIdx
	}
	h := newHasher(false)
	defer returnHasherToPool(h)
	if firstMask == 0 {
		t.root = applyFullNode(h, t.touchFunc, ctime, t.root, []byte{}, masks, shortKeys, values, hashes,
			&maskIdx, &shortIdx, &valueIdx, &hashIdx, trace)
	} else {
		t.root = applyShortNode(h, t.touchFunc, ctime, t.root, []byte{}, masks, shortKeys, values, hashes,
			&maskIdx, &shortIdx, &valueIdx, &hashIdx, trace)
	}
	return maskIdx, hashIdx, shortIdx, valueIdx
}

func (t *Trie) AsProof(trace bool) (
	masks []uint16,
	shortKeys [][]byte,
	values [][]byte,
	hashes []common.Hash,
) {
	return
}
