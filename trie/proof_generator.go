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

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
)

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
	proofMasks       map[string]uint32
	sMasks           map[string]map[string]uint32
	proofHashes      map[string][16]common.Hash
	sHashes          map[string]map[string][16]common.Hash
	soleHashes       map[string]common.Hash
	sSoleHashes      map[string]map[string]common.Hash
	createdProofs    map[string]struct{}
	sCreatedProofs   map[string]map[string]struct{}
	proofShorts      map[string][]byte
	sShorts          map[string]map[string][]byte
	createdShorts    map[string]struct{}
	sCreatedShorts   map[string]map[string]struct{}
	proofValues      map[string][]byte
	sValues          map[string]map[string][]byte
	proofCodes       map[common.Hash][]byte
	createdCodes     map[common.Hash][]byte
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
