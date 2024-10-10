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
	"encoding/binary"
	"fmt"
	"os"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/rlphacks"
)

// StreamItem is an enum type for values that help distinguish different
// types of key-value pairs in the stream of values produce by function `ToStream`
type StreamItem uint8

const (
	// NoItem is used to signal the end of iterator
	NoItem StreamItem = iota
	// AccountStreamItem used for marking a key-value pair in the stream as belonging to an account
	AccountStreamItem
	// StorageStreamItem used for marking a key-value pair in the stream as belonging to a storage item leaf
	StorageStreamItem
	// AHashStreamItem used for marking a key-value pair in the stream as belonging to an intermediate hash
	// within the accounts (main state trie)
	AHashStreamItem
	// SHashStreamItem used for marking a key-value pair in the stream as belonging to an intermediate hash
	// within the storage items (storage tries)
	SHashStreamItem
	// CutoffStremItem used for marking the end of the subtrie
	CutoffStreamItem
)

// Stream represents the collection of key-value pairs, sorted by keys, where values may belong
// to three different types - accounts, strage item leaves, and intermediate hashes
type Stream struct {
	keyBytes  []byte
	keySizes  []uint8
	itemTypes []StreamItem
	aValues   []*accounts.Account
	aCodes    [][]byte
	sValues   [][]byte
	hashes    []byte
}

// Reset sets all slices to zero sizes, without de-allocating
func (s *Stream) Reset() {
	if len(s.keyBytes) > 0 {
		s.keyBytes = s.keyBytes[:0]
	}
	if len(s.keySizes) > 0 {
		s.keySizes = s.keySizes[:0]
	}
	if len(s.itemTypes) > 0 {
		s.itemTypes = s.itemTypes[:0]
	}
	if len(s.aValues) > 0 {
		s.aValues = s.aValues[:0]
	}
	if len(s.aCodes) > 0 {
		s.aCodes = s.aCodes[:0]
	}
	if len(s.sValues) > 0 {
		s.sValues = s.sValues[:0]
	}
	if len(s.hashes) > 0 {
		s.hashes = s.hashes[:0]
	}
}

type StreamIterator interface {
	Next() (itemType StreamItem, hex1 []byte, aValue *accounts.Account, hash []byte, value []byte)
}

// Iterator helps iterate over a trie according to a given resolve set
type Iterator struct {
	rl           *RetainList
	hex          []byte
	nodeStack    []node
	iStack       []int
	goDeepStack  []bool
	lenStack     []int
	accountStack []bool
	top          int // Top of the stack
	trace        bool
}

// NewIterator creates a new iterator from scratch from a given trie and resolve set
func NewIterator(t *Trie, rl *RetainList, trace bool) *Iterator {
	return &Iterator{
		rl:           rl,
		hex:          []byte{},
		nodeStack:    []node{t.root},
		iStack:       []int{0},
		goDeepStack:  []bool{true},
		lenStack:     []int{0},
		accountStack: []bool{true},
		top:          1,
		trace:        trace,
	}
}

// Reset prepares iterator to be reused
func (it *Iterator) Reset(t *Trie, rl *RetainList, trace bool) {
	it.rl = rl
	it.hex = it.hex[:0]
	if len(it.nodeStack) > 0 {
		it.nodeStack = it.nodeStack[:0]
	}
	it.nodeStack = append(it.nodeStack, t.root)
	if len(it.iStack) > 0 {
		it.iStack = it.iStack[:0]
	}
	it.lenStack = append(it.lenStack, 0)
	if len(it.goDeepStack) > 0 {
		it.goDeepStack = it.goDeepStack[:0]
	}
	it.goDeepStack = append(it.goDeepStack, true)
	if len(it.accountStack) > 0 {
		it.accountStack = it.accountStack[:0]
	}
	it.accountStack = append(it.accountStack, true)
	it.top = 1
	it.trace = trace
}

// Next delivers the next item from the iterator
func (it *Iterator) Next() (itemType StreamItem, hex1 []byte, aValue *accounts.Account, hash []byte, value []byte) {
	for {
		if it.top == 0 {
			return NoItem, nil, nil, nil, nil
		}
		l := it.top - 1
		nd := it.nodeStack[l]
		hexLen := it.lenStack[l]
		hex := it.hex[:hexLen]
		goDeep := it.goDeepStack[l]
		accounts := it.accountStack[l]
		switch n := nd.(type) {
		case nil:
			return NoItem, nil, nil, nil, nil
		case valueNode:
			if it.trace {
				fmt.Printf("valueNode %x\n", hex)
			}
			it.top--
			return StorageStreamItem, hex, nil, nil, n
		case *shortNode:
			if it.trace {
				fmt.Printf("shortNode %x\n", hex)
			}
			nKey := n.Key
			if nKey[len(nKey)-1] == 16 {
				nKey = nKey[:len(nKey)-1]
			}
			hex = append(hex, nKey...)
			switch v := n.Val.(type) {
			case hashNode:
				it.top--
				if accounts {
					return AHashStreamItem, hex, nil, v.hash, nil
				}
				return SHashStreamItem, hex, nil, v.hash, nil
			case valueNode:
				it.top--
				return StorageStreamItem, hex, nil, nil, v
			case *accountNode:
				if it.trace {
					fmt.Printf("accountNode %x\n", hex)
				}
				if v.storage != nil {
					binary.BigEndian.PutUint64(bytes8[:], v.Incarnation)
					// Add decompressed incarnation to the hex
					for i, b := range bytes8[:] {
						bytes16[i*2] = b / 16
						bytes16[i*2+1] = b % 16
					}
					it.hex = append(hex, bytes16[:]...)
					it.nodeStack[l] = v.storage
					it.iStack[l] = 0
					it.goDeepStack[l] = true
					it.lenStack[l] = len(it.hex)
					it.accountStack[l] = false
				} else {
					it.top--
				}
				return AccountStreamItem, hex, &v.Account, nil, nil
			}
			it.hex = hex
			it.nodeStack[l] = n.Val
			it.iStack[l] = 0
			it.goDeepStack[l] = it.rl.Retain(hex)
			it.lenStack[l] = len(hex)
			it.accountStack[l] = accounts
		case *duoNode:
			if it.trace {
				fmt.Printf("duoNode %x\n", hex)
			}
			if !goDeep {
				it.top--
				if accounts {
					return AHashStreamItem, hex, nil, n.reference(), nil
				}
				return SHashStreamItem, hex, nil, n.reference(), nil
			}
			i1, i2 := n.childrenIdx()
			index := it.iStack[l]
			if index <= int(i1) {
				it.iStack[l] = int(i2)
				hex = append(hex, i1)
				var childGoDeep bool
				switch v := n.child1.(type) {
				case hashNode:
					if accounts {
						return AHashStreamItem, hex, nil, v.hash, nil
					}
					return SHashStreamItem, hex, nil, v.hash, nil
				case *duoNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				case *fullNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				}
				it.hex = hex
				l++
				it.top++
				if l >= len(it.nodeStack) {
					it.nodeStack = append(it.nodeStack, n.child1)
					it.iStack = append(it.iStack, 0)
					it.goDeepStack = append(it.goDeepStack, childGoDeep)
					it.lenStack = append(it.lenStack, len(hex))
					it.accountStack = append(it.accountStack, accounts)
				} else {
					it.nodeStack[l] = n.child1
					it.iStack[l] = 0
					it.goDeepStack[l] = childGoDeep
					it.lenStack[l] = len(hex)
					it.accountStack[l] = accounts
				}
			} else {
				hex = append(hex, i2)
				var childGoDeep bool
				switch v := n.child2.(type) {
				case hashNode:
					it.top--
					if accounts {
						return AHashStreamItem, hex, nil, v.hash, nil
					}
					return SHashStreamItem, hex, nil, v.hash, nil
				case *duoNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						it.top--
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				case *fullNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						it.top--
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				}
				it.hex = hex
				it.nodeStack[l] = n.child2
				it.iStack[l] = 0
				it.goDeepStack[l] = childGoDeep
				it.lenStack[l] = len(hex)
				it.accountStack[l] = accounts
			}
		case *fullNode:
			if it.trace {
				fmt.Printf("fullNode %x\n", hex)
			}
			if !goDeep {
				it.top--
				if accounts {
					return AHashStreamItem, hex, nil, n.reference(), nil
				}
				return SHashStreamItem, hex, nil, n.reference(), nil
			}
			var i1, i2 int
			i1Found := false
			i2Found := false
			index := it.iStack[l]
			for i := index; i < len(n.Children); i++ {
				if n.Children[i] != nil {
					if i1Found {
						i2 = i
						i2Found = true
						break
					} else {
						i1 = i
						i1Found = true
					}
				}
			}
			if i2Found {
				it.iStack[l] = i2
				hex = append(hex, byte(i1))
				var childGoDeep bool
				switch v := n.Children[i1].(type) {
				case hashNode:
					if accounts {
						return AHashStreamItem, hex, nil, v.hash, nil
					}
					return SHashStreamItem, hex, nil, v.hash, nil
				case *duoNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				case *fullNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				}
				it.hex = hex
				l++
				it.top++
				if l >= len(it.nodeStack) {
					it.nodeStack = append(it.nodeStack, n.Children[i1])
					it.iStack = append(it.iStack, 0)
					it.goDeepStack = append(it.goDeepStack, childGoDeep)
					it.lenStack = append(it.lenStack, len(hex))
					it.accountStack = append(it.accountStack, accounts)
				} else {
					it.nodeStack[l] = n.Children[i1]
					it.iStack[l] = 0
					it.goDeepStack[l] = childGoDeep
					it.lenStack[l] = len(hex)
					it.accountStack[l] = accounts
				}
			} else {
				hex = append(hex, byte(i1))
				var childGoDeep bool
				switch v := n.Children[i1].(type) {
				case hashNode:
					it.top--
					if accounts {
						return AHashStreamItem, hex, nil, v.hash, nil
					}
					return SHashStreamItem, hex, nil, v.hash, nil
				case *duoNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						it.top--
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				case *fullNode:
					childGoDeep = it.rl.Retain(hex)
					if !childGoDeep {
						it.top--
						if accounts {
							return AHashStreamItem, hex, nil, v.reference(), nil
						}
						return SHashStreamItem, hex, nil, v.reference(), nil
					}
				}
				it.hex = hex
				it.nodeStack[l] = n.Children[i1]
				it.iStack[l] = 0
				it.goDeepStack[l] = childGoDeep
				it.lenStack[l] = len(hex)
				it.accountStack[l] = accounts
			}
		case *accountNode:
			if it.trace {
				fmt.Printf("accountNode %x\n", hex)
			}
			if n.storage != nil {
				binary.BigEndian.PutUint64(bytes8[:], n.Incarnation)
				// Add decompressed incarnation to the hex
				for i, b := range bytes8[:] {
					bytes16[i*2] = b / 16
					bytes16[i*2+1] = b % 16
				}
				it.hex = append(hex, bytes16[:]...)
				it.nodeStack[l] = n.storage
				it.iStack[l] = 0
				it.goDeepStack[l] = true
				it.lenStack[l] = len(it.hex)
				it.accountStack[l] = false
			} else {
				it.top--
			}
			return AccountStreamItem, hex, &n.Account, nil, nil
		case hashNode:
			if it.trace {
				fmt.Printf("hashNode %x\n", hex)
			}
			it.top--
			if accounts {
				return AHashStreamItem, hex, nil, n.hash, nil
			}
			return SHashStreamItem, hex, nil, n.hash, nil
		default:
			panic(fmt.Errorf("unexpected node: %T", nd))
		}
	}
}

// StreamMergeIterator merges an Iterator and a Stream
type StreamMergeIterator struct {
	it          *Iterator
	s           *Stream
	trace       bool
	ki, ai, si  int
	offset      int
	hex         []byte
	itemType    StreamItem
	oldItemType StreamItem
	oldHex      []byte
	oldHexCopy  []byte
	oldAVal     *accounts.Account
	oldHash     []byte
	oldHashCopy []byte
	oldVal      []byte
}

// NewStreamMergeIterator create a brand new StreamMergeIterator
func NewStreamMergeIterator(it *Iterator, s *Stream, trace bool) *StreamMergeIterator {
	smi := &StreamMergeIterator{
		it:    it,
		s:     s,
		trace: trace,
	}
	smi.oldItemType, smi.oldHex, smi.oldAVal, smi.oldHash, smi.oldVal = it.Next()
	return smi
}

// Reset prepares StreamMergeIterator for reuse
func (smi *StreamMergeIterator) Reset(it *Iterator, s *Stream, trace bool) {
	smi.it = it
	smi.s = s
	smi.trace = trace
	smi.ki = 0
	smi.ai = 0
	smi.si = 0
	smi.offset = 0
	smi.oldItemType, smi.oldHex, smi.oldAVal, smi.oldHash, smi.oldVal = it.Next()
}

// Next returns the next item in the merged iterator
func (smi *StreamMergeIterator) Next() (itemType1 StreamItem, hex1 []byte, aValue *accounts.Account, aCode []byte, hash []byte, value []byte) {
	for {
		if smi.trace {
			fmt.Printf("smi.hex %x, smi.ki %d, len(smi.s.keySizes) %d, smi.oldItemType %d smi.oldHex %x\n", smi.hex, smi.ki, len(smi.s.keySizes), smi.oldItemType, smi.oldHex)
		}
		if smi.hex == nil && smi.ki >= len(smi.s.keySizes) && smi.oldItemType == NoItem {
			return NoItem, nil, nil, nil, nil, nil
		}
		if smi.hex == nil && smi.ki < len(smi.s.keySizes) {
			size := int(smi.s.keySizes[smi.ki])
			smi.hex = smi.s.keyBytes[smi.offset : smi.offset+size]
			smi.itemType = smi.s.itemTypes[smi.ki]
			smi.ki++
			smi.offset += size
		}
		hex := smi.hex // Save it to be able to use it even after assigning to nil
		if smi.oldHex == nil {
			smi.hex = nil // To be consumed
			switch smi.itemType {
			case AccountStreamItem:
				ai := smi.ai
				smi.ai++
				if smi.s.aValues[ai] != nil {
					return AccountStreamItem, hex, smi.s.aValues[ai], smi.s.aCodes[ai], nil, nil
				}
			case StorageStreamItem:
				si := smi.si
				smi.si++
				if len(smi.s.sValues[si]) > 0 {
					return StorageStreamItem, hex, nil, nil, nil, smi.s.sValues[si]
				}
			default:
				panic(fmt.Errorf("unexpected stream item type (oldHex == nil): %d", smi.itemType))
			}
		} else if hex == nil {
			if len(smi.oldHexCopy) > 0 {
				smi.oldHexCopy = smi.oldHexCopy[:0]
			}
			smi.oldHexCopy = append(smi.oldHexCopy, smi.oldHex...)
			oldItemType := smi.oldItemType
			oldAVal := smi.oldAVal
			if len(smi.oldHashCopy) > 0 {
				smi.oldHashCopy = smi.oldHashCopy[:0]
			}
			smi.oldHashCopy = append(smi.oldHashCopy, smi.oldHash...)
			oldVal := smi.oldVal
			smi.oldItemType, smi.oldHex, smi.oldAVal, smi.oldHash, smi.oldVal = smi.it.Next()
			return oldItemType, smi.oldHexCopy, oldAVal, nil, smi.oldHashCopy, oldVal
		} else {
			// Special case - account gets deleted
			if smi.itemType == AccountStreamItem && smi.s.aValues[smi.ai] == nil && bytes.HasPrefix(smi.oldHex, hex) {
				smi.oldItemType, smi.oldHex, smi.oldAVal, smi.oldHash, smi.oldVal = smi.it.Next()
			} else {
				switch bytes.Compare(smi.oldHex, hex) {
				case -1:
					if len(smi.oldHexCopy) > 0 {
						smi.oldHexCopy = smi.oldHexCopy[:0]
					}
					smi.oldHexCopy = append(smi.oldHexCopy, smi.oldHex...)
					oldItemType := smi.oldItemType
					oldAVal := smi.oldAVal
					if len(smi.oldHashCopy) > 0 {
						smi.oldHashCopy = smi.oldHashCopy[:0]
					}
					smi.oldHashCopy = append(smi.oldHashCopy, smi.oldHash...)
					oldVal := smi.oldVal
					smi.oldItemType, smi.oldHex, smi.oldAVal, smi.oldHash, smi.oldVal = smi.it.Next()
					return oldItemType, smi.oldHexCopy, oldAVal, nil, smi.oldHashCopy, oldVal
				case 1:
					smi.hex = nil // To be consumed
					switch smi.itemType {
					case AccountStreamItem:
						ai := smi.ai
						smi.ai++
						if smi.s.aValues[ai] != nil {
							return AccountStreamItem, hex, smi.s.aValues[ai], smi.s.aCodes[ai], nil, nil
						}
					case StorageStreamItem:
						si := smi.si
						smi.si++
						if len(smi.s.sValues[si]) > 0 {
							return StorageStreamItem, hex, nil, nil, nil, smi.s.sValues[si]
						}
					default:
						panic(fmt.Errorf("unexpected stream item type (oldHex == nil): %d", smi.itemType))
					}
				case 0:
					smi.hex = nil // To be consumed
					smi.oldItemType, smi.oldHex, smi.oldAVal, smi.oldHash, smi.oldVal = smi.it.Next()
					switch smi.itemType {
					case AccountStreamItem:
						ai := smi.ai
						smi.ai++
						if smi.s.aValues[ai] != nil {
							return AccountStreamItem, hex, smi.s.aValues[ai], smi.s.aCodes[ai], nil, nil
						}
					case StorageStreamItem:
						si := smi.si
						smi.si++
						if len(smi.s.sValues[si]) > 0 {
							return StorageStreamItem, hex, nil, nil, nil, smi.s.sValues[si]
						}
					default:
						panic(fmt.Errorf("unexpected stream item type (oldHex == nil): %d", smi.itemType))
					}
				}
			}
		}
	}
}

// StreamHash computes the hash of a stream, as if it was a trie
func StreamHash(it *StreamMergeIterator, storagePrefixLen int, hb *HashBuilder, trace bool) (libcommon.Hash, error) {
	var succ bytes.Buffer
	var curr bytes.Buffer
	var succStorage bytes.Buffer
	var currStorage bytes.Buffer
	var value bytes.Buffer
	var hashBuf libcommon.Hash
	var hashBufStorage libcommon.Hash
	var hashRef []byte
	var hashRefStorage []byte
	var groups, hasTree, hasHash []uint16 // Separate groups slices for storage items and for accounts
	var aRoot libcommon.Hash
	var aEmptyRoot = true
	var isAccount bool
	var fieldSet uint32
	var itemType, sItemType StreamItem
	var hashData GenStructStepHashData
	var leafData GenStructStepLeafData
	var accData GenStructStepAccountData

	hb.Reset()
	curr.Reset()
	currStorage.Reset()

	makeData := func(fieldSet uint32, hashRef []byte) GenStructStepData {
		if hashRef != nil {
			copy(hashData.Hash[:], hashRef)
			return &hashData
		} else if !isAccount {
			leafData.Value = rlphacks.RlpSerializableBytes(value.Bytes())
			return &leafData
		} else {
			accData.FieldSet = fieldSet
			return &accData
		}
	}

	retain := func(_ []byte) bool { return trace }
	for newItemType, hex, aVal, aCode, hash, val := it.Next(); newItemType != NoItem; newItemType, hex, aVal, aCode, hash, val = it.Next() {
		if newItemType == AccountStreamItem || newItemType == AHashStreamItem {
			// If there was an open storage "sub-stream", close it and set the storage flag on
			if succStorage.Len() > 0 {
				currStorage.Reset()
				currStorage.Write(succStorage.Bytes())
				succStorage.Reset()
				if currStorage.Len() > 0 {
					isAccount = false
					var err error
					groups, hasTree, hasHash, err = GenStructStep(retain, currStorage.Bytes(), succStorage.Bytes(), hb, nil /* hashCollector */, makeData(0, hashRefStorage), groups, hasTree, hasHash, trace)
					if err != nil {
						return libcommon.Hash{}, err
					}
					currStorage.Reset()
					fieldSet += AccountFieldStorageOnly
				}
			} else if itemType == AccountStreamItem && !aEmptyRoot {
				if err := hb.hash(aRoot[:]); err != nil {
					return libcommon.Hash{}, err
				}
				fieldSet += AccountFieldStorageOnly
			}
			curr.Reset()
			curr.Write(succ.Bytes())
			succ.Reset()
			succ.Write(hex)
			if newItemType == AccountStreamItem {
				succ.WriteByte(16)
			}
			if curr.Len() > 0 {
				isAccount = true
				var err error
				groups, hasTree, hasHash, err = GenStructStep(retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, makeData(fieldSet, hashRef), groups, hasTree, hasHash, trace)
				if err != nil {
					return libcommon.Hash{}, err
				}
			}
			itemType = newItemType
			switch itemType {
			case AccountStreamItem:
				var a = aVal
				accData.Balance.Set(&a.Balance)
				accData.Nonce = a.Nonce
				accData.Incarnation = a.Incarnation
				aEmptyRoot = a.IsEmptyRoot()
				copy(aRoot[:], a.Root[:])
				fieldSet = 0
				if !a.Balance.IsZero() {
					fieldSet |= AccountFieldBalanceOnly
				}
				if a.Nonce != 0 {
					fieldSet |= AccountFieldNonceOnly
				}
				if aCode != nil {
					fieldSet |= AccountFieldCodeOnly
					if err := hb.code(aCode); err != nil {
						return libcommon.Hash{}, err
					}
				} else if !a.IsEmptyCodeHash() {
					fieldSet |= AccountFieldCodeOnly
					if err := hb.hash(a.CodeHash[:]); err != nil {
						return libcommon.Hash{}, err
					}
				}
				hashRef = nil
			case AHashStreamItem:
				copy(hashBuf[:], hash)
				hashRef = hashBuf[:]
			}
		} else {
			currStorage.Reset()
			currStorage.Write(succStorage.Bytes())
			succStorage.Reset()
			succStorage.Write(hex[2*storagePrefixLen:])
			if newItemType == StorageStreamItem {
				succStorage.WriteByte(16)
			}
			if currStorage.Len() > 0 {
				isAccount = false
				var err error
				groups, hasTree, hasHash, err = GenStructStep(retain, currStorage.Bytes(), succStorage.Bytes(), hb, nil /* hashCollector */, makeData(0, hashRefStorage), groups, hasTree, hasHash, trace)
				if err != nil {
					return libcommon.Hash{}, err
				}
			}
			sItemType = newItemType
			switch sItemType {
			case StorageStreamItem:
				value.Reset()
				value.Write(val)
				hashRefStorage = nil
			case SHashStreamItem:
				copy(hashBufStorage[:], hash)
				hashRefStorage = hashBufStorage[:]
			}
		}
	}
	// If there was an open storage "sub-stream", close it and set the storage flag on
	if succStorage.Len() > 0 {
		currStorage.Reset()
		currStorage.Write(succStorage.Bytes())
		succStorage.Reset()
		if currStorage.Len() > 0 {
			isAccount = false
			var err error
			_, _, _, err = GenStructStep(retain, currStorage.Bytes(), succStorage.Bytes(), hb, nil /* hashCollector */, makeData(0, hashRefStorage), groups, hasTree, hasHash, trace)
			if err != nil {
				return libcommon.Hash{}, err
			}
			currStorage.Reset()
			fieldSet |= AccountFieldStorageOnly
		}
	} else if itemType == AccountStreamItem && !aEmptyRoot {
		if err := hb.hash(aRoot[:]); err != nil {
			return libcommon.Hash{}, err
		}
		fieldSet |= AccountFieldStorageOnly
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if curr.Len() > 0 {
		isAccount = true
		var err error
		_, _, _, err = GenStructStep(retain, curr.Bytes(), succ.Bytes(), hb, nil /* hashCollector */, makeData(fieldSet, hashRef), groups, hasTree, hasHash, trace)
		if err != nil {
			return libcommon.Hash{}, err
		}
	}
	if trace {
		tt := New(libcommon.Hash{})
		tt.root = hb.root()
		filename := "root.txt"
		f, err1 := os.Create(filename)
		if err1 == nil {
			defer f.Close()
			tt.Print(f)
		}
	}
	if hb.hasRoot() {
		return hb.rootHash(), nil
	}
	return EmptyRoot, nil
}

// HashWithModifications computes the hash of the would-be modified trie, but without any modifications
func HashWithModifications(
	t *Trie,
	aKeys common.Hashes, aValues []*accounts.Account, aCodes [][]byte,
	sKeys common.StorageKeys, sValues [][]byte,
	storagePrefixLen int,
	newStream *Stream, // Streams that will be reused for old and new stream
	hb *HashBuilder, // HashBuilder will be reused
	trace bool,
) (libcommon.Hash, error) {
	keyCount := len(aKeys) + len(sKeys)
	var stream = Stream{
		keyBytes:  make([]byte, len(aKeys)*(2*length.Hash)+len(sKeys)*(4*length.Hash+2*length.Incarnation)),
		keySizes:  make([]uint8, keyCount),
		itemTypes: make([]StreamItem, keyCount),
		aValues:   aValues,
		aCodes:    aCodes,
		sValues:   sValues,
	}
	var accountKeyHex []byte
	var storageKeyHex []byte
	var ki, ai, si int
	// Convert all account keys and storage keys to HEX encoding and merge them into one sorted list
	// when we merge, the keys are never equal
	offset := 0
	for ki < keyCount {
		if accountKeyHex == nil && ai < len(aKeys) {
			accountKeyHex = keybytesToHex(aKeys[ai][:])
			accountKeyHex = accountKeyHex[:len(accountKeyHex)-1]
			ai++
		}
		if storageKeyHex == nil && si < len(sKeys) {
			storageKeyHex = keybytesToHex(sKeys[si][:])
			storageKeyHex = storageKeyHex[:len(storageKeyHex)-1]
			si++
		}
		if accountKeyHex == nil {
			copy(stream.keyBytes[offset:], storageKeyHex)
			stream.keySizes[ki] = uint8(len(storageKeyHex))
			stream.itemTypes[ki] = StorageStreamItem
			ki++
			offset += len(storageKeyHex)
			storageKeyHex = nil // consumed
		} else if storageKeyHex == nil {
			copy(stream.keyBytes[offset:], accountKeyHex)
			stream.keySizes[ki] = uint8(len(accountKeyHex))
			stream.itemTypes[ki] = AccountStreamItem
			ki++
			offset += len(accountKeyHex)
			accountKeyHex = nil // consumed
		} else if bytes.Compare(accountKeyHex, storageKeyHex) < 0 {
			copy(stream.keyBytes[offset:], accountKeyHex)
			stream.keySizes[ki] = uint8(len(accountKeyHex))
			stream.itemTypes[ki] = AccountStreamItem
			ki++
			offset += len(accountKeyHex)
			accountKeyHex = nil // consumed
		} else {
			copy(stream.keyBytes[offset:], storageKeyHex)
			stream.keySizes[ki] = uint8(len(storageKeyHex))
			stream.itemTypes[ki] = StorageStreamItem
			ki++
			offset += len(storageKeyHex)
			storageKeyHex = nil // consumed
		}
	}
	if trace {
		fmt.Printf("len(stream.hexes)=%d\n", keyCount)
		printOffset := 0
		for _, size := range stream.keySizes {
			fmt.Printf("%x\n", stream.keyBytes[printOffset:printOffset+int(size)])
			printOffset += int(size)
		}
		fmt.Printf("accounts\n")
		for i, key := range aKeys {
			fmt.Printf("%x: value present %t, code present %t\n", key, aValues[i] != nil, aCodes[i] != nil)
		}
		fmt.Printf("storage\n")
		for i, key := range sKeys {
			fmt.Printf("%x: %x\n", key, sValues[i])
		}
	}
	rl := NewRetainList(0)
	offset = 0
	for _, size := range stream.keySizes {
		rl.AddHex(stream.keyBytes[offset : offset+int(size)])
		offset += int(size)
	}
	// Now we merge old and new streams, preferring the new
	newStream.Reset()

	oldIt := NewIterator(t, rl, trace)

	it := NewStreamMergeIterator(oldIt, &stream, trace)
	return StreamHash(it, storagePrefixLen, hb, trace)
}
