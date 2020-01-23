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
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
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
)

// Stream represents the collection of key-value pairs, sorted by keys, where values may belong
// to three different types - accounts, strage item leaves, and intermediate hashes
type Stream struct {
	keyBytes  []byte
	keySizes  []uint8
	itemTypes []StreamItem
	aValues   []*accounts.Account
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
	if len(s.sValues) > 0 {
		s.sValues = s.sValues[:0]
	}
	if len(s.hashes) > 0 {
		s.hashes = s.hashes[:0]
	}
}

// Iterator helps iterate over a trie according to a given resolve set
type Iterator struct {
	hr           *hasher
	rs           *ResolveSet
	hn           common.Hash
	hex          []byte
	nodeStack    []node
	iStack       []int
	lenStack     []int
	accountStack []bool
	forceStack   []bool
	trace        bool
}

// NewIterator creates a new iterator from scratch from a given trie and resolve set
func NewIterator(t *Trie, rs *ResolveSet, hr *hasher, trace bool) *Iterator {
	return &Iterator{
		hr:           hr,
		rs:           rs,
		hex:          []byte{},
		nodeStack:    []node{t.root},
		iStack:       []int{0},
		lenStack:     []int{0},
		accountStack: []bool{true},
		forceStack:   []bool{true},
		trace:        trace,
	}
}

// Reset prepares iterator to be reused
func (it *Iterator) Reset(t *Trie, rs *ResolveSet, trace bool) {
	it.rs = rs
	it.hex = it.hex[:0]
	if len(it.nodeStack) > 0 {
		it.nodeStack = it.nodeStack[:0]
	}
	it.nodeStack = append(it.nodeStack, t.root)
	if len(it.iStack) > 0 {
		it.iStack = it.iStack[:0]
	}
	it.lenStack = append(it.lenStack, 0)
	if len(it.accountStack) > 0 {
		it.accountStack = it.accountStack[:0]
	}
	it.accountStack = append(it.accountStack, true)
	if len(it.forceStack) > 0 {
		it.forceStack = it.forceStack[:0]
	}
	it.forceStack = append(it.forceStack, true)
	it.trace = trace
}

// Next delivers the next item from the iterator
func (it *Iterator) Next() (itemType StreamItem, hex1 []byte, aValue *accounts.Account, hash []byte, value []byte) {
	for {
		l := len(it.nodeStack)
		if l == 0 {
			return NoItem, nil, nil, nil, nil
		}
		nd := it.nodeStack[l-1]
		hexLen := it.lenStack[l-1]
		it.hex = it.hex[:hexLen]
		index := it.iStack[l-1]
		accounts := it.accountStack[l-1]
		force := it.forceStack[l-1]
		it.nodeStack = it.nodeStack[:l-1]
		it.iStack = it.iStack[:l-1]
		it.lenStack = it.lenStack[:l-1]
		it.accountStack = it.accountStack[:l-1]
		it.forceStack = it.forceStack[:l-1]
		switch n := nd.(type) {
		case nil:
		case valueNode:
			if it.trace {
				fmt.Printf("valueNode %x\n", it.hex)
			}
			return StorageStreamItem, it.hex, nil, nil, []byte(n)
		case *shortNode:
			if it.trace {
				fmt.Printf("shortNode %x\n", it.hex)
			}
			it.hex = append(it.hex, n.Key...)
			it.nodeStack = append(it.nodeStack, n.Val)
			it.iStack = append(it.iStack, 0)
			it.lenStack = append(it.lenStack, len(it.hex))
			it.accountStack = append(it.accountStack, accounts)
			it.forceStack = append(it.forceStack, false)
		case *duoNode:
			if it.trace {
				fmt.Printf("duoNode %x\n", it.hex)
			}
			hashOnly := it.rs.HashOnly(it.hex) // Save this because rs can move on to other keys during the recursive invocation
			if hashOnly {
				if _, err := it.hr.hash(n, force, it.hn[:]); err != nil {
					panic(fmt.Sprintf("could not hash duoNode: %v", err))
				}
				if accounts {
					return AHashStreamItem, it.hex, nil, it.hn[:], nil
				} else {
					return SHashStreamItem, it.hex, nil, it.hn[:], nil
				}
			} else {
				i1, i2 := n.childrenIdx()
				hexLen := len(it.hex)
				if index <= int(i1) {
					it.nodeStack = append(it.nodeStack, n)
					it.iStack = append(it.iStack, int(i2))
					it.lenStack = append(it.lenStack, hexLen)
					it.accountStack = append(it.accountStack, accounts)
					it.forceStack = append(it.forceStack, force)
					it.hex = append(it.hex, i1)
					it.nodeStack = append(it.nodeStack, n.child1)
					it.iStack = append(it.iStack, 0)
					it.lenStack = append(it.lenStack, len(it.hex))
					it.accountStack = append(it.accountStack, accounts)
					it.forceStack = append(it.forceStack, false)
				} else {
					it.hex = append(it.hex, i2)
					it.nodeStack = append(it.nodeStack, n.child2)
					it.iStack = append(it.iStack, 0)
					it.lenStack = append(it.lenStack, len(it.hex))
					it.accountStack = append(it.accountStack, accounts)
					it.forceStack = append(it.forceStack, false)
				}
			}
		case *fullNode:
			if it.trace {
				fmt.Printf("fullNode %x[%d]\n", it.hex, index)
			}
			hashOnly := it.rs.HashOnly(it.hex) // Save this because rs can move on to other keys during the recursive invocation
			if hashOnly {
				if _, err := it.hr.hash(n, force, it.hn[:]); err != nil {
					panic(fmt.Sprintf("could not hash duoNode: %v", err))
				}
				if accounts {
					return AHashStreamItem, it.hex, nil, it.hn[:], nil
				} else {
					return SHashStreamItem, it.hex, nil, it.hn[:], nil
				}
			} else {
				hexLen := len(it.hex)
				var i1, i2 int
				i1Found := false
				i2Found := false
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
					it.nodeStack = append(it.nodeStack, n)
					it.iStack = append(it.iStack, int(i2))
					it.lenStack = append(it.lenStack, hexLen)
					it.accountStack = append(it.accountStack, accounts)
					it.forceStack = append(it.forceStack, force)
					it.hex = append(it.hex, byte(i1))
					it.nodeStack = append(it.nodeStack, n.Children[i1])
					it.iStack = append(it.iStack, 0)
					it.lenStack = append(it.lenStack, len(it.hex))
					it.accountStack = append(it.accountStack, accounts)
					it.forceStack = append(it.forceStack, false)
				} else {
					it.hex = append(it.hex, byte(i1))
					it.nodeStack = append(it.nodeStack, n.Children[i1])
					it.iStack = append(it.iStack, 0)
					it.lenStack = append(it.lenStack, len(it.hex))
					it.accountStack = append(it.accountStack, accounts)
					it.forceStack = append(it.forceStack, false)
				}
			}
		case *accountNode:
			if it.trace {
				fmt.Printf("accountNode %x\n", it.hex)
			}
			hashOnly := it.rs.HashOnly(it.hex)
			if !n.IsEmptyRoot() && !hashOnly {
				if n.storage != nil {
					it.nodeStack = append(it.nodeStack, n.storage)
					it.iStack = append(it.iStack, 0)
					it.lenStack = append(it.lenStack, len(it.hex))
					it.accountStack = append(it.accountStack, false)
					it.forceStack = append(it.forceStack, true)
				}
			}
			return AccountStreamItem, it.hex, &n.Account, nil, nil
		case hashNode:
			if it.trace {
				fmt.Printf("hashNode %x\n", it.hex)
			}
			hashOnly := it.rs.HashOnly(it.hex)
			if !hashOnly {
				if c := it.rs.Current(); len(c) == len(it.hex)+1 && c[len(c)-1] == 16 {
					hashOnly = true
				}
			}
			if hashOnly {
				if accounts {
					return AHashStreamItem, it.hex, nil, []byte(n), nil
				} else {
					return SHashStreamItem, it.hex, nil, []byte(n), nil
				}
			} else {
				panic(fmt.Errorf("unexpected hashNode: %s, at hex: %x (%d)", n, it.hex, len(it.hex)))
			}
		default:
			panic(fmt.Errorf("unexpected node: %T", nd))
		}
	}
}

// StreamHash computes the hash of a stream, as if it was a trie
func StreamHash(s *Stream, storagePrefixLen int, hb *HashBuilder, trace bool) (common.Hash, error) {
	var succ bytes.Buffer
	var curr bytes.Buffer
	var succStorage bytes.Buffer
	var currStorage bytes.Buffer
	var value bytes.Buffer
	var hashRef []byte
	var hashRefStorage []byte
	var groups, sGroups []uint16 // Separate groups slices for storage items and for accounts
	var aRoot common.Hash
	var aEmptyRoot = true
	var fieldSet uint32
	var ki, ai, si, hi int
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
		} else if fieldSet == AccountFieldSetNotAccount {
			leafData.Value = rlphacks.RlpSerializableBytes(value.Bytes())
			return &leafData
		} else {
			accData.FieldSet = fieldSet
			return &accData
		}
	}

	hashOnly := func(_ []byte) bool { return !trace }
	offset := 0
	for ki < len(s.keySizes) {
		size := int(s.keySizes[ki])
		hex := s.keyBytes[offset : offset+size]
		newItemType := s.itemTypes[ki]
		if newItemType == AccountStreamItem || newItemType == AHashStreamItem {
			// If there was an open storage "sub-stream", close it and set the storage flag on
			if succStorage.Len() > 0 {
				currStorage.Reset()
				currStorage.Write(succStorage.Bytes())
				succStorage.Reset()
				if currStorage.Len() > 0 {
					var err error
					sGroups, err = GenStructStep(hashOnly, currStorage.Bytes(), succStorage.Bytes(), hb, makeData(AccountFieldSetNotAccount, hashRefStorage), sGroups, trace)
					if err != nil {
						return common.Hash{}, err
					}
					currStorage.Reset()
					fieldSet += AccountFieldRootOnly
				}
			} else if itemType == AccountStreamItem && !aEmptyRoot {
				if err := hb.hash(aRoot[:]); err != nil {
					return common.Hash{}, err
				}
				fieldSet += AccountFieldRootOnly
			}
			curr.Reset()
			curr.Write(succ.Bytes())
			succ.Reset()
			succ.Write(hex)
			if curr.Len() > 0 {
				var err error
				groups, err = GenStructStep(hashOnly, curr.Bytes(), succ.Bytes(), hb, makeData(fieldSet, hashRef), groups, trace)
				if err != nil {
					return common.Hash{}, err
				}
			}
			itemType = newItemType
			switch itemType {
			case AccountStreamItem:
				if s.aValues[ai] == nil {
					return common.Hash{}, fmt.Errorf("s.aValues[%d] == nil", ai)
				}
				var a *accounts.Account = s.aValues[ai]
				accData.StorageSize = a.StorageSize
				accData.Balance.Set(&a.Balance)
				accData.Nonce = a.Nonce
				accData.Incarnation = a.Incarnation
				aEmptyRoot = a.IsEmptyRoot()
				copy(aRoot[:], a.Root[:])
				ai++
				fieldSet = AccountFieldSetNotContract // base level - nonce and balance
				if a.HasStorageSize {
					fieldSet += AccountFieldSSizeOnly
				}
				if !a.IsEmptyCodeHash() {
					fieldSet += AccountFieldCodeHashOnly
					if err := hb.hash(a.CodeHash[:]); err != nil {
						return common.Hash{}, err
					}
				}
				hashRef = nil
			case AHashStreamItem:
				hashRef = s.hashes[hi : hi+common.HashLength]
				hi += common.HashLength

			}
		} else {
			currStorage.Reset()
			currStorage.Write(succStorage.Bytes())
			succStorage.Reset()
			succStorage.Write(hex[2*storagePrefixLen+1:])
			if currStorage.Len() > 0 {
				var err error
				sGroups, err = GenStructStep(hashOnly, currStorage.Bytes(), succStorage.Bytes(), hb, makeData(AccountFieldSetNotAccount, hashRefStorage), sGroups, trace)
				if err != nil {
					return common.Hash{}, err
				}
			}
			sItemType = newItemType
			switch sItemType {
			case StorageStreamItem:
				v := s.sValues[si]
				si++
				value.Reset()
				value.Write(v)
				hashRefStorage = nil
			case SHashStreamItem:
				hashRefStorage = s.hashes[hi : hi+common.HashLength]
				hi += common.HashLength
			}
		}
		ki++
		offset += size
	}
	// If there was an open storage "sub-stream", close it and set the storage flag on
	if succStorage.Len() > 0 {
		currStorage.Reset()
		currStorage.Write(succStorage.Bytes())
		succStorage.Reset()
		if currStorage.Len() > 0 {
			var err error
			_, err = GenStructStep(hashOnly, currStorage.Bytes(), succStorage.Bytes(), hb, makeData(AccountFieldSetNotAccount, hashRefStorage), sGroups, trace)
			if err != nil {
				return common.Hash{}, err
			}
			currStorage.Reset()
			fieldSet += AccountFieldRootOnly
		}
	} else if itemType == AccountStreamItem && !aEmptyRoot {
		if err := hb.hash(aRoot[:]); err != nil {
			return common.Hash{}, err
		}
		fieldSet += AccountFieldRootOnly
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if curr.Len() > 0 {
		var err error
		_, err = GenStructStep(hashOnly, curr.Bytes(), succ.Bytes(), hb, makeData(fieldSet, hashRef), groups, trace)
		if err != nil {
			return common.Hash{}, err
		}
	}
	if trace {
		filename := "root.txt"
		f, err1 := os.Create(filename)
		if err1 == nil {
			defer f.Close()
			hb.root().print(f)
		}
	}
	return hb.rootHash(), nil
}

// HashWithModifications computes the hash of the would-be modified trie, but without any modifications
func HashWithModifications(
	t *Trie,
	aKeys common.Hashes, aValues []*accounts.Account,
	sKeys common.StorageKeys, sValues [][]byte,
	storagePrefixLen int,
	oldStream, newStream *Stream, // Streams that will be reused for old and new stream
	hb *HashBuilder, // HashBuilder will be reused
	trace bool,
) (common.Hash, error) {
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	keyCount := len(aKeys) + len(sKeys)
	var stream = Stream{
		keyBytes:  make([]byte, len(aKeys)*(2*common.HashLength+1)+len(sKeys)*(4*common.HashLength+2)),
		keySizes:  make([]uint8, keyCount),
		itemTypes: make([]StreamItem, keyCount),
		aValues:   aValues,
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
			ai++
		}
		if storageKeyHex == nil && si < len(sKeys) {
			storageKeyHex = concat(keybytesToHex(sKeys[si][:storagePrefixLen]), keybytesToHex(sKeys[si][storagePrefixLen:])...)
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
	}
	rs := NewResolveSet(0)
	offset = 0
	for _, size := range stream.keySizes {
		rs.AddHex(stream.keyBytes[offset : offset+int(size)])
		offset += int(size)
	}
	// Now we merge old and new streams, preferring the new
	ki = 0
	ai = 0
	si = 0
	var hex []byte
	var itemType StreamItem
	newStream.Reset()

	oldIt := NewIterator(t, rs, hr, trace)
	oldItemType, oldHex, oldAVal, oldHash, oldVal := oldIt.Next()

	offset = 0
	for hex != nil || oldItemType != NoItem || ki < keyCount {
		if hex == nil && ki < keyCount {
			size := int(stream.keySizes[ki])
			hex = stream.keyBytes[offset : offset+size]
			itemType = stream.itemTypes[ki]
			ki++
			offset += size
		}
		if oldHex == nil {
			switch itemType {
			case AccountStreamItem:
				if stream.aValues[ai] != nil {
					newStream.keyBytes = append(newStream.keyBytes, hex...)
					newStream.keySizes = append(newStream.keySizes, uint8(len(hex)))
					newStream.aValues = append(newStream.aValues, stream.aValues[ai])
					newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
				}
				ai++
			case StorageStreamItem:
				if len(stream.sValues[si]) > 0 {
					newStream.keyBytes = append(newStream.keyBytes, hex...)
					newStream.keySizes = append(newStream.keySizes, uint8(len(hex)))
					newStream.sValues = append(newStream.sValues, stream.sValues[si])
					newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
				}
				si++
			default:
				return common.Hash{}, fmt.Errorf("unexpected stream item type (oldHex == nil): %d", itemType)
			}
			hex = nil // consumed
		} else if hex == nil {
			newStream.keyBytes = append(newStream.keyBytes, oldHex...)
			newStream.keySizes = append(newStream.keySizes, uint8(len(oldHex)))
			switch oldItemType {
			case AccountStreamItem:
				newStream.aValues = append(newStream.aValues, oldAVal)
				newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
			case StorageStreamItem:
				newStream.sValues = append(newStream.sValues, oldVal)
				newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
			case AHashStreamItem:
				newStream.hashes = append(newStream.hashes, oldHash...)
				newStream.itemTypes = append(newStream.itemTypes, AHashStreamItem)
			case SHashStreamItem:
				newStream.hashes = append(newStream.hashes, oldHash...)
				newStream.itemTypes = append(newStream.itemTypes, SHashStreamItem)
			}
			oldItemType, oldHex, oldAVal, oldHash, oldVal = oldIt.Next()
		} else {
			// Special case - account gets deleted
			if itemType == AccountStreamItem && stream.aValues[ai] == nil && bytes.HasPrefix(oldHex, hex) {
				oldHex = nil
			} else {
				switch bytes.Compare(oldHex, hex) {
				case -1:
					newStream.keyBytes = append(newStream.keyBytes, oldHex...)
					newStream.keySizes = append(newStream.keySizes, uint8(len(oldHex)))
					switch oldItemType {
					case AccountStreamItem:
						newStream.aValues = append(newStream.aValues, oldAVal)
						newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
					case StorageStreamItem:
						newStream.sValues = append(newStream.sValues, oldVal)
						newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
					case AHashStreamItem:
						newStream.hashes = append(newStream.hashes, oldHash...)
						newStream.itemTypes = append(newStream.itemTypes, AHashStreamItem)
					case SHashStreamItem:
						newStream.hashes = append(newStream.hashes, oldHash...)
						newStream.itemTypes = append(newStream.itemTypes, SHashStreamItem)
					}
					oldItemType, oldHex, oldAVal, oldHash, oldVal = oldIt.Next()
				case 1:
					switch itemType {
					case AccountStreamItem:
						if stream.aValues[ai] != nil {
							newStream.keyBytes = append(newStream.keyBytes, hex...)
							newStream.keySizes = append(newStream.keySizes, uint8(len(hex)))
							newStream.aValues = append(newStream.aValues, stream.aValues[ai])
							newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
						}
						ai++
					case StorageStreamItem:
						if len(stream.sValues[si]) > 0 {
							newStream.keyBytes = append(newStream.keyBytes, hex...)
							newStream.keySizes = append(newStream.keySizes, uint8(len(hex)))
							newStream.sValues = append(newStream.sValues, stream.sValues[si])
							newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
						}
						si++
					default:
						return common.Hash{}, fmt.Errorf("unexpected stream item type (oldHex > hex): %d", itemType)
					}
					hex = nil // consumed
				case 0:
					switch itemType {
					case AccountStreamItem:
						if stream.aValues[ai] != nil {
							newStream.keyBytes = append(newStream.keyBytes, hex...)
							newStream.keySizes = append(newStream.keySizes, uint8(len(hex)))
							newStream.aValues = append(newStream.aValues, stream.aValues[ai])
							newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
						}
						ai++
					case StorageStreamItem:
						if len(stream.sValues[si]) > 0 {
							newStream.keyBytes = append(newStream.keyBytes, hex...)
							newStream.keySizes = append(newStream.keySizes, uint8(len(hex)))
							newStream.sValues = append(newStream.sValues, stream.sValues[si])
							newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
						}
						si++
					default:
						return common.Hash{}, fmt.Errorf("unexpected stream item type (oldHex == hex): %d", itemType)
					}
					hex = nil // consumed
					oldItemType, oldHex, oldAVal, oldHash, oldVal = oldIt.Next()
				}
			}
		}
	}
	if trace {
		fmt.Printf("len(newStream.hexes)=%d\n", len(newStream.keySizes))
		printOffset := 0
		for _, size := range newStream.keySizes {
			fmt.Printf("%x\n", newStream.keyBytes[printOffset:printOffset+int(size)])
			printOffset += int(size)
		}
	}
	return StreamHash(newStream, storagePrefixLen, hb, trace)
}
