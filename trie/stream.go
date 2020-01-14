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
	// AccountStreamItem used for marking a key-value pair in the stream as belonging to an account
	AccountStreamItem StreamItem = iota
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
	hexes     [][]byte
	itemTypes []StreamItem
	aValues   []*accounts.Account
	sValues   [][]byte
	hashes    []common.Hash
}

// ToStream generates the stream of key hexes, and corresponding values, with branch nodes
// folded into hashes according to givin ResolveSet `rs`
func ToStream(t *Trie, rs *ResolveSet, trace bool) *Stream {
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	var st Stream
	toStream(t.root, []byte{}, true, rs, hr, true, &st, trace)
	return &st
}

func toStream(nd node, hex []byte, accounts bool, rs *ResolveSet, hr *hasher, force bool, s *Stream, trace bool) {
	switch n := nd.(type) {
	case nil:
	case valueNode:
		if trace {
			fmt.Printf("valueNode %x\n", hex)
		}
		s.hexes = append(s.hexes, hex)
		s.sValues = append(s.sValues, []byte(n))
		s.itemTypes = append(s.itemTypes, StorageStreamItem)
	case *shortNode:
		if trace {
			fmt.Printf("shortNode %x\n", hex)
		}
		hexVal := concat(hex, n.Key...)
		toStream(n.Val, hexVal, accounts, rs, hr, false, s, trace)
	case *duoNode:
		if trace {
			fmt.Printf("duoNode %x\n", hex)
		}
		hashOnly := rs.HashOnly(hex) // Save this because rs can move on to other keys during the recursive invocation
		if hashOnly {
			var hn common.Hash
			if _, err := hr.hash(n, force, hn[:]); err != nil {
				panic(fmt.Sprintf("could not hash duoNode: %v", err))
			}
			s.hexes = append(s.hexes, hex)
			s.hashes = append(s.hashes, hn)
			if accounts {
				s.itemTypes = append(s.itemTypes, AHashStreamItem)
			} else {
				s.itemTypes = append(s.itemTypes, SHashStreamItem)
			}
		} else {
			i1, i2 := n.childrenIdx()
			hex1 := make([]byte, len(hex)+1)
			copy(hex1, hex)
			hex1[len(hex)] = i1
			hex2 := make([]byte, len(hex)+1)
			copy(hex2, hex)
			hex2[len(hex)] = i2
			toStream(n.child1, hex1, accounts, rs, hr, false, s, trace)
			toStream(n.child2, hex2, accounts, rs, hr, false, s, trace)
		}
	case *fullNode:
		if trace {
			fmt.Printf("fullNode %x\n", hex)
		}
		hashOnly := rs.HashOnly(hex) // Save this because rs can move on to other keys during the recursive invocation
		if hashOnly {
			var hn common.Hash
			if _, err := hr.hash(n, force, hn[:]); err != nil {
				panic(fmt.Sprintf("could not hash duoNode: %v", err))
			}
			s.hexes = append(s.hexes, hex)
			s.hashes = append(s.hashes, hn)
			if accounts {
				s.itemTypes = append(s.itemTypes, AHashStreamItem)
			} else {
				s.itemTypes = append(s.itemTypes, SHashStreamItem)
			}
		} else {
			for i, child := range n.Children {
				if child != nil {
					toStream(child, concat(hex, byte(i)), accounts, rs, hr, false, s, trace)
				}
			}
		}
	case *accountNode:
		if trace {
			fmt.Printf("accountNode %x\n", hex)
		}
		s.hexes = append(s.hexes, hex)
		s.aValues = append(s.aValues, &n.Account)
		s.itemTypes = append(s.itemTypes, AccountStreamItem)
		hashOnly := rs.HashOnly(hex)
		if !n.IsEmptyRoot() && !hashOnly {
			if n.storage != nil {
				toStream(n.storage, hex, false /*accounts*/, rs, hr, true /*force*/, s, trace)
			}
		}
	case hashNode:
		if trace {
			fmt.Printf("hashNode %x\n", hex)
		}
		hashOnly := rs.HashOnly(hex)
		if !hashOnly {
			if c := rs.Current(); len(c) == len(hex)+1 && c[len(c)-1] == 16 {
				hashOnly = true
			}
		}
		if hashOnly {
			var hn common.Hash
			copy(hn[:], []byte(n))
			s.hexes = append(s.hexes, hex)
			s.hashes = append(s.hashes, hn)
			if accounts {
				s.itemTypes = append(s.itemTypes, AHashStreamItem)
			} else {
				s.itemTypes = append(s.itemTypes, SHashStreamItem)
			}
		} else {
			panic(fmt.Errorf("unexpected hashNode: %s, at hex: %x (%d)", n, hex, len(hex)))
		}
	default:
		panic(fmt.Errorf("unexpected node: %T", nd))
	}
}

// StreamHash computes the hash of a stream, as if it was a trie
func StreamHash(s *Stream, storagePrefixLen int, trace bool) (common.Hash, error) {
	hb := NewHashBuilder(trace)
	var succ bytes.Buffer
	var curr bytes.Buffer
	var succStorage bytes.Buffer
	var currStorage bytes.Buffer
	var value bytes.Buffer
	var hashRef *common.Hash
	var groups, sGroups []uint16 // Separate groups slices for storage items and for accounts
	var a accounts.Account
	var fieldSet uint32
	var ki, ai, si, hi int
	var itemType, sItemType StreamItem

	hb.Reset()
	curr.Reset()
	currStorage.Reset()

	makeData := func(fieldSet uint32, hashRef *common.Hash) GenStructStepData {
		if hashRef != nil {
			return GenStructStepHashData{*hashRef}
		} else if fieldSet == 0 {
			return GenStructStepLeafData{Value: rlphacks.RlpSerializableBytes(value.Bytes())}
		} else {
			return GenStructStepAccountData{
				FieldSet:    fieldSet,
				StorageSize: a.StorageSize,
				Balance:     &a.Balance,
				Nonce:       a.Nonce,
				Incarnation: a.Incarnation,
			}
		}
	}

	hashOnly := func(_ []byte) bool { return !trace }
	for ki < len(s.hexes) {
		hex := s.hexes[ki]
		newItemType := s.itemTypes[ki]
		if newItemType == AccountStreamItem || newItemType == AHashStreamItem {
			// If there was an open storage "sub-stream", close it and set the storage flag on
			if succStorage.Len() > 0 {
				currStorage.Reset()
				currStorage.Write(succStorage.Bytes())
				succStorage.Reset()
				if currStorage.Len() > 0 {
					var err error
					sGroups, err = GenStructStep(hashOnly, currStorage.Bytes(), succStorage.Bytes(), hb, makeData(AccountFieldSetNotAccount, hashRef), sGroups)
					if err != nil {
						return common.Hash{}, err
					}
					currStorage.Reset()
					fieldSet += AccountFieldRootOnly
				}
			} else if itemType == AccountStreamItem && !a.IsEmptyRoot() {
				if err := hb.hash(a.Root); err != nil {
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
				groups, err = GenStructStep(hashOnly, curr.Bytes(), succ.Bytes(), hb, makeData(fieldSet, hashRef), groups)
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
				a.Copy(s.aValues[ai])
				ai++
				fieldSet = AccountFieldSetNotContract // base level - nonce and balance
				if a.HasStorageSize {
					fieldSet += AccountFieldSSizeOnly
				}
				if !a.IsEmptyCodeHash() {
					fieldSet += AccountFieldCodeHashOnly
					if err := hb.hash(a.CodeHash); err != nil {
						return common.Hash{}, err
					}
				}
			case AHashStreamItem:
				h := s.hashes[hi]
				hi++
				hashRef = &h
			}
		} else {
			currStorage.Reset()
			currStorage.Write(succStorage.Bytes())
			succStorage.Reset()
			succStorage.Write(hex[2*storagePrefixLen+1:])
			if currStorage.Len() > 0 {
				var err error
				sGroups, err = GenStructStep(hashOnly, currStorage.Bytes(), succStorage.Bytes(), hb, makeData(AccountFieldSetNotAccount, hashRef), sGroups)
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
			case SHashStreamItem:
				h := s.hashes[hi]
				hi++
				hashRef = &h
			}
		}
		ki++
	}
	// If there was an open storage "sub-stream", close it and set the storage flag on
	if succStorage.Len() > 0 {
		currStorage.Reset()
		currStorage.Write(succStorage.Bytes())
		succStorage.Reset()
		if currStorage.Len() > 0 {
			var err error
			sGroups, err = GenStructStep(hashOnly, currStorage.Bytes(), succStorage.Bytes(), hb, makeData(AccountFieldSetNotAccount, hashRef), sGroups)
			if err != nil {
				return common.Hash{}, err
			}
			currStorage.Reset()
			fieldSet += AccountFieldRootOnly
		}
	} else if itemType == AccountStreamItem && !a.IsEmptyRoot() {
		if err := hb.hash(a.Root); err != nil {
			return common.Hash{}, err
		}
		fieldSet += AccountFieldRootOnly
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if curr.Len() > 0 {
		var err error
		groups, err = GenStructStep(hashOnly, curr.Bytes(), succ.Bytes(), hb, makeData(fieldSet, hashRef), groups)
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
	trace bool,
) (common.Hash, error) {
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	keyCount := len(aKeys) + len(sKeys)
	var stream = Stream{
		hexes:     make([][]byte, keyCount),
		itemTypes: make([]StreamItem, keyCount),
		aValues:   aValues,
		sValues:   sValues,
	}
	var accountKeyHex []byte
	var storageKeyHex []byte
	var ki, ai, si int
	// Convert all account keys and storage keys to HEX encoding and merge them into one sorted list
	// when we merge, the keys are never equal
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
			stream.hexes[ki] = storageKeyHex
			stream.itemTypes[ki] = StorageStreamItem
			storageKeyHex = nil // consumed
			ki++
		} else if storageKeyHex == nil {
			stream.hexes[ki] = accountKeyHex
			stream.itemTypes[ki] = AccountStreamItem
			accountKeyHex = nil // consumed
			ki++
		} else if bytes.Compare(accountKeyHex, storageKeyHex) < 0 {
			stream.hexes[ki] = accountKeyHex
			stream.itemTypes[ki] = AccountStreamItem
			accountKeyHex = nil // consumed
			ki++
		} else {
			stream.hexes[ki] = storageKeyHex
			stream.itemTypes[ki] = StorageStreamItem
			storageKeyHex = nil // consumed
			ki++
		}
	}
	if trace {
		fmt.Printf("len(stream.hexes)=%d\n", len(stream.hexes))
		for _, hex := range stream.hexes {
			fmt.Printf("%x\n", hex)
		}
	}
	rs := &ResolveSet{minLength: 0, hexes: sortable(stream.hexes), inited: true, lteIndex: 0}
	oldStream := ToStream(t, rs, false)
	if trace {
		fmt.Printf("len(oldStream.hexes)=%d\n", len(oldStream.hexes))
		for _, hex := range oldStream.hexes {
			fmt.Printf("%x\n", hex)
		}
	}
	// Now we merge old and new streams, preferring the new
	var oldKi, oldAi, oldSi, oldHi int
	ki = 0
	ai = 0
	si = 0
	var oldHex, hex []byte
	var oldItemType, itemType StreamItem
	var newStream Stream

	oldKeyCount := len(oldStream.hexes)
	for hex != nil || oldHex != nil || oldKi < oldKeyCount || ki < keyCount {
		if oldHex == nil && oldKi < oldKeyCount {
			oldHex = oldStream.hexes[oldKi]
			oldItemType = oldStream.itemTypes[oldKi]
			oldKi++
		}
		if hex == nil && ki < keyCount {
			hex = stream.hexes[ki]
			itemType = stream.itemTypes[ki]
			ki++
		}
		if oldHex == nil {
			switch itemType {
			case AccountStreamItem:
				if stream.aValues[ai] != nil {
					newStream.hexes = append(newStream.hexes, hex)
					newStream.aValues = append(newStream.aValues, stream.aValues[ai])
					newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
				}
				ai++
			case StorageStreamItem:
				if len(stream.sValues[si]) > 0 {
					newStream.hexes = append(newStream.hexes, hex)
					newStream.sValues = append(newStream.sValues, stream.sValues[si])
					newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
				}
				si++
			default:
				return common.Hash{}, fmt.Errorf("unexpected stream item type (oldHex == nil): %d", itemType)
			}
			hex = nil // consumed
		} else if hex == nil {
			newStream.hexes = append(newStream.hexes, oldHex)
			switch oldItemType {
			case AccountStreamItem:
				newStream.aValues = append(newStream.aValues, oldStream.aValues[oldAi])
				oldAi++
				newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
			case StorageStreamItem:
				newStream.sValues = append(newStream.sValues, oldStream.sValues[oldSi])
				oldSi++
				newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
			case AHashStreamItem:
				newStream.hashes = append(newStream.hashes, oldStream.hashes[oldHi])
				oldHi++
				newStream.itemTypes = append(newStream.itemTypes, AHashStreamItem)
			case SHashStreamItem:
				newStream.hashes = append(newStream.hashes, oldStream.hashes[oldHi])
				oldHi++
				newStream.itemTypes = append(newStream.itemTypes, SHashStreamItem)
			}
			oldHex = nil // consumed
		} else {
			// Special case - account gets deleted
			if itemType == AccountStreamItem && stream.aValues[ai] == nil && bytes.HasPrefix(oldHex, hex) {
				switch oldItemType {
				case AccountStreamItem:
					oldAi++
				case StorageStreamItem:
					oldSi++
				case AHashStreamItem, SHashStreamItem:
					oldHi++
				}
				oldHex = nil
			} else {
				switch bytes.Compare(oldHex, hex) {
				case -1:
					newStream.hexes = append(newStream.hexes, oldHex)
					switch oldItemType {
					case AccountStreamItem:
						newStream.aValues = append(newStream.aValues, oldStream.aValues[oldAi])
						oldAi++
						newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
					case StorageStreamItem:
						newStream.sValues = append(newStream.sValues, oldStream.sValues[oldSi])
						oldSi++
						newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
					case AHashStreamItem:
						newStream.hashes = append(newStream.hashes, oldStream.hashes[oldHi])
						oldHi++
						newStream.itemTypes = append(newStream.itemTypes, AHashStreamItem)
					case SHashStreamItem:
						newStream.hashes = append(newStream.hashes, oldStream.hashes[oldHi])
						oldHi++
						newStream.itemTypes = append(newStream.itemTypes, SHashStreamItem)
					}
					oldHex = nil // consumed
				case 1:
					switch itemType {
					case AccountStreamItem:
						if stream.aValues[ai] != nil {
							newStream.hexes = append(newStream.hexes, hex)
							newStream.aValues = append(newStream.aValues, stream.aValues[ai])
							newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
						}
						ai++
					case StorageStreamItem:
						if len(stream.sValues[si]) > 0 {
							newStream.hexes = append(newStream.hexes, hex)
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
							newStream.hexes = append(newStream.hexes, hex)
							newStream.aValues = append(newStream.aValues, stream.aValues[ai])
							newStream.itemTypes = append(newStream.itemTypes, AccountStreamItem)
						}
						ai++
						oldAi++ // Discard old values
					case StorageStreamItem:
						if len(stream.sValues[si]) > 0 {
							newStream.hexes = append(newStream.hexes, hex)
							newStream.sValues = append(newStream.sValues, stream.sValues[si])
							newStream.itemTypes = append(newStream.itemTypes, StorageStreamItem)
						}
						si++
						oldSi++ // Discard old values
					default:
						return common.Hash{}, fmt.Errorf("unexpected stream item type (oldHex == hex): %d", itemType)
					}
					hex = nil    // consumed
					oldHex = nil // consumed
				}
			}
		}
	}
	if trace {
		fmt.Printf("len(newStream.hexes)=%d\n", len(newStream.hexes))
		for _, hex := range newStream.hexes {
			fmt.Printf("%x\n", hex)
		}
	}
	return StreamHash(&newStream, storagePrefixLen, trace)
}
