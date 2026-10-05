// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type StreamItem uint8

const (
	NoItem StreamItem = iota
	AccountStreamItem
	StorageStreamItem
)

const (
	AccountFieldNonceOnly   uint32 = 0x01
	AccountFieldBalanceOnly uint32 = 0x02
	AccountFieldStorageOnly uint32 = 0x04
	AccountFieldCodeOnly    uint32 = 0x08
)

type GenStructStepAccountData struct {
	FieldSet    uint32
	Balance     uint256.Int
	Nonce       uint64
	Incarnation uint64
}

func (GenStructStepAccountData) GenStructStepData() {}

type HashStreamIterator interface {
	Next() (itemType StreamItem, hex []byte, aValue *accounts.Account, aCode []byte, hash []byte, value []byte)
}

func StreamHashIterator(it HashStreamIterator, storagePrefixLen int, hb *HashBuilder, trace bool) (common.Hash, error) {
	return streamHash(it, storagePrefixLen, hb, trace)
}

func streamHash(it HashStreamIterator, storagePrefixLen int, hb *HashBuilder, trace bool) (common.Hash, error) {
	var succ bytes.Buffer
	var curr bytes.Buffer
	var succStorage bytes.Buffer
	var currStorage bytes.Buffer
	var value bytes.Buffer
	var groups, hasTree, hasHash []uint16
	var aRoot common.Hash
	aEmptyRoot := true
	var isAccount bool
	var fieldSet uint32
	var itemType, sItemType StreamItem
	var leafData GenStructStepLeafData
	var accData GenStructStepAccountData

	hb.Reset()
	curr.Reset()
	currStorage.Reset()

	makeData := func(fieldSet uint32) GenStructStepData {
		if !isAccount {
			leafData.Value = rlp.RlpSerializableBytes(value.Bytes())
			return &leafData
		}
		accData.FieldSet = fieldSet
		return &accData
	}

	retain := func(_ []byte) bool { return trace }
	for newItemType, hex, aVal, aCode, _, val := it.Next(); newItemType != NoItem; newItemType, hex, aVal, aCode, _, val = it.Next() {
		if newItemType == AccountStreamItem {
			if succStorage.Len() > 0 {
				currStorage.Reset()
				currStorage.Write(succStorage.Bytes())
				succStorage.Reset()
				if currStorage.Len() > 0 {
					isAccount = false
					var err error
					groups, hasTree, hasHash, err = GenStructStep(retain, currStorage.Bytes(), succStorage.Bytes(), hb, nil, makeData(0), groups, hasTree, hasHash, trace)
					if err != nil {
						return common.Hash{}, err
					}
					currStorage.Reset()
					fieldSet += AccountFieldStorageOnly
				}
			} else if itemType == AccountStreamItem && !aEmptyRoot {
				if err := hb.hash(aRoot[:]); err != nil {
					return common.Hash{}, err
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
				groups, hasTree, hasHash, err = GenStructStep(retain, curr.Bytes(), succ.Bytes(), hb, nil, makeData(fieldSet), groups, hasTree, hasHash, trace)
				if err != nil {
					return common.Hash{}, err
				}
			}
			itemType = newItemType
			if itemType == AccountStreamItem {
				a := aVal
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
						return common.Hash{}, err
					}
				} else if !a.IsEmptyCodeHash() {
					fieldSet |= AccountFieldCodeOnly
					codeHashValue := a.CodeHash.Value()
					if err := hb.hash(codeHashValue[:]); err != nil {
						return common.Hash{}, err
					}
				}
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
				groups, hasTree, hasHash, err = GenStructStep(retain, currStorage.Bytes(), succStorage.Bytes(), hb, nil, makeData(0), groups, hasTree, hasHash, trace)
				if err != nil {
					return common.Hash{}, err
				}
			}
			sItemType = newItemType
			if sItemType == StorageStreamItem {
				value.Reset()
				value.Write(val)
			}
		}
	}
	if succStorage.Len() > 0 {
		currStorage.Reset()
		currStorage.Write(succStorage.Bytes())
		succStorage.Reset()
		if currStorage.Len() > 0 {
			isAccount = false
			var err error
			_, _, _, err = GenStructStep(retain, currStorage.Bytes(), succStorage.Bytes(), hb, nil, makeData(0), groups, hasTree, hasHash, trace)
			if err != nil {
				return common.Hash{}, err
			}
			currStorage.Reset()
			fieldSet |= AccountFieldStorageOnly
		}
	} else if itemType == AccountStreamItem && !aEmptyRoot {
		if err := hb.hash(aRoot[:]); err != nil {
			return common.Hash{}, err
		}
		fieldSet |= AccountFieldStorageOnly
	}
	curr.Reset()
	curr.Write(succ.Bytes())
	succ.Reset()
	if curr.Len() > 0 {
		isAccount = true
		var err error
		_, _, _, err = GenStructStep(retain, curr.Bytes(), succ.Bytes(), hb, nil, makeData(fieldSet), groups, hasTree, hasHash, trace)
		if err != nil {
			return common.Hash{}, err
		}
	}
	if hb.hasRoot() {
		return hb.rootHash(), nil
	}
	return empty.RootHash, nil
}
