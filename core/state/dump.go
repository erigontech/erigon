// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type trieHasher interface {
	GetTrieHash() common.Hash
}

type DumperSource interface {
	GetKey([]byte) []byte
}

type StateDumper struct {
	source DumperSource
	db     ethdb.Getter
}

// DumpAccount represents an account in the state.
type DumpAccount struct {
	Balance     string            `json:"balance"`
	Nonce       uint64            `json:"nonce"`
	Root        string            `json:"root"`
	CodeHash    string            `json:"codeHash"`
	Code        string            `json:"code,omitempty"`
	Storage     map[string]string `json:"storage,omitempty"`
	StorageSize *uint64           `json:",omitempty"`
	Address     *common.Address   `json:"address,omitempty"` // Address only present in iterative (line-by-line) mode
	SecureKey   hexutil.Bytes     `json:"key,omitempty"`     // If we don't have address, we can output the key
}

// Dump represents the full dump in a collected format, as one large map.
type Dump struct {
	Root     string                         `json:"root"`
	Accounts map[common.Address]DumpAccount `json:"accounts"`
}

// iterativeDump is a 'collector'-implementation which dump output line-by-line iteratively.
type iterativeDump struct {
	*json.Encoder
}

// IteratorDump is an implementation for iterating over data.
type IteratorDump struct {
	Root     string                         `json:"root"`
	Accounts map[common.Address]DumpAccount `json:"accounts"`
	Next     []byte                         `json:"next,omitempty"` // nil if no more accounts
}

// Collector interface which the state trie calls during iteration
type collector interface {
	onRoot(common.Hash)
	onAccount(common.Address, DumpAccount)
}

func (d *Dump) onRoot(root common.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

func (d *Dump) onAccount(addr common.Address, account DumpAccount) {
	d.Accounts[addr] = account
}
func (d *IteratorDump) onRoot(root common.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

func (d *IteratorDump) onAccount(addr common.Address, account DumpAccount) {
	d.Accounts[addr] = account
}

func (d iterativeDump) onAccount(addr common.Address, account DumpAccount) {
	dumpAccount := &DumpAccount{
		Balance:   account.Balance,
		Nonce:     account.Nonce,
		Root:      account.Root,
		CodeHash:  account.CodeHash,
		Code:      account.Code,
		Storage:   account.Storage,
		SecureKey: account.SecureKey,
		Address:   nil,
	}
	if addr != (common.Address{}) {
		dumpAccount.Address = &addr
	}
	//nolint:errcheck
	d.Encode(dumpAccount)
}

func (d iterativeDump) onRoot(root common.Hash) {
	//nolint:errcheck
	d.Encoder.Encode(struct {
		Root common.Hash `json:"root"`
	}{root})
}
func (tds *StateDumper) dump(c collector, excludeCode, excludeStorage, excludeMissingPreimages bool, start []byte, maxResults int) (nextKey []byte) {
	emptyAddress := common.Address{}
	missingPreimages := 0

	if hasher, ok := tds.source.(trieHasher); ok {
		h := hasher.GetTrieHash()
		c.onRoot(h)
	}

	var acc accounts.Account
	numberOfResults := 0
	err := tds.db.Walk(dbutils.CurrentStateBucket, start, 0, func(k, v []byte) (bool, error) {
		nextKey = k
		if numberOfResults >= maxResults {
			return false, nil
		}

		if len(k) > 32 {
			return true, nil
		}
		addr := common.BytesToAddress(tds.source.GetKey(k))
		var err error
		if err = acc.DecodeForStorage(v); err != nil {
			return false, err
		}
		root, err := tds.db.Get(dbutils.IntermediateTrieHashBucket, dbutils.GenerateStoragePrefix(common.BytesToHash(k), acc.GetIncarnation()))
		if err != nil {
			return false, err
		}
		acc.Root = common.BytesToHash(root)

		var code []byte

		if !acc.IsEmptyCodeHash() {
			if code, err = tds.db.Get(dbutils.CodeBucket, acc.CodeHash[:]); err != nil {
				return false, err
			}
		}
		account := DumpAccount{
			Balance:  acc.Balance.String(),
			Nonce:    acc.Nonce,
			Root:     common.Bytes2Hex(acc.Root[:]),
			CodeHash: common.Bytes2Hex(acc.CodeHash[:]),
			Storage:  make(map[string]string),
		}
		if emptyAddress == addr {
			// Preimage missing
			missingPreimages++
			if excludeMissingPreimages {
				return true, nil
			}
			account.SecureKey = common.CopyBytes(k)
		}
		if !excludeCode {
			account.Code = common.Bytes2Hex(code)
		}

		if acc.HasStorageSize {
			var storageSize = acc.StorageSize
			account.StorageSize = &storageSize
		}

		buf := make([]byte, binary.MaxVarintLen64)
		binary.PutUvarint(buf, acc.GetIncarnation())

		addrHash, err := common.HashData(addr[:])
		if err != nil {
			return false, err
		}

		err = tds.db.Walk(dbutils.CurrentStateBucket, dbutils.GenerateStoragePrefix(addrHash, acc.GetIncarnation()), uint(common.HashLength*8+common.IncarnationLength), func(ks, vs []byte) (bool, error) {
			key := tds.source.GetKey(ks[common.HashLength+common.IncarnationLength:]) //remove account address and version from composite key

			if !excludeStorage {
				account.Storage[common.BytesToHash(key).String()] = common.Bytes2Hex(vs)
			}

			return true, nil
		})
		if err != nil {
			return false, err
		}
		c.onAccount(addr, account)
		numberOfResults++

		return true, nil
	})
	if err != nil {
		panic(err)
	}

	return nextKey
}

// RawDump returns the entire state an a single large object
func (tds *StateDumper) RawDump(excludeCode, excludeStorage, excludeMissingPreimages bool) Dump {
	dump := &Dump{
		Accounts: make(map[common.Address]DumpAccount),
	}
	tds.dump(dump, excludeCode, excludeStorage, excludeMissingPreimages, nil, 0)
	return *dump
}

// Dump returns a JSON string representing the entire state as a single json-object
func (tds *StateDumper) Dump(excludeCode, excludeStorage, excludeMissingPreimages bool) []byte {
	dump := tds.RawDump(excludeCode, excludeStorage, excludeMissingPreimages)
	json, err := json.MarshalIndent(dump, "", "    ")
	if err != nil {
		fmt.Println("dump err", err)
	}
	return json
}

// IterativeDump dumps out accounts as json-objects, delimited by linebreaks on stdout
func (tds *StateDumper) IterativeDump(excludeCode, excludeStorage, excludeMissingPreimages bool, output *json.Encoder) {
	tds.dump(iterativeDump{output}, excludeCode, excludeStorage, excludeMissingPreimages, nil, 0)
}

// IteratorDump dumps out a batch of accounts starts with the given start key
func (tds *StateDumper) IteratorDump(excludeCode, excludeStorage, excludeMissingPreimages bool, start []byte, maxResults int) IteratorDump {
	iterator := &IteratorDump{
		Accounts: make(map[common.Address]DumpAccount),
	}
	iterator.Next = tds.dump(iterator, excludeCode, excludeStorage, excludeMissingPreimages, start, maxResults)
	return *iterator
}

func (tds *StateDumper) DefaultRawDump() Dump {
	return tds.RawDump(false, false, false)
}

// DefaultDump returns a JSON string representing the state with the default params
func (tds *StateDumper) DefaultDump() []byte {
	return tds.Dump(false, false, false)
}
