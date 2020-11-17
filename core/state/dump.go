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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

type trieHasher interface {
	GetTrieHash() common.Hash
}

type Dumper struct {
	blockNumber uint64
	db          ethdb.Tx
	hashedState bool
}

// DumpAccount represents an account in the state.
type DumpAccount struct {
	Balance   string            `json:"balance"`
	Nonce     uint64            `json:"nonce"`
	Root      string            `json:"root"`
	CodeHash  string            `json:"codeHash"`
	Code      string            `json:"code,omitempty"`
	Storage   map[string]string `json:"storage,omitempty"`
	Address   *common.Address   `json:"address,omitempty"` // Address only present in iterative (line-by-line) mode
	SecureKey hexutil.Bytes     `json:"key,omitempty"`     // If we don't have address, we can output the key
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

// DumpCollector interface which the state trie calls during iteration
type DumpCollector interface {
	// OnRoot is called with the state root
	OnRoot(common.Hash)
	// OnAccount is called once for each account in the trie
	OnAccount(common.Address, DumpAccount)
}

// OnRoot implements DumpCollector interface
func (d *Dump) OnRoot(root common.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

// OnAccount implements DumpCollector interface
func (d *Dump) OnAccount(addr common.Address, account DumpAccount) {
	d.Accounts[addr] = account
}

// OnRoot implements DumpCollector interface
func (d *IteratorDump) OnRoot(root common.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

// OnAccount implements DumpCollector interface
func (d *IteratorDump) OnAccount(addr common.Address, account DumpAccount) {
	d.Accounts[addr] = account
}

// OnAccount implements DumpCollector interface
func (d iterativeDump) OnAccount(addr common.Address, account DumpAccount) {
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

// OnRoot implements DumpCollector interface
func (d iterativeDump) OnRoot(root common.Hash) {
	//nolint:errcheck
	d.Encoder.Encode(struct {
		Root common.Hash `json:"root"`
	}{root})
}

func NewDumper(db ethdb.Tx, blockNumber uint64) *Dumper {
	return &Dumper{
		db:          db,
		blockNumber: blockNumber,
		hashedState: false,
	}
}

func (d *Dumper) DumpToCollector(c DumpCollector, excludeCode, excludeStorage, _ bool, startAddress common.Address, maxResults int) (nextKey []byte, err error) {
	var emptyCodeHash = crypto.Keccak256Hash(nil)
	var emptyHash = common.Hash{}
	var accountList []*DumpAccount
	var incarnationList []uint64
	var addrList []common.Address

	c.OnRoot(emptyHash) // We do not calculate the root

	var acc accounts.Account
	numberOfResults := 0

	if err = WalkAsOfAccounts(d.db, startAddress, d.blockNumber+1, func(k, v []byte) (bool, error) {
		if maxResults > 0 && numberOfResults >= maxResults {
			if nextKey == nil {
				nextKey = make([]byte, len(k))
			}
			copy(nextKey, k)
			return false, nil
		}

		if len(k) > 32 {
			return true, nil
		}
		//fmt.Printf("Got account %x\n", k)
		var err error
		if err = acc.DecodeForStorage(v); err != nil {
			return false, fmt.Errorf("decoding %x for %x: %v", v, k, err)
		}
		account := DumpAccount{
			Balance:  acc.Balance.ToBig().String(),
			Nonce:    acc.Nonce,
			Root:     strings.TrimPrefix(common.Bytes2Hex(emptyHash[:]), "0x"), // We cannot provide historical storage hash
			CodeHash: strings.TrimPrefix(common.Bytes2Hex(emptyCodeHash[:]), "0x"),
			Storage:  make(map[string]string),
		}
		accountList = append(accountList, &account)
		addrList = append(addrList, common.BytesToAddress(k))
		incarnationList = append(incarnationList, acc.Incarnation)

		numberOfResults++
		return true, nil
	}); err != nil {
		return nil, err
	}

	for i, addr := range addrList {
		account := accountList[i]
		incarnation := incarnationList[i]
		storagePrefix := dbutils.PlainGenerateStoragePrefix(addr[:], incarnation)
		if incarnation > 0 {
			var codeHash []byte
			codeHash, err = ethdb.Get(d.db, dbutils.PlainContractCodeBucket, storagePrefix)
			if err != nil && err != ethdb.ErrKeyNotFound {
				return nil, fmt.Errorf("getting code hash for %x: %v", addr, err)
			}
			if codeHash != nil {
				account.CodeHash = common.Bytes2Hex(codeHash)
			} else {
				account.CodeHash = emptyCodeHash.String()
			}

			if !excludeCode && codeHash != nil && !bytes.Equal(codeHash, emptyCodeHash[:]) {
				var code []byte
				if code, err = ethdb.Get(d.db, dbutils.CodeBucket, codeHash); err != nil {
					return nil, err
				}
				account.Code = common.Bytes2Hex(code)
			}
		}

		if !excludeStorage {
			t := trie.New(common.Hash{})
			err = WalkAsOfStorage(d.db,
				addr,
				incarnation,
				common.Hash{}, /* startLocation */
				d.blockNumber,
				func(_, loc, vs []byte) (bool, error) {
					account.Storage[common.BytesToHash(loc).String()] = common.Bytes2Hex(vs)
					h, _ := common.HashData(loc)
					t.Update(h.Bytes(), common.CopyBytes(vs))
					return true, nil
				})
			if err != nil {
				return nil, fmt.Errorf("walking over storage for %x: %v", addr, err)
			}
			account.Root = t.Hash().String()
		}
		c.OnAccount(addr, *account)
	}

	return nextKey, nil
}

// RawDump returns the entire state an a single large object
func (d *Dumper) RawDump(excludeCode, excludeStorage, excludeMissingPreimages bool) Dump {
	dump := &Dump{
		Accounts: make(map[common.Address]DumpAccount),
	}
	//nolint:errcheck
	d.DumpToCollector(dump, excludeCode, excludeStorage, excludeMissingPreimages, common.Address{}, 0)
	return *dump
}

// Dump returns a JSON string representing the entire state as a single json-object
func (d *Dumper) Dump(excludeCode, excludeStorage, excludeMissingPreimages bool) []byte {
	dump := d.RawDump(excludeCode, excludeStorage, excludeMissingPreimages)
	json, err := json.MarshalIndent(dump, "", "    ")
	if err != nil {
		fmt.Println("dump err", err)
	}
	return json
}

// IterativeDump dumps out accounts as json-objects, delimited by linebreaks on stdout
func (d *Dumper) IterativeDump(excludeCode, excludeStorage, excludeMissingPreimages bool, output *json.Encoder) {
	//nolint:errcheck
	d.DumpToCollector(iterativeDump{output}, excludeCode, excludeStorage, excludeMissingPreimages, common.Address{}, 0)
}

// IteratorDump dumps out a batch of accounts starts with the given start key
func (d *Dumper) IteratorDump(excludeCode, excludeStorage, excludeMissingPreimages bool, start common.Address, maxResults int) (IteratorDump, error) {
	iterator := &IteratorDump{
		Accounts: make(map[common.Address]DumpAccount),
	}
	var err error
	iterator.Next, err = d.DumpToCollector(iterator, excludeCode, excludeStorage, excludeMissingPreimages, start, maxResults)
	return *iterator, err
}

func (d *Dumper) DefaultRawDump() Dump {
	return d.RawDump(false, false, false)
}

// DefaultDump returns a JSON string representing the state with the default params
func (d *Dumper) DefaultDump() []byte {
	return d.Dump(false, false, false)
}
