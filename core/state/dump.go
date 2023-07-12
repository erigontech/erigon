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

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

type Dumper struct {
	blockNumber uint64
	db          kv.Tx
	hashedState bool
	historyV3   bool
}

// DumpAccount represents an account in the state.
type DumpAccount struct {
	Balance   string             `json:"balance"`
	Nonce     uint64             `json:"nonce"`
	Root      hexutility.Bytes   `json:"root"`
	CodeHash  hexutility.Bytes   `json:"codeHash"`
	Code      hexutility.Bytes   `json:"code,omitempty"`
	Storage   map[string]string  `json:"storage,omitempty"`
	Address   *libcommon.Address `json:"address,omitempty"` // Address only present in iterative (line-by-line) mode
	SecureKey *hexutility.Bytes  `json:"key,omitempty"`     // If we don't have address, we can output the key
}

// Dump represents the full dump in a collected format, as one large map.
type Dump struct {
	Root     string                            `json:"root"`
	Accounts map[libcommon.Address]DumpAccount `json:"accounts"`
}

// iterativeDump is a 'collector'-implementation which dump output line-by-line iteratively.
type iterativeDump struct {
	*json.Encoder
}

// IteratorDump is an implementation for iterating over data.
type IteratorDump struct {
	Root     string                            `json:"root"`
	Accounts map[libcommon.Address]DumpAccount `json:"accounts"`
	Next     []byte                            `json:"next,omitempty"` // nil if no more accounts
}

// DumpCollector interface which the state trie calls during iteration
type DumpCollector interface {
	// OnRoot is called with the state root
	OnRoot(libcommon.Hash)
	// OnAccount is called once for each account in the trie
	OnAccount(libcommon.Address, DumpAccount)
}

// OnRoot implements DumpCollector interface
func (d *Dump) OnRoot(root libcommon.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

// OnAccount implements DumpCollector interface
func (d *Dump) OnAccount(addr libcommon.Address, account DumpAccount) {
	d.Accounts[addr] = account
}

// OnRoot implements DumpCollector interface
func (d *IteratorDump) OnRoot(root libcommon.Hash) {
	d.Root = fmt.Sprintf("%x", root)
}

// OnAccount implements DumpCollector interface
func (d *IteratorDump) OnAccount(addr libcommon.Address, account DumpAccount) {
	d.Accounts[addr] = account
}

// OnAccount implements DumpCollector interface
func (d iterativeDump) OnAccount(addr libcommon.Address, account DumpAccount) {
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
	if addr != (libcommon.Address{}) {
		dumpAccount.Address = &addr
	}
	//nolint:errcheck
	d.Encode(dumpAccount)
}

// OnRoot implements DumpCollector interface
func (d iterativeDump) OnRoot(root libcommon.Hash) {
	//nolint:errcheck
	d.Encoder.Encode(struct {
		Root libcommon.Hash `json:"root"`
	}{root})
}

func NewDumper(db kv.Tx, blockNumber uint64, historyV3 bool) *Dumper {
	return &Dumper{
		db:          db,
		blockNumber: blockNumber,
		hashedState: false,
		historyV3:   historyV3,
	}
}

func (d *Dumper) DumpToCollector(c DumpCollector, excludeCode, excludeStorage bool, startAddress libcommon.Address, maxResults int) ([]byte, error) {
	var nextKey []byte
	var emptyCodeHash = crypto.Keccak256Hash(nil)
	var emptyHash = libcommon.Hash{}
	var accountList []*DumpAccount
	var incarnationList []uint64
	var addrList []libcommon.Address

	c.OnRoot(emptyHash) // We do not calculate the root

	var acc accounts.Account
	numberOfResults := 0

	var txNum, txNumForStorage uint64
	if d.historyV3 {
		ttx := d.db.(kv.TemporalTx)
		var err error
		// Why only account does +1?
		txNum, err = rawdbv3.TxNums.Min(ttx, d.blockNumber+1)
		if err != nil {
			return nil, err
		}
		txNumForStorage, err = rawdbv3.TxNums.Min(ttx, d.blockNumber)
		if err != nil {
			return nil, err
		}

		it, err := ttx.DomainRange(kv.AccountsDomain, startAddress[:], nil, txNum, order.Asc, maxResults+1)
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return nil, err
			}
			if maxResults > 0 && numberOfResults >= maxResults {
				if nextKey == nil {
					nextKey = make([]byte, len(k))
				}
				copy(nextKey, k)
				break
			}
			if len(v) == 0 {
				continue
			}

			if e := acc.DecodeForStorage(v); e != nil {
				return nil, fmt.Errorf("decoding %x for %x: %w", v, k, e)
			}
			account := DumpAccount{
				Balance:  acc.Balance.ToBig().String(),
				Nonce:    acc.Nonce,
				Root:     hexutility.Bytes(emptyHash[:]), // We cannot provide historical storage hash
				CodeHash: hexutility.Bytes(emptyCodeHash[:]),
				Storage:  make(map[string]string),
			}
			accountList = append(accountList, &account)
			addrList = append(addrList, libcommon.BytesToAddress(k))
			incarnationList = append(incarnationList, acc.Incarnation)

			numberOfResults++
		}
	} else {
		if err := WalkAsOfAccounts(d.db, startAddress, d.blockNumber+1, func(k, v []byte) (bool, error) {
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
			if e := acc.DecodeForStorage(v); e != nil {
				return false, fmt.Errorf("decoding %x for %x: %w", v, k, e)
			}
			account := DumpAccount{
				Balance:  acc.Balance.ToBig().String(),
				Nonce:    acc.Nonce,
				Root:     hexutility.Bytes(emptyHash[:]), // We cannot provide historical storage hash
				CodeHash: hexutility.Bytes(emptyCodeHash[:]),
				Storage:  make(map[string]string),
			}
			accountList = append(accountList, &account)
			addrList = append(addrList, libcommon.BytesToAddress(k))
			incarnationList = append(incarnationList, acc.Incarnation)

			numberOfResults++
			return true, nil
		}); err != nil {
			return nil, err
		}
	}

	for i, addr := range addrList {
		account := accountList[i]
		incarnation := incarnationList[i]
		storagePrefix := dbutils.PlainGenerateStoragePrefix(addr[:], incarnation)
		if incarnation > 0 {
			codeHash, err := d.db.GetOne(kv.PlainContractCode, storagePrefix)
			if err != nil {
				return nil, fmt.Errorf("getting code hash for %x: %w", addr, err)
			}
			if codeHash != nil {
				account.CodeHash = codeHash
			} else {
				account.CodeHash = emptyCodeHash[:]
			}

			if !excludeCode && codeHash != nil && !bytes.Equal(codeHash, emptyCodeHash[:]) {
				var code []byte
				if code, err = d.db.GetOne(kv.Code, codeHash); err != nil {
					return nil, err
				}
				account.Code = code
			}
		}

		if !excludeStorage {
			t := trie.New(libcommon.Hash{})
			if d.historyV3 {
				r, err := d.db.(kv.TemporalTx).DomainRange(kv.StorageDomain, addr[:], nil, txNumForStorage, order.Asc, kv.Unlim)
				if err != nil {
					return nil, fmt.Errorf("walking over storage for %x: %w", addr, err)
				}
				for r.HasNext() {
					k, vs, err := r.Next()
					if err != nil {
						return nil, fmt.Errorf("walking over storage for %x: %w", addr, err)
					}
					if len(vs) == 0 {
						continue // Skip deleted entries
					}
					loc := k[20:]
					account.Storage[libcommon.BytesToHash(loc).String()] = common.Bytes2Hex(vs)
					h, _ := common.HashData(loc)
					t.Update(h.Bytes(), libcommon.Copy(vs))
				}
			} else {
				if err := WalkAsOfStorage(d.db,
					addr,
					incarnation,
					libcommon.Hash{}, /* startLocation */
					d.blockNumber,
					func(_, loc, vs []byte) (bool, error) {
						account.Storage[libcommon.BytesToHash(loc).String()] = common.Bytes2Hex(vs)
						h, _ := common.HashData(loc)
						t.Update(h.Bytes(), libcommon.Copy(vs))
						return true, nil
					}); err != nil {
					return nil, fmt.Errorf("walking over storage for %x: %w", addr, err)
				}
			}

			account.Root = t.Hash().Bytes()
		}
		c.OnAccount(addr, *account)
	}

	return nextKey, nil
}

// RawDump returns the entire state an a single large object
func (d *Dumper) RawDump(excludeCode, excludeStorage bool) Dump {
	dump := &Dump{
		Accounts: make(map[libcommon.Address]DumpAccount),
	}
	//nolint:errcheck
	d.DumpToCollector(dump, excludeCode, excludeStorage, libcommon.Address{}, 0)
	return *dump
}

// Dump returns a JSON string representing the entire state as a single json-object
func (d *Dumper) Dump(excludeCode, excludeStorage bool) []byte {
	dump := d.RawDump(excludeCode, excludeStorage)
	json, err := json.MarshalIndent(dump, "", "    ")
	if err != nil {
		fmt.Println("dump err", err)
	}
	return json
}

// IterativeDump dumps out accounts as json-objects, delimited by linebreaks on stdout
func (d *Dumper) IterativeDump(excludeCode, excludeStorage bool, output *json.Encoder) {
	//nolint:errcheck
	d.DumpToCollector(iterativeDump{output}, excludeCode, excludeStorage, libcommon.Address{}, 0)
}

// IteratorDump dumps out a batch of accounts starts with the given start key
func (d *Dumper) IteratorDump(excludeCode, excludeStorage bool, start libcommon.Address, maxResults int) (IteratorDump, error) {
	iterator := &IteratorDump{
		Accounts: make(map[libcommon.Address]DumpAccount),
	}
	var err error
	iterator.Next, err = d.DumpToCollector(iterator, excludeCode, excludeStorage, start, maxResults)
	return *iterator, err
}

func (d *Dumper) DefaultRawDump() Dump {
	return d.RawDump(false, false)
}

// DefaultDump returns a JSON string representing the state with the default params
func (d *Dumper) DefaultDump() []byte {
	return d.Dump(false, false)
}
