// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package state

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type Dumper struct {
	blockNumber  uint64
	tx           kv.TemporalTx
	hashedState  bool
	txNumsReader rawdbv3.TxNumsReader
}

// DumpAccount represents tan account in the state.
type DumpAccount struct {
	Balance   string            `json:"balance"`
	Nonce     uint64            `json:"nonce"`
	Root      hexutil.Bytes     `json:"root"`
	CodeHash  hexutil.Bytes     `json:"codeHash"`
	Code      hexutil.Bytes     `json:"code,omitempty"`
	Storage   map[string]string `json:"storage,omitempty"`
	Address   *common.Address   `json:"address,omitempty"` // Address only present in iterative (line-by-line) mode
	SecureKey *hexutil.Bytes    `json:"key,omitempty"`     // If we don't have address, we can output the key
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

func NewDumper(db kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, blockNumber uint64) *Dumper {
	return &Dumper{
		tx:           db,
		blockNumber:  blockNumber,
		hashedState:  false,
		txNumsReader: txNumsReader,
	}
}

var ErrTooManyIterations = errors.New("[rpc] dumper: too many iterations protection triggered")

func (d *Dumper) DumpToCollector(c DumpCollector, excludeCode, excludeStorage bool, startAddress common.Address, maxResults int) ([]byte, error) {
	var emptyHash = common.Hash{}
	var accountList []*DumpAccount
	var addrList []common.Address
	var acc accounts.Account
	var numberOfResults int
	if maxResults == 0 {
		maxResults = kv.Unlim
	}

	c.OnRoot(emptyHash) // We do not calculate the root

	ttx := d.tx
	txNum, err := d.txNumsReader.Min(ttx, d.blockNumber+1)
	if err != nil {
		return nil, err
	}
	txNumForStorage, err := d.txNumsReader.Min(ttx, d.blockNumber+1)
	if err != nil {
		return nil, err
	}

	var nextKey []byte
	it, err := ttx.RangeAsOf(kv.AccountsDomain, startAddress[:], nil, txNum, order.Asc, kv.Unlim) //unlim because need skip empty vals
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			continue
		}
		if maxResults > 0 && numberOfResults >= maxResults {
			nextKey = append(nextKey[:0], k...)
			break
		}

		if e := accounts.DeserialiseV3(&acc, v); e != nil {
			return nil, fmt.Errorf("decoding %x for %x: %w", v, k, e)
		}
		account := DumpAccount{
			Balance:  acc.Balance.ToBig().String(),
			Nonce:    acc.Nonce,
			Root:     hexutil.Bytes(emptyHash[:]), // We cannot provide historical storage hash
			CodeHash: hexutil.Bytes(empty.CodeHash[:]),
			Storage:  make(map[string]string),
		}
		if acc.CodeHash != empty.CodeHash {
			account.CodeHash = hexutil.Bytes(acc.CodeHash.Bytes())

			if !excludeCode {
				r, _, err := ttx.GetLatest(kv.CodeDomain, k)
				if err != nil {
					return nil, err
				}
				if r != nil {
					account.Code = r
				}
			}
		}
		accountList = append(accountList, &account)
		addrList = append(addrList, common.BytesToAddress(k))

		numberOfResults++
	}
	it.Close()

	for i, addr := range addrList {
		account := accountList[i]
		if !excludeStorage {
			t := trie.New(common.Hash{})
			nextAcc, _ := kv.NextSubtree(addr[:])
			r, err := ttx.RangeAsOf(kv.StorageDomain, addr[:], nextAcc, txNumForStorage, order.Asc, kv.Unlim) //unlim because need skip empty vals
			if err != nil {
				return nil, fmt.Errorf("walking over storage for %x: %w", addr, err)
			}
			defer r.Close()
			for r.HasNext() {
				k, vs, err := r.Next()
				if err != nil {
					return nil, fmt.Errorf("walking over storage for %x: %w", addr, err)
				}
				if len(vs) == 0 {
					continue // Skip deleted entries
				}
				loc := k[20:]
				account.Storage[common.BytesToHash(loc).String()] = common.Bytes2Hex(vs)
				h, _ := common.HashData(loc)
				t.Update(h.Bytes(), common.Copy(vs))
			}
			r.Close()

			account.Root = t.Hash().Bytes()
		}
		c.OnAccount(addr, *account)
	}

	return nextKey, nil
}

// RawDump returns the entire state an a single large object
func (d *Dumper) RawDump(excludeCode, excludeStorage bool) Dump {
	dump := &Dump{
		Accounts: make(map[common.Address]DumpAccount),
	}
	//nolint:errcheck
	d.DumpToCollector(dump, excludeCode, excludeStorage, common.Address{}, 0)
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
	d.DumpToCollector(iterativeDump{output}, excludeCode, excludeStorage, common.Address{}, 0)
}

// IteratorDump dumps out a batch of accounts starts with the given start key
func (d *Dumper) IteratorDump(excludeCode, excludeStorage bool, start common.Address, maxResults int) (IteratorDump, error) {
	iterator := &IteratorDump{
		Accounts: make(map[common.Address]DumpAccount),
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
