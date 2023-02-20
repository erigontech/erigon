// Copyright 2019 The go-ethereum Authors
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
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/google/btree"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state/historyv2read"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type storageItem struct {
	key, seckey libcommon.Hash
	value       uint256.Int
}

func (a *storageItem) Less(b btree.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.key[:], bi.key[:]) < 0
}

// State at the beginning of blockNr
type PlainState struct {
	accHistoryC, storageHistoryC kv.Cursor
	accChangesC, storageChangesC kv.CursorDupSort
	tx                           kv.Tx
	blockNr                      uint64
	storage                      map[libcommon.Address]*btree.BTree
	trace                        bool
	systemContractLookup         map[libcommon.Address][]libcommon.CodeRecord
}

func NewPlainState(tx kv.Tx, blockNr uint64, systemContractLookup map[libcommon.Address][]libcommon.CodeRecord) *PlainState {
	histV3, _ := kvcfg.HistoryV3.Enabled(tx)
	if histV3 {
		panic("Please use HistoryStateReaderV3 with HistoryV3")
	}
	ps := &PlainState{
		tx:                   tx,
		blockNr:              blockNr,
		storage:              make(map[libcommon.Address]*btree.BTree),
		systemContractLookup: systemContractLookup,
	}

	c1, _ := tx.Cursor(kv.AccountsHistory)
	c2, _ := tx.Cursor(kv.StorageHistory)
	c3, _ := tx.CursorDupSort(kv.AccountChangeSet)
	c4, _ := tx.CursorDupSort(kv.StorageChangeSet)

	ps.accHistoryC = c1
	ps.storageHistoryC = c2
	ps.accChangesC = c3
	ps.storageChangesC = c4
	return ps
}

func (s *PlainState) SetTrace(trace bool) {
	s.trace = trace
}

func (s *PlainState) SetBlockNr(blockNr uint64) {
	s.blockNr = blockNr
}

func (s *PlainState) GetBlockNr() uint64 {
	return s.blockNr
}

func (s *PlainState) ForEachStorage(addr libcommon.Address, startLocation libcommon.Hash, cb func(key, seckey libcommon.Hash, value uint256.Int) bool, maxResults int) error {
	st := btree.New(16)
	var k [length.Addr + length.Incarnation + length.Hash]byte
	copy(k[:], addr[:])
	accData, err := historyv2read.GetAsOf(s.tx, s.accHistoryC, s.accChangesC, false /* storage */, addr[:], s.blockNr)
	if err != nil {
		return err
	}

	var acc accounts.Account
	if err := acc.DecodeForStorage(accData); err != nil {
		log.Error("Error decoding account", "err", err)
		return err
	}
	binary.BigEndian.PutUint64(k[length.Addr:], acc.Incarnation)
	copy(k[length.Addr+length.Incarnation:], startLocation[:])
	var lastKey libcommon.Hash
	overrideCounter := 0
	min := &storageItem{key: startLocation}
	if t, ok := s.storage[addr]; ok {
		t.AscendGreaterOrEqual(min, func(i btree.Item) bool {
			item := i.(*storageItem)
			st.ReplaceOrInsert(item)
			if !item.value.IsZero() {
				copy(lastKey[:], item.key[:])
				// Only count non-zero items
				overrideCounter++
			}
			return overrideCounter < maxResults
		})
	}
	numDeletes := st.Len() - overrideCounter
	if err := WalkAsOfStorage(s.tx, addr, acc.Incarnation, startLocation, s.blockNr, func(kAddr, kLoc, vs []byte) (bool, error) {
		if !bytes.Equal(kAddr, addr[:]) {
			return false, nil
		}
		if len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		keyHash, err1 := common.HashData(kLoc)
		if err1 != nil {
			return false, err1
		}
		//fmt.Printf("seckey: %x\n", seckey)
		si := storageItem{}
		copy(si.key[:], kLoc)
		copy(si.seckey[:], keyHash[:])
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.ReplaceOrInsert(&si)
		if bytes.Compare(kLoc, lastKey[:]) > 0 {
			// Beyond overrides
			return st.Len() < maxResults+numDeletes, nil
		}
		return st.Len() < maxResults+overrideCounter+numDeletes, nil
	}); err != nil {
		log.Error("ForEachStorage walk error", "err", err)
		return err
	}
	results := 0
	var innerErr error
	st.AscendGreaterOrEqual(min, func(i btree.Item) bool {
		item := i.(*storageItem)
		if !item.value.IsZero() {
			// Skip if value == 0
			cb(item.key, item.seckey, item.value)
			results++
		}
		return results < maxResults
	})
	return innerErr
}

func (s *PlainState) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, err := historyv2read.GetAsOf(s.tx, s.accHistoryC, s.accChangesC, false /* storage */, address[:], s.blockNr)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		if s.trace {
			fmt.Printf("ReadAccountData [%x] => []\n", address)
		}
		return nil, nil
	}
	var a accounts.Account
	if err = a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	//restore codehash
	if records, ok := s.systemContractLookup[address]; ok {
		p := sort.Search(len(records), func(i int) bool {
			return records[i].BlockNumber > s.blockNr
		})
		a.CodeHash = records[p-1].CodeHash
	} else if a.Incarnation > 0 && a.IsEmptyCodeHash() {
		if codeHash, err1 := s.tx.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], a.Incarnation)); err1 == nil {
			if len(codeHash) > 0 {
				a.CodeHash = libcommon.BytesToHash(codeHash)
			}
		} else {
			return nil, err1
		}
	}
	if s.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func (s *PlainState) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := historyv2read.GetAsOf(s.tx, s.storageHistoryC, s.storageChangesC, true /* storage */, compositeKey, s.blockNr)
	if err != nil {
		return nil, err
	}
	if s.trace {
		fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, *key, enc)
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (s *PlainState) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	code, err := s.tx.GetOne(kv.Code, codeHash[:])
	if s.trace {
		fmt.Printf("ReadAccountCode [%x %x] => [%x]\n", address, codeHash, code)
	}
	if err != nil {
		return nil, err
	}
	if len(code) == 0 {
		return nil, nil
	}
	return code, nil
}

func (s *PlainState) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	code, err := s.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (s *PlainState) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	enc, err := historyv2read.GetAsOf(s.tx, s.accHistoryC, s.accChangesC, false /* storage */, address[:], s.blockNr+1)
	if err != nil {
		return 0, err
	}
	if len(enc) == 0 {
		if s.trace {
			fmt.Printf("ReadAccountIncarnation [%x] => [%d]\n", address, 0)
		}
		return 0, nil
	}
	var acc accounts.Account
	if err = acc.DecodeForStorage(enc); err != nil {
		return 0, err
	}
	if acc.Incarnation == 0 {
		if s.trace {
			fmt.Printf("ReadAccountIncarnation [%x] => [%d]\n", address, 0)
		}
		return 0, nil
	}
	if s.trace {
		fmt.Printf("ReadAccountIncarnation [%x] => [%d]\n", address, acc.Incarnation-1)
	}
	return acc.Incarnation - 1, nil
}

func (s *PlainState) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	return nil
}

func (s *PlainState) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	return nil
}

func (s *PlainState) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	return nil
}

func (s *PlainState) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	t, ok := s.storage[address]
	if !ok {
		t = btree.New(16)
		s.storage[address] = t
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	_, err := h.Sha.Write(key[:])
	if err != nil {
		return err
	}
	i := &storageItem{key: *key, value: *value}
	_, err = h.Sha.Read(i.seckey[:])
	if err != nil {
		return err
	}

	t.ReplaceOrInsert(i)
	return nil
}

func (s *PlainState) CreateContract(address libcommon.Address) error {
	delete(s.storage, address)
	return nil
}
