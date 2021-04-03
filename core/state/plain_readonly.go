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
	"context"
	"encoding/binary"
	"errors"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/petar/GoLLRB/llrb"
)

type storageItem struct {
	key, seckey common.Hash
	value       uint256.Int
}

func (a *storageItem) Less(b llrb.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.key[:], bi.key[:]) < 0
}

// Implements StateReader by wrapping database only, without trie
type PlainDBState struct {
	db      ethdb.GetterBeginner
	blockNr uint64
	storage map[common.Address]*llrb.LLRB
}

func NewPlainDBState(db ethdb.GetterBeginner, blockNr uint64) *PlainDBState {
	return &PlainDBState{
		db:      db,
		blockNr: blockNr,
		storage: make(map[common.Address]*llrb.LLRB),
	}
}

func (dbs *PlainDBState) SetBlockNr(blockNr uint64) {
	dbs.blockNr = blockNr
}

func (dbs *PlainDBState) GetBlockNr() uint64 {
	return dbs.blockNr
}

func (dbs *PlainDBState) ForEachStorage(addr common.Address, startLocation common.Hash, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error {
	var tx ethdb.Tx
	if hasTx, ok := dbs.db.(ethdb.HasTx); ok {
		tx = hasTx.Tx()
	} else {
		dbtx, err := dbs.db.BeginGetter(context.Background())
		if err != nil {
			return err
		}
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	}
	st := llrb.New()
	var s [common.AddressLength + common.IncarnationLength + common.HashLength]byte
	copy(s[:], addr[:])
	accData, _ := GetAsOf(tx, false /* storage */, addr[:], dbs.blockNr+1)
	var acc accounts.Account
	if err := acc.DecodeForStorage(accData); err != nil {
		log.Error("Error decoding account", "error", err)
		return err
	}
	binary.BigEndian.PutUint64(s[common.AddressLength:], acc.Incarnation)
	copy(s[common.AddressLength+common.IncarnationLength:], startLocation[:])
	var lastKey common.Hash
	overrideCounter := 0
	min := &storageItem{key: startLocation}
	if t, ok := dbs.storage[addr]; ok {
		t.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
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
	if err := WalkAsOfStorage(tx, addr, acc.Incarnation, startLocation, dbs.blockNr+1, func(kAddr, kLoc, vs []byte) (bool, error) {
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
		st.InsertNoReplace(&si)
		if bytes.Compare(kLoc[:], lastKey[:]) > 0 {
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
	st.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
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

func (dbs *PlainDBState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var tx ethdb.Tx
	if hasTx, ok := dbs.db.(ethdb.HasTx); ok {
		tx = hasTx.Tx()
	} else {
		dbtx, err := dbs.db.BeginGetter(context.Background())
		if err != nil {
			return nil, err
		}
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	}
	enc, err := GetAsOf(tx, false /* storage */, address[:], dbs.blockNr+1)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	//restore codehash
	if a.Incarnation > 0 && a.IsEmptyCodeHash() {
		if codeHash, err1 := tx.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], a.Incarnation)); err1 == nil {
			if len(codeHash) > 0 {
				a.CodeHash = common.BytesToHash(codeHash)
			}
		} else {
			return nil, err1
		}
	}
	return &a, nil
}

func (dbs *PlainDBState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	var tx ethdb.Tx
	if hasTx, ok := dbs.db.(ethdb.HasTx); ok {
		tx = hasTx.Tx()
	} else {
		dbtx, err := dbs.db.BeginGetter(context.Background())
		if err != nil {
			return nil, err
		}
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())
	enc, err := GetAsOf(tx, true /* storage */, compositeKey, dbs.blockNr+1)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (dbs *PlainDBState) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	var tx ethdb.Tx
	if hasTx, ok := dbs.db.(ethdb.HasTx); ok {
		tx = hasTx.Tx()
	} else {
		dbtx, err := dbs.db.BeginGetter(context.Background())
		if err != nil {
			return nil, err
		}
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	}
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	code, err := tx.GetOne(dbutils.CodeBucket, codeHash[:])
	if len(code) == 0 {
		return nil, nil
	}
	return code, err
}

func (dbs *PlainDBState) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	code, err := dbs.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (dbs *PlainDBState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	var tx ethdb.Tx
	if hasTx, ok := dbs.db.(ethdb.HasTx); ok {
		tx = hasTx.Tx()
	} else {
		dbtx, err := dbs.db.BeginGetter(context.Background())
		if err != nil {
			return 0, err
		}
		defer dbtx.Rollback()
		tx = dbtx.(ethdb.HasTx).Tx()
	}
	enc, err := GetAsOf(tx, false /* storage */, address[:], dbs.blockNr+2)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, err
	}
	if len(enc) == 0 {
		return 0, nil
	}
	var acc accounts.Account
	if err = acc.DecodeForStorage(enc); err != nil {
		return 0, err
	}
	if acc.Incarnation == 0 {
		return 0, nil
	}
	return acc.Incarnation - 1, nil
}

func (dbs *PlainDBState) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	return nil
}

func (dbs *PlainDBState) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	return nil
}

func (dbs *PlainDBState) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (dbs *PlainDBState) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	t, ok := dbs.storage[address]
	if !ok {
		t = llrb.New()
		dbs.storage[address] = t
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	h.Sha.Reset()
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

func (dbs *PlainDBState) CreateContract(address common.Address) error {
	delete(dbs.storage, address)
	return nil
}
