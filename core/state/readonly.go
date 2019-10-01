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

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/petar/GoLLRB/llrb"
)

type storageItem struct {
	key, seckey, value common.Hash
}

func (a *storageItem) Less(b llrb.Item) bool {
	bi := b.(*storageItem)
	return bytes.Compare(a.seckey[:], bi.seckey[:]) < 0
}

// Implements StateReader by wrapping database only, without trie
type DbState struct {
	db      ethdb.Getter
	blockNr uint64
	storage map[common.Address]*llrb.LLRB
}

func NewDbState(db ethdb.Getter, blockNr uint64) *DbState {
	return &DbState{
		db:      db,
		blockNr: blockNr,
		storage: make(map[common.Address]*llrb.LLRB),
	}
}

func (dbs *DbState) SetBlockNr(blockNr uint64) {
	dbs.blockNr = blockNr
}

// TODO: support incarnations
func (dbs *DbState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey, value common.Hash) bool, maxResults int) {
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		log.Error("Error on hashing", "err", err)
		return
	}

	st := llrb.New()
	var s [common.HashLength + IncarnationLength + common.HashLength]byte
	copy(s[:], addrHash[:])
	binary.BigEndian.PutUint64(s[common.HashLength:], ^uint64(0))
	copy(s[common.HashLength+IncarnationLength:], start)
	var lastSecKey common.Hash
	overrideCounter := 0
	emptyHash := common.Hash{}
	min := &storageItem{seckey: common.BytesToHash(start)}
	if t, ok := dbs.storage[addr]; ok {
		t.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
			item := i.(*storageItem)
			st.ReplaceOrInsert(item)
			if item.value != emptyHash {
				copy(lastSecKey[:], item.seckey[:])
				// Only count non-zero items
				overrideCounter++
			}
			return overrideCounter < maxResults
		})
	}
	numDeletes := st.Len() - overrideCounter
	err = dbs.db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, s[:], 0, dbs.blockNr+1, func(ks, vs []byte) (bool, error) {
		if !bytes.HasPrefix(ks, addrHash[:]) {
			return false, nil
		}
		if vs == nil || len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		seckey := ks[common.HashLength+IncarnationLength:]
		//fmt.Printf("seckey: %x\n", seckey)
		si := storageItem{}
		copy(si.seckey[:], seckey)
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.InsertNoReplace(&si)
		if bytes.Compare(seckey[:], lastSecKey[:]) > 0 {
			// Beyond overrides
			return st.Len() < maxResults+numDeletes, nil
		}
		return st.Len() < maxResults+overrideCounter+numDeletes, nil
	})
	if err != nil {
		log.Error("ForEachStorage walk error", "err", err)
	}
	results := 0
	st.AscendGreaterOrEqual(min, func(i llrb.Item) bool {
		item := i.(*storageItem)
		if item.value != emptyHash {
			// Skip if value == 0
			if item.key == emptyHash {
				key, err := dbs.db.Get(dbutils.PreimagePrefix, item.seckey[:])
				if err == nil {
					copy(item.key[:], key)
				} else {
					log.Error("Error getting preimage", "err", err)
				}
			}
			cb(item.key, item.seckey, item.value)
			results++
		}
		return results < maxResults
	})
}

func (dbs *DbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	enc, err := dbs.db.GetAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash[:], dbs.blockNr+1)
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (dbs *DbState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	keyHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	enc, err := dbs.db.GetAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, dbutils.GenerateCompositeStorageKey(addrHash, incarnation, keyHash), dbs.blockNr+1)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (dbs *DbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return dbs.db.Get(dbutils.CodeBucket, codeHash[:])
}

func (dbs *DbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := dbs.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (dbs *DbState) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	return nil
}

func (dbs *DbState) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	return nil
}

func (dbs *DbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (dbs *DbState) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
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

func (dbs *DbState) CreateContract(address common.Address) error {
	delete(dbs.storage, address)
	return nil
}
