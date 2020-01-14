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
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
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

func (dbs *DbState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey, value common.Hash) bool, maxResults int) {
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		log.Error("Error on hashing", "err", err)
		return
	}

	st := llrb.New()
	var s [common.HashLength + IncarnationLength + common.HashLength]byte
	copy(s[:], addrHash[:])
	// TODO: [Issue 99] support incarnations
	binary.BigEndian.PutUint64(s[common.HashLength:], ^uint64(FirstContractIncarnation))
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

func (dbs *DbState) ForEachAccount(start []byte, cb func(address *common.Address, addrHash common.Hash), maxResults int) {
	results := 0
	err := dbs.db.WalkAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, start[:], 0, dbs.blockNr+1, func(ks, vs []byte) (bool, error) {
		if vs == nil || len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		addrHash := common.BytesToHash(ks[:common.HashLength])
		preimage, err := dbs.db.Get(dbutils.PreimagePrefix, addrHash[:])
		if err == nil {
			addr := &common.Address{}
			addr.SetBytes(preimage)
			cb(addr, addrHash)
		} else {
			cb(nil, addrHash)
		}
		results++
		return results < maxResults, nil
	})
	if err != nil {
		log.Error("ForEachAccount walk error", "err", err)
	}
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
	if debug.IsThinHistory() {
		codeHash, err := dbs.db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash, acc.Incarnation))
		if err != nil {
			acc.CodeHash = common.BytesToHash(codeHash)
		} else {
			log.Error("ReadAccountData Get code hash is incorrect")
		}
	}
	return &acc, nil
}

func (dbs *DbState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	keyHash, err := common.HashData(key[:])
	if err != nil {
		return nil, err
	}

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, keyHash)
	enc, err := dbs.db.GetAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, compositeKey, dbs.blockNr+1)
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (dbs *DbState) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return dbs.db.Get(dbutils.CodeBucket, codeHash[:])
}

func (dbs *DbState) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	code, err := dbs.ReadAccountCode(address, codeHash)
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

func (dbs *DbState) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
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

func (dbs *DbState) GetKey(shaKey []byte) []byte {
	key, _ := dbs.db.Get(dbutils.PreimagePrefix, shaKey)
	return key
}

func (dbs *DbState) dump(c collector, excludeCode, excludeStorage, excludeMissingPreimages bool) {
	emptyAddress := (common.Address{})
	missingPreimages := 0
	var acc accounts.Account
	var prefix [32]byte
	err := dbs.db.Walk(dbutils.AccountsBucket, prefix[:], 0, func(k, v []byte) (bool, error) {
		addr := common.BytesToAddress(dbs.GetKey(k))
		var err error
		if err = acc.DecodeForStorage(v); err != nil {
			return false, err
		}
		var code []byte

		if !acc.IsEmptyCodeHash() {
			if code, err = dbs.db.Get(dbutils.CodeBucket, acc.CodeHash[:]); err != nil {
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

		err = dbs.db.Walk(dbutils.StorageBucket, dbutils.GenerateStoragePrefix(addrHash, acc.GetIncarnation()), uint(common.HashLength*8+IncarnationLength), func(ks, vs []byte) (bool, error) {
			key := dbs.GetKey(ks[common.HashLength+IncarnationLength:]) //remove account address and version from composite key

			if !excludeStorage {
				account.Storage[common.BytesToHash(key).String()] = common.Bytes2Hex(vs)
			}

			return true, nil
		})
		if err != nil {
			return false, err
		}
		c.onAccount(addr, account)
		return true, nil
	})
	if err != nil {
		panic(err)
	}
}

// RawDump returns the entire state an a single large object
func (dbs *DbState) RawDump(excludeCode, excludeStorage, excludeMissingPreimages bool) Dump {
	dump := &Dump{
		Accounts: make(map[common.Address]DumpAccount),
	}
	dbs.dump(dump, excludeCode, excludeStorage, excludeMissingPreimages)
	return *dump
}

func (dbs *DbState) DefaultRawDump() Dump {
	return dbs.RawDump(false, false, false)
}

// WalkStorageRange calls the walker for each storage item whose key starts with a given prefix,
// for no more than maxItems.
// Returns whether all matching storage items were traversed (provided there was no error).
func (dbs *DbState) WalkStorageRange(addrHash common.Hash, prefix trie.Keybytes, maxItems int, walker func(common.Hash, big.Int)) (bool, error) {
	startkey := make([]byte, common.HashLength+IncarnationLength+common.HashLength)
	copy(startkey, addrHash[:])
	// TODO: [Issue 99] Support incarnations
	binary.BigEndian.PutUint64(startkey[common.HashLength:], ^uint64(1))
	copy(startkey[common.HashLength+IncarnationLength:], prefix.Data)

	fixedbits := (common.HashLength + IncarnationLength + uint(len(prefix.Data))) * 8
	if prefix.Odd {
		fixedbits -= 4
	}

	i := 0

	err := dbs.db.WalkAsOf(dbutils.StorageBucket, dbutils.StorageHistoryBucket, startkey, fixedbits, dbs.blockNr+1,
		func(key []byte, value []byte) (bool, error) {
			val := new(big.Int).SetBytes(value)

			if i < maxItems {
				walker(common.BytesToHash(key), *val)
			}
			i++
			return i <= maxItems, nil
		},
	)

	return i <= maxItems, err
}

// WalkRangeOfAccounts calls the walker for each account whose key starts with a given prefix,
// for no more than maxItems.
// Returns whether all matching accounts were traversed (provided there was no error).
func (dbs *DbState) WalkRangeOfAccounts(prefix trie.Keybytes, maxItems int, walker func(common.Hash, *accounts.Account)) (bool, error) {
	startkey := make([]byte, common.HashLength)
	copy(startkey, prefix.Data)

	fixedbits := uint(len(prefix.Data)) * 8
	if prefix.Odd {
		fixedbits -= 4
	}

	i := 0

	var acc accounts.Account
	err := dbs.db.WalkAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, startkey, fixedbits, dbs.blockNr+1,
		func(key []byte, value []byte) (bool, error) {
			if len(value) > 0 {
				if err := acc.DecodeForStorage(value); err != nil {
					return false, err
				}
				if i < maxItems {
					walker(common.BytesToHash(key), &acc)
				}
				i++
			}
			return i <= maxItems, nil
		},
	)

	return i <= maxItems, err
}
