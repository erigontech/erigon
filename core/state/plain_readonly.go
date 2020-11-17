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
	"math/big"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
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
	tx            ethdb.Tx
	blockNr       uint64
	storage       map[common.Address]*llrb.LLRB
	accountCache  *fastcache.Cache
	storageCache  *fastcache.Cache
	codeCache     *fastcache.Cache
	codeSizeCache *fastcache.Cache
}

func NewPlainDBState(tx ethdb.Tx, blockNr uint64) *PlainDBState {
	return &PlainDBState{
		tx:      tx,
		blockNr: blockNr,
		storage: make(map[common.Address]*llrb.LLRB),
	}
}

func (dbs *PlainDBState) SetAccountCache(accountCache *fastcache.Cache) {
	dbs.accountCache = accountCache
}

func (dbs *PlainDBState) SetStorageCache(storageCache *fastcache.Cache) {
	dbs.storageCache = storageCache
}

func (dbs *PlainDBState) SetCodeCache(codeCache *fastcache.Cache) {
	dbs.codeCache = codeCache
}

func (dbs *PlainDBState) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	dbs.codeSizeCache = codeSizeCache
}

func (dbs *PlainDBState) SetBlockNr(blockNr uint64) {
	dbs.blockNr = blockNr
}

func (dbs *PlainDBState) GetBlockNr() uint64 {
	return dbs.blockNr
}

func (dbs *PlainDBState) ForEachStorage(addr common.Address, start []byte, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error {
	st := llrb.New()
	var s [common.AddressLength + common.IncarnationLength + common.HashLength]byte
	copy(s[:], addr[:])
	accData, _ := GetAsOf(dbs.tx, false /* storage */, addr[:], dbs.blockNr+1)
	var acc accounts.Account
	if err := acc.DecodeForStorage(accData); err != nil {
		log.Error("Error decoding account", "error", err)
		return err
	}
	binary.BigEndian.PutUint64(s[common.AddressLength:], acc.Incarnation)
	copy(s[common.AddressLength+common.IncarnationLength:], start)
	var lastKey common.Hash
	overrideCounter := 0
	min := &storageItem{key: common.BytesToHash(start)}
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
	if err := WalkAsOf(dbs.tx, true /* storage */, s[:], 8*(common.AddressLength+common.IncarnationLength), dbs.blockNr+1, func(ks, vs []byte) (bool, error) {
		if !bytes.HasPrefix(ks, addr[:]) {
			return false, nil
		}
		if len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		if len(ks) < common.AddressLength+common.IncarnationLength+common.HashLength {
			// Skip non storage items
			return true, nil
		}
		key := ks[common.AddressLength+common.IncarnationLength:]
		keyHash, err1 := common.HashData(key)
		if err1 != nil {
			return false, err1
		}
		//fmt.Printf("seckey: %x\n", seckey)
		si := storageItem{}
		copy(si.key[:], key)
		copy(si.seckey[:], keyHash[:])
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.InsertNoReplace(&si)
		if bytes.Compare(key[:], lastKey[:]) > 0 {
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

func (dbs *PlainDBState) ForEachAccount(start []byte, cb func(address *common.Address, addrHash common.Hash), maxResults int) {
	results := 0
	err := WalkAsOf(dbs.tx, false /* storage */, start[:], 0, dbs.blockNr+1, func(ks, vs []byte) (bool, error) {
		if len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}

		if len(ks) > 20 {
			return true, nil
		}
		addr := common.BytesToAddress(ks)
		addrHash, err1 := common.HashData(ks)
		if err1 != nil {
			return false, err1
		}
		cb(&addr, addrHash)
		results++
		return results < maxResults, nil
	})
	if err != nil {
		log.Error("ForEachAccount walk error", "err", err)
	}
}

func (dbs *PlainDBState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var enc []byte
	var ok bool
	if dbs.accountCache != nil {
		enc, ok = dbs.accountCache.HasGet(nil, address[:])
	}
	if !ok {
		var err error
		enc, err = GetAsOf(dbs.tx, false /* storage */, address[:], dbs.blockNr+1)
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			return nil, err
		}
	}
	if !ok && dbs.accountCache != nil {
		dbs.accountCache.Set(address[:], enc)
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	//restore codehash
	if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
		codeHash, err := dbs.tx.GetOne(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation))
		if err != nil {
			return nil, err
		}
		if len(codeHash) > 0 {
			acc.CodeHash = common.BytesToHash(codeHash)
		}
	}
	return &acc, nil
}

func (dbs *PlainDBState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)
	if dbs.storageCache != nil {
		if enc, ok := dbs.storageCache.HasGet(nil, compositeKey); ok {
			return enc, nil
		}
	}
	enc, err := GetAsOf(dbs.tx, true /* storage */, compositeKey, dbs.blockNr+1)
	if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		return nil, err
	}
	if dbs.storageCache != nil {
		dbs.storageCache.Set(compositeKey, enc)
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (dbs *PlainDBState) ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if dbs.codeCache != nil {
		if code, ok := dbs.codeCache.HasGet(nil, address[:]); ok {
			return code, nil
		}
	}
	code, err := ethdb.Get(dbs.tx, dbutils.CodeBucket, codeHash[:])
	if dbs.codeCache != nil && len(code) <= 1024 {
		dbs.codeCache.Set(address[:], code)
	}
	if dbs.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		dbs.codeSizeCache.Set(address[:], b[:])
	}
	return code, err
}

func (dbs *PlainDBState) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return 0, nil
	}
	if dbs.codeSizeCache != nil {
		if b, ok := dbs.codeSizeCache.HasGet(nil, address[:]); ok {
			return int(binary.BigEndian.Uint32(b)), nil
		}
	}
	code, err := ethdb.Get(dbs.tx, dbutils.CodeBucket, codeHash[:])
	if err != nil {
		return 0, err
	}
	if dbs.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		dbs.codeSizeCache.Set(address[:], b[:])
	}
	return len(code), nil
}

func (dbs *PlainDBState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	enc, err := GetAsOf(dbs.tx, false /* storage */, address[:], dbs.blockNr+2)
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

// WalkStorageRange calls the walker for each storage item whose key starts with a given prefix,
// for no more than maxItems.
// Returns whether all matching storage items were traversed (provided there was no error).
func (dbs *PlainDBState) WalkStorageRange(addrHash common.Hash, prefix trie.Keybytes, maxItems int, walker func(common.Hash, big.Int)) (bool, error) {
	startkey := make([]byte, common.HashLength+common.IncarnationLength+common.HashLength)
	copy(startkey, addrHash[:])

	binary.BigEndian.PutUint64(startkey[common.HashLength:], changeset.DefaultIncarnation)
	copy(startkey[common.HashLength+common.IncarnationLength:], prefix.Data)

	fixedbits := (common.HashLength + common.IncarnationLength + len(prefix.Data)) * 8
	if prefix.Odd {
		fixedbits -= 4
	}

	i := 0

	err := WalkAsOf(dbs.tx, true /* storage */, startkey, fixedbits, dbs.blockNr+1,
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
func (dbs *PlainDBState) WalkRangeOfAccounts(prefix trie.Keybytes, maxItems int, walker func(common.Hash, *accounts.Account)) (bool, error) {
	startkey := make([]byte, common.HashLength)
	copy(startkey, prefix.Data)

	fixedbits := len(prefix.Data) * 8
	if prefix.Odd {
		fixedbits -= 4
	}

	i := 0

	var acc accounts.Account
	err := WalkAsOf(dbs.tx, false /* storage */, startkey, fixedbits, dbs.blockNr+1,
		func(key []byte, value []byte) (bool, error) {
			if len(key) > 32 {
				return true, nil
			}
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
