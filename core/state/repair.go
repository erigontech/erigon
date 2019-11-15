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
	"fmt"
	"runtime"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

type RepairDbState struct {
	currentDb        ethdb.Database
	historyDb        ethdb.Database
	blockNr          uint64
	storageTries     map[common.Address]*trie.Trie
	storageUpdates   map[common.Address]map[common.Hash][]byte
	generationCounts map[uint64]int
	nodeCount        int
	oldestGeneration uint64
	accountsKeys     map[string]struct{}
	storageKeys      map[string]struct{}
}

func NewRepairDbState(currentDb ethdb.Database, historyDb ethdb.Database, blockNr uint64) *RepairDbState {
	return &RepairDbState{
		currentDb:        currentDb,
		historyDb:        historyDb,
		blockNr:          blockNr,
		storageTries:     make(map[common.Address]*trie.Trie),
		storageUpdates:   make(map[common.Address]map[common.Hash][]byte),
		generationCounts: make(map[uint64]int, 4096),
		oldestGeneration: blockNr,
		accountsKeys:     make(map[string]struct{}),
		storageKeys:      make(map[string]struct{}),
	}
}

func (rds *RepairDbState) SetBlockNr(blockNr uint64) {
	rds.blockNr = blockNr
	rds.accountsKeys = make(map[string]struct{})
	rds.storageKeys = make(map[string]struct{})
}

func (rds *RepairDbState) CheckKeys() {
	aSet := make(map[string]struct{})
	suffix := dbutils.EncodeTimestamp(rds.blockNr)
	{
		suffixkey := make([]byte, len(suffix)+len(dbutils.AccountsHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], dbutils.AccountsHistoryBucket)
		v, _ := rds.historyDb.Get(dbutils.ChangeSetBucket, suffixkey)
		if len(v) > 0 {
			keycount := int(binary.BigEndian.Uint32(v))
			for i, ki := 4, 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				aSet[string(v[i:i+l])] = struct{}{}
				i += l
			}
		}
	}
	aDiff := len(aSet) != len(rds.accountsKeys)
	if !aDiff {
		for a := range aSet {
			if _, ok := rds.accountsKeys[a]; !ok {
				aDiff = true
				break
			}
		}
	}
	if aDiff {
		fmt.Printf("Accounts key set does not match for block %d\n", rds.blockNr)
		newlen := 4 + len(rds.accountsKeys)
		for key := range rds.accountsKeys {
			newlen += len(key)
		}
		dv := make([]byte, newlen)
		binary.BigEndian.PutUint32(dv, uint32(len(rds.accountsKeys)))
		i := 4
		for key := range rds.accountsKeys {
			dv[i] = byte(len(key))
			i++
			copy(dv[i:], key)
			i += len(key)
		}
		suffixkey := make([]byte, len(suffix)+len(dbutils.AccountsHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], dbutils.AccountsHistoryBucket)
		//if err := rds.historyDb.Put(ethdb.ChangeSetBucket, suffixkey, dv); err != nil {
		//	panic(err)
		//}
	}
	sSet := make(map[string]struct{})
	{
		suffixkey := make([]byte, len(suffix)+len(dbutils.StorageHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], dbutils.StorageHistoryBucket)
		v, _ := rds.historyDb.Get(dbutils.ChangeSetBucket, suffixkey)
		if len(v) > 0 {
			keycount := int(binary.BigEndian.Uint32(v))
			for i, ki := 4, 0; ki < keycount; ki++ {
				l := int(v[i])
				i++
				sSet[string(v[i:i+l])] = struct{}{}
				i += l
			}
		}
	}
	sDiff := len(sSet) != len(rds.storageKeys)
	if !sDiff {
		for s := range sSet {
			if _, ok := rds.storageKeys[s]; !ok {
				sDiff = true
				break
			}
		}
	}
	if sDiff {
		fmt.Printf("Storage key set does not match for block %d\n", rds.blockNr)
		newlen := 4 + len(rds.storageKeys)
		for key := range rds.storageKeys {
			newlen += len(key)
		}
		dv := make([]byte, newlen)
		binary.BigEndian.PutUint32(dv, uint32(len(rds.storageKeys)))
		i := 4
		for key := range rds.storageKeys {
			dv[i] = byte(len(key))
			i++
			copy(dv[i:], key)
			i += len(key)
		}
		suffixkey := make([]byte, len(suffix)+len(dbutils.StorageHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], dbutils.StorageHistoryBucket)
		//if err := rds.historyDb.Put(ethdb.ChangeSetBucket, suffixkey, dv); err != nil {
		//	panic(err)
		//}
	}
}

func (rds *RepairDbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	enc, err := rds.currentDb.Get(dbutils.AccountsBucket, addrHash[:])
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &acc, nil
}

func (rds *RepairDbState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	keyHash, err := common.HashData(address[:])
	if err != nil {
		return []byte{}, err
	}
	enc, err := rds.currentDb.Get(dbutils.StorageBucket, append(address[:], keyHash[:]...))
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (rds *RepairDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return rds.currentDb.Get(dbutils.CodeBucket, codeHash[:])
}

func (rds *RepairDbState) ReadAccountCodeSize(codeHash common.Hash) (int, error) {
	code, err := rds.ReadAccountCode(codeHash)
	if err != nil {
		return 0, err
	}
	return len(code), nil
}

func (rds *RepairDbState) getStorageTrie(address common.Address, create bool) (*trie.Trie, error) {
	t, ok := rds.storageTries[address]
	if !ok && create {
		account, err := rds.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if account == nil {
			t = trie.New(common.Hash{})
		} else {
			t = trie.New(account.Root)
		}
		rds.storageTries[address] = t
	}
	return t, nil
}

func (rds *RepairDbState) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	// Perform resolutions first
	var resolver *trie.Resolver
	var storageTrie *trie.Trie
	var err error
	if m, ok := rds.storageUpdates[address]; ok {
		storageTrie, err = rds.getStorageTrie(address, true)
		if err != nil {
			return err
		}
		hashes := make(Hashes, len(m))
		i := 0
		for keyHash := range m {
			hashes[i] = keyHash
			i++
		}
		sort.Sort(hashes)
		for _, keyHash := range hashes {
			addrHash := crypto.Keccak256(address.Bytes())
			if need, req := storageTrie.NeedResolution(addrHash, keyHash[:]); need {
				if resolver == nil {
					resolver = trie.NewResolver(0, false, rds.blockNr)
				}
				resolver.AddRequest(req)
			}
		}
	}
	if resolver != nil {
		if err := resolver.ResolveWithDb(rds.currentDb, rds.blockNr); err != nil {
			return err
		}
		resolver = nil
	}
	if m, ok := rds.storageUpdates[address]; ok {
		storageTrie, err = rds.getStorageTrie(address, true)
		if err != nil {
			return err
		}
		hashes := make(Hashes, len(m))
		i := 0
		for keyHash := range m {
			hashes[i] = keyHash
			i++
		}
		sort.Sort(hashes)
		for _, keyHash := range hashes {
			v := m[keyHash]
			if len(v) > 0 {
				storageTrie.Update(keyHash[:], v, rds.blockNr)
			} else {
				storageTrie.Delete(keyHash[:], rds.blockNr)
			}
		}
		delete(rds.storageUpdates, address)
	}
	if storageTrie != nil {
		account.Root = storageTrie.Hash()
	}
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	rds.accountsKeys[string(addrHash[:])] = struct{}{}
	dataLen := account.EncodingLengthForStorage()
	data := make([]byte, dataLen)
	account.EncodeForStorage(data)
	if err = rds.currentDb.Put(dbutils.AccountsBucket, addrHash[:], data); err != nil {
		return err
	}
	var originalData []byte
	if !original.Initialised {
		originalData = []byte{}
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
	}
	v, _ := rds.historyDb.GetS(dbutils.AccountsHistoryBucket, addrHash[:], rds.blockNr)
	if !bytes.Equal(v, originalData) {
		fmt.Printf("REPAIR (UpdateAccountData): At block %d, address: %x, expected %x, found %x\n", rds.blockNr, address, originalData, v)
		//return rds.historyDb.PutS(dbutils.AccountsHistoryBucket, addrHash[:], originalData, rds.blockNr)
		return nil
	}
	return nil
}

func (rds *RepairDbState) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return err
	}
	rds.accountsKeys[string(addrHash[:])] = struct{}{}

	delete(rds.storageUpdates, address)

	if err := rds.currentDb.Delete(dbutils.AccountsBucket, addrHash[:]); err != nil {
		return err
	}
	var originalData []byte
	if !original.Initialised {
		// Account has been created and deleted in the same block
		originalData = []byte{}
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
	}
	v, _ := rds.historyDb.GetS(dbutils.AccountsHistoryBucket, addrHash[:], rds.blockNr)
	if !bytes.Equal(v, originalData) {
		fmt.Printf("REPAIR (DeleteAccount): At block %d, address: %x, expected %x, found %x\n", rds.blockNr, address, originalData, v)
		//return rds.historyDb.PutS(dbutils.AccountsHistoryBucket, addrHash[:], originalData, rds.blockNr)
		return nil
	}
	return nil
}

func (rds *RepairDbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return rds.currentDb.Put(dbutils.CodeBucket, codeHash[:], code)
}

func (rds *RepairDbState) WriteAccountStorage(address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	seckey, err := common.HashData(key[:])
	if err != nil {
		return err
	}
	compositeKey := append(address[:], seckey[:]...)
	if *original == *value {
		val, _ := rds.historyDb.GetS(dbutils.StorageHistoryBucket, compositeKey, rds.blockNr)
		if val != nil {
			fmt.Printf("REPAIR (WriteAccountStorage): At block %d, address: %x, key %x, expected nil, found %x\n", rds.blockNr, address, key, val)
			//suffix := encodeTimestamp(rds.blockNr)
			//return rds.historyDb.Delete(dbutils.StorageHistoryBucket, append(compositeKey, suffix...))
			return nil
		}
		return nil
	}
	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := rds.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		rds.storageUpdates[address] = m
	}
	if len(v) > 0 {
		m[seckey] = AddExtraRLPLevel(v)
	} else {
		m[seckey] = nil
	}
	rds.storageKeys[string(compositeKey)] = struct{}{}
	if len(v) == 0 {
		err = rds.currentDb.Delete(dbutils.StorageBucket, compositeKey)
	} else {
		err = rds.currentDb.Put(dbutils.StorageBucket, compositeKey, common.CopyBytes(v))
	}
	if err != nil {
		return err
	}

	o := bytes.TrimLeft(original[:], "\x00")
	oo := make([]byte, len(o))
	copy(oo, o)
	val, _ := rds.historyDb.GetS(dbutils.StorageHistoryBucket, compositeKey, rds.blockNr)
	if !bytes.Equal(val, oo) {
		fmt.Printf("REPAIR (WriteAccountStorage): At block %d, address: %x, key %x, expected %x, found %x\n", rds.blockNr, address, key, oo, val)
		//return rds.historyDb.PutS(dbutils.StorageHistoryBucket, compositeKey, oo, rds.blockNr)
		return nil
	}
	return nil
}

func (rds *RepairDbState) CreateContract(address common.Address) error {
	return nil
}

func (rds *RepairDbState) PruneTries() {
	// TODO Reintroduce pruning if necessary
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory: nodes=%d, alloc=%d, sys=%d\n", rds.nodeCount, int(m.Alloc/1024), int(m.Sys/1024))
}
