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
	"runtime"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
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

// If highZero is true, the most significant bits of every byte is left zero
func encodeTimestamp(timestamp uint64) []byte {
	var suffix []byte
	var limit uint64
	limit = 32
	for bytecount := 1; bytecount <= 8; bytecount++ {
		if timestamp < limit {
			suffix = make([]byte, bytecount)
			b := timestamp
			for i := bytecount - 1; i > 0; i-- {
				suffix[i] = byte(b & 0xff)
				b >>= 8
			}
			suffix[0] = byte(b) | (byte(bytecount) << 5) // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return suffix
}

func (rds *RepairDbState) CheckKeys() {
	aSet := make(map[string]struct{})
	suffix := encodeTimestamp(rds.blockNr)
	{
		suffixkey := make([]byte, len(suffix)+len(AccountsHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], AccountsHistoryBucket)
		v, _ := rds.historyDb.Get(ethdb.SuffixBucket, suffixkey)
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
		for a, _ := range aSet {
			if _, ok := rds.accountsKeys[a]; !ok {
				aDiff = true
				break
			}
		}
	}
	if aDiff {
		fmt.Printf("Accounts key set does not match for block %d\n", rds.blockNr)
		newlen := 4 + len(rds.accountsKeys)
		for key, _ := range rds.accountsKeys {
			newlen += len(key)
		}
		dv := make([]byte, newlen)
		binary.BigEndian.PutUint32(dv, uint32(len(rds.accountsKeys)))
		i := 4
		for key, _ := range rds.accountsKeys {
			dv[i] = byte(len(key))
			i++
			copy(dv[i:], key)
			i += len(key)
		}
		suffixkey := make([]byte, len(suffix)+len(AccountsHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], AccountsHistoryBucket)
		//if err := rds.historyDb.Put(ethdb.SuffixBucket, suffixkey, dv); err != nil {
		//	panic(err)
		//}
	}
	sSet := make(map[string]struct{})
	{
		suffixkey := make([]byte, len(suffix)+len(StorageHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], StorageHistoryBucket)
		v, _ := rds.historyDb.Get(ethdb.SuffixBucket, suffixkey)
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
		for s, _ := range sSet {
			if _, ok := rds.storageKeys[s]; !ok {
				sDiff = true
				break
			}
		}
	}
	if sDiff {
		fmt.Printf("Storage key set does not match for block %d\n", rds.blockNr)
		newlen := 4 + len(rds.storageKeys)
		for key, _ := range rds.storageKeys {
			newlen += len(key)
		}
		dv := make([]byte, newlen)
		binary.BigEndian.PutUint32(dv, uint32(len(rds.storageKeys)))
		i := 4
		for key, _ := range rds.storageKeys {
			dv[i] = byte(len(key))
			i++
			copy(dv[i:], key)
			i += len(key)
		}
		suffixkey := make([]byte, len(suffix)+len(StorageHistoryBucket))
		copy(suffixkey, suffix)
		copy(suffixkey[len(suffix):], StorageHistoryBucket)
		//if err := rds.historyDb.Put(ethdb.SuffixBucket, suffixkey, dv); err != nil {
		//	panic(err)
		//}
	}
}

func (rds *RepairDbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := rds.currentDb.Get(AccountsBucket, buf[:])
	if err != nil || enc == nil || len(enc) == 0 {
		return nil, nil
	}
	return encodingToAccount(enc)
}

func (rds *RepairDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := rds.currentDb.Get(StorageBucket, append(address[:], buf[:]...))
	if err != nil || enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (rds *RepairDbState) ReadAccountCode(codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	return rds.currentDb.Get(CodeBucket, codeHash[:])
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
			t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		} else {
			t = trie.New(account.Root, StorageBucket, address[:], true)
		}
		t.MakeListed(rds.joinGeneration, rds.leftGeneration)
		rds.storageTries[address] = t
	}
	return t, nil
}

func (rds *RepairDbState) UpdateAccountData(address common.Address, original, account *Account) error {
	// Perform resolutions first
	var resolver *trie.TrieResolver
	var storageTrie *trie.Trie
	var err error
	if m, ok := rds.storageUpdates[address]; ok {
		storageTrie, err = rds.getStorageTrie(address, true)
		if err != nil {
			return err
		}
		hashes := make(Hashes, len(m))
		i := 0
		for keyHash, _ := range m {
			hashes[i] = keyHash
			i++
		}
		sort.Sort(hashes)
		for _, keyHash := range hashes {
			if need, c := storageTrie.NeedResolution(keyHash[:]); need {
				if resolver == nil {
					resolver = trie.NewResolver(rds.currentDb, false, false, rds.blockNr)
				}
				resolver.AddContinuation(c)
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
		for keyHash, _ := range m {
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
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	rds.accountsKeys[string(addrHash[:])] = struct{}{}
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	if err = rds.currentDb.Put(AccountsBucket, addrHash[:], data); err != nil {
		return err
	}
	var originalData []byte
	if original.Balance == nil {
		originalData = []byte{}
	} else {
		originalData, err = accountToEncoding(original)
		if err != nil {
			return err
		}
	}
	v, _ := rds.historyDb.GetS(AccountsHistoryBucket, addrHash[:], rds.blockNr)
	if !bytes.Equal(v, originalData) {
		fmt.Printf("REPAIR (UpdateAccountData): At block %d, address: %x, expected %x, found %x\n", rds.blockNr, address, originalData, v)
		//return rds.historyDb.PutS(AccountsHistoryBucket, addrHash[:], originalData, rds.blockNr)
		return nil
	}
	return nil
}

func (rds *RepairDbState) DeleteAccount(address common.Address, original *Account) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var addrHash common.Hash
	h.sha.Read(addrHash[:])
	rds.accountsKeys[string(addrHash[:])] = struct{}{}

	delete(rds.storageUpdates, address)

	if err := rds.currentDb.Delete(AccountsBucket, addrHash[:]); err != nil {
		return err
	}
	var originalData []byte
	var err error
	if original.Balance == nil {
		// Account has been created and deleted in the same block
		originalData = []byte{}
	} else {
		originalData, err = accountToEncoding(original)
		if err != nil {
			return err
		}
	}
	v, _ := rds.historyDb.GetS(AccountsHistoryBucket, addrHash[:], rds.blockNr)
	if !bytes.Equal(v, originalData) {
		fmt.Printf("REPAIR (DeleteAccount): At block %d, address: %x, expected %x, found %x\n", rds.blockNr, address, originalData, v)
		//return rds.historyDb.PutS(AccountsHistoryBucket, addrHash[:], originalData, rds.blockNr)
		return nil
	}
	return nil
}

func (rds *RepairDbState) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return rds.currentDb.Put(CodeBucket, codeHash[:], code)
}

func (rds *RepairDbState) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var seckey common.Hash
	h.sha.Read(seckey[:])
	compositeKey := append(address[:], seckey[:]...)
	if *original == *value {
		val, _ := rds.historyDb.GetS(StorageHistoryBucket, compositeKey, rds.blockNr)
		if val != nil {
			fmt.Printf("REPAIR (WriteAccountStorage): At block %d, address: %x, key %x, expected nil, found %x\n", rds.blockNr, address, key, val)
			//suffix := encodeTimestamp(rds.blockNr)
			//return rds.historyDb.Delete(StorageHistoryBucket, append(compositeKey, suffix...))
			return nil
		}
		return nil
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	m, ok := rds.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		rds.storageUpdates[address] = m
	}
	if len(v) > 0 {
		m[seckey] = vv
	} else {
		m[seckey] = nil
	}

	rds.storageKeys[string(compositeKey)] = struct{}{}
	var err error
	if len(v) == 0 {
		err = rds.currentDb.Delete(StorageBucket, compositeKey)
	} else {
		err = rds.currentDb.Put(StorageBucket, compositeKey, vv)
	}
	if err != nil {
		return err
	}

	o := bytes.TrimLeft(original[:], "\x00")
	oo := make([]byte, len(o))
	copy(oo, o)
	val, _ := rds.historyDb.GetS(StorageHistoryBucket, compositeKey, rds.blockNr)
	if !bytes.Equal(val, oo) {
		fmt.Printf("REPAIR (WriteAccountStorage): At block %d, address: %x, key %x, expected %x, found %x\n", rds.blockNr, address, key, oo, val)
		//return rds.historyDb.PutS(StorageHistoryBucket, compositeKey, oo, rds.blockNr)
		return nil
	}
	return nil
}

func (rds *RepairDbState) joinGeneration(gen uint64) {
	rds.nodeCount++
	rds.generationCounts[gen]++

}

func (rds *RepairDbState) leftGeneration(gen uint64) {
	rds.nodeCount--
	rds.generationCounts[gen]--
}

func (rds *RepairDbState) PruneTries() {
	if rds.nodeCount > int(MaxTrieCacheGen) {
		toRemove := 0
		excess := rds.nodeCount - int(MaxTrieCacheGen)
		gen := rds.oldestGeneration
		for excess > 0 {
			excess -= rds.generationCounts[gen]
			toRemove += rds.generationCounts[gen]
			delete(rds.generationCounts, gen)
			gen++
		}
		// Unload all nodes with touch timestamp < gen
		for address, storageTrie := range rds.storageTries {
			empty := storageTrie.UnloadOlderThan(gen, false)
			if empty {
				delete(rds.storageTries, address)
			}
		}
		rds.oldestGeneration = gen
		rds.nodeCount -= toRemove
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory: nodes=%d, alloc=%d, sys=%d\n", rds.nodeCount, int(m.Alloc/1024), int(m.Sys/1024))
}
