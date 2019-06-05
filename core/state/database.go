// Copyright 2017 The go-ethereum Authors
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
	"fmt"
	"hash"
	"io"
	"math/big"
	"runtime"
	"sort"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
	"golang.org/x/crypto/sha3"
)

// Trie cache generation limit after which to evict trie nodes from memory.
var MaxTrieCacheGen = uint32(4 * 1024 * 1024)

var AccountsBucket = []byte("AT")
var AccountsHistoryBucket = []byte("hAT")
var StorageBucket = []byte("ST")
var StorageHistoryBucket = []byte("hST")
var CodeBucket = []byte("CODE")

const (
	// Number of past tries to keep. This value is chosen such that
	// reasonable chain reorg depths will hit an existing trie.
	maxPastTries = 12

	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 100000
)

type StateReader interface {
	ReadAccountData(address common.Address) (*Account, error)
	ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error)
	ReadAccountCode(codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(codeHash common.Hash) (int, error)
}

type StateWriter interface {
	UpdateAccountData(address common.Address, original, account *Account) error
	UpdateAccountCode(codeHash common.Hash, code []byte) error
	DeleteAccount(address common.Address, original *Account) error
	WriteAccountStorage(address common.Address, key, original, value *common.Hash) error
}

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type hasher struct {
	sha keccakState
}

var hasherPool = make(chan *hasher, 128)

func newHasher() *hasher {
	var h *hasher
	select {
	case h = <-hasherPool:
	default:
		h = &hasher{sha: sha3.NewLegacyKeccak256().(keccakState)}
	}
	return h
}

func returnHasherToPool(h *hasher) {
	select {
	case hasherPool <- h:
	default:
		fmt.Printf("Allowing hasher to be garbage collected, pool is full\n")
	}
}

type NoopWriter struct {
}

func NewNoopWriter() *NoopWriter {
	return &NoopWriter{}
}

func (nw *NoopWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	return nil
}

func (nw *NoopWriter) DeleteAccount(address common.Address, original *Account) error {
	return nil
}

func (nw *NoopWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (nw *NoopWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	return nil
}

// Implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                *trie.Trie
	db               ethdb.Database
	blockNr          uint64
	storageTries     map[common.Hash]*trie.Trie
	storageUpdates   map[common.Address]map[common.Hash][]byte
	accountUpdates   map[common.Hash]*Account
	deleted          map[common.Hash]struct{}
	codeCache        *lru.Cache
	codeSizeCache    *lru.Cache
	historical       bool
	generationCounts map[uint64]int
	nodeCount        int
	oldestGeneration uint64
	noHistory        bool
	resolveReads     bool
	proofMasks       map[string]uint32
	sMasks           map[string]map[string]uint32
	proofHashes      map[string][16]common.Hash
	sHashes          map[string]map[string][16]common.Hash
	soleHashes       map[string]common.Hash
	sSoleHashes      map[string]map[string]common.Hash
	createdProofs    map[string]struct{}
	sCreatedProofs   map[string]map[string]struct{}
	proofShorts      map[string][]byte
	sShorts          map[string]map[string][]byte
	createdShorts    map[string]struct{}
	sCreatedShorts   map[string]map[string]struct{}
	proofValues      map[string][]byte
	sValues          map[string]map[string][]byte
	proofCodes       map[common.Hash][]byte
	createdCodes     map[common.Hash]struct{}
}

func NewTrieDbState(root common.Hash, db ethdb.Database, blockNr uint64) (*TrieDbState, error) {
	csc, err := lru.New(100000)
	if err != nil {
		return nil, err
	}
	cc, err := lru.New(10000)
	if err != nil {
		return nil, err
	}
	t := trie.New(root, AccountsBucket, nil, false)
	tds := TrieDbState{
		t:              t,
		db:             db,
		blockNr:        blockNr,
		storageTries:   make(map[common.Hash]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted:        make(map[common.Hash]struct{}),
		proofMasks:     make(map[string]uint32),
		sMasks:         make(map[string]map[string]uint32),
		proofHashes:    make(map[string][16]common.Hash),
		sHashes:        make(map[string]map[string][16]common.Hash),
		soleHashes:     make(map[string]common.Hash),
		sSoleHashes:    make(map[string]map[string]common.Hash),
		createdProofs:  make(map[string]struct{}),
		sCreatedProofs: make(map[string]map[string]struct{}),
		proofShorts:    make(map[string][]byte),
		sShorts:        make(map[string]map[string][]byte),
		createdShorts:  make(map[string]struct{}),
		sCreatedShorts: make(map[string]map[string]struct{}),
		proofValues:    make(map[string][]byte),
		sValues:        make(map[string]map[string][]byte),
		proofCodes:     make(map[common.Hash][]byte),
		createdCodes:   make(map[common.Hash]struct{}),
		codeCache:      cc,
		codeSizeCache:  csc,
	}
	t.MakeListed(tds.joinGeneration, tds.leftGeneration)
	t.ProofFunctions(tds.addProof, tds.addSoleHash, tds.createProof, tds.addValue, tds.addShort, tds.createShort)
	tds.generationCounts = make(map[uint64]int, 4096)
	tds.oldestGeneration = blockNr
	return &tds, nil
}

func (tds *TrieDbState) SetHistorical(h bool) {
	tds.historical = h
	tds.t.SetHistorical(h)
}

func (tds *TrieDbState) SetResolveReads(rr bool) {
	if tds.resolveReads != rr {
		tds.resolveReads = rr
		tds.t.SetResolveReads(rr)
		for _, st := range tds.storageTries {
			st.SetResolveReads(rr)
		}
	}
}

func (tds *TrieDbState) SetNoHistory(nh bool) {
	tds.noHistory = nh
}

func (tds *TrieDbState) Copy() *TrieDbState {
	tcopy := *tds.t
	cpy := TrieDbState{
		t:              &tcopy,
		db:             tds.db,
		blockNr:        tds.blockNr,
		storageTries:   make(map[common.Hash]*trie.Trie),
		storageUpdates: make(map[common.Address]map[common.Hash][]byte),
		accountUpdates: make(map[common.Hash]*Account),
		deleted:        make(map[common.Hash]struct{}),
		proofMasks:     make(map[string]uint32),
		sMasks:         make(map[string]map[string]uint32),
		proofHashes:    make(map[string][16]common.Hash),
		sHashes:        make(map[string]map[string][16]common.Hash),
		soleHashes:     make(map[string]common.Hash),
		sSoleHashes:    make(map[string]map[string]common.Hash),
		createdProofs:  make(map[string]struct{}),
		sCreatedProofs: make(map[string]map[string]struct{}),
		proofShorts:    make(map[string][]byte),
		sShorts:        make(map[string]map[string][]byte),
		createdShorts:  make(map[string]struct{}),
		sCreatedShorts: make(map[string]map[string]struct{}),
		proofValues:    make(map[string][]byte),
		sValues:        make(map[string]map[string][]byte),
		proofCodes:     make(map[common.Hash][]byte),
		createdCodes:   make(map[common.Hash]struct{}),
	}
	return &cpy
}

func (tds *TrieDbState) Database() ethdb.Database {
	return tds.db
}

func (tds *TrieDbState) AccountTrie() *trie.Trie {
	return tds.t
}

func (tds *TrieDbState) TrieRoot() (common.Hash, error) {
	root, err := tds.trieRoot(true)
	tds.clearUpdates()
	return root, err
}

func (tds *TrieDbState) extractProofs(prefix []byte, trace bool) (
	masks []uint16, hashes []common.Hash, shortKeys [][]byte, values [][]byte,
) {
	if trace {
		fmt.Printf("Extracting proofs for prefix %x\n", prefix)
		if prefix != nil {
			h := newHasher()
			defer returnHasherToPool(h)
			h.sha.Reset()
			h.sha.Write(prefix)
			var buf common.Hash
			h.sha.Read(buf[:])
			fmt.Printf("prefix hash: %x\n", buf)
		}
	}
	var proofMasks map[string]uint32
	if prefix == nil {
		proofMasks = tds.proofMasks
	} else {
		var ok bool
		ps := string(prefix)
		proofMasks, ok = tds.sMasks[ps]
		if !ok {
			proofMasks = make(map[string]uint32)
		}
	}
	var proofHashes map[string][16]common.Hash
	if prefix == nil {
		proofHashes = tds.proofHashes
	} else {
		var ok bool
		ps := string(prefix)
		proofHashes, ok = tds.sHashes[ps]
		if !ok {
			proofHashes = make(map[string][16]common.Hash)
		}
	}
	var soleHashes map[string]common.Hash
	if prefix == nil {
		soleHashes = tds.soleHashes
	} else {
		var ok bool
		ps := string(prefix)
		soleHashes, ok = tds.sSoleHashes[ps]
		if !ok {
			soleHashes = make(map[string]common.Hash)
		}
	}
	var proofValues map[string][]byte
	if prefix == nil {
		proofValues = tds.proofValues
	} else {
		var ok bool
		ps := string(prefix)
		proofValues, ok = tds.sValues[ps]
		if !ok {
			proofValues = make(map[string][]byte)
		}
	}
	var proofShorts map[string][]byte
	if prefix == nil {
		proofShorts = tds.proofShorts
	} else {
		var ok bool
		ps := string(prefix)
		proofShorts, ok = tds.sShorts[ps]
		if !ok {
			proofShorts = make(map[string][]byte)
		}
	}
	// Collect all the strings
	keys := []string{}
	keySet := make(map[string]struct{})
	for key := range proofMasks {
		if _, ok := keySet[key]; !ok {
			keys = append(keys, key)
			keySet[key] = struct{}{}
		}
	}
	for key := range proofShorts {
		if _, ok := keySet[key]; !ok {
			keys = append(keys, key)
			keySet[key] = struct{}{}
		}
	}
	for key := range proofValues {
		if _, ok := keySet[key]; !ok {
			keys = append(keys, key)
			keySet[key] = struct{}{}
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if trace {
			fmt.Printf("%x\n", key)
		}
		if hashmask, ok := proofMasks[key]; ok {
			// Determine the downward mask
			var fullnodemask uint16
			var shortnodemask uint16
			for nibble := byte(0); nibble < 16; nibble++ {
				if _, ok2 := proofShorts[key+string(nibble)]; ok2 {
					shortnodemask |= (uint16(1) << nibble)
				}
				if _, ok3 := proofMasks[key+string(nibble)]; ok3 {
					fullnodemask |= (uint16(1) << nibble)
				}
			}
			h := proofHashes[key]
			for i := byte(0); i < 16; i++ {
				if (hashmask & (uint32(1) << i)) != 0 {
					hashes = append(hashes, h[i])
				}
			}
			if trace {
				fmt.Printf("%x: hash %16b, full %16b, short %16b\n", key, hashmask, fullnodemask, shortnodemask)
			}
			if len(masks) == 0 {
				masks = append(masks, 0)
			}
			masks = append(masks, uint16(hashmask))      // Hash mask
			masks = append(masks, uint16(fullnodemask))  // Fullnode mask
			masks = append(masks, uint16(shortnodemask)) // Short node mask
		}
		if short, ok := proofShorts[key]; ok {
			if trace {
				fmt.Printf("Short %x: %x\n", []byte(key), short)
			}
			var downmask uint16
			if _, ok2 := proofHashes[key+string(short)]; ok2 {
				downmask = 1
			} else if h, ok1 := soleHashes[key+string(short)]; ok1 {
				if trace {
					fmt.Printf("Sole hash: %x\n", h[:2])
				}
				hashes = append(hashes, h)
			}
			if trace {
				fmt.Printf("Down %16b\n", downmask)
			}
			if len(masks) == 0 {
				masks = append(masks, 1)
			}
			masks = append(masks, downmask)
			shortKeys = append(shortKeys, short)
		}
		if value, ok := proofValues[key]; ok {
			if trace {
				fmt.Printf("Value %x\n", value)
			}
			values = append(values, value)
		}
	}
	if trace {
		fmt.Printf("Masks:")
		for _, mask := range masks {
			fmt.Printf(" %16b", mask)
		}
		fmt.Printf("\n")
		fmt.Printf("Shorts:")
		for _, short := range shortKeys {
			fmt.Printf(" %x", short)
		}
		fmt.Printf("\n")
		fmt.Printf("Hashes:")
		for _, hash := range hashes {
			fmt.Printf(" %x", hash[:4])
		}
		fmt.Printf("\n")
		fmt.Printf("Values:")
		for _, value := range values {
			if value == nil {
				fmt.Printf(" nil")
			} else {
				fmt.Printf(" %x", value)
			}
		}
		fmt.Printf("\n")
	}
	return masks, hashes, shortKeys, values
}

func (tds *TrieDbState) ExtractProofs(trace bool) BlockProof {
	if trace {
		fmt.Printf("Extracting proofs for block %d\n", tds.blockNr)
	}
	// Collect prefixes
	prefixes := []string{}
	prefixSet := make(map[string]struct{})
	for prefix := range tds.sMasks {
		if _, ok := prefixSet[prefix]; !ok {
			prefixes = append(prefixes, prefix)
			prefixSet[prefix] = struct{}{}
		}
	}
	for prefix := range tds.sShorts {
		if _, ok := prefixSet[prefix]; !ok {
			prefixes = append(prefixes, prefix)
			prefixSet[prefix] = struct{}{}
		}
	}
	for prefix := range tds.sValues {
		if _, ok := prefixSet[prefix]; !ok {
			prefixes = append(prefixes, prefix)
			prefixSet[prefix] = struct{}{}
		}
	}
	sort.Strings(prefixes)
	var contracts []common.Address
	var cMasks []uint16
	var cHashes []common.Hash
	var cShortKeys [][]byte
	var cValues [][]byte
	for _, prefix := range prefixes {
		m, h, s, v := tds.extractProofs([]byte(prefix), trace)
		if len(m) > 0 || len(h) > 0 || len(s) > 0 || len(v) > 0 {
			contracts = append(contracts, common.BytesToAddress([]byte(prefix)))
			cMasks = append(cMasks, m...)
			cHashes = append(cHashes, h...)
			cShortKeys = append(cShortKeys, s...)
			cValues = append(cValues, v...)
		}
	}
	masks, hashes, shortKeys, values := tds.extractProofs(nil, trace)
	var codes [][]byte
	for _, code := range tds.proofCodes {
		codes = append(codes, code)
	}
	tds.proofMasks = make(map[string]uint32)
	tds.sMasks = make(map[string]map[string]uint32)
	tds.proofHashes = make(map[string][16]common.Hash)
	tds.sHashes = make(map[string]map[string][16]common.Hash)
	tds.soleHashes = make(map[string]common.Hash)
	tds.sSoleHashes = make(map[string]map[string]common.Hash)
	tds.createdProofs = make(map[string]struct{})
	tds.sCreatedProofs = make(map[string]map[string]struct{})
	tds.proofShorts = make(map[string][]byte)
	tds.sShorts = make(map[string]map[string][]byte)
	tds.createdShorts = make(map[string]struct{})
	tds.sCreatedShorts = make(map[string]map[string]struct{})
	tds.proofValues = make(map[string][]byte)
	tds.sValues = make(map[string]map[string][]byte)
	tds.proofCodes = make(map[common.Hash][]byte)
	tds.createdCodes = make(map[common.Hash]struct{})
	return BlockProof{contracts, cMasks, cHashes, cShortKeys, cValues, codes, masks, hashes, shortKeys, values}
}

func (tds *TrieDbState) PrintTrie(w io.Writer) {
	tds.t.Print(w)
	for _, storageTrie := range tds.storageTries {
		storageTrie.Print(w)
	}
}

func (tds *TrieDbState) PrintStorageTrie(w io.Writer, addrHash common.Hash) {
	storageTrie := tds.storageTries[addrHash]
	storageTrie.Print(w)
}

type Hashes []common.Hash

func (hashes Hashes) Len() int {
	return len(hashes)
}
func (hashes Hashes) Less(i, j int) bool {
	return bytes.Compare(hashes[i][:], hashes[j][:]) == -1
}
func (hashes Hashes) Swap(i, j int) {
	hashes[i], hashes[j] = hashes[j], hashes[i]
}

func (tds *TrieDbState) trieRoot(forward bool) (common.Hash, error) {
	if len(tds.storageUpdates) == 0 && len(tds.accountUpdates) == 0 {
		return tds.t.Hash(), nil
	}
	// Perform resolutions first
	var resolver *trie.TrieResolver
	for address, m := range tds.storageUpdates {
		addrHash, err := tds.HashAddress(&address, false /*save*/)
		if err != nil {
			return common.Hash{}, nil
		}
		if _, ok := tds.deleted[addrHash]; ok {
			continue
		}
		storageTrie, err := tds.getStorageTrie(address, addrHash, true)
		if err != nil {
			return common.Hash{}, err
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
					resolver = trie.NewResolver(tds.db, false, false, tds.blockNr)
					resolver.SetHistorical(tds.historical)
				}
				resolver.AddContinuation(c)
			}
		}
	}
	if resolver != nil {
		if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
			return common.Hash{}, err
		}
		resolver = nil
	}
	for address, m := range tds.storageUpdates {
		addrHash, err := tds.HashAddress(&address, false /*save*/)
		if err != nil {
			return common.Hash{}, nil
		}
		if _, ok := tds.deleted[addrHash]; ok {
			continue
		}
		storageTrie, err := tds.getStorageTrie(address, addrHash, true)
		if err != nil {
			return common.Hash{}, err
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
				storageTrie.Update(keyHash[:], v, tds.blockNr)
			} else {
				storageTrie.Delete(keyHash[:], tds.blockNr)
			}
		}
	}
	addrs := make(Hashes, len(tds.accountUpdates))
	i := 0
	resolver = nil
	for addrHash, _ := range tds.accountUpdates {
		addrs[i] = addrHash
		i++
	}
	sort.Sort(addrs)
	for _, addrHash := range addrs {
		if need, c := tds.t.NeedResolution(addrHash[:]); need {
			if resolver == nil {
				resolver = trie.NewResolver(tds.db, false, true, tds.blockNr)
				resolver.SetHistorical(tds.historical)
			}
			resolver.AddContinuation(c)
		}
	}
	if resolver != nil {
		if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
			return common.Hash{}, err
		}
		resolver = nil
	}
	for _, addrHash := range addrs {
		account := tds.accountUpdates[addrHash]
		// first argument to getStorageTrie is not used unless the last one == true
		storageTrie, err := tds.getStorageTrie(common.Address{}, addrHash, false)
		if err != nil {
			return common.Hash{}, err
		}
		deleteStorageTrie := false
		if account != nil {
			if _, ok := tds.deleted[addrHash]; ok {
				deleteStorageTrie = true
				account.Root = emptyRoot
			} else if storageTrie != nil && forward {
				account.Root = storageTrie.Hash()
			}
			//fmt.Printf("Set root %x %x\n", address[:], account.Root[:])
			data, err := rlp.EncodeToBytes(account)
			if err != nil {
				return common.Hash{}, err
			}
			tds.t.Update(addrHash[:], data, tds.blockNr)
		} else {
			deleteStorageTrie = true
			tds.t.Delete(addrHash[:], tds.blockNr)
		}
		if deleteStorageTrie && storageTrie != nil {
			delete(tds.storageTries, addrHash)
			storageTrie.PrepareToRemove()
		}
	}
	hash := tds.t.Hash()
	return hash, nil
}

func (tds *TrieDbState) clearUpdates() {
	tds.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	tds.accountUpdates = make(map[common.Hash]*Account)
	tds.deleted = make(map[common.Hash]struct{})
}

func (tds *TrieDbState) Rebuild() {
	tr := tds.AccountTrie()
	tr.Rebuild(tds.db, tds.blockNr)
}

func (tds *TrieDbState) SetBlockNr(blockNr uint64) {
	tds.blockNr = blockNr
}

func (tds *TrieDbState) UnwindTo(blockNr uint64) error {
	fmt.Printf("Rewinding from block %d to block %d\n", tds.blockNr, blockNr)
	var accountPutKeys [][]byte
	var accountPutVals [][]byte
	var accountDelKeys [][]byte
	var storagePutKeys [][]byte
	var storagePutVals [][]byte
	var storageDelKeys [][]byte
	if err := tds.db.RewindData(tds.blockNr, blockNr, func(bucket, key, value []byte) error {
		//var pre []byte
		if len(key) == 32 {
			//pre, _ = tds.db.Get(trie.SecureKeyPrefix, key)
		} else {
			//pre, _ = tds.db.Get(trie.SecureKeyPrefix, key[20:52])
		}
		//fmt.Printf("Rewind with key %x (%x) value %x\n", key, pre, value)
		var err error
		if bytes.Equal(bucket, AccountsHistoryBucket) {
			var addrHash common.Hash
			copy(addrHash[:], key)
			if len(value) > 0 {
				tds.accountUpdates[addrHash], err = encodingToAccount(value)
				if err != nil {
					return err
				}
				accountPutKeys = append(accountPutKeys, key)
				accountPutVals = append(accountPutVals, value)
			} else {
				//fmt.Printf("Deleted account\n")
				tds.accountUpdates[addrHash] = nil
				tds.deleted[addrHash] = struct{}{}
				accountDelKeys = append(accountDelKeys, key)
			}
		} else if bytes.Equal(bucket, StorageHistoryBucket) {
			var address common.Address
			copy(address[:], key[:20])
			var keyHash common.Hash
			copy(keyHash[:], key[20:])
			m, ok := tds.storageUpdates[address]
			if !ok {
				m = make(map[common.Hash][]byte)
				tds.storageUpdates[address] = m
			}
			m[keyHash] = value
			if len(value) > 0 {
				storagePutKeys = append(storagePutKeys, key)
				storagePutVals = append(storagePutVals, value)
			} else {
				//fmt.Printf("Deleted storage item\n")
				storageDelKeys = append(storageDelKeys, key)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := tds.trieRoot(false); err != nil {
		return err
	}
	for addrHash, account := range tds.accountUpdates {
		if account == nil {
			if err := tds.db.Delete(AccountsBucket, addrHash[:]); err != nil {
				return err
			}
		} else {
			value, err := accountToEncoding(account)
			if err != nil {
				return err
			}
			if err := tds.db.Put(AccountsBucket, addrHash[:], value); err != nil {
				return err
			}
		}
	}
	for address, m := range tds.storageUpdates {
		for keyHash, value := range m {
			if len(value) == 0 {
				if err := tds.db.Delete(StorageBucket, append(address[:], keyHash[:]...)); err != nil {
					return err
				}
			} else {
				if err := tds.db.Put(StorageBucket, append(address[:], keyHash[:]...), value); err != nil {
					return err
				}
			}
		}
	}
	for i := tds.blockNr; i > blockNr; i-- {
		if err := tds.db.DeleteTimestamp(i); err != nil {
			return err
		}
	}
	tds.clearUpdates()
	tds.blockNr = blockNr
	return nil
}

func accountToEncoding(account *Account) ([]byte, error) {
	var data []byte
	var err error
	if (account.CodeHash == nil || bytes.Equal(account.CodeHash, emptyCodeHash)) && (account.Root == emptyRoot || account.Root == common.Hash{}) {
		if (account.Balance == nil || account.Balance.Sign() == 0) && account.Nonce == 0 {
			data = []byte{byte(192)}
		} else {
			var extAccount ExtAccount
			extAccount.Nonce = account.Nonce
			extAccount.Balance = account.Balance
			if extAccount.Balance == nil {
				extAccount.Balance = new(big.Int)
			}
			data, err = rlp.EncodeToBytes(extAccount)
			if err != nil {
				return nil, err
			}
		}
	} else {
		a := *account
		if a.Balance == nil {
			a.Balance = new(big.Int)
		}
		if a.CodeHash == nil {
			a.CodeHash = emptyCodeHash
		}
		if a.Root == (common.Hash{}) {
			a.Root = emptyRoot
		}
		data, err = rlp.EncodeToBytes(a)
		if err != nil {
			return nil, err
		}
	}
	return data, err
}

func encodingToAccount(enc []byte) (*Account, error) {
	if enc == nil || len(enc) == 0 {
		return nil, nil
	}
	var data Account
	// Kind of hacky
	if len(enc) == 1 {
		data.Balance = new(big.Int)
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else if len(enc) < 60 {
		var extData ExtAccount
		if err := rlp.DecodeBytes(enc, &extData); err != nil {
			return nil, err
		}
		data.Nonce = extData.Nonce
		data.Balance = extData.Balance
		data.CodeHash = emptyCodeHash
		data.Root = emptyRoot
	} else {
		if err := rlp.DecodeBytes(enc, &data); err != nil {
			return nil, err
		}
	}
	return &data, nil
}

func (tds *TrieDbState) joinGeneration(gen uint64) {
	tds.nodeCount++
	tds.generationCounts[gen]++

}

func (tds *TrieDbState) leftGeneration(gen uint64) {
	tds.nodeCount--
	tds.generationCounts[gen]--
}

func (tds *TrieDbState) addProof(prefix, key []byte, pos int, mask uint32, hashes []common.Hash) {
	if tds.resolveReads {
		var createdProofs map[string]struct{}
		if prefix == nil {
			createdProofs = tds.createdProofs
		} else {
			var ok bool
			ps := string(prefix)
			createdProofs, ok = tds.sCreatedProofs[ps]
			if !ok {
				createdProofs = make(map[string]struct{})
			}
		}
		var proofShorts map[string][]byte
		if prefix == nil {
			proofShorts = tds.proofShorts
		} else {
			var ok bool
			proofShorts, ok = tds.sShorts[string(common.CopyBytes(prefix))]
			if !ok {
				proofShorts = make(map[string][]byte)
			}
		}
		if prefix == nil {
			//fmt.Printf("addProof %x %x\n", prefix, key[:pos])
		}
		k := make([]byte, pos)
		copy(k, key[:pos])
		for i := len(k); i >= 0; i-- {
			if i < len(k) {
				if short, ok := proofShorts[string(k[:i])]; ok && i+len(short) <= len(k) && bytes.Equal(short, k[i:i+len(short)]) {
					break
				}
			}
			if _, ok := createdProofs[string(k[:i])]; ok {
				return
			}
		}
		if prefix == nil {
			//fmt.Printf("addProof %x %x added\n", prefix, key[:pos])
		}
		var proofMasks map[string]uint32
		if prefix == nil {
			proofMasks = tds.proofMasks
		} else {
			var ok bool
			ps := string(prefix)
			proofMasks, ok = tds.sMasks[ps]
			if !ok {
				proofMasks = make(map[string]uint32)
				tds.sMasks[ps] = proofMasks
			}
		}
		var proofHashes map[string][16]common.Hash
		if prefix == nil {
			proofHashes = tds.proofHashes
		} else {
			var ok bool
			ps := string(prefix)
			proofHashes, ok = tds.sHashes[ps]
			if !ok {
				proofHashes = make(map[string][16]common.Hash)
				tds.sHashes[ps] = proofHashes
			}
		}
		ks := string(k)
		if m, ok := proofMasks[ks]; ok {
			intersection := m & mask
			//if mask != 0 {
			proofMasks[ks] = intersection
			//}
			h := proofHashes[ks]
			idx := 0
			for i := byte(0); i < 16; i++ {
				if intersection&(uint32(1)<<i) != 0 {
					h[i] = hashes[idx]
				} else {
					h[i] = common.Hash{}
				}
				if mask&(uint32(1)<<i) != 0 {
					idx++
				}
			}
			proofHashes[ks] = h
		} else {
			//if mask != 0 {
			proofMasks[ks] = mask
			//}
			var h [16]common.Hash
			idx := 0
			for i := byte(0); i < 16; i++ {
				if mask&(uint32(1)<<i) != 0 {
					h[i] = hashes[idx]
					idx++
				}
			}
			proofHashes[ks] = h
		}
	}
}

func (tds *TrieDbState) addSoleHash(prefix, key []byte, pos int, hash common.Hash) {
	if tds.resolveReads {
		var soleHashes map[string]common.Hash
		if prefix == nil {
			soleHashes = tds.soleHashes
		} else {
			var ok bool
			ps := string(prefix)
			soleHashes, ok = tds.sSoleHashes[ps]
			if !ok {
				soleHashes = make(map[string]common.Hash)
				tds.sSoleHashes[ps] = soleHashes
			}
		}
		if prefix == nil {
			//fmt.Printf("addSoleHash %x %x\n", prefix, key[:pos])
		}
		k := make([]byte, pos)
		copy(k, key[:pos])
		ks := string(k)
		if _, ok := soleHashes[ks]; !ok {
			soleHashes[ks] = hash
		}
	}
}

func (tds *TrieDbState) createProof(prefix, key []byte, pos int) {
	if tds.resolveReads {
		if prefix == nil {
			//fmt.Printf("createProof %x %x\n", prefix, key[:pos])
		}
		var createdProofs map[string]struct{}
		if prefix == nil {
			createdProofs = tds.createdProofs
		} else {
			var ok bool
			ps := string(common.CopyBytes(prefix))
			createdProofs, ok = tds.sCreatedProofs[ps]
			if !ok {
				createdProofs = make(map[string]struct{})
				tds.sCreatedProofs[ps] = createdProofs
			}
		}
		k := make([]byte, pos)
		copy(k, key[:pos])
		ks := string(k)
		if _, ok := createdProofs[ks]; !ok {
			createdProofs[ks] = struct{}{}
		}
	}
}

func (tds *TrieDbState) addValue(prefix, key []byte, pos int, value []byte) {
	if tds.resolveReads {
		var proofShorts map[string][]byte
		if prefix == nil {
			proofShorts = tds.proofShorts
		} else {
			var ok bool
			ps := string(common.CopyBytes(prefix))
			proofShorts, ok = tds.sShorts[ps]
			if !ok {
				proofShorts = make(map[string][]byte)
			}
		}
		// Find corresponding short
		found := false
		for i := 0; i < pos; i++ {
			if short, ok := proofShorts[string(key[:i])]; ok && bytes.Equal(short, key[i:pos]) {
				found = true
				break
			}
		}
		if !found {
			return
		}
		var proofValues map[string][]byte
		if prefix == nil {
			proofValues = tds.proofValues
		} else {
			var ok bool
			ps := string(common.CopyBytes(prefix))
			proofValues, ok = tds.sValues[ps]
			if !ok {
				proofValues = make(map[string][]byte)
				tds.sValues[ps] = proofValues
			}
		}
		k := make([]byte, pos)
		copy(k, key[:pos])
		ks := string(k)
		if _, ok := proofValues[ks]; !ok {
			proofValues[ks] = common.CopyBytes(value)
		}
	}
}

func (tds *TrieDbState) createShort(prefix, key []byte, pos int) {
	if tds.resolveReads {
		if prefix == nil {
			//fmt.Printf("createShort %x %x\n", prefix, key[:pos])
		}
		var createdShorts map[string]struct{}
		if prefix == nil {
			createdShorts = tds.createdShorts
		} else {
			var ok bool
			ps := string(common.CopyBytes(prefix))
			createdShorts, ok = tds.sCreatedShorts[ps]
			if !ok {
				createdShorts = make(map[string]struct{})
				tds.sCreatedShorts[ps] = createdShorts
			}
		}
		k := make([]byte, pos)
		copy(k, key[:pos])
		ks := string(k)
		if _, ok := createdShorts[ks]; !ok {
			createdShorts[ks] = struct{}{}
		}
	}
}

func (tds *TrieDbState) addShort(prefix, key []byte, pos int, short []byte) bool {
	if tds.resolveReads {
		var createdShorts map[string]struct{}
		if prefix == nil {
			createdShorts = tds.createdShorts
		} else {
			var ok bool
			ps := string(common.CopyBytes(prefix))
			createdShorts, ok = tds.sCreatedShorts[ps]
			if !ok {
				createdShorts = make(map[string]struct{})
				tds.sCreatedShorts[ps] = createdShorts
			}
		}
		var proofShorts map[string][]byte
		if prefix == nil {
			proofShorts = tds.proofShorts
		} else {
			var ok bool
			ps := string(common.CopyBytes(prefix))
			proofShorts, ok = tds.sShorts[ps]
			if !ok {
				proofShorts = make(map[string][]byte)
				tds.sShorts[ps] = proofShorts
			}
		}
		k := make([]byte, pos)
		copy(k, key[:pos])
		ks := string(k)
		if _, ok := createdShorts[ks]; ok {
			return false
		}
		if prefix == nil {
			//fmt.Printf("addShort %x %x\n", prefix, key[:pos])
		}
		if _, ok := proofShorts[ks]; !ok {
			proofShorts[ks] = common.CopyBytes(short)
			return true
		}
	}
	return false
}

func (tds *TrieDbState) ReadAccountData(address common.Address) (*Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	enc, err := tds.t.TryGet(tds.db, buf[:], tds.blockNr)
	if err != nil {
		return nil, err
	}
	return encodingToAccount(enc)
}

func (tds *TrieDbState) savePreimage(save bool, hash, preimage []byte) error {
	if !save {
		return nil
	}
	return tds.db.Put(trie.SecureKeyPrefix, hash, preimage)
}

func (tds *TrieDbState) HashAddress(address *common.Address, save bool) (common.Hash, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	return buf, tds.savePreimage(save, buf[:], address[:])
}

func (tds *TrieDbState) HashKey(key *common.Hash, save bool) (common.Hash, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(key[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	return buf, tds.savePreimage(save, buf[:], key[:])
}

func (tds *TrieDbState) GetKey(shaKey []byte) []byte {
	key, _ := tds.db.Get(trie.SecureKeyPrefix, shaKey)
	return key
}

func (tds *TrieDbState) getStorageTrie(address common.Address, addrHash common.Hash, create bool) (*trie.Trie, error) {
	t, ok := tds.storageTries[addrHash]
	if !ok && create {
		account, err := tds.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if account == nil {
			//fmt.Printf("Creating storage trie for address %x with empty storage root\n", address)
			t = trie.New(common.Hash{}, StorageBucket, address[:], true)
		} else {
			//fmt.Printf("Creating storage trie for address %x with storage root %x\n", address, account.Root)
			t = trie.New(account.Root, StorageBucket, address[:], true)
		}
		t.SetHistorical(tds.historical)
		t.SetResolveReads(tds.resolveReads)
		t.MakeListed(tds.joinGeneration, tds.leftGeneration)
		t.ProofFunctions(tds.addProof, tds.addSoleHash, tds.createProof, tds.addValue, tds.addShort, tds.createShort)
		tds.storageTries[addrHash] = t
	}
	return t, nil
}

func (tds *TrieDbState) ReadAccountStorage(address common.Address, key *common.Hash) ([]byte, error) {
	addrHash, err := tds.HashAddress(&address, false /*save*/)
	if err != nil {
		return nil, err
	}
	t, err := tds.getStorageTrie(address, addrHash, true)
	if err != nil {
		return nil, err
	}
	seckey, err := tds.HashKey(key, false /*save*/)
	if err != nil {
		return nil, err
	}
	enc, err := t.TryGet(tds.db, seckey[:], tds.blockNr)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func (tds *TrieDbState) ReadAccountCode(codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}
	if cached, ok := tds.codeCache.Get(codeHash); ok {
		code, err = cached.([]byte), nil
	} else {
		code, err = tds.db.Get(CodeBucket, codeHash[:])
		if err == nil {
			tds.codeSizeCache.Add(codeHash, len(code))
			tds.codeCache.Add(codeHash, code)
		}
	}
	if tds.resolveReads {
		if _, ok := tds.createdCodes[codeHash]; !ok {
			tds.proofCodes[codeHash] = code
		}
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(codeHash common.Hash) (codeSize int, err error) {
	var code []byte
	if cached, ok := tds.codeSizeCache.Get(codeHash); ok {
		codeSize, err = cached.(int), nil
		if tds.resolveReads {
			if cachedCode, ok := tds.codeCache.Get(codeHash); ok {
				code, err = cachedCode.([]byte), nil
			} else {
				code, err = tds.ReadAccountCode(codeHash)
				if err != nil {
					return 0, err
				}
			}
		}
	} else {
		code, err = tds.ReadAccountCode(codeHash)
		if err != nil {
			return 0, err
		}
		codeSize = len(code)
	}
	if tds.resolveReads {
		if _, ok := tds.createdCodes[codeHash]; !ok {
			tds.proofCodes[codeHash] = code
		}
	}
	return codeSize, nil
}

var prevMemStats runtime.MemStats

func (tds *TrieDbState) PruneTries(print bool) {
	if tds.nodeCount > int(MaxTrieCacheGen) {
		toRemove := 0
		excess := tds.nodeCount - int(MaxTrieCacheGen)
		gen := tds.oldestGeneration
		for excess > 0 {
			excess -= tds.generationCounts[gen]
			toRemove += tds.generationCounts[gen]
			delete(tds.generationCounts, gen)
			gen++
		}
		// Unload all nodes with touch timestamp < gen
		for addrHash, storageTrie := range tds.storageTries {
			empty := storageTrie.UnloadOlderThan(gen, false)
			if empty {
				delete(tds.storageTries, addrHash)
			}
		}
		tds.t.UnloadOlderThan(gen, false)
		tds.oldestGeneration = gen
		tds.nodeCount -= toRemove
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Info("Memory", "nodes", tds.nodeCount, "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
		if print {
			fmt.Printf("Pruning done. Nodes: %d, alloc: %d, sys: %d, numGC: %d\n", tds.nodeCount, int(m.Alloc/1024), int(m.Sys/1024), int(m.NumGC))
		}
	}
}

type TrieStateWriter struct {
	tds *TrieDbState
}

type DbStateWriter struct {
	tds *TrieDbState
}

func (tds *TrieDbState) TrieStateWriter() *TrieStateWriter {
	return &TrieStateWriter{tds: tds}
}

func (tds *TrieDbState) DbStateWriter() *DbStateWriter {
	return &DbStateWriter{tds: tds}
}

var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

func accountsEqual(a1, a2 *Account) bool {
	if a1.Nonce != a2.Nonce {
		return false
	}
	if a1.Balance == nil {
		if a2.Balance != nil {
			return false
		}
	} else if a2.Balance == nil {
		return false
	} else if a1.Balance.Cmp(a2.Balance) != 0 {
		return false
	}
	if a1.Root != a2.Root {
		return false
	}
	if a1.CodeHash == nil {
		if a2.CodeHash != nil {
			return false
		}
	} else if a2.CodeHash == nil {
		return false
	} else if !bytes.Equal(a1.CodeHash, a2.CodeHash) {
		return false
	}
	return true
}

func (tsw *TrieStateWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	addrHash, err := tsw.tds.HashAddress(&address, false /*save*/)
	if err != nil {
		return err
	}
	tsw.tds.accountUpdates[addrHash] = account
	return nil
}

func (dsw *DbStateWriter) UpdateAccountData(address common.Address, original, account *Account) error {
	data, err := accountToEncoding(account)
	if err != nil {
		return err
	}
	addrHash, err := dsw.tds.HashAddress(&address, true /*save*/)
	if err != nil {
		return err
	}
	if err = dsw.tds.db.Put(AccountsBucket, addrHash[:], data); err != nil {
		return err
	}
	if dsw.tds.noHistory {
		return nil
	}
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
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
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) DeleteAccount(address common.Address, original *Account) error {
	addrHash, err := tsw.tds.HashAddress(&address, false /*save*/)
	if err != err {
		return err
	}
	tsw.tds.accountUpdates[addrHash] = nil
	tsw.tds.deleted[addrHash] = struct{}{}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(address common.Address, original *Account) error {
	addrHash, err := dsw.tds.HashAddress(&address, true /*save*/)
	if err != nil {
		return err
	}
	if err := dsw.tds.db.Delete(AccountsBucket, addrHash[:]); err != nil {
		return err
	}
	if dsw.tds.noHistory {
		return nil
	}
	var originalData []byte
	if original.Balance == nil {
		// Account has been created and deleted in the same block
		originalData = []byte{}
	} else {
		originalData, err = accountToEncoding(original)
		if err != nil {
			return err
		}
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if tsw.tds.resolveReads {
		if _, ok := tsw.tds.createdCodes[codeHash]; !ok {
			tsw.tds.createdCodes[codeHash] = struct{}{}
		}
	}
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if dsw.tds.resolveReads {
		if _, ok := dsw.tds.createdCodes[codeHash]; !ok {
			dsw.tds.createdCodes[codeHash] = struct{}{}
		}
	}
	return dsw.tds.db.Put(CodeBucket, codeHash[:], code)
}

func (tsw *TrieStateWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := tsw.tds.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.storageUpdates[address] = m
	}
	seckey, err := tsw.tds.HashKey(key, false /*save*/)
	if err != nil {
		return err
	}
	if len(v) > 0 {
		m[seckey] = common.CopyBytes(v)
	} else {
		m[seckey] = nil
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	//fmt.Printf("WriteAccountStorage address %x, key %x, original %x, value %x\n", address, *key, *original, *value)
	if *original == *value {
		return nil
	}
	seckey, err := dsw.tds.HashKey(key, true /*save*/)
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)
	compositeKey := append(address[:], seckey[:]...)
	if len(v) == 0 {
		err = dsw.tds.db.Delete(StorageBucket, compositeKey)
	} else {
		err = dsw.tds.db.Put(StorageBucket, compositeKey, vv)
	}
	if err != nil {
		return err
	}
	if dsw.tds.noHistory {
		return nil
	}
	o := bytes.TrimLeft(original[:], "\x00")
	oo := make([]byte, len(o))
	copy(oo, o)
	return dsw.tds.db.PutS(StorageHistoryBucket, compositeKey, oo, dsw.tds.blockNr)
}
