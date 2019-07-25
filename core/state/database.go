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
	"context"
	"fmt"
	"hash"
	"io"
	"math/big"
	"runtime"
	"sort"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
	"golang.org/x/crypto/sha3"
)

// Trie cache generation limit after which to evict trie nodes from memory.
var MaxTrieCacheGen = uint32(1024 * 1024)

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
	ReadAccountData(address common.Address) (*accounts.Account, error)
	ReadAccountStorage(address common.Address, version uint8,  key *common.Hash) ([]byte, error)
	ReadAccountCode(codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(codeHash common.Hash) (int, error)
}

type StateWriter interface {
	UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error
	UpdateAccountCode(codeHash common.Hash, code []byte) error
	DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error
	WriteAccountStorage(address common.Address, version uint8,  key, original, value *common.Hash) error
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

func (nw *NoopWriter) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	return nil
}

func (nw *NoopWriter) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	return nil
}

func (nw *NoopWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	return nil
}

func (nw *NoopWriter) WriteAccountStorage(address common.Address, key, original, value *common.Hash) error {
	return nil
}

// Structure holding updates, deletes, and reads registered within one change period
// A change period can be transaction within a block, or a block within group of blocks
type Buffer struct {
	storageUpdates map[common.Address]map[common.Hash][]byte
	storageReads   map[common.Address]map[common.Hash]struct{}
	accountUpdates map[common.Hash]*accounts.Account
	accountReads   map[common.Hash]struct{}
	deleted        map[common.Address]struct{}
}

// Prepares buffer for work or clears previous data
func (b *Buffer) initialise() {
	b.storageUpdates = make(map[common.Address]map[common.Hash][]byte)
	b.storageReads = make(map[common.Address]map[common.Hash]struct{})
	b.accountUpdates = make(map[common.Hash]*accounts.Account)
	b.accountReads = make(map[common.Hash]struct{})
	b.deleted = make(map[common.Address]struct{})
}

// Replaces account pointer with pointers to the copies
func (b *Buffer) detachAccounts() {
	for addrHash, account := range b.accountUpdates {
		if account != nil {
			b.accountUpdates[addrHash] = &accounts.Account{
				Nonce:       account.Nonce,
				Balance:     new(big.Int).Set(account.Balance),
				Root:        account.Root,
				CodeHash:    account.CodeHash,
				StorageSize: account.StorageSize,
			}
		}
	}
}

// Merges the content of another buffer into this one
func (b *Buffer) merge(other *Buffer) {
	for address, om := range other.storageUpdates {
		m, ok := b.storageUpdates[address]
		if !ok {
			m = make(map[common.Hash][]byte)
			b.storageUpdates[address] = m
		}
		for keyHash, v := range om {
			m[keyHash] = v
		}
	}
	for address, om := range other.storageReads {
		m, ok := b.storageReads[address]
		if !ok {
			m = make(map[common.Hash]struct{})
			b.storageReads[address] = m
		}
		for keyHash := range om {
			m[keyHash] = struct{}{}
		}
	}
	for addrHash, account := range other.accountUpdates {
		b.accountUpdates[addrHash] = account
	}
	for addrHash := range other.accountReads {
		b.accountReads[addrHash] = struct{}{}
	}
	for address := range other.deleted {
		b.deleted[address] = struct{}{}
	}
}

// TrieDbState implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t               *trie.Trie
	db              ethdb.Database
	blockNr         uint64
	storageTrie    *trie.Trie
	buffers         []*Buffer
	aggregateBuffer *Buffer // Merge of all buffers
	currentBuffer   *Buffer
	codeCache       *lru.Cache
	codeSizeCache   *lru.Cache
	historical      bool
	noHistory       bool
	resolveReads    bool
	pg              *trie.ProofGenerator
	tp              *trie.TriePruning
	ctx             context.Context
}

func NewTrieDbState(ctx context.Context, root common.Hash, db ethdb.Database, blockNr uint64) (*TrieDbState, error) {
	csc, err := lru.New(100000)
	if err != nil {
		return nil, err
	}
	cc, err := lru.New(10000)
	if err != nil {
		return nil, err
	}
	t := trie.New(root)
	tp := trie.NewTriePruning(blockNr)

	tds := TrieDbState{
		t:             t,
		db:            db,
		blockNr:       blockNr,
		storageTrie:   trie.New(common.Hash{}),
		codeCache:     cc,
		codeSizeCache: csc,
		pg:            trie.NewProofGenerator(),
		tp:            tp,
		ctx:           ctx,
	}
	t.SetTouchFunc(func(hex []byte, del bool) {
		tp.Touch(hex, del)
	})
	return &tds, nil
}

func (tds *TrieDbState) SetHistorical(h bool) {
	tds.historical = h
}

func (tds *TrieDbState) SetResolveReads(rr bool) {
	tds.resolveReads = rr
}

func (tds *TrieDbState) SetNoHistory(nh bool) {
	tds.noHistory = nh
}

func (tds *TrieDbState) Copy() *TrieDbState {
	tcopy := *tds.t
	tStorageCopy := *tds.storageTrie

	tp := trie.NewTriePruning(tds.blockNr)

	cpy := TrieDbState{
		t:            &tcopy,
		db:           tds.db,
		blockNr:      tds.blockNr,
		storageTrie:  &tStorageCopy,
		tp:           tp,
	}
	return &cpy
}

func (tds *TrieDbState) Database() ethdb.Database {
	return tds.db
}

func (tds *TrieDbState) AccountTrie() *trie.Trie {
	return tds.t
}

func (tds *TrieDbState) StartNewBuffer() {
	if tds.currentBuffer != nil {
		if tds.aggregateBuffer == nil {
			tds.aggregateBuffer = &Buffer{}
			tds.aggregateBuffer.initialise()
		}
		tds.aggregateBuffer.merge(tds.currentBuffer)
		tds.currentBuffer.detachAccounts()
	}
	tds.currentBuffer = &Buffer{}
	tds.currentBuffer.initialise()
	tds.buffers = append(tds.buffers, tds.currentBuffer)
}

func (tds *TrieDbState) LastRoot() common.Hash {
	return tds.t.Hash()
}

// DESCRIBED: docs/programmers_guide/guide.md#organising-ethereum-state-into-a-merkle-tree
func (tds *TrieDbState) ComputeTrieRoots(ctx context.Context) ([]common.Hash, error) {
	roots, err := tds.computeTrieRoots(ctx, true)
	tds.clearUpdates()
	return roots, err
}

func (tds *TrieDbState) PrintTrie(w io.Writer) {
	tds.t.Print(w)
	fmt.Fprintln(w,"") //nolint
	tds.storageTrie.Print(w)
}

func (tds *TrieDbState) PrintStorageTrie(w io.Writer, address common.Address) {
	tds.storageTrie.Print(w)
}

// WalkRangeOfAccounts calls the walker for each account whose key starts with a given prefix,
// for no more than maxItems.
// Returns whether all matching accounts were traversed (provided there was no error).
func (tds *TrieDbState) WalkRangeOfAccounts(prefix trie.Keybytes, maxItems int, walker func(common.Hash, *accounts.Account)) (bool, error) {
	startkey := make([]byte, common.HashLength)
	copy(startkey, prefix.Data)

	fixedbits := uint(len(prefix.Data)) * 8
	if prefix.Odd {
		fixedbits -= 4
	}

	i := 0

	err := tds.db.WalkAsOf(AccountsBucket, AccountsHistoryBucket, startkey, fixedbits, tds.blockNr+1,
		func(key []byte, value []byte) (bool, error) {
			acc, err := accounts.Decode(value)
			if err != nil {
				return false, err
			}
			if acc != nil {
				if i < maxItems {
					walker(common.BytesToHash(key), acc)
				}
				i++
			}
			return i <= maxItems, nil
		},
	)

	return i <= maxItems, err
}

// WalkStorageRange calls the walker for each storage item whose key starts with a given prefix,
// for no more than maxItems.
// Returns whether all matching storage items were traversed (provided there was no error).
func (tds *TrieDbState) WalkStorageRange(address common.Address, prefix trie.Keybytes, maxItems int, walker func(common.Hash, big.Int)) (bool, error) {
	startkey := make([]byte, common.AddressLength+common.HashLength)
	copy(startkey, address[:])
	copy(startkey[common.AddressLength:], prefix.Data)

	fixedbits := (common.AddressLength + uint(len(prefix.Data))) * 8
	if prefix.Odd {
		fixedbits -= 4
	}

	i := 0

	err := tds.db.WalkAsOf(StorageBucket, StorageHistoryBucket, startkey, fixedbits, tds.blockNr+1,
		func(key []byte, value []byte) (bool, error) {
			var val big.Int
			if err := rlp.DecodeBytes(value, &val); err != nil {
				return false, err
			}

			if i < maxItems {
				walker(common.BytesToHash(key), val)
			}
			i++
			return i <= maxItems, nil
		},
	)

	return i <= maxItems, err
}

// Hashes are a slice of hashes.
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

// Builds a map where for each address (of a smart contract) there is
// a sorted list of all key hashes that were touched within the
// period for which we are aggregating updates
func (tds *TrieDbState) buildStorageTouches() map[common.Address]Hashes {
	storageTouches := make(map[common.Address]Hashes)
	for address, m := range tds.aggregateBuffer.storageUpdates {
		var hashes Hashes
		mRead := tds.aggregateBuffer.storageReads[address]
		i := 0
		hashes = make(Hashes, len(m)+len(mRead))
		for keyHash := range m {
			hashes[i] = keyHash
			i++
		}
		for keyHash := range mRead {
			if _, ok := m[keyHash]; !ok {
				hashes[i] = keyHash
				i++
			}
		}
		if len(hashes) > 0 {
			sort.Sort(hashes)
			storageTouches[address] = hashes
		}
	}
	for address, m := range tds.aggregateBuffer.storageReads {
		if _, ok := tds.aggregateBuffer.storageUpdates[address]; ok {
			continue
		}
		hashes := make(Hashes, len(m))
		i := 0
		for keyHash := range m {
			hashes[i] = keyHash
			i++
		}
		sort.Sort(hashes)
		storageTouches[address] = hashes
	}
	return storageTouches
}

// Expands the storage tries (by loading data from the database) if it is required
// for accessing storage slots containing in the storageTouches map
func (tds *TrieDbState) resolveStorageTouches(storageTouches map[common.Address]Hashes) error {
	var resolver *trie.TrieResolver
	for address, hashes := range storageTouches {
		var contract = address // To avoid the value being overwritten, though still shared between continuations
		for _, keyHash := range hashes {
			//todo @need resolution for prefix
			if need, req := tds.storageTrie.NeedResolution(contract[:], keyHash[:]); need {
				if resolver == nil {
					resolver = trie.NewResolver(tds.ctx, 0, false, tds.blockNr)
					resolver.SetHistorical(tds.historical)
				}
				resolver.AddRequest(req)
			}
		}
	}
	if resolver != nil {
		if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
			return err
		}
	}
	return nil
}

// Populate pending block proof so that it will be sufficient for accessing all storage slots in storageTouches
func (tds *TrieDbState) populateStorageBlockProof(storageTouches map[common.Address]Hashes) error {
	for address, hashes := range storageTouches {
		if _, ok := tds.aggregateBuffer.deleted[address]; ok && len(tds.aggregateBuffer.storageReads[address]) == 0 {
			// We can only skip the proof of storage entirely if
			// there were no reads before writes and account got deleted
			continue
		}

		_=hashes
		//@todo(b00ris) PopulateBlockProofData for data with prefix
		//storageTrie, err := tds.getStorageTrie(address, true)
		//if err != nil {
		//	return err
		//}
		//var contract = address
		//for _, keyHash := range hashes {
		//	storageTrie.PopulateBlockProofData(contract[:], keyHash[:], tds.pg)
		//}
	}
	return nil
}

// Builds a sorted list of all address hashes that were touched within the
// period for which we are aggregating updates
func (tds *TrieDbState) buildAccountTouches() Hashes {
	accountTouches := make(Hashes, len(tds.aggregateBuffer.accountUpdates)+len(tds.aggregateBuffer.accountReads))
	i := 0
	for addrHash := range tds.aggregateBuffer.accountUpdates {
		accountTouches[i] = addrHash
		i++
	}
	for addrHash := range tds.aggregateBuffer.accountReads {
		if _, ok := tds.aggregateBuffer.accountUpdates[addrHash]; !ok {
			accountTouches[i] = addrHash
			i++
		}
	}
	sort.Sort(accountTouches)
	return accountTouches
}

// Expands the accounts trie (by loading data from the database) if it is required
// for accessing accounts whose addresses are contained in the accountTouches
func (tds *TrieDbState) resolveAccountTouches(accountTouches Hashes) error {
	var resolver *trie.TrieResolver
	for _, addrHash := range accountTouches {
		if need, req := tds.t.NeedResolution(nil, addrHash[:]); need {
			if resolver == nil {
				resolver = trie.NewResolver(tds.ctx, 0, true, tds.blockNr)
				resolver.SetHistorical(tds.historical)
			}
			resolver.AddRequest(req)
		}
	}
	if resolver != nil {
		if err := resolver.ResolveWithDb(tds.db, tds.blockNr); err != nil {
			return err
		}
		resolver = nil
	}
	return nil
}

func (tds *TrieDbState) populateAccountBlockProof(accountTouches Hashes) {
	for _, addrHash := range accountTouches {
		tds.t.PopulateBlockProofData(nil, addrHash[:], tds.pg)
	}
}

// forward is `true` if the function is used to progress the state forward (by adding blocks)
// forward is `false` if the function is used to rewind the state (for reorgs, for example)
func (tds *TrieDbState) computeTrieRoots(ctx context.Context, forward bool) ([]common.Hash, error) {
	// Aggregating the current buffer, if any
	if tds.currentBuffer != nil {
		if tds.aggregateBuffer == nil {
			tds.aggregateBuffer = &Buffer{}
			tds.aggregateBuffer.initialise()
		}
		tds.aggregateBuffer.merge(tds.currentBuffer)
	}
	if tds.aggregateBuffer == nil {
		return nil, nil
	}
	accountUpdates := tds.aggregateBuffer.accountUpdates

	// Prepare (resolve) storage tries so that actual modifications can proceed without database access
	storageTouches := tds.buildStorageTouches()

	if err := tds.resolveStorageTouches(storageTouches); err != nil {
		return nil, err
	}
	if tds.resolveReads {
		if err := tds.populateStorageBlockProof(storageTouches); err != nil {
			return nil, err
		}
	}

	// Prepare (resolve) accounts trie so that actual modifications can proceed without database access
	accountTouches := tds.buildAccountTouches()
	if err := tds.resolveAccountTouches(accountTouches); err != nil {
		return nil, err
	}
	if tds.resolveReads {
		tds.populateAccountBlockProof(accountTouches)
	}

	// Perform actual updates on the tries, and compute one trie root per buffer
	// These roots can be used to populate receipt.PostState on pre-Byzantium
	roots := make([]common.Hash, len(tds.buffers))
	for i, b := range tds.buffers {
		for address, m := range b.storageUpdates {
			if _, ok := b.deleted[address]; ok {
				// Deleted contracts will be dealth with later, in the next loop
				continue
			}

			addrHash, err := tds.HashAddress(address, false /*save*/)
			if err != nil {
				return nil, err
			}

			acc,err:=tds.ReadAccountData(address)
			if err != nil {
				return nil, err
			}

			for keyHash, v := range m {
				version:=uint8(0)
				if acc!=nil {
					version=acc.GetIncarnation()
				}
				addrHash,err:=tds.HashAddress(address, false)
				if err != nil {
					return nil, err
				}

				cKey:=GenerateCompositeStorageKey(addrHash, version, keyHash)
				if len(v) > 0 {
					tds.storageTrie.Update(cKey, v, tds.blockNr)
				} else {
					tds.storageTrie.Delete(cKey, tds.blockNr)
				}
			}
			if forward {
				if account, ok := b.accountUpdates[addrHash]; ok && account != nil {
					addrHash,err:=tds.HashAddress(address, false)
					if err != nil {
						return nil, err
					}

					ok,account.Root = tds.storageTrie.DeepHash(GenerateStoragePrefix(addrHash, account.GetIncarnation()))
					if ok==false {
						fmt.Println("---------------------------------------")
						fmt.Println("core/state/database.go:596 tds.storageTrie.DeepHash(GenerateStoragePrefix(address, account.GetIncarnation())) !=ok", ok,account.Root)
						fmt.Println("---------------------------------------")
					}
				}
				if account, ok := accountUpdates[addrHash]; ok && account != nil {
					addrHash,err:=tds.HashAddress(address, false)
					if err != nil {
						return nil, err
					}

					ok,account.Root = tds.storageTrie.DeepHash(GenerateStoragePrefix(addrHash, account.GetIncarnation()))
					if ok==false {
						fmt.Println("---------------------------------------")
						fmt.Println("core/state/database.go:604 tds.storageTrie.DeepHash(GenerateStoragePrefix(address, account.GetIncarnation())) !=ok", ok,account.Root)
						fmt.Println("---------------------------------------")
					}

				}
			} else {
				// Simply comparing the correctness of the storageRoot computations
				if account, ok := b.accountUpdates[addrHash]; ok && account != nil {
					addrHash,err:=tds.HashAddress(address, false)
					if err != nil {
						return nil, err
					}

					ok,hash := tds.storageTrie.DeepHash(GenerateStoragePrefix(addrHash, account.GetIncarnation()))
					if ok==false {
						fmt.Println("---------------------------------------")
						fmt.Println("core/state/database.go:615 tds.storageTrie.DeepHash(GenerateStoragePrefix(address, account.GetIncarnation())) !=ok", ok,account.Root)
						fmt.Println("---------------------------------------")
					}

					if account.Root != hash {
						return nil, fmt.Errorf("mismatched storage root for %x: expected %x, got %x", address, account.Root, hash)
					}
				}
				if account, ok := accountUpdates[addrHash]; ok && account != nil {
					addrHash,err:=tds.HashAddress(address, false)
					if err != nil {
						return nil, err
					}

					ok,hash := tds.storageTrie.DeepHash(GenerateStoragePrefix(addrHash, account.GetIncarnation()))
					if ok==false {
						fmt.Println("---------------------------------------")
						fmt.Println("core/state/database.go:627 tds.storageTrie.DeepHash(GenerateStoragePrefix(address, account.GetIncarnation())) !=ok", ok,account.Root)
						fmt.Println("---------------------------------------")
					}

					if account.Root != hash {
						return nil, fmt.Errorf("mismatched storage root for %x: expected %x, got %x", address, account.Root, hash)
					}
				}
			}
		}

		// For the contracts that got deleted
		for address := range b.deleted {
			addrHash, err := tds.HashAddress(address, false /*save*/)
			if err != nil {
				return nil, err
			}
			if account, ok := b.accountUpdates[addrHash]; ok && account != nil {
				account.Root = trie.EmptyRoot
			}
			if account, ok := accountUpdates[addrHash]; ok && account != nil {
				account.Root = trie.EmptyRoot
			}
			tds.storageTrie.Delete(GenerateStoragePrefix(addrHash,0), tds.blockNr)
		}

		for addrHash, account := range b.accountUpdates {
			if account != nil {
				data, err := account.EncodeRLP(ctx)
				if err != nil {
					return nil, err
				}

				tds.t.Update(addrHash[:], data, tds.blockNr)
			} else {
				tds.t.Delete(addrHash[:], tds.blockNr)
			}
		}
		roots[i] = tds.t.Hash()
	}
	return roots, nil
}

func (tds *TrieDbState) clearUpdates() {
	tds.buffers = nil
	tds.currentBuffer = nil
	tds.aggregateBuffer = nil
}

func (tds *TrieDbState) Rebuild() error {
	if err := tds.AccountTrie().Rebuild(tds.ctx, tds.db, tds.blockNr); err != nil {
		return err
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory after rebuild", "nodes", tds.tp.NodeCount(), "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
	return nil
}

func (tds *TrieDbState) SetBlockNr(ctx context.Context, blockNr uint64) {
	tds.blockNr = blockNr
	tds.tp.SetBlockNr(blockNr)
	tds.ctx = ctx
}

func (tds *TrieDbState) UnwindTo(ctx context.Context, blockNr uint64) error {
	fmt.Printf("Rewinding from block %d to block %d\n", tds.blockNr, blockNr)
	var accountPutKeys [][]byte
	var accountPutVals [][]byte
	var accountDelKeys [][]byte
	var storagePutKeys [][]byte
	var storagePutVals [][]byte
	var storageDelKeys [][]byte
	tds.StartNewBuffer()
	b := tds.currentBuffer
	if err := tds.db.RewindData(tds.blockNr, blockNr, func(bucket, key, value []byte) error {
		if bytes.Equal(bucket, AccountsHistoryBucket) {
			var addrHash common.Hash
			copy(addrHash[:], key)
			if len(value) > 0 {
				acc, err := accounts.Decode(value)
				if err != nil {
					return err
				}
				b.accountUpdates[addrHash] = acc
				accountPutKeys = append(accountPutKeys, key)
				accountPutVals = append(accountPutVals, value)
			} else {
				b.accountUpdates[addrHash] = nil
				accountDelKeys = append(accountDelKeys, key)
			}
		} else if bytes.Equal(bucket, StorageHistoryBucket) {
			var address common.Address
			copy(address[:], key[:20])
			var keyHash common.Hash
			copy(keyHash[:], key[20:])
			m, ok := b.storageUpdates[address]
			if !ok {
				m = make(map[common.Hash][]byte)
				b.storageUpdates[address] = m
			}
			if len(value) > 0 {
				// Write into 1 extra RLP level
				var vv []byte
				if len(value) > 1 || value[0] >= 128 {
					vv = make([]byte, len(value)+1)
					vv[0] = byte(128 + len(value))
					copy(vv[1:], value)
				} else {
					vv = make([]byte, 1)
					vv[0] = value[0]
				}
				m[keyHash] = vv
				storagePutKeys = append(storagePutKeys, key)
				storagePutVals = append(storagePutVals, vv)
			} else {
				//fmt.Printf("Deleted storage item\n")
				storageDelKeys = append(storageDelKeys, key)
				m[keyHash] = nil
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := tds.computeTrieRoots(ctx, false); err != nil {
		return err
	}
	for addrHash, account := range tds.aggregateBuffer.accountUpdates {
		if account == nil {
			if err := tds.db.Delete(AccountsBucket, addrHash[:]); err != nil {
				return err
			}
		} else {
			//todo is aggregateBuffer collect data from one block?
			value, err := account.Encode(ctx)
			if err != nil {
				return err
			}
			if err := tds.db.Put(AccountsBucket, addrHash[:], value); err != nil {
				return err
			}
		}
	}
	for address, m := range tds.aggregateBuffer.storageUpdates {
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

func (tds *TrieDbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	h := newHasher()
	defer returnHasherToPool(h)
	h.sha.Reset()
	h.sha.Write(address[:])
	var buf common.Hash
	h.sha.Read(buf[:])
	if tds.resolveReads {
		if _, ok := tds.currentBuffer.accountUpdates[buf]; !ok {
			tds.currentBuffer.accountReads[buf] = struct{}{}
		}
	}

	enc, ok := tds.t.Get(buf[:], tds.blockNr)
	fmt.Println("core/state/database.go:759 tds.t.Get", ok)
	if !ok {
		// Not present in the trie, try the database
		var err error
		if tds.historical {
			enc, err = tds.db.GetAsOf(AccountsBucket, AccountsHistoryBucket, buf[:], tds.blockNr+1)
			if err != nil {
				enc = nil
			}
		} else {
			fmt.Println("core/state/database.go:768 ReadAccountData read from db", address.String())
			enc, err = tds.db.Get(AccountsBucket, buf[:])
			if err != nil {
				enc = nil
			}
		}
	}
	if ok {
		a, err := tds.db.Get(AccountsBucket, buf[:])
		if err != nil {
			enc = a
		}
	}
	return accounts.Decode(enc)
}

func (tds *TrieDbState) savePreimage(save bool, hash, preimage []byte) error {
	if !save {
		return nil
	}
	return tds.db.Put(trie.SecureKeyPrefix, hash, preimage)
}

func (tds *TrieDbState) HashAddress(address common.Address, save bool) (common.Hash, error) {
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

//func (tds *TrieDbState) getStorageTrie(address common.Address, create bool) (*trie.Trie, error) {
//	fmt.Println("core/state/database.go:828 getStorageTrie", address.String())
//	fmt.Println(caller(7))
//
//	t, ok := tds.storageTries[address]
//	if ok {
//		fmt.Println("core/state/database.go:813 ok, t.version=", t.Version)
//		account, err := tds.ReadAccountData(address)
//		fmt.Println("core/state/database.go:815 tds.ReadAccountData(address) err=", err, address.String())
//		if err==nil && account!=nil {
//			fmt.Println("core/state/database.go:817 account found", account)
//			fmt.Println("core/state/database.go:817 versCheck", address.String(), "acc.version", account.GetIncarnation(), "trie version", t.Version)
//			if t.Version!=account.GetIncarnation() {
//				t = trie.New(common.Hash{})
//				t.Version=account.GetIncarnation()
//				tds.storageTries[address] = t
//				return t, nil
//			}
//		}
//		return t, nil
//	}
//	if !ok && create {
//		fmt.Println("core/state/database.go:827 Create trie")
//		account, err := tds.ReadAccountData(address)
//		if err != nil {
//			return nil, err
//		}
//		if account == nil {
//			fmt.Println("core/state/database.go:833 account==nil")
//			t = trie.New(common.Hash{})
//			//t.Version=1
//		} else {
//			fmt.Println("core/state/database.go:833 account!=nil", account.GetIncarnation())
//			//t.Version=account.GetIncarnation()
//
//
//			t = trie.New(account.Root)
//		}
//		t.SetTouchFunc(func(hex []byte, del bool) {
//			tds.tp.TouchContract(address, hex, del)
//		})
//		tds.storageTries[address] = t
//	}
//	return t, nil
//}

func (tds *TrieDbState) ReadAccountStorage(address common.Address, version uint8, key *common.Hash) ([]byte, error) {
	fmt.Println("core/state/database.go:828 ReadAccountStorage addr=", address.String(), "version=", version, "k=", key.String())

	seckey, err := tds.HashKey(key, false /*save*/)
	fmt.Println("core/state/database.go:836 seckey", seckey)
	if err != nil {
		return nil, err
	}
	if tds.resolveReads {
		fmt.Println("core/state/database.go:841 tds.resolveReads")
		var addReadRecord = false
		if mWrite, ok := tds.currentBuffer.storageUpdates[address]; ok {
			if _, ok1 := mWrite[seckey]; !ok1 {
				addReadRecord = true
			}
		} else {
			addReadRecord = true
		}
		if addReadRecord {
			m, ok := tds.currentBuffer.storageReads[address]
			if !ok {
				m = make(map[common.Hash]struct{})
				tds.currentBuffer.storageReads[address] = m
			}
			m[seckey] = struct{}{}
		}
	}

	addrHash,err:=tds.HashAddress(address, false)
	if err != nil {
		return nil, err
	}

	cKey:=GenerateCompositeStorageKey(addrHash, version, seckey)
	enc, ok := tds.storageTrie.Get(cKey, tds.blockNr)
	if ok {
		// Unwrap one RLP level
		if len(enc) > 1 {
			enc = enc[1:]
		}
	} else {
		// Not present in the trie, try database
		if tds.historical {
			enc, err = tds.db.GetAsOf(StorageBucket, StorageHistoryBucket, cKey, tds.blockNr)
			if err != nil {
				enc = nil
			}
		} else {
			enc, err = tds.db.Get(StorageBucket, cKey)
			if err != nil {
				enc = nil
			}
		}
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
		tds.pg.ReadCode(codeHash, code)
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
		tds.pg.ReadCode(codeHash, code)
	}
	return codeSize, nil
}

var prevMemStats runtime.MemStats

func (tds *TrieDbState) PruneTries(print bool) {
	/*
		if print {
			mainPrunable := tds.t.CountPrunableNodes()
			prunableNodes := mainPrunable
			for _, storageTrie := range tds.storageTries {
				prunableNodes += storageTrie.CountPrunableNodes()
			}
			fmt.Printf("[Before] Actual prunable nodes: %d (main %d), accounted: %d\n", prunableNodes, mainPrunable, tds.tp.NodeCount())
		}
	*/
	pruned, emptyAddresses, err := tds.tp.PruneTo(tds.t, int(MaxTrieCacheGen), func(contract common.Address) (*trie.Trie, error) {
		//@todo(b00ris) impliment pruning for tds.storageTrie
		//return tds.getStorageTrie(contract, false)
		return nil, nil
	})
	if err != nil {
		fmt.Printf("Error while pruning: %v\n", err)
	}
	if !pruned {
		//return
	}
	/*
		if print {
			mainPrunable := tds.t.CountPrunableNodes()
			prunableNodes := mainPrunable
			for _, storageTrie := range tds.storageTries {
				prunableNodes += storageTrie.CountPrunableNodes()
			}
			fmt.Printf("[After] Actual prunable nodes: %d (main %d), accounted: %d\n", prunableNodes, mainPrunable, tds.tp.NodeCount())
		}
	*/
	// Storage tries that were completely pruned
	for _, address := range emptyAddresses {
		//@todo(b00ris) Remove it from tds.storageTrie
		//delete(tds.storageTries, address)
		_=address
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory", "nodes", tds.tp.NodeCount(), "alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
	if print {
		fmt.Printf("Pruning done. Nodes: %d, alloc: %d, sys: %d, numGC: %d\n", tds.tp.NodeCount(), int(m.Alloc/1024), int(m.Sys/1024), int(m.NumGC))
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

func accountsEqual(a1, a2 *accounts.Account) bool {
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

func (tsw *TrieStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	addrHash, err := tsw.tds.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}
	tsw.tds.currentBuffer.accountUpdates[addrHash] = account
	return nil
}

func (dsw *DbStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	fmt.Println("core/state/database.go:1044 UpdateAccountData", address.String(), "orig.vers", original.GetIncarnation(), "acc.vers",account.GetIncarnation())
	data, err := account.Encode(ctx)
	fmt.Println("core/state/database.go:1046 encode", data)
	if err != nil {
		return err
	}

	acc,err:=accounts.Decode(data)
	fmt.Println("core/state/database.go:1046 decoded version", acc.GetIncarnation(), err)


	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
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
		originalData, err = original.Encode(ctx)
		if err != nil {
			return err
		}
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := tsw.tds.HashAddress(address, false /*save*/)
	if err != err {
		return err
	}
	tsw.tds.currentBuffer.accountUpdates[addrHash] = nil
	tsw.tds.currentBuffer.deleted[address] = struct{}{}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
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
		originalData, err = original.Encode(ctx)
		if err != nil {
			return err
		}
	}
	return dsw.tds.db.PutS(AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if tsw.tds.resolveReads {
		tsw.tds.pg.CreateCode(codeHash, code)
	}
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(codeHash common.Hash, code []byte) error {
	if dsw.tds.resolveReads {
		dsw.tds.pg.CreateCode(codeHash, code)
	}
	return dsw.tds.db.Put(CodeBucket, codeHash[:], code)
}

func (tsw *TrieStateWriter) WriteAccountStorage(address common.Address, version uint8,  key, original, value *common.Hash) error {
	v := bytes.TrimLeft(value[:], "\x00")
	m, ok := tsw.tds.currentBuffer.storageUpdates[address]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.currentBuffer.storageUpdates[address] = m
	}
	seckey, err := tsw.tds.HashKey(key, false /*save*/)
	if err != nil {
		return err
	}
	if len(v) > 0 {
		// Write into 1 extra RLP level
		var vv []byte
		if len(v) > 1 || v[0] >= 128 {
			vv = make([]byte, len(v)+1)
			vv[0] = byte(128 + len(v))
			copy(vv[1:], v)
		} else {
			vv = make([]byte, 1)
			vv[0] = v[0]
		}
		m[seckey] = vv
	} else {
		m[seckey] = nil
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(address common.Address, version uint8, key, original, value *common.Hash) error {
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

	addrHash,err:=dsw.tds.HashAddress(address, false)
	if err != nil {
		return  err
	}

	compositeKey := GenerateCompositeStorageKey(addrHash, version, seckey)
	fmt.Println("core/state/database.go:1129 WriteAccountStorage acc=", address.String(),"version=", version, key, compositeKey, value)

	//compositeKey := append(address[:], seckey[:]...)
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

func (tds *TrieDbState) ExtractProofs(trace bool) trie.BlockProof {
	return tds.pg.ExtractProofs(trace)
}


func caller(n int) string {
	buf:=new(bytes.Buffer)
	for i:=1;i<=n;i++ {
		_,f,l,_:=runtime.Caller(i)
		fmt.Fprintln(buf, f,l)
	}
	return buf.String()
}

func GenerateCompositeStorageKey(addressHash common.Hash, version uint8, seckey common.Hash) []byte {
	compositeKey:=make([]byte, 0, 32+1+32)
	compositeKey = append(compositeKey, GenerateStoragePrefix(addressHash,version)...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}
func GenerateStoragePrefix(addressHash common.Hash, version uint8) []byte {
	prefix :=make([]byte, 0, 32+1)
	prefix = append(prefix, addressHash[:]...)
	prefix = append(prefix, version)
	return prefix
}