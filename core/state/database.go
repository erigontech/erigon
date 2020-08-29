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

//nolint:scopelint
package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
)

var _ StateWriter = (*TrieStateWriter)(nil)

// MaxTrieCacheSize is the trie cache size limit after which to evict trie nodes from memory.
var MaxTrieCacheSize = uint64(1024 * 1024)

const (
	//FirstContractIncarnation - first incarnation for contract accounts. After 1 it increases by 1.
	FirstContractIncarnation = 1
	//NonContractIncarnation incarnation for non contracts
	NonContractIncarnation = 0
)

type StateReader interface {
	ReadAccountData(address common.Address) (*accounts.Account, error)
	ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error)
	ReadAccountCode(address common.Address, codeHash common.Hash) ([]byte, error)
	ReadAccountCodeSize(address common.Address, codeHash common.Hash) (int, error)
	ReadAccountIncarnation(address common.Address) (uint64, error)
}

type StateWriter interface {
	UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error
	UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error
	DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error
	WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error
	CreateContract(address common.Address) error
}

type WriterWithChangeSets interface {
	StateWriter
	WriteChangeSets() error
	WriteHistory() error
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

func (nw *NoopWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	return nil
}

func (nw *NoopWriter) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	return nil
}

func (nw *NoopWriter) CreateContract(address common.Address) error {
	return nil
}

// Buffer is a structure holding updates, deletes, and reads registered within one change period
// A change period can be transaction within a block, or a block within group of blocks
type Buffer struct {
	codeReads     map[common.Hash]common.Hash
	codeSizeReads map[common.Hash]common.Hash
	codeUpdates   map[common.Hash][]byte
	// storageUpdates structure collects the effects of the block (or transaction) execution. It does not necessarily
	// include all the intermediate reads and write that happened. For example, if the storage of some contract has
	// been modified, and then the contract has subsequently self-destructed, this structure will not contain any
	// keys related to the storage of this contract, because they are irrelevant for the final state
	storageUpdates     map[common.Hash]map[common.Hash][]byte
	storageIncarnation map[common.Hash]uint64
	// storageReads structure collects all the keys of items that have been modified (or also just read, if the
	// tds.resolveReads flag is turned on, which happens during the generation of block witnesses).
	// Even if the final results of the execution do not include some items, they will still be present in this structure.
	// For example, if the storage of some contract has been modified, and then the contract has subsequently self-destructed,
	// this structure will contain all the keys that have been modified or deleted prior to the self-destruction.
	// It is important to keep them because they will be used to apply changes to the trie one after another.
	// There is a potential for optimisation - we may actually skip all the intermediate modification of the trie if
	// we know that in the end, the entire storage will be dropped. However, this optimisation has not yet been
	// implemented.
	storageReads map[common.StorageKey]struct{}
	// accountUpdates structure collects the effects of the block (or transaxction) execution.
	accountUpdates map[common.Hash]*accounts.Account
	// accountReads structure collects all the address hashes of the accounts that have been modified (or also just read,
	// if tds.resolveReads flag is turned on, which happens during the generation of block witnesses).
	accountReads            map[common.Hash]struct{}
	accountReadsIncarnation map[common.Hash]uint64
	deleted                 map[common.Hash]struct{}
	created                 map[common.Hash]struct{}
}

// Prepares buffer for work or clears previous data
func (b *Buffer) initialise() {
	b.codeReads = make(map[common.Hash]common.Hash)
	b.codeSizeReads = make(map[common.Hash]common.Hash)
	b.codeUpdates = make(map[common.Hash][]byte)
	b.storageUpdates = make(map[common.Hash]map[common.Hash][]byte)
	b.storageIncarnation = make(map[common.Hash]uint64)
	b.storageReads = make(map[common.StorageKey]struct{})
	b.accountUpdates = make(map[common.Hash]*accounts.Account)
	b.accountReads = make(map[common.Hash]struct{})
	b.accountReadsIncarnation = make(map[common.Hash]uint64)
	b.deleted = make(map[common.Hash]struct{})
	b.created = make(map[common.Hash]struct{})
}

// Replaces account pointer with pointers to the copies
func (b *Buffer) detachAccounts() {
	for addrHash, account := range b.accountUpdates {
		if account != nil {
			b.accountUpdates[addrHash] = account.SelfCopy()
		}
	}
}

// Merges the content of another buffer into this one
func (b *Buffer) merge(other *Buffer) {
	for addrHash, codeHash := range other.codeReads {
		b.codeReads[addrHash] = codeHash
	}

	for addrHash, code := range other.codeUpdates {
		b.codeUpdates[addrHash] = code
	}

	for address, codeHash := range other.codeSizeReads {
		b.codeSizeReads[address] = codeHash
	}

	for addrHash := range other.deleted {
		b.deleted[addrHash] = struct{}{}
		delete(b.storageUpdates, addrHash)
		delete(b.storageIncarnation, addrHash)
		delete(b.codeUpdates, addrHash)
	}
	for addrHash := range other.created {
		b.created[addrHash] = struct{}{}
		delete(b.storageUpdates, addrHash)
		delete(b.storageIncarnation, addrHash)
	}
	for addrHash, om := range other.storageUpdates {
		m, ok := b.storageUpdates[addrHash]
		if !ok {
			m = make(map[common.Hash][]byte)
			b.storageUpdates[addrHash] = m
		}
		for keyHash, v := range om {
			m[keyHash] = v
		}
	}
	for addrHash, incarnation := range other.storageIncarnation {
		b.storageIncarnation[addrHash] = incarnation
	}
	for storageKey := range other.storageReads {
		b.storageReads[storageKey] = struct{}{}
	}
	for addrHash, account := range other.accountUpdates {
		b.accountUpdates[addrHash] = account
	}
	for addrHash := range other.accountReads {
		b.accountReads[addrHash] = struct{}{}
	}
	for addrHash, incarnation := range other.accountReadsIncarnation {
		b.accountReadsIncarnation[addrHash] = incarnation
	}
}

// TrieDbState implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                 *trie.Trie
	tMu               *sync.Mutex
	db                ethdb.Database
	blockNr           uint64
	buffers           []*Buffer
	aggregateBuffer   *Buffer // Merge of all buffers
	currentBuffer     *Buffer
	historical        bool
	noHistory         bool
	resolveReads      bool
	retainListBuilder *trie.RetainListBuilder
	tp                *trie.Eviction
	newStream         trie.Stream
	hashBuilder       *trie.HashBuilder
	loader            *trie.SubTrieLoader
	pw                *PreimageWriter
	incarnationMap    map[common.Address]uint64 // Temporary map of incarnation for the cases when contracts are deleted and recreated within 1 block
}

func NewTrieDbState(root common.Hash, db ethdb.Database, blockNr uint64) *TrieDbState {
	t := trie.New(root)
	tp := trie.NewEviction()

	tds := &TrieDbState{
		t:                 t,
		tMu:               new(sync.Mutex),
		db:                db,
		blockNr:           blockNr,
		retainListBuilder: trie.NewRetainListBuilder(),
		tp:                tp,
		pw:                &PreimageWriter{db: db, savePreimages: true},
		hashBuilder:       trie.NewHashBuilder(false),
		incarnationMap:    make(map[common.Address]uint64),
	}

	tp.SetBlockNumber(blockNr)

	t.AddObserver(tp)

	return tds
}

func (tds *TrieDbState) EnablePreimages(ep bool) {
	tds.pw.SetSavePreimages(ep)
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
	tds.tMu.Lock()
	tcopy := *tds.t
	tds.tMu.Unlock()

	n := tds.getBlockNr()
	tp := trie.NewEviction()
	tp.SetBlockNumber(n)

	cpy := TrieDbState{
		t:              &tcopy,
		tMu:            new(sync.Mutex),
		db:             tds.db,
		blockNr:        n,
		tp:             tp,
		pw:             &PreimageWriter{db: tds.db, savePreimages: true},
		hashBuilder:    trie.NewHashBuilder(false),
		incarnationMap: make(map[common.Address]uint64),
	}

	cpy.t.AddObserver(tp)

	return &cpy
}

func (tds *TrieDbState) Database() ethdb.Database {
	return tds.db
}

func (tds *TrieDbState) Trie() *trie.Trie {
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

func (tds *TrieDbState) WithNewBuffer() *TrieDbState {
	aggregateBuffer := &Buffer{}
	aggregateBuffer.initialise()

	currentBuffer := &Buffer{}
	currentBuffer.initialise()

	buffers := []*Buffer{currentBuffer}

	tds.tMu.Lock()
	t := &TrieDbState{
		t:                 tds.t,
		tMu:               tds.tMu,
		db:                tds.db,
		blockNr:           tds.getBlockNr(),
		buffers:           buffers,
		aggregateBuffer:   aggregateBuffer,
		currentBuffer:     currentBuffer,
		historical:        tds.historical,
		noHistory:         tds.noHistory,
		resolveReads:      tds.resolveReads,
		retainListBuilder: tds.retainListBuilder,
		tp:                tds.tp,
		pw:                tds.pw,
		hashBuilder:       trie.NewHashBuilder(false),
		incarnationMap:    make(map[common.Address]uint64),
	}
	tds.tMu.Unlock()

	return t
}

func (tds *TrieDbState) LastRoot() common.Hash {
	if tds == nil || tds.tMu == nil {
		return common.Hash{}
	}
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	return tds.t.Hash()
}

// ComputeTrieRoots is a combination of `ResolveStateTrie` and `UpdateStateTrie`
// DESCRIBED: docs/programmers_guide/guide.md#organising-ethereum-state-into-a-merkle-tree
func (tds *TrieDbState) ComputeTrieRoots() ([]common.Hash, error) {
	if _, err := tds.ResolveStateTrie(false, false); err != nil {
		return nil, err
	}
	return tds.UpdateStateTrie()
}

// UpdateStateTrie assumes that the state trie is already fully resolved, i.e. any operations
// will find necessary data inside the trie.
func (tds *TrieDbState) UpdateStateTrie() ([]common.Hash, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	roots, err := tds.updateTrieRoots(true)
	tds.clearUpdates()
	return roots, err
}

func (tds *TrieDbState) PrintTrie(w io.Writer) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	tds.t.Print(w)
}

// buildStorageReads builds a sorted list of all storage key hashes that were modified
// (or also just read, if tds.resolveReads flag is turned on) within the
// period for which we are aggregating updates. It includes the keys of items that
// were nullified by subsequent updates - best example is the
// self-destruction of a contract, which nullifies all previous
// modifications of the contract's storage. In such case, all previously modified storage
// item updates would be inclided.
func (tds *TrieDbState) buildStorageReads() common.StorageKeys {
	storageTouches := common.StorageKeys{}
	for storageKey := range tds.aggregateBuffer.storageReads {
		storageTouches = append(storageTouches, storageKey)
	}
	sort.Sort(storageTouches)
	return storageTouches
}

// buildStorageWrites builds a sorted list of all storage key hashes that were modified within the
// period for which we are aggregating updates. It skips the updates that
// were nullified by subsequent updates - best example is the
// self-destruction of a contract, which nullifies all previous
// modifications of the contract's storage. In such case, no storage
// item updates would be inclided.
func (tds *TrieDbState) buildStorageWrites() (common.StorageKeys, [][]byte) {
	storageTouches := common.StorageKeys{}
	for addrHash, m := range tds.aggregateBuffer.storageUpdates {
		for keyHash := range m {
			var storageKey common.StorageKey
			copy(storageKey[:], addrHash[:])
			binary.BigEndian.PutUint64(storageKey[common.HashLength:], tds.aggregateBuffer.storageIncarnation[addrHash])
			copy(storageKey[common.HashLength+common.IncarnationLength:], keyHash[:])
			storageTouches = append(storageTouches, storageKey)
		}
	}
	sort.Sort(storageTouches)
	var addrHash common.Hash
	var keyHash common.Hash
	var values = make([][]byte, len(storageTouches))
	for i, storageKey := range storageTouches {
		copy(addrHash[:], storageKey[:])
		copy(keyHash[:], storageKey[common.HashLength+common.IncarnationLength:])
		values[i] = tds.aggregateBuffer.storageUpdates[addrHash][keyHash]
	}
	return storageTouches, values
}

// Populate pending block proof so that it will be sufficient for accessing all storage slots in storageTouches
func (tds *TrieDbState) populateStorageBlockProof(storageTouches common.StorageKeys) error { //nolint
	for _, storageKey := range storageTouches {
		addr, _, hash := dbutils.ParseCompositeStorageKey(storageKey[:])
		key := dbutils.GenerateCompositeTrieKey(addr, hash)
		tds.retainListBuilder.AddStorageTouch(key[:])
	}
	return nil
}

func (tds *TrieDbState) buildCodeTouches() map[common.Hash]common.Hash {
	return tds.aggregateBuffer.codeReads
}

func (tds *TrieDbState) buildCodeSizeTouches() map[common.Hash]common.Hash {
	return tds.aggregateBuffer.codeSizeReads
}

// buildAccountReads builds a sorted list of all address hashes that were modified
// (or also just read, if tds.resolveReads flags is turned one) within the
// period for which we are aggregating update
func (tds *TrieDbState) buildAccountReads() common.Hashes {
	accountTouches := common.Hashes{}
	for addrHash := range tds.aggregateBuffer.accountReads {
		accountTouches = append(accountTouches, addrHash)
	}
	sort.Sort(accountTouches)
	return accountTouches
}

// buildAccountWrites builds a sorted list of all address hashes that were modified within the
// period for which we are aggregating updates.
func (tds *TrieDbState) buildAccountWrites() (common.Hashes, []*accounts.Account, [][]byte) {
	accountTouches := common.Hashes{}
	for addrHash, aValue := range tds.aggregateBuffer.accountUpdates {
		if aValue != nil {
			if _, ok := tds.aggregateBuffer.deleted[addrHash]; ok {
				// This adds an extra entry that wipes out the storage of the accout in the stream
				accountTouches = append(accountTouches, addrHash)
			} else if _, ok1 := tds.aggregateBuffer.created[addrHash]; ok1 {
				// This adds an extra entry that wipes out the storage of the accout in the stream
				accountTouches = append(accountTouches, addrHash)
			}
		}
		accountTouches = append(accountTouches, addrHash)
	}
	sort.Sort(accountTouches)
	aValues := make([]*accounts.Account, len(accountTouches))
	aCodes := make([][]byte, len(accountTouches))
	for i, addrHash := range accountTouches {
		if i < len(accountTouches)-1 && addrHash == accountTouches[i+1] {
			aValues[i] = nil // Entry that would wipe out existing storage
		} else {
			a := tds.aggregateBuffer.accountUpdates[addrHash]
			if a != nil {
				if _, ok := tds.aggregateBuffer.storageUpdates[addrHash]; ok {
					var ac accounts.Account
					ac.Copy(a)
					ac.Root = trie.EmptyRoot
					a = &ac
				}
			}
			aValues[i] = a
			if code, ok := tds.aggregateBuffer.codeUpdates[addrHash]; ok {
				aCodes[i] = code
			}
		}
	}
	return accountTouches, aValues, aCodes
}

func (tds *TrieDbState) resolveCodeTouches(
	codeTouches map[common.Hash]common.Hash,
	codeSizeTouches map[common.Hash]common.Hash,
	loadFunc trie.LoadFunc,
) error {
	firstRequest := true
	for address, codeHash := range codeTouches {
		delete(codeSizeTouches, codeHash)
		if need, req := tds.t.NeedLoadCode(address, codeHash, true /*bytecode*/); need {
			if tds.loader == nil {
				tds.loader = trie.NewSubTrieLoader(tds.blockNr)
			} else if firstRequest {
				tds.loader.Reset(tds.blockNr)
			}
			firstRequest = false
			tds.loader.AddCodeRequest(req)
		}
	}

	for address, codeHash := range codeSizeTouches {
		if need, req := tds.t.NeedLoadCode(address, codeHash, false /*bytecode*/); need {
			if tds.loader == nil {
				tds.loader = trie.NewSubTrieLoader(tds.blockNr)
			} else if firstRequest {
				tds.loader.Reset(tds.blockNr)
			}
			firstRequest = false
			tds.loader.AddCodeRequest(req)
		}
	}

	if !firstRequest {
		if _, err := loadFunc(tds.loader, nil, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

var bytes8 [8]byte

func (tds *TrieDbState) resolveAccountAndStorageTouches(accountTouches common.Hashes, storageTouches common.StorageKeys, loadFunc trie.LoadFunc) error {
	// Build the retain list
	rl := trie.NewRetainList(0)
	for _, addrHash := range accountTouches {
		var incarnation uint64
		if inc, ok := tds.aggregateBuffer.accountReadsIncarnation[addrHash]; ok {
			incarnation = inc
		}
		var nibbles = make([]byte, 2*(common.HashLength+common.IncarnationLength))
		for i, b := range addrHash[:] {
			nibbles[i*2] = b / 16
			nibbles[i*2+1] = b % 16
		}
		binary.BigEndian.PutUint64(bytes8[:], incarnation)
		for i, b := range bytes8[:] {
			nibbles[2*common.HashLength+i*2] = b / 16
			nibbles[2*common.HashLength+i*2+1] = b % 16
		}
		rl.AddHex(nibbles)
	}
	for _, storageKey := range storageTouches {
		var nibbles = make([]byte, 2*len(storageKey))
		for i, b := range storageKey[:] {
			nibbles[i*2] = b / 16
			nibbles[i*2+1] = b % 16
		}
		rl.AddHex(nibbles)
	}

	dbPrefixes, fixedbits, hooks := tds.t.FindSubTriesToLoad(rl)
	// FindSubTriesToLoad would have gone through the entire rs, so we need to rewind to the beginning
	rl.Rewind()
	loader := trie.NewSubTrieLoader(tds.blockNr)
	subTries, err := loadFunc(loader, rl, dbPrefixes, fixedbits)
	if err != nil {
		return err
	}
	if err := tds.t.HookSubTries(subTries, hooks); err != nil {
		for i, hash := range subTries.Hashes {
			log.Error("Info for error", "dbPrefix", fmt.Sprintf("%x", dbPrefixes[i]), "fixedbits", fixedbits[i], "hash", hash)
		}
		return err
	}
	return nil
}

func (tds *TrieDbState) populateAccountBlockProof(accountTouches common.Hashes) {
	for _, addrHash := range accountTouches {
		a := addrHash
		tds.retainListBuilder.AddTouch(a[:])
	}
}

// ExtractTouches returns two lists of keys - for accounts and storage items correspondingly
// Each list is the collection of keys that have been "touched" (inserted, updated, or simply accessed)
// since the last invocation of `ExtractTouches`.
func (tds *TrieDbState) ExtractTouches() (accountTouches [][]byte, storageTouches [][]byte) {
	return tds.retainListBuilder.ExtractTouches()
}

func (tds *TrieDbState) resolveStateTrieWithFunc(loadFunc trie.LoadFunc) error {
	// Aggregating the current buffer, if any
	if tds.currentBuffer != nil {
		if tds.aggregateBuffer == nil {
			tds.aggregateBuffer = &Buffer{}
			tds.aggregateBuffer.initialise()
		}
		tds.aggregateBuffer.merge(tds.currentBuffer)
	}
	if tds.aggregateBuffer == nil {
		return nil
	}

	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	// Prepare (resolve) storage tries so that actual modifications can proceed without database access
	storageTouches := tds.buildStorageReads()

	// Prepare (resolve) accounts trie so that actual modifications can proceed without database access
	accountTouches := tds.buildAccountReads()

	// Prepare (resolve) contract code reads so that actual modifications can proceed without database access
	codeTouches := tds.buildCodeTouches()

	// Prepare (resolve) contract code size reads so that actual modifications can proceed without database access
	codeSizeTouches := tds.buildCodeSizeTouches()

	var err error
	if err = tds.resolveAccountAndStorageTouches(accountTouches, storageTouches, loadFunc); err != nil {
		return err
	}

	if err = tds.resolveCodeTouches(codeTouches, codeSizeTouches, loadFunc); err != nil {
		return err
	}

	if tds.resolveReads {
		tds.populateAccountBlockProof(accountTouches)
	}

	if tds.resolveReads {
		if err := tds.populateStorageBlockProof(storageTouches); err != nil {
			return err
		}
	}
	return nil
}

// ResolveStateTrie resolves parts of the state trie that would be necessary for any updates
// (and reads, if `resolveReads` is set).
func (tds *TrieDbState) ResolveStateTrie(extractWitnesses bool, trace bool) ([]*trie.Witness, error) {
	var witnesses []*trie.Witness

	loadFunc := func(loader *trie.SubTrieLoader, rl *trie.RetainList, dbPrefixes [][]byte, fixedbits []int) (trie.SubTries, error) {
		if loader == nil {
			return trie.SubTries{}, nil
		}
		subTries, err := loader.LoadSubTries(tds.db, tds.blockNr, rl, nil /* hashCollector */, dbPrefixes, fixedbits, trace)
		if err != nil {
			return subTries, err
		}

		if !extractWitnesses {
			return subTries, nil
		}

		rl.Rewind()
		witnesses, err = trie.ExtractWitnesses(subTries, trace, rl)
		return subTries, err
	}
	if err := tds.resolveStateTrieWithFunc(loadFunc); err != nil {
		return nil, err
	}

	return witnesses, nil
}

// ResolveStateTrieStateless uses a witness DB to resolve subtries
func (tds *TrieDbState) ResolveStateTrieStateless(database trie.WitnessStorage) error {
	var startPos int64
	loadFunc := func(loader *trie.SubTrieLoader, rl *trie.RetainList, dbPrefixes [][]byte, fixedbits []int) (trie.SubTries, error) {
		if loader == nil {
			return trie.SubTries{}, nil
		}

		subTries, pos, err := loader.LoadFromWitnessDb(database, tds.blockNr, uint32(MaxTrieCacheSize), startPos, len(dbPrefixes))
		if err != nil {
			return subTries, err
		}

		startPos = pos
		return subTries, nil
	}

	return tds.resolveStateTrieWithFunc(loadFunc)
}

// CalcTrieRoots calculates trie roots without modifying the state trie
func (tds *TrieDbState) CalcTrieRoots(trace bool) (common.Hash, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	// Retrive the list of inserted/updated/deleted storage items (keys and values)
	storageKeys, sValues := tds.buildStorageWrites()
	if trace {
		fmt.Printf("len(storageKeys)=%d, len(sValues)=%d\n", len(storageKeys), len(sValues))
	}
	// Retrive the list of inserted/updated/deleted accounts (keys and values)
	accountKeys, aValues, aCodes := tds.buildAccountWrites()
	if trace {
		fmt.Printf("len(accountKeys)=%d, len(aValues)=%d\n", len(accountKeys), len(aValues))
	}
	var hb *trie.HashBuilder
	if trace {
		hb = trie.NewHashBuilder(true)
	} else {
		hb = tds.hashBuilder
	}
	if len(accountKeys) == 0 && len(storageKeys) == 0 {
		return tds.t.Hash(), nil
	}
	return trie.HashWithModifications(tds.t, accountKeys, aValues, aCodes, storageKeys, sValues, common.HashLength+common.IncarnationLength, &tds.newStream, hb, trace)
}

// forward is `true` if the function is used to progress the state forward (by adding blocks)
// forward is `false` if the function is used to rewind the state (for reorgs, for example)
func (tds *TrieDbState) updateTrieRoots(forward bool) ([]common.Hash, error) {
	accountUpdates := tds.aggregateBuffer.accountUpdates
	// Perform actual updates on the tries, and compute one trie root per buffer
	// These roots can be used to populate receipt.PostState on pre-Byzantium
	roots := make([]common.Hash, len(tds.buffers))
	for i, b := range tds.buffers {
		// For the contracts that got deleted, we clear the storage
		for addrHash := range b.deleted {
			// The only difference between Delete and DeleteSubtree is that Delete would delete accountNode too,
			// wherewas DeleteSubtree will keep the accountNode, but will make the storage sub-trie empty
			tds.t.DeleteSubtree(addrHash[:])
		}
		// New contracts are being created at these addresses. Therefore, we need to clear the storage items
		// that might be remaining in the trie and figure out the next incarnations
		for addrHash := range b.created {
			// The only difference between Delete and DeleteSubtree is that Delete would delete accountNode too,
			// wherewas DeleteSubtree will keep the accountNode, but will make the storage sub-trie empty
			tds.t.DeleteSubtree(addrHash[:])
		}

		for addrHash, account := range b.accountUpdates {
			if account != nil {
				//fmt.Println("updateTrieRoots b.accountUpdates", addrHash.String(), account.Incarnation)
				tds.t.UpdateAccount(addrHash[:], account)
			} else {
				tds.t.Delete(addrHash[:])
			}
		}

		for addrHash, newCode := range b.codeUpdates {
			if err := tds.t.UpdateAccountCode(addrHash[:], newCode); err != nil {
				return nil, err
			}
		}
		for addrHash, m := range b.storageUpdates {
			for keyHash, v := range m {
				cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
				if len(v) > 0 {
					//fmt.Printf("Update storage trie addrHash %x, keyHash %x: %x\n", addrHash, keyHash, v)
					if forward {
						tds.t.Update(cKey, v)
					} else {
						// If rewinding, it might not be possible to execute storage item update.
						// If we rewind from the state where a contract does not exist anymore (it was self-destructed)
						// to the point where it existed (with storage), then rewinding to the point of existence
						// will not bring back the full storage trie. Instead there will be one hashNode.
						// So we probe for this situation first
						if _, ok := tds.t.Get(cKey); ok {
							tds.t.Update(cKey, v)
						}
					}
				} else {
					if forward {
						tds.t.Delete(cKey)
					} else {
						// If rewinding, it might not be possible to execute storage item update.
						// If we rewind from the state where a contract does not exist anymore (it was self-destructed)
						// to the point where it existed (with storage), then rewinding to the point of existence
						// will not bring back the full storage trie. Instead there will be one hashNode.
						// So we probe for this situation first
						if _, ok := tds.t.Get(cKey); ok {
							tds.t.Delete(cKey)
						}
					}
				}
			}

			if account, ok := b.accountUpdates[addrHash]; ok && account != nil {
				ok, root := tds.t.DeepHash(addrHash[:])
				if ok {
					account.Root = root
					//fmt.Printf("(b)Set %x root for addrHash %x\n", root, addrHash)
				} else {
					//fmt.Printf("(b)Set empty root for addrHash %x\n", addrHash)
					account.Root = trie.EmptyRoot
				}
			}
			if account, ok := accountUpdates[addrHash]; ok && account != nil {
				ok, root := tds.t.DeepHash(addrHash[:])
				if ok {
					account.Root = root
					//fmt.Printf("Set %x root for addrHash %x\n", root, addrHash)
				} else {
					//fmt.Printf("Set empty root for addrHash %x\n", addrHash)
					account.Root = trie.EmptyRoot
				}
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

func (tds *TrieDbState) SetBlockNr(blockNr uint64) {
	tds.setBlockNr(blockNr)
	tds.tp.SetBlockNumber(blockNr)
}

func (tds *TrieDbState) GetBlockNr() uint64 {
	return tds.getBlockNr()
}

func (tds *TrieDbState) UnwindTo(blockNr uint64) error {
	//fmt.Printf("Unwind from block %d to block %d\n", tds.blockNr, blockNr)
	tds.StartNewBuffer()
	b := tds.currentBuffer

	accountMap, storageMap, err := ethdb.RewindData(tds.db, tds.blockNr, blockNr)
	if err != nil {
		return err
	}
	for key, value := range accountMap {
		var addrHash common.Hash
		copy(addrHash[:], []byte(key))
		if len(value) > 0 {
			var acc accounts.Account
			if err := acc.DecodeForStorage(value); err != nil {
				return err
			}
			// Fetch the code hash
			if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
				if codeHash, err := tds.db.Get(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash[:], acc.Incarnation)); err == nil {
					copy(acc.CodeHash[:], codeHash)
				}
			}
			b.accountUpdates[addrHash] = &acc
			if err := rawdb.WriteAccount(tds.db, addrHash, acc); err != nil {
				return err
			}
			b.accountReadsIncarnation[addrHash] = acc.Incarnation
		} else {
			b.accountUpdates[addrHash] = nil
			if err := rawdb.DeleteAccount(tds.db, addrHash); err != nil {
				return err
			}
		}
		b.accountReads[addrHash] = struct{}{}
	}
	for key, value := range storageMap {
		var addrHash common.Hash
		copy(addrHash[:], []byte(key)[:common.HashLength])
		var keyHash common.Hash
		copy(keyHash[:], []byte(key)[common.HashLength+common.IncarnationLength:])
		m, ok := b.storageUpdates[addrHash]
		if !ok {
			m = make(map[common.Hash][]byte)
			b.storageUpdates[addrHash] = m
		}
		b.storageIncarnation[addrHash] = binary.BigEndian.Uint64([]byte(key)[common.HashLength:])
		var storageKey common.StorageKey
		copy(storageKey[:], []byte(key))
		b.storageReads[storageKey] = struct{}{}
		if len(value) > 0 {
			m[keyHash] = value
			if err := tds.db.Put(dbutils.CurrentStateBucket, []byte(key)[:common.HashLength+common.IncarnationLength+common.HashLength], value); err != nil {
				return err
			}
		} else {
			m[keyHash] = nil
			if err := tds.db.Delete(dbutils.CurrentStateBucket, []byte(key)[:common.HashLength+common.IncarnationLength+common.HashLength]); err != nil {
				return err
			}
		}
	}
	if _, err := tds.ResolveStateTrie(false, false); err != nil {
		return err
	}

	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	if _, err := tds.updateTrieRoots(false); err != nil {
		return err
	}
	for i := tds.blockNr; i > blockNr; i-- {
		if err := tds.deleteTimestamp(i); err != nil {
			return err
		}
	}
	if err := tds.truncateHistory(blockNr, accountMap, storageMap); err != nil {
		return err
	}
	tds.clearUpdates()
	tds.setBlockNr(blockNr)
	return nil
}

func (tds *TrieDbState) deleteTimestamp(timestamp uint64) error {
	changeSetKey := dbutils.EncodeTimestamp(timestamp)
	changedAccounts, err := tds.db.Get(dbutils.AccountChangeSetBucket, changeSetKey)
	if err != nil && err != ethdb.ErrKeyNotFound {
		return err
	}
	changedStorage, err := tds.db.Get(dbutils.StorageChangeSetBucket, changeSetKey)
	if err != nil && err != ethdb.ErrKeyNotFound {
		return err
	}
	if len(changedAccounts) > 0 {
		if err := tds.db.Delete(dbutils.AccountChangeSetBucket, changeSetKey); err != nil {
			return err
		}
	}
	if len(changedStorage) > 0 {
		if err := tds.db.Delete(dbutils.StorageChangeSetBucket, changeSetKey); err != nil {
			return err
		}
	}
	return nil
}

func (tds *TrieDbState) truncateHistory(timestampTo uint64, accountMap map[string][]byte, storageMap map[string][]byte) error {
	accountHistoryEffects := make(map[string][]byte)
	startKey := make([]byte, common.HashLength+8)
	for key := range accountMap {
		copy(startKey, []byte(key))
		binary.BigEndian.PutUint64(startKey[common.HashLength:], timestampTo)
		if err := tds.db.Walk(dbutils.AccountsHistoryBucket, startKey, 8*common.HashLength, func(k, v []byte) (bool, error) {
			timestamp := binary.BigEndian.Uint64(k[common.HashLength:]) // the last timestamp in the chunk
			kStr := string(common.CopyBytes(k))
			if timestamp > timestampTo {
				accountHistoryEffects[kStr] = nil
				// truncate the chunk
				index := dbutils.WrapHistoryIndex(v)
				index = index.TruncateGreater(timestampTo)
				if len(index) > 8 { // If the chunk is empty after truncation, it gets simply deleted
					// Truncated chunk becomes "the last chunk" with the timestamp 0xffff....ffff
					lastK := make([]byte, len(k))
					copy(lastK, k[:common.HashLength])
					binary.BigEndian.PutUint64(lastK[common.HashLength:], ^uint64(0))
					accountHistoryEffects[string(lastK)] = common.CopyBytes(index)
				}
			}
			return true, nil
		}); err != nil {
			return err
		}
	}
	storageHistoryEffects := make(map[string][]byte)
	startKey = make([]byte, 2*common.HashLength+8)
	for key := range storageMap {
		copy(startKey, []byte(key)[:common.HashLength])
		copy(startKey[common.HashLength:], []byte(key)[common.HashLength+8:])
		binary.BigEndian.PutUint64(startKey[2*common.HashLength:], timestampTo)
		if err := tds.db.Walk(dbutils.StorageHistoryBucket, startKey, 8*2*common.HashLength, func(k, v []byte) (bool, error) {
			timestamp := binary.BigEndian.Uint64(k[2*common.HashLength:]) // the last timestamp in the chunk
			kStr := string(common.CopyBytes(k))
			if timestamp > timestampTo {
				storageHistoryEffects[kStr] = nil
				index := dbutils.WrapHistoryIndex(v)
				index = index.TruncateGreater(timestampTo)
				if len(index) > 8 { // If the chunk is empty after truncation, it gets simply deleted
					// Truncated chunk becomes "the last chunk" with the timestamp 0xffff....ffff
					lastK := make([]byte, len(k))
					copy(lastK, k[:2*common.HashLength])
					binary.BigEndian.PutUint64(lastK[2*common.HashLength:], ^uint64(0))
					storageHistoryEffects[string(lastK)] = common.CopyBytes(index)
				}
			}
			return true, nil
		}); err != nil {
			return err
		}
	}
	for key, value := range accountHistoryEffects {
		if value == nil {
			if err := tds.db.Delete(dbutils.AccountsHistoryBucket, []byte(key)); err != nil {
				return err
			}
		} else {
			if err := tds.db.Put(dbutils.AccountsHistoryBucket, []byte(key), value); err != nil {
				return err
			}
		}
	}
	for key, value := range storageHistoryEffects {
		if value == nil {
			if err := tds.db.Delete(dbutils.StorageHistoryBucket, []byte(key)); err != nil {
				return err
			}
		} else {
			if err := tds.db.Put(dbutils.StorageHistoryBucket, []byte(key), value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tds *TrieDbState) readAccountDataByHash(addrHash common.Hash) (*accounts.Account, error) {
	if acc, ok := tds.GetAccount(addrHash); ok {
		return acc, nil
	}

	// Not present in the trie, try the database
	var a accounts.Account
	if ok, err := rawdb.ReadAccount(tds.db, addrHash, &a); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}
	return &a, nil
}

func (tds *TrieDbState) GetAccount(addrHash common.Hash) (*accounts.Account, bool) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	acc, ok := tds.t.GetAccount(addrHash[:])
	return acc, ok
}

func (tds *TrieDbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}
	var account *accounts.Account
	account, err = tds.readAccountDataByHash(addrHash)
	if err != nil {
		return nil, err
	}
	if tds.resolveReads {
		tds.currentBuffer.accountReads[addrHash] = struct{}{}
		if account != nil {
			tds.currentBuffer.accountReadsIncarnation[addrHash] = account.Incarnation
		}
	}
	return account, nil
}

func (tds *TrieDbState) GetKey(shaKey []byte) []byte {
	key, _ := tds.db.Get(dbutils.PreimagePrefix, shaKey)
	return key
}

func (tds *TrieDbState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	addrHash, err := tds.pw.HashAddress(address, false /*save*/)
	if err != nil {
		return nil, err
	}
	if tds.currentBuffer != nil {
		if _, ok := tds.currentBuffer.deleted[addrHash]; ok {
			return nil, nil
		}
	}
	if tds.aggregateBuffer != nil {
		if _, ok := tds.aggregateBuffer.deleted[addrHash]; ok {
			return nil, nil
		}
	}
	seckey, err := tds.pw.HashKey(key, false /*save*/)
	if err != nil {
		return nil, err
	}

	if tds.resolveReads {
		var storageKey common.StorageKey
		copy(storageKey[:], dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))
		tds.currentBuffer.storageReads[storageKey] = struct{}{}
	}

	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	enc, ok := tds.t.Get(dbutils.GenerateCompositeTrieKey(addrHash, seckey))
	if !ok {
		// Not present in the trie, try database
		enc, err = tds.db.Get(dbutils.CurrentStateBucket, dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))
		if err != nil {
			enc = nil
		}
	}
	return enc, nil
}

func (tds *TrieDbState) ReadCodeByHash(codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}

	code, err = tds.db.Get(dbutils.CodeBucket, codeHash[:])
	if tds.resolveReads {
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		tds.retainListBuilder.ReadCode(codeHash)
	}
	return code, err
}

func (tds *TrieDbState) readAccountCodeFromTrie(addrHash []byte) ([]byte, bool) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	return tds.t.GetAccountCode(addrHash)
}

func (tds *TrieDbState) readAccountCodeSizeFromTrie(addrHash []byte) (int, bool) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	return tds.t.GetAccountCodeSize(addrHash)
}

func (tds *TrieDbState) ReadAccountCode(address common.Address, codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}

	addrHash, err := tds.pw.HashAddress(address, false /*save*/)
	if err != nil {
		return nil, err
	}

	if cached, ok := tds.readAccountCodeFromTrie(addrHash[:]); ok {
		code, err = cached, nil
	} else {
		code, err = tds.db.Get(dbutils.CodeBucket, codeHash[:])
	}
	if tds.resolveReads {
		addrHash, err1 := common.HashData(address[:])
		if err1 != nil {
			return nil, err
		}
		tds.currentBuffer.accountReads[addrHash] = struct{}{}
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		tds.currentBuffer.codeReads[addrHash] = codeHash
		tds.retainListBuilder.ReadCode(codeHash)
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(address common.Address, codeHash common.Hash) (codeSize int, err error) {
	addrHash, err := tds.pw.HashAddress(address, false /*save*/)
	if err != nil {
		return 0, err
	}

	if cached, ok := tds.readAccountCodeSizeFromTrie(addrHash[:]); ok {
		codeSize, err = cached, nil
	} else {
		var code []byte
		code, err = tds.db.Get(dbutils.CodeBucket, codeHash[:])
		if err != nil {
			return 0, err
		}
		codeSize = len(code)
	}
	if tds.resolveReads {
		addrHash, err1 := common.HashData(address[:])
		if err1 != nil {
			return 0, err
		}
		tds.currentBuffer.accountReads[addrHash] = struct{}{}
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		tds.currentBuffer.codeSizeReads[addrHash] = codeHash
		// FIXME: support codeSize in witnesses if makes sense
		tds.retainListBuilder.ReadCode(codeHash)
	}
	return codeSize, nil
}

func (tds *TrieDbState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if inc, ok := tds.incarnationMap[address]; ok {
		return inc, nil
	}
	if b, err := tds.db.Get(dbutils.IncarnationMapBucket, address[:]); err == nil {
		return binary.BigEndian.Uint64(b), nil
	} else if errors.Is(err, ethdb.ErrKeyNotFound) {
		return 0, nil
	} else {
		return 0, err
	}
}

var prevMemStats runtime.MemStats

type TrieStateWriter struct {
	tds *TrieDbState
}

func (tds *TrieDbState) EvictTries(print bool) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	strict := print
	tds.incarnationMap = make(map[common.Address]uint64)
	if print {
		trieSize := tds.t.TrieSize()
		fmt.Println("") // newline for better formatting
		fmt.Printf("[Before] Actual nodes size: %d, accounted size: %d\n", trieSize, tds.tp.TotalSize())
	}

	if strict {
		actualAccounts := uint64(tds.t.NumberOfAccounts())
		fmt.Println("number of leaves: ", actualAccounts)
		accountedAccounts := tds.tp.NumberOf()
		if actualAccounts != accountedAccounts {
			panic(fmt.Errorf("account number mismatch: trie=%v eviction=%v", actualAccounts, accountedAccounts))
		}
		fmt.Printf("checking number --> ok\n")

		actualSize := uint64(tds.t.TrieSize())
		accountedSize := tds.tp.TotalSize()

		if actualSize != accountedSize {
			panic(fmt.Errorf("account size mismatch: trie=%v eviction=%v", actualSize, accountedSize))
		}
		fmt.Printf("checking size --> ok\n")
	}

	tds.tp.EvictToFitSize(tds.t, MaxTrieCacheSize)

	if strict {
		actualAccounts := uint64(tds.t.NumberOfAccounts())
		fmt.Println("number of leaves: ", actualAccounts)
		accountedAccounts := tds.tp.NumberOf()
		if actualAccounts != accountedAccounts {
			panic(fmt.Errorf("after eviction account number mismatch: trie=%v eviction=%v", actualAccounts, accountedAccounts))
		}
		fmt.Printf("checking number --> ok\n")

		actualSize := uint64(tds.t.TrieSize())
		accountedSize := tds.tp.TotalSize()

		if actualSize != accountedSize {
			panic(fmt.Errorf("after eviction account size mismatch: trie=%v eviction=%v", actualSize, accountedSize))
		}
		fmt.Printf("checking size --> ok\n")
	}

	if print {
		trieSize := tds.t.TrieSize()
		fmt.Printf("[After] Actual nodes size: %d, accounted size: %d\n", trieSize, tds.tp.TotalSize())

		actualAccounts := uint64(tds.t.NumberOfAccounts())
		fmt.Println("number of leaves: ", actualAccounts)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory", "nodes size", tds.tp.TotalSize(), "hashes", tds.t.HashMapSize(),
		"alloc", int(m.Alloc/1024), "sys", int(m.Sys/1024), "numGC", int(m.NumGC))
	if print {
		fmt.Printf("Eviction done. Nodes size: %d, alloc: %d, sys: %d, numGC: %d\n", tds.tp.TotalSize(), int(m.Alloc/1024), int(m.Sys/1024), int(m.NumGC))
	}
}

func (tds *TrieDbState) TrieStateWriter() *TrieStateWriter {
	return &TrieStateWriter{tds: tds}
}

// DbStateWriter creates a writer that is designed to write changes into the database batch
func (tds *TrieDbState) DbStateWriter() *DbStateWriter {
	return &DbStateWriter{blockNr: tds.blockNr, db: tds.db, pw: tds.pw, csw: NewChangeSetWriter()}
}

// DbStateWriter creates a writer that is designed to write changes into the database batch
func (tds *TrieDbState) PlainStateWriter() *PlainStateWriter {
	return NewPlainStateWriter(tds.db, nil, tds.blockNr)
}

func (tsw *TrieStateWriter) UpdateAccountData(_ context.Context, address common.Address, original, account *accounts.Account) error {
	addrHash, err := tsw.tds.pw.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}
	tsw.tds.currentBuffer.accountUpdates[addrHash] = account
	tsw.tds.currentBuffer.accountReads[addrHash] = struct{}{}
	if original != nil {
		tsw.tds.currentBuffer.accountReadsIncarnation[addrHash] = original.Incarnation
	}
	return nil
}

func (tsw *TrieStateWriter) DeleteAccount(_ context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := tsw.tds.pw.HashAddress(address, false /*save*/)
	if err != err {
		return err
	}
	tsw.tds.currentBuffer.accountUpdates[addrHash] = nil
	tsw.tds.currentBuffer.accountReads[addrHash] = struct{}{}
	if original != nil {
		tsw.tds.currentBuffer.accountReadsIncarnation[addrHash] = original.Incarnation
	}
	delete(tsw.tds.currentBuffer.storageUpdates, addrHash)
	delete(tsw.tds.currentBuffer.storageIncarnation, addrHash)
	delete(tsw.tds.currentBuffer.codeUpdates, addrHash)
	tsw.tds.currentBuffer.deleted[addrHash] = struct{}{}
	if original.Incarnation > 0 {
		tsw.tds.incarnationMap[address] = original.Incarnation
	}
	return nil
}

func (tsw *TrieStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if tsw.tds.resolveReads {
		tsw.tds.retainListBuilder.CreateCode(codeHash)
	}
	addrHash, err := common.HashData(address.Bytes())
	if err != nil {
		return err
	}
	tsw.tds.currentBuffer.codeUpdates[addrHash] = code
	return nil
}

func (tsw *TrieStateWriter) WriteAccountStorage(_ context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	addrHash, err := tsw.tds.pw.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}

	v := value.Bytes()
	m, ok := tsw.tds.currentBuffer.storageUpdates[addrHash]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.currentBuffer.storageUpdates[addrHash] = m
	}
	tsw.tds.currentBuffer.storageIncarnation[addrHash] = incarnation
	seckey, err := tsw.tds.pw.HashKey(key, false /*save*/)
	if err != nil {
		return err
	}
	var storageKey common.StorageKey
	copy(storageKey[:], dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))
	tsw.tds.currentBuffer.storageReads[storageKey] = struct{}{}
	if len(v) > 0 {
		m[seckey] = v
	} else {
		m[seckey] = nil
	}
	//fmt.Printf("WriteAccountStorage %x %x: %x, buffer %d\n", addrHash, seckey, value, len(tsw.tds.buffers))
	return nil
}

// ExtractWitness produces block witness for the block just been processed, in a serialised form
func (tds *TrieDbState) ExtractWitness(trace bool, isBinary bool) (*trie.Witness, error) {
	rs := tds.retainListBuilder.Build(isBinary)

	return tds.makeBlockWitness(trace, rs, isBinary)
}

// ExtractWitness produces block witness for the block just been processed, in a serialised form
func (tds *TrieDbState) ExtractWitnessForPrefix(prefix []byte, trace bool, isBinary bool) (*trie.Witness, error) {
	rs := tds.retainListBuilder.Build(isBinary)

	return tds.makeBlockWitnessForPrefix(prefix, trace, rs, isBinary)
}

func (tds *TrieDbState) makeBlockWitnessForPrefix(prefix []byte, trace bool, rl trie.RetainDecider, isBinary bool) (*trie.Witness, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	t := tds.t
	if isBinary {
		t = trie.HexToBin(tds.t).Trie()
	}

	return t.ExtractWitnessForPrefix(prefix, trace, rl)
}

func (tds *TrieDbState) makeBlockWitness(trace bool, rl trie.RetainDecider, isBinary bool) (*trie.Witness, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	t := tds.t
	if isBinary {
		t = trie.HexToBin(tds.t).Trie()
	}

	return t.ExtractWitness(trace, rl)
}

func (tsw *TrieStateWriter) CreateContract(address common.Address) error {
	addrHash, err := tsw.tds.pw.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	tsw.tds.currentBuffer.created[addrHash] = struct{}{}
	tsw.tds.currentBuffer.accountReads[addrHash] = struct{}{}
	delete(tsw.tds.currentBuffer.storageUpdates, addrHash)
	delete(tsw.tds.currentBuffer.storageIncarnation, addrHash)
	return nil
}

func (tds *TrieDbState) TriePruningDebugDump() string {
	return tds.tp.DebugDump()
}

func (tds *TrieDbState) getBlockNr() uint64 {
	return atomic.LoadUint64(&tds.blockNr)
}

func (tds *TrieDbState) setBlockNr(n uint64) {
	atomic.StoreUint64(&tds.blockNr, n)
}

// GetNodeByHash gets node's RLP by hash.
func (tds *TrieDbState) GetNodeByHash(hash common.Hash) []byte {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	return tds.t.GetNodeByHash(hash)
}

func (tds *TrieDbState) GetTrieHash() common.Hash {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	return tds.t.Hash()
}
