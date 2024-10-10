package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/length"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/holiman/uint256"
	eriCommon "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

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
	storageReads map[eriCommon.StorageKey]struct{}
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
	b.storageReads = make(map[eriCommon.StorageKey]struct{})
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
	t               *trie.Trie
	tMu             *sync.Mutex
	db              kv.Tx
	stateReader     StateReader
	rl              *trie.RetainList
	blockNr         uint64
	buffers         []*Buffer
	aggregateBuffer *Buffer // Merge of all buffers
	currentBuffer   *Buffer
	historical      bool
	noHistory       bool
	resolveReads    bool
	newStream       trie.Stream
	hashBuilder     *trie.HashBuilder
	loader          *trie.SubTrieLoader
	incarnationMap  map[common.Address]uint64 // Temporary map of incarnation for the cases when contracts are deleted and recreated within 1 block

	preimageMap map[common.Hash][]byte
}

func NewTrieDbState(root common.Hash, db kv.Tx, blockNr uint64, stateReader StateReader) *TrieDbState {
	t := trie.New(root)

	tds := &TrieDbState{
		t:              t,
		tMu:            new(sync.Mutex),
		db:             db,
		stateReader:    stateReader,
		blockNr:        blockNr,
		hashBuilder:    trie.NewHashBuilder(false),
		incarnationMap: make(map[common.Address]uint64),
		preimageMap:    make(map[common.Hash][]byte),
	}

	return tds
}

func (tds *TrieDbState) SetRetainList(rl *trie.RetainList) {
	tds.rl = rl
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

func (tds *TrieDbState) SetStateReader(sr StateReader) {
	tds.stateReader = sr
}

func (tds *TrieDbState) Copy() *TrieDbState {
	tds.tMu.Lock()
	tcopy := *tds.t
	tds.tMu.Unlock()

	n := tds.getBlockNr()

	cpy := TrieDbState{
		t:              &tcopy,
		tMu:            new(sync.Mutex),
		db:             tds.db,
		blockNr:        n,
		hashBuilder:    trie.NewHashBuilder(false),
		incarnationMap: make(map[common.Address]uint64),
	}

	return &cpy
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
		t:               tds.t,
		tMu:             tds.tMu,
		db:              tds.db,
		blockNr:         tds.getBlockNr(),
		buffers:         buffers,
		aggregateBuffer: aggregateBuffer,
		currentBuffer:   currentBuffer,
		historical:      tds.historical,
		noHistory:       tds.noHistory,
		resolveReads:    tds.resolveReads,
		hashBuilder:     trie.NewHashBuilder(false),
		incarnationMap:  make(map[common.Address]uint64),
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
func (tds *TrieDbState) buildStorageReads() eriCommon.StorageKeys {
	storageTouches := eriCommon.StorageKeys{}
	for storageKey := range tds.aggregateBuffer.storageReads {
		storageTouches = append(storageTouches, storageKey)
	}
	sort.Sort(storageTouches)
	return storageTouches
}

func buildStorageKey(address common.Address, incarnation uint64, slot common.Hash) eriCommon.StorageKey {
	var storageKey eriCommon.StorageKey
	copy(storageKey[:], address.Bytes())
	binary.BigEndian.PutUint64(storageKey[length.Hash:], incarnation)
	copy(storageKey[length.Hash+length.Incarnation:], slot.Bytes())
	return storageKey
}

// buildStorageWrites builds a sorted list of all storage key hashes that were modified within the
// period for which we are aggregating updates. It skips the updates that
// were nullified by subsequent updates - best example is the
// self-destruction of a contract, which nullifies all previous
// modifications of the contract's storage. In such case, no storage
// item updates would be inclided.
func (tds *TrieDbState) buildStorageWrites() (eriCommon.StorageKeys, [][]byte) {
	storageTouches := eriCommon.StorageKeys{}
	for addrHash, m := range tds.aggregateBuffer.storageUpdates {
		for keyHash := range m {
			var storageKey eriCommon.StorageKey
			copy(storageKey[:], addrHash[:])
			binary.BigEndian.PutUint64(storageKey[length.Hash:], tds.aggregateBuffer.storageIncarnation[addrHash])
			copy(storageKey[length.Hash+length.Incarnation:], keyHash[:])
			storageTouches = append(storageTouches, storageKey)
		}
	}
	sort.Sort(storageTouches)
	var addrHash common.Hash
	var keyHash common.Hash
	var values = make([][]byte, len(storageTouches))
	for i, storageKey := range storageTouches {
		copy(addrHash[:], storageKey[:])
		copy(keyHash[:], storageKey[length.Hash+length.Incarnation:])
		values[i] = tds.aggregateBuffer.storageUpdates[addrHash][keyHash]
	}
	return storageTouches, values
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
func (tds *TrieDbState) buildAccountReads() eriCommon.Hashes {
	accountTouches := eriCommon.Hashes{}
	for addrHash := range tds.aggregateBuffer.accountReads {
		accountTouches = append(accountTouches, addrHash)
	}
	sort.Sort(accountTouches)
	return accountTouches
}

// buildAccountWrites builds a sorted list of all address hashes that were modified within the
// period for which we are aggregating updates.
func (tds *TrieDbState) buildAccountWrites() (eriCommon.Hashes, []*accounts.Account, [][]byte) {
	accountTouches := eriCommon.Hashes{}
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
	return trie.HashWithModifications(tds.t, accountKeys, aValues, aCodes, storageKeys, sValues, length.Hash+length.Incarnation, &tds.newStream, hb, trace)
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
}

func (tds *TrieDbState) GetBlockNr() uint64 {
	return tds.getBlockNr()
}

func (tds *TrieDbState) GetAccount(addrHash common.Hash) (*accounts.Account, bool) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	acc, ok := tds.t.GetAccount(addrHash[:])
	return acc, ok
}

func (tds *TrieDbState) HashSave(data []byte) (common.Hash, error) {
	cpy := make([]byte, len(data))
	copy(cpy, data)
	h, err := eriCommon.HashData(cpy)
	if err != nil {
		return common.Hash{}, err
	} else {
		h1 := common.Hash{}
		copy(h1[:], h[:])
		tds.preimageMap[h1] = cpy
		return h, nil
	}
}

func (tds *TrieDbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var account *accounts.Account

	addrHash, err := tds.HashSave(address[:])
	if err != nil {
		return nil, err
	}

	account, err = tds.stateReader.ReadAccountData(address)

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

func (tds *TrieDbState) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	addrHash, err := tds.HashSave(address[:])
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
	seckey, err := tds.HashSave(key[:])
	if err != nil {
		return nil, err
	}

	if tds.resolveReads {
		var storageKey eriCommon.StorageKey
		copy(storageKey[:], dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))
		tds.currentBuffer.storageReads[storageKey] = struct{}{}
	}

	if tds.stateReader != nil {
		enc, err := tds.stateReader.ReadAccountStorage(address, incarnation, key)

		if err != nil {
			return nil, err
		}

		return enc, nil
	} else {
		tds.tMu.Lock()
		defer tds.tMu.Unlock()
		enc, ok := tds.t.Get(dbutils.GenerateCompositeTrieKey(addrHash, seckey))
		if !ok {
			// Not present in the trie, try database
			enc, err = tds.db.GetOne(kv.HashedAccounts, dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))
			if err != nil {
				enc = nil
			}
		}

		return enc, nil
	}
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

func (tds *TrieDbState) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) (code []byte, err error) {
	if bytes.Equal(codeHash[:], emptyCodeHash) {
		return nil, nil
	}

	addrHash, err := tds.HashSave(address[:])
	if err != nil {
		return nil, err
	}

	if cached, ok := tds.readAccountCodeFromTrie(addrHash[:]); ok {
		code, err = cached, nil
	} else {
		if tds.stateReader != nil {
			code, err = tds.stateReader.ReadAccountCode(address, incarnation, codeHash)
		} else {
			code, err = tds.db.GetOne(kv.Code, codeHash[:])
		}
	}
	if tds.resolveReads {
		addrHash, err1 := tds.HashSave(address[:])
		if err1 != nil {
			return nil, err
		}
		tds.currentBuffer.accountReads[addrHash] = struct{}{}
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		tds.currentBuffer.codeReads[addrHash] = codeHash
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (codeSize int, err error) {
	addrHash, err := tds.HashSave(address[:])
	if err != nil {
		return 0, err
	}

	if cached, ok := tds.readAccountCodeSizeFromTrie(addrHash[:]); ok {
		codeSize, err = cached, nil
	} else {
		if tds.stateReader != nil {
			codeSize, err = tds.stateReader.ReadAccountCodeSize(address, incarnation, codeHash)
		} else {
			var code []byte
			code, err = tds.db.GetOne(kv.Code, codeHash[:])
			if err != nil {
				return 0, err
			}
			codeSize = len(code)
		}
	}
	if tds.resolveReads {
		addrHash, err1 := tds.HashSave(address[:])
		if err1 != nil {
			return 0, err
		}
		tds.currentBuffer.accountReads[addrHash] = struct{}{}
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		tds.currentBuffer.codeSizeReads[addrHash] = codeHash
	}
	return codeSize, nil
}

func (tds *TrieDbState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if inc, ok := tds.incarnationMap[address]; ok {
		return inc, nil
	}

	if tds.stateReader != nil {
		inc, err := tds.stateReader.ReadAccountIncarnation(address)
		if err != nil {
			return 0, err
		} else {
			return inc, nil
		}
	} else {
		if b, err := tds.db.GetOne(kv.IncarnationMap, address[:]); err == nil {
			if len(b) == 0 {
				return 0, nil
			}

			return binary.BigEndian.Uint64(b), nil
		} else if errors.Is(err, ethdb.ErrKeyNotFound) {
			return 0, nil
		} else {
			return 0, err
		}
	}
}

var prevMemStats runtime.MemStats

type TrieStateWriter struct {
	tds *TrieDbState
}

func (tds *TrieDbState) TrieStateWriter() *TrieStateWriter {
	return &TrieStateWriter{tds: tds}
}

func (tsw *TrieStateWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addrHash, err := tsw.tds.HashSave(address[:])
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

func (tsw *TrieStateWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	addrHash, err := tsw.tds.HashSave(address[:])
	if err != nil {
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
	addrHash, err := tsw.tds.HashSave(address.Bytes())
	if err != nil {
		return err
	}
	tsw.tds.currentBuffer.codeUpdates[addrHash] = code
	return nil
}

func (tsw *TrieStateWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	addrHash, err := tsw.tds.HashSave(address[:])
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

	// we need to clone the key as it's passed as a pointer
	cloneKey := make([]byte, len(*key))
	copy(cloneKey, key[:])
	seckey, err := tsw.tds.HashSave(cloneKey)
	if err != nil {
		return err
	}

	var storageKey eriCommon.StorageKey
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

func (tsw *TrieStateWriter) WriteChangeSets() error {
	return nil
}
func (tsw *TrieStateWriter) WriteHistory() error {
	return nil
}

func (tds *TrieDbState) makeBlockWitnessForPrefix(prefix []byte, trace bool, rl trie.RetainDecider, isBinary bool) (*trie.Witness, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	t := tds.t
	// if isBinary {
	// 	t = trie.HexToBin(tds.t).Trie()
	// }

	return t.ExtractWitnessForPrefix(prefix, trace, rl)
}

func (tds *TrieDbState) makeBlockWitness(trace bool, rl trie.RetainDecider, isBinary bool) (*trie.Witness, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	t := tds.t
	// if isBinary {
	// 	t = trie.HexToBin(tds.t).Trie()
	// }

	return t.ExtractWitness(trace, rl)
}

func (tsw *TrieStateWriter) CreateContract(address common.Address) error {
	addrHash, err := tsw.tds.HashSave(address[:])
	if err != nil {
		return err
	}
	tsw.tds.currentBuffer.created[addrHash] = struct{}{}
	tsw.tds.currentBuffer.accountReads[addrHash] = struct{}{}
	delete(tsw.tds.currentBuffer.storageUpdates, addrHash)
	delete(tsw.tds.currentBuffer.storageIncarnation, addrHash)
	return nil
}

func (tds *TrieDbState) getBlockNr() uint64 {
	return atomic.LoadUint64(&tds.blockNr)
}

func (tds *TrieDbState) setBlockNr(n uint64) {
	atomic.StoreUint64(&tds.blockNr, n)
}

// GetNodeByHash gets node's RLP by hash.
// func (tds *TrieDbState) GetNodeByHash(hash common.Hash) []byte {
// 	tds.tMu.Lock()
// 	defer tds.tMu.Unlock()

// 	return tds.t.GetNodeByHash(hash)
// }

func (tds *TrieDbState) GetTrieHash() common.Hash {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	return tds.t.Hash()
}

func (tds *TrieDbState) ResolveSMTRetainList() (*trie.RetainList, error) {
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

	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	// Prepare (resolve) storage tries so that actual modifications can proceed without database access
	storageTouches := tds.buildStorageReads()

	// Prepare (resolve) accounts trie so that actual modifications can proceed without database access
	accountTouches := tds.buildAccountReads()

	keys := make([][]int, 0)

	// Add account keys to the retain list
	for _, addrHash := range accountTouches {
		addr := common.BytesToAddress(tds.preimageMap[addrHash]).String()

		nonceKey := utils.KeyEthAddrNonce(addr)
		keys = append(keys, nonceKey.GetPath())

		balanceKey := utils.KeyEthAddrBalance(addr)
		keys = append(keys, balanceKey.GetPath())

		codeKey := utils.KeyContractCode(addr)
		keys = append(keys, codeKey.GetPath())

		codeLengthKey := utils.KeyContractLength(addr)
		keys = append(keys, codeLengthKey.GetPath())
	}

	getSMTPath := func(ethAddr string, key string) ([]int, error) {
		a := utils.ConvertHexToBigInt(ethAddr)
		addr := utils.ScalarToArrayBig(a)

		storageKey := utils.KeyContractStorage(addr, key)

		return storageKey.GetPath(), nil
	}

	for _, storageKey := range storageTouches {
		addrHash, _, keyHash := dbutils.ParseCompositeStorageKey(storageKey[:])

		ethAddr := common.BytesToAddress(tds.preimageMap[addrHash]).String()
		key := common.BytesToHash(tds.preimageMap[keyHash]).String()

		smtPath, err := getSMTPath(ethAddr, key)

		if err != nil {
			return nil, err
		}

		keys = append(keys, smtPath)
	}

	/*add 0x00...05ca1ab1e and GER manager values*/
	for _, slot := range []common.Hash{LAST_BLOCK_STORAGE_POS, STATE_ROOT_STORAGE_POS, TIMESTAMP_STORAGE_POS, BLOCK_INFO_ROOT_STORAGE_POS} {
		smtPath, err := getSMTPath(ADDRESS_SCALABLE_L2.String(), slot.String())
		if err != nil {
			return nil, err
		}
		keys = append(keys, smtPath)
	}

	for _, slot := range []common.Hash{GLOBAL_EXIT_ROOT_STORAGE_POS, GLOBAL_EXIT_ROOT_POS_1} {
		smtPath, err := getSMTPath(GER_MANAGER_ADDRESS.String(), slot.String())
		if err != nil {
			return nil, err
		}
		keys = append(keys, smtPath)
	}

	rl := trie.NewRetainList(0)

	for _, key := range keys {
		// Convert key to bytes
		keyBytes := make([]byte, 0, len(key))

		for _, v := range key {
			keyBytes = append(keyBytes, byte(v))
		}

		rl.AddHex(keyBytes)
	}

	return rl, nil
}
