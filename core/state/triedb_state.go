package state

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
	witnesstypes "github.com/erigontech/erigon/execution/types/witness"
)

// Buffer is a structure holding updates, deletes, and reads registered within one change period
// A change period can be transaction within a block, or a block within group of blocks
type Buffer struct {
	codeReads     map[common.Hash]witnesstypes.CodeWithHash
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
	storageReads map[common.StorageKey][]byte
	// accountUpdates structure collects the effects of the block (or transaxction) execution.
	accountUpdates map[common.Hash]witnesstypes.AccountWithAddress
	// accountReads structure collects all the address hashes of the accounts that have been modified (or also just read,
	// if tds.resolveReads flag is turned on, which happens during the generation of block witnesses).
	accountReads            map[common.Hash]common.Address
	accountReadsIncarnation map[common.Hash]uint64
	deleted                 map[common.Hash]common.Address
	created                 map[common.Hash]common.Address
}

// Prepares buffer for work or clears previous data
func (b *Buffer) initialise() {
	b.codeReads = make(map[common.Hash]witnesstypes.CodeWithHash)
	b.codeSizeReads = make(map[common.Hash]common.Hash)
	b.codeUpdates = make(map[common.Hash][]byte)
	b.storageUpdates = make(map[common.Hash]map[common.Hash][]byte)
	b.storageIncarnation = make(map[common.Hash]uint64)
	b.storageReads = make(map[common.StorageKey][]byte)
	b.accountUpdates = make(map[common.Hash]witnesstypes.AccountWithAddress)
	b.accountReads = make(map[common.Hash]common.Address)
	b.accountReadsIncarnation = make(map[common.Hash]uint64)
	b.deleted = make(map[common.Hash]common.Address)
	b.created = make(map[common.Hash]common.Address)
}

// Replaces account pointer with pointers to the copies
func (b *Buffer) detachAccounts() {
	for addrHash, accountWithAddress := range b.accountUpdates {
		address := accountWithAddress.Address
		account := accountWithAddress.Account
		if account != nil {
			b.accountUpdates[addrHash] = witnesstypes.AccountWithAddress{Address: address, Account: account.SelfCopy()}
		}
	}
}

// Merges the content of another buffer into this one
func (b *Buffer) merge(other *Buffer) {
	for addrHash, codeWithHash := range other.codeReads {
		b.codeReads[addrHash] = codeWithHash
	}

	for addrHash, code := range other.codeUpdates {
		b.codeUpdates[addrHash] = code
	}

	for address, codeHash := range other.codeSizeReads {
		b.codeSizeReads[address] = codeHash
	}

	for addrHash := range other.deleted {
		b.deleted[addrHash] = other.deleted[addrHash]
		delete(b.storageUpdates, addrHash)
		delete(b.storageIncarnation, addrHash)
		delete(b.codeUpdates, addrHash)
	}
	for addrHash := range other.created {
		b.created[addrHash] = other.created[addrHash]
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
		b.storageReads[storageKey] = other.storageReads[storageKey]
	}
	for addrHash, accountAddr := range other.accountUpdates {
		b.accountUpdates[addrHash] = accountAddr
	}
	for addrHash := range other.accountReads {
		b.accountReads[addrHash] = other.accountReads[addrHash]
	}
	for addrHash, incarnation := range other.accountReadsIncarnation {
		b.accountReadsIncarnation[addrHash] = incarnation
	}
}

// TrieDbState implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                 *trie.Trie
	tMu               *sync.Mutex
	StateReader       StateReader
	rl                *trie.RetainList
	blockNr           uint64
	buffers           []*Buffer
	aggregateBuffer   *Buffer // Merge of all buffers
	currentBuffer     *Buffer
	resolveReads      bool
	retainListBuilder *trie.RetainListBuilder
	hashBuilder       *trie.HashBuilder
	incarnationMap    map[common.Address]uint64 // Temporary map of incarnation for the cases when contracts are deleted and recreated within 1 block
}

func NewTrieDbState(root common.Hash, blockNr uint64, stateReader StateReader) *TrieDbState {
	t := trie.New(root)
	tds := &TrieDbState{
		t:                 t,
		tMu:               new(sync.Mutex),
		StateReader:       stateReader,
		blockNr:           blockNr,
		retainListBuilder: trie.NewRetainListBuilder(),
		hashBuilder:       trie.NewHashBuilder(false),
		incarnationMap:    make(map[common.Address]uint64),
	}
	return tds
}

func (tds *TrieDbState) SetRetainList(rl *trie.RetainList) {
	tds.rl = rl
}

func (tds *TrieDbState) SetTrie(tr *trie.Trie) {
	tds.t = tr
}

func (tds *TrieDbState) SetResolveReads(rr bool) {
	tds.resolveReads = rr
}

func (tds *TrieDbState) Copy() *TrieDbState {
	tds.tMu.Lock()
	tcopy := *tds.t
	tds.tMu.Unlock()

	n := tds.getBlockNr()
	cpy := TrieDbState{
		t:              &tcopy,
		tMu:            new(sync.Mutex),
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
		t:                 tds.t,
		tMu:               tds.tMu,
		blockNr:           tds.getBlockNr(),
		buffers:           buffers,
		aggregateBuffer:   aggregateBuffer,
		currentBuffer:     currentBuffer,
		resolveReads:      tds.resolveReads,
		retainListBuilder: tds.retainListBuilder,
		hashBuilder:       trie.NewHashBuilder(false),
		incarnationMap:    make(map[common.Address]uint64),
	}
	tds.tMu.Unlock()

	return t
}

func (tds *TrieDbState) WithLastBuffer() *TrieDbState {
	tds.tMu.Lock()
	aggregateBuffer := &Buffer{}
	aggregateBuffer.initialise()
	currentBuffer := tds.currentBuffer
	buffers := []*Buffer{currentBuffer}
	tds.tMu.Unlock()

	return &TrieDbState{
		t:                 tds.t,
		tMu:               tds.tMu,
		blockNr:           tds.getBlockNr(),
		buffers:           buffers,
		aggregateBuffer:   aggregateBuffer,
		currentBuffer:     currentBuffer,
		resolveReads:      tds.resolveReads,
		retainListBuilder: tds.retainListBuilder.Copy(),
		hashBuilder:       trie.NewHashBuilder(false),
		incarnationMap:    make(map[common.Address]uint64),
	}
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
	tds.ClearUpdates()
	return roots, err
}

func (tds *TrieDbState) PrintTrie(w io.Writer) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	tds.t.Print(w)
}

func (tds *TrieDbState) buildPlainStorageReads() ([][]byte, [][]byte) {
	storagePlainKeys := make([][]byte, 0, len(tds.aggregateBuffer.storageReads))
	storageHashedKeys := make([][]byte, 0, len(tds.aggregateBuffer.storageReads))

	for storageHashedKey, storagePlainKey := range tds.aggregateBuffer.storageReads {
		// to prevent variable capture in Go 1.21
		storagePlainKeyCopy := make([]byte, len(storagePlainKey))
		copy(storagePlainKeyCopy, storagePlainKey)
		storageHashedKeyCopy := make([]byte, len(storageHashedKey))
		copy(storageHashedKeyCopy, storageHashedKey[:])
		storagePlainKeys = append(storagePlainKeys, storagePlainKeyCopy)
		storageHashedKeys = append(storageHashedKeys, storageHashedKeyCopy)
	}

	// Create a slice of indices to track original positions
	indices := make([]int, len(storagePlainKeys))
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on accountAddresses
	sort.SliceStable(indices, func(i, j int) bool {
		return bytes.Compare(storagePlainKeys[indices[i]], storagePlainKeys[indices[j]]) < 0
	})

	// Apply the sorted order to accountAddresses and accountAddressHashes
	sortedStoragePlainKeys := make([][]byte, len(storagePlainKeys))
	sortedStorageHashedKeys := make([][]byte, len(storageHashedKeys))
	for i, idx := range indices {
		sortedStoragePlainKeys[i] = storagePlainKeys[idx]
		sortedStorageHashedKeys[i] = storageHashedKeys[idx]
	}
	return sortedStorageHashedKeys, sortedStoragePlainKeys
}

// BuildStorageReads builds a sorted list of all storage key hashes that were modified
// (or also just read, if tds.resolveReads flag is turned on) within the
// period for which we are aggregating updates. It includes the keys of items that
// were nullified by subsequent updates - best example is the
// self-destruction of a contract, which nullifies all previous
// modifications of the contract's storage. In such case, all previously modified storage
// item updates would be inclided.
func (tds *TrieDbState) BuildStorageReads() common.StorageKeys {
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

// Populate pending block proof so that it will be sufficient for accessing all storage slots in storageTouches
func (tds *TrieDbState) PopulateStorageBlockProof(storageTouches common.StorageKeys) error { //nolint
	for _, storageKey := range storageTouches {
		addr, _, hash := dbutils.ParseCompositeStorageKey(storageKey[:])
		key := dbutils.GenerateCompositeTrieKey(addr, hash)
		tds.retainListBuilder.AddStorageTouch(key)
	}
	return nil
}

func (tds *TrieDbState) BuildCodeTouches() map[common.Hash]witnesstypes.CodeWithHash {
	return tds.aggregateBuffer.codeReads
}

func (tds *TrieDbState) buildCodeSizeTouches() map[common.Hash]common.Hash {
	return tds.aggregateBuffer.codeSizeReads
}

// BuildAccountReads builds a sorted list of all address hashes that were modified
// (or also just read, if tds.resolveReads flags is turned one) within the
// period for which we are aggregating update
func (tds *TrieDbState) BuildAccountReads() common.Hashes {
	accountTouches := common.Hashes{}
	for addrHash := range tds.aggregateBuffer.accountReads {
		accountTouches = append(accountTouches, addrHash)
	}
	sort.Sort(accountTouches)
	return accountTouches
}

func (tds *TrieDbState) buildAccountAddressReads() ([][]byte, [][]byte) {
	accountAddressHashes := make([][]byte, 0, len(tds.aggregateBuffer.accountReads))
	accountAddresses := make([][]byte, 0, len(tds.aggregateBuffer.accountReads))
	for addrHash, address := range tds.aggregateBuffer.accountReads {
		computedAddrHash := crypto.Keccak256(address.Bytes())
		if !bytes.Equal(addrHash[:], computedAddrHash) {
			panic("could not reproduce addrHash found in the map")
		}
		accountAddresses = append(accountAddresses, address.Bytes())
		accountAddressHashes = append(accountAddressHashes, addrHash.Bytes())
	}

	// Create a slice of indices to track original positions
	indices := make([]int, len(accountAddresses))
	for i := range indices {
		indices[i] = i
	}

	// Sort indices based on accountAddresses
	sort.SliceStable(indices, func(i, j int) bool {
		return bytes.Compare(accountAddresses[indices[i]], accountAddresses[indices[j]]) < 0
	})

	// Apply the sorted order to accountAddresses and accountAddressHashes
	sortedAccountAddresses := make([][]byte, len(accountAddresses))
	sortedAccountAddressHashes := make([][]byte, len(accountAddressHashes))
	for i, idx := range indices {
		sortedAccountAddresses[i] = accountAddresses[idx]
		sortedAccountAddressHashes[i] = accountAddressHashes[idx]
	}

	// Check if sorting is correct
	for i := 0; i < len(sortedAccountAddresses); i++ {
		addrHash := sortedAccountAddressHashes[i]
		accountAddress := sortedAccountAddresses[i]
		computedHash := crypto.Keccak256(accountAddress)
		if !bytes.Equal(addrHash, computedHash) {
			panic("sorting is not correct, this should not happen")
		}
	}

	return sortedAccountAddressHashes, sortedAccountAddresses
}

// buildAccountWrites builds a sorted list of all address hashes that were modified within the
// period for which we are aggregating updates.
func (tds *TrieDbState) buildAccountWrites() (common.Hashes, []*accounts.Account, [][]byte) {
	accountTouches := common.Hashes{}
	for addrHash := range tds.aggregateBuffer.accountUpdates {
		if _, ok := tds.aggregateBuffer.deleted[addrHash]; ok {
			// This adds an extra entry that wipes out the storage of the accout in the stream
			accountTouches = append(accountTouches, addrHash)
		} else if _, ok1 := tds.aggregateBuffer.created[addrHash]; ok1 {
			// This adds an extra entry that wipes out the storage of the accout in the stream
			accountTouches = append(accountTouches, addrHash)
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
			if a.Account != nil {
				if _, ok := tds.aggregateBuffer.storageUpdates[addrHash]; ok {
					var ac accounts.Account
					ac.Copy(a.Account)
					ac.Root = trie.EmptyRoot
					a.Account = &ac
				}
			}
			aValues[i] = a.Account
			if code, ok := tds.aggregateBuffer.codeUpdates[addrHash]; ok {
				aCodes[i] = code
			}
		}
	}
	return accountTouches, aValues, aCodes
}

func (tds *TrieDbState) PopulateAccountBlockProof(accountTouches common.Hashes) {
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

func (tds *TrieDbState) GetRetainList() *trie.RetainList {
	return tds.retainListBuilder.Build(false)
}

// Get list of account and storage touches
// First come the account touches then the storage touches
func (tds *TrieDbState) GetTouchedPlainKeys() (plainKeys [][]byte, hashedKeys [][]byte) {
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
	accountHashTouches, accountAddressTouches := tds.buildAccountAddressReads()
	storageHashTouches, storagePlainKeyTouches := tds.buildPlainStorageReads()
	plainKeys = append(accountAddressTouches, storagePlainKeyTouches...)
	hashedKeys = append(accountHashTouches, storageHashTouches...)
	return plainKeys, hashedKeys

}

func (tds *TrieDbState) ResolveBuffer() {
	if tds.currentBuffer != nil {
		if tds.aggregateBuffer == nil {
			tds.aggregateBuffer = &Buffer{}
			tds.aggregateBuffer.initialise()
		}
		tds.aggregateBuffer.merge(tds.currentBuffer)
	}
}

// forward is `true` if the function is used to progress the state forward (by adding blocks)
// forward is `false` if the function is used to rewind the state (for reorgs, for example)
func (tds *TrieDbState) updateTrieRoots(forward bool) ([]common.Hash, error) {
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

		for addrHash, accountWithAddress := range b.accountUpdates {
			if accountWithAddress.Account != nil {
				//fmt.Println("updateTrieRoots b.accountUpdates", addrHash.String(), account.Incarnation)
				tds.t.UpdateAccount(addrHash[:], accountWithAddress.Account)
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

			if accountWithAddress, ok := b.accountUpdates[addrHash]; ok && accountWithAddress.Account != nil {
				ok, root := tds.t.DeepHash(addrHash[:])
				if ok {
					accountWithAddress.Account.Root = root
					//fmt.Printf("(b)Set %x root for addrHash %x\n", root, addrHash)
				} else {
					//fmt.Printf("(b)Set empty root for addrHash %x\n", addrHash)
					accountWithAddress.Account.Root = trie.EmptyRoot
				}
			}
		}
		roots[i] = tds.t.Hash()
	}

	return roots, nil
}

func (tds *TrieDbState) ClearUpdates() {
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

func (tds *TrieDbState) ReadAccountDataForDebug(address common.Address) (*accounts.Account, error) {
	return tds.ReadAccountData(address)
}

func (tds *TrieDbState) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var account *accounts.Account

	addrHash, err := common.HashData(address[:])
	if err != nil {
		return nil, err
	}

	account, ok := tds.GetAccount(addrHash)
	if !ok {
		account, err = tds.StateReader.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
	}

	if tds.resolveReads {
		tds.currentBuffer.accountReads[addrHash] = address
		if account != nil {
			tds.currentBuffer.accountReadsIncarnation[addrHash] = account.Incarnation
		}
	}
	return account, nil
}

func (tds *TrieDbState) ReadAccountStorage(address common.Address, key common.Hash) (uint256.Int, bool, error) {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))
	if tds.currentBuffer != nil {
		if _, ok := tds.currentBuffer.deleted[addrHash]; ok {
			return uint256.Int{}, false, nil
		}
	}
	if tds.aggregateBuffer != nil {
		if _, ok := tds.aggregateBuffer.deleted[addrHash]; ok {
			return uint256.Int{}, false, nil
		}
	}
	seckey, err := common.HashData(key.Bytes())
	if err != nil {
		return uint256.Int{}, false, err
	}

	storagePlainKey := dbutils.GenerateStoragePlainKey(address, key)

	if tds.resolveReads {
		var storageKey common.StorageKey
		copy(storageKey[:], dbutils.GenerateCompositeStorageKey(addrHash, 1, seckey))
		tds.currentBuffer.storageReads[storageKey] = storagePlainKey
	}

	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	enc, ok := tds.t.Get(dbutils.GenerateCompositeTrieKey(addrHash, seckey))
	if !ok {
		enc, ok, err := tds.StateReader.ReadAccountStorage(address, key)
		if err != nil {
			return uint256.Int{}, false, err
		}
		return enc, ok, nil
	}

	var res uint256.Int
	(&res).SetBytes(enc)
	return res, true, nil
}

func (tds *TrieDbState) HasStorage(address common.Address) (bool, error) {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))
	// check if we know about any storage updates with non-empty values
	for _, v := range tds.currentBuffer.storageUpdates[addrHash] {
		if len(v) > 0 {
			return true, nil
		}
	}
	// fallback to underlying state reader if we don't know of non-empty storage slots yet
	return tds.StateReader.HasStorage(address)
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

func (tds *TrieDbState) ReadAccountCode(address common.Address) (code []byte, err error) {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))

	if cached, ok := tds.readAccountCodeFromTrie(addrHash[:]); ok {
		code, err = cached, nil
	} else {
		code, err = tds.StateReader.ReadAccountCode(address)
	}
	if tds.resolveReads {
		addrHash, err1 := common.HashData(address[:])
		if err1 != nil {
			return nil, err
		}
		tds.currentBuffer.accountReads[addrHash] = address
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		codeHash := crypto.Keccak256Hash(code)
		tds.currentBuffer.codeReads[addrHash] = witnesstypes.CodeWithHash{Code: code, CodeHash: codeHash}
		tds.retainListBuilder.ReadCode(codeHash, code)
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(address common.Address) (codeSize int, err error) {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))
	if cached, ok := tds.readAccountCodeSizeFromTrie(addrHash[:]); ok {
		return cached, nil
	} else {
		codeSize, err = tds.StateReader.ReadAccountCodeSize(address)
		if err != nil {
			return 0, err
		}
	}
	if tds.resolveReads {
		// We will need to read the code explicitly to make sure code is in the witness
		code, err := tds.ReadAccountCode(address)
		if err != nil {
			return 0, err
		}

		codeHash := crypto.Keccak256Hash(code)

		addrHash, err1 := common.HashData(address[:])
		if err1 != nil {
			return 0, err1
		}
		tds.currentBuffer.accountReads[addrHash] = address
		// we have to be careful, because the code might change
		// during the block executuion, so we are always
		// storing the latest code hash
		tds.currentBuffer.codeSizeReads[addrHash] = codeHash
		// FIXME: support codeSize in witnesses if makes sense
		tds.retainListBuilder.ReadCode(codeHash, code)
	}
	return codeSize, nil
}

func (tds *TrieDbState) ReadAccountIncarnation(address common.Address) (uint64, error) {
	if inc, ok := tds.incarnationMap[address]; ok {
		return inc, nil
	}
	inc, err := tds.StateReader.ReadAccountIncarnation(address)
	if err != nil {
		return 0, err
	} else {
		return inc, nil
	}
}

type TrieStateWriter struct {
	tds *TrieDbState
}

func (tds *TrieDbState) TrieStateWriter() *TrieStateWriter {
	return &TrieStateWriter{tds: tds}
}

func (tsw *TrieStateWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))
	tsw.tds.currentBuffer.accountUpdates[addrHash] = witnesstypes.AccountWithAddress{Address: address, Account: account}
	tsw.tds.currentBuffer.accountReads[addrHash] = address
	if original != nil {
		tsw.tds.currentBuffer.accountReadsIncarnation[addrHash] = original.Incarnation
	}
	return nil
}

func (tsw *TrieStateWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))
	tsw.tds.currentBuffer.accountUpdates[addrHash] = witnesstypes.AccountWithAddress{Address: address, Account: original} // TODO: might be needed to use *AccountWithAddress to point to nil
	tsw.tds.currentBuffer.accountReads[addrHash] = address
	if original != nil {
		tsw.tds.currentBuffer.accountReadsIncarnation[addrHash] = original.Incarnation
	}
	delete(tsw.tds.currentBuffer.storageUpdates, addrHash)
	delete(tsw.tds.currentBuffer.storageIncarnation, addrHash)
	delete(tsw.tds.currentBuffer.codeUpdates, addrHash)
	tsw.tds.currentBuffer.deleted[addrHash] = address
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

func (tsw *TrieStateWriter) WriteAccountStorage(address common.Address, incarnation uint64, key common.Hash, original, value uint256.Int) error {
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))

	v := value.Bytes()
	m, ok := tsw.tds.currentBuffer.storageUpdates[addrHash]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.currentBuffer.storageUpdates[addrHash] = m
	}
	tsw.tds.currentBuffer.storageIncarnation[addrHash] = incarnation
	seckey, err := common.HashData(key.Bytes())
	if err != nil {
		return err
	}
	var storageKey common.StorageKey
	copy(storageKey[:], dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))

	storagePlainKey := dbutils.GenerateStoragePlainKey(address, key)
	tsw.tds.currentBuffer.storageReads[storageKey] = storagePlainKey
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
	addrHash := common.Hash(crypto.Keccak256(address.Bytes()))
	tsw.tds.currentBuffer.created[addrHash] = address
	tsw.tds.currentBuffer.accountReads[addrHash] = address
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

func (tds *TrieDbState) GetTrieHash() common.Hash {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()
	return tds.t.Hash()
}
