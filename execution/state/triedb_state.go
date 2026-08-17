package state

import (
	"bytes"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/commitment/trie"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Buffer holds updates/deletes/reads for one change period — a tx within a block, or a block within a group.
type Buffer struct {
	codeReads     map[common.Hash]witnesstypes.CodeWithHash
	codeSizeReads map[common.Hash]common.Hash
	codeUpdates   map[common.Hash][]byte
	// storageUpdates holds only the final effect; a self-destructed contract's prior writes are dropped as irrelevant.
	storageUpdates     map[common.Hash]map[common.Hash][]byte
	storageIncarnation map[common.Hash]uint64
	// storageReads keeps every touched key, including ones storageUpdates drops after a self-destruct, to replay trie changes in order.
	storageReads            map[common.StorageKey][]byte
	accountUpdates          map[common.Hash]witnesstypes.AccountWithAddress
	accountReads            map[common.Hash]accounts.Address
	accountReadsIncarnation map[common.Hash]uint64
	deleted                 map[common.Hash]accounts.Address
	created                 map[common.Hash]accounts.Address
}

func (b *Buffer) initialise() {
	b.codeReads = make(map[common.Hash]witnesstypes.CodeWithHash)
	b.codeSizeReads = make(map[common.Hash]common.Hash)
	b.codeUpdates = make(map[common.Hash][]byte)
	b.storageUpdates = make(map[common.Hash]map[common.Hash][]byte)
	b.storageIncarnation = make(map[common.Hash]uint64)
	b.storageReads = make(map[common.StorageKey][]byte)
	b.accountUpdates = make(map[common.Hash]witnesstypes.AccountWithAddress)
	b.accountReads = make(map[common.Hash]accounts.Address)
	b.accountReadsIncarnation = make(map[common.Hash]uint64)
	b.deleted = make(map[common.Hash]accounts.Address)
	b.created = make(map[common.Hash]accounts.Address)
}

func (b *Buffer) detachAccounts() {
	for addrHash, accountWithAddress := range b.accountUpdates {
		address := accountWithAddress.Address
		account := accountWithAddress.Account
		if account != nil {
			b.accountUpdates[addrHash] = witnesstypes.AccountWithAddress{Address: address, Account: account.SelfCopy()}
		}
	}
}

func (b *Buffer) merge(other *Buffer) {
	maps.Copy(b.codeReads, other.codeReads)

	maps.Copy(b.codeUpdates, other.codeUpdates)

	maps.Copy(b.codeSizeReads, other.codeSizeReads)

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
		maps.Copy(m, om)
	}
	maps.Copy(b.storageIncarnation, other.storageIncarnation)
	for storageKey := range other.storageReads {
		b.storageReads[storageKey] = other.storageReads[storageKey]
	}
	maps.Copy(b.accountUpdates, other.accountUpdates)
	for addrHash := range other.accountReads {
		b.accountReads[addrHash] = other.accountReads[addrHash]
	}
	maps.Copy(b.accountReadsIncarnation, other.accountReadsIncarnation)
}

// TrieDbState implements StateReader by wrapping a trie and a database, where trie acts as a cache for the database
type TrieDbState struct {
	t                 *trie.Trie
	tMu               *sync.Mutex
	StateReader       StateReader
	rl                *trie.RetainList
	blockNr           uint64
	buffers           []*Buffer
	aggregateBuffer   *Buffer
	currentBuffer     *Buffer
	resolveReads      bool
	retainListBuilder *trie.RetainListBuilder
	incarnationMap    map[accounts.Address]uint64 // tracks incarnation across a delete+recreate of the same contract within one block
}

func NewTrieDbState(root common.Hash, blockNr uint64, stateReader StateReader) *TrieDbState {
	t := trie.New(root)
	tds := &TrieDbState{
		t:                 t,
		tMu:               new(sync.Mutex),
		StateReader:       stateReader,
		blockNr:           blockNr,
		retainListBuilder: trie.NewRetainListBuilder(),
		incarnationMap:    make(map[accounts.Address]uint64),
	}
	return tds
}

func (tds *TrieDbState) SetRetainList(rl *trie.RetainList) {
	tds.rl = rl
}

func (tds *TrieDbState) SetTrie(tr *trie.Trie) {
	tds.t = tr
}

func (tds *TrieDbState) SetTrace(_ bool, _ string) {}
func (tds *TrieDbState) Trace() bool               { return false }
func (tds *TrieDbState) TracePrefix() string       { return "" }

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
		incarnationMap: make(map[accounts.Address]uint64),
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
		incarnationMap:    make(map[accounts.Address]uint64),
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
		incarnationMap:    make(map[accounts.Address]uint64),
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

// UpdateStateTrie assumes the trie is already fully resolved.
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
		storagePlainKeyCopy := make([]byte, len(storagePlainKey))
		copy(storagePlainKeyCopy, storagePlainKey)
		storageHashedKeyCopy := make([]byte, len(storageHashedKey))
		copy(storageHashedKeyCopy, storageHashedKey[:])
		storagePlainKeys = append(storagePlainKeys, storagePlainKeyCopy)
		storageHashedKeys = append(storageHashedKeys, storageHashedKeyCopy)
	}

	indices := make([]int, len(storagePlainKeys))
	for i := range indices {
		indices[i] = i
	}

	slices.SortStableFunc(indices, func(a, b int) int {
		return bytes.Compare(storagePlainKeys[a], storagePlainKeys[b])
	})

	sortedStoragePlainKeys := make([][]byte, len(storagePlainKeys))
	sortedStorageHashedKeys := make([][]byte, len(storageHashedKeys))
	for i, idx := range indices {
		sortedStoragePlainKeys[i] = storagePlainKeys[idx]
		sortedStorageHashedKeys[i] = storageHashedKeys[idx]
	}
	return sortedStorageHashedKeys, sortedStoragePlainKeys
}

// BuildStorageReads returns all touched storage key hashes, sorted.
func (tds *TrieDbState) BuildStorageReads() common.StorageKeys {
	storageTouches := make(common.StorageKeys, 0, len(tds.aggregateBuffer.storageReads))
	for storageKey := range tds.aggregateBuffer.storageReads {
		storageTouches = append(storageTouches, storageKey)
	}
	storageTouches.Sort()
	return storageTouches
}

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

func (tds *TrieDbState) BuildAccountReads() common.Hashes {
	accountTouches := make(common.Hashes, 0, len(tds.aggregateBuffer.accountReads))
	for addrHash := range tds.aggregateBuffer.accountReads {
		accountTouches = append(accountTouches, addrHash)
	}
	accountTouches.Sort()
	return accountTouches
}

func (tds *TrieDbState) buildAccountAddressReads() ([][]byte, [][]byte) {
	accountAddressHashes := make([][]byte, 0, len(tds.aggregateBuffer.accountReads))
	accountAddresses := make([][]byte, 0, len(tds.aggregateBuffer.accountReads))
	for addrHash, address := range tds.aggregateBuffer.accountReads {
		addressValue := address.Value()
		computedAddrHash := crypto.Keccak256(addressValue[:])
		if !bytes.Equal(addrHash[:], computedAddrHash) {
			panic("could not reproduce addrHash found in the map")
		}
		accountAddresses = append(accountAddresses, addressValue[:])
		accountAddressHashes = append(accountAddressHashes, addrHash[:])
	}

	indices := make([]int, len(accountAddresses))
	for i := range indices {
		indices[i] = i
	}

	slices.SortStableFunc(indices, func(a, b int) int {
		return bytes.Compare(accountAddresses[a], accountAddresses[b])
	})

	sortedAccountAddresses := make([][]byte, len(accountAddresses))
	sortedAccountAddressHashes := make([][]byte, len(accountAddressHashes))
	for i, idx := range indices {
		sortedAccountAddresses[i] = accountAddresses[idx]
		sortedAccountAddressHashes[i] = accountAddressHashes[idx]
	}

	for i := range sortedAccountAddresses {
		addrHash := sortedAccountAddressHashes[i]
		accountAddress := sortedAccountAddresses[i]
		computedHash := crypto.Keccak256(accountAddress)
		if !bytes.Equal(addrHash, computedHash) {
			panic("sorting is not correct, this should not happen")
		}
	}

	return sortedAccountAddressHashes, sortedAccountAddresses
}

func (tds *TrieDbState) PopulateAccountBlockProof(accountTouches common.Hashes) {
	for _, addrHash := range accountTouches {
		a := addrHash
		tds.retainListBuilder.AddTouch(a[:])
	}
}

// ExtractTouches returns account and storage keys touched since the last call.
func (tds *TrieDbState) ExtractTouches() (accountTouches [][]byte, storageTouches [][]byte) {
	return tds.retainListBuilder.ExtractTouches()
}

func (tds *TrieDbState) GetRetainList() *trie.RetainList {
	return tds.retainListBuilder.Build(false)
}

// GetTouchedPlainKeys returns account touches before storage touches in both slices.
func (tds *TrieDbState) GetTouchedPlainKeys() (plainKeys [][]byte, hashedKeys [][]byte) {
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
	plainKeys = accountAddressTouches
	plainKeys = append(plainKeys, storagePlainKeyTouches...)
	hashedKeys = accountHashTouches
	hashedKeys = append(hashedKeys, storageHashTouches...)
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

// forward is false when rewinding the state during a reorg; per-buffer roots populate receipt.PostState on pre-Byzantium chains.
func (tds *TrieDbState) updateTrieRoots(forward bool) ([]common.Hash, error) {
	roots := make([]common.Hash, len(tds.buffers))
	for i, b := range tds.buffers {
		for addrHash := range b.deleted {
			// DeleteSubtree clears the storage sub-trie but keeps the accountNode, unlike Delete.
			tds.t.DeleteSubtree(addrHash[:])
		}
		for addrHash := range b.created {
			tds.t.DeleteSubtree(addrHash[:])
		}

		for addrHash, accountWithAddress := range b.accountUpdates {
			if accountWithAddress.Account != nil {
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
					if forward {
						tds.t.Update(cKey, v)
					} else {
						// Rewinding past a self-destruct can leave only a hashNode, so probe with Get before Update/Delete.
						if _, ok := tds.t.Get(cKey); ok {
							tds.t.Update(cKey, v)
						}
					}
				} else {
					if forward {
						tds.t.Delete(cKey)
					} else {
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
				} else {
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

func (tds *TrieDbState) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return tds.ReadAccountData(address)
}

func (tds *TrieDbState) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	var account *accounts.Account
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])

	account, ok := tds.GetAccount(addrHash)
	if !ok {
		var err error
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

func (tds *TrieDbState) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
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
	keyValue := key.Value()
	seckey := crypto.Keccak256Hash(keyValue[:])

	storagePlainKey := dbutils.GenerateStoragePlainKey(addressValue, keyValue)

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

func (tds *TrieDbState) HasStorage(address accounts.Address) (bool, error) {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
	for _, v := range tds.currentBuffer.storageUpdates[addrHash] {
		if len(v) > 0 {
			return true, nil
		}
	}
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

func (tds *TrieDbState) ReadAccountCode(address accounts.Address) (code []byte, err error) {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])

	if cached, ok := tds.readAccountCodeFromTrie(addrHash[:]); ok {
		code, err = cached, nil
	} else {
		code, err = tds.StateReader.ReadAccountCode(address)
	}
	if tds.resolveReads {
		tds.currentBuffer.accountReads[addrHash] = address
		// Recomputes the hash each time since code can change mid-block.
		codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
		tds.currentBuffer.codeReads[addrHash] = witnesstypes.CodeWithHash{Code: code, CodeHash: codeHash}
		tds.retainListBuilder.ReadCode(codeHash, code)
	}
	return code, err
}

func (tds *TrieDbState) ReadAccountCodeSize(address accounts.Address) (codeSize int, err error) {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
	if cached, ok := tds.readAccountCodeSizeFromTrie(addrHash[:]); ok {
		return cached, nil
	} else {
		codeSize, err = tds.StateReader.ReadAccountCodeSize(address)
		if err != nil {
			return 0, err
		}
	}
	if tds.resolveReads {
		// Reads the code explicitly (discarding it) so the code itself ends up in the witness too.
		code, err := tds.ReadAccountCode(address)
		if err != nil {
			return 0, err
		}

		codeHash := crypto.Keccak256Hash(code)

		tds.currentBuffer.accountReads[addrHash] = address
		tds.currentBuffer.codeSizeReads[addrHash] = codeHash
		tds.retainListBuilder.ReadCode(accounts.InternCodeHash(codeHash), code)
	}
	return codeSize, nil
}

func (tds *TrieDbState) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
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

func (tsw *TrieStateWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
	tsw.tds.currentBuffer.accountUpdates[addrHash] = witnesstypes.AccountWithAddress{Address: addressValue, Account: account}
	tsw.tds.currentBuffer.accountReads[addrHash] = address
	if original != nil {
		tsw.tds.currentBuffer.accountReadsIncarnation[addrHash] = original.Incarnation
	}
	return nil
}

func (tsw *TrieStateWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
	tsw.tds.currentBuffer.accountUpdates[addrHash] = witnesstypes.AccountWithAddress{Address: addressValue, Account: original}
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

func (tsw *TrieStateWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	if tsw.tds.resolveReads {
		tsw.tds.retainListBuilder.CreateCode(codeHash)
	}
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
	tsw.tds.currentBuffer.codeUpdates[addrHash] = code
	return nil
}

func (tsw *TrieStateWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])

	v := value.Bytes()
	m, ok := tsw.tds.currentBuffer.storageUpdates[addrHash]
	if !ok {
		m = make(map[common.Hash][]byte)
		tsw.tds.currentBuffer.storageUpdates[addrHash] = m
	}
	tsw.tds.currentBuffer.storageIncarnation[addrHash] = incarnation
	keyValue := key.Value()
	seckey := crypto.Keccak256Hash(keyValue[:])
	var storageKey common.StorageKey
	copy(storageKey[:], dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey))

	storagePlainKey := dbutils.GenerateStoragePlainKey(addressValue, keyValue)
	tsw.tds.currentBuffer.storageReads[storageKey] = storagePlainKey
	m[seckey] = v
	return nil
}

func (tds *TrieDbState) ExtractWitness(trace bool, isBinary bool) (*trie.Witness, error) {
	rs := tds.retainListBuilder.Build(isBinary)

	return tds.makeBlockWitness(trace, rs, isBinary)
}

func (tds *TrieDbState) ExtractWitnessForPrefix(prefix []byte, trace bool, isBinary bool) (*trie.Witness, error) {
	rs := tds.retainListBuilder.Build(isBinary)

	return tds.makeBlockWitnessForPrefix(prefix, trace, rs, isBinary)
}

func (tds *TrieDbState) makeBlockWitnessForPrefix(prefix []byte, trace bool, rl trie.RetainDecider, isBinary bool) (*trie.Witness, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	t := tds.t

	return t.ExtractWitnessForPrefix(prefix, trace, rl)
}

func (tds *TrieDbState) makeBlockWitness(trace bool, rl trie.RetainDecider, isBinary bool) (*trie.Witness, error) {
	tds.tMu.Lock()
	defer tds.tMu.Unlock()

	t := tds.t

	return t.ExtractWitness(trace, rl)
}

func (tsw *TrieStateWriter) CreateContract(address accounts.Address) error {
	addressValue := address.Value()
	addrHash := crypto.Keccak256Hash(addressValue[:])
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
