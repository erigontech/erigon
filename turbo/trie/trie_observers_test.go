package trie

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/stretchr/testify/assert"
)

func genAccount() *accounts.Account {
	acc := &accounts.Account{}
	acc.Nonce = 123
	return acc
}

func genNKeys(n int) [][]byte {
	result := make([][]byte, n)
	for i := range result {
		result[i] = crypto.Keccak256([]byte{0x0, 0x0, 0x0, byte(i % 256), byte(i / 256)})
	}
	return result
}

func genByteArrayOfLen(n int) []byte {
	result := make([]byte, n)
	for i := range result {
		result[i] = byte(rand.Intn(255))
	}
	return result
}

type partialObserver struct {
	NoopObserver
	callbackCalled bool
}

func (o *partialObserver) BranchNodeDeleted(_ []byte) {
	o.callbackCalled = true
}

type mockObserver struct {
	createdNodes map[string]uint
	deletedNodes map[string]struct{}
	touchedNodes map[string]int

	unloadedNodes      map[string]int
	unloadedNodeHashes map[string][]byte
	reloadedNodes      map[string]int
}

func newMockObserver() *mockObserver {
	return &mockObserver{
		createdNodes:       make(map[string]uint),
		deletedNodes:       make(map[string]struct{}),
		touchedNodes:       make(map[string]int),
		unloadedNodes:      make(map[string]int),
		unloadedNodeHashes: make(map[string][]byte),
		reloadedNodes:      make(map[string]int),
	}
}

func (m *mockObserver) BranchNodeCreated(hex []byte) {
	m.createdNodes[common.Bytes2Hex(hex)] = 1
}

func (m *mockObserver) CodeNodeCreated(hex []byte, size uint) {
	m.createdNodes[common.Bytes2Hex(hex)] = size
}

func (m *mockObserver) BranchNodeDeleted(hex []byte) {
	m.deletedNodes[common.Bytes2Hex(hex)] = struct{}{}
}

func (m *mockObserver) CodeNodeDeleted(hex []byte) {
	m.deletedNodes[common.Bytes2Hex(hex)] = struct{}{}
}

func (m *mockObserver) BranchNodeTouched(hex []byte) {
	value := m.touchedNodes[common.Bytes2Hex(hex)]
	value++
	m.touchedNodes[common.Bytes2Hex(hex)] = value
}

func (m *mockObserver) CodeNodeTouched(hex []byte) {
	value := m.touchedNodes[common.Bytes2Hex(hex)]
	value++
	m.touchedNodes[common.Bytes2Hex(hex)] = value
}

func (m *mockObserver) CodeNodeSizeChanged(hex []byte, newSize uint) {
	m.createdNodes[common.Bytes2Hex(hex)] = newSize
}

func (m *mockObserver) WillUnloadBranchNode(hex []byte, hash common.Hash, incarnation uint64) {
}

func (m *mockObserver) WillUnloadNode(hex []byte, hash common.Hash) {
	dictKey := common.Bytes2Hex(hex)
	value := m.unloadedNodes[dictKey]
	value++
	m.unloadedNodes[dictKey] = value
	m.unloadedNodeHashes[dictKey] = common.CopyBytes(hash[:])
}

func (m *mockObserver) BranchNodeLoaded(hex []byte, incarnation uint64) {
	dictKey := common.Bytes2Hex(hex)
	value := m.reloadedNodes[dictKey]
	value++
	m.reloadedNodes[dictKey] = value
}

func TestObserversBranchNodesCreateDelete(t *testing.T) {
	trie := newEmpty()

	observer := newMockObserver()

	trie.AddObserver(observer)

	keys := genNKeys(100)

	for _, key := range keys {
		acc := genAccount()
		code := genByteArrayOfLen(100)
		codeHash := crypto.Keccak256(code)
		acc.CodeHash = common.BytesToHash(codeHash)
		trie.UpdateAccount(key, acc)
		trie.UpdateAccountCode(key, codeNode(code)) //nolint:errcheck
	}

	expectedNodes := calcSubtreeNodes(trie.root)

	assert.True(t, expectedNodes >= 100, "should register all code nodes")
	assert.Equal(t, expectedNodes, len(observer.createdNodes), "should register all")

	for _, key := range keys {
		trie.Delete(key)
	}

	assert.Equal(t, expectedNodes, len(observer.deletedNodes), "should register all")
}

func TestObserverCodeSizeChanged(t *testing.T) {
	rand.Seed(9999)

	trie := newEmpty()

	observer := newMockObserver()

	trie.AddObserver(observer)

	keys := genNKeys(10)

	for _, key := range keys {
		acc := genAccount()
		code := genByteArrayOfLen(100)
		codeHash := crypto.Keccak256(code)
		acc.CodeHash = common.BytesToHash(codeHash)
		trie.UpdateAccount(key, acc)
		trie.UpdateAccountCode(key, codeNode(code)) //nolint:errcheck

		hex := keybytesToHex(key)
		hex = hex[:len(hex)-1]
		newSize, ok := observer.createdNodes[common.Bytes2Hex(hex)]
		assert.True(t, ok, "account should be registed as created")
		assert.Equal(t, 100, int(newSize), "account size should increase when the account code grows")

		code2 := genByteArrayOfLen(50)
		codeHash2 := crypto.Keccak256(code2)
		acc.CodeHash = common.BytesToHash(codeHash2)
		trie.UpdateAccount(key, acc)
		trie.UpdateAccountCode(key, codeNode(code2)) //nolint:errcheck

		newSize2, ok := observer.createdNodes[common.Bytes2Hex(hex)]
		assert.True(t, ok, "account should be registed as created")
		assert.Equal(t, -50, int(newSize2)-int(newSize), "account size should decrease when the account code shrinks")
	}
}

func TestObserverUnloadStorageNodes(t *testing.T) {
	rand.Seed(9999)

	trie := newEmpty()

	observer := newMockObserver()

	trie.AddObserver(observer)

	key := genNKeys(1)[0]

	storageKeys := genNKeys(10)
	// group all storage keys into a single fullNode
	for i := range storageKeys {
		storageKeys[i][0] = byte(0)
		storageKeys[i][1] = byte(i)
	}

	fullKeys := make([][]byte, len(storageKeys))
	for i, storageKey := range storageKeys {
		fullKey := dbutils.GenerateCompositeTrieKey(common.BytesToHash(key), common.BytesToHash(storageKey))
		fullKeys[i] = fullKey
	}

	acc := genAccount()
	trie.UpdateAccount(key, acc)

	for i, fullKey := range fullKeys {
		trie.Update(fullKey, []byte(fmt.Sprintf("test-value-%d", i)))
	}

	rootHash := trie.Hash()

	// adding nodes doesn't add anything
	assert.Equal(t, 0, len(observer.reloadedNodes), "adding nodes doesn't add anything")

	// unloading nodes adds to the list

	hex := keybytesToHex(key)
	hex = hex[:len(hex)-1]
	hex = append(hex, 0x0, 0x0, 0x0)
	trie.EvictNode(hex)

	newRootHash := trie.Hash()
	assert.Equal(t, rootHash, newRootHash, "root hash shouldn't change")

	assert.Equal(t, 1, len(observer.unloadedNodes), "should unload one full node")

	hex = keybytesToHex(key)

	storageKey := fmt.Sprintf("%s000000", common.Bytes2Hex(hex[:len(hex)-1]))
	assert.Equal(t, 1, observer.unloadedNodes[storageKey], "should unload structure nodes")

	accNode, ok := trie.getAccount(trie.root, hex, 0)
	assert.True(t, ok, "account should be found")

	sn, ok := accNode.storage.(*shortNode)
	assert.True(t, ok, "storage should be the shortnode contaning hash")

	_, ok = sn.Val.(hashNode)
	assert.True(t, ok, "storage should be the shortnode contaning hash")
}

func TestObserverLoadNodes(t *testing.T) {
	rand.Seed(9999)

	subtrie := newEmpty()

	observer := newMockObserver()

	// this test needs a specific trie structure
	//                            ( full )
	//         (full)              (duo)               (duo)
	// (short)(short)(short)    (short)(short)      (short)(short)
	// (acc1) (acc2) (acc3)      (acc4) (acc5)       (acc6) (acc7)
	//
	// to ensure this structure we override prefixes of
	// random account keys with the follwing paths

	prefixes := [][]byte{
		{0x00, 0x00}, //acc1
		{0x00, 0x02}, //acc2
		{0x00, 0x05}, //acc3
		{0x02, 0x02}, //acc4
		{0x02, 0x05}, //acc5
		{0x0A, 0x00}, //acc6
		{0x0A, 0x03}, //acc7
	}

	keys := genNKeys(7)
	for i := range keys {
		copy(keys[i][:2], prefixes[i])
	}

	storageKeys := genNKeys(10)
	// group all storage keys into a single fullNode
	for i := range storageKeys {
		storageKeys[i][0] = byte(0)
		storageKeys[i][1] = byte(i)
	}

	for _, key := range keys {
		acc := genAccount()
		subtrie.UpdateAccount(key, acc)

		for i, storageKey := range storageKeys {
			fullKey := dbutils.GenerateCompositeTrieKey(common.BytesToHash(key), common.BytesToHash(storageKey))
			subtrie.Update(fullKey, []byte(fmt.Sprintf("test-value-%d", i)))
		}
	}

	trie := newEmpty()
	trie.AddObserver(observer)

	hash := subtrie.Hash()
	//nolint:errcheck
	trie.hook([]byte{}, subtrie.root, hash[:])

	// fullNode
	assert.Equal(t, 1, observer.reloadedNodes["000000"], "should reload structure nodes")
	// duoNode
	assert.Equal(t, 1, observer.reloadedNodes["000200"], "should reload structure nodes")
	// duoNode
	assert.Equal(t, 1, observer.reloadedNodes["000a00"], "should reload structure nodes")
	// root
	assert.Equal(t, 1, observer.reloadedNodes["00"], "should reload structure nodes")

	// check storages (should have a single fullNode per account)
	for _, key := range keys {
		hex := keybytesToHex(key)
		storageKey := fmt.Sprintf("%s000000", common.Bytes2Hex(hex[:len(hex)-1]))
		assert.Equal(t, 1, observer.reloadedNodes[storageKey], "should reload structure nodes")
	}
}

func TestObserverTouches(t *testing.T) {
	rand.Seed(9999)

	trie := newEmpty()

	observer := newMockObserver()

	trie.AddObserver(observer)

	keys := genNKeys(3)
	for i := range keys {
		keys[i][0] = 0x00 // 3 belong to the same branch
	}

	var acc *accounts.Account
	for _, key := range keys {
		// creation touches the account
		acc = genAccount()
		trie.UpdateAccount(key, acc)
	}

	key := keys[0]
	branchNodeHex := "0000"

	assert.Equal(t, uint(1), observer.createdNodes[branchNodeHex], "node is created")
	assert.Equal(t, 1, observer.touchedNodes[branchNodeHex])

	// updating touches the account
	code := genByteArrayOfLen(100)
	codeHash := crypto.Keccak256(code)
	acc.CodeHash = common.BytesToHash(codeHash)
	trie.UpdateAccount(key, acc)
	assert.Equal(t, 2, observer.touchedNodes[branchNodeHex])

	// updating code touches the account
	trie.UpdateAccountCode(key, codeNode(code)) //nolint:errcheck
	// 2 touches -- retrieve + updae
	assert.Equal(t, 4, observer.touchedNodes[branchNodeHex])

	// changing storage touches the account
	storageKey := genNKeys(1)[0]
	fullKey := dbutils.GenerateCompositeTrieKey(common.BytesToHash(key), common.BytesToHash(storageKey))

	trie.Update(fullKey, []byte("value-1"))
	assert.Equal(t, 5, observer.touchedNodes[branchNodeHex])

	trie.Update(fullKey, []byte("value-2"))
	assert.Equal(t, 6, observer.touchedNodes[branchNodeHex])

	// getting storage touches the account
	_, ok := trie.Get(fullKey)
	assert.True(t, ok, "should be able to receive storage")
	assert.Equal(t, 7, observer.touchedNodes[branchNodeHex])

	// deleting storage touches the account
	trie.Delete(fullKey)
	assert.Equal(t, 8, observer.touchedNodes[branchNodeHex])

	// getting code touches the account
	_, ok = trie.GetAccountCode(key)
	assert.True(t, ok, "should be able to receive code")
	assert.Equal(t, 9, observer.touchedNodes[branchNodeHex])

	// getting account touches the account
	_, ok = trie.GetAccount(key)
	assert.True(t, ok, "should be able to receive account")
	assert.Equal(t, 10, observer.touchedNodes[branchNodeHex])
}

func TestObserverMux(t *testing.T) {
	trie := newEmpty()

	observer1 := newMockObserver()
	observer2 := newMockObserver()
	mux := NewTrieObserverMux()
	mux.AddChild(observer1)
	mux.AddChild(observer2)

	trie.AddObserver(mux)

	keys := genNKeys(100)
	for _, key := range keys {
		acc := genAccount()
		trie.UpdateAccount(key, acc)

		code := genByteArrayOfLen(100)
		codeHash := crypto.Keccak256(code)
		acc.CodeHash = common.BytesToHash(codeHash)
		trie.UpdateAccount(key, acc)
		trie.UpdateAccountCode(key, codeNode(code)) //nolint:errcheck

		_, ok := trie.GetAccount(key)
		assert.True(t, ok, "acount should be found")

	}

	trie.Hash()

	for i, key := range keys {
		if i < 80 {
			trie.Delete(key)
		} else {
			hex := keybytesToHex(key)
			hex = hex[:len(hex)-1]
			trie.EvictNode(CodeKeyFromAddrHash(hex))
		}
	}

	assert.Equal(t, observer1.createdNodes, observer2.createdNodes, "should propagate created events")
	assert.Equal(t, observer1.deletedNodes, observer2.deletedNodes, "should propagate deleted events")
	assert.Equal(t, observer1.touchedNodes, observer2.touchedNodes, "should propagate touched events")

	assert.Equal(t, observer1.unloadedNodes, observer2.unloadedNodes, "should propagage unloads")
	assert.Equal(t, observer1.unloadedNodeHashes, observer2.unloadedNodeHashes, "should propagage unloads")
	assert.Equal(t, observer1.reloadedNodes, observer2.reloadedNodes, "should propagage reloads")
}

func TestObserverPartial(t *testing.T) {
	trie := newEmpty()

	observer := &partialObserver{callbackCalled: false} // only implements `BranchNodeDeleted`
	trie.AddObserver(observer)

	keys := genNKeys(2)
	for _, key := range keys {
		acc := genAccount()
		trie.UpdateAccount(key, acc)

		code := genByteArrayOfLen(100)
		codeHash := crypto.Keccak256(code)
		acc.CodeHash = common.BytesToHash(codeHash)
		trie.UpdateAccount(key, acc)
		trie.UpdateAccountCode(key, codeNode(code)) //nolint:errcheck

	}
	for _, key := range keys {
		trie.Delete(key)
	}

	assert.True(t, observer.callbackCalled, "should be called")
}
