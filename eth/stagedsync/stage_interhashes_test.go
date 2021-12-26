package stagedsync

import (
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/trie"

	"github.com/stretchr/testify/assert"
)

func addTestAccount(tx kv.Putter, hash common.Hash, balance uint64, incarnation uint64) error {
	acc := accounts.NewAccount()
	acc.Balance.SetUint64(balance)
	acc.Incarnation = incarnation
	if incarnation != 0 {
		acc.CodeHash = common.HexToHash("0x5be74cad16203c4905c068b012a2e9fb6d19d036c410f16fd177f337541440dd")
	}
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)
	return tx.Put(kv.HashedAccounts, hash[:], encoded)
}

func TestAccountAndStorageTrie(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	hash1 := common.HexToHash("0xB000000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash1, 3*params.Ether, 0))

	hash2 := common.HexToHash("0xB040000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash2, 1*params.Ether, 0))

	incarnation := uint64(1)
	hash3 := common.HexToHash("0xB041000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash3, 2*params.Ether, incarnation))

	loc1 := common.HexToHash("0x1200000000000000000000000000000000000000000000000000000000000000")
	loc2 := common.HexToHash("0x1400000000000000000000000000000000000000000000000000000000000000")
	loc3 := common.HexToHash("0x3000000000000000000000000000000000000000000000000000000000E00000")
	loc4 := common.HexToHash("0x3000000000000000000000000000000000000000000000000000000000E00001")

	val1 := common.FromHex("0x42")
	val2 := common.FromHex("0x01")
	val3 := common.FromHex("0x127a89")
	val4 := common.FromHex("0x05")

	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc1), val1))
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc2), val2))
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc3), val3))
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc4), val4))

	hash4a := common.HexToHash("0xB1A0000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash4a, 4*params.Ether, 0))

	hash5 := common.HexToHash("0xB310000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash5, 8*params.Ether, 0))

	hash6 := common.HexToHash("0xB340000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash6, 1*params.Ether, 0))

	// ----------------------------------------------------------------
	// Populate account & storage trie DB tables
	// ----------------------------------------------------------------

	blockReader := snapshotsync.NewBlockReader()
	cfg := StageTrieCfg(nil, false, true, t.TempDir(), blockReader)
	_, err := RegenerateIntermediateHashes("IH", tx, cfg, common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	// ----------------------------------------------------------------
	// Check account trie
	// ----------------------------------------------------------------

	accountTrieA := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfAccounts, nil, func(k, v []byte) error {
		accountTrieA[string(k)] = common.CopyBytes(v)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 2, len(accountTrieA))

	hasState1a, hasTree1a, hasHash1a, hashes1a, rootHash1a := trie.UnmarshalTrieNode(accountTrieA[string(common.FromHex("0B"))])
	assert.Equal(t, uint16(0b1011), hasState1a)
	assert.Equal(t, uint16(0b0001), hasTree1a)
	assert.Equal(t, uint16(0b1001), hasHash1a)
	assert.Equal(t, 2*length.Hash, len(hashes1a))
	assert.Equal(t, 0, len(rootHash1a))

	hasState2a, hasTree2a, hasHash2a, hashes2a, rootHash2a := trie.UnmarshalTrieNode(accountTrieA[string(common.FromHex("0B00"))])
	assert.Equal(t, uint16(0b10001), hasState2a)
	assert.Equal(t, uint16(0b00000), hasTree2a)
	assert.Equal(t, uint16(0b10000), hasHash2a)
	assert.Equal(t, 1*length.Hash, len(hashes2a))
	assert.Equal(t, 0, len(rootHash2a))

	// ----------------------------------------------------------------
	// Check storage trie
	// ----------------------------------------------------------------

	storageTrie := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfStorage, nil, func(k, v []byte) error {
		storageTrie[string(k)] = common.CopyBytes(v)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 1, len(storageTrie))

	storageKey := make([]byte, length.Hash+8)
	copy(storageKey, hash3.Bytes())
	binary.BigEndian.PutUint64(storageKey[length.Hash:], incarnation)

	hasState3, hasTree3, hasHash3, hashes3, rootHash3 := trie.UnmarshalTrieNode(storageTrie[string(storageKey)])
	assert.Equal(t, uint16(0b1010), hasState3)
	assert.Equal(t, uint16(0b0000), hasTree3)
	assert.Equal(t, uint16(0b0010), hasHash3)
	assert.Equal(t, 1*length.Hash, len(hashes3))
	assert.Equal(t, length.Hash, len(rootHash3))

	// ----------------------------------------------------------------
	// Incremental trie
	// ----------------------------------------------------------------

	newAddress := common.HexToAddress("0x4f61f2d5ebd991b85aa1677db97307caf5215c91")
	hash4b, err := common.HashData(newAddress[:])
	assert.Nil(t, err)
	assert.Equal(t, hash4a[0], hash4b[0])

	assert.Nil(t, addTestAccount(tx, hash4b, 5*params.Ether, 0))

	err = tx.Put(kv.AccountChangeSet, dbutils.EncodeBlockNumber(1), newAddress[:])
	assert.Nil(t, err)

	var s StageState
	s.BlockNumber = 0
	_, err = incrementIntermediateHashes("IH", &s, tx, 1 /* to */, cfg, common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	accountTrieB := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfAccounts, nil, func(k, v []byte) error {
		accountTrieB[string(k)] = common.CopyBytes(v)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 2, len(accountTrieB))

	hasState1b, hasTree1b, hasHash1b, hashes1b, rootHash1b := trie.UnmarshalTrieNode(accountTrieB[string(common.FromHex("0B"))])
	assert.Equal(t, hasState1a, hasState1b)
	assert.Equal(t, hasTree1a, hasTree1b)
	assert.Equal(t, uint16(0b1011), hasHash1b)
	assert.Equal(t, 3*length.Hash, len(hashes1b))
	assert.Equal(t, rootHash1a, rootHash1b)

	assert.Equal(t, accountTrieA[string(common.FromHex("0B00"))], accountTrieB[string(common.FromHex("0B00"))])
}

func TestAccountTrieAroundExtensionNode(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	acc := accounts.NewAccount()
	acc.Balance.SetUint64(1 * params.Ether)
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)

	hash1 := common.HexToHash("0x30af561000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash1[:], encoded))

	hash2 := common.HexToHash("0x30af569000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash2[:], encoded))

	hash3 := common.HexToHash("0x30af650000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash3[:], encoded))

	hash4 := common.HexToHash("0x30af6f0000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash4[:], encoded))

	hash5 := common.HexToHash("0x30af8f0000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash5[:], encoded))

	hash6 := common.HexToHash("0x3100000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash6[:], encoded))

	blockReader := snapshotsync.NewBlockReader()
	_, err := RegenerateIntermediateHashes("IH", tx, StageTrieCfg(nil, false, true, t.TempDir(), blockReader), common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	accountTrie := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfAccounts, nil, func(k, v []byte) error {
		accountTrie[string(k)] = v
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 2, len(accountTrie))

	hasState1, hasTree1, hasHash1, hashes, rootHash1 := trie.UnmarshalTrieNode(accountTrie[string(common.FromHex("03"))])
	assert.Equal(t, uint16(0b11), hasState1)
	assert.Equal(t, uint16(0b01), hasTree1)
	assert.Equal(t, uint16(0b00), hasHash1)
	assert.Equal(t, 0, len(hashes))
	assert.Equal(t, 0, len(rootHash1))

	hasState2, hasTree2, hasHash2, hashes2, rootHash2 := trie.UnmarshalTrieNode(accountTrie[string(common.FromHex("03000a0f"))])
	assert.Equal(t, uint16(0b101100000), hasState2)
	assert.Equal(t, uint16(0b000000000), hasTree2)
	assert.Equal(t, uint16(0b001000000), hasHash2)
	assert.Equal(t, length.Hash, len(hashes2))
	assert.Equal(t, 0, len(rootHash2))
}

func TestStorageDeletion(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	address := common.HexToAddress("0x1000000000000000000000000000000000000000")
	hashedAddress, err := common.HashData(address[:])
	assert.Nil(t, err)
	incarnation := uint64(1)
	assert.Nil(t, addTestAccount(tx, hashedAddress, params.Ether, incarnation))

	plainLocation1 := common.HexToHash("0x1000000000000000000000000000000000000000000000000000000000000000")
	hashedLocation1, err := common.HashData(plainLocation1[:])
	assert.Nil(t, err)

	plainLocation2 := common.HexToHash("0x1A00000000000000000000000000000000000000000000000000000000000000")
	hashedLocation2, err := common.HashData(plainLocation2[:])
	assert.Nil(t, err)

	plainLocation3 := common.HexToHash("0x1E00000000000000000000000000000000000000000000000000000000000000")
	hashedLocation3, err := common.HashData(plainLocation3[:])
	assert.Nil(t, err)

	value1 := common.FromHex("0xABCD")
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation1), value1))

	value2 := common.FromHex("0x4321")
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation2), value2))

	value3 := common.FromHex("0x4444")
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation3), value3))

	// ----------------------------------------------------------------
	// Populate account & storage trie DB tables
	// ----------------------------------------------------------------

	blockReader := snapshotsync.NewBlockReader()
	cfg := StageTrieCfg(nil, false, true, t.TempDir(), blockReader)
	_, err = RegenerateIntermediateHashes("IH", tx, cfg, common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	// ----------------------------------------------------------------
	// Check storage trie
	// ----------------------------------------------------------------

	storageTrieA := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfStorage, nil, func(k, v []byte) error {
		storageTrieA[string(k)] = common.CopyBytes(v)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 1, len(storageTrieA))

	// ----------------------------------------------------------------
	// Delete storage and increment the trie
	// ----------------------------------------------------------------

	assert.Nil(t, tx.Delete(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation1), value1))
	assert.Nil(t, tx.Delete(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation2), value2))
	assert.Nil(t, tx.Delete(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation3), value3))

	err = tx.Put(kv.StorageChangeSet, append(dbutils.EncodeBlockNumber(1), dbutils.PlainGenerateStoragePrefix(address[:], incarnation)...), plainLocation1[:])
	assert.Nil(t, err)

	err = tx.Put(kv.StorageChangeSet, append(dbutils.EncodeBlockNumber(1), dbutils.PlainGenerateStoragePrefix(address[:], incarnation)...), plainLocation2[:])
	assert.Nil(t, err)

	err = tx.Put(kv.StorageChangeSet, append(dbutils.EncodeBlockNumber(1), dbutils.PlainGenerateStoragePrefix(address[:], incarnation)...), plainLocation3[:])
	assert.Nil(t, err)

	var s StageState
	s.BlockNumber = 0
	_, err = incrementIntermediateHashes("IH", &s, tx, 1 /* to */, cfg, common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	storageTrieB := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfStorage, nil, func(k, v []byte) error {
		storageTrieB[string(k)] = common.CopyBytes(v)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 0, len(storageTrieB))
}
