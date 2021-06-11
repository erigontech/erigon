package stagedsync

import (
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func addTestAccount(tx ethdb.Putter, hash common.Hash, balance uint64, incarnation uint64) error {
	acc := accounts.NewAccount()
	acc.Balance.SetUint64(balance)
	acc.Incarnation = incarnation
	if incarnation != 0 {
		acc.CodeHash = common.HexToHash("0x5be74cad16203c4905c068b012a2e9fb6d19d036c410f16fd177f337541440dd")
	}
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)
	return tx.Put(dbutils.HashedAccountsBucket, hash[:], encoded)
}

func TestAccountAndStorageTrie(t *testing.T) {
	db, tx := ethdb.NewTestTx(t)

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

	assert.Nil(t, tx.Put(dbutils.HashedStorageBucket, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc1), val1))
	assert.Nil(t, tx.Put(dbutils.HashedStorageBucket, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc2), val2))
	assert.Nil(t, tx.Put(dbutils.HashedStorageBucket, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc3), val3))
	assert.Nil(t, tx.Put(dbutils.HashedStorageBucket, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc4), val4))

	hash4 := common.HexToHash("0xB100000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash4, 4*params.Ether, 0))

	hash5 := common.HexToHash("0xB310000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash5, 8*params.Ether, 0))

	hash6 := common.HexToHash("0xB340000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash6, 1*params.Ether, 0))

	_, err := RegenerateIntermediateHashes("IH", tx, StageTrieCfg(db, false, true, t.TempDir()), common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	accountTrie := make(map[string][]byte)
	accountCursor, err := tx.Cursor(dbutils.TrieOfAccountsBucket)
	require.NoError(t, err)
	defer accountCursor.Close()
	err = ethdb.ForEach(accountCursor, func(k, v []byte) (bool, error) {
		accountTrie[string(k)] = v
		return true, nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 2, len(accountTrie))

	hasState1, hasTree1, hasHash1, hashes1, rootHash1 := trie.UnmarshalTrieNode(accountTrie[string(common.FromHex("0B"))])
	assert.Equal(t, uint16(0b1011), hasState1)
	assert.Equal(t, uint16(0b0001), hasTree1)
	assert.Equal(t, uint16(0b1001), hasHash1)
	assert.Equal(t, 2*common.HashLength, len(hashes1))
	assert.Equal(t, 0, len(rootHash1))

	hasState2, hasTree2, hasHash2, hashes2, rootHash2 := trie.UnmarshalTrieNode(accountTrie[string(common.FromHex("0B00"))])
	assert.Equal(t, uint16(0b10001), hasState2)
	assert.Equal(t, uint16(0b00000), hasTree2)
	assert.Equal(t, uint16(0b10000), hasHash2)
	assert.Equal(t, 1*common.HashLength, len(hashes2))
	assert.Equal(t, 0, len(rootHash2))

	storageTrie := make(map[string][]byte)
	storageCursor, err := tx.Cursor(dbutils.TrieOfStorageBucket)
	require.NoError(t, err)
	defer storageCursor.Close()
	err = ethdb.ForEach(storageCursor, func(k, v []byte) (bool, error) {
		storageTrie[string(k)] = v
		return true, nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 1, len(storageTrie))

	storageKey := make([]byte, common.HashLength+8)
	copy(storageKey, hash3.Bytes())
	binary.BigEndian.PutUint64(storageKey[common.HashLength:], incarnation)

	hasState3, hasTree3, hasHash3, hashes3, rootHash3 := trie.UnmarshalTrieNode(storageTrie[string(storageKey)])
	assert.Equal(t, uint16(0b1010), hasState3)
	assert.Equal(t, uint16(0b0000), hasTree3)
	assert.Equal(t, uint16(0b0010), hasHash3)
	assert.Equal(t, 1*common.HashLength, len(hashes3))
	assert.Equal(t, common.HashLength, len(rootHash3))
}

func TestAccountTrieAroundExtensionNode(t *testing.T) {
	db, tx := ethdb.NewTestTx(t)

	acc := accounts.NewAccount()
	acc.Balance.SetUint64(1 * params.Ether)
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)

	hash1 := common.HexToHash("0x30af561000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(dbutils.HashedAccountsBucket, hash1[:], encoded))

	hash2 := common.HexToHash("0x30af569000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(dbutils.HashedAccountsBucket, hash2[:], encoded))

	hash3 := common.HexToHash("0x30af650000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(dbutils.HashedAccountsBucket, hash3[:], encoded))

	hash4 := common.HexToHash("0x30af6f0000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(dbutils.HashedAccountsBucket, hash4[:], encoded))

	hash5 := common.HexToHash("0x30af8f0000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(dbutils.HashedAccountsBucket, hash5[:], encoded))

	hash6 := common.HexToHash("0x3100000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(dbutils.HashedAccountsBucket, hash6[:], encoded))

	_, err := RegenerateIntermediateHashes("IH", tx, StageTrieCfg(db, false, true, t.TempDir()), common.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	accountTrie := make(map[string][]byte)
	accountCursor, err := tx.Cursor(dbutils.TrieOfAccountsBucket)
	require.NoError(t, err)
	defer accountCursor.Close()
	err = ethdb.ForEach(accountCursor, func(k, v []byte) (bool, error) {
		accountTrie[string(k)] = v
		return true, nil
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
	assert.Equal(t, common.HashLength, len(hashes2))
	assert.Equal(t, 0, len(rootHash2))
}
