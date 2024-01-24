package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
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
	"github.com/stretchr/testify/require"
)

func addTestAccount(tx kv.Putter, hash libcommon.Hash, balance uint64, incarnation uint64) error {
	acc := accounts.NewAccount()
	acc.Balance.SetUint64(balance)
	acc.Incarnation = incarnation
	if incarnation != 0 {
		acc.CodeHash = libcommon.HexToHash("0x5be74cad16203c4905c068b012a2e9fb6d19d036c410f16fd177f337541440dd")
	}
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)
	return tx.Put(kv.HashedAccounts, hash[:], encoded)
}

func TestAccountAndStorageTrie(t *testing.T) {
	db, tx := memdb.NewTestTx(t)
	ctx := context.Background()

	hash1 := libcommon.HexToHash("0xB000000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash1, 3*params.Ether, 0))

	hash2 := libcommon.HexToHash("0xB040000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash2, 1*params.Ether, 0))

	incarnation := uint64(1)
	hash3 := libcommon.HexToHash("0xB041000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash3, 2*params.Ether, incarnation))

	loc1 := libcommon.HexToHash("0x1200000000000000000000000000000000000000000000000000000000000000")
	loc2 := libcommon.HexToHash("0x1400000000000000000000000000000000000000000000000000000000000000")
	loc3 := libcommon.HexToHash("0x3000000000000000000000000000000000000000000000000000000000E00000")
	loc4 := libcommon.HexToHash("0x3000000000000000000000000000000000000000000000000000000000E00001")

	val1 := common.FromHex("0x42")
	val2 := common.FromHex("0x01")
	val3 := common.FromHex("0x127a89")
	val4 := common.FromHex("0x05")

	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc1), val1))
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc2), val2))
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc3), val3))
	assert.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hash3, incarnation, loc4), val4))

	hash4a := libcommon.HexToHash("0xB1A0000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash4a, 4*params.Ether, 0))

	hash5 := libcommon.HexToHash("0xB310000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash5, 8*params.Ether, 0))

	hash6 := libcommon.HexToHash("0xB340000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, addTestAccount(tx, hash6, 1*params.Ether, 0))

	// ----------------------------------------------------------------
	// Populate account & storage trie DB tables
	// ----------------------------------------------------------------

	historyV3 := false
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(nil, false)
	cfg := StageTrieCfg(db, false, true, false, t.TempDir(), blockReader, nil, historyV3, nil)
	_, err := RegenerateIntermediateHashes("IH", tx, cfg, libcommon.Hash{} /* expectedRootHash */, ctx)
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

	newAddress := libcommon.HexToAddress("0x4f61f2d5ebd991b85aa1677db97307caf5215c91")
	hash4b, err := common.HashData(newAddress[:])
	assert.Nil(t, err)
	assert.Equal(t, hash4a[0], hash4b[0])

	assert.Nil(t, addTestAccount(tx, hash4b, 5*params.Ether, 0))

	err = tx.Put(kv.AccountChangeSet, hexutility.EncodeTs(1), newAddress[:])
	assert.Nil(t, err)

	var s StageState
	s.BlockNumber = 0
	_, err = incrementIntermediateHashes("IH", &s, tx, 1 /* to */, cfg, libcommon.Hash{} /* expectedRootHash */, nil /* quit */)
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
	db, tx := memdb.NewTestTx(t)
	ctx := context.Background()
	historyV3 := false

	acc := accounts.NewAccount()
	acc.Balance.SetUint64(1 * params.Ether)
	encoded := make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(encoded)

	hash1 := libcommon.HexToHash("0x30af561000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash1[:], encoded))

	hash2 := libcommon.HexToHash("0x30af569000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash2[:], encoded))

	hash3 := libcommon.HexToHash("0x30af650000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash3[:], encoded))

	hash4 := libcommon.HexToHash("0x30af6f0000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash4[:], encoded))

	hash5 := libcommon.HexToHash("0x30af8f0000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash5[:], encoded))

	hash6 := libcommon.HexToHash("0x3100000000000000000000000000000000000000000000000000000000000000")
	assert.Nil(t, tx.Put(kv.HashedAccounts, hash6[:], encoded))

	blockReader := snapshotsync.NewBlockReaderWithSnapshots(nil, false)
	_, err := RegenerateIntermediateHashes("IH", tx, StageTrieCfg(db, false, true, false, t.TempDir(), blockReader, nil, historyV3, nil), libcommon.Hash{} /* expectedRootHash */, ctx)
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
	db, tx := memdb.NewTestTx(t)
	ctx := context.Background()

	address := libcommon.HexToAddress("0x1000000000000000000000000000000000000000")
	hashedAddress, err := common.HashData(address[:])
	assert.Nil(t, err)
	incarnation := uint64(1)
	assert.Nil(t, addTestAccount(tx, hashedAddress, params.Ether, incarnation))

	plainLocation1 := libcommon.HexToHash("0x1000000000000000000000000000000000000000000000000000000000000000")
	hashedLocation1, err := common.HashData(plainLocation1[:])
	assert.Nil(t, err)

	plainLocation2 := libcommon.HexToHash("0x1A00000000000000000000000000000000000000000000000000000000000000")
	hashedLocation2, err := common.HashData(plainLocation2[:])
	assert.Nil(t, err)

	plainLocation3 := libcommon.HexToHash("0x1E00000000000000000000000000000000000000000000000000000000000000")
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
	historyV3 := false
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(nil, false)
	cfg := StageTrieCfg(db, false, true, false, t.TempDir(), blockReader, nil, historyV3, nil)
	_, err = RegenerateIntermediateHashes("IH", tx, cfg, libcommon.Hash{} /* expectedRootHash */, ctx)
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

	assert.Nil(t, tx.Delete(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation1)))
	assert.Nil(t, tx.Delete(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation2)))
	assert.Nil(t, tx.Delete(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress, incarnation, hashedLocation3)))

	err = tx.Put(kv.StorageChangeSet, append(hexutility.EncodeTs(1), dbutils.PlainGenerateStoragePrefix(address[:], incarnation)...), plainLocation1[:])
	assert.Nil(t, err)

	err = tx.Put(kv.StorageChangeSet, append(hexutility.EncodeTs(1), dbutils.PlainGenerateStoragePrefix(address[:], incarnation)...), plainLocation2[:])
	assert.Nil(t, err)

	err = tx.Put(kv.StorageChangeSet, append(hexutility.EncodeTs(1), dbutils.PlainGenerateStoragePrefix(address[:], incarnation)...), plainLocation3[:])
	assert.Nil(t, err)

	var s StageState
	s.BlockNumber = 0
	_, err = incrementIntermediateHashes("IH", &s, tx, 1 /* to */, cfg, libcommon.Hash{} /* expectedRootHash */, nil /* quit */)
	assert.Nil(t, err)

	storageTrieB := make(map[string][]byte)
	err = tx.ForEach(kv.TrieOfStorage, nil, func(k, v []byte) error {
		storageTrieB[string(k)] = common.CopyBytes(v)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, 0, len(storageTrieB))
}

func TestHiveTrieRoot(t *testing.T) {
	db, tx := memdb.NewTestTx(t)
	ctx := context.Background()

	hashedAddress1, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000000"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress1[:],
		common.FromHex("02081bc5e32fd4403800")))

	hashedAddress2, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000314"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress2[:],
		common.FromHex("0c0101203e6de602146067c01322e2528a8f320c504fd3d19a4d6c4c53b54d2b2f9357ec")))

	hashedLocA, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000000000000000000000000000000"))
	require.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress2, 1, hashedLocA),
		common.FromHex("1234")))

	hashedLocB, _ := common.HashData(common.FromHex("6661e9d6d8b923d5bbaab1b96e1dd51ff6ea2a93520fdc9eb75d059238b8c5e9"))
	require.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress2, 1, hashedLocB),
		common.FromHex("01")))

	hashedAddress3, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000315"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress3[:],
		common.FromHex("0e100999999999999999999999999999999901012052de487a82a5e45f90f7fb0edf025b1d23f85c308ae7543736a91ac6295217f3")))

	hashedAddress4, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000316"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress4[:],
		common.FromHex("0c010120803ac275052ba5360d44e51a7d4a49ed9156c461a21119ff650506869827f2c8")))

	hashedLocC, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000000000000000000000000000001"))
	require.Nil(t, tx.Put(kv.HashedStorage, dbutils.GenerateCompositeStorageKey(hashedAddress4, 1, hashedLocC),
		common.FromHex("030000")))

	hashedAddress5, _ := common.HashData(common.FromHex("0000000000000000000000000000000000000317"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress5[:],
		common.FromHex("0c010120247c40b032c36acb07ca105280db053d204d3133302420f403dfbb54f775d0e2")))

	hashedAddress6, _ := common.HashData(common.FromHex("0161e041aad467a890839d5b08b138c1e6373072"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress6[:],
		common.FromHex("020b0123450000000000000000")))

	hashedAddress7, _ := common.HashData(common.FromHex("6e53b788a8e675377c5f160e5c6cca6b46074af8"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress7[:],
		common.FromHex("02081bc16d674ec80000")))

	hashedAddress8, _ := common.HashData(common.FromHex("87da6a8c6e9eff15d703fc2773e32f6af8dbe301"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress8[:],
		common.FromHex("020b0123450000000000000000")))

	hashedAddress9, _ := common.HashData(common.FromHex("b97de4b8c857e4f6bc354f226dc3249aaee49209"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress9[:],
		common.FromHex("020b0123450000000000000000")))

	hashedAddress10, _ := common.HashData(common.FromHex("c5065c9eeebe6df2c2284d046bfc906501846c51"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress10[:],
		common.FromHex("020b0123450000000000000000")))

	hashedAddress11, _ := common.HashData(common.FromHex("cf49fda3be353c69b41ed96333cd24302da4556f"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress11[:],
		common.FromHex("0301010b012344fffb67ea09bf8000")))

	hashedAddress12, _ := common.HashData(common.FromHex("e0840414c530d72e5c2f1fe64f6311cc3136cab1"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress12[:],
		common.FromHex("02081bc16d674ec80000")))

	hashedAddress13, _ := common.HashData(common.FromHex("f8e0e7f6f1d0514ddfbc00bec204641f1f4d8cc8"))
	require.Nil(t, tx.Put(kv.HashedAccounts, hashedAddress13[:],
		common.FromHex("02081bc16d674ec80000")))

	historyV3 := false
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(nil, false)
	cfg := StageTrieCfg(db, false, true, false, t.TempDir(), blockReader, nil, historyV3, nil)
	_, err := RegenerateIntermediateHashes("IH", tx, cfg, libcommon.Hash{} /* expectedRootHash */, ctx)
	require.Nil(t, err)

	// Now add a new account
	newAddress := libcommon.HexToAddress("0xf76fefb6608ca3d826945a9571d1f8e53bb6f366")
	newHash, err := common.HashData(newAddress[:])
	require.Nil(t, err)

	require.Nil(t, tx.Put(kv.HashedAccounts, newHash[:], common.FromHex("02081bc16d674ec80000")))
	require.Nil(t, tx.Put(kv.AccountChangeSet, hexutility.EncodeTs(1), newAddress[:]))

	var s StageState
	s.BlockNumber = 0
	incrementalRoot, err := incrementIntermediateHashes("IH", &s, tx, 1 /* to */, cfg, libcommon.Hash{} /* expectedRootHash */, nil /* quit */)
	require.Nil(t, err)

	regeneratedRoot, err := RegenerateIntermediateHashes("IH", tx, cfg, libcommon.Hash{} /* expectedRootHash */, ctx)
	require.Nil(t, err)

	assert.Equal(t, regeneratedRoot, incrementalRoot)
}
