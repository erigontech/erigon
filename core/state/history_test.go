package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"math/big"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func TestMutation_DeleteTimestamp(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	mutDB := db.NewBatch()

	acc := make([]*accounts.Account, 10)
	addr := make([]common.Address, 10)
	addrHashes := make([]common.Hash, 10)
	tds := NewTrieDbState(common.Hash{}, mutDB, 1)
	blockWriter := tds.DbStateWriter()
	ctx := context.Background()
	emptyAccount := accounts.NewAccount()
	for i := range acc {
		acc[i], addr[i], addrHashes[i] = randomAccount(t)
		if err := blockWriter.UpdateAccountData(ctx, addr[i], &emptyAccount /* original */, acc[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}
	_, err := mutDB.Commit()
	if err != nil {
		t.Fatal(err)
	}

	csData, err := db.Get(dbutils.AccountChangeSetBucket, dbutils.EncodeTimestamp(1))
	if err != nil {
		t.Fatal(err)
	}

	if changeset.Len(csData) != 10 {
		t.FailNow()
	}

	indexBytes, innerErr := db.GetIndexChunk(dbutils.AccountsHistoryBucket, addrHashes[0].Bytes(), 1)
	if innerErr != nil {
		t.Fatal(err)
	}

	index := dbutils.WrapHistoryIndex(indexBytes)

	parsed, _, innerErr := index.Decode()
	if innerErr != nil {
		t.Fatal(innerErr)
	}
	if parsed[0] != 1 {
		t.Fatal("incorrect block num")
	}

	err = tds.deleteTimestamp(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mutDB.Commit()
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Get(dbutils.AccountChangeSetBucket, dbutils.EncodeTimestamp(1))
	if err != ethdb.ErrKeyNotFound {
		t.Fatal("changeset must be deleted")
	}

	_, err = db.Get(dbutils.AccountsHistoryBucket, addrHashes[0].Bytes())
	if err != ethdb.ErrKeyNotFound {
		t.Fatal("account must be deleted")
	}
}

func TestMutationCommitThinHistory(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	mutDB := db.NewBatch()

	numOfAccounts := 5
	numOfStateKeys := 5

	addrs, accState, accStateStorage, accHistory, accHistoryStateStorage := generateAccountsWithStorageAndHistory(t, mutDB, numOfAccounts, numOfStateKeys)

	_, commitErr := mutDB.Commit()
	if commitErr != nil {
		t.Fatal(commitErr)
	}

	for i, addr := range addrs {
		acc := accounts.NewAccount()
		if ok, err := rawdb.PlainReadAccount(db, addr, &acc); err != nil {
			t.Fatal("error on get account", i, err)
		} else if !ok {
			t.Fatal("error on get account", i)
		}

		if !accState[i].Equals(&acc) {
			spew.Dump("got", acc)
			spew.Dump("expected", accState[i])
			t.Fatal("Accounts not equals")
		}

		indexBytes, err := db.GetIndexChunk(dbutils.AccountsHistoryBucket, addr.Bytes(), 2)
		if err != nil {
			t.Fatal("error on get account", i, err)
		}

		index := dbutils.WrapHistoryIndex(indexBytes)
		parsedIndex, _, err := index.Decode()
		if err != nil {
			t.Fatal("error on get account", i, err)
		}

		if parsedIndex[0] != 1 && index.Len() != 1 {
			t.Fatal("incorrect history index")
		}

		resAccStorage := make(map[common.Hash]uint256.Int)
		err = db.Walk(dbutils.PlainStateBucket, dbutils.PlainGenerateStoragePrefix(addr[:], acc.Incarnation), 8*(common.AddressLength+8), func(k, v []byte) (b bool, e error) {
			resAccStorage[common.BytesToHash(k[common.AddressLength+8:])] = *uint256.NewInt().SetBytes(v)
			return true, nil
		})
		if err != nil {
			t.Fatal("error on get account storage", i, err)
		}

		if !reflect.DeepEqual(resAccStorage, accStateStorage[i]) {
			spew.Dump("res", resAccStorage)
			spew.Dump("expected", accStateStorage[i])
			t.Fatal("incorrect storage", i)
		}

		for k, v := range accHistoryStateStorage[i] {
			res, err := GetAsOf(db.KV(), true /* plain */, true /* storage */, dbutils.PlainGenerateCompositeStorageKey(addr, acc.Incarnation, k), 1)
			if err != nil {
				t.Fatal(err)
			}

			result := uint256.NewInt().SetBytes(res)
			if !v.Eq(result) {
				t.Fatalf("incorrect storage history for %x %x %x", addr.String(), v, result)
			}
		}
	}

	csData, err := db.Get(dbutils.PlainAccountChangeSetBucket, dbutils.EncodeTimestamp(2))
	if err != nil {
		t.Fatal(err)
	}

	expectedChangeSet := changeset.NewAccountChangeSetPlain()
	for i := range addrs {
		// Make ajustments for THIN_HISTORY
		c := accHistory[i].SelfCopy()
		copy(c.CodeHash[:], emptyCodeHash)
		c.Root = trie.EmptyRoot
		bLen := c.EncodingLengthForStorage()
		b := make([]byte, bLen)
		c.EncodeForStorage(b)
		innerErr := expectedChangeSet.Add(addrs[i].Bytes(), b)
		if innerErr != nil {
			t.Fatal(innerErr)
		}
	}
	sort.Sort(expectedChangeSet)
	expectedData, err := changeset.EncodeAccountsPlain(expectedChangeSet)
	assert.NoError(t, err)
	if !bytes.Equal(csData, expectedData) {
		spew.Dump("res", csData)
		spew.Dump("expected", expectedData)
		t.Fatal("incorrect changeset")
	}

	csData, err = db.Get(dbutils.PlainStorageChangeSetBucket, dbutils.EncodeTimestamp(2))
	if err != nil {
		t.Fatal(err)
	}

	cs, _ := changeset.DecodeStoragePlain(csData)
	if cs.Len() != numOfAccounts*numOfStateKeys {
		t.Errorf("Length does not match, got %d, expected %d", cs.Len(), numOfAccounts*numOfStateKeys)
	}

	expectedChangeSet = changeset.NewStorageChangeSetPlain()
	for i, addr := range addrs {
		for j := 0; j < numOfStateKeys; j++ {
			key := common.Hash{uint8(i*100 + j)}
			value := uint256.NewInt().SetUint64(uint64(10 + j))
			if err2 := expectedChangeSet.Add(dbutils.PlainGenerateCompositeStorageKey(addr, accHistory[i].Incarnation, key), value.Bytes()); err2 != nil {
				t.Fatal(err2)
			}
		}
	}
	sort.Sort(expectedChangeSet)
	expectedData, err = changeset.EncodeStoragePlain(expectedChangeSet)
	assert.NoError(t, err)
	if !bytes.Equal(csData, expectedData) {
		spew.Dump("res", csData)
		spew.Dump("expected", expectedData)
		t.Fatal("incorrect changeset")
	}
}

func generateAccountsWithStorageAndHistory(t *testing.T, db ethdb.Database, numOfAccounts, numOfStateKeys int) ([]common.Address, []*accounts.Account, []map[common.Hash]uint256.Int, []*accounts.Account, []map[common.Hash]uint256.Int) {
	t.Helper()

	accHistory := make([]*accounts.Account, numOfAccounts)
	accState := make([]*accounts.Account, numOfAccounts)
	accStateStorage := make([]map[common.Hash]uint256.Int, numOfAccounts)
	accHistoryStateStorage := make([]map[common.Hash]uint256.Int, numOfAccounts)
	addrs := make([]common.Address, numOfAccounts)
	tds := NewTrieDbState(common.Hash{}, db, 1)
	tds.SetBlockNr(2)
	blockWriter := tds.PlainStateWriter()
	ctx := context.Background()
	for i := range accHistory {
		accHistory[i], addrs[i], _ = randomAccount(t)
		accHistory[i].Balance = *uint256.NewInt().SetUint64(100)
		accHistory[i].CodeHash = common.Hash{uint8(10 + i)}
		accHistory[i].Root = common.Hash{uint8(10 + i)}
		accHistory[i].Incarnation = uint64(i + 1)

		accState[i] = accHistory[i].SelfCopy()
		accState[i].Nonce++
		accState[i].Balance = *uint256.NewInt().SetUint64(200)

		accStateStorage[i] = make(map[common.Hash]uint256.Int)
		accHistoryStateStorage[i] = make(map[common.Hash]uint256.Int)
		for j := 0; j < numOfStateKeys; j++ {
			key := common.Hash{uint8(i*100 + j)}
			newValue := uint256.NewInt().SetUint64(uint64(j))
			if !newValue.IsZero() {
				// Empty value is not considered to be present
				accStateStorage[i][key] = *newValue
			}

			value := uint256.NewInt().SetUint64(uint64(10 + j))
			accHistoryStateStorage[i][key] = *value
			if err := blockWriter.WriteAccountStorage(ctx, addrs[i], accHistory[i].Incarnation, &key, value, newValue); err != nil {
				t.Fatal(err)
			}
		}
		if err := blockWriter.UpdateAccountData(ctx, addrs[i], accHistory[i] /* original */, accState[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}
	return addrs, accState, accStateStorage, accHistory, accHistoryStateStorage
}

func randomAccount(t *testing.T) (*accounts.Account, common.Address, common.Hash) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	acc := accounts.NewAccount()
	acc.Initialised = true
	acc.Balance = *uint256.NewInt().SetUint64(uint64(rand.Int63()))
	addr := crypto.PubkeyToAddress(key.PublicKey)
	addrHash, err := common.HashData(addr.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return &acc, addr, addrHash
}

func TestUnwindTruncateHistory(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	mutDB := db.NewBatch()
	tds := NewTrieDbState(common.Hash{}, mutDB, 1)
	ctx := context.Background()
	acc1 := accounts.NewAccount()
	acc := &acc1
	acc.Initialised = true
	var addr common.Address = common.HexToAddress("0x1234567890")
	acc.Balance.SetUint64(0)
	// We will so that balance (in wei) always matches the block number
	// We will also insert a extra storage item every block
	for blockNumber := uint64(1); blockNumber < uint64(100); blockNumber++ {
		tds.StartNewBuffer()
		newAcc := acc.SelfCopy()
		newAcc.Balance.SetUint64(blockNumber)
		tds.SetBlockNr(blockNumber)
		txWriter := tds.TrieStateWriter()
		blockWriter := tds.DbStateWriter()
		if blockNumber == 1 {
			err := txWriter.CreateContract(addr)
			if err != nil {
				t.Fatal(err)
			}
			newAcc.Incarnation = FirstContractIncarnation
		}
		var oldValue uint256.Int
		var newValue uint256.Int
		newValue[0] = 1
		var location common.Hash
		location.SetBytes(big.NewInt(int64(blockNumber)).Bytes())
		if err := txWriter.WriteAccountStorage(ctx, addr, newAcc.Incarnation, &location, &oldValue, &newValue); err != nil {
			t.Fatal(err)
		}
		if err := txWriter.UpdateAccountData(ctx, addr, acc /* original */, newAcc /* new account */); err != nil {
			t.Fatal(err)
		}
		if _, err := tds.ComputeTrieRoots(); err != nil {
			t.Fatal(err)
		}
		if blockNumber == 1 {
			err := blockWriter.CreateContract(addr)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := blockWriter.WriteAccountStorage(ctx, addr, newAcc.Incarnation, &location, &oldValue, &newValue); err != nil {
			t.Fatal(err)
		}
		if err := blockWriter.UpdateAccountData(ctx, addr, acc /* original */, newAcc /* new account */); err != nil {
			t.Fatal(err)
		}
		if err := blockWriter.WriteChangeSets(); err != nil {
			t.Fatal(err)
		}
		if err := blockWriter.WriteHistory(); err != nil {
			t.Fatal(err)
		}
		if _, err := mutDB.Commit(); err != nil {
			t.Fatal(err)
		}
		acc = newAcc
	}
	// Recreate tds not to rely on the trie
	tds = NewTrieDbState(tds.LastRoot(), mutDB, tds.blockNr)
	a, err := tds.ReadAccountData(addr)
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance.Uint64() != 99 {
		t.Errorf("wrong balance on the account, expected %d, got %d", 99, a.Balance.Uint64())
	}
	// Check 100 storage locations
	for l := 0; l <= 100; l++ {
		var location common.Hash
		location.SetBytes(big.NewInt(int64(l)).Bytes())
		enc, err1 := tds.ReadAccountStorage(addr, a.Incarnation, &location)
		if err1 != nil {
			t.Fatal(err1)
		}
		if l > 0 && l < 100 {
			if len(enc) == 0 {
				t.Errorf("expected non-empty storage at location %d, got empty", l)
			}
		} else {
			if len(enc) > 0 {
				t.Errorf("expected empty storage at location %d, got non-empty", l)
			}
		}
	}
	// New we are goint to unwind 50 blocks back and check the balance
	if err = tds.UnwindTo(50); err != nil {
		t.Fatal(err)
	}
	if _, err = mutDB.Commit(); err != nil {
		t.Fatal(err)
	}
	a, err = tds.ReadAccountData(addr)
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance.Uint64() != 50 {
		t.Errorf("wrong balance on the account, expected %d, got %d", 50, a.Balance.Uint64())
	}
	// Check 100 storage locations
	for l := 0; l <= 100; l++ {
		var location common.Hash
		location.SetBytes(big.NewInt(int64(l)).Bytes())
		enc, err1 := tds.ReadAccountStorage(addr, a.Incarnation, &location)
		if err1 != nil {
			t.Fatal(err1)
		}
		if l > 0 && l <= 50 {
			if len(enc) == 0 {
				t.Errorf("expected non-empty storage at location %d, got empty", l)
			}
		} else {
			if len(enc) > 0 {
				t.Errorf("expected empty storage at location %d, got non-empty", l)
			}
		}
	}
}

/*
	before 3:
	addr1(f22b):""
	addr2(1f0e):""
	addr3(3e05):""
	addr4(d12e):""
	block 3
	addr1(f22b):"block3"
	addr2(1f0e):""
	addr3(3e05):"state"
	addr4(d12e):"block3"
	block 5
	addr1(f22b):"state"
	addr2(1f0e):"state"
	addr3(3e05):"state"
	addr4(d12e):""
*/

func TestWalkAsOfStateHashed(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	emptyVal := uint256.NewInt()
	block3Val := uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	key := common.Hash{123}
	keyHash, _ := common.HashData(key.Bytes())
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	withoutInc := func(addrHash, keyHash common.Hash) []byte {
		expectedKey := make([]byte, common.HashLength*2)
		copy(expectedKey[:common.HashLength], addrHash.Bytes())
		copy(expectedKey[common.HashLength:], keyHash.Bytes())
		return expectedKey
	}
	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	writeStorageBlockData(t, tds, 3, []storageData{
		{
			addrs[0],
			1,
			key,
			emptyVal,
			block3Val,
		},
		{
			addrs[2],
			1,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			1,
			key,
			emptyVal,
			block3Val,
		},
	}, false, true)

	writeStorageBlockData(t, tds, 5, []storageData{
		{
			addrs[0],
			1,
			key,
			block3Val,
			stateVal,
		},
		{
			addrs[1],
			1,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			1,
			key,
			block3Val,
			emptyVal,
		},
	}, false, true)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//walk and collect walkAsOf result
	var err error
	var startKey [72]byte
	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes = []changeset.Change{
		{
			withoutInc(addrHashes[0], keyHash),
			block3Val.Bytes(),
		},
		{
			withoutInc(addrHashes[2], keyHash),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrHashes[3], keyHash),
			block3Val.Bytes(),
		},
	}

	assertChangesEquals(t, block4, block4Expected)

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block6Expected.Changes = []changeset.Change{
		{
			withoutInc(addrHashes[0], keyHash),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrHashes[1], keyHash), stateVal.Bytes(),
		},
		{
			withoutInc(addrHashes[2], keyHash), stateVal.Bytes(),
		},
	}

	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfStatePlain(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)

	emptyVal := uint256.NewInt()
	block3Val := uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	key := common.Hash{123}
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	withoutInc := func(addr common.Address, keyHash common.Hash) []byte {
		expectedKey := make([]byte, common.HashLength+common.AddressLength)
		copy(expectedKey[:common.AddressLength], addr.Bytes())
		copy(expectedKey[common.AddressLength:], keyHash.Bytes())
		return expectedKey
	}

	writeStorageBlockData(t, tds, 3, []storageData{
		{
			addrs[0],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			block3Val,
		},
		{
			addrs[2],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			block3Val,
		},
	}, true, true)

	writeStorageBlockData(t, tds, 5, []storageData{
		{
			addrs[0],
			changeset.DefaultIncarnation,
			key,
			block3Val,
			stateVal,
		},
		{
			addrs[1],
			changeset.DefaultIncarnation,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			changeset.DefaultIncarnation,
			key,
			block3Val,
			emptyVal,
		},
	}, true, true)

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//walk and collect walkAsOf result
	var err error
	var startKey [60]byte
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes = []changeset.Change{
		{
			withoutInc(addrs[0], key),
			block3Val.Bytes(),
		},
		{
			withoutInc(addrs[2], key),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrs[3], key),
			block3Val.Bytes(),
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block6Expected.Changes = []changeset.Change{
		{
			withoutInc(addrs[0], key),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrs[1], key),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrs[2], key),
			stateVal.Bytes(),
		},
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfUsingFixedBytesStatePlain(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)

	emptyVal := uint256.NewInt()
	block3Val := uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt().SetBytes([]byte("state"))

	addr1 := common.Address{1}
	addr2 := common.Address{2}

	key1 := common.Hash{1}
	key2 := common.Hash{2}
	key3 := common.Hash{3}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	withoutInc := func(addr common.Address, keyHash common.Hash) []byte {
		expectedKey := make([]byte, common.HashLength+common.AddressLength)
		copy(expectedKey[:common.AddressLength], addr.Bytes())
		copy(expectedKey[common.AddressLength:], keyHash.Bytes())
		return expectedKey
	}

	writeStorageBlockData(t, tds, 3, []storageData{
		{
			addr1,
			changeset.DefaultIncarnation,
			key1,
			emptyVal,
			block3Val,
		},
		{
			addr1,
			changeset.DefaultIncarnation,
			key2,
			emptyVal,
			block3Val,
		},
		{
			addr1,
			changeset.DefaultIncarnation,
			key3,
			emptyVal,
			block3Val,
		},
		{
			addr2,
			changeset.DefaultIncarnation,
			key3,
			emptyVal,
			block3Val,
		},
	}, true, true)

	writeStorageBlockData(t, tds, 5, []storageData{
		{
			addr1,
			changeset.DefaultIncarnation,
			key1,
			block3Val,
			stateVal,
		},
		{
			addr1,
			changeset.DefaultIncarnation,
			key2,
			block3Val,
			stateVal,
		},
		{
			addr1,
			changeset.DefaultIncarnation,
			key3,
			block3Val,
			emptyVal,
		},
		{
			addr2,
			changeset.DefaultIncarnation,
			key3,
			block3Val,
			stateVal,
		},
	}, true, true)

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//walk and collect walkAsOf result
	var err error
	startKey := make([]byte, 60)
	copy(startKey[:common.AddressLength], addr1.Bytes())
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey, 0, 2, func(k []byte, v []byte) (b bool, e error) {
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey, common.AddressLength*8, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes = []changeset.Change{
		{
			withoutInc(addr1, key1),
			block3Val.Bytes(),
		},
		{
			withoutInc(addr1, key2),
			block3Val.Bytes(),
		},
		{
			withoutInc(addr1, key3),
			block3Val.Bytes(),
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	block4.Changes = block4.Changes[:0]
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, make([]byte, 60), 0, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes = append(block4Expected.Changes, changeset.Change{
		Key:   withoutInc(addr2, key3),
		Value: block3Val.Bytes(),
	})
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey, common.AddressLength*8, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block6Expected.Changes = []changeset.Change{
		{
			withoutInc(addr1, key1),
			stateVal.Bytes(),
		},
		{
			withoutInc(addr1, key2),
			stateVal.Bytes(),
		},
	}
	assertChangesEquals(t, block6, block6Expected)

	block6.Changes = block6.Changes[:0]
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, make([]byte, 60), 0, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	block6Expected.Changes = append(block6Expected.Changes, changeset.Change{
		Key:   withoutInc(addr2, key3),
		Value: stateVal.Bytes(),
	})
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountHashed(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	emptyValAcc := accounts.NewAccount()
	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc := emptyValAcc.SelfCopy()
	block3ValAcc.Nonce = 3
	block3ValAcc.Initialised = true
	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc := emptyValAcc.SelfCopy()
	stateValAcc.Nonce = 5
	stateValAcc.Initialised = true
	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	writeBlockData(t, tds, 3, []accData{
		{
			addrs[0],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[2],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[3],
			&emptyValAcc,
			block3ValAcc,
		},
	}, false, true)

	writeBlockData(t, tds, 5, []accData{
		{
			addrs[0], block3ValAcc, stateValAcc,
		},
		{
			addrs[1],
			&emptyValAcc,
			stateValAcc,
		},
		{
			addrs[3],
			block3ValAcc,
			nil,
		},
	}, false, true)

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	var startKey [32]byte
	err := WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	block4Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrHashes[0].Bytes(),
				block3Val,
			},
			{
				addrHashes[2].Bytes(),
				block3Val,
			},
			{
				addrHashes[3].Bytes(),
				block3Val,
			},
		},
	}
	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrHashes[0].Bytes(),
				stateVal,
			},
			{
				addrHashes[1].Bytes(),
				stateVal,
			},
			{
				addrHashes[2].Bytes(),
				block3Val,
			},
		},
	}

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountPlain(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	emptyValAcc := accounts.NewAccount()
	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc := emptyValAcc.SelfCopy()
	block3ValAcc.Nonce = 3
	block3ValAcc.Initialised = true
	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc := emptyValAcc.SelfCopy()
	stateValAcc.Nonce = 5
	stateValAcc.Initialised = true
	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	writeBlockData(t, tds, 3, []accData{
		{
			addrs[0],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[2],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[3],
			&emptyValAcc,
			block3ValAcc,
		},
	}, true, true)

	writeBlockData(t, tds, 5, []accData{
		{
			addrs[0],
			block3ValAcc,
			stateValAcc,
		},
		{
			addrs[1],
			&emptyValAcc,
			stateValAcc,
		},
		{
			addrs[3],
			block3ValAcc,
			nil,
		},
	}, true, true)

	var startKey [20]byte
	err := WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrs[0].Bytes(),
				block3Val,
			},
			{
				addrs[2].Bytes(),
				block3Val,
			},
			{
				addrs[3].Bytes(),
				block3Val,
			},
		},
	}

	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrs[0].Bytes(),
				stateVal,
			},
			{
				addrs[1].Bytes(),
				stateVal,
			},
			{
				addrs[2].Bytes(),
				block3Val,
			},
		},
	}

	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfStateHashed_WithoutIndex(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)

	emptyVal := uint256.NewInt()
	block3Val := uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	key := common.Hash{123}
	keyHash, _ := common.HashData(key.Bytes())
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	withoutInc := func(addrHash, keyHash common.Hash) []byte {
		expectedKey := make([]byte, common.HashLength*2)
		copy(expectedKey[:common.HashLength], addrHash.Bytes())
		copy(expectedKey[common.HashLength:], keyHash.Bytes())
		return expectedKey
	}
	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	writeStorageBlockData(t, tds, 3, []storageData{
		{
			addrs[0],
			1,
			key,
			emptyVal,
			block3Val,
		},
		{
			addrs[2],
			1,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			1,
			key,
			emptyVal,
			block3Val,
		},
	}, false, false)

	writeStorageBlockData(t, tds, 5, []storageData{
		{
			addrs[0],
			1,
			key,
			block3Val,
			stateVal,
		},
		{
			addrs[1],
			1,
			key,
			emptyVal,
			stateVal,
		},
		{
			addrs[3],
			1,
			key,
			block3Val,
			emptyVal,
		},
	}, false, false)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 0)
	err := db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.StorageHistoryIndex], b)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(b, 7)
	err = db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.Execution], b)
	if err != nil {
		t.Fatal(err)
	}

	//walk and collect walkAsOf result
	var startKey [72]byte
	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes = []changeset.Change{
		{
			withoutInc(addrHashes[0], keyHash),
			block3Val.Bytes(),
		},
		{
			withoutInc(addrHashes[2], keyHash),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrHashes[3], keyHash),
			block3Val.Bytes(),
		},
	}

	assertChangesEquals(t, block4, block4Expected)

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	block6Expected.Changes = []changeset.Change{
		{
			withoutInc(addrHashes[0], keyHash),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrHashes[1], keyHash),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrHashes[2], keyHash),
			stateVal.Bytes(),
		},
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfStatePlain_WithoutIndex(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)

	emptyVal := uint256.NewInt()
	block3Val := uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	key := common.Hash{123}
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	withoutInc := func(addrHash common.Address, keyHash common.Hash) []byte {
		expectedKey := make([]byte, common.AddressLength+common.HashLength)
		copy(expectedKey[:common.AddressLength], addrHash.Bytes())
		copy(expectedKey[common.AddressLength:], keyHash.Bytes())
		return expectedKey
	}
	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	writeStorageBlockData(t, tds, 3, []storageData{
		{
			addrs[0], 1, key, emptyVal, block3Val,
		},
		{
			addrs[2], 1, key, emptyVal, stateVal,
		},
		{
			addrs[3], 1, key, emptyVal, block3Val,
		},
	}, true, false)

	writeStorageBlockData(t, tds, 5, []storageData{
		{
			addrs[0], 1, key, block3Val, stateVal,
		},
		{
			addrs[1], 1, key, emptyVal, stateVal,
		},
		{
			addrs[3], 1, key, block3Val, emptyVal,
		},
	}, true, false)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 0)
	err := db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.StorageHistoryIndex], b)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(b, 7)
	err = db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.Execution], b)
	if err != nil {
		t.Fatal(err)
	}

	var startKey [60]byte
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		err = block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	block4Expected.Changes = []changeset.Change{
		{
			withoutInc(addrs[0], key),
			block3Val.Bytes(),
		},
		{
			withoutInc(addrs[2], key),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrs[3], key),
			block3Val.Bytes(),
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		err = block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block6Expected.Changes = []changeset.Change{
		{
			withoutInc(addrs[0], key),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrs[1], key),
			stateVal.Bytes(),
		},
		{
			withoutInc(addrs[2], key),
			stateVal.Bytes(),
		},
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountHashed_WithoutIndex(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	emptyValAcc := accounts.NewAccount()
	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc := emptyValAcc.SelfCopy()
	block3ValAcc.Nonce = 3
	block3ValAcc.Initialised = true
	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc := emptyValAcc.SelfCopy()
	stateValAcc.Nonce = 5
	stateValAcc.Initialised = true
	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	writeBlockData(t, tds, 3, []accData{
		{
			addrs[0],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[2],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[3],
			&emptyValAcc,
			block3ValAcc,
		},
	}, false, false)

	writeBlockData(t, tds, 5, []accData{
		{
			addrs[0], block3ValAcc, stateValAcc,
		},
		{
			addrs[1], &emptyValAcc, stateValAcc,
		},
		{
			addrs[3], block3ValAcc, nil,
		},
	}, false, false)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 0)
	err := db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.AccountHistoryIndex], b)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(b, 7)
	err = db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.Execution], b)
	if err != nil {
		t.Fatal(err)
	}

	var startKey [32]byte
	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrHashes[0].Bytes(),
				block3Val,
			},
			{
				addrHashes[2].Bytes(),
				block3Val,
			},
			{
				addrHashes[3].Bytes(),
				block3Val,
			},
		},
	}

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertChangesEquals(t, block4, block4Expected)
	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrHashes[0].Bytes(),
				stateVal,
			},
			{
				addrHashes[1].Bytes(),
				stateVal,
			},
			{
				addrHashes[2].Bytes(),
				block3Val,
			},
		},
	}

	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountPlain_WithoutIndex(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	emptyValAcc := accounts.NewAccount()
	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc := emptyValAcc.SelfCopy()
	block3ValAcc.Nonce = 3
	block3ValAcc.Initialised = true
	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc := emptyValAcc.SelfCopy()
	stateValAcc.Nonce = 5
	stateValAcc.Initialised = true
	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	writeBlockData(t, tds, 3, []accData{
		{
			addrs[0],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[2],
			&emptyValAcc,
			block3ValAcc,
		},
		{
			addrs[3],
			&emptyValAcc,
			block3ValAcc,
		},
	}, true, false)

	writeBlockData(t, tds, 5, []accData{
		{
			addrs[0],
			block3ValAcc,
			stateValAcc,
		},
		{
			addrs[1],
			&emptyValAcc,
			stateValAcc,
		},
		{
			addrs[3],
			block3ValAcc,
			nil,
		},
	}, true, false)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, 0)
	err := db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.AccountHistoryIndex], b)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(b, 7)
	err = db.Put(dbutils.SyncStageProgress, stages.DBKeys[stages.Execution], b)
	if err != nil {
		t.Fatal(err)
	}

	var startKey [32]byte
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 4, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	block4Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrs[0].Bytes(),
				block3Val,
			},
			{
				addrs[2].Bytes(),
				block3Val,
			},
			{
				addrs[3].Bytes(),
				block3Val,
			},
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	err = WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, 6, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	block6Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				addrs[0].Bytes(),
				stateVal,
			},
			{
				addrs[1].Bytes(),
				stateVal,
			},
			{
				addrs[2].Bytes(),
				block3Val,
			},
		},
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountPlain_WithChunks(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	emptyValAcc := accounts.NewAccount()
	emptyVal := make([]byte, emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc := emptyValAcc.SelfCopy()
	block3ValAcc.Nonce = 3
	block3ValAcc.Initialised = true
	block3Val := make([]byte, block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc := emptyValAcc.SelfCopy()
	stateValAcc.Nonce = 5
	stateValAcc.Initialised = true
	stateVal := make([]byte, stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	addr1Old := emptyValAcc.SelfCopy()
	addr1Old.Initialised = true
	addr1Old.Nonce = 1
	addr2Old := emptyValAcc.SelfCopy()
	addr2Old.Initialised = true
	addr2Old.Nonce = 1
	addr3Old := emptyValAcc.SelfCopy()
	addr3Old.Initialised = true
	addr3Old.Nonce = 1

	var addr1New, addr2New, addr3New *accounts.Account

	writeBlockData(t, tds, 1, []accData{
		{
			addrs[0],
			&emptyValAcc,
			addr1Old,
		},
		{
			addrs[1],
			&emptyValAcc,
			addr1Old,
		},
		{
			addrs[2],
			&emptyValAcc,
			addr1Old,
		},
	}, true, true)

	for i := 2; i < 1100; i++ {
		addr1New = addr1Old.SelfCopy()
		addr1New.Nonce = uint64(i)
		addr2New = addr2Old.SelfCopy()
		addr2New.Nonce = uint64(i)
		addr3New = addr3Old.SelfCopy()
		addr3New.Nonce = uint64(i)
		writeBlockData(t, tds, uint64(i), []accData{
			{
				addrs[0],
				addr1Old,
				addr1New,
			},
			{
				addrs[1],
				addr2Old,
				addr2New,
			},
			{
				addrs[2],
				addr3Old,
				addr3New,
			},
		}, true, true)
		addr1Old = addr1New.SelfCopy()
		addr2Old = addr2New.SelfCopy()
		addr3Old = addr3New.SelfCopy()
	}

	addr1New = addr1Old.SelfCopy()
	addr1New.Nonce = 1100
	addr2New = addr2Old.SelfCopy()
	addr2New.Nonce = 1100
	addr3New = addr3Old.SelfCopy()
	addr3New.Nonce = 1100

	writeBlockData(t, tds, 1100, []accData{
		{
			addrs[0],
			addr1Old,
			addr1New,
		},
		{
			addrs[1],
			addr1Old,
			addr1New,
		},
		{
			addrs[2],
			addr1Old,
			addr1New,
		},
	}, true, true)

	for _, blockNum := range []uint64{5, 100, 1000, 1050} {
		obtained := &changeset.ChangeSet{
			Changes: make([]changeset.Change, 0),
		}

		var startKey [20]byte
		err := WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.AccountsHistoryBucket, startKey[:], 0, blockNum, func(k []byte, v []byte) (b bool, e error) {
			innerErr := obtained.Add(common.CopyBytes(k), common.CopyBytes(v))
			if innerErr != nil {
				t.Fatal(innerErr)
			}
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		acc := addr1Old.SelfCopy()
		acc.Nonce = blockNum - 1
		accBytes := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(accBytes)
		expected := &changeset.ChangeSet{
			Changes: []changeset.Change{
				{
					Key:   addrs[0].Bytes(),
					Value: accBytes,
				},
				{
					Key:   addrs[1].Bytes(),
					Value: accBytes,
				},
				{
					Key:   addrs[2].Bytes(),
					Value: accBytes,
				},
			},
		}
		assertChangesEquals(t, obtained, expected)
	}
}

func TestWalkAsOfStoragePlain_WithChunks(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}
	key := common.Hash{123}
	emptyVal := uint256.NewInt()

	val := uint256.NewInt().SetBytes([]byte("block 1"))
	writeStorageBlockData(t, tds, 1, []storageData{
		{
			addrs[0],
			1,
			key,
			emptyVal,
			val,
		},
		{
			addrs[1],
			1,
			key,
			emptyVal,
			val,
		},
		{
			addrs[2],
			1,
			key,
			emptyVal,
			val,
		},
	}, true, true)

	prev := val
	for i := 2; i < 1100; i++ {
		val = uint256.NewInt().SetBytes([]byte("block " + strconv.Itoa(i)))
		writeStorageBlockData(t, tds, uint64(i), []storageData{
			{
				addrs[0],
				1,
				key,
				prev,
				val,
			},
			{
				addrs[1],
				1,
				key,
				prev,
				val,
			},
			{
				addrs[2],
				1,
				key,
				prev,
				val,
			},
		}, true, true)
		prev = val
	}

	val = uint256.NewInt().SetBytes([]byte("block 1100"))

	writeStorageBlockData(t, tds, 1100, []storageData{
		{
			addrs[0],
			1,
			key,
			prev,
			val,
		},
		{
			addrs[1],
			1,
			key,
			prev,
			val,
		},
		{
			addrs[2],
			1,
			key,
			prev,
			val,
		},
	}, true, true)

	for _, blockNum := range []uint64{5, 100, 1000, 1050} {
		obtained := &changeset.ChangeSet{
			Changes: make([]changeset.Change, 0),
		}

		var startKey [20]byte
		err := WalkAsOf(db.KV(), dbutils.PlainStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, blockNum, func(k []byte, v []byte) (b bool, e error) {
			innerErr := obtained.Add(common.CopyBytes(k), common.CopyBytes(v))
			if innerErr != nil {
				t.Fatal(innerErr)
			}
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		valBytes := uint256.NewInt().SetBytes([]byte("block " + strconv.FormatUint(blockNum-1, 10))).Bytes()
		expected := &changeset.ChangeSet{
			Changes: []changeset.Change{
				{
					Key:   append(addrs[0].Bytes(), key.Bytes()...),
					Value: valBytes,
				},
				{
					Key:   append(addrs[1].Bytes(), key.Bytes()...),
					Value: valBytes,
				},
				{
					Key:   append(addrs[2].Bytes(), key.Bytes()...),
					Value: valBytes,
				},
			},
		}
		assertChangesEquals(t, obtained, expected)
	}
}

type accData struct {
	addr   common.Address
	oldVal *accounts.Account
	newVal *accounts.Account
}

func writeBlockData(t *testing.T, tds *TrieDbState, blockNum uint64, data []accData, plain, writeHistory bool) {
	tds.SetBlockNr(blockNum)
	var blockWriter WriterWithChangeSets
	if plain {
		blockWriter = tds.PlainStateWriter()
	} else {
		blockWriter = tds.DbStateWriter()
	}

	for i := range data {
		if data[i].newVal != nil {
			if err := blockWriter.UpdateAccountData(context.Background(), data[i].addr, data[i].oldVal, data[i].newVal); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := blockWriter.DeleteAccount(context.Background(), data[i].addr, data[i].oldVal); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if writeHistory {
		if err := blockWriter.WriteHistory(); err != nil {
			t.Fatal(err)
		}
	}
}

type storageData struct {
	addr   common.Address
	inc    uint64
	key    common.Hash
	oldVal *uint256.Int
	newVal *uint256.Int
}

func writeStorageBlockData(t *testing.T, tds *TrieDbState, blockNum uint64, data []storageData, plain, writeHistory bool) {
	tds.SetBlockNr(blockNum)
	var blockWriter WriterWithChangeSets
	if plain {
		blockWriter = tds.PlainStateWriter()
	} else {
		blockWriter = tds.DbStateWriter()
	}

	for i := range data {
		if err := blockWriter.WriteAccountStorage(context.Background(),
			data[i].addr,
			data[i].inc,
			&data[i].key,
			data[i].oldVal,
			data[i].newVal); err != nil {
			t.Fatal(err)
		}
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if writeHistory {
		if err := blockWriter.WriteHistory(); err != nil {
			t.Fatal(err)
		}
	}
}
func assertChangesEquals(t *testing.T, changesObtained, changesExpected *changeset.ChangeSet) {
	t.Helper()
	sort.Sort(changesObtained)
	sort.Sort(changesExpected)
	if !reflect.DeepEqual(changesObtained, changesExpected) {
		fmt.Println("expected:")
		fmt.Println(changesExpected.String())
		fmt.Println("obtained:")
		fmt.Println(changesObtained.String())
		t.Fatal("block result is incorrect")
	}
}
