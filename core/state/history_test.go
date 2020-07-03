package state

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"sort"
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

func TestWalkAsOfStateHashed(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	ctx := context.Background()
	emptyVal := uint256.NewInt()
	block3Val:= uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal:= uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts:=uint8(4)
	addrs:=make([]common.Address, numOfAccounts)
	addrHashes:=make([]common.Hash, numOfAccounts)
	key:=common.Hash{123}
	keyHash,_:=common.HashData(key.Bytes())
	for i:=uint8(0); i<numOfAccounts; i++ {
		addrs[i] = common.Address{i+1}
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

	withoutInc:= func(addrHash, keyHash common.Hash) []byte {
		expectedKey:=make([]byte, common.HashLength*2)
		copy(expectedKey[:common.HashLength], addrHash.Bytes())
		copy(expectedKey[common.HashLength:], keyHash.Bytes())
		return expectedKey
	}
	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}


	tds.SetBlockNr(3)
	blockWriter := tds.DbStateWriter()
	if err := blockWriter.WriteAccountStorage(ctx, addrs[0], 1, &key, emptyVal, block3Val); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[2], 1, &key, emptyVal, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[3], 1, &key, emptyVal, block3Val); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

	//walk and collect walkAsOf result
	var err error
	var startKey [72]byte
	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		fmt.Println("core/state/history_test.go:435",len(k), common.Bytes2Hex(k))
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Sort(block2Expected)
	sort.Sort(block2)
	if !reflect.DeepEqual(block2, block2Expected) {
		t.Log("expected:")
		fmt.Println(block2Expected.String())
		t.Log("obtained:", )
		fmt.Println(block2.String())
		t.Fatal("block 2 result is incorrect")
	}


	tds.SetBlockNr(5)
	blockWriter = tds.DbStateWriter()
	if err := blockWriter.WriteAccountStorage(ctx, addrs[0], 1, &key, block3Val, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[1], 1, &key, emptyVal, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[3], 1, &key, block3Val, emptyVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes=append(block4Expected.Changes, changeset.Change{
		withoutInc(addrHashes[0],keyHash), block3Val.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[2],keyHash), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[3],keyHash), block3Val.Bytes(),
	},)

	block6Expected.Changes=append(block6Expected.Changes, changeset.Change{
		withoutInc(addrHashes[0],keyHash), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[1],keyHash), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[2],keyHash), stateVal.Bytes(),
	},)

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

	sort.Sort(block4Expected)
	if !reflect.DeepEqual(block4, block4Expected) {
		spew.Dump("expected", block4Expected)
		spew.Dump("current", block4)
		t.Fatal("block 4 result is incorrect")
	}
	sort.Sort(block6Expected)
	if !reflect.DeepEqual(block6, block6Expected) {
		spew.Dump("expected", block6Expected)
		spew.Dump("current", block6)
		t.Fatal("block 6 result is incorrect")
	}
}

func TestWalkAsOfStatePlain(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	ctx := context.Background()
	emptyVal := uint256.NewInt()
	block3Val:= uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal:= uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts:=uint8(4)
	addrs:=make([]common.Address, numOfAccounts)
	addrHashes:=make([]common.Hash, numOfAccounts)
	key:=common.Hash{123}
	for i:=uint8(0); i<numOfAccounts; i++ {
		addrs[i] = common.Address{i+1}
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

	withoutInc:= func(addr common.Address, keyHash common.Hash) []byte {
		expectedKey:=make([]byte, common.HashLength+common.AddressLength)
		copy(expectedKey[:common.AddressLength], addr.Bytes())
		copy(expectedKey[common.AddressLength:], keyHash.Bytes())
		return expectedKey
	}
	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}


	tds.SetBlockNr(3)
	blockWriter := tds.PlainStateWriter()
	if err := blockWriter.WriteAccountStorage(ctx, addrs[0], 1, &key, emptyVal, block3Val); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[2], 1, &key, emptyVal, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[3], 1, &key, emptyVal, block3Val); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

	tds.SetBlockNr(5)
	blockWriter = tds.PlainStateWriter()
	if err := blockWriter.WriteAccountStorage(ctx, addrs[0], 1, &key, block3Val, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[1], 1, &key, emptyVal, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[3], 1, &key, block3Val, emptyVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
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
	sort.Sort(block2Expected)
	sort.Sort(block2)
	if !reflect.DeepEqual(block2, block2Expected) {
		t.Log("expected:")
		fmt.Println(block2Expected.String())
		t.Log("obtained:", )
		fmt.Println(block2.String())
		t.Fatal("block 2 result is incorrect")
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

	block4Expected.Changes=append(block4Expected.Changes, changeset.Change{
		withoutInc(addrs[0],key), block3Val.Bytes(),
	},changeset.Change{
		withoutInc(addrs[2],key), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrs[3],key), block3Val.Bytes(),
	},)

	sort.Sort(block4Expected)
	if !reflect.DeepEqual(block4, block4Expected) {
		spew.Dump("expected", block4Expected)
		spew.Dump("current", block4)
		t.Fatal("block 4 result is incorrect")
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

	block6Expected.Changes=append(block6Expected.Changes, changeset.Change{
		withoutInc(addrs[0],key), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrs[1],key), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrs[2],key), stateVal.Bytes(),
	},)

	sort.Sort(block6Expected)
	if !reflect.DeepEqual(block6, block6Expected) {
		spew.Dump("expected", block6Expected)
		spew.Dump("current", block6)
		t.Fatal("block 6 result is incorrect")
	}
}

func TestWalkAsOfAccountHashed(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	ctx := context.Background()
	emptyValAcc := accounts.NewAccount()
	emptyVal:=make([]byte,emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc:= emptyValAcc.SelfCopy()
	block3ValAcc.Nonce=3
	block3ValAcc.Initialised=true
	block3Val:=make([]byte,block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc:= emptyValAcc.SelfCopy()
	stateValAcc.Nonce=5
	stateValAcc.Initialised=true
	stateVal:=make([]byte,stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	/*
	block 3 acc 0, 2,3 - create
	block 5 acc 0,1 - create,  3 delete

	walk as of
	block 2 - empty
	block 4 - 0,2,3 - block 3
	block 6 - 0, 1 - state, 2 - block 3
	 */

	numOfAccounts:=uint8(4)
	addrs:=make([]common.Address, numOfAccounts)
	addrHashes:=make([]common.Hash, numOfAccounts)
	for i:=uint8(0); i<numOfAccounts; i++ {
		addrs[i] = common.Address{i+1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	tds.SetBlockNr(3)
	blockWriter := tds.DbStateWriter()

	if err := blockWriter.UpdateAccountData(ctx, addrs[0], &emptyValAcc, block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.UpdateAccountData(ctx, addrs[2], &emptyValAcc, block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.UpdateAccountData(ctx, addrs[3], &emptyValAcc, block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

	tds.SetBlockNr(5)
	blockWriter = tds.DbStateWriter()

	if err := blockWriter.UpdateAccountData(ctx, addrs[0], block3ValAcc, stateValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.UpdateAccountData(ctx, addrs[1], &emptyValAcc, stateValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.DeleteAccount(ctx, addrs[3], block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
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
	sort.Sort(block2Expected)
	sort.Sort(block2)
	if !reflect.DeepEqual(block2, block2Expected) {
		fmt.Println("expected:")
		fmt.Println(block2Expected.String())
		fmt.Println("obtained:")
		fmt.Println(block2.String())
		t.Fatal("block 2 result is incorrect")
	}


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

	sort.Sort(block4Expected)
	sort.Sort(block4)
	if !reflect.DeepEqual(block4, block4Expected) {
		fmt.Println("expected:")
		fmt.Println(block4Expected.String())
		fmt.Println("obtained:", )
		fmt.Println(block4.String())
		t.Fatal("block 4 result is incorrect")
	}

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

	sort.Sort(block6Expected)
	sort.Sort(block6)
	if !reflect.DeepEqual(block6, block6Expected) {
		fmt.Println("expected:")
		fmt.Println(block6Expected.String())
		fmt.Println("obtained:")
		fmt.Println(block6.String())
		t.Fatal("block 6 result is incorrect")
	}
}

func TestWalkAsOfAccountPlain(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	ctx := context.Background()
	emptyValAcc := accounts.NewAccount()
	emptyVal:=make([]byte,emptyValAcc.EncodingLengthForStorage())
	emptyValAcc.EncodeForStorage(emptyVal)

	block3ValAcc:= emptyValAcc.SelfCopy()
	block3ValAcc.Nonce=3
	block3ValAcc.Initialised=true
	block3Val:=make([]byte,block3ValAcc.EncodingLengthForStorage())
	block3ValAcc.EncodeForStorage(block3Val)

	stateValAcc:= emptyValAcc.SelfCopy()
	stateValAcc.Nonce=5
	stateValAcc.Initialised=true
	stateVal:=make([]byte,stateValAcc.EncodingLengthForStorage())
	stateValAcc.EncodeForStorage(stateVal)

	/*
	block 3 acc 0, 2,3 - create
	block 5 acc 0,1 - create,  3 delete

	walk as of
	block 2 - empty
	block 4 - 0,2,3 - block 3
	block 6 - 0, 1 - state, 2 - block 3
	 */

	numOfAccounts:=uint8(4)
	addrs:=make([]common.Address, numOfAccounts)
	addrHashes:=make([]common.Hash, numOfAccounts)
	for i:=uint8(0); i<numOfAccounts; i++ {
		addrs[i] = common.Address{i+1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block2Expected := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	tds.SetBlockNr(3)
	blockWriter := tds.PlainStateWriter()

	if err := blockWriter.UpdateAccountData(ctx, addrs[0], &emptyValAcc, block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.UpdateAccountData(ctx, addrs[2], &emptyValAcc, block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.UpdateAccountData(ctx, addrs[3], &emptyValAcc, block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

	tds.SetBlockNr(5)
	blockWriter = tds.PlainStateWriter()

	if err := blockWriter.UpdateAccountData(ctx, addrs[0], block3ValAcc, stateValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.UpdateAccountData(ctx, addrs[1], &emptyValAcc, stateValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.DeleteAccount(ctx, addrs[3], block3ValAcc); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

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
	sort.Sort(block2Expected)
	sort.Sort(block2)
	if !reflect.DeepEqual(block2, block2Expected) {
		fmt.Println("expected:")
		fmt.Println(block2Expected.String())
		fmt.Println("obtained:")
		fmt.Println(block2.String())
		t.Fatal("block 2 result is incorrect")
	}


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

	sort.Sort(block4Expected)
	sort.Sort(block4)
	if !reflect.DeepEqual(block4, block4Expected) {
		fmt.Println("expected:")
		fmt.Println(block4Expected.String())
		fmt.Println("obtained:", )
		fmt.Println(block4.String())
		t.Fatal("block 4 result is incorrect")
	}

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

	sort.Sort(block6Expected)
	sort.Sort(block6)
	if !reflect.DeepEqual(block6, block6Expected) {
		fmt.Println("expected:")
		fmt.Println(block6Expected.String())
		fmt.Println("obtained:")
		fmt.Println(block6.String())
		t.Fatal("block 6 result is incorrect")
	}
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








func TestWalkAsOfStateHashed_WithoutIndex(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	tds := NewTrieDbState(common.Hash{}, db, 1)
	ctx := context.Background()
	emptyVal := uint256.NewInt()
	block3Val:= uint256.NewInt().SetBytes([]byte("block 3"))
	stateVal:= uint256.NewInt().SetBytes([]byte("state"))
	numOfAccounts:=uint8(4)
	addrs:=make([]common.Address, numOfAccounts)
	addrHashes:=make([]common.Hash, numOfAccounts)
	key:=common.Hash{123}
	keyHash,_:=common.HashData(key.Bytes())
	for i:=uint8(0); i<numOfAccounts; i++ {
		addrs[i] = common.Address{i+1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		fmt.Println("core/state/history_test.go:1159", i+1, common.Bytes2Hex(addrHash.Bytes()))
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

	withoutInc:= func(addrHash, keyHash common.Hash) []byte {
		expectedKey:=make([]byte, common.HashLength*2)
		copy(expectedKey[:common.HashLength], addrHash.Bytes())
		copy(expectedKey[common.HashLength:], keyHash.Bytes())
		return expectedKey
	}
	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}


	tds.SetBlockNr(3)
	blockWriter := tds.DbStateWriter()
	if err := blockWriter.WriteAccountStorage(ctx, addrs[0], 1, &key, emptyVal, block3Val); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[2], 1, &key, emptyVal, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[3], 1, &key, emptyVal, block3Val); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}




	tds.SetBlockNr(5)
	blockWriter = tds.DbStateWriter()
	if err := blockWriter.WriteAccountStorage(ctx, addrs[0], 1, &key, block3Val, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[1], 1, &key, emptyVal, stateVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteAccountStorage(ctx, addrs[3], 1, &key, block3Val, emptyVal); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes=append(block4Expected.Changes, changeset.Change{
		withoutInc(addrHashes[0],keyHash), block3Val.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[2],keyHash), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[3],keyHash), block3Val.Bytes(),
	},)

	block6Expected.Changes=append(block6Expected.Changes, changeset.Change{
		withoutInc(addrHashes[0],keyHash), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[1],keyHash), stateVal.Bytes(),
	},changeset.Change{
		withoutInc(addrHashes[2],keyHash), stateVal.Bytes(),
	},)


	//walk and collect walkAsOf result
	var err error
	var startKey [72]byte
	err = WalkAsOf(db.KV(), dbutils.CurrentStateBucket, dbutils.StorageHistoryBucket, startKey[:], 0, 2, func(k []byte, v []byte) (b bool, e error) {
		fmt.Println("core/state/history_test.go:435",len(k), common.Bytes2Hex(k))
		err = block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}

		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Sort(block2Expected)
	sort.Sort(block2)
	if !reflect.DeepEqual(block2, block2Expected) {
		fmt.Println("expected:")
		fmt.Println(block2Expected.String())
		fmt.Println("obtained:", )
		fmt.Println(block2.String())
		t.Fatal("block 2 result is incorrect")
	}


	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	fmt.Println("expected:")
	fmt.Println(block4Expected.String())

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

	/*
	obtained:
	block 4
	addr1(f22b):"block3"
	addr2(1f0e):""
	addr3(3e05):""
	addr4(d12e):"block3"


	*/

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
	sort.Sort(block4Expected)
	if !reflect.DeepEqual(block4, block4Expected) {
		fmt.Println("expected:")
		fmt.Println(block4Expected.String())
		fmt.Println("obtained:", )
		fmt.Println(block4.String())
		t.Fatal("block 4 result is incorrect")
	}


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

	sort.Sort(block6Expected)
	if !reflect.DeepEqual(block6, block6Expected) {
		fmt.Println("expected:")
		fmt.Println(block6Expected.String())
		fmt.Println("obtained:", )
		fmt.Println(block6.String())
		t.Fatal("block 6 result is incorrect")
	}
}
//
//func TestChangesetSearchDecorator_Next(t *testing.T) {
//	db:=ethdb.NewMemDatabase()
//	acc1:=common.Address{1}
//	acc2:=common.Address{2}
//	key:=common.Hash{1}
//	type nextRes struct {
//		k1,k2,k3,v []byte
//	}
//	addrHash1,err:=common.HashData(acc1.Bytes())
//	if err!=nil {
//		t.Fatal(err)
//	}
//	addrHash2,err:=common.HashData(acc2.Bytes())
//	if err!=nil {
//		t.Fatal(err)
//	}
//	keyHash,err:=common.HashData(key.Bytes())
//	if err!=nil {
//		t.Fatal(err)
//	}
//	//cs:=e
//	//changeset.EncodeStorage()
//	res:=[]nextRes{}
//	var startKey = make([]byte,72)
//	db.KV().View(context.Background(), func(tx ethdb.Tx) error {
//		hB := tx.Bucket(dbutils.StorageHistoryBucket)
//		csBucket:= dbutils.StorageChangeSetBucket
//		csB := tx.Bucket(csBucket)
//		part1End:=common.HashLength
//		part2Start:=common.HashLength+common.IncarnationLength
//		part3Start:=common.HashLength+common.IncarnationLength+common.HashLength
//		startkeyNoInc:=dbutils.CompositeKeyWithoutIncarnation(startKey)
//		//for historic data
//		var historyCursor historyCursor = ethdb.NewSplitCursor(
//			hB,
//			startkeyNoInc,
//			0,
//			part1End,   /* part1end */
//			part2Start,   /* part2start */
//			part3Start, /* part3start */
//		)
//
//		part1End=common.HashLength
//		part2Start=common.HashLength+common.IncarnationLength
//		part3Start=common.HashLength+common.IncarnationLength+common.HashLength
//
//		decorator:=NewChangesetSearchDecorator(historyCursor, csB, startKey, 0, part1End, part2Start, part3Start, 2, changeset.Mapper[string(dbutils.StorageChangeSetBucket)].WalkerAdapter)
//		err:=decorator.buildChangeset(4, 7)
//		if err!=nil {
//			return err
//		}
//
//		k1,k2,k3,v,err:=decorator.Next()
//		for ;k1!=nil; {
//			res = append(res, nextRes{
//				k1, k2,k3, v,
//			})
//			k1,k2,k3,v,err=decorator.Next()
//		}
//		if err!=nil {
//			t.Fatal(err)
//		}
//
//
//		return nil
//	})
//	fmt.Println(res)
//}