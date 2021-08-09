package state

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutationDeleteTimestamp(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	acc := make([]*accounts.Account, 10)
	addr := make([]common.Address, 10)
	blockWriter := NewPlainStateWriter(tx, tx, 1)
	emptyAccount := accounts.NewAccount()
	for i := range acc {
		acc[i], addr[i] = randomAccount(t)
		if err := blockWriter.UpdateAccountData(addr[i], &emptyAccount, acc[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}

	i := 0
	err := changeset.Walk(tx, kv.AccountChangeSet, nil, 0, func(blockN uint64, k, v []byte) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if i != 10 {
		t.FailNow()
	}

	index, err := bitmapdb.Get64(tx, kv.AccountsHistory, addr[0].Bytes(), 0, math.MaxUint32)
	if err != nil {
		t.Fatal(err)
	}

	parsed := index.ToArray()
	if parsed[0] != 1 {
		t.Fatal("incorrect block num")
	}

	count := 0
	err = changeset.Walk(tx, kv.StorageChangeSet, dbutils.EncodeBlockNumber(1), 8*8, func(blockN uint64, k, v []byte) (bool, error) {
		count++
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("changeset must be deleted")
	}

	found, err := tx.GetOne(kv.AccountsHistory, addr[0].Bytes())
	require.NoError(t, err)
	require.Nil(t, found, "account must be deleted")
}

func TestMutationCommit(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	numOfAccounts := 5
	numOfStateKeys := 5

	addrs, accState, accStateStorage, accHistory, accHistoryStateStorage := generateAccountsWithStorageAndHistory(t, NewPlainStateWriter(tx, tx, 2), numOfAccounts, numOfStateKeys)

	for i, addr := range addrs {
		acc, err := NewPlainStateReader(tx).ReadAccountData(addr)

		if err != nil {
			t.Fatal("error on get account", i, err)
		} else if acc == nil {
			t.Fatal("error on get account", i)
		}

		if !accState[i].Equals(acc) {
			spew.Dump("got", acc)
			spew.Dump("expected", accState[i])
			t.Fatal("Accounts not equals")
		}

		index, err := bitmapdb.Get64(tx, kv.AccountsHistory, addr.Bytes(), 0, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}

		parsedIndex := index.ToArray()
		if parsedIndex[0] != 2 || index.GetCardinality() != 1 {
			t.Fatal("incorrect history index")
		}

		resAccStorage := make(map[common.Hash]uint256.Int)
		err = tx.ForPrefix(kv.PlainState, dbutils.PlainGenerateStoragePrefix(addr[:], acc.Incarnation), func(k, v []byte) error {
			resAccStorage[common.BytesToHash(k[common.AddressLength+8:])] = *uint256.NewInt(0).SetBytes(v)
			return nil
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
			res, err := GetAsOf(tx, true /* storage */, dbutils.PlainGenerateCompositeStorageKey(addr.Bytes(), acc.Incarnation, k.Bytes()), 1)
			if err != nil {
				t.Fatal(err)
			}

			result := uint256.NewInt(0).SetBytes(res)
			if !v.Eq(result) {
				t.Fatalf("incorrect storage history for %x %x %x", addr.String(), v, result)
			}
		}
	}

	changeSetInDB := changeset.NewAccountChangeSet()
	err := changeset.Walk(tx, kv.AccountChangeSet, dbutils.EncodeBlockNumber(2), 8*8, func(_ uint64, k, v []byte) (bool, error) {
		if err := changeSetInDB.Add(k, v); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedChangeSet := changeset.NewAccountChangeSet()
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
	if !reflect.DeepEqual(changeSetInDB, expectedChangeSet) {
		spew.Dump("res", changeSetInDB)
		spew.Dump("expected", expectedChangeSet)
		t.Fatal("incorrect changeset")
	}

	cs := changeset.NewStorageChangeSet()
	err = changeset.Walk(tx, kv.StorageChangeSet, dbutils.EncodeBlockNumber(2), 8*8, func(_ uint64, k, v []byte) (bool, error) {
		if err2 := cs.Add(k, v); err2 != nil {
			return false, err2
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if cs.Len() != numOfAccounts*numOfStateKeys {
		t.Errorf("Length does not match, got %d, expected %d", cs.Len(), numOfAccounts*numOfStateKeys)
	}

	expectedChangeSet = changeset.NewStorageChangeSet()
	for i, addr := range addrs {
		for j := 0; j < numOfStateKeys; j++ {
			key := common.Hash{uint8(i*100 + j)}
			value := uint256.NewInt(uint64(10 + j))
			if err2 := expectedChangeSet.Add(dbutils.PlainGenerateCompositeStorageKey(addr.Bytes(), accHistory[i].Incarnation, key.Bytes()), value.Bytes()); err2 != nil {
				t.Fatal(err2)
			}
		}
	}
	sort.Sort(expectedChangeSet)

	assert.Equal(t, cs, expectedChangeSet)
}

func generateAccountsWithStorageAndHistory(t *testing.T, blockWriter *PlainStateWriter, numOfAccounts, numOfStateKeys int) ([]common.Address, []*accounts.Account, []map[common.Hash]uint256.Int, []*accounts.Account, []map[common.Hash]uint256.Int) {
	t.Helper()

	accHistory := make([]*accounts.Account, numOfAccounts)
	accState := make([]*accounts.Account, numOfAccounts)
	accStateStorage := make([]map[common.Hash]uint256.Int, numOfAccounts)
	accHistoryStateStorage := make([]map[common.Hash]uint256.Int, numOfAccounts)
	addrs := make([]common.Address, numOfAccounts)
	for i := range accHistory {
		accHistory[i], addrs[i] = randomAccount(t)
		accHistory[i].Balance = *uint256.NewInt(100)
		accHistory[i].CodeHash = common.Hash{uint8(10 + i)}
		accHistory[i].Root = common.Hash{uint8(10 + i)}
		accHistory[i].Incarnation = uint64(i + 1)

		accState[i] = accHistory[i].SelfCopy()
		accState[i].Nonce++
		accState[i].Balance = *uint256.NewInt(200)

		accStateStorage[i] = make(map[common.Hash]uint256.Int)
		accHistoryStateStorage[i] = make(map[common.Hash]uint256.Int)
		for j := 0; j < numOfStateKeys; j++ {
			key := common.Hash{uint8(i*100 + j)}
			newValue := uint256.NewInt(uint64(j))
			if !newValue.IsZero() {
				// Empty value is not considered to be present
				accStateStorage[i][key] = *newValue
			}

			value := uint256.NewInt(uint64(10 + j))
			accHistoryStateStorage[i][key] = *value
			if err := blockWriter.WriteAccountStorage(addrs[i], accHistory[i].Incarnation, &key, value, newValue); err != nil {
				t.Fatal(err)
			}
		}
		if err := blockWriter.UpdateAccountData(addrs[i], accHistory[i], accState[i]); err != nil {
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

func randomAccount(t *testing.T) (*accounts.Account, common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	acc := accounts.NewAccount()
	acc.Initialised = true
	acc.Balance = *uint256.NewInt(uint64(rand.Int63()))
	addr := crypto.PubkeyToAddress(key.PublicKey)
	return &acc, addr
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

func TestWalkAsOfStatePlain(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	emptyVal := uint256.NewInt(0)
	block3Val := uint256.NewInt(0).SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt(0).SetBytes([]byte("state"))
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

	writeStorageBlockData(t, NewPlainStateWriter(tx, tx, 3), []storageData{
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
	})

	writeStorageBlockData(t, NewPlainStateWriter(tx, tx, 5), []storageData{
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
	})

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	for _, addr := range addrs {
		if err := WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 2, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
			err := block2.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
			if err != nil {
				t.Fatal(err)
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	for _, addr := range addrs {
		if err := WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 4, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
			err := block4.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
			if err != nil {
				t.Fatal(err)
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	block4Expected.Changes = []changeset.Change{
		{
			Key:   withoutInc(addrs[0], key),
			Value: block3Val.Bytes(),
		},
		{
			Key:   withoutInc(addrs[2], key),
			Value: stateVal.Bytes(),
		},
		{
			Key:   withoutInc(addrs[3], key),
			Value: block3Val.Bytes(),
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	for _, addr := range addrs {
		if err := WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 6, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
			err := block6.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
			if err != nil {
				t.Fatal(err)
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	block6Expected.Changes = []changeset.Change{
		{
			Key:   withoutInc(addrs[0], key),
			Value: stateVal.Bytes(),
		},
		{
			Key:   withoutInc(addrs[1], key),
			Value: stateVal.Bytes(),
		},
		{
			Key:   withoutInc(addrs[2], key),
			Value: stateVal.Bytes(),
		},
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfUsingFixedBytesStatePlain(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	emptyVal := uint256.NewInt(0)
	block3Val := uint256.NewInt(0).SetBytes([]byte("block 3"))
	stateVal := uint256.NewInt(0).SetBytes([]byte("state"))

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

	writeStorageBlockData(t, NewPlainStateWriter(tx, tx, 3), []storageData{
		{
			addr:   addr1,
			inc:    changeset.DefaultIncarnation,
			key:    key1,
			oldVal: emptyVal,
			newVal: block3Val,
		},
		{
			addr:   addr1,
			inc:    changeset.DefaultIncarnation,
			key:    key2,
			oldVal: emptyVal,
			newVal: block3Val,
		},
		{
			addr:   addr1,
			inc:    changeset.DefaultIncarnation,
			key:    key3,
			oldVal: emptyVal,
			newVal: block3Val,
		},
		{
			addr:   addr2,
			inc:    changeset.DefaultIncarnation,
			key:    key3,
			oldVal: emptyVal,
			newVal: block3Val,
		},
	})

	writeStorageBlockData(t, NewPlainStateWriter(tx, tx, 5), []storageData{
		{
			addr:   addr1,
			inc:    changeset.DefaultIncarnation,
			key:    key1,
			oldVal: block3Val,
			newVal: stateVal,
		},
		{
			addr:   addr1,
			inc:    changeset.DefaultIncarnation,
			key:    key2,
			oldVal: block3Val,
			newVal: stateVal,
		},
		{
			addr:   addr1,
			inc:    changeset.DefaultIncarnation,
			key:    key3,
			oldVal: block3Val,
			newVal: emptyVal,
		},
		{
			addr:   addr2,
			inc:    changeset.DefaultIncarnation,
			key:    key3,
			oldVal: block3Val,
			newVal: stateVal,
		},
	})

	block2 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	//walk and collect walkAsOf result
	startKey := make([]byte, 60)
	copy(startKey[:common.AddressLength], addr1.Bytes())

	if err := WalkAsOfStorage(tx, addr1, changeset.DefaultIncarnation, common.Hash{}, 2, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
		err := block2.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	if err := WalkAsOfStorage(tx, addr1, changeset.DefaultIncarnation, common.Hash{}, 4, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
		err := block4.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	block4Expected.Changes = []changeset.Change{
		{
			Key:   withoutInc(addr1, key1),
			Value: block3Val.Bytes(),
		},
		{
			Key:   withoutInc(addr1, key2),
			Value: block3Val.Bytes(),
		},
		{
			Key:   withoutInc(addr1, key3),
			Value: block3Val.Bytes(),
		},
	}
	assertChangesEquals(t, block4, block4Expected)

	block4.Changes = block4.Changes[:0]
	for _, addr := range []common.Address{addr1, addr2} {
		if err := WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 4, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
			err := block4.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
			if err != nil {
				t.Fatal(err)
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	block4Expected.Changes = append(block4Expected.Changes, changeset.Change{
		Key:   withoutInc(addr2, key3),
		Value: block3Val.Bytes(),
	})
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}
	if err := WalkAsOfStorage(tx, addr1, changeset.DefaultIncarnation, common.Hash{}, 6, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
		err := block6.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
		if err != nil {
			t.Fatal(err)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	block6Expected.Changes = []changeset.Change{
		{
			Key:   withoutInc(addr1, key1),
			Value: stateVal.Bytes(),
		},
		{
			Key:   withoutInc(addr1, key2),
			Value: stateVal.Bytes(),
		},
	}
	assertChangesEquals(t, block6, block6Expected)

	block6.Changes = block6.Changes[:0]
	for _, addr := range []common.Address{addr1, addr2} {
		if err := WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, 6, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
			err := block6.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v))
			if err != nil {
				t.Fatal(err)
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	block6Expected.Changes = append(block6Expected.Changes, changeset.Change{
		Key:   withoutInc(addr2, key3),
		Value: stateVal.Bytes(),
	})
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountPlain(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

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

	writeBlockData(t, NewPlainStateWriter(tx, tx, 3), []accData{
		{
			addr:   addrs[0],
			oldVal: &emptyValAcc,
			newVal: block3ValAcc,
		},
		{
			addr:   addrs[2],
			oldVal: &emptyValAcc,
			newVal: block3ValAcc,
		},
		{
			addr:   addrs[3],
			oldVal: &emptyValAcc,
			newVal: block3ValAcc,
		},
	})

	writeBlockData(t, NewPlainStateWriter(tx, tx, 5), []accData{
		{
			addr:   addrs[0],
			oldVal: block3ValAcc,
			newVal: stateValAcc,
		},
		{
			addr:   addrs[1],
			oldVal: &emptyValAcc,
			newVal: stateValAcc,
		},
		{
			addr:   addrs[3],
			oldVal: block3ValAcc,
			newVal: nil,
		},
	})

	if err := WalkAsOfAccounts(tx, common.Address{}, 2, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block2.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block2, block2Expected)

	block4 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block4Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				Key:   addrs[0].Bytes(),
				Value: block3Val,
			},
			{
				Key:   addrs[2].Bytes(),
				Value: block3Val,
			},
			{
				Key:   addrs[3].Bytes(),
				Value: block3Val,
			},
		},
	}

	if err := WalkAsOfAccounts(tx, common.Address{}, 4, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block4.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block4, block4Expected)

	block6 := &changeset.ChangeSet{
		Changes: make([]changeset.Change, 0),
	}

	block6Expected := &changeset.ChangeSet{
		Changes: []changeset.Change{
			{
				Key:   addrs[0].Bytes(),
				Value: stateVal,
			},
			{
				Key:   addrs[1].Bytes(),
				Value: stateVal,
			},
			{
				Key:   addrs[2].Bytes(),
				Value: block3Val,
			},
		},
	}

	if err := WalkAsOfAccounts(tx, common.Address{}, 6, func(k []byte, v []byte) (b bool, e error) {
		innerErr := block6.Add(common.CopyBytes(k), common.CopyBytes(v))
		if innerErr != nil {
			t.Fatal(innerErr)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertChangesEquals(t, block6, block6Expected)
}

func TestWalkAsOfAccountPlain_WithChunks(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

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

	writeBlockData(t, NewPlainStateWriter(tx, tx, 1), []accData{
		{
			addr:   addrs[0],
			oldVal: &emptyValAcc,
			newVal: addr1Old,
		},
		{
			addr:   addrs[1],
			oldVal: &emptyValAcc,
			newVal: addr1Old,
		},
		{
			addr:   addrs[2],
			oldVal: &emptyValAcc,
			newVal: addr1Old,
		},
	})

	for i := 2; i < 1100; i++ {
		addr1New = addr1Old.SelfCopy()
		addr1New.Nonce = uint64(i)
		addr2New = addr2Old.SelfCopy()
		addr2New.Nonce = uint64(i)
		addr3New = addr3Old.SelfCopy()
		addr3New.Nonce = uint64(i)
		writeBlockData(t, NewPlainStateWriter(tx, tx, uint64(i)), []accData{
			{
				addr:   addrs[0],
				oldVal: addr1Old,
				newVal: addr1New,
			},
			{
				addr:   addrs[1],
				oldVal: addr2Old,
				newVal: addr2New,
			},
			{
				addr:   addrs[2],
				oldVal: addr3Old,
				newVal: addr3New,
			},
		})
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

	writeBlockData(t, NewPlainStateWriter(tx, tx, 1100), []accData{
		{
			addr:   addrs[0],
			oldVal: addr1Old,
			newVal: addr1New,
		},
		{
			addr:   addrs[1],
			oldVal: addr1Old,
			newVal: addr1New,
		},
		{
			addr:   addrs[2],
			oldVal: addr1Old,
			newVal: addr1New,
		},
	})

	for _, blockNum := range []uint64{5, 100, 1000, 1050} {
		obtained := &changeset.ChangeSet{
			Changes: make([]changeset.Change, 0),
		}

		if err := WalkAsOfAccounts(tx, common.Address{}, blockNum, func(k []byte, v []byte) (b bool, e error) {
			innerErr := obtained.Add(common.CopyBytes(k), common.CopyBytes(v))
			if innerErr != nil {
				t.Fatal(innerErr)
			}
			return true, nil
		}); err != nil {
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
	_, tx := memdb.NewTestTx(t)

	numOfAccounts := uint8(4)
	addrs := make([]common.Address, numOfAccounts)
	addrHashes := make([]common.Hash, numOfAccounts)
	for i := uint8(0); i < numOfAccounts; i++ {
		addrs[i] = common.Address{i + 1}
		addrHash, _ := common.HashData(addrs[i].Bytes())
		addrHashes[i] = addrHash
	}
	key := common.Hash{123}
	emptyVal := uint256.NewInt(0)

	val := uint256.NewInt(0).SetBytes([]byte("block 1"))
	writeStorageBlockData(t, NewPlainStateWriter(tx, tx, 1), []storageData{
		{
			addr:   addrs[0],
			inc:    1,
			key:    key,
			oldVal: emptyVal,
			newVal: val,
		},
		{
			addr:   addrs[1],
			inc:    1,
			key:    key,
			oldVal: emptyVal,
			newVal: val,
		},
		{
			addr:   addrs[2],
			inc:    1,
			key:    key,
			oldVal: emptyVal,
			newVal: val,
		},
	})

	prev := val
	for i := 2; i < 1100; i++ {
		val = uint256.NewInt(0).SetBytes([]byte("block " + strconv.Itoa(i)))
		writeStorageBlockData(t, NewPlainStateWriter(tx, tx, uint64(i)), []storageData{
			{
				addr:   addrs[0],
				inc:    1,
				key:    key,
				oldVal: prev,
				newVal: val,
			},
			{
				addr:   addrs[1],
				inc:    1,
				key:    key,
				oldVal: prev,
				newVal: val,
			},
			{
				addr:   addrs[2],
				inc:    1,
				key:    key,
				oldVal: prev,
				newVal: val,
			},
		})
		prev = val
	}

	val = uint256.NewInt(0).SetBytes([]byte("block 1100"))

	writeStorageBlockData(t, NewPlainStateWriter(tx, tx, 1100), []storageData{
		{
			addr:   addrs[0],
			inc:    1,
			key:    key,
			oldVal: prev,
			newVal: val,
		},
		{
			addr:   addrs[1],
			inc:    1,
			key:    key,
			oldVal: prev,
			newVal: val,
		},
		{
			addr:   addrs[2],
			inc:    1,
			key:    key,
			oldVal: prev,
			newVal: val,
		},
	})

	for _, blockNum := range []uint64{5, 100, 1000, 1050} {
		obtained := &changeset.ChangeSet{
			Changes: make([]changeset.Change, 0),
		}

		for _, addr := range addrs {
			if err := WalkAsOfStorage(tx, addr, changeset.DefaultIncarnation, common.Hash{}, blockNum, func(kAddr, kLoc []byte, v []byte) (b bool, e error) {
				if innerErr := obtained.Add(append(common.CopyBytes(kAddr), kLoc...), common.CopyBytes(v)); innerErr != nil {
					t.Fatal(innerErr)
				}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		valBytes := uint256.NewInt(0).SetBytes([]byte("block " + strconv.FormatUint(blockNum-1, 10))).Bytes()
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

func writeBlockData(t *testing.T, blockWriter *PlainStateWriter, data []accData) {
	for i := range data {
		if data[i].newVal != nil {
			if err := blockWriter.UpdateAccountData(data[i].addr, data[i].oldVal, data[i].newVal); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := blockWriter.DeleteAccount(data[i].addr, data[i].oldVal); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
	}
}

type storageData struct {
	addr   common.Address
	inc    uint64
	key    common.Hash
	oldVal *uint256.Int
	newVal *uint256.Int
}

func writeStorageBlockData(t *testing.T, blockWriter *PlainStateWriter, data []storageData) {

	for i := range data {
		if err := blockWriter.WriteAccountStorage(data[i].addr, data[i].inc, &data[i].key, data[i].oldVal, data[i].newVal); err != nil {
			t.Fatal(err)
		}
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		t.Fatal(err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		t.Fatal(err)
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
