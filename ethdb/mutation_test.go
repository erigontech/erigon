package ethdb

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
)

func TestMutation_DeleteTimestamp(t *testing.T) {
	db:=NewMemDatabase()
	mutDB:=db.NewBatch()

	acc:=make([]*accounts.Account, 10)
	addrHashes:=make([]common.Hash, 10)
	for i:=range acc {
		acc[i], _, addrHashes[i] = randomAccount(t)
		b:=make([]byte, acc[i].EncodingLengthForStorage())
		acc[i].EncodeForStorage(b)
		err:=db.PutS(dbutils.AccountsHistoryBucket, addrHashes[i].Bytes(), b, 1,false)
		if err!=nil {
			t.Fatal(err)
		}
	}
	_,err:=mutDB.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	csData, err:=db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.AccountsHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}

	cs,err:=dbutils.DecodeChangeSet(csData)
	if err!=nil {
		t.Fatal(err)
	}

	if cs.KeyCount()!=10 {
		t.FailNow()
	}

	if debug.IsThinHistory() {
		csData, err = db.Get(dbutils.AccountsHistoryBucket, addrHashes[0].Bytes())
		if err!=nil {
			t.Fatal(err)
		}
		index:=new(HistoryIndex)
		err=index.Decode(csData)
		if err!=nil {
			t.Fatal(err)
		}
		if (*index)[0]!=1 {
			t.Fatal("incorrect block num")
		}

	} else {
		compositeKey,_:=dbutils.CompositeKeySuffix(addrHashes[0].Bytes(), 1)
		csData, err = db.Get(dbutils.AccountsHistoryBucket, compositeKey)
		if err!=nil {
			t.Fatal(err)
		}
	}

	err=mutDB.DeleteTimestamp(1)
	if err!=nil {
		t.Fatal(err)
	}
	_, err=mutDB.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	csData, err = db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.AccountsHistoryBucket))
	if err!=ErrKeyNotFound {
		t.Fatal("changeset must be deleted")
	}

	if debug.IsThinHistory() {
		csData, err = db.Get(dbutils.AccountsHistoryBucket, addrHashes[0].Bytes())
		if err!=ErrKeyNotFound {
			t.Fatal("account must be deleted")
		}
	} else {
		compositeKey,_:=dbutils.CompositeKeySuffix(addrHashes[0].Bytes(), 1)
		csData, err = db.Get(dbutils.AccountsHistoryBucket, compositeKey)
		if err!=ErrKeyNotFound {
			t.Fatal("account must be deleted")
		}
	}
}

func TestMutationCommit(t *testing.T) {
	if debug.IsThinHistory() {
		t.Skip()
	}
	db:=NewMemDatabase()
	mutDB:=db.NewBatch()

	numOfAccounts:=5
	numOfStateKeys:=5
	addrHashes, accState, accStateStorage, accHistory, accHistoryStateStorage:= generateAccountsWithStorageAndHistory(t, mutDB, numOfAccounts, numOfStateKeys)

	_,err:=mutDB.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	for i, addrHash :=range addrHashes {
		b,err:=db.Get(dbutils.AccountsBucket, addrHash.Bytes())
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}

		acc:=accounts.NewAccount()
		err = acc.DecodeForStorage(b)
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}
		if !accState[i].Equals(&acc) {
			spew.Dump("got",acc)
			spew.Dump("expected", accState[i])
			t.Fatal("Accounts not equals")
		}

		compositeKey,_:=dbutils.CompositeKeySuffix(addrHash.Bytes(), 1)
		b,err=db.Get(dbutils.AccountsHistoryBucket, compositeKey)
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}

		acc=accounts.NewAccount()
		err = acc.DecodeForStorage(b)
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}
		if !accHistory[i].Equals(&acc) {
			spew.Dump("got",acc)
			spew.Dump("expected", accState[i])
			t.Fatal("Accounts not equals")
		}


		resAccStorage:=make(map[common.Hash]common.Hash)
		err = db.Walk(dbutils.StorageBucket, dbutils.GenerateStoragePrefix(addrHash, acc.Incarnation), common.HashLength+8, func(k, v []byte) (b bool, e error) {
			resAccStorage[common.BytesToHash(k[common.HashLength+8:])]=common.BytesToHash(v)
			return true, nil
		})
		if err!=nil {
			t.Fatal("error on get account storage", i,err)
		}

		if !reflect.DeepEqual(resAccStorage, accStateStorage[i]) {
			spew.Dump("res", resAccStorage)
			spew.Dump("expected", accHistoryStateStorage[i])
			t.Log("incorrect storage", i)
		}

		resAccStorage=make(map[common.Hash]common.Hash)
		err = db.Walk(dbutils.StorageHistoryBucket, dbutils.GenerateStoragePrefix(addrHash, acc.Incarnation), common.HashLength+8, func(k, v []byte) (b bool, e error) {
			resAccStorage[common.BytesToHash(k[common.HashLength+8:common.HashLength+8+common.HashLength])]=common.BytesToHash(v)
			return true, nil
		})
		if err!=nil {
			t.Fatal("error on get account storage", i,err)
		}

		if !reflect.DeepEqual(resAccStorage, accHistoryStateStorage[i]) {
			spew.Dump("res", resAccStorage)
			spew.Dump("expected", accHistoryStateStorage[i])
			t.Fatal("incorrect history storage",i)
		}
	}

	csData, err:=db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.AccountsHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}

	cs,err:=dbutils.DecodeChangeSet(csData)
	if err!=nil {
		t.Fatal(err)
	}

	expectedChangeSet:=dbutils.NewChangeSet()
	for i:=range addrHashes {
		b:=make([]byte, accHistory[i].EncodingLengthForStorage())
		accHistory[i].EncodeForStorage(b)
		expectedChangeSet.Add(addrHashes[i].Bytes(), b)
	}

	if !reflect.DeepEqual(cs, expectedChangeSet) {
		spew.Dump("res", cs)
		spew.Dump("expected", expectedChangeSet)
		t.Fatal("incorrect account changeset")
	}
	
	csData, err=db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.StorageHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}

	cs,err=dbutils.DecodeChangeSet(csData)
	if err!=nil {
		t.Fatal(err)
	}

	if cs.KeyCount()!=uint32(numOfAccounts*numOfStateKeys) {
		t.FailNow()
	}

	expectedChangeSet=dbutils.NewChangeSet()
	for i,addrHash:=range addrHashes {
		for j:=0;j<numOfStateKeys;j++ {
			key:=common.Hash{uint8(i*100 +j)}
			value:=common.Hash{uint8(10+j)}
			expectedChangeSet.Add(dbutils.GenerateCompositeStorageKey(addrHash,accHistory[i].Incarnation,key),value.Bytes())
		}
	}

	if !reflect.DeepEqual(cs, expectedChangeSet) {
		spew.Dump("res", cs)
		spew.Dump("expected", expectedChangeSet)
		t.Fatal("incorrect storage changeset")
	}
}


func TestMutationCommitThinHistory(t *testing.T) {
	if !debug.IsThinHistory() {
		t.Skip()
	}

	db:=NewMemDatabase()
	mutDB:=db.NewBatch()

	numOfAccounts:=5
	numOfStateKeys:=5

	addrHashes, accState, accStateStorage, accHistory, accHistoryStateStorage:= generateAccountsWithStorageAndHistory(t, mutDB, numOfAccounts, numOfStateKeys)

	_,err:=mutDB.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	for i, addrHash :=range addrHashes {
		b,err:=db.Get(dbutils.AccountsBucket, addrHash.Bytes())
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}

		acc:=accounts.NewAccount()
		err = acc.DecodeForStorage(b)
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}
		if !accState[i].Equals(&acc) {
			spew.Dump("got",acc)
			spew.Dump("expected", accState[i])
			t.Fatal("Accounts not equals")
		}

		b,err=db.Get(dbutils.AccountsHistoryBucket, addrHash.Bytes())
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}
		index:=new(HistoryIndex)
		err = index.Decode(b)
		if err!=nil {
			t.Fatal("error on get account", i,err)
		}

		if (*index)[0]!=1 && len(*index)!=1 {
			t.Fatal("incorrect history index")
		}

		resAccStorage:=make(map[common.Hash]common.Hash)
		err = db.Walk(dbutils.StorageBucket, dbutils.GenerateStoragePrefix(addrHash, acc.Incarnation), common.HashLength+8, func(k, v []byte) (b bool, e error) {
			resAccStorage[common.BytesToHash(k[common.HashLength+8:])]=common.BytesToHash(v)
			return true, nil
		})
		if err!=nil {
			t.Fatal("error on get account storage", i,err)
		}

		if !reflect.DeepEqual(resAccStorage, accStateStorage[i]) {
			spew.Dump("res", resAccStorage)
			spew.Dump("expected", accHistoryStateStorage[i])
			t.Log("incorrect storage", i)
		}

		resAccStorage=make(map[common.Hash]common.Hash)
		err = db.Walk(dbutils.StorageHistoryBucket, dbutils.GenerateStoragePrefix(addrHash, acc.Incarnation), common.HashLength+8, func(k, v []byte) (b bool, e error) {
			resAccStorage[common.BytesToHash(k[common.HashLength+8:common.HashLength+8+common.HashLength])]=common.BytesToHash(v)
			return true, nil
		})
		if err!=nil {
			t.Fatal("error on get account storage", i,err)
		}

		if !reflect.DeepEqual(resAccStorage, accHistoryStateStorage[i]) {
			spew.Dump("res", resAccStorage)
			spew.Dump("expected", accHistoryStateStorage[i])
			t.Fatal("incorrect history storage",i)
		}
	}

	csData, err:=db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.AccountsHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}

	cs,err:=dbutils.DecodeChangeSet(csData)
	if err!=nil {
		t.Fatal(err)
	}

	expectedChangeSet:=dbutils.NewChangeSet()
	for i:=range addrHashes {
		b:=make([]byte, accHistory[i].EncodingLengthForStorage())
		accHistory[i].EncodeForStorage(b)
		expectedChangeSet.Add(addrHashes[i].Bytes(), b)
	}

	if !reflect.DeepEqual(cs, expectedChangeSet) {
		spew.Dump("res", cs)
		spew.Dump("expected", expectedChangeSet)
		t.Fatal("incorrect changeset")
	}



	csData, err=db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.StorageHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}

	cs,err=dbutils.DecodeChangeSet(csData)
	if err!=nil {
		t.Fatal(err)
	}

	if cs.KeyCount()!=uint32(numOfAccounts*numOfStateKeys) {
		t.FailNow()
	}

	expectedChangeSet=dbutils.NewChangeSet()
	for i,addrHash:=range addrHashes {
		for j:=0;j<numOfStateKeys;j++ {
			key:=common.Hash{uint8(i*100 +j)}
			value:=common.Hash{uint8(10+j)}
			expectedChangeSet.Add(dbutils.GenerateCompositeStorageKey(addrHash,accHistory[i].Incarnation,key),value.Bytes())
		}
	}

	if !reflect.DeepEqual(cs, expectedChangeSet) {
		spew.Dump("res", cs)
		spew.Dump("expected", expectedChangeSet)
		t.Fatal("incorrect changeset")
	}
}


func generateAccountsWithStorageAndHistory(t *testing.T, db Database, numOfAccounts, numOfStateKeys int) ([]common.Hash, []*accounts.Account, []map[common.Hash]common.Hash, []*accounts.Account, []map[common.Hash]common.Hash ) {
	t.Helper()

	accHistory:=make([]*accounts.Account, numOfAccounts)
	accState:=make([]*accounts.Account, numOfAccounts)
	accStateStorage:=make([]map[common.Hash]common.Hash, numOfAccounts)
	accHistoryStateStorage:=make([]map[common.Hash]common.Hash, numOfAccounts)
	addrHashes:=make([]common.Hash, numOfAccounts)
	for i:=range accHistory {
		var b []byte
		accHistory[i], _, addrHashes[i] = randomAccount(t)
		accHistory[i].Balance=*big.NewInt(100)
		accHistory[i].CodeHash= common.Hash{uint8(10+i)}
		accHistory[i].Root= common.Hash{uint8(10+i)}
		accHistory[i].Incarnation=uint64(i)

		b = make([]byte, accHistory[i].EncodingLengthForStorage())
		accHistory[i].EncodeForStorage(b)

		err:=db.PutS(dbutils.AccountsHistoryBucket, addrHashes[i].Bytes(), b, 1,false)
		if err!=nil {
			t.Fatal(err)
		}

		accState[i] = accHistory[i].SelfCopy()
		accState[i].Nonce++
		accState[i].Balance=*big.NewInt(200)
		b = make([]byte, accState[i].EncodingLengthForStorage())
		accState[i].EncodeForStorage(b)

		err=db.Put(dbutils.AccountsBucket, addrHashes[i].Bytes(), b)
		if err!=nil {
			t.Fatal(err)
		}

		accStateStorage[i]=make(map[common.Hash]common.Hash)
		accHistoryStateStorage[i]=make(map[common.Hash]common.Hash)
		for j:=0;j<numOfStateKeys; j++ {
			key:=common.Hash{uint8(i*100 +j)}
			value:=common.Hash{uint8(j)}
			accStateStorage[i][key]=value
			err=db.Put(dbutils.StorageBucket, dbutils.GenerateCompositeStorageKey(addrHashes[i], accHistory[i].Incarnation,key), value.Bytes())
			if err!=nil {
				t.Fatal(err)
			}

			newValue:=common.Hash{uint8(10+j)}
			accHistoryStateStorage[i][key]=newValue
			err=db.PutS(
				dbutils.StorageHistoryBucket,
				dbutils.GenerateCompositeStorageKey(addrHashes[i], accHistory[i].Incarnation,key),
				newValue.Bytes(),
				1,
				false,
			)
			if err!=nil {
				t.Fatal(err)
			}
		}
	}
	return addrHashes, accState, accStateStorage, accHistory, accHistoryStateStorage
}


func TestMutation_GetAsOf(t *testing.T) {
	db:=NewMemDatabase()
	mutDB:=db.NewBatch()

	acc, _, addrHash := randomAccount(t)
	acc2 := acc.SelfCopy()
	acc2.Nonce=1
	acc4 := acc.SelfCopy()
	acc4.Nonce=3

	b:=make([]byte, acc.EncodingLengthForStorage())
	acc.EncodeForStorage(b)
	err:=db.Put(dbutils.AccountsBucket, addrHash.Bytes(), b)
	if err!=nil {
		t.Fatal(err)
	}

	b=make([]byte, acc2.EncodingLengthForStorage())
	acc2.EncodeForStorage(b)
	err=db.PutS(dbutils.AccountsHistoryBucket, addrHash.Bytes(), b, 2,false)
	if err!=nil {
		t.Fatal(err)
	}

	b=make([]byte, acc4.EncodingLengthForStorage())
	acc4.EncodeForStorage(b)
	err=db.PutS(dbutils.AccountsHistoryBucket, addrHash.Bytes(), b, 4,false)
	if err!=nil {
		t.Fatal(err)
	}

	_,err=mutDB.Commit()
	if err!=nil {
		t.Fatal(err)
	}

	b, err=db.Get(dbutils.AccountsBucket, addrHash.Bytes())
	if err!=nil {
		t.Fatal(err)
	}
	resAcc:=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !acc.Equals(resAcc) {
		t.Fatal("Account from Get is incorrect")
	}

	b, err=db.GetAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash.Bytes(), 1)
	if err!=nil {
		t.Fatal("incorrect value on block 1", err)
	}
	resAcc=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	//todo Почему???
	if !acc.Equals(resAcc) {
		spew.Dump(resAcc)
		t.Fatal("Account from GetAsOf(1) is incorrect")
	}

	b, err=db.GetS(dbutils.AccountsHistoryBucket, addrHash.Bytes(), 2)
	if err!=nil {
		t.Fatal(err)
	}
	resAcc=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !acc2.Equals(resAcc) {
		t.Fatal("Account from GetS(1) is incorrect")
	}

	_, err=db.GetS(dbutils.AccountsHistoryBucket, addrHash.Bytes(), 3)
	if err!=ErrKeyNotFound {
		t.Fatal(err)
	}

	b, err=db.GetAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash.Bytes(), 2)
	if err!=nil {
		t.Fatal(err)
	}
	resAcc=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !acc2.Equals(resAcc) {
		spew.Dump(resAcc)
		t.Fatal("Account from GetAsOf(2) is incorrect")
	}

	b, err=db.GetAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash.Bytes(), 3)
	if err!=nil {
		t.Fatal(err)
	}
	resAcc=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !acc4.Equals(resAcc) {
		spew.Dump(resAcc)
		t.Fatal("Account from GetAsOf(2) is incorrect")
	}

	b, err=db.GetAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash.Bytes(), 5)
	if err!=nil {
		t.Fatal(err)
	}
	resAcc=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !acc.Equals(resAcc) {
		t.Fatal("Account from GetAsOf(4) is incorrect")
	}


	b, err=db.GetAsOf(dbutils.AccountsBucket, dbutils.AccountsHistoryBucket, addrHash.Bytes(), 7)
	if err!=nil {
		t.Fatal(err)
	}
	resAcc=new(accounts.Account)
	err=resAcc.DecodeForStorage(b)
	if err!=nil {
		t.Fatal(err)
	}
	if !acc.Equals(resAcc) {
		t.Fatal("Account from GetAsOf(7) is incorrect")
	}
}


func randomAccount(t *testing.T) (*accounts.Account,common.Address, common.Hash) {
	t.Helper()
	key,err:=crypto.GenerateKey()
	if err!=nil {
		t.Fatal(err)
	}
	acc:=accounts.NewAccount()
	acc.Initialised=true
	acc.Balance = *big.NewInt(rand.Int63())
	addr:=crypto.PubkeyToAddress(key.PublicKey)
	addrHash,err:=common.HashData(addr.Bytes())
	if err!=nil {
		t.Fatal(err)
	}
	return &acc,addr, addrHash
}