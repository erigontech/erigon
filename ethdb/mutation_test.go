package ethdb

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"math/big"
	"math/rand"
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

	if debug.IsDataLayoutExperiment() {
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

	fmt.Println("DeleteTimestamp")
	err=mutDB.DeleteTimestamp(1)
	fmt.Println("DeleteTimestamp+")
	if err!=nil {
		t.Fatal(err)
	}
	_, err=mutDB.Commit()
	fmt.Println("2")
	if err!=nil {
		t.Fatal(err)
	}

	csData, err = db.Get(dbutils.ChangeSetBucket,dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(1), dbutils.AccountsHistoryBucket))
	if err!=ErrKeyNotFound {
		t.Fatal("changeset must be deleted")
	}

	if debug.IsDataLayoutExperiment() {
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

func randomAccount(t *testing.T) (*accounts.Account,common.Address, common.Hash) {
	t.Helper()
	key,err:=crypto.GenerateKey()
	if err!=nil {
		t.Fatal(err)
	}
	acc:=accounts.NewAccount()
	acc.Balance = *big.NewInt(rand.Int63())
	addr:=crypto.PubkeyToAddress(key.PublicKey)
	addrHash,err:=common.HashData(addr.Bytes())
	if err!=nil {
		t.Fatal(err)
	}
	return &acc,addr, addrHash
}