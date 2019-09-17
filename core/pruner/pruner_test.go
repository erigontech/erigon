package pruner

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"math/big"
	"testing"
)

func TestOne(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		db      = ethdb.NewMemDatabase()
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
		funds = big.NewInt(1000000000)
		gspec = &core.Genesis{
			Config: &params.ChainConfig{
				ChainID:             big.NewInt(1),
				HomesteadBlock:      new(big.Int),
				EIP155Block:         new(big.Int),
				EIP158Block:         big.NewInt(1),
				EIP2027Block:        big.NewInt(4),
				ConstantinopleBlock: big.NewInt(1),
			},
			Alloc: core.GenesisAlloc{
				address: {Balance: funds},
				address1: {Balance: funds},
				address2: {Balance: funds},
			},
		}
		genesis   = gspec.MustCommit(db)
		genesisDb = db.MemCopy()
		// this code generates a log
		signer = types.HomesteadSigner{}
	)

	numBlocks :=10
	engine := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, gspec.Config, engine, vm.Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	blocks, _ := core.GenerateChain(gspec.Config, genesis, engine, genesisDb, numBlocks, func(i int, block *core.BlockGen) {
		var (
			tx     *types.Transaction
			genErr error
		)
		var addr common.Address
		var k *ecdsa.PrivateKey
		switch i%3 {
		case 0:
			addr = address
			k = key
		case 1:
			addr = address1
			k = key1
		case 2:
			addr = address2
			k = key2
		}
		tx, genErr = types.SignTx(types.NewTransaction(block.TxNonce(addr), theAddr, big.NewInt(1000), 21000, new(big.Int), nil), signer, k)
		if genErr != nil {
			t.Fatal(genErr)
		}
		block.AddTx(tx)
	})

	for i := range blocks {
		_, err = blockchain.InsertChain(types.Blocks{blocks[i]})
		if err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("=============================================================================================")
	//accHisCountInDB:=0
	//accCountInDB:=0
	//err = db.DB().View(func(tx *bolt.Tx) error {
	//	return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
	//
	//		var nameCopy = make([]byte, len(name))
	//		copy(nameCopy, name)
	//
	//
	//		return b.ForEach(func(k, _ []byte) error {
	//			var keyCopy = make([]byte, len(k)+len(name))
	//			copy(keyCopy, nameCopy)
	//			copy(keyCopy[len(name):], k)
	//			fmt.Println(" - ", string(keyCopy))
	//			fmt.Println(" -- ", keyCopy)
	//
	//			if bytes.HasPrefix(name, dbutils.AccountsHistoryBucket) {
	//				accHisCountInDB++
	//			}
	//
	//			if bytes.HasPrefix(name, dbutils.AccountsBucket) {
	//				accCountInDB++
	//			}
	//
	//			return nil
	//		})
	//	})
	//})
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//accHisCountOverSuffix:=0
	//
	//if err!=nil {
	//	t.Fatal(err)
	//}
	//t.Log("accCountInDB", accCountInDB)
	//t.Log("accHisCountInDB", accHisCountInDB)
	//t.Log("accHisCountOverSuffix", accHisCountOverSuffix)

	fmt.Println("======================STAT BEFORE PRUNE ==================================")
	spew.Dump(getStat(db))
	fmt.Println("========================================================")


	fmt.Println("======================PRUNE START==================================")

	err = Prune(db, 0, uint64(numBlocks))
	if err!=nil {
		t.Log("Prune", err)
	}
	fmt.Println("======================PRUNE END==================================")


	//accHisCountInDB=0
	//accCountInDB=0
	//err = db.DB().View(func(tx *bolt.Tx) error {
	//	return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
	//
	//		var nameCopy = make([]byte, len(name))
	//		copy(nameCopy, name)
	//
	//
	//		return b.ForEach(func(k, _ []byte) error {
	//			var keyCopy = make([]byte, len(k)+len(name))
	//			copy(keyCopy, nameCopy)
	//			copy(keyCopy[len(name):], k)
	//			fmt.Println(" - ", string(keyCopy))
	//			fmt.Println(" -- ", keyCopy)
	//
	//			if bytes.HasPrefix(name, dbutils.AccountsHistoryBucket) {
	//				fmt.Println("name", name, keyCopy)
	//				accHisCountInDB++
	//			}
	//
	//			if bytes.HasPrefix(name, dbutils.AccountsBucket) {
	//				accCountInDB++
	//			}
	//
	//
	//			return nil
	//		})
	//	})
	//})
	//if err != nil {
	//	t.Fatal(err)
	//}

	//
	//accHisCountOverSuffix=0
	//err=db.Walk(dbutils.SuffixBucket,[]byte{}, 0, func(key, v []byte) (b bool, e error) {
	//	fmt.Println("---------------------------------------------")
	//	ts, _:=dbutils.DecodeTimestamp(key)
	//	fmt.Println("k", key, "DecodeTimestamp",  ts)
	//
	//	fmt.Println("AccountsHistoryBucket", bytes.HasSuffix(key, dbutils.AccountsHistoryBucket))
	//	fmt.Println("StorageHistoryBucket", bytes.HasSuffix(key, dbutils.StorageHistoryBucket))
	//
	//	fmt.Println("v", v)
	//	d:=dbutils.Suffix(v)
	//	fmt.Println("count", d.KeyCount())
	//	d.Walk(func(k []byte) error {
	//		fmt.Println("-- ", common.Bytes2Hex(k))
	//		compKey,_:=dbutils.CompositeKeySuffix(k, ts)
	//		//fmt.Println("compKey",compKey)
	//		b, err:=db.Get(dbutils.AccountsHistoryBucket, compKey)
	//		if len(b)==0 {
	//			//fmt.Println("empty account", ts, k)
	//			return nil
	//		}
	//		if err!=nil {
	//			fmt.Println(err)
	//		} else {
	//			acc:=&accounts.Account{}
	//			//fmt.Println(b)
	//			errInn:=acc.DecodeForStorage(b)
	//			if errInn!=nil {
	//				fmt.Println(errInn)
	//			} else {
	//				accHisCountOverSuffix++
	//				fmt.Println("Account found", ts)
	//				//spew.Dump(acc)
	//			}
	//		}
	//
	//		return nil
	//	})
	//	return true, nil
	//})
	//if err!=nil {
	//	t.Fatal(err)
	//}


	fmt.Println("======================STAT BEFORE PRUNE ==================================")
	spew.Dump(getStat(db))
	fmt.Println("========================================================")

}

func generateChain() {

}

type stateStats struct {
	NotFoundAccountsInHistory uint64
	ErrAccountsInHistory uint64
	ErrDecodedAccountsInHistory uint64
	NumOfChangesInAccountsHistory uint64
	AccountSuffixRecordsByTimestamp map[uint64]uint32
	StorageSuffixRecordsByTimestamp map[uint64]uint32
}

func getStat(db *ethdb.BoltDatabase) (stateStats, error)  {
	stat:=stateStats{
		AccountSuffixRecordsByTimestamp:make(map[uint64]uint32,0),
		StorageSuffixRecordsByTimestamp:make(map[uint64]uint32,0),

	}
	err:=db.Walk(dbutils.SuffixBucket,[]byte{}, 0, func(key, v []byte) (b bool, e error) {
		fmt.Println("---------------------------------------------")
		timestamp, _:=dbutils.DecodeTimestamp(key)

		changedAccounts :=dbutils.Suffix(v)
		if bytes.HasSuffix(key, dbutils.AccountsHistoryBucket) {
			if _,ok:=stat.AccountSuffixRecordsByTimestamp[timestamp]; ok {
				panic("core/pruner/pruner_test.go:256")
			}
			stat.AccountSuffixRecordsByTimestamp[timestamp] = changedAccounts.KeyCount()
		}
		if bytes.HasSuffix(key, dbutils.StorageHistoryBucket) {
			if _,ok:=stat.StorageSuffixRecordsByTimestamp[timestamp]; ok {
				panic("core/pruner/pruner_test.go:261")
			}
			stat.StorageSuffixRecordsByTimestamp[timestamp] = changedAccounts.KeyCount()
		}

		err:= changedAccounts.Walk(func(k []byte) error {
			compKey,_:=dbutils.CompositeKeySuffix(k, timestamp)
			b, err:=db.Get(dbutils.AccountsHistoryBucket, compKey)
			if len(b)==0 {
				stat.NotFoundAccountsInHistory++
				return nil
			}
			if err!=nil {
				stat.ErrAccountsInHistory++
			} else {
				acc:=&accounts.Account{}
				errInn:=acc.DecodeForStorage(b)
				if errInn!=nil {
					stat.ErrDecodedAccountsInHistory++
				} else {
					stat.NumOfChangesInAccountsHistory++
				}
			}
			return nil
		})
		if err!=nil {
			return false, err
		}
		return true, nil
	})

	if err != nil {
		return stateStats{}, err
	}
	return stat, nil
}