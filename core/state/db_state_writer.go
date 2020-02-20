package state

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/trie"
)

type DbStateWriter struct {
	tds *TrieDbState
}

func (dsw *DbStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	dataLen := account.EncodingLengthForStorage()
	data := make([]byte, dataLen)
	account.EncodeForStorage(data)

	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	if err = dsw.tds.db.Put(dbutils.AccountsBucket, addrHash[:], data); err != nil {
		return err
	}

	noHistory := dsw.tds.noHistory
	// Don't write historical record if the account did not change
	if accountsEqual(original, account) {
		return nil
	}
	var originalData []byte
	if !original.Initialised {
		originalData = []byte{}
	} else {
		// we can reduce storage size for history there
		// because we have accountHash+incarnation -> codehash of contract in separate bucket
		// and we don't need root in history requests
		testAcc := original.SelfCopy()
		if debug.IsThinHistory() {
			copy(testAcc.CodeHash[:], emptyCodeHash)
			testAcc.Root = trie.EmptyRoot
		}

		originalDataLen := testAcc.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		testAcc.EncodeForStorage(originalData)
	}
	return dsw.tds.db.PutS(dbutils.AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr, noHistory)
}

func (dsw *DbStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	if err := dsw.tds.db.Delete(dbutils.AccountsBucket, addrHash[:]); err != nil {
		return err
	}

	var originalData []byte
	if !original.Initialised {
		// Account has been created and deleted in the same block
		originalData = []byte{}
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
		// We must keep root using thin history on deleting account as is
	}

	noHistory := dsw.tds.noHistory
	return dsw.tds.db.PutS(dbutils.AccountsHistoryBucket, addrHash[:], originalData, dsw.tds.blockNr, noHistory)
}

func (dsw *DbStateWriter) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
	//save contract code mapping
	if err := dsw.tds.db.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}
	if debug.IsThinHistory() {
		//save contract to codeHash mapping
		return dsw.tds.db.Put(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash, incarnation), codeHash.Bytes())
	}
	return nil
}

// nonCreatedStorageHash it's fixed size array of zeroes. can use == operator to compare.
var nonExistingStorageHash common.Hash

func (dsw *DbStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	if *original == *value {
		return nil
	}
	seckey, err := dsw.tds.HashKey(key, true /*save*/)
	if err != nil {
		return err
	}
	v := bytes.TrimLeft(value[:], "\x00")
	vv := make([]byte, len(v))
	copy(vv, v)

	addrHash, err := dsw.tds.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}

	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey)
	if len(v) == 0 {
		err = dsw.tds.db.Delete(dbutils.StorageBucket, compositeKey)
	} else {
		if debug.IsIntermediateTrieHash() {
			now := time.Now()
			toPrint := false
			if original == nil || *original == nonExistingStorageHash {
				buf := pool.GetBuffer(128)
				defer pool.PutBuffer(buf)

				_ = dsw.tds.db.Walk(dbutils.IntermediateTrieHashBucket, compositeKey, (common.HashLength+1)*8, func(k, v []byte) (bool, error) {
					if !bytes.HasPrefix(compositeKey, k) {
						return true, nil
					}

					_ = dsw.tds.db.Delete(dbutils.IntermediateTrieHashBucket, k)

					buf.Reset()
					copy(buf.B, compositeKey)
					for j := 0; j < 256; j++ {
						if buf.B[len(k)] == uint8(j) {
							_ = dsw.tds.db.Delete(dbutils.IntermediateTrieHashBucket, buf.B[:len(k)])
							continue
						}
						buf.B[len(k)] = uint8(j)
						_ = dsw.tds.db.Put(dbutils.IntermediateTrieHashBucket, buf.B[:len(k)], []byte{})
					}

					toPrint = true
					return true, nil
				})

				//for i := common.HashLength + 1; i < len(compositeKey); i++ {
				//	val, _ := dsw.tds.db.Get(dbutils.IntermediateTrieHashBucket, buf.B[:i])
				//	if val == nil || len(val) > 0 {
				//		continue
				//	}
				//	fmt.Printf("ReCreated storage: %x\n", compositeKey)
				//	_ = dsw.tds.db.Delete(dbutils.IntermediateTrieHashBucket, buf.B[:i])
				//	for j := 0; j < 256; j++ {
				//		if buf.B[i+1] == uint8(j) {
				//			_ = dsw.tds.db.Delete(dbutils.IntermediateTrieHashBucket, buf.B[:i+1])
				//			continue
				//		}
				//		buf.B[i+1] = uint8(j)
				//		_ = dsw.tds.db.Put(dbutils.IntermediateTrieHashBucket, buf.B[:i+1], []byte{})
				//	}
				//}
			}
			if toPrint {
				fmt.Printf("Storage recreate time: %s\n", time.Since(now))
			}
		}

		err = dsw.tds.db.Put(dbutils.StorageBucket, compositeKey, vv)
	}
	//fmt.Printf("WriteAccountStorage (db) %x %d %x: %x\n", address, incarnation, key, value)
	if err != nil {
		return err
	}

	noHistory := dsw.tds.noHistory
	o := bytes.TrimLeft(original[:], "\x00")
	originalValue := make([]byte, len(o))
	copy(originalValue, o)
	return dsw.tds.db.PutS(dbutils.StorageHistoryBucket, compositeKey, originalValue, dsw.tds.blockNr, noHistory)
}

func (dsw *DbStateWriter) CreateContract(address common.Address) error {
	return nil
}
