package state

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

type DbStateWriter struct {
	tds *TrieDbState
	csw *ChangeSetWriter
}

func originalAccountData(original *accounts.Account, omitHashes bool) []byte {
	var originalData []byte
	if !original.Initialised {
		originalData = []byte{}
	} else if omitHashes {
		testAcc := original.SelfCopy()
		copy(testAcc.CodeHash[:], emptyCodeHash)
		testAcc.Root = trie.EmptyRoot
		originalDataLen := testAcc.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		testAcc.EncodeForStorage(originalData)
	} else {
		originalDataLen := original.EncodingLengthForStorage()
		originalData = make([]byte, originalDataLen)
		original.EncodeForStorage(originalData)
	}
	return originalData
}

func (dsw *DbStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if err := dsw.csw.UpdateAccountData(ctx, address, original, account); err != nil {
		return err
	}
	dataLen := account.EncodingLengthForStorage()
	data := make([]byte, dataLen)
	account.EncodeForStorage(data)

	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	return dsw.tds.db.Put(dbutils.AccountsBucket, addrHash[:], data)
}

func (dsw *DbStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if err := dsw.csw.DeleteAccount(ctx, address, original); err != nil {
		return err
	}
	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	return dsw.tds.db.Delete(dbutils.AccountsBucket, addrHash[:])
}

func (dsw *DbStateWriter) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := dsw.csw.UpdateAccountCode(addrHash, incarnation, codeHash, code); err != nil {
		return err
	}
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

func (dsw *DbStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	// We delegate here first to let the changeSetWrite make its own decision on whether to proceed in case *original == *value
	if err := dsw.csw.WriteAccountStorage(ctx, address, incarnation, key, original, value); err != nil {
		return err
	}
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
		return dsw.tds.db.Delete(dbutils.StorageBucket, compositeKey)
	} else {
		return dsw.tds.db.Put(dbutils.StorageBucket, compositeKey, vv)
	}
}

func (dsw *DbStateWriter) CreateContract(address common.Address) error {
	if err := dsw.csw.CreateContract(address); err != nil {
		return err
	}
	return nil
}

// WriteChangeSets causes accumulated change sets to be written into
// the database (or batch) associated with the `dsw`
func (dsw *DbStateWriter) WriteChangeSets() error {
	accountChanges, err := dsw.csw.GetAccountChanges()
	if err != nil {
		return err
	}
	var accountSerialised []byte
	if debug.IsThinHistory() {
		accountSerialised, err = changeset.EncodeAccounts(accountChanges)
	} else {
		accountSerialised, err = changeset.EncodeChangeSet(accountChanges)
	}
	if err != nil {
		return err
	}
	key := dbutils.EncodeTimestamp(dsw.tds.blockNr)
	if err = dsw.tds.db.Put(dbutils.AccountChangeSetBucket, key, accountSerialised); err != nil {
		return err
	}
	storageChanges, err := dsw.csw.GetStorageChanges()
	if err != nil {
		return err
	}
	var storageSerialized []byte
	if storageChanges.Len() > 0 {
		if debug.IsThinHistory() {
			storageSerialized, err = changeset.EncodeStorage(storageChanges)
		} else {
			storageSerialized, err = changeset.EncodeChangeSet(storageChanges)
		}
		if err != nil {
			return err
		}
		if err = dsw.tds.db.Put(dbutils.StorageChangeSetBucket, key, storageSerialized); err != nil {
			return err
		}
	}
	return nil
}

func (dsw *DbStateWriter) WriteHistory() error {
	accountChanges, err := dsw.csw.GetAccountChanges()
	if err != nil {
		return err
	}
	if debug.IsThinHistory() {
		for _, change := range accountChanges.Changes {
			value, err1 := dsw.tds.db.Get(dbutils.AccountsHistoryBucket, change.Key)
			if err1 != nil && err1 != ethdb.ErrKeyNotFound {
				return fmt.Errorf("db.Get failed: %w", err1)
			}
			index := dbutils.WrapHistoryIndex(value)
			index.Append(dsw.tds.blockNr)
			if err2 := dsw.tds.db.Put(dbutils.AccountsHistoryBucket, change.Key, *index); err2 != nil {
				return err2
			}
		}
	} else {
		for _, change := range accountChanges.Changes {
			composite, _ := dbutils.CompositeKeySuffix(change.Key, dsw.tds.blockNr)
			if err2 := dsw.tds.db.Put(dbutils.AccountsHistoryBucket, composite, change.Value); err2 != nil {
				return err2
			}
		}
	}
	storageChanges, err := dsw.csw.GetStorageChanges()
	if err != nil {
		return err
	}
	if debug.IsThinHistory() {
		for _, change := range storageChanges.Changes {
			keyNoInc := make([]byte, len(change.Key)-common.IncarnationLength)
			copy(keyNoInc, change.Key[:common.HashLength])
			copy(keyNoInc[common.HashLength:], change.Key[common.HashLength+common.IncarnationLength:])
			value, err1 := dsw.tds.db.Get(dbutils.StorageHistoryBucket, keyNoInc)
			if err1 != nil && err1 != ethdb.ErrKeyNotFound {
				return fmt.Errorf("db.Get failed: %w", err1)
			}
			index := dbutils.WrapHistoryIndex(value)
			index.Append(dsw.tds.blockNr)
			if err := dsw.tds.db.Put(dbutils.StorageHistoryBucket, keyNoInc, *index); err != nil {
				return err
			}
		}
	} else {
		for _, change := range storageChanges.Changes {
			composite, _ := dbutils.CompositeKeySuffix(change.Key, dsw.tds.blockNr)
			if err := dsw.tds.db.Put(dbutils.StorageHistoryBucket, composite, change.Value); err != nil {
				return err
			}
		}
	}
	return nil
}
