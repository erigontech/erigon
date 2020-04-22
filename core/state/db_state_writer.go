package state

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
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
	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	if err := rawdb.WriteAccount(dsw.tds.db, addrHash, *account); err != nil {
		return err
	}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if err := dsw.csw.DeleteAccount(ctx, address, original); err != nil {
		return err
	}
	addrHash, err := dsw.tds.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	if err := rawdb.DeleteAccount(dsw.tds.db, addrHash); err != nil {
		return err
	}
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(addrHash common.Hash, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := dsw.csw.UpdateAccountCode(addrHash, incarnation, codeHash, code); err != nil {
		return err
	}
	//save contract code mapping
	if err := dsw.tds.db.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}
	//save contract to codeHash mapping
	if addrHash == common.HexToHash("008a847b18f2031712b3bf761a1147dd938d9d8f4eddf7d79180edd782ee7471") {
		fmt.Printf("dsw.tds.db.Put(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(%x, %d), %x[:])",
		addrHash, incarnation, codeHash,
		)
	}
	return dsw.tds.db.Put(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash[:], incarnation), codeHash[:])
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
		return dsw.tds.db.Delete(dbutils.CurrentStateBucket, compositeKey)
	} else {
		return dsw.tds.db.Put(dbutils.CurrentStateBucket, compositeKey, vv)
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
	accountSerialised, err = changeset.EncodeAccounts(accountChanges)
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
		storageSerialized, err = changeset.EncodeStorage(storageChanges)
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
	err = dsw.writeIndex(accountChanges, dbutils.AccountsHistoryBucket)
	if err != nil {
		return err
	}

	storageChanges, err := dsw.csw.GetStorageChanges()
	if err != nil {
		return err
	}
	err = dsw.writeIndex(storageChanges, dbutils.StorageHistoryBucket)
	if err != nil {
		return err
	}

	return nil
}

func (dsw *DbStateWriter) writeIndex(changes *changeset.ChangeSet, bucket []byte) error {
	for _, change := range changes.Changes {
		currentChunkKey := dbutils.IndexChunkKey(change.Key, ^uint64(0))
		indexBytes, err := dsw.tds.db.Get(bucket, currentChunkKey)
		if err != nil && err != ethdb.ErrKeyNotFound {
			return fmt.Errorf("find chunk failed: %w", err)
		}

		var index dbutils.HistoryIndexBytes
		if len(indexBytes) == 0 {
			index = dbutils.NewHistoryIndex()
		} else if dbutils.CheckNewIndexChunk(indexBytes) {
			// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
			index = dbutils.WrapHistoryIndex(indexBytes)
			indexKey, err := index.Key(change.Key)
			if err != nil {
				return err
			}
			// Flush the old chunk
			if err := dsw.tds.db.Put(bucket, indexKey, index); err != nil {
				return err
			}
			// Start a new chunk
			index = dbutils.NewHistoryIndex()
		} else {
			index = dbutils.WrapHistoryIndex(indexBytes)
		}

		index = index.Append(dsw.tds.blockNr)

		if err := dsw.tds.db.Put(bucket, currentChunkKey, index); err != nil {
			return err
		}
	}

	return nil
}
