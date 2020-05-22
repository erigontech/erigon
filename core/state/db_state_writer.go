package state

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/VictoriaMetrics/fastcache"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

var _ WriterWithChangeSets = (*DbStateWriter)(nil)

func NewDbStateWriter(stateDb, changeDb ethdb.Database, blockNr uint64, incarnationMap map[common.Address]uint64) *DbStateWriter {
	return &DbStateWriter{
		stateDb:        stateDb,
		changeDb:       changeDb,
		blockNr:        blockNr,
		pw:             &PreimageWriter{db: stateDb, savePreimages: false},
		csw:            NewChangeSetWriter(),
		incarnationMap: incarnationMap,
	}
}

type DbStateWriter struct {
	stateDb        ethdb.Database
	changeDb       ethdb.Database
	pw             *PreimageWriter
	blockNr        uint64
	csw            *ChangeSetWriter
	incarnationMap map[common.Address]uint64
	accountCache   *fastcache.Cache
	storageCache   *fastcache.Cache
	codeCache      *fastcache.Cache
	codeSizeCache  *fastcache.Cache
}

func (dsw *DbStateWriter) SetAccountCache(accountCache *fastcache.Cache) {
	dsw.accountCache = accountCache
}

func (dsw *DbStateWriter) SetStorageCache(storageCache *fastcache.Cache) {
	dsw.storageCache = storageCache
}

func (dsw *DbStateWriter) SetCodeCache(codeCache *fastcache.Cache) {
	dsw.codeCache = codeCache
}

func (dsw *DbStateWriter) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	dsw.codeSizeCache = codeSizeCache
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
	addrHash, err := dsw.pw.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if err := dsw.stateDb.Put(dbutils.CurrentStateBucket, addrHash[:], value); err != nil {
		return err
	}
	if dsw.accountCache != nil {
		dsw.accountCache.Set(address[:], value)
	}
	return nil
}

func (dsw *DbStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if err := dsw.csw.DeleteAccount(ctx, address, original); err != nil {
		return err
	}
	addrHash, err := dsw.pw.HashAddress(address, true /*save*/)
	if err != nil {
		return err
	}
	if err := rawdb.DeleteAccount(dsw.stateDb, addrHash); err != nil {
		return err
	}
	if original.Incarnation > 0 {
		dsw.incarnationMap[address] = original.Incarnation
	}
	if dsw.accountCache != nil {
		dsw.accountCache.Set(address[:], nil)
	}
	if dsw.codeCache != nil {
		dsw.codeCache.Set(address[:], nil)
	}
	if dsw.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], 0)
		dsw.codeSizeCache.Set(address[:], b[:])
	}
	return nil
}

func (dsw *DbStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := dsw.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
		return err
	}
	//save contract code mapping
	if err := dsw.stateDb.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}
	addrHash, err := common.HashData(address.Bytes())
	if err != nil {
		return err
	}
	//save contract to codeHash mapping
	if err := dsw.stateDb.Put(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash[:], incarnation), codeHash[:]); err != nil {
		return err
	}
	if dsw.codeCache != nil && len(code) <= 1024 {
		dsw.codeCache.Set(address[:], code)
	}
	if dsw.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		dsw.codeSizeCache.Set(address[:], b[:])
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
	seckey, err := dsw.pw.HashKey(key, true /*save*/)
	if err != nil {
		return err
	}
	addrHash, err := dsw.pw.HashAddress(address, false /*save*/)
	if err != nil {
		return err
	}
	compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, incarnation, seckey)

	v := cleanUpTrailingZeroes(value[:])
	if dsw.storageCache != nil {
		dsw.storageCache.Set(compositeKey, v)
	}
	if len(v) == 0 {
		return dsw.stateDb.Delete(dbutils.CurrentStateBucket, compositeKey)
	}
	return dsw.stateDb.Put(dbutils.CurrentStateBucket, compositeKey, common.CopyBytes(v))
}

func (dsw *DbStateWriter) CreateContract(address common.Address) error {
	return dsw.csw.CreateContract(address)
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
	key := dbutils.EncodeTimestamp(dsw.blockNr)
	if err = dsw.changeDb.Put(dbutils.AccountChangeSetBucket, key, accountSerialised); err != nil {
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
		if err = dsw.changeDb.Put(dbutils.StorageChangeSetBucket, key, storageSerialized); err != nil {
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
		indexBytes, err := dsw.changeDb.Get(bucket, currentChunkKey)
		if err != nil && err != ethdb.ErrKeyNotFound {
			return fmt.Errorf("find chunk failed: %w", err)
		}
		v := dsw.blockNr

		var index dbutils.HistoryIndexBytes
		if len(indexBytes) == 0 {
			index = dbutils.NewHistoryIndex()
		} else if dbutils.CheckNewIndexChunk(indexBytes, v) {
			// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
			index = dbutils.WrapHistoryIndex(indexBytes)
			indexKey, err := index.Key(change.Key)
			if err != nil {
				return err
			}
			// Flush the old chunk
			if err := dsw.changeDb.Put(bucket, indexKey, index); err != nil {
				return err
			}
			// Start a new chunk
			index = dbutils.NewHistoryIndex()
		} else {
			index = dbutils.WrapHistoryIndex(indexBytes)
		}
		index = index.Append(v, len(change.Value) == 0)

		if err := dsw.changeDb.Put(bucket, currentChunkKey, index); err != nil {
			return err
		}
	}

	return nil
}
