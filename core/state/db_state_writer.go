package state

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

// This type is now used in GenerateChain to generate blockchains for the tests (core/chain_makers.go)
// Main mode of operation uses PlainDbStateWriter

var _ WriterWithChangeSets = (*DbStateWriter)(nil)

func NewDbStateWriter(db ethdb.Database, blockNr uint64) *DbStateWriter {
	return &DbStateWriter{
		db:      db,
		blockNr: blockNr,
		pw:      &PreimageWriter{db: db, savePreimages: false},
		csw:     NewChangeSetWriter(),
	}
}

type DbStateWriter struct {
	db            ethdb.Database
	pw            *PreimageWriter
	blockNr       uint64
	csw           *ChangeSetWriter
	accountCache  *fastcache.Cache
	storageCache  *fastcache.Cache
	codeCache     *fastcache.Cache
	codeSizeCache *fastcache.Cache
}

func (dsw *DbStateWriter) ChangeSetWriter() *ChangeSetWriter {
	return dsw.csw
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
	if err := dsw.db.Put(dbutils.CurrentStateBucket, addrHash[:], value); err != nil {
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
	if err := rawdb.DeleteAccount(dsw.db, addrHash); err != nil {
		return err
	}
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		if err := dsw.db.Put(dbutils.IncarnationMapBucket, address[:], b[:]); err != nil {
			return err
		}
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
	if err := dsw.db.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}
	addrHash, err := common.HashData(address.Bytes())
	if err != nil {
		return err
	}
	//save contract to codeHash mapping
	if err := dsw.db.Put(dbutils.ContractCodeBucket, dbutils.GenerateStoragePrefix(addrHash[:], incarnation), codeHash[:]); err != nil {
		return err
	}
	if dsw.codeCache != nil {
		if len(code) <= 1024 {
			dsw.codeCache.Set(address[:], code)
		} else {
			dsw.codeCache.Del(address[:])
		}
	}
	if dsw.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		dsw.codeSizeCache.Set(address[:], b[:])
	}
	return nil
}

func (dsw *DbStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
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

	v := value.Bytes()
	if dsw.storageCache != nil {
		dsw.storageCache.Set(compositeKey, v)
	}
	if len(v) == 0 {
		return dsw.db.Delete(dbutils.CurrentStateBucket, compositeKey)
	}
	return dsw.db.Put(dbutils.CurrentStateBucket, compositeKey, v)
}

func (dsw *DbStateWriter) CreateContract(address common.Address) error {
	if err := dsw.csw.CreateContract(address); err != nil {
		return err
	}
	if err := dsw.db.Delete(dbutils.IncarnationMapBucket, address[:]); err != nil {
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
	key := dbutils.EncodeTimestamp(dsw.blockNr)
	if err = dsw.db.Put(dbutils.AccountChangeSetBucket, key, accountSerialised); err != nil {
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
		if err = dsw.db.Put(dbutils.StorageChangeSetBucket, key, storageSerialized); err != nil {
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
	err = writeIndex(dsw.blockNr, accountChanges, dbutils.AccountsHistoryBucket, dsw.db)
	if err != nil {
		return err
	}

	storageChanges, err := dsw.csw.GetStorageChanges()
	if err != nil {
		return err
	}
	err = writeIndex(dsw.blockNr, storageChanges, dbutils.StorageHistoryBucket, dsw.db)
	if err != nil {
		return err
	}

	return nil
}

func writeIndex(blocknum uint64, changes *changeset.ChangeSet, bucket []byte, changeDb ethdb.Database) error {
	for _, change := range changes.Changes {
		currentChunkKey := dbutils.CurrentChunkKey(change.Key)
		indexBytes, err := changeDb.Get(bucket, currentChunkKey)
		if err != nil && err != ethdb.ErrKeyNotFound {
			return fmt.Errorf("find chunk failed: %w", err)
		}

		var index dbutils.HistoryIndexBytes
		if len(indexBytes) == 0 {
			index = dbutils.NewHistoryIndex()
		} else if dbutils.CheckNewIndexChunk(indexBytes, blocknum) {
			// Chunk overflow, need to write the "old" current chunk under its key derived from the last element
			index = dbutils.WrapHistoryIndex(indexBytes)
			indexKey, err := index.Key(change.Key)
			if err != nil {
				return err
			}
			// Flush the old chunk
			if err := changeDb.Put(bucket, indexKey, index); err != nil {
				return err
			}
			// Start a new chunk
			index = dbutils.NewHistoryIndex()
		} else {
			index = dbutils.WrapHistoryIndex(indexBytes)
		}
		index = index.Append(blocknum, len(change.Value) == 0)

		if err := changeDb.Put(bucket, currentChunkKey, index); err != nil {
			return err
		}
	}

	return nil
}
