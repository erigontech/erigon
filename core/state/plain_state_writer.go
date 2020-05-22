package state

import (
	"context"
	"encoding/binary"

	"github.com/VictoriaMetrics/fastcache"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var _ WriterWithChangeSets = (*PlainStateWriter)(nil)

type PlainStateWriter struct {
	stateDb                ethdb.Database
	changeDb               ethdb.Database
	uncommitedIncarnations map[common.Address]uint64
	csw                    *ChangeSetWriter
	blockNumber            uint64
	accountCache           *fastcache.Cache
	storageCache           *fastcache.Cache
	codeCache              *fastcache.Cache
	codeSizeCache          *fastcache.Cache
}

func NewPlainStateWriter(stateDb, changeDb ethdb.Database, blockNumber uint64, uncommitedIncarnations map[common.Address]uint64) *PlainStateWriter {
	return &PlainStateWriter{
		stateDb:                stateDb,
		changeDb:               changeDb,
		uncommitedIncarnations: uncommitedIncarnations,
		csw:                    NewChangeSetWriterPlain(),
		blockNumber:            blockNumber,
	}
}

func (w *PlainStateWriter) SetAccountCache(accountCache *fastcache.Cache) {
	w.accountCache = accountCache
}

func (w *PlainStateWriter) SetStorageCache(storageCache *fastcache.Cache) {
	w.storageCache = storageCache
}

func (w *PlainStateWriter) SetCodeCache(codeCache *fastcache.Cache) {
	w.codeCache = codeCache
}

func (w *PlainStateWriter) SetCodeSizeCache(codeSizeCache *fastcache.Cache) {
	w.codeSizeCache = codeSizeCache
}

func (w *PlainStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if err := w.csw.UpdateAccountData(ctx, address, original, account); err != nil {
		return err
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if w.accountCache != nil {
		w.accountCache.Set(address[:], value)
	}
	return w.stateDb.Put(dbutils.PlainStateBucket, address[:], value)
}

func (w *PlainStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := w.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
		return err
	}
	if w.codeCache != nil && len(code) <= 1024 {
		w.codeCache.Set(address[:], code)
	}
	if w.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(code)))
		w.codeSizeCache.Set(address[:], b[:])
	}
	if err := w.stateDb.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}
	return w.stateDb.Put(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address, incarnation), codeHash[:])
}

func (w *PlainStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if err := w.csw.DeleteAccount(ctx, address, original); err != nil {
		return err
	}
	if original.Incarnation > 0 {
		w.uncommitedIncarnations[address] = original.Incarnation
	}
	if w.accountCache != nil {
		w.accountCache.Set(address[:], nil)
	}
	if w.codeCache != nil {
		w.codeCache.Set(address[:], nil)
	}
	if w.codeSizeCache != nil {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], 0)
		w.codeSizeCache.Set(address[:], b[:])
	}
	return w.stateDb.Delete(dbutils.PlainStateBucket, address[:])
}

func (w *PlainStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key, original, value *common.Hash) error {
	if err := w.csw.WriteAccountStorage(ctx, address, incarnation, key, original, value); err != nil {
		return err
	}
	if *original == *value {
		return nil
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address, incarnation, *key)

	v := cleanUpTrailingZeroes(value[:])
	if w.storageCache != nil {
		w.storageCache.Set(compositeKey, v)
	}
	if len(v) == 0 {
		return w.stateDb.Delete(dbutils.PlainStateBucket, compositeKey)
	}
	return w.stateDb.Put(dbutils.PlainStateBucket, compositeKey, common.CopyBytes(v))
}

func (w *PlainStateWriter) CreateContract(address common.Address) error {
	return w.csw.CreateContract(address)
}

func (w *PlainStateWriter) WriteChangeSets() error {
	accountChanges, err := w.csw.GetAccountChanges()
	if err != nil {
		return err
	}
	var accountSerialised []byte
	accountSerialised, err = changeset.EncodeAccountsPlain(accountChanges)
	if err != nil {
		return err
	}
	key := dbutils.EncodeTimestamp(w.blockNumber)
	if err = w.changeDb.Put(dbutils.PlainAccountChangeSetBucket, key, accountSerialised); err != nil {
		return err
	}
	storageChanges, err := w.csw.GetStorageChanges()
	if err != nil {
		return err
	}
	var storageSerialized []byte
	if storageChanges.Len() > 0 {
		storageSerialized, err = changeset.EncodeStoragePlain(storageChanges)
		if err != nil {
			return err
		}
		if err = w.changeDb.Put(dbutils.PlainStorageChangeSetBucket, key, storageSerialized); err != nil {
			return err
		}
	}
	return nil
}
