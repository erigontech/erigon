package state

import (
	"context"

	lru "github.com/hashicorp/golang-lru"

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
	accountCache           *lru.Cache
	storageCache           *lru.Cache
	codeCache              *lru.Cache
	codeSizeCache          *lru.Cache
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

func (w *PlainStateWriter) SetAccountCache(accountCache *lru.Cache) {
	w.accountCache = accountCache
}

func (w *PlainStateWriter) SetStorageCache(storageCache *lru.Cache) {
	w.storageCache = storageCache
}

func (w *PlainStateWriter) SetCodeCache(codeCache *lru.Cache) {
	w.codeCache = codeCache
}

func (w *PlainStateWriter) SetCodeSizeCache(codeSizeCache *lru.Cache) {
	w.codeSizeCache = codeSizeCache
}

func (w *PlainStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if err := w.csw.UpdateAccountData(ctx, address, original, account); err != nil {
		return err
	}
	if w.accountCache != nil {
		w.accountCache.Add(address, account)
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	return w.stateDb.Put(dbutils.PlainStateBucket, address[:], value)
}

func (w *PlainStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := w.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
		return err
	}
	if w.codeCache != nil {
		w.codeCache.Add(address, code)
	}
	if w.codeSizeCache != nil {
		w.codeSizeCache.Add(address, len(code))
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
		w.accountCache.Add(address, nil)
	}
	if err := w.stateDb.Delete(dbutils.PlainStateBucket, address[:]); err != nil {
		if err != ethdb.ErrKeyNotFound {
			return err
		}
	}
	return nil
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
	if len(v) == common.HashLength {
		v = common.CopyBytes(v)
	}
	if w.storageCache != nil {
		var storageKey [20 + 32]byte
		copy(storageKey[:], address[:])
		copy(storageKey[20:], key[:])
		w.storageCache.Add(storageKey, v)
	}
	if len(v) == 0 {
		if err := w.stateDb.Delete(dbutils.PlainStateBucket, compositeKey); err != nil {
			if err != ethdb.ErrKeyNotFound {
				return err
			}
		}
		return nil
	}
	return w.stateDb.Put(dbutils.PlainStateBucket, compositeKey, v)
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
