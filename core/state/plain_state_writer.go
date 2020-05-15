package state

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var _ WriterWithChangeSets = (*PlainStateWriter)(nil)

type PlainStateWriter struct {
	db                     ethdb.Database
	uncommitedIncarnations map[common.Address]uint64
	csw                    *ChangeSetWriter
	blockNumber            uint64
}

func NewPlainStateWriter(db ethdb.Database, blockNumber uint64, uncommitedIncarnations map[common.Address]uint64) *PlainStateWriter {
	return &PlainStateWriter{
		db:                     db,
		uncommitedIncarnations: uncommitedIncarnations,
		csw:                    NewChangeSetWriterPlain(),
		blockNumber:            blockNumber,
	}
}

func (w *PlainStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if err := w.csw.UpdateAccountData(ctx, address, original, account); err != nil {
		return err
	}

	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	return w.db.Put(dbutils.PlainStateBucket, address[:], value)
}

func (w *PlainStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := w.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
		return err
	}
	if err := w.db.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}

	return w.db.Put(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address, incarnation), codeHash[:])
}

func (w *PlainStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if err := w.csw.DeleteAccount(ctx, address, original); err != nil {
		return err
	}

	if original.Incarnation > 0 {
		w.uncommitedIncarnations[address] = original.Incarnation
	}

	if err := w.db.Delete(dbutils.PlainStateBucket, address[:]); err != nil {
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

	if len(v) == 0 {
		if err := w.db.Delete(dbutils.PlainStateBucket, compositeKey); err != nil {
			if err != ethdb.ErrKeyNotFound {
				return err
			}
		}
		return nil
	}

	vv := make([]byte, len(v))
	copy(vv, v)
	return w.db.Put(dbutils.PlainStateBucket, compositeKey, vv)
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
	if err = w.db.Put(dbutils.PlainAccountChangeSetBucket, key, accountSerialised); err != nil {
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
		if err = w.db.Put(dbutils.PlainStorageChangeSetBucket, key, storageSerialized); err != nil {
			return err
		}
	}
	return nil
}
