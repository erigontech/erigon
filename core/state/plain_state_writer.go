package state

import (
	"context"
	"encoding/binary"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

var _ WriterWithChangeSets = (*PlainStateWriter)(nil)

type PlainStateWriter struct {
	db          ethdb.Database
	csw         *ChangeSetWriter
	accumulator *shards.Accumulator
}

func NewPlainStateWriter(db ethdb.Database, changeSetsDB ethdb.RwTx, blockNumber uint64) *PlainStateWriter {
	return &PlainStateWriter{
		db:  db,
		csw: NewChangeSetWriterPlain(changeSetsDB, blockNumber),
	}
}

func NewPlainStateWriterNoHistory(db ethdb.Database) *PlainStateWriter {
	return &PlainStateWriter{
		db: db,
	}
}

func (w *PlainStateWriter) SetAccumulator(accumulator *shards.Accumulator) *PlainStateWriter {
	w.accumulator = accumulator
	return w
}

func (w *PlainStateWriter) UpdateAccountData(ctx context.Context, address common.Address, original, account *accounts.Account) error {
	if w.csw != nil {
		if err := w.csw.UpdateAccountData(ctx, address, original, account); err != nil {
			return err
		}
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address, value)
	}
	return w.db.Put(dbutils.PlainStateBucket, address[:], value)
}

func (w *PlainStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.csw != nil {
		if err := w.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
			return err
		}
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address, incarnation, code)
	}
	if err := w.db.Put(dbutils.CodeBucket, codeHash[:], code); err != nil {
		return err
	}
	return w.db.Put(dbutils.PlainContractCodeBucket, dbutils.PlainGenerateStoragePrefix(address[:], incarnation), codeHash[:])
}

func (w *PlainStateWriter) DeleteAccount(ctx context.Context, address common.Address, original *accounts.Account) error {
	if w.csw != nil {
		if err := w.csw.DeleteAccount(ctx, address, original); err != nil {
			return err
		}
	}
	if w.accumulator != nil {
		w.accumulator.DeleteAccount(address)
	}
	if err := w.db.Delete(dbutils.PlainStateBucket, address[:], nil); err != nil {
		return err
	}
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		if err := w.db.Put(dbutils.IncarnationMapBucket, address[:], b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (w *PlainStateWriter) WriteAccountStorage(ctx context.Context, address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if w.csw != nil {
		if err := w.csw.WriteAccountStorage(ctx, address, incarnation, key, original, value); err != nil {
			return err
		}
	}
	if *original == *value {
		return nil
	}
	compositeKey := dbutils.PlainGenerateCompositeStorageKey(address.Bytes(), incarnation, key.Bytes())

	v := value.Bytes()
	if w.accumulator != nil {
		w.accumulator.ChangeStorage(address, incarnation, *key, v)
	}
	if len(v) == 0 {
		return w.db.Delete(dbutils.PlainStateBucket, compositeKey, nil)
	}
	return w.db.Put(dbutils.PlainStateBucket, compositeKey, v)
}

func (w *PlainStateWriter) CreateContract(address common.Address) error {
	if w.csw != nil {
		if err := w.csw.CreateContract(address); err != nil {
			return err
		}
	}
	return nil
}

func (w *PlainStateWriter) WriteChangeSets() error {
	if w.csw != nil {
		return w.csw.WriteChangeSets()
	}

	return nil
}

func (w *PlainStateWriter) WriteHistory() error {
	if w.csw != nil {
		return w.csw.WriteHistory()
	}

	return nil
}

func (w *PlainStateWriter) ChangeSetWriter() *ChangeSetWriter {
	return w.csw
}
