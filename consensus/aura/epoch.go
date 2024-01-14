package aura

import (
	"context"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
)

type NonTransactionalEpochReader struct {
	db kv.RwDB
}

func newEpochReader(db kv.RwDB) *NonTransactionalEpochReader {
	return &NonTransactionalEpochReader{db: db}
}

func (cr *NonTransactionalEpochReader) GetEpoch(hash libcommon.Hash, number uint64) (v []byte, err error) {
	return v, cr.db.View(context.Background(), func(tx kv.Tx) error {
		v, err = rawdb.ReadEpoch(tx, number, hash)
		return err
	})
}
func (cr *NonTransactionalEpochReader) PutEpoch(hash libcommon.Hash, number uint64, proof []byte) error {
	return cr.db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteEpoch(tx, number, hash, proof)
	})
}
func (cr *NonTransactionalEpochReader) GetPendingEpoch(hash libcommon.Hash, number uint64) (v []byte, err error) {
	return v, cr.db.View(context.Background(), func(tx kv.Tx) error {
		v, err = rawdb.ReadPendingEpoch(tx, number, hash)
		return err
	})
}
func (cr *NonTransactionalEpochReader) PutPendingEpoch(hash libcommon.Hash, number uint64, proof []byte) error {
	return cr.db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WritePendingEpoch(tx, number, hash, proof)
	})
}
func (cr *NonTransactionalEpochReader) FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash libcommon.Hash, transitionProof []byte, err error) {
	return blockNum, blockHash, transitionProof, cr.db.View(context.Background(), func(tx kv.Tx) error {
		blockNum, blockHash, transitionProof, err = rawdb.FindEpochBeforeOrEqualNumber(tx, number)
		return err
	})
}
