// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package aura

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
)

type NonTransactionalEpochReader struct {
	db       kv.RwDB
	readonly bool
}

func newEpochReader(db kv.RwDB) *NonTransactionalEpochReader {
	return &NonTransactionalEpochReader{db: db}
}

func (cr *NonTransactionalEpochReader) GetEpoch(hash common.Hash, number uint64) (v []byte, err error) {
	return v, cr.db.View(context.Background(), func(tx kv.Tx) error {
		v, err = rawdb.ReadEpoch(tx, number, hash)
		return err
	})
}
func (cr *NonTransactionalEpochReader) PutEpoch(hash common.Hash, number uint64, proof []byte) error {
	if cr.readonly {
		return nil
	}
	return cr.db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteEpoch(tx, number, hash, proof)
	})
}
func (cr *NonTransactionalEpochReader) GetPendingEpoch(hash common.Hash, number uint64) (v []byte, err error) {
	return v, cr.db.View(context.Background(), func(tx kv.Tx) error {
		v, err = rawdb.ReadPendingEpoch(tx, number, hash)
		return err
	})
}
func (cr *NonTransactionalEpochReader) PutPendingEpoch(hash common.Hash, number uint64, proof []byte) error {
	if cr.readonly {
		return nil
	}
	return cr.db.UpdateNosync(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WritePendingEpoch(tx, number, hash, proof)
	})
}
func (cr *NonTransactionalEpochReader) FindBeforeOrEqualNumber(number uint64) (blockNum uint64, blockHash common.Hash, transitionProof []byte, err error) {
	return blockNum, blockHash, transitionProof, cr.db.View(context.Background(), func(tx kv.Tx) error {
		blockNum, blockHash, transitionProof, err = rawdb.FindEpochBeforeOrEqualNumber(tx, number)
		return err
	})
}
