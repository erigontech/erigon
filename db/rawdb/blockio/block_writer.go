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

package blockio

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
)

//Naming:
//  Prune: delete old data
//  Unwind: delete recent data

// BlockWriter can write blocks from db and snapshots
type BlockWriter struct {
}

func NewBlockWriter() *BlockWriter {
	return &BlockWriter{}
}

func (w *BlockWriter) FillHeaderNumberIndex(logPrefix string, tx kv.RwTx, tmpDir string, from, to uint64, ctx context.Context, logger log.Logger) error {
	startKey := make([]byte, 8)
	binary.BigEndian.PutUint64(startKey, from)
	endKey := dbutils.HeaderKey(to, common.Hash{}) // etl.Tranform uses ExractEndKey as exclusive bound, therefore +1

	return etl.Transform(
		logPrefix,
		tx,
		kv.Headers,
		kv.HeaderNumber,
		tmpDir,
		extractHeaders,
		etl.IdentityLoadFunc,
		etl.TransformArgs{
			ExtractStartKey: startKey,
			ExtractEndKey:   endKey,
			Quit:            ctx.Done(),
		},
		logger,
	)
}

func (w *BlockWriter) MakeBodiesCanonical(tx kv.RwTx, from uint64) error {
	if err := rawdb.AppendCanonicalTxNums(tx, from); err != nil {
		var e1 rawdbv3.ErrTxNumsAppendWithGap
		if ok := errors.As(err, &e1); ok {
			// try again starting from latest available  block
			return rawdb.AppendCanonicalTxNums(tx, e1.LastBlock()+1)
		}
		return err
	}
	return nil
}
func (w *BlockWriter) MakeBodiesNonCanonical(tx kv.RwTx, from uint64) error {
	if err := rawdbv3.TxNums.Truncate(tx, from); err != nil {
		return err
	}
	return nil
}

func extractHeaders(k []byte, _ []byte, next etl.ExtractNextFunc) error {
	// We only want to extract entries composed by Block Number + Header Hash
	if len(k) != 40 {
		return nil
	}
	return next(k, k[8:], k[:8])
}

func (w *BlockWriter) TruncateBodies(db kv.RoDB, tx kv.RwTx, from uint64) error {
	fromB := hexutil.EncodeTs(from)
	if err := tx.ForEach(kv.BlockBody, fromB, func(k, _ []byte) error { return tx.Delete(kv.BlockBody, k) }); err != nil {
		return err
	}

	if err := backup.ClearTables(context.Background(), tx, kv.EthTx, kv.MaxTxNum); err != nil {
		return err
	}
	if err := tx.ResetSequence(kv.EthTx, 0); err != nil {
		return err
	}

	return nil
}

var (
	mxPruneTookBlocks = metrics.GetOrCreateSummary(`prune_seconds{type="blocks"}`)
)

// PruneBlocks - [1, to) old blocks after moving it to snapshots.
// keeps genesis in db
// doesn't change sequences of kv.EthTx
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
func (w *BlockWriter) PruneBlocks(ctx context.Context, tx kv.RwTx, blockTo uint64, blocksDeleteLimit int) (deleted int, err error) {
	defer mxPruneTookBlocks.ObserveDuration(time.Now())
	return rawdb.PruneBlocks(tx, blockTo, blocksDeleteLimit)
}
