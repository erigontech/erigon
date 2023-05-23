package blockio

import (
	"context"
	"encoding/binary"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/backup"
	"github.com/ledgerwatch/log/v3"
)

// BlockReader can read blocks from db and snapshots
type BlockWriter struct {
	txsV3 bool
}

func NewBlockWriter(txsV3 bool) *BlockWriter { return &BlockWriter{txsV3: txsV3} }

func (w *BlockWriter) TxsV3Enabled() bool { return w.txsV3 }
func (w *BlockWriter) WriteHeader(tx kv.RwTx, header *types.Header) error {
	return rawdb.WriteHeader(tx, header)
}
func (w *BlockWriter) WriteHeaderRaw(tx kv.StatelessRwTx, number uint64, hash common.Hash, headerRlp []byte, skipIndexing bool) error {
	return rawdb.WriteHeaderRaw(tx, number, hash, headerRlp, skipIndexing)
}
func (w *BlockWriter) WriteCanonicalHash(tx kv.RwTx, hash common.Hash, number uint64) error {
	return rawdb.WriteCanonicalHash(tx, hash, number)
}
func (w *BlockWriter) WriteTd(db kv.Putter, hash common.Hash, number uint64, td *big.Int) error {
	return rawdb.WriteTd(db, hash, number, td)
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

func extractHeaders(k []byte, v []byte, next etl.ExtractNextFunc) error {
	// We only want to extract entries composed by Block Number + Header Hash
	if len(k) != 40 {
		return nil
	}
	return next(k, common.Copy(k[8:]), common.Copy(k[:8]))
}

func (w *BlockWriter) WriteRawBodyIfNotExists(tx kv.RwTx, hash common.Hash, number uint64, body *types.RawBody) (ok bool, lastTxnNum uint64, err error) {
	return rawdb.WriteRawBodyIfNotExists(tx, hash, number, body)
}
func (w *BlockWriter) WriteBody(tx kv.RwTx, hash common.Hash, number uint64, body *types.Body) error {
	return rawdb.WriteBody(tx, hash, number, body)
}
func (w *BlockWriter) TruncateBodies(db kv.RoDB, tx kv.RwTx, from uint64) error {
	fromB := hexutility.EncodeTs(from)
	if err := tx.ForEach(kv.BlockBody, fromB, func(k, _ []byte) error { return tx.Delete(kv.BlockBody, k) }); err != nil {
		return err
	}
	ethtx := kv.EthTx
	transactionV3, err := kvcfg.TransactionsV3.Enabled(tx)
	if err != nil {
		panic(err)
	}
	if transactionV3 {
		ethtx = kv.EthTxV3
	}

	if err := backup.ClearTables(context.Background(), db, tx,
		kv.NonCanonicalTxs,
		ethtx,
		kv.MaxTxNum,
	); err != nil {
		return err
	}
	if err := rawdb.ResetSequence(tx, ethtx, 0); err != nil {
		return err
	}

	if err := rawdb.ResetSequence(tx, kv.NonCanonicalTxs, 0); err != nil {
		return err
	}
	return nil
}

func (w *BlockWriter) TruncateBlocks(ctx context.Context, tx kv.RwTx, blockFrom uint64) error {
	return rawdb.TruncateBlocks(ctx, tx, blockFrom)
}
func (w *BlockWriter) TruncateTd(tx kv.RwTx, blockFrom uint64) error {
	return rawdb.TruncateTd(tx, blockFrom)
}
func (w *BlockWriter) ResetSenders(ctx context.Context, db kv.RoDB, tx kv.RwTx) error {
	return backup.ClearTables(ctx, db, tx, kv.Senders)
}
