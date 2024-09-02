package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func NewGenericLogIndexerUnwinder() UnwindExecutor {
	return func(ctx context.Context, tx kv.RwTx, u *UnwindState, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker) error {
		erc20Unwinder, err := NewTransferLogIndexerUnwinder(tx, kv.OtsERC20TransferIndex, kv.OtsERC20TransferCounter, false)
		if err != nil {
			return err
		}
		defer erc20Unwinder.Dispose()

		erc721Unwinder, err := NewTransferLogIndexerUnwinder(tx, kv.OtsERC721TransferIndex, kv.OtsERC721TransferCounter, true)
		if err != nil {
			return err
		}
		defer erc721Unwinder.Dispose()

		return runLogUnwind(ctx, tx, blockReader, isShortInterval, logEvery, u, TRANSFER_TOPIC, []UnwindHandler{erc20Unwinder, erc721Unwinder})
	}
}

type LogIndexerUnwinder interface {
	UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error
	UnwindAddressHolding(tx kv.RwTx, addr, token common.Address, ethTx uint64) error
	Dispose() error
}

type UnwindHandler interface {
	Unwind(tx kv.RwTx, results []*TransferAnalysisResult, ethTx uint64) error
}

type TransferLogIndexerUnwinder struct {
	indexBucket   string
	counterBucket string
	isNFT         bool
	target        kv.RwCursor
	targetDel     kv.RwCursor
	counter       kv.RwCursorDupSort
}

func NewTransferLogIndexerUnwinder(tx kv.RwTx, indexBucket, counterBucket string, isNFT bool) (*TransferLogIndexerUnwinder, error) {
	target, err := tx.RwCursor(indexBucket)
	if err != nil {
		return nil, err
	}

	targetDel, err := tx.RwCursor(indexBucket)
	if err != nil {
		return nil, err
	}

	counter, err := tx.RwCursorDupSort(counterBucket)
	if err != nil {
		return nil, err
	}

	return &TransferLogIndexerUnwinder{
		indexBucket,
		counterBucket,
		isNFT,
		target,
		targetDel,
		counter,
	}, nil
}

func (u *TransferLogIndexerUnwinder) Dispose() error {
	u.target.Close()
	u.targetDel.Close()
	u.counter.Close()

	return nil
}

func runLogUnwind(ctx context.Context, tx kv.RwTx, blockReader services.FullBlockReader, isShortInterval bool, logEvery *time.Ticker, u *UnwindState, topic []byte, unwinders []UnwindHandler) error {
	analyzer, err := NewTransferLogAnalyzer()
	if err != nil {
		return err
	}

	logs, err := tx.Cursor(kv.Log)
	if err != nil {
		return err
	}
	defer logs.Close()

	// The unwind interval is ]u.UnwindPoint, EOF]
	startBlock := u.UnwindPoint + 1

	// Traverse blocks logs [startBlock, EOF], determine txs that should've matched the criteria,
	// their logs, and their addresses.
	blocks, err := newBlockBitmapFromTopic(tx, startBlock, u.CurrentBlockNumber, topic)
	if err != nil {
		return err
	}
	defer bitmapdb.ReturnToPool(blocks)

	for it := blocks.Iterator(); it.HasNext(); {
		blockNum := uint64(it.Next())

		// Avoid recalculating txid from the block basetxid for each match
		baseTxId, err := blockReader.BaseTxIdForBlock(ctx, tx, blockNum)
		if err != nil {
			return err
		}

		// Inspect each block's tx logs
		logPrefix := hexutility.EncodeTs(blockNum)
		k, v, err := logs.Seek(logPrefix)
		if err != nil {
			return err
		}
		for k != nil && bytes.HasPrefix(k, logPrefix) {
			txLogs := newTxLogsFromRaw[TransferAnalysisResult](blockNum, baseTxId, k, v)
			results, err := AnalyzeLogs[TransferAnalysisResult](tx, analyzer, txLogs.rawLogs)
			if err != nil {
				return err
			}

			if len(results) > 0 {
				for _, unwinder := range unwinders {
					if err := unwinder.Unwind(tx, results, txLogs.ethTx); err != nil {
						return err
					}
				}
			}

			select {
			default:
			case <-ctx.Done():
				return common.ErrStopped
			case <-logEvery.C:
				log.Info("Unwinding log indexer", "blockNum", blockNum)
			}

			k, v, err = logs.Next()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (u *TransferLogIndexerUnwinder) Unwind(tx kv.RwTx, results []*TransferAnalysisResult, ethTx uint64) error {
	for _, r := range results {
		if err := r.Unwind(tx, u.isNFT, u, ethTx); err != nil {
			return err
		}
	}

	return nil
}

func (u *TransferLogIndexerUnwinder) UnwindAddressHolding(tx kv.RwTx, addr, token common.Address, ethTx uint64) error {
	return fmt.Errorf("NOT IMPLEMENTED; SHOULDN'T BE CALLED")
}

func (u *TransferLogIndexerUnwinder) UnwindAddress(tx kv.RwTx, addr common.Address, ethTx uint64) error {
	return unwindAddress(tx, u.target, u.targetDel, u.counter, u.indexBucket, u.counterBucket, addr, ethTx)
}
