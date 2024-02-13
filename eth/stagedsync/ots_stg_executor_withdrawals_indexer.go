package stagedsync

import (
	"context"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func WithdrawalsExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	withdrawalHandler, err := NewWithdrawalsIndexerHandler(tx, tmpDir, s, logger)
	if err != nil {
		return startBlock, err
	}
	defer withdrawalHandler.Close()

	return runIncrementalBodyIndexerExecutor(db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, withdrawalHandler)
}

// Implements BodyIndexerHandler interface in order to index block withdrawals from CL
type WithdrawalsIndexerHandler struct {
	IndexHandler
	withdrawalIdx2Block kv.RwCursor
}

func NewWithdrawalsIndexerHandler(tx kv.RwTx, tmpDir string, s *StageState, logger log.Logger) (BodyIndexerHandler, error) {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}
	withdrawalIdx2Block, err := tx.RwCursor(kv.OtsWithdrawalIdx2Block)
	if err != nil {
		return nil, err
	}

	return &WithdrawalsIndexerHandler{
		&StandardIndexHandler{kv.OtsWithdrawalsIndex, kv.OtsWithdrawalsCounter, collector, bitmaps},
		withdrawalIdx2Block,
	}, nil
}

// Index all withdrawals from a block body;
// withdrawal address -> withdrawal index (NOT blockNum!!!)
func (h *WithdrawalsIndexerHandler) HandleMatch(blockNum uint64, body *types.Body) error {
	withdrawals := body.Withdrawals
	if len(withdrawals) == 0 {
		return nil
	}
	last := withdrawals[len(withdrawals)-1]

	k := hexutility.EncodeTs(last.Index)
	v := hexutility.EncodeTs(blockNum)
	if err := h.withdrawalIdx2Block.Put(k, v); err != nil {
		return err
	}

	for _, w := range withdrawals {
		h.TouchIndex(w.Address, w.Index)
	}

	return nil
}

func (h *WithdrawalsIndexerHandler) Close() {
	h.IndexHandler.Close()
	h.withdrawalIdx2Block.Close()
}
