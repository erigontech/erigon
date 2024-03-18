package stagedsync

import (
	"context"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func BlocksRewardedExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	blocksRewardedHandler := NewBlocksRewardedIndexerHandler(tmpDir, s, logger)
	defer blocksRewardedHandler.Close()

	return runIncrementalHeaderIndexerExecutor(db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, blocksRewardedHandler)
}

// Implements HeaderIndexerHandler interface in order to index blocks rewarded
type BlocksRewardedIndexerHandler struct {
	IndexHandler
}

func NewBlocksRewardedIndexerHandler(tmpDir string, s *StageState, logger log.Logger) HeaderIndexerHandler {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}

	return &BlocksRewardedIndexerHandler{
		&StandardIndexHandler{kv.OtsBlocksRewardedIndex, kv.OtsBlocksRewardedCounter, collector, bitmaps},
	}
}

// Index fee recipient address -> blockNum
func (h *BlocksRewardedIndexerHandler) HandleMatch(header *types.Header) {
	h.TouchIndex(header.Coinbase, header.Number.Uint64())
}
