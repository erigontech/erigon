package stagedsync

import (
	"context"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

// Handles ERC20 and ERC721 indexing simultaneously
type MultiLogIndexerHandler[T any] struct {
	handlers []LogIndexerHandler[T]
}

func NewMultiIndexerHandler[T any](handlers ...LogIndexerHandler[T]) *MultiLogIndexerHandler[T] {
	return &MultiLogIndexerHandler[T]{
		handlers,
	}
}

func (c *MultiLogIndexerHandler[T]) HandleMatch(output *TxMatchedLogs[T]) {
	for _, h := range c.handlers {
		h.HandleMatch(output)
	}
}

func (c *MultiLogIndexerHandler[T]) Flush(force bool) error {
	for _, h := range c.handlers {
		if err := h.Flush(force); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiLogIndexerHandler[T]) Load(ctx context.Context, tx kv.RwTx) error {
	for _, h := range c.handlers {
		if err := h.Load(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func (c *MultiLogIndexerHandler[T]) Close() {
	for _, h := range c.handlers {
		h.Close()
	}
}

// Implements LogIndexerHandler interface in order to index token transfers
// (ERC20/ERC721)
type TransferLogIndexerHandler struct {
	IndexHandler
	nft bool
}

func NewTransferLogIndexerHandler(tmpDir string, s *StageState, nft bool, indexBucket, counterBucket string, logger log.Logger) LogIndexerHandler[TransferAnalysisResult] {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}

	return &TransferLogIndexerHandler{
		&StandardIndexHandler{indexBucket, counterBucket, collector, bitmaps},
		nft,
	}
}

// Add log's ethTx index to from/to addresses indexes
func (h *TransferLogIndexerHandler) HandleMatch(match *TxMatchedLogs[TransferAnalysisResult]) {
	for _, res := range match.matchResults {
		if res.nft != h.nft {
			continue
		}

		// Register this ethTx into from/to transfer addresses indexes
		h.TouchIndex(res.from, match.ethTx)
		h.TouchIndex(res.to, match.ethTx)
	}
}
