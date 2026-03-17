package l1sync

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/nitro-erigon/arbutil"
)

func (s *L1SyncService) addRecentBatch(info batchInfo) {
	s.recentBatches = append(s.recentBatches, info)
	if len(s.recentBatches) > maxRecentBatches {
		s.recentBatches = s.recentBatches[len(s.recentBatches)-maxRecentBatches:]
	}
}

// loadRecentBatches loads the last N batches from DB into the in-memory buffer on startup.
func (s *L1SyncService) loadRecentBatches(ctx context.Context, lastBatchSeqNum uint64) {
	start := uint64(0)
	if lastBatchSeqNum >= maxRecentBatches {
		start = lastBatchSeqNum - maxRecentBatches + 1
	}

	_ = s.db.View(ctx, func(tx kv.Tx) error {
		for seq := start; seq <= lastBatchSeqNum; seq++ {
			_, l1Block, cumMsgCount, _, err := readBatch(tx, seq)
			if err != nil {
				continue
			}
			s.recentBatches = append(s.recentBatches, batchInfo{
				seqNum:             seq,
				l1Block:            l1Block,
				cumulativeMsgCount: cumMsgCount,
			})
		}
		// Set cumulative msg count from latest batch
		if len(s.recentBatches) > 0 {
			s.cumulativeMsgCount = s.recentBatches[len(s.recentBatches)-1].cumulativeMsgCount
		}
		return nil
	})

	s.logger.Info("loaded recent batches for finality", "count", len(s.recentBatches))
}

// updateFinality queries L1 for finalized and safe block numbers, maps them to L2 blocks
// via the in-memory batch buffer, and writes the forkchoice entries so that
// eth_getBlockByNumber("finalized") and ("safe") work.
func (s *L1SyncService) updateFinality(ctx context.Context) error {
	if len(s.recentBatches) == 0 {
		return nil
	}

	// Query L1 finalized and safe block numbers
	finalizedHeader, err := s.l1Client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return fmt.Errorf("failed to get L1 finalized block: %w", err)
	}
	safeHeader, err := s.l1Client.HeaderByNumber(ctx, big.NewInt(int64(rpc.SafeBlockNumber)))
	if err != nil {
		return fmt.Errorf("failed to get L1 safe block: %w", err)
	}

	l1Finalized := finalizedHeader.Number.Uint64()
	l1Safe := safeHeader.Number.Uint64()

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin DB transaction for finality: %w", err)
	}
	defer tx.Rollback()

	updated := false

	if hash, ok := s.resolveL2BlockForL1Block(tx, l1Finalized); ok {
		rawdb.WriteForkchoiceFinalized(tx, hash)
		updated = true
		s.logger.Debug("updated finalized block", "l1Finalized", l1Finalized, "l2Hash", hash)
	}

	if hash, ok := s.resolveL2BlockForL1Block(tx, l1Safe); ok {
		rawdb.WriteForkchoiceSafe(tx, hash)
		updated = true
		s.logger.Debug("updated safe block", "l1Safe", l1Safe, "l2Hash", hash)
	}

	if updated {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit finality update: %w", err)
		}
	}

	return nil
}

// resolveL2BlockForL1Block finds the last batch posted at or before the given L1 block,
// converts its cumulative message count to an L2 block number, and returns the L2 block hash.
func (s *L1SyncService) resolveL2BlockForL1Block(tx kv.Tx, l1Block uint64) (common.Hash, bool) {
	msgCount := s.getMsgCountAtL1Block(l1Block)
	if msgCount == 0 {
		return common.Hash{}, false
	}

	l2BlockNum := arbutil.MessageCountToBlockNumber(arbutil.MessageIndex(msgCount), s.config.GenesisBlockNumber)
	if l2BlockNum < 0 {
		return common.Hash{}, false
	}

	hash, err := rawdb.ReadCanonicalHash(tx, uint64(l2BlockNum))
	if err != nil || hash == (common.Hash{}) {
		return common.Hash{}, false
	}

	return hash, true
}

// getMsgCountAtL1Block walks the in-memory batch buffer backward to find
// the last batch with l1Block <= the given L1 block number.
func (s *L1SyncService) getMsgCountAtL1Block(l1Block uint64) uint64 {
	for i := len(s.recentBatches) - 1; i >= 0; i-- {
		if s.recentBatches[i].l1Block <= l1Block {
			return s.recentBatches[i].cumulativeMsgCount
		}
	}
	return 0
}
