package stagedsync

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func WithdrawalsExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	withdrawalHandler := NewWithdrawalsIndexerHandler(tmpDir, s, logger)
	defer withdrawalHandler.Close()

	return runIncrementalBlockIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, withdrawalHandler)
}
