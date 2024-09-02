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

func ERC20And721HolderIndexerExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	analyzer, err := NewTransferLogAnalyzer()
	if err != nil {
		return startBlock, err
	}

	aggrHandler := NewMultiIndexerHandler[TransferAnalysisResult](
		NewTransferLogHolderHandler(tmpDir, s, false, kv.OtsERC20Holdings, logger),
		NewTransferLogHolderHandler(tmpDir, s, true, kv.OtsERC721Holdings, logger),
	)
	defer aggrHandler.Close()

	if startBlock == 0 && isInternalTx {
		return runConcurrentLogIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, analyzer, aggrHandler)
	}
	return runIncrementalLogIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, analyzer, aggrHandler)
}
