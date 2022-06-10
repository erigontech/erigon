package builder

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
)

// BlockBuilder builds Proof-of-Stake payloads (PoS "mining")
type BlockBuilder struct {
}

func NewBlockBuilder(ctx context.Context, tx kv.Tx, param *core.BlockBuilderParameters) *BlockBuilder {
	// TODO: start a builder goroutine that stops on ctx.Done
	tx.Rollback()
	return &BlockBuilder{}
}

/*
	// proof-of-stake mining
	assembleBlockPOS := func(param *core.BlockProposerParametersPOS) (*types.Block, error) {
		miningStatePos := stagedsync.NewProposingState(&config.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			stagedsync.MiningStages(backend.sentryCtx,
				stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miningStatePos, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, param, tmpdir),
				stagedsync.StageMiningExecCfg(backend.chainDB, miningStatePos, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir),
				stagedsync.StageHashStateCfg(backend.chainDB, tmpdir),
				stagedsync.StageTrieCfg(backend.chainDB, false, true, tmpdir, blockReader),
				stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miningStatePos, backend.miningSealingQuit),
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)
		// We start the mining step
		if err := stages2.MiningStep(ctx, backend.chainDB, proposingSync); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultPOSCh
		return block, nil
	}
*/

func (b *BlockBuilder) Stop() *types.Block {
	// TODO: implement idempotently
	return nil
}
