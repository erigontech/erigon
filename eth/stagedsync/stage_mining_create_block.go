package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func SpawnMiningCreateBlockStage(s *StageState, tx ethdb.Database, chainConfig *params.ChainConfig, chainContext *core.TinyChainContext, vmConfig *vm.Config, block *types.Block, quit <-chan struct{}, params ExecuteBlockStageParams) (*types.Block, error) {
	//TODO: create real block, use code from /Users/alex.sharov/projects/turbo-geth/miner/worker.go
	miningBlock := types.NewBlockWithHeader(nil)
	s.Done()
	return miningBlock, nil
}
