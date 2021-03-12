package stagedsync

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

func SpawnMiningExecuteBlockStage(s *StageState, tx ethdb.Database, chainConfig *params.ChainConfig, chainContext *core.TinyChainContext, vmConfig *vm.Config, block *types.Block, quit <-chan struct{}, params ExecuteBlockStageParams) error {
	logPrefix := s.state.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Blocks execution", logPrefix), "from", s.BlockNumber)
	batch := tx.NewBatch()
	defer batch.Rollback()
	chainContext.SetDB(tx)
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	if err := executeBlockWithGo(block, tx, nil, batch, chainConfig, chainContext, vmConfig, params); err != nil {
		return err
	}
	if err := s.Update(batch, s.BlockNumber+1); err != nil {
		return err
	}
	if _, err := batch.Commit(); err != nil {
		return fmt.Errorf("%s: failed to write batch commit: %v", logPrefix, err)
	}
	s.Done()
	return nil
}
