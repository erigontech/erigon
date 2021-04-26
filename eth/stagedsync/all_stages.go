package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func createStageBuilders(blocks []*types.Block, blockNum uint64, checkRoot bool) StageBuilders {
	return []StageBuilder{
		{
			ID: stages.BlockHashes,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.BlockHashes,
					Description: "Write block hashes",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnBlockHashStage(s, world.DB, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return u.Done(world.DB)
					},
				}
			},
		},
		{
			ID: stages.Bodies,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.Bodies,
					Description: "Download block bodies",
					ExecFunc: func(s *StageState, u Unwinder) error {
						if _, err := core.InsertBodyChain("logPrefix", context.TODO(), world.TX, blocks, true /* newCanonical */); err != nil {
							return err
						}
						return s.DoneAndUpdate(world.TX, blockNum)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return u.Done(world.DB)
					},
				}
			},
		},
		{
			ID: stages.Senders,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.Senders,
					Description: "Recover senders from tx signatures",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnRecoverSendersStage(world.senders, s, world.TX, 0, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindSendersStage(u, s, world.TX)
					},
				}
			},
		},
		{
			ID: stages.Execution,
			Build: func(world StageParameters) *Stage {
				execCfg := StageExecuteBlocksCfg(world.storageMode.Receipts, world.BatchSize, world.stateReaderBuilder, world.stateWriterBuilder, world.silkwormExecutionFunc, nil, world.ChainConfig, world.Engine, world.vmConfig, world.TmpDir)
				return &Stage{
					ID:          stages.Execution,
					Description: "Execute blocks w/o hash checks",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnExecuteBlocksStage(s, world.TX, 0, world.QuitCh, execCfg)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindExecutionStage(u, s, world.TX, world.QuitCh, execCfg)
					},
				}
			},
		},
		{
			ID: stages.HashState,
			Build: func(world StageParameters) *Stage {
				hashStateCfg := StageHashStateCfg(world.TmpDir)
				return &Stage{
					ID:          stages.HashState,
					Description: "Hash the key in the state",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnHashStateStage(s, world.TX, hashStateCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindHashStateStage(u, s, world.TX, hashStateCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.IntermediateHashes,
			Build: func(world StageParameters) *Stage {
				stageTrieCfg := StageTrieCfg(checkRoot, true, world.TmpDir)
				return &Stage{
					ID:          stages.IntermediateHashes,
					Description: "Generate intermediate hashes and computing state root",
					ExecFunc: func(s *StageState, u Unwinder) error {
						_, err := SpawnIntermediateHashesStage(s, world.TX, stageTrieCfg, world.QuitCh)
						return err
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindIntermediateHashesStage(u, s, world.TX, stageTrieCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.AccountHistoryIndex,
			Build: func(world StageParameters) *Stage {
				cfg := StageHistoryCfg(world.TmpDir)
				return &Stage{
					ID:                  stages.AccountHistoryIndex,
					Description:         "Generate account history index",
					Disabled:            !world.storageMode.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnAccountHistoryIndex(s, world.TX, cfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindAccountHistoryIndex(u, s, world.TX, cfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.StorageHistoryIndex,
			Build: func(world StageParameters) *Stage {
				cfg := StageHistoryCfg(world.TmpDir)
				return &Stage{
					ID:                  stages.StorageHistoryIndex,
					Description:         "Generate storage history index",
					Disabled:            !world.storageMode.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnStorageHistoryIndex(s, world.TX, cfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindStorageHistoryIndex(u, s, world.TX, cfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.LogIndex,
			Build: func(world StageParameters) *Stage {
				cfg := StageLogIndexCfg(world.TmpDir)
				return &Stage{
					ID:                  stages.LogIndex,
					Description:         "Generate receipt logs index",
					Disabled:            !world.storageMode.Receipts,
					DisabledDescription: "Enable by adding `r` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnLogIndex(s, world.TX, cfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindLogIndex(u, s, world.TX, cfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.CallTraces,
			Build: func(world StageParameters) *Stage {
				callTracesCfg := StageCallTracesCfg(0, world.BatchSize, world.TmpDir, world.ChainConfig, world.Engine)

				return &Stage{
					ID:                  stages.CallTraces,
					Description:         "Generate call traces index",
					Disabled:            !world.storageMode.CallTraces,
					DisabledDescription: "Work In Progress",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnCallTraces(s, world.TX, world.QuitCh, callTracesCfg)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindCallTraces(u, s, world.TX, world.QuitCh, callTracesCfg)
					},
				}
			},
		},
		{
			ID: stages.TxLookup,
			Build: func(world StageParameters) *Stage {
				txLookupCfg := StageTxLookupCfg(world.TmpDir)
				return &Stage{
					ID:                  stages.TxLookup,
					Description:         "Generate tx lookup index",
					Disabled:            !world.storageMode.TxIndex,
					DisabledDescription: "Enable by adding `t` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnTxLookup(s, world.TX, txLookupCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindTxLookup(u, s, world.TX, txLookupCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.Finish,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.Finish,
					Description: "Final: update current block for the RPC API",
					ExecFunc: func(s *StageState, _ Unwinder) error {
						return FinishForward(s, world.DB, world.notifier)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindFinish(u, s, world.DB)
					},
				}
			},
		},
	}
}

// Emulates the effect of blockchain.SetHead() in go-ethereum
func SetHead(db ethdb.Database, config *params.ChainConfig, vmConfig *vm.Config, engine consensus.Engine, newHead uint64, checkRoot bool) error {
	newHeadHash, err := rawdb.ReadCanonicalHash(db, newHead)
	if err != nil {
		return err
	}
	rawdb.WriteHeadBlockHash(db, newHeadHash)
	if writeErr := rawdb.WriteHeadHeaderHash(db, newHeadHash); writeErr != nil {
		return writeErr
	}
	if err = stages.SaveStageProgress(db, stages.Headers, newHead); err != nil {
		return err
	}
	stageBuilders := createStageBuilders([]*types.Block{}, newHead, checkRoot)
	stagedSync := New(stageBuilders, []int{0, 1, 2, 3, 5, 4, 6, 7, 8, 9, 10, 11}, OptionalParameters{})
	syncState, err1 := stagedSync.Prepare(
		nil,
		config,
		engine,
		vmConfig,
		db,
		db,
		"1",
		ethdb.DefaultStorageMode,
		"",
		8*1024,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
		StageSendersCfg(config),
	)
	if err1 != nil {
		return err1
	}
	if err = syncState.UnwindTo(newHead, db); err != nil {
		return err
	}
	if err = syncState.Run(db, db); err != nil {
		return err
	}
	return nil
}

func InsertHeadersInStages(db ethdb.Database, config *params.ChainConfig, engine consensus.Engine, headers []*types.Header) (bool, bool, uint64, error) {
	blockNum := headers[len(headers)-1].Number.Uint64()
	if err := VerifyHeaders(db, headers, config, engine, 1); err != nil {
		return false, false, 0, err
	}
	newCanonical, reorg, forkblocknumber, err := InsertHeaderChain("logPrefix", db, headers)
	if err != nil {
		return false, false, 0, err
	}
	if !newCanonical {
		return false, false, 0, nil
	}
	if err = stages.SaveStageProgress(db, stages.Headers, blockNum); err != nil {
		return false, false, 0, err
	}
	return newCanonical, reorg, forkblocknumber, nil
}

func InsertBlocksInStages(db ethdb.Database, storageMode ethdb.StorageMode, config *params.ChainConfig, vmConfig *vm.Config, engine consensus.Engine, blocks []*types.Block, checkRoot bool) (bool, error) {
	if len(blocks) == 0 {
		return false, nil
	}
	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	// Header verification happens outside of the transaction
	if err := VerifyHeaders(db, headers, config, engine, 1); err != nil {
		return false, err
	}
	var tx ethdb.DbWithPendingMutations
	var useExternalTx bool
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = db.(ethdb.DbWithPendingMutations)
		useExternalTx = true
	} else {
		var err error
		tx, err = db.Begin(context.Background(), ethdb.RW)
		if err != nil {
			return false, nil
		}
		defer tx.Rollback()
	}
	newCanonical, reorg, forkblocknumber, err := InsertHeaderChain("Headers", tx, headers)
	if err != nil {
		return false, err
	}
	if !newCanonical {
		if _, err = core.InsertBodyChain("Bodies", context.Background(), tx, blocks, false /* newCanonical */); err != nil {
			return false, fmt.Errorf("inserting block bodies chain for non-canonical chain")
		}
		if !useExternalTx {
			if err1 := tx.Commit(); err1 != nil {
				return false, fmt.Errorf("committing transaction after importing blocks: %v", err1)
			}
		}
		return false, nil // No change of the chain
	}
	blockNum := blocks[len(blocks)-1].Number().Uint64()
	if err = stages.SaveStageProgress(tx, stages.Headers, blockNum); err != nil {
		return false, err
	}
	stageBuilders := createStageBuilders(blocks, blockNum, checkRoot)
	cc := &core.TinyChainContext{}
	cc.SetDB(nil)
	cc.SetEngine(engine)
	stagedSync := New(stageBuilders, []int{0, 1, 2, 3, 5, 4, 6, 7, 8, 9, 10, 11}, OptionalParameters{})
	syncState, err2 := stagedSync.Prepare(
		nil,
		config,
		engine,
		vmConfig,
		tx,
		tx,
		"1",
		storageMode,
		"",
		8*1024,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
		StageSendersCfg(config),
	)
	if err2 != nil {
		return false, err2
	}

	if reorg {
		if err = syncState.UnwindTo(forkblocknumber, tx); err != nil {
			return false, err
		}
	}
	if err = syncState.Run(tx, tx); err != nil {
		return false, err
	}
	if !useExternalTx {
		if err1 := tx.Commit(); err1 != nil {
			return false, fmt.Errorf("committing transaction after importing blocks: %v", err1)
		}
	}
	return true, nil
}

func InsertBlockInStages(db ethdb.Database, config *params.ChainConfig, vmConfig *vm.Config, engine consensus.Engine, block *types.Block, checkRoot bool) (bool, error) {
	return InsertBlocksInStages(db, ethdb.DefaultStorageMode, config, vmConfig, engine, []*types.Block{block}, checkRoot)
}

// UpdateMetrics - need update metrics manually because current "metrics" package doesn't support labels
// need to fix it in future
func UpdateMetrics(db ethdb.Getter) error {
	var progress uint64
	var err error
	progress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	stageHeadersGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}
	stageBodiesGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		return err
	}
	stageExecutionGauge.Update(int64(progress))
	return nil
}
