package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
)

func createStageBuilders(blocks []*types.Block, blockNum uint64, checkRoot bool) StageBuilders {
	return []StageBuilder{
		{
			ID: stages.BlockHashes,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.BlockHashes,
					Description: "Write block hashes",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						blockHashCfg := StageBlockHashesCfg(world.DB.RwKV(), world.TmpDir)
						return SpawnBlockHashStage(s, tx, blockHashCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return u.Done(tx)
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
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						if _, err := core.InsertBodyChain("logPrefix", context.TODO(), ethdb.WrapIntoTxDB(tx), blocks, true /* newCanonical */); err != nil {
							return err
						}
						return s.DoneAndUpdate(tx, blockNum)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return u.Done(tx)
					},
				}
			},
		},
		{
			ID: stages.Senders,
			Build: func(world StageParameters) *Stage {
				sendersCfg := StageSendersCfg(world.DB.RwKV(), world.ChainConfig, world.TmpDir)
				return &Stage{
					ID:          stages.Senders,
					Description: "Recover senders from tx signatures",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnRecoverSendersStage(sendersCfg, s, u, tx, 0, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindSendersStage(u, s, tx, sendersCfg)
					},
				}
			},
		},
		{
			ID: stages.Execution,
			Build: func(world StageParameters) *Stage {
				execCfg := StageExecuteBlocksCfg(
					world.DB.RwKV(),
					world.storageMode.Receipts,
					world.storageMode.CallTraces,
					world.storageMode.TEVM,
					0,
					world.BatchSize,
					world.stateReaderBuilder,
					world.stateWriterBuilder,
					nil,
					world.ChainConfig,
					world.Engine,
					world.vmConfig,
					world.TmpDir,
				)
				return &Stage{
					ID:          stages.Execution,
					Description: "Execute blocks w/o hash checks",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnExecuteBlocksStage(s, u, tx, 0, world.QuitCh, execCfg, nil)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindExecutionStage(u, s, tx, world.QuitCh, execCfg, nil)
					},
				}
			},
		},
		{
			ID: stages.HashState,
			Build: func(world StageParameters) *Stage {
				hashStateCfg := StageHashStateCfg(world.DB.RwKV(), world.TmpDir)
				return &Stage{
					ID:          stages.HashState,
					Description: "Hash the key in the state",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnHashStateStage(s, tx, hashStateCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindHashStateStage(u, s, tx, hashStateCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.IntermediateHashes,
			Build: func(world StageParameters) *Stage {
				stageTrieCfg := StageTrieCfg(world.DB.RwKV(), checkRoot, true, world.TmpDir)
				return &Stage{
					ID:          stages.IntermediateHashes,
					Description: "Generate intermediate hashes and computing state root",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						_, err := SpawnIntermediateHashesStage(s, u, tx, stageTrieCfg, world.QuitCh)
						return err
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindIntermediateHashesStage(u, s, tx, stageTrieCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.AccountHistoryIndex,
			Build: func(world StageParameters) *Stage {
				cfg := StageHistoryCfg(world.DB.RwKV(), world.TmpDir)
				return &Stage{
					ID:                  stages.AccountHistoryIndex,
					Description:         "Generate account history index",
					Disabled:            !world.storageMode.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnAccountHistoryIndex(s, tx, cfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindAccountHistoryIndex(u, s, tx, cfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.StorageHistoryIndex,
			Build: func(world StageParameters) *Stage {
				cfg := StageHistoryCfg(world.DB.RwKV(), world.TmpDir)
				return &Stage{
					ID:                  stages.StorageHistoryIndex,
					Description:         "Generate storage history index",
					Disabled:            !world.storageMode.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnStorageHistoryIndex(s, tx, cfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindStorageHistoryIndex(u, s, tx, cfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.LogIndex,
			Build: func(world StageParameters) *Stage {
				cfg := StageLogIndexCfg(world.DB.RwKV(), world.TmpDir)
				return &Stage{
					ID:                  stages.LogIndex,
					Description:         "Generate receipt logs index",
					Disabled:            !world.storageMode.Receipts,
					DisabledDescription: "Enable by adding `r` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnLogIndex(s, tx, cfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindLogIndex(u, s, tx, cfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.CallTraces,
			Build: func(world StageParameters) *Stage {
				callTracesCfg := StageCallTracesCfg(world.DB.RwKV(), 0, world.BatchSize, world.TmpDir, world.ChainConfig, world.Engine)

				return &Stage{
					ID:                  stages.CallTraces,
					Description:         "Generate call traces index",
					Disabled:            !world.storageMode.CallTraces,
					DisabledDescription: "Work In Progress",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnCallTraces(s, tx, world.QuitCh, callTracesCfg)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindCallTraces(u, s, tx, world.QuitCh, callTracesCfg)
					},
				}
			},
		},
		{
			ID: stages.TxLookup,
			Build: func(world StageParameters) *Stage {
				txLookupCfg := StageTxLookupCfg(world.DB.RwKV(), world.TmpDir)
				return &Stage{
					ID:                  stages.TxLookup,
					Description:         "Generate tx lookup index",
					Disabled:            !world.storageMode.TxIndex,
					DisabledDescription: "Enable by adding `t` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
						return SpawnTxLookup(s, tx, txLookupCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindTxLookup(u, s, tx, txLookupCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.Finish,
			Build: func(world StageParameters) *Stage {
				finishCfg := StageFinishCfg(world.DB.RwKV(), world.TmpDir)
				return &Stage{
					ID:          stages.Finish,
					Description: "Final: update current block for the RPC API",
					ExecFunc: func(s *StageState, _ Unwinder, tx ethdb.RwTx) error {
						return FinishForward(s, tx, finishCfg, world.btClient, world.SnapshotBuilder)
					},
					UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
						return UnwindFinish(u, s, tx, finishCfg)
					},
				}
			},
		},
	}
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
	var tx ethdb.RwTx
	if hasTx, ok := db.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		tx = hasTx.Tx().(ethdb.RwTx)
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = db.RwKV().BeginRw(context.Background())
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
	}
	newCanonical, reorg, forkblocknumber, err := InsertHeaderChain("Headers", ethdb.WrapIntoTxDB(tx), headers, 0)
	if err != nil {
		return false, err
	}
	if !newCanonical {
		if _, err = core.InsertBodyChain("Bodies", context.Background(), ethdb.WrapIntoTxDB(tx), blocks, false /* newCanonical */); err != nil {
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
	stagedSync := New(stageBuilders, []int{0, 1, 2, 3, 5, 4, 6, 7, 8, 9, 10, 11}, OptionalParameters{})
	syncState, err2 := stagedSync.Prepare(
		nil,
		config,
		engine,
		vmConfig,
		db,
		tx,
		"1",
		storageMode,
		"",
		8*1024,
		nil,
		nil,
		nil,
		false,
		nil,
		nil,
	)
	if err2 != nil {
		return false, err2
	}
	syncState.DisableStages(stages.Finish)
	if reorg {
		if err = syncState.UnwindTo(forkblocknumber, tx, common.Hash{}); err != nil {
			return false, err
		}
	}

	if err = syncState.Run(db.RwKV(), tx); err != nil {
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
func UpdateMetrics(tx ethdb.Tx) error {
	var progress uint64
	var err error
	progress, err = stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	stageHeadersGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return err
	}
	stageBodiesGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	stageExecutionGauge.Update(int64(progress))

	progress, err = stages.GetStageProgress(tx, stages.Translation)
	if err != nil {
		return err
	}
	stageTranspileGauge.Update(int64(progress))
	return nil
}
