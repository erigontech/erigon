package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
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
						const batchSize = 10000
						const blockSize = 4096
						n := secp256k1.NumOfContexts() // we can only be as parallels as our crypto library supports

						cfg := Stage3Config{
							BatchSize:       batchSize,
							BlockSize:       blockSize,
							BufferSize:      (blockSize * 10 / 20) * 10000, // 20*4096
							NumOfGoroutines: n,
							ReadChLen:       4,
							Now:             time.Now(),
						}
						return SpawnRecoverSendersStage(cfg, s, world.TX, world.ChainConfig, 0, world.TmpDir, world.QuitCh)
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
				return &Stage{
					ID:          stages.Execution,
					Description: "Execute blocks w/o hash checks",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnExecuteBlocksStage(s, world.TX,
							world.ChainConfig, world.chainContext, world.vmConfig,
							world.QuitCh,
							ExecuteBlockStageParams{
								WriteReceipts:         world.storageMode.Receipts,
								Cache:                 world.cache,
								BatchSize:             world.BatchSize,
								ChangeSetHook:         world.changeSetHook,
								ReaderBuilder:         world.stateReaderBuilder,
								WriterBuilder:         world.stateWriterBuilder,
								SilkwormExecutionFunc: world.silkwormExecutionFunc,
							})
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindExecutionStage(u, s, world.TX, world.QuitCh, ExecuteBlockStageParams{
							WriteReceipts:         world.storageMode.Receipts,
							Cache:                 world.cache,
							BatchSize:             world.BatchSize,
							ChangeSetHook:         world.changeSetHook,
							ReaderBuilder:         world.stateReaderBuilder,
							WriterBuilder:         world.stateWriterBuilder,
							SilkwormExecutionFunc: world.silkwormExecutionFunc,
						})
					},
				}
			},
		},
		{
			ID: stages.HashState,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.HashState,
					Description: "Hash the key in the state",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnHashStateStage(s, world.TX, world.cache, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindHashStateStage(u, s, world.TX, world.cache, world.TmpDir, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.IntermediateHashes,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.IntermediateHashes,
					Description: "Generate intermediate hashes and computing state root",
					ExecFunc: func(s *StageState, u Unwinder) error {
						/*
							var a accounts.Account
							c := world.TX.(ethdb.HasTx).Tx().Cursor(dbutils.PlainStateBucket)
							for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
								if err != nil {
									return err
								}
								if len(k) != 20 {
									fmt.Printf("%x => %x\n", k, v)
								} else {
									if err1 := a.DecodeForStorage(v); err1 != nil {
										return err1
									}
									fmt.Printf("%x => bal: %d nonce: %d codehash: %x, inc: %d\n", k, a.Balance.ToBig(), a.Nonce, a.CodeHash, a.Incarnation)
								}
							}
							c.Close()
						*/
						/*
							c = world.TX.(ethdb.HasTx).Tx().Cursor(dbutils.CurrentStateBucket)
							for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
								if err != nil {
									return err
								}
								if len(k) != 32 {
									fmt.Printf("%x => %x\n", k, v)
								} else {
									if err1 := a.DecodeForStorage(v); err1 != nil {
										return err1
									}
									fmt.Printf("%x => bal: %d nonce: %d codehash: %x, inc: %d\n", k, a.Balance.ToBig(), a.Nonce, a.CodeHash, a.Incarnation)
								}
							}
							c.Close()
						*/
						_, err := SpawnIntermediateHashesStage(s, world.TX, checkRoot /* checkRoot */, world.cache, world.TmpDir, world.QuitCh)
						return err
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindIntermediateHashesStage(u, s, world.TX, world.cache, world.TmpDir, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.AccountHistoryIndex,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:                  stages.AccountHistoryIndex,
					Description:         "Generate account history index",
					Disabled:            !world.storageMode.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnAccountHistoryIndex(s, world.TX, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindAccountHistoryIndex(u, s, world.TX, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.StorageHistoryIndex,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:                  stages.StorageHistoryIndex,
					Description:         "Generate storage history index",
					Disabled:            !world.storageMode.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnStorageHistoryIndex(s, world.TX, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindStorageHistoryIndex(u, s, world.TX, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.LogIndex,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:                  stages.LogIndex,
					Description:         "Generate receipt logs index",
					Disabled:            !world.storageMode.Receipts,
					DisabledDescription: "Enable by adding `r` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnLogIndex(s, world.TX, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindLogIndex(u, s, world.TX, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.CallTraces,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:                  stages.CallTraces,
					Description:         "Generate call traces index",
					Disabled:            !world.storageMode.CallTraces,
					DisabledDescription: "Work In Progress",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnCallTraces(s, world.TX, world.ChainConfig, world.chainContext, world.TmpDir, world.QuitCh,
							CallTracesStageParams{
								Cache:     world.cache,
								BatchSize: world.BatchSize,
							})
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindCallTraces(u, s, world.TX, world.ChainConfig, world.chainContext, world.QuitCh,
							CallTracesStageParams{
								Cache:     world.cache,
								BatchSize: world.BatchSize,
							})
					},
				}
			},
		},
		{
			ID: stages.TxLookup,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:                  stages.TxLookup,
					Description:         "Generate tx lookup index",
					Disabled:            !world.storageMode.TxIndex,
					DisabledDescription: "Enable by adding `t` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnTxLookup(s, world.TX, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindTxLookup(u, s, world.TX, world.TmpDir, world.QuitCh)
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
						var executionAt uint64
						var err error
						if executionAt, err = s.ExecutionAt(world.TX); err != nil {
							return err
						}
						logPrefix := s.state.LogPrefix()
						log.Info(fmt.Sprintf("[%s] Update current block for the RPC API", logPrefix), "to", executionAt)

						err = NotifyRpcDaemon(s.BlockNumber+1, executionAt, world.notifier, world.TX)
						if err != nil {
							return err
						}

						return s.DoneAndUpdate(world.TX, executionAt)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						var executionAt uint64
						var err error
						if executionAt, err = s.ExecutionAt(world.TX); err != nil {
							return err
						}
						return s.DoneAndUpdate(world.TX, executionAt)
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
	rawdb.WriteHeadHeaderHash(db, newHeadHash)
	if err = stages.SaveStageProgress(db, stages.Headers, newHead); err != nil {
		return err
	}
	stageBuilders := createStageBuilders([]*types.Block{}, newHead, checkRoot)
	cc := &core.TinyChainContext{}
	cc.SetDB(nil)
	cc.SetEngine(engine)
	stagedSync := New(stageBuilders, []int{0, 1, 2, 3, 5, 4, 6, 7, 8, 9, 10, 11}, OptionalParameters{})
	var cache *shards.StateCache // Turn off cache for now
	syncState, err1 := stagedSync.Prepare(
		nil,
		config,
		cc,
		vmConfig,
		db,
		db,
		"1",
		ethdb.DefaultStorageMode,
		"",
		cache,
		8*1024,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
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
	tx, err1 := db.Begin(context.Background(), ethdb.RW)
	if err1 != nil {
		return false, fmt.Errorf("starting transaction for importing the blocks: %v", err1)
	}
	defer tx.Rollback()
	newCanonical, reorg, forkblocknumber, err := InsertHeaderChain("Headers", tx, headers)
	if err != nil {
		return false, err
	}
	if !newCanonical {
		if _, err = core.InsertBodyChain("Bodies", context.Background(), tx, blocks, false /* newCanonical */); err != nil {
			return false, fmt.Errorf("inserting block bodies chain for non-canonical chain")
		}
		if err1 = tx.Commit(); err1 != nil {
			return false, fmt.Errorf("committing transaction after importing blocks: %v", err1)
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
	var cache *shards.StateCache // Turn off cache for now
	syncState, err2 := stagedSync.Prepare(
		nil,
		config,
		cc,
		vmConfig,
		tx,
		tx,
		"1",
		storageMode,
		"",
		cache,
		8*1024,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
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
	if err1 = tx.Commit(); err1 != nil {
		return false, fmt.Errorf("committing transaction after importing blocks: %v", err1)
	}
	return true, nil
}

func InsertBlockInStages(db ethdb.Database, config *params.ChainConfig, vmConfig *vm.Config, engine consensus.Engine, block *types.Block, checkRoot bool) (bool, error) {
	return InsertBlocksInStages(db, ethdb.DefaultStorageMode, config, vmConfig, engine, []*types.Block{block}, checkRoot)
}
