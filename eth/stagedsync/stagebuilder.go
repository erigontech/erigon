package stagedsync

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"strings"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
)

type ChainEventNotifier interface {
	OnNewHeader(*types.Header)
	OnNewPendingLogs(types.Logs)
	OnNewPendingBlock(*types.Block)
	OnNewPendingTxs([]types.Transaction)
}

// StageParameters contains the stage that stages receives at runtime when initializes.
// Then the stage can use it to receive different useful functions.
type StageParameters struct {
	d           DownloaderGlue
	ChainConfig *params.ChainConfig
	vmConfig    *vm.Config
	Engine      consensus.Engine
	DB          ethdb.Database
	// TX is a current transaction that staged sync runs in. It contains all the latest changes that DB has.
	// It can be used for both reading and writing.
	TX          ethdb.Database
	pid         string
	BatchSize   datasize.ByteSize // Batch size for the execution stage
	storageMode ethdb.StorageMode
	TmpDir      string
	// QuitCh is a channel that is closed. This channel is useful to listen to when
	// the stage can take significant time and gracefully shutdown at Ctrl+C.
	QuitCh                <-chan struct{}
	headersFetchers       []func() error
	txPool                *core.TxPool
	prefetchedBlocks      *bodydownload.PrefetchedBlocks
	stateReaderBuilder    StateReaderBuilder
	stateWriterBuilder    StateWriterBuilder
	notifier              ChainEventNotifier
	silkwormExecutionFunc unsafe.Pointer
	InitialCycle          bool
	mining                *MiningCfg
	senders SendersCfg

	snapshotsDir		 string
	btClient			 *snapshotsync.Client
	SnapshotBuilder		 *snapshotsync.SnapshotMigrator
}

type MiningCfg struct {
	// configs
	params.MiningConfig

	// noempty is the flag used to control whether the feature of pre-seal empty
	// block is enabled. The default value is false(pre-seal is enabled by default).
	// But in some special scenario the consensus engine will seal blocks instantaneously,
	// in this case this feature will add all empty blocks into canonical chain
	// non-stop and no real transaction will be included.
	noempty bool

	resultCh   chan<- *types.Block
	sealCancel <-chan struct{}

	// runtime dat
	Block *miningBlock
}

func StageMiningCfg(cfg params.MiningConfig, noempty bool, resultCh chan<- *types.Block, sealCancel <-chan struct{}) *MiningCfg {
	return &MiningCfg{MiningConfig: cfg, noempty: noempty, Block: &miningBlock{}, resultCh: resultCh, sealCancel: sealCancel}

}

// StageBuilder represent an object to create a single stage for staged sync
type StageBuilder struct {
	// ID is the stage identifier. Should be unique. It is recommended to prefix it with reverse domain `com.example.my-stage` to avoid conflicts.
	ID stages.SyncStage
	// Build is a factory function that initializes the sync stage based on the `StageParameters` provided.
	Build func(StageParameters) *Stage
}

// StageBuilders represents an ordered list of builders to build different stages. It also contains helper methods to change the list of stages.
type StageBuilders []StageBuilder

// MustReplace finds a stage with a specific ID and then sets the new one instead of that.
// Chainable but panics if it can't find stage to replace.
func (bb StageBuilders) MustReplace(id stages.SyncStage, newBuilder StageBuilder) StageBuilders {
	result := make([]StageBuilder, len(bb))

	found := false

	for i, originalBuilder := range bb {
		if strings.EqualFold(string(originalBuilder.ID), string(id)) {
			found = true
			result[i] = newBuilder
		} else {
			result[i] = originalBuilder
		}
	}

	if !found {
		panic(fmt.Sprintf("StageBuilders#Replace can't find the stage with id %s", string(id)))
	}

	return result
}

// Build creates sync states out of builders
func (bb StageBuilders) Build(world StageParameters) []*Stage {
	stages := make([]*Stage, len(bb))
	for i, builder := range bb {
		stages[i] = builder.Build(world)
	}
	return stages
}

// DefaultStages contains the list of default stage builders that are used by turbo-geth.
func DefaultStages() StageBuilders {
	return []StageBuilder{
		{
			ID: stages.Headers,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.Headers,
					Description: "Download headers",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnHeaderDownloadStage(s, u, world.d, world.headersFetchers)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return u.Done(world.DB)
					},
				}
			},
		},
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
						return spawnBodyDownloadStage(s, u, world.d, world.pid, world.prefetchedBlocks)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return unwindBodyDownloadStage(u, world.DB)
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
				return &Stage{
					ID:          stages.HashState,
					Description: "Hash the key in the state",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnHashStateStage(s, world.TX, StageHashStateCfg(world.TmpDir), world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindHashStateStage(u, s, world.TX, StageHashStateCfg(world.TmpDir), world.QuitCh)
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
						_, err := SpawnIntermediateHashesStage(s, world.TX, StageTrieCfg(true, true, world.TmpDir), world.QuitCh)
						return err
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindIntermediateHashesStage(u, s, world.TX, StageTrieCfg(true, true, world.TmpDir), world.QuitCh)
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
				logIndexCfg := StageLogIndexCfg(world.TmpDir)
				return &Stage{
					ID:                  stages.LogIndex,
					Description:         "Generate receipt logs index",
					Disabled:            !world.storageMode.Receipts,
					DisabledDescription: "Enable by adding `r` to --storage-mode",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnLogIndex(s, world.TX, logIndexCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindLogIndex(u, s, world.TX, logIndexCfg, world.QuitCh)
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
			ID: stages.TxPool,
			Build: func(world StageParameters) *Stage {
				txPoolCfg := StageTxPoolCfg(world.txPool)
				return &Stage{
					ID:          stages.TxPool,
					Description: "Update transaction pool",
					ExecFunc: func(s *StageState, _ Unwinder) error {
						return SpawnTxPool(s, world.TX, txPoolCfg, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindTxPool(u, s, world.TX, txPoolCfg, world.QuitCh)
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
						if executionAt, err = s.ExecutionAt(world.DB); err != nil {
							return err
						}
						logPrefix := s.state.LogPrefix()
						log.Info(fmt.Sprintf("[%s] Update current block for the RPC API", logPrefix), "to", executionAt)

						err = NotifyNewHeaders(s.BlockNumber+1, executionAt, world.notifier, world.DB)
						if err != nil {
							return err
						}
						err = MigrateSnapshot(s.BlockNumber+1, world.TX, world.DB, world.btClient, world.SnapshotBuilder)
						if err != nil {
							return err
						}
						return s.DoneAndUpdate(world.DB, executionAt)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						var executionAt uint64
						var err error
						if executionAt, err = s.ExecutionAt(world.DB); err != nil {
							return err
						}
						return s.DoneAndUpdate(world.DB, executionAt)
					},
				}
			},
		},
	}
}

func MiningStages() StageBuilders {
	return []StageBuilder{
		{
			ID: stages.MiningCreateBlock,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.MiningCreateBlock,
					Description: "Mining: construct new block from tx pool",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnMiningCreateBlockStage(s, world.TX,
							world.mining.Block,
							world.ChainConfig,
							world.Engine,
							world.mining.ExtraData,
							world.mining.GasFloor,
							world.mining.GasCeil,
							world.mining.Etherbase,
							world.txPool,
							world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error { return nil },
				}
			},
		},
		{
			ID: stages.MiningExecution,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.MiningExecution,
					Description: "Mining: construct new block from tx pool",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnMiningExecStage(s, world.TX,
							world.mining.Block,
							world.ChainConfig,
							world.vmConfig,
							world.Engine,
							world.mining.Block.LocalTxs,
							world.mining.Block.RemoteTxs,
							world.mining.Etherbase,
							world.mining.noempty,
							world.notifier,
							world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error { return nil },
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
						return SpawnHashStateStage(s, world.TX, StageHashStateCfg(world.TmpDir), world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error { return nil },
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
						stateRoot, err := SpawnIntermediateHashesStage(s, world.TX, StageTrieCfg(false, true, world.TmpDir), world.QuitCh)
						if err != nil {
							return err
						}
						world.mining.Block.Header.Root = stateRoot
						return nil
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error { return nil },
				}
			},
		},
		{
			ID: stages.MiningFinish,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.MiningFinish,
					Description: "Mining: create and propagate valid block",
					ExecFunc: func(s *StageState, u Unwinder) error {
						return SpawnMiningFinishStage(s, world.TX, world.mining.Block, world.Engine, world.ChainConfig, world.mining.resultCh, world.mining.sealCancel, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error { return nil },
				}
			},
		},
	}
}

// UnwindOrder represents the order in which the stages needs to be unwound.
// Currently it is using indexes of stages, 0-based.
// The unwind order is important and not always just stages going backwards.
// Let's say, there is tx pool (state 10) can be unwound only after execution
// is fully unwound (stages 9...3).
type UnwindOrder []int

// DefaultUnwindOrder contains the default unwind order for `DefaultStages()`.
// Just adding stages that don't do unwinding, don't require altering the default order.
func DefaultUnwindOrder() UnwindOrder {
	return []int{
		0, 1, 2,
		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
		// also tx pool is before senders because senders unwind is inside cycle transaction
		12,
		3, 4,
		// Unwinding of IHashes needs to happen after unwinding HashState
		6, 5,
		7, 8, 9, 10, 11,
	}
}

func WithSnapshotsStages() StageBuilders {
	defaultStages :=DefaultStages()
	final:= defaultStages[len(defaultStages)-1]
	defaultStages = defaultStages[:1]
	defaultStages = append(defaultStages, StageBuilder{
		ID: stages.CreateHeadersSnapshot,
		Build: func(world StageParameters) *Stage {
			return &Stage{
				ID:          stages.CreateHeadersSnapshot,
				Description: "Create headers snapshot",
				ExecFunc: func(s *StageState, u Unwinder) error {
					return SpawnHeadersSnapshotGenerationStage(s, world.DB, world.SnapshotBuilder, world.snapshotsDir,  world.btClient, world.QuitCh)
				},
				UnwindFunc: func(u *UnwindState, s *StageState) error {
					return u.Done(world.DB)
				},
			}
		},
	})
	defaultStages = append(defaultStages, final)
	return defaultStages

	blockHashesStageIndex:=-1
	sendersStageIndex:=-1
	hashedStateStageIndex:=-1
	for i:=range defaultStages {
		if defaultStages[i].ID==stages.Bodies {
			blockHashesStageIndex = i
		}
		if defaultStages[i].ID==stages.Senders {
			sendersStageIndex = i
		}
		if defaultStages[i].ID==stages.HashState {
			hashedStateStageIndex = i
		}
	}
	if blockHashesStageIndex < 0 || sendersStageIndex < 0 ||  hashedStateStageIndex < 0{
		log.Error("Unrecognized block hashes stage")
		return DefaultStages()
	}

	stagesWithSnapshots:=make(StageBuilders, 0, len(defaultStages)+1)
	stagesWithSnapshots = append(stagesWithSnapshots, defaultStages[:blockHashesStageIndex]...)
	stagesWithSnapshots = append(stagesWithSnapshots, StageBuilder{
		ID: stages.CreateHeadersSnapshot,
		Build: func(world StageParameters) *Stage {
			return &Stage{
				ID:          stages.CreateHeadersSnapshot,
				Description: "Create headers snapshot",
				ExecFunc: func(s *StageState, u Unwinder) error {
					return SpawnHeadersSnapshotGenerationStage(s, world.DB, world.SnapshotBuilder, world.snapshotsDir,  world.btClient, world.QuitCh)
				},
				UnwindFunc: func(u *UnwindState, s *StageState) error {
					return u.Done(world.DB)
				},
			}
		},
	})
	stagesWithSnapshots = append(stagesWithSnapshots, defaultStages[blockHashesStageIndex:sendersStageIndex]...)
	stagesWithSnapshots = append(stagesWithSnapshots, StageBuilder{
		ID: stages.CreateBodiesSnapshot,
		Build: func(world StageParameters) *Stage {
			return &Stage{
				ID:          stages.CreateBodiesSnapshot,
				Description: "Create bodies snapshot",
				ExecFunc: func(s *StageState, u Unwinder) error {
					return SpawnBodiesSnapshotGenerationStage(s, world.DB,world.snapshotsDir,  world.btClient, world.QuitCh)
				},
				UnwindFunc: func(u *UnwindState, s *StageState) error {
					return u.Done(world.DB)
				},
			}
		},

	})
	stagesWithSnapshots = append(stagesWithSnapshots, defaultStages[sendersStageIndex:hashedStateStageIndex]...)
	stagesWithSnapshots = append(stagesWithSnapshots, StageBuilder{
		ID: stages.CreateStateSnapshot,
		Build: func(world StageParameters) *Stage {
			return &Stage{
				ID:          stages.CreateStateSnapshot,
				Description: "Create state snapshot",
				ExecFunc: func(s *StageState, u Unwinder) error {
					return SpawnStateSnapshotGenerationStage(s, world.DB,world.snapshotsDir,  world.btClient, world.QuitCh)
				},
				UnwindFunc: func(u *UnwindState, s *StageState) error {
					return u.Done(world.DB)
				},
			}
		},

	})
	stagesWithSnapshots = append(stagesWithSnapshots, defaultStages[hashedStateStageIndex:]...)
	return stagesWithSnapshots
}


//func DefaultUnwindOrder() UnwindOrder {
//	return []int{
//		0, 1, 2,
//		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
//		// also tx pool is before senders because senders unwind is inside cycle transaction
//		12, //tx lookup
//		3, 4, //bodies, senders
//		// Unwinding of IHashes needs to happen after unwinding HashState
//		6, 5, // hashed state, execution
//		7, 8, 9, 10, 11,
//	}
//}

func UnwindOrderWithSnapshots() UnwindOrder {
	return []int{
		0, 1, 2,
	}
	//todo fix unwind order
	return []int{
		0, 1, 2, 3,
		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
		// also tx pool is before senders because senders unwind is inside cycle transaction
		13,
		4, 5,
		// Unwinding of IHashes needs to happen after unwinding HashState
		6, 7,
		8, 9, 10, 11, 13, 14,
	}
}





func MiningUnwindOrder() UnwindOrder {
	return []int{0, 1, 2, 3, 4}
}
