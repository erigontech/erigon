package stagedsync

import (
	"time"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto/secp256k1"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

// StageParameters contains the stage that stages receives at runtime when initializes.
// Then the stage can use it to receive different useful functions.
type StageParameters struct {
	d            DownloaderGlue
	chainConfig  *params.ChainConfig
	chainContext core.ChainContext
	vmConfig     *vm.Config
	db           ethdb.Database
	// TX is a current transaction that staged sync runs in. It contains all the latest changes that DB has.
	// It can be used for both reading and writing.
	TX          ethdb.Database
	pid         string
	hdd         bool
	storageMode ethdb.StorageMode
	datadir     string
	// QuitCh is a channel that is closed. This channel is useful to listen to when
	// the stage can take significant time and gracefully shutdown at Ctrl+C.
	QuitCh           <-chan struct{}
	headersFetchers  []func() error
	txPool           *core.TxPool
	poolStart        func() error
	changeSetHook    ChangeSetHook
	prefetchedBlocks *PrefetchedBlocks
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
						return u.Done(world.db)
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
						return SpawnBlockHashStage(s, world.db, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return u.Done(world.db)
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
						return unwindBodyDownloadStage(u, world.db)
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
							StartTrace:      false,
							Prof:            false,
							NumOfGoroutines: n,
							ReadChLen:       4,
							Now:             time.Now(),
						}
						return SpawnRecoverSendersStage(cfg, s, world.TX, world.chainConfig, 0, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindSendersStage(u, world.TX)
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
						return SpawnExecuteBlocksStage(s, world.TX, world.chainConfig, world.chainContext, world.vmConfig, 0 /* limit (meaning no limit) */, world.QuitCh, world.storageMode.Receipts, world.hdd, world.changeSetHook)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindExecutionStage(u, s, world.TX, world.storageMode.Receipts)
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
						return SpawnHashStateStage(s, world.TX, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindHashStateStage(u, s, world.TX, world.datadir, world.QuitCh)
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
						return SpawnIntermediateHashesStage(s, world.TX, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindIntermediateHashesStage(u, s, world.TX, world.datadir, world.QuitCh)
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
						return SpawnAccountHistoryIndex(s, world.TX, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindAccountHistoryIndex(u, world.TX, world.QuitCh)
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
						return SpawnStorageHistoryIndex(s, world.TX, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindStorageHistoryIndex(u, world.TX, world.QuitCh)
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
						return SpawnTxLookup(s, world.TX, world.datadir, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return UnwindTxLookup(u, s, world.TX, world.datadir, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.TxPool,
			Build: func(world StageParameters) *Stage {
				return &Stage{
					ID:          stages.TxPool,
					Description: "Update transaction pool",
					ExecFunc: func(s *StageState, _ Unwinder) error {
						return spawnTxPool(s, world.TX, world.txPool, world.poolStart, world.QuitCh)
					},
					UnwindFunc: func(u *UnwindState, s *StageState) error {
						return unwindTxPool(u, s, world.TX, world.txPool, world.QuitCh)
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
		10,
		3, 4,
		// Unwinding of IHashes needs to happen after unwinding HashState
		6, 5,
		7, 8, 9,
	}
}
