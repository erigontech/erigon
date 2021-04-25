package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

const (
	logInterval = 30 * time.Second
)

func NewStagedSync(
	ctx context.Context,
	sm ethdb.StorageMode,
	headers stagedsync.HeadersCfg,
	bodies stagedsync.BodiesCfg,
	senders stagedsync.SendersCfg,
	exec stagedsync.ExecuteBlockCfg,
	hashState stagedsync.HashStateCfg,
	trieCfg stagedsync.TrieCfg,
	history stagedsync.HistoryCfg,
	logIndex stagedsync.LogIndexCfg,
) *stagedsync.StagedSync {

	return stagedsync.New(
		ReplacementStages(ctx, sm, headers, bodies, senders, exec, hashState, trieCfg, history, logIndex),
		ReplacementUnwindOrder(),
		stagedsync.OptionalParameters{},
	)
}

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.Database,
	sync *stagedsync.StagedSync,
	hd *headerdownload.HeaderDownload,
	chainConfig *params.ChainConfig,
) error {
	initialCycle := true
	stopped := false
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	for !stopped {
		// Estimate the current top height seen from the peer
		height := hd.TopSeenHeight()

		origin, err := stages.GetStageProgress(db, stages.Headers)
		if err != nil {
			return err
		}
		hashStateStageProgress, err1 := stages.GetStageProgress(db, stages.Bodies) // TODO: shift this when more stages are added
		if err1 != nil {
			return err1
		}

		canRunCycleInOneTransaction := !initialCycle && height-origin < 1024 && height-hashStateStageProgress < 1024

		var writeDB ethdb.Database // on this variable will run sync cycle.

		// create empty TxDb object, it's not usable before .Begin() call which will use this object
		// It allows inject tx object to stages now, define rollback now,
		// but call .Begin() after hearer/body download stages
		var tx ethdb.DbWithPendingMutations
		if canRunCycleInOneTransaction {
			tx, err = db.Begin(context.Background(), ethdb.RW)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			writeDB = tx
		} else {
			writeDB = db
		}

		st, err1 := sync.Prepare(nil, chainConfig, nil, &vm.Config{}, db, writeDB, "downloader", ethdb.DefaultStorageMode, ".", 512*datasize.MB, make(chan struct{}), nil, nil, func() error { return nil }, initialCycle, nil, stagedsync.StageSendersCfg(chainConfig))
		if err1 != nil {
			return fmt.Errorf("prepare staged sync: %w", err1)
		}

		err = st.Run(db, writeDB)
		if err != nil {
			return err
		}
		if canRunCycleInOneTransaction {
			commitStart := time.Now()
			errTx := tx.Commit()
			if errTx != nil {
				return errTx
			}
			log.Info("Commit cycle", "in", time.Since(commitStart))
		}
		initialCycle = false
		select {
		case <-ctx.Done():
			stopped = true
		default:
		}
	}
	return nil
}

func ReplacementStages(ctx context.Context,
	sm ethdb.StorageMode,
	headers stagedsync.HeadersCfg,
	bodies stagedsync.BodiesCfg,
	senders stagedsync.SendersCfg,
	exec stagedsync.ExecuteBlockCfg,
	hashState stagedsync.HashStateCfg,
	trieCfg stagedsync.TrieCfg,
	history stagedsync.HistoryCfg,
	logIndex stagedsync.LogIndexCfg,
) stagedsync.StageBuilders {
	return []stagedsync.StageBuilder{
		{
			ID: stages.Headers,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.Headers,
					Description: "Download headers",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.HeadersForward(s, u, ctx, world.TX, headers, world.InitialCycle)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.HeadersUnwind(u, s, world.TX)
					},
				}
			},
		},
		{
			ID: stages.BlockHashes,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.BlockHashes,
					Description: "Write block hashes",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnBlockHashStage(s, world.TX, world.TmpDir, world.QuitCh)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return u.Done(world.DB)
					},
				}
			},
		},
		{
			ID: stages.Bodies,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.Bodies,
					Description: "Download block bodies",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.BodiesForward(s, ctx, world.TX, bodies)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return u.Done(world.DB)
					},
				}
			},
		},
		{
			ID: stages.Senders,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.Senders,
					Description: "Recover senders from tx signatures",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnRecoverSendersStage(senders, s, world.TX, 0, world.TmpDir, ctx.Done())
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindSendersStage(u, s, world.TX)
					},
				}
			},
		},
		{
			ID: stages.Execution,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.Execution,
					Description: "Execute blocks w/o hash checks",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnExecuteBlocksStage(s, world.TX, 0, ctx.Done(), exec)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindExecutionStage(u, s, world.TX, ctx.Done(), exec)
					},
				}
			},
		},
		{
			ID: stages.HashState,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.HashState,
					Description: "Hash the key in the state",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnHashStateStage(s, world.TX, hashState, world.QuitCh)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindHashStateStage(u, s, world.TX, hashState, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.IntermediateHashes,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.IntermediateHashes,
					Description: "Generate intermediate hashes and computing state root",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						_, err := stagedsync.SpawnIntermediateHashesStage(s, world.TX, trieCfg, world.QuitCh)
						return err
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindIntermediateHashesStage(u, s, world.TX, trieCfg, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.AccountHistoryIndex,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:                  stages.AccountHistoryIndex,
					Description:         "Generate account history index",
					Disabled:            !sm.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnAccountHistoryIndex(s, world.TX, history, world.QuitCh)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindAccountHistoryIndex(u, s, world.TX, history, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.StorageHistoryIndex,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:                  stages.StorageHistoryIndex,
					Description:         "Generate storage history index",
					Disabled:            !sm.History,
					DisabledDescription: "Enable by adding `h` to --storage-mode",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnStorageHistoryIndex(s, world.TX, history, world.QuitCh)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindStorageHistoryIndex(u, s, world.TX, history, world.QuitCh)
					},
				}
			},
		},
		{
			ID: stages.LogIndex,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:                  stages.LogIndex,
					Description:         "Generate receipt logs index",
					Disabled:            !sm.Receipts,
					DisabledDescription: "Enable by adding `r` to --storage-mode",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.SpawnLogIndex(s, world.TX, logIndex, world.QuitCh)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return stagedsync.UnwindLogIndex(u, s, world.TX, logIndex, world.QuitCh)
					},
				}
			},
		},
	}
}

func ReplacementUnwindOrder() stagedsync.UnwindOrder {
	return []int{
		0, 1, 2, // download headers/bodies
		// Unwinding of tx pool (reinjecting transactions into the pool needs to happen after unwinding execution)
		// also tx pool is before senders because senders unwind is inside cycle transaction
		//12,
		3, 4, // senders, exec
		6, 5, // Unwinding of IHashes needs to happen after unwinding HashState
		7, 8, // history
		9, // log index
		// 10, 11,
	}
}
