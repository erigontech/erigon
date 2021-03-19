package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

const (
	logInterval = 30 * time.Second
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.Database,
	hd *headerdownload.HeaderDownload,
	bd *bodydownload.BodyDownload,
	chainConfig *params.ChainConfig,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) []byte,
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte,
	penalise func(context.Context, []byte),
	updateHead func(context.Context, uint64, common.Hash, *uint256.Int),
	wakeUpChan chan struct{},
	timeout int,
) error {
	sync := stagedsync.New(
		ReplacementStages(ctx, hd, bd, headerReqSend, bodyReqSend, penalise, updateHead, wakeUpChan, timeout),
		ReplacementUnwindOrder(),
		stagedsync.OptionalParameters{},
	)
	initialCycle := true
	stopped := false
	for !stopped {
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()

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

		canRunCycleInOneTransaction := height-origin < 1024 && height-hashStateStageProgress < 1024

		var writeDB ethdb.Database // on this variable will run sync cycle.

		// create empty TxDb object, it's not usable before .Begin() call which will use this object
		// It allows inject tx object to stages now, define rollback now,
		// but call .Begin() after hearer/body download stages
		var tx ethdb.DbWithPendingMutations
		if canRunCycleInOneTransaction {
			tx = ethdb.NewTxDbWithoutTransaction(db, ethdb.RW)
			defer tx.Rollback()
			writeDB = tx
		} else {
			writeDB = db
		}

		cc := &core.TinyChainContext{}
		cc.SetDB(tx)
		//cc.SetEngine(d.blockchain.Engine())
		st, err1 := sync.Prepare(nil, chainConfig, cc, &vm.Config{}, db, writeDB, "downloader", ethdb.DefaultStorageMode, ".", nil, 512*1024*1024, make(chan struct{}), nil, nil, func() error { return nil }, nil, initialCycle)
		if err1 != nil {
			return fmt.Errorf("prepare staged sync: %w", err1)
		}
		if err != nil {
			return err
		}

		// begin tx at stage right after head/body download Or at first unwind stage
		// it's temporary solution
		st.BeforeStageRun(stages.Senders, func() error {
			if !canRunCycleInOneTransaction {
				return nil
			}

			var errTx error
			log.Debug("Begin tx")
			tx, errTx = tx.Begin(context.Background(), ethdb.RW)
			return errTx
		})
		st.OnBeforeUnwind(func(id stages.SyncStage) error {
			if !canRunCycleInOneTransaction {
				return nil
			}
			if st.IsAfter(id, stages.TxPool) {
				return nil
			}
			if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
				return nil
			}
			var errTx error
			log.Debug("Begin tx")
			tx, errTx = tx.Begin(context.Background(), ethdb.RW)
			return errTx
		})
		st.BeforeStageUnwind(stages.Bodies, func() error {
			if !canRunCycleInOneTransaction {
				return nil
			}
			if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() == nil {
				return nil
			}
			log.Info("Commit cycle")
			_, errCommit := tx.Commit()
			return errCommit
		})

		err = st.Run(db, writeDB)
		if err != nil {
			return err
		}
		if canRunCycleInOneTransaction {
			if hasTx, ok := tx.(ethdb.HasTx); !ok || hasTx.Tx() != nil {
				commitStart := time.Now()
				_, errTx := tx.Commit()
				if errTx == nil {
					log.Info("Commit cycle", "in", time.Since(commitStart))
				} else {
					return errTx
				}
			}
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
	hd *headerdownload.HeaderDownload,
	bd *bodydownload.BodyDownload,
	headerReqSend func(context.Context, *headerdownload.HeaderRequest) []byte,
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte,
	penalise func(context.Context, []byte),
	updateHead func(context.Context, uint64, common.Hash, *uint256.Int),
	wakeUpChan chan struct{},
	timeout int,
) stagedsync.StageBuilders {
	return []stagedsync.StageBuilder{
		{
			ID: stages.Headers,
			Build: func(world stagedsync.StageParameters) *stagedsync.Stage {
				return &stagedsync.Stage{
					ID:          stages.Headers,
					Description: "Download headers",
					ExecFunc: func(s *stagedsync.StageState, u stagedsync.Unwinder) error {
						return stagedsync.HeadersForward(s, u, ctx, world.TX, hd, world.ChainConfig, headerReqSend, world.InitialCycle, wakeUpChan)
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
						return stagedsync.BodiesForward(s, ctx, world.TX, bd, bodyReqSend, penalise, updateHead, wakeUpChan, timeout)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return u.Done(world.DB)
					},
				}
			},
		},
	}
}

func ReplacementUnwindOrder() stagedsync.UnwindOrder {
	return []int{
		0, 1, 2,
	}
}
