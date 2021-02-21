package stages

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.Database,
	hd *headerdownload.HeaderDownload,
	bd *bodydownload.BodyDownload,
	headerReqSend func(context.Context, []*headerdownload.HeaderRequest),
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte,
	penalise func(context.Context, []byte),
	updateHead func(context.Context, uint64, common.Hash, *big.Int),
	wakeUpChan chan struct{},
	timeout int,
) error {
	if _, _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock(), false /* history */, false /* overwrite */); err != nil {
		return fmt.Errorf("setup genesis block: %w", err)
	}
	sync := stagedsync.New(
		ReplacementStages(ctx, hd, bd, headerReqSend, bodyReqSend, penalise, updateHead, wakeUpChan, timeout),
		ReplacementUnwindOrder(),
		stagedsync.OptionalParameters{},
	)
	for {
		var ready bool
		var height uint64
		// Keep requesting more headers until there is a heaviest chain
		for ready, height = hd.Ready(); !ready; ready, height = hd.Ready() {
			reqs, timer := hd.RequestMoreHeaders(uint64(time.Now().Unix()), 5 /*timeout */)
			headerReqSend(ctx, reqs)
			select {
			case <-ctx.Done(): // When terminate signal is sent or Ctrl-C is pressed
				return nil
			case <-timer.C: // When it is time to check on previously sent requests
			case <-wakeUpChan: // When new message comes from the sentry
			case <-hd.StageReadyChannel(): // When heaviest chain is ready
			}
		}

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
		st, err1 := sync.Prepare(nil, nil /* chainConfig */, cc, &vm.Config{}, db, writeDB, "downloader", ethdb.DefaultStorageMode, ".", nil, 512*1024*1024, make(chan struct{}), nil, nil, func() error { return nil }, nil)
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
	}
}

func ReplacementStages(ctx context.Context,
	hd *headerdownload.HeaderDownload,
	bd *bodydownload.BodyDownload,
	headerReqSend func(context.Context, []*headerdownload.HeaderRequest),
	bodyReqSend func(context.Context, *bodydownload.BodyRequest) []byte,
	penalise func(context.Context, []byte),
	updateHead func(context.Context, uint64, common.Hash, *big.Int),
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
						return stagedsync.HeadersForward(s, ctx, world.TX, hd)
					},
					UnwindFunc: func(u *stagedsync.UnwindState, s *stagedsync.StageState) error {
						return u.Done(world.TX)
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
						return u.Done(world.TX)
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
						return u.Done(world.TX)
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
