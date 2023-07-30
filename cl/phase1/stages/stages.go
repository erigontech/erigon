package stages

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context,
	forkchoice StageForkChoiceCfg,
) []*stagedsync.Stage {
	gossipBlocks := forkchoice.gossipManager.SubscribeSignedBeaconBlocks(ctx)
	cfg := forkchoice

	saveBlock := func(block *cltypes.SignedBeaconBlock) error {
		err := clpersist.SaveBlockWithConfig(afero.NewBasePathFs(forkchoice.dataDirFs, "caplin/beacon"), block, cfg.beaconCfg)
		if err != nil {
			log.Error("failed to persist block to store", "slot", block.Block.Slot, "err", err)
		}
		return err

	}
	return []*stagedsync.Stage{
		{
			ID:          "WaitForPeers",
			Description: "wait for enough peers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageWaitForPeers(forkchoice, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "CatchUpEpochs",
			Description: "catch up epochs",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetEpoch := utils.GetCurrentEpoch(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) - 1
				seenSlot := cfg.forkChoice.HighestSeen()
				seenEpoch := seenSlot / cfg.beaconCfg.SlotsPerEpoch
				currentSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				if seenEpoch >= targetEpoch {
					return nil
				}
				totalEpochs := targetEpoch - seenEpoch
				logger.Info("we are epochs behind - downloading epochs from reqresp",
					"fromEpoch", seenEpoch,
					"toEpoch", targetEpoch,
					"epochs", totalEpochs,
					"seenSlot", seenSlot,
					"targetSlot", (1+targetEpoch)*cfg.beaconCfg.SlotsPerEpoch-1,
					"currentSlot", currentSlot,
				)
				// now we download the missing blocks

				type resp struct {
					blocks []*cltypes.SignedBeaconBlock
				}
				chans := make([]chan resp, 0, totalEpochs)
				ctx, cn := context.WithCancel(ctx)
				egg, ctx := errgroup.WithContext(ctx)
				egg.SetLimit(8)
				defer cn()
				for i := seenEpoch; i <= targetEpoch; i = i + 1 {
					ii := i
					o := make(chan resp, 0)
					chans = append(chans, o)
					egg.Go(func() error {
						logger.Debug("request epoch from reqresp", "epoch", ii)
						blocks, err := cfg.source.GetRange(ctx, ii*cfg.beaconCfg.SlotsPerEpoch, cfg.beaconCfg.SlotsPerEpoch)
						if err != nil {
							return err
						}
						logger.Debug("got epoch from reqresp", "epoch", ii)
						o <- resp{blocks}
						return nil
					})
				}
				errchan := make(chan error)
				go func() {
					defer func() {
						close(errchan)
					}()
					for _, v := range chans {
						select {
						case <-ctx.Done():
							return
						default:
						}
						epochResp := <-v
						for _, block := range epochResp.blocks {
							if block.Block.Slot <= seenSlot {
								continue
							}
							if err := cfg.forkChoice.OnBlock(block, false, true); err != nil {
								log.Warn("fail to process block", "reason", err, "slot", block.Block.Slot)
								err2 := cfg.source.PurgeRange(context.Background(), block.Block.Slot, totalEpochs)
								if err2 != nil {
									log.Error("failed to purge range", "err", err2)
								}
								errchan <- err
								return
							}
							saveBlock(block)
						}
					}
				}()
				err := egg.Wait()
				if err != nil {
					return err
				}
				return <-errchan
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},

		{
			ID: "CatchUpBlocks",
			Description: `this stage runs if the current node is not at head, otherwise it moves on.
			`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				seenSlot := cfg.forkChoice.HighestSeen()
				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)

				seenEpoch := seenSlot / cfg.beaconCfg.SlotsPerEpoch
				targetEpoch := utils.GetCurrentEpoch(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) - 1
				if seenEpoch < targetEpoch {
					return nil
				}
				if seenSlot >= targetSlot {
					return nil
				}

				// wait for three slots... should be plenty enough time
				ctx, cn := context.WithTimeout(ctx, time.Duration(cfg.beaconCfg.SecondsPerSlot)*time.Second*3)
				defer cn()

				go func() {
					cfg := forkchoice
					for {
						select {
						case sbb := <-gossipBlocks:
							if err := cfg.forkChoice.OnBlock(sbb.Data, true, true); err != nil {
								logger.Info("gossip block unused", "err", err)
							} else {
								logger.Info("gossip block processed", "slot", sbb.Data.Block.Slot)
								saveBlock(sbb.Data)
							}
							seenSlot := cfg.forkChoice.HighestSeen()
							targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
							if seenSlot >= targetSlot {
								cn()
								return
							}
						case <-ctx.Done():
							return
						}
					}
				}()
				logger.Info("waiting for blocks...",
					"seenSlot", seenSlot,
					"targetSlot", targetSlot)
				blocks, err := cfg.source.GetRange(ctx, seenSlot+1, targetSlot-seenSlot)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					if strings.Contains(err.Error(), "context canceled") {
						return nil
					}
					return err
				}
				for _, block := range blocks {
					if err := cfg.forkChoice.OnBlock(block, false, true); err != nil {
						log.Warn("fail to process block", "reason", err, "slot", block.Block.Slot)
						err2 := cfg.source.PurgeRange(context.Background(), block.Block.Slot, targetSlot)
						if err2 != nil {
							log.Error("failed to purge range", "err", err2)
						}
						return err
					} else {
						log.Info("block processed", "slot", block.Block.Slot)
						saveBlock(block)
					}
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "ForkChoice",
			Description: `fork choice stage. the wait until the next block is also here (only if at head)`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				seenSlot := cfg.forkChoice.HighestSeen()
				if seenSlot != targetSlot {
					return nil
				}

				////////block.Block.Body.Attestations.Range(func(idx int, a *solid.Attestation, total int) bool {
				////////	if err = g.forkChoice.OnAttestation(a, true); err != nil {
				////////		return false
				////////	}
				////////	return true
				////////})
				////////if err != nil {
				////////	return err
				////////}

				// Now check the head
				headRoot, _, err := cfg.forkChoice.GetHead()
				if err != nil {
					return err
				}

				// Do forkchoice if possible
				if cfg.forkChoice.Engine() != nil {
					finalizedCheckpoint := cfg.forkChoice.FinalizedCheckpoint()
					logger.Info("Caplin is sending forkchoice")
					// Run forkchoice
					if err := cfg.forkChoice.Engine().ForkChoiceUpdate(
						cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
						cfg.forkChoice.GetEth1Hash(headRoot),
					); err != nil {
						logger.Warn("Could not set forkchoice", "err", err)
						return err
					}
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "SleepForEpoch",
			Description: `if at head and we get to this stage, we sleep until the next slot`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				seenSlot := cfg.forkChoice.HighestSeen()
				if seenSlot != targetSlot {
					return nil
				}
				nextSlot := targetSlot + 1
				nextSlotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, nextSlot)
				nextSlotDur := nextSlotTime.Sub(time.Now())
				logger.Info("sleeping until next slot", "slot", nextSlot, "time", nextSlotTime, "dur", nextSlotDur)
				time.Sleep(nextSlotDur)
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
	}
}
