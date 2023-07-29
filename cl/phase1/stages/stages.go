package stages

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
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
	return []*stagedsync.Stage{
		{
			ID:          "wait_for_peers",
			Description: "wait for enough peers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageWaitForPeers(forkchoice, s, tx, ctx)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "catch_up_epochs",
			Description: "catch up epochs",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice
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
				for i := seenEpoch; i < targetEpoch; i = i + 1 {
					ii := i
					o := make(chan resp, 0)
					chans = append(chans, o)
					egg.Go(func() error {
						log.Debug("request epoch from reqresp", "epoch", ii)
						blocks, err := cfg.source.GetRange(ctx, ii*cfg.beaconCfg.SlotsPerEpoch, cfg.beaconCfg.SlotsPerEpoch)
						if err != nil {
							return err
						}
						log.Debug("got epoch from reqresp", "epoch", ii)
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
			ID:          "process_gossip",
			Description: "try all gossip received blocks in the mean time to see if any stick",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice
				//			targetSlot := utils.GetCurreforkChoice.ntSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				//			slotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, targetSlot)
				for {
					select {
					case sbb := <-gossipBlocks:
						if err := cfg.forkChoice.OnBlock(sbb.Data, true, true); err != nil {
							//TODO: rules for pruning
							//if uint64(slotTime.Unix()) < cfg.forkChoice.HighestSeen()+(cfg.beaconCfg.SlotsPerEpoch/4) {
							//	cfg.rpc.BanPeer(sbb.Peer)
							//}
							return err
						}
					default:
						// nothing to read, so return and move to next stage
						return nil
					}
				}
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID: "catch_up_blocks",
			Description: `this stage runs if the current node is not at head, otherwise it moves on.
			that means that we missed the blocks from gossip in the previous stage.
			`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice

				seenSlot := cfg.forkChoice.HighestSeen()
				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)

				seenEpoch := seenSlot / cfg.beaconCfg.SlotsPerEpoch
				targetEpoch := utils.GetCurrentEpoch(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch)

				if seenEpoch < targetEpoch {
					return nil
				}

				if seenSlot >= targetSlot {
					return nil
				}
				logger.Info("we are blocks behind - downloading blocks from reqresp", "from", seenSlot, "to", targetSlot)
				blocks, err := cfg.source.GetRange(ctx, seenSlot+1, targetSlot-seenSlot)
				if err != nil {
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
					}
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "fork_choice",
			Description: "fork choice stage",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				cfg := forkchoice
				targetSlot := utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
				seenSlot := cfg.forkChoice.HighestSeen()
				if seenSlot != targetSlot {
					return nil
				}
				nextSlot := targetSlot + 1
				nextSlotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, nextSlot)
				nextSlotDur := nextSlotTime.Sub(time.Now())

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
					log.Info("Caplin is sending forkchoice")
					// Run forkchoice
					if err := cfg.forkChoice.Engine().ForkChoiceUpdate(
						cfg.forkChoice.GetEth1Hash(finalizedCheckpoint.BlockRoot()),
						cfg.forkChoice.GetEth1Hash(headRoot),
					); err != nil {
						log.Warn("Could not set forkchoice", "err", err)
						return err
					}
				}
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
