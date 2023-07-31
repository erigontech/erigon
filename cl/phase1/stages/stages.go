package stages

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"
)

// StateStages are all stages necessary for basic unwind and stage computation, it is primarly used to process side forks and memory execution.
func ConsensusStages(ctx context.Context,
	cfg CaplinStagedSyncCfg,
) []*stagedsync.Stage {
	rpcSource := clpersist.NewBeaconRpcSource(cfg.rpc)
	gossipSource := clpersist.NewGossipSource(ctx, cfg.gossipManager)
	processBlock := func(block *peers.PeeredObject[*cltypes.SignedBeaconBlock], newPayload, fullValidation bool) error {
		if err := cfg.forkChoice.OnBlock(block.Data, newPayload, fullValidation); err != nil {
			log.Warn("fail to process block", "reason", err, "slot", block.Data.Block.Slot)
			return err
		}
		// NOTE: this error is ignored and logged only!
		err := clpersist.SaveBlockWithConfig(afero.NewBasePathFs(cfg.dataDirFs, "caplin/beacon"), block.Data, cfg.beaconCfg)
		if err != nil {
			log.Error("failed to persist block to store", "slot", block.Data.Block.Slot, "err", err)
		}
		return nil
	}
	return []*stagedsync.Stage{
		{
			ID:          "WaitForPeers",
			Description: "wait for enough peers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				return SpawnStageWaitForPeers(ctx, cfg, s)
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "CatchUpEpochs",
			Description: `if we are 1 or more epochs behind, we download in parallel by epoch`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetEpoch, seenEpoch := getEpochs(cfg)
				if seenEpoch >= targetEpoch {
					return nil
				}
				currentSlot, seenSlot := getSlots(cfg)
				if seenSlot >= currentSlot {
					return nil
				}

				totalEpochs := targetEpoch - seenEpoch
				logger = logger.New(
					"slot", fmt.Sprintf("%d/%d", seenSlot, currentSlot),
					"epoch", fmt.Sprintf("%d/%d(%d)", seenEpoch, targetEpoch, (1+targetEpoch)*cfg.beaconCfg.SlotsPerEpoch-1),
				)
				logger.Info("downloading epochs from reqresp")
				counter := atomic.Int64{}
				// now we download the missing blocks
				chans := make([]chan []*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0, totalEpochs)
				ctx, cn := context.WithCancel(ctx)
				egg, ctx := errgroup.WithContext(ctx)

				// is 8 too many?
				egg.SetLimit(8)
				defer cn()
				for i := seenEpoch; i <= targetEpoch; i = i + 1 {
					ii := i
					o := make(chan []*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0)
					chans = append(chans, o)
					egg.Go(func() error {
						blocks, err := rpcSource.GetRange(ctx, ii*cfg.beaconCfg.SlotsPerEpoch, cfg.beaconCfg.SlotsPerEpoch)
						if err != nil {
							return err
						}
						logger.Info("downloading epochs from reqresp", "progress", fmt.Sprintf("%f", float64(counter.Add(1))/float64(totalEpochs))+"%")
						o <- blocks
						return nil
					})
				}
				errchan := make(chan error)
				go func() {
					defer close(errchan)
					for _, v := range chans {
						select {
						case <-ctx.Done():
							return
						case epochResp := <-v:
							for _, block := range epochResp {
								if block.Data.Block.Slot <= seenSlot {
									continue
								}
								err := processBlock(block, false, true)
								if err != nil {
									errchan <- err
									return
								}
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
			ID: "CatchUpBlocks",
			Description: `this stage runs if the current node is not at head, otherwise it moves on.
			`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetEpoch, seenEpoch := getEpochs(cfg)
				if seenEpoch < targetEpoch {
					return nil
				}
				targetSlot, seenSlot := getSlots(cfg)
				if seenSlot >= targetSlot {
					return nil
				}
				// wait for three slots... should be plenty enough time
				ctx, cn := context.WithTimeout(ctx, time.Duration(cfg.beaconCfg.SecondsPerSlot)*time.Second*3)
				defer cn()
				logger.Info("waiting for blocks...",
					"seenSlot", seenSlot,
					"targetSlot", targetSlot,
				)
				respCh := make(chan []*peers.PeeredObject[*cltypes.SignedBeaconBlock])
				errCh := make(chan error)
				sources := []clpersist.BlockSource{gossipSource, rpcSource}

				for _, v := range sources {
					sourceFunc := v.GetRange
					go func() {
						blocks, err := sourceFunc(ctx, seenSlot+1, targetSlot-seenSlot)
						if err != nil {
							errCh <- err
							return
						}
						respCh <- blocks
					}()
				}

				select {
				case err := <-errCh:
					return err
				case blocks := <-respCh:
					for _, block := range blocks {
						if err := processBlock(block, false, true); err != nil {
							return err
						}
						log.Info("block processed", "slot", block.Data.Block.Slot)
					}
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID: "ForkChoice",
			Description: `fork choice stage. We will send all fork choise things here
			also, we will wait up to delay seconds to deal with attestations + side forks
			`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetSlot, seenSlot := getSlots(cfg)
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
			ID: "CleanupAndPruning",
			Description: ` cleanup and pruning is done here, but only if we are at head
			`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetSlot, seenSlot := getSlots(cfg)
				if seenSlot != targetSlot {
					return nil
				}
				// things below this will only happen on epoch boundaries
				if seenSlot%cfg.beaconCfg.SlotsPerEpoch != 0 {
					return nil
				}
				// clean up some old ranges
				err := gossipSource.PurgeRange(ctx, 1, seenSlot-cfg.beaconCfg.SlotsPerEpoch*4)
				if err != nil {
					logger.Error("could not prune old ranges", "err", err, "slot", seenSlot-cfg.beaconCfg.SlotsPerEpoch*4)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, logger log.Logger) error {
				return nil
			},
		},
		{
			ID:          "SleepForSlot",
			Description: `if at head and we get to this stage, we sleep until the next slot`,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *stagedsync.StageState, u stagedsync.Unwinder, tx kv.RwTx, logger log.Logger) error {
				targetSlot, seenSlot := getSlots(cfg)
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

func getSlots(cfg CaplinStagedSyncCfg) (targetSlot, seenSlot uint64) {
	targetSlot = utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
	seenSlot = cfg.forkChoice.HighestSeen()
	return
}
func getEpochs(cfg CaplinStagedSyncCfg) (targetEpoch, seenEpoch uint64) {
	seenSlot := cfg.forkChoice.HighestSeen()
	seenEpoch = seenSlot / cfg.beaconCfg.SlotsPerEpoch
	targetEpoch = utils.GetCurrentEpoch(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) - 1
	return
}

func SpawnStageWaitForPeers(ctx context.Context, cfg CaplinStagedSyncCfg, s *stagedsync.StageState) error {
	peersCount, err := cfg.rpc.Peers()
	if err != nil {
		return nil
	}
	waitWhenNotEnoughPeers := 3 * time.Second
	for {
		if peersCount > minPeersForDownload {
			break
		}
		log.Debug("[Caplin] Waiting For Peers", "have", peersCount, "needed", minPeersForSyncStart, "retryIn", waitWhenNotEnoughPeers)
		time.Sleep(waitWhenNotEnoughPeers)
		peersCount, err = cfg.rpc.Peers()
		if err != nil {
			peersCount = 0
		}
	}
	return nil
}
