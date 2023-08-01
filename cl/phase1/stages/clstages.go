package stages

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/ledgerwatch/erigon/cl/clstages"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	network2 "github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
)

type Cfg struct {
	rpc             *rpc.BeaconRpcP2P
	genesisCfg      *clparams.GenesisConfig
	beaconCfg       *clparams.BeaconChainConfig
	executionClient *execution_client.ExecutionClient
	state           *state.CachingBeaconState
	gossipManager   *network2.GossipManager
	forkChoice      *forkchoice.ForkChoiceStore
	dataDirFs       afero.Fs
}

type Args struct {
	targetEpoch, seenEpoch uint64
	targetSlot, seenSlot   uint64
}

func ClStagesCfg(
	rpc *rpc.BeaconRpcP2P,
	genesisCfg *clparams.GenesisConfig,
	beaconCfg *clparams.BeaconChainConfig,
	state *state.CachingBeaconState,
	executionClient *execution_client.ExecutionClient,
	gossipManager *network2.GossipManager,
	forkChoice *forkchoice.ForkChoiceStore,
	dataDirFs afero.Fs,
) *Cfg {
	return &Cfg{
		rpc:             rpc,
		genesisCfg:      genesisCfg,
		beaconCfg:       beaconCfg,
		state:           state,
		executionClient: executionClient,
		gossipManager:   gossipManager,
		forkChoice:      forkChoice,
		dataDirFs:       dataDirFs,
	}
}

// ConsensusClStages creates a stage loop container to be used to run caplin
func ConsensusClStages(ctx context.Context,
	cfg *Cfg,
) *clstages.StageGraph[*Cfg, Args] {
	rpcSource := clpersist.NewBeaconRpcSource(cfg.rpc)
	gossipSource := clpersist.NewGossipSource(ctx, cfg.gossipManager)
	processBlock := func(block *peers.PeeredObject[*cltypes.SignedBeaconBlock], newPayload, fullValidation bool) error {
		if err := cfg.forkChoice.OnBlock(block.Data, newPayload, fullValidation); err != nil {
			log.Warn("fail to process block", "reason", err, "slot", block.Data.Block.Slot)
			cfg.rpc.BanPeer(block.Peer)
			return err
		}
		// NOTE: this error is ignored and logged only!
		err := clpersist.SaveBlockWithConfig(afero.NewBasePathFs(cfg.dataDirFs, "caplin/beacon"), block.Data, cfg.beaconCfg)
		if err != nil {
			log.Error("failed to persist block to store", "slot", block.Data.Block.Slot, "err", err)
		}
		return nil
	}

	// TODO: this is an ugly hack, but it works!
	shouldForkChoiceSinceReorg := false
	return &clstages.StageGraph[*Cfg, Args]{
		ArgsFunc: func(ctx context.Context, cfg *Cfg) (args Args) {
			args.seenSlot = cfg.forkChoice.HighestSeen()
			args.seenEpoch = args.seenSlot / cfg.beaconCfg.SlotsPerEpoch
			args.targetSlot = utils.GetCurrentSlot(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot)
			args.targetEpoch = utils.GetCurrentEpoch(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, cfg.beaconCfg.SlotsPerEpoch) - 1
			return
		},
		Stages: map[string]clstages.Stage[*Cfg, Args]{
			"WaitForPeers": {
				Description: `wait for enough peers. This is also a safe stage to go to when unsure of what stage to use`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if args.seenEpoch < args.targetEpoch {
						return "CatchUpEpochs"
					}
					return "CatchUpBlocks"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					peersCount, err := cfg.rpc.Peers()
					if err != nil {
						return nil
					}
					waitWhenNotEnoughPeers := 3 * time.Second
					minPeersForDownload := uint64(4)
					for {
						if peersCount > minPeersForDownload {
							break
						}
						logger.Debug("[Caplin] Waiting For Peers", "have", peersCount, "needed", minPeersForDownload, "retryIn", waitWhenNotEnoughPeers)
						time.Sleep(waitWhenNotEnoughPeers)
						peersCount, err = cfg.rpc.Peers()
						if err != nil {
							peersCount = 0
						}
					}
					return nil
				},
			},
			"CatchUpEpochs": {
				Description: `if we are 1 or more epochs behind, we download in parallel by epoch`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if args.seenEpoch < args.targetEpoch {
						return "CatchUpEpochs"
					}
					return "CatchUpBlocks"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					totalEpochs := args.targetEpoch - args.seenEpoch
					logger = logger.New(
						"slot", fmt.Sprintf("%d/%d", args.seenSlot, args.targetSlot),
						"epoch", fmt.Sprintf("%d/%d(%d)", args.seenEpoch, args.targetEpoch, (1+args.targetEpoch)*cfg.beaconCfg.SlotsPerEpoch-1),
					)
					logger.Info("downloading epochs from reqresp")
					ctx, cn := context.WithTimeout(ctx, time.Duration(cfg.beaconCfg.SecondsPerSlot*cfg.beaconCfg.SlotsPerEpoch)*time.Second)
					defer cn()
					counter := atomic.Int64{}
					// now we download the missing blocks
					chans := make([]chan []*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0, totalEpochs)
					ctx, cn = context.WithCancel(ctx)
					egg, ctx := errgroup.WithContext(ctx)

					// is 8 too many?
					egg.SetLimit(8)
					defer cn()
					for i := args.seenEpoch; i <= args.targetEpoch; i = i + 1 {
						ii := i
						o := make(chan []*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0)
						chans = append(chans, o)
						egg.Go(func() error {
							blocks, err := rpcSource.GetRange(ctx, ii*cfg.beaconCfg.SlotsPerEpoch, cfg.beaconCfg.SlotsPerEpoch)
							if err != nil {
								return err
							}
							logger.Info("downloading epochs from reqresp", "progress", fmt.Sprintf("%d", int(100*(float64(counter.Add(1))/float64(totalEpochs+1))))+"%")
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
									if block.Data.Block.Slot <= args.seenSlot {
										continue
									}
									err := processBlock(block, false, false)
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
			},
			"CatchUpBlocks": {
				Description: `if we are within the epoch but not at head, we run catchupblocks`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if args.seenEpoch < args.targetEpoch {
						return "CatchUpEpochs"
					}
					if args.seenSlot < args.targetSlot {
						return "CatchUpBlocks"
					}
					return "ForkChoice"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					// wait for three slots... should be plenty enough time
					ctx, cn := context.WithTimeout(ctx, time.Duration(cfg.beaconCfg.SecondsPerSlot)*time.Second*3)
					defer cn()
					logger.Info("waiting for blocks...",
						"seenSlot", args.seenSlot,
						"targetSlot", args.targetSlot,
					)
					respCh := make(chan []*peers.PeeredObject[*cltypes.SignedBeaconBlock])
					errCh := make(chan error)
					sources := []clpersist.BlockSource{gossipSource, rpcSource}
					for _, v := range sources {
						sourceFunc := v.GetRange
						go func() {
							blocks, err := sourceFunc(ctx, args.seenSlot+1, 1)
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
							if err := processBlock(block, true, true); err != nil {
								return err
							}
							logger.Info("block processed", "slot", block.Data.Block.Slot)
						}
					}
					return nil
				},
			},
			"ForkChoice": {
				Description: `fork choice stage. We will send all fork choise things here
			also, we will wait up to delay seconds to deal with attestations + side forks`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if args.seenEpoch < args.targetEpoch {
						return "CatchUpEpochs"
					}
					if args.seenSlot < args.targetSlot {
						return "CatchUpBlocks"
					}
					return "ListenForForks"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {

					// TODO: we need to get the last run block in order to process attestations here
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
			},
			"ListenForForks": {
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					defer func() {
						shouldForkChoiceSinceReorg = false
					}()
					if args.seenEpoch < args.targetEpoch {
						return "CatchUpEpochs"
					}
					if args.seenSlot < args.targetSlot {
						return "CatchUpBlocks"
					}
					if shouldForkChoiceSinceReorg {
						return "ForkChoice"
					}
					if args.seenSlot%32 == 0 {
						return "CleanupAndPruning"
					}
					return "SleepForSlot"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					slotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, args.targetSlot).Add(
						time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second / 3,
					)
					waitDur := slotTime.Sub(time.Now())
					ctx, cn := context.WithTimeout(ctx, waitDur)
					defer cn()
					// try to get the current block
					blocks, err := gossipSource.GetRange(ctx, args.seenSlot, 1)
					if err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return nil
						}
						return err
					}
					for _, block := range blocks {
						if err := processBlock(block, false, true); err != nil {
							// its okay if block processing fails
							logger.Warn("reorg block failed validation", "err", err)
							return nil
						}
						shouldForkChoiceSinceReorg = true
					}
					return nil
				},
			},
			"CleanupAndPruning": {
				Description: `cleanup and pruning is done here`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					if args.seenEpoch < args.targetEpoch {
						return "CatchUpEpochs"
					}
					if args.seenSlot < args.targetSlot {
						return "CatchUpBlocks"
					}
					return "SleepForSlot"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					// clean up some old ranges
					err := gossipSource.PurgeRange(ctx, 1, args.seenSlot-cfg.beaconCfg.SlotsPerEpoch*16)
					if err != nil {
						return err
					}
					//TODO: probably can clear old superepoch in fs here as well!
					return nil
				},
			},
			"SleepForSlot": {
				Description: `sleep until the next slot`,
				TransitionFunc: func(cfg *Cfg, args Args, err error) string {
					return "WaitForPeers"
				},
				ActionFunc: func(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
					nextSlot := args.seenSlot + 1
					nextSlotTime := utils.GetSlotTime(cfg.genesisCfg.GenesisTime, cfg.beaconCfg.SecondsPerSlot, nextSlot)
					nextSlotDur := nextSlotTime.Sub(time.Now())
					logger.Info("sleeping until next slot", "slot", nextSlot, "time", nextSlotTime, "dur", nextSlotDur)
					time.Sleep(nextSlotDur)
					return nil
				},
			},
		},
	}
}
