package epbs

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	ethevent "github.com/erigontech/erigon/p2p/event"
)

// BuilderService holds all builder components for lifecycle management.
type BuilderService struct {
	Loop    *BuilderLoop
	Manager *BuilderManager
}

// BuilderDeps bundles the Caplin components that the builder needs.
type BuilderDeps struct {
	Ctx        context.Context
	BeaconCfg  *clparams.BeaconChainConfig
	EthClock   eth_clock.EthereumClock
	SyncedData *synced_data.SyncedDataManager
	ForkChoice *forkchoice.ForkChoiceStore
	Exec       PayloadAssembler
	EpbsPool   *pool.EpbsPool
	Gossip     GossipPublisher
	Emitters   *beaconevents.EventEmitter
}

// InitBuilderService initialises and starts the ePBS builder.
// Returns nil (with no error) if the builder is not enabled.
// The returned BuilderService.Loop.OnBidWon should be wired as the
// block service's OnBidWon callback.
func InitBuilderService(cfg epbscfg.Config, deps BuilderDeps) (*BuilderService, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	// --- Load signer ---
	signer, err := NewLocalSignerFromFile(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("epbs/integration: load builder key from %s: %w", cfg.KeyPath, err)
	}

	// --- Resolve builder index from head state ---
	genesisValidatorsRoot := deps.EthClock.GenesisValidatorsRoot()
	// Start with nil (unresolved); ResolveIndex needs only signer pubkey, not index.
	manager := NewBuilderManager(signer, nil, deps.BeaconCfg, genesisValidatorsRoot)
	builderIndex, found, err := manager.ResolveIndex(deps.SyncedData)
	if err != nil {
		log.Warn("ePBS builder: could not resolve builder index (state may not be synced yet)", "err", err)
		// Continue with nil index; it will be resolved later by the balance monitor.
	} else if !found {
		log.Warn("ePBS builder: pubkey not found in builders registry — deposit may be pending",
			"pubkey", signer.Pubkey())
	} else {
		log.Info("ePBS builder: resolved on-chain index", "builderIndex", builderIndex)
		manager.SetBuilderIndex(builderIndex)
	}

	// --- Build components ---
	prefsWatch := NewPreferencesWatcher()
	deps.EpbsPool.OnPreferencesReceived = prefsWatch.OnPreferencesReceived

	submitter := NewCaplinBidSubmitter(deps.EpbsPool, deps.Gossip, deps.ForkChoice)
	strategy := &FixedMarginStrategy{
		Margin:    cfg.BidMargin,
		MinProfit: cfg.MinProfit,
	}

	loop := NewBuilderLoop(manager, strategy, deps.Exec, prefsWatch, submitter, deps.BeaconCfg)

	// --- Start background goroutines ---

	// 1. Head watcher: subscribe to head events and call OnNewHead for speculative builds.
	go runHeadWatcher(deps.Ctx, deps.Emitters, deps.ForkChoice, deps.EthClock, deps.SyncedData, deps.BeaconCfg, loop)

	// 2. Slot watcher: fires at slot boundaries to trigger OnSlot.
	go runSlotWatcher(deps.Ctx, deps.EthClock, deps.ForkChoice, deps.SyncedData, deps.BeaconCfg, loop)

	// 3. Balance monitor: periodic on-chain status check + index re-resolve.
	go RunBalanceMonitor(deps.Ctx, deps.SyncedData, manager)

	svc := &BuilderService{
		Loop:    loop,
		Manager: manager,
	}

	log.Info("ePBS builder: service started",
		"builderIndex", builderIndex,
		"bidMargin", cfg.BidMargin,
		"feeRecipient", cfg.FeeRecipient,
	)

	return svc, nil
}

// runHeadWatcher subscribes to head events and triggers speculative builds.
func runHeadWatcher(
	ctx context.Context,
	emitters *beaconevents.EventEmitter,
	fc *forkchoice.ForkChoiceStore,
	ethClock eth_clock.EthereumClock,
	sd *synced_data.SyncedDataManager,
	beaconCfg *clparams.BeaconChainConfig,
	loop *BuilderLoop,
) {
	ch := make(chan *beaconevents.EventStream, 16)
	sub := emitters.State().Subscribe(ch)
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev.Event != beaconevents.StateHead {
				continue
			}
			headData, ok := ev.Data.(*beaconevents.HeadData)
			if !ok || headData == nil {
				continue
			}
			handleHeadEvent(ctx, headData, fc, ethClock, sd, beaconCfg, loop)
		case err := <-sub.Err():
			if err != nil {
				log.Warn("ePBS builder: head subscription error", "err", err)
			}
			return
		}
	}
}

// handleHeadEvent processes a single head event by starting speculative builds
// for all active parent candidates.
func handleHeadEvent(
	ctx context.Context,
	headData *beaconevents.HeadData,
	fc *forkchoice.ForkChoiceStore,
	ethClock eth_clock.EthereumClock,
	sd *synced_data.SyncedDataManager,
	beaconCfg *clparams.BeaconChainConfig,
	loop *BuilderLoop,
) {
	nextSlot := headData.Slot + 1
	parents := fc.ActiveParents(nextSlot)
	if len(parents) == 0 {
		return
	}

	// Get RANDAO from head state. If the state is unavailable (not synced),
	// skip — building with a zero RANDAO would produce an invalid payload
	// that gets rejected during state transition (operations.go:561).
	var randaoMix common.Hash
	if err := sd.ViewHeadState(func(s *state.CachingBeaconState) error {
		epoch := nextSlot / beaconCfg.SlotsPerEpoch
		randaoMix = s.GetRandaoMixes(epoch)
		return nil
	}); err != nil {
		log.Debug("ePBS builder: head state unavailable for OnNewHead, skipping",
			"slot", nextSlot, "err", err)
		return
	}

	timestamp := ethClock.GenesisTime() + beaconCfg.SecondsPerSlot*nextSlot

	for _, parent := range parents {
		sc := SlotContext{
			Slot: nextSlot,
			Parent: ParentInfo{
				Slot:          parent.Slot,
				BlockRoot:     parent.BlockRoot,
				ExecutionHash: parent.ExecutionHash,
				ShouldExtend:  parent.ShouldExtend,
			},
			Timestamp:  timestamp,
			PrevRandao: randaoMix,
		}
		if err := loop.OnNewHead(ctx, sc); err != nil {
			log.Warn("ePBS builder: OnNewHead failed",
				"slot", nextSlot,
				"parentHash", parent.ExecutionHash,
				"err", err,
			)
		}
	}
}

// runSlotWatcher wakes up at the start of each slot and triggers OnSlot.
func runSlotWatcher(
	ctx context.Context,
	ethClock eth_clock.EthereumClock,
	fc *forkchoice.ForkChoiceStore,
	sd *synced_data.SyncedDataManager,
	beaconCfg *clparams.BeaconChainConfig,
	loop *BuilderLoop,
) {
	for {
		currentSlot := ethClock.GetCurrentSlot()
		nextSlot := currentSlot + 1
		nextSlotTime := ethClock.GetSlotTime(nextSlot)
		sleepDur := time.Until(nextSlotTime)
		if sleepDur > 0 {
			timer := time.NewTimer(sleepDur)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		slot := ethClock.GetCurrentSlot()

		parents := fc.ActiveParents(slot)
		if len(parents) == 0 {
			continue
		}

		// Get RANDAO from head state. If unavailable, skip this slot —
		// a zero RANDAO would produce an invalid bid/envelope.
		var randaoMix common.Hash
		if err := sd.ViewHeadState(func(s *state.CachingBeaconState) error {
			epoch := slot / beaconCfg.SlotsPerEpoch
			randaoMix = s.GetRandaoMixes(epoch)
			return nil
		}); err != nil {
			log.Debug("ePBS builder: head state unavailable for OnSlot, skipping",
				"slot", slot, "err", err)
			continue
		}

		timestamp := ethClock.GenesisTime() + beaconCfg.SecondsPerSlot*slot

		for _, parent := range parents {
			sc := SlotContext{
				Slot: slot,
				Parent: ParentInfo{
					Slot:          parent.Slot,
					BlockRoot:     parent.BlockRoot,
					ExecutionHash: parent.ExecutionHash,
					ShouldExtend:  parent.ShouldExtend,
				},
				Timestamp:  timestamp,
				PrevRandao: randaoMix,
			}
			if err := loop.OnSlot(ctx, sc); err != nil {
				log.Warn("ePBS builder: OnSlot failed",
					"slot", slot,
					"parentHash", parent.ExecutionHash,
					"err", err,
				)
			}
		}
	}
}

// OnBidWonFunc returns a callback suitable for BlockService.OnBidWon.
// It delegates to BuilderLoop.OnBidWon in a goroutine to avoid blocking gossip.
func OnBidWonFunc(ctx context.Context, loop *BuilderLoop) func(slot uint64, builderIndex uint64, parentBlockHash common.Hash, parentBlockRoot common.Hash, beaconBlockRoot common.Hash) {
	return func(slot uint64, builderIndex uint64, parentBlockHash common.Hash, parentBlockRoot common.Hash, beaconBlockRoot common.Hash) {
		go func() {
			if err := loop.OnBidWon(ctx, slot, builderIndex, parentBlockHash, parentBlockRoot, beaconBlockRoot); err != nil {
				log.Warn("ePBS builder: OnBidWon failed",
					"slot", slot,
					"builderIndex", builderIndex,
					"parentHash", parentBlockHash,
					"err", err,
				)
			}
		}()
	}
}

// Shutdown cleans up the builder loop's pending payloads.
// Call this when Caplin shuts down (ctx cancellation handles goroutine cleanup).
func (s *BuilderService) Shutdown() {
	if s == nil || s.Loop == nil {
		return
	}
	s.Loop.mu.Lock()
	defer s.Loop.mu.Unlock()
	for k := range s.Loop.pendingPayloads {
		delete(s.Loop.pendingPayloads, k)
	}
	for k := range s.Loop.speculativePayloads {
		delete(s.Loop.speculativePayloads, k)
	}
	log.Info("ePBS builder: shutdown complete")
}

// Ensure the event subscription interface matches what we use.
var _ ethevent.Subscription = (ethevent.Subscription)(nil)
