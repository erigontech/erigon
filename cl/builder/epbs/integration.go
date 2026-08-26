package epbs

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	eth2 "github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
)

// BuilderService holds all builder components for lifecycle management.
type BuilderService struct {
	Loop    *BuilderLoop
	Manager *BuilderManager
	pool    *pool.EpbsPool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stop    sync.Once
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
	Columns    ColumnSidecarStorage
	Pending    PendingPayloadStore
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
	if cfg.KeyPath == "" {
		return nil, fmt.Errorf("epbs/integration: builder key path is required")
	}
	if math.IsNaN(cfg.BidMargin) || math.IsInf(cfg.BidMargin, 0) || cfg.BidMargin < 0 || cfg.BidMargin > 1 {
		return nil, fmt.Errorf("epbs/integration: bid margin must be between 0 and 1")
	}
	if cfg.MinProfit != nil && cfg.MinProfit.Sign() < 0 {
		return nil, fmt.Errorf("epbs/integration: minimum profit cannot be negative")
	}
	switch {
	case deps.Ctx == nil:
		return nil, fmt.Errorf("epbs/integration: context is required")
	case deps.BeaconCfg == nil:
		return nil, fmt.Errorf("epbs/integration: beacon config is required")
	case deps.EthClock == nil:
		return nil, fmt.Errorf("epbs/integration: ethereum clock is required")
	case deps.SyncedData == nil:
		return nil, fmt.Errorf("epbs/integration: synced data manager is required")
	case deps.ForkChoice == nil:
		return nil, fmt.Errorf("epbs/integration: fork choice store is required")
	case deps.Exec == nil:
		return nil, fmt.Errorf("epbs/integration: payload assembler is required")
	case deps.EpbsPool == nil:
		return nil, fmt.Errorf("epbs/integration: ePBS pool is required")
	case deps.Gossip == nil:
		return nil, fmt.Errorf("epbs/integration: gossip publisher is required")
	case deps.Columns == nil:
		return nil, fmt.Errorf("epbs/integration: data column storage is required")
	case deps.Pending == nil:
		return nil, fmt.Errorf("epbs/integration: pending payload storage is required")
	case deps.Emitters == nil:
		return nil, fmt.Errorf("epbs/integration: event emitter is required")
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
	switch {
	case err != nil:
		log.Warn("ePBS builder: could not resolve builder index (state may not be synced yet)", "err", err)
		// Continue with nil index; it will be resolved later by the balance monitor.
	case !found:
		log.Warn("ePBS builder: pubkey not found in builders registry — deposit may be pending",
			"pubkey", signer.Pubkey())
	default:
		log.Info("ePBS builder: resolved on-chain index", "builderIndex", builderIndex)
		manager.SetBuilderIndex(builderIndex)
		status, balanceErr := CheckBalance(deps.SyncedData, builderIndex, manager.Pubkey())
		if balanceErr != nil {
			log.Warn("ePBS builder: initial balance check failed", "err", balanceErr)
		} else {
			manager.SetBalanceStatus(status)
		}
	}

	// --- Build components ---
	prefsWatch := NewPreferencesWatcher()
	deps.EpbsPool.SetPreferencesHandler(prefsWatch.OnPreferencesReceived)

	submitter := NewCaplinBidSubmitter(deps.EpbsPool, deps.Gossip, deps.ForkChoice, deps.Columns)
	strategy := &FixedMarginStrategy{
		Margin:    cfg.BidMargin,
		MinProfit: cfg.MinProfit,
	}

	loop := NewBuilderLoop(manager, strategy, deps.Exec, prefsWatch, submitter, deps.BeaconCfg)
	loop.pendingStore = deps.Pending
	if err := loop.restorePendingPayloads(deps.Ctx); err != nil {
		return nil, fmt.Errorf("epbs/integration: restore pending payloads: %w", err)
	}
	currentSlot := deps.EthClock.GetCurrentSlot()
	loop.pruneBeforeSlot(currentSlot)
	if !time.Now().Before(payloadRevealDeadline(deps.EthClock, deps.BeaconCfg, currentSlot)) {
		loop.pruneBeforeSlot(currentSlot + 1)
	}

	serviceCtx, cancel := context.WithCancel(deps.Ctx)
	svc := &BuilderService{
		Loop:    loop,
		Manager: manager,
		pool:    deps.EpbsPool,
		cancel:  cancel,
	}

	// 1. Head watcher: subscribe to head events and call OnNewHead for speculative builds.
	svc.run(func() {
		runHeadWatcher(serviceCtx, deps.Emitters, deps.ForkChoice, deps.EthClock, deps.SyncedData, deps.BeaconCfg, loop)
	})
	svc.run(func() {
		runImportedBlockWatcher(serviceCtx, deps.Emitters, deps.ForkChoice, deps.EthClock, deps.BeaconCfg, loop)
	})

	// 2. Slot watcher: fires at slot boundaries to trigger OnSlot.
	svc.run(func() {
		runSlotWatcher(serviceCtx, deps.EthClock, deps.ForkChoice, deps.SyncedData, deps.BeaconCfg, loop)
	})

	// 3. Balance monitor: periodic on-chain status check + index re-resolve.
	svc.run(func() {
		RunBalanceMonitor(serviceCtx, deps.SyncedData, manager)
	})

	log.Info("ePBS builder: service started",
		"builderIndex", builderIndex,
		"bidMargin", cfg.BidMargin,
	)

	return svc, nil
}

func (s *BuilderService) run(fn func()) {
	s.wg.Go(fn)
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
	workerCtx, cancel := context.WithCancel(ctx)
	headEvents := make(chan *beaconevents.HeadData, 1)
	var worker sync.WaitGroup
	worker.Go(func() {
		for {
			select {
			case <-workerCtx.Done():
				return
			case headData := <-headEvents:
				handleHeadEvent(workerCtx, headData, fc, ethClock, sd, beaconCfg, loop)
			}
		}
	})
	defer func() {
		cancel()
		worker.Wait()
	}()

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
			select {
			case headEvents <- headData:
			default:
				select {
				case <-headEvents:
				default:
				}
				headEvents <- headData
			}
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
	if beaconCfg.GetCurrentStateVersion(nextSlot/beaconCfg.SlotsPerEpoch) < clparams.GloasVersion {
		return
	}
	parents := fc.ActiveParents(nextSlot)
	if len(parents) == 0 {
		return
	}

	timestamp := ethClock.GenesisTime() + beaconCfg.SecondsPerSlot*nextSlot

	for _, parent := range parents {
		if parent.BlockRoot != headData.Block || parent.Slot != headData.Slot {
			continue
		}
		sc, err := buildSlotContext(fc, beaconCfg, nextSlot, timestamp, parent, loop.manager.Pubkey())
		if err != nil {
			log.Debug("ePBS builder: unable to prepare OnNewHead slot context, skipping",
				"slot", nextSlot, "parentRoot", parent.BlockRoot, "err", err)
			continue
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
		if beaconCfg.GetCurrentStateVersion(slot/beaconCfg.SlotsPerEpoch) < clparams.GloasVersion {
			continue
		}

		parents := fc.ActiveParents(slot)
		if len(parents) == 0 {
			continue
		}

		timestamp := ethClock.GenesisTime() + beaconCfg.SecondsPerSlot*slot

		for _, parent := range parents {
			sc, err := buildSlotContext(fc, beaconCfg, slot, timestamp, parent, loop.manager.Pubkey())
			if err != nil {
				log.Debug("ePBS builder: unable to prepare OnSlot slot context, skipping",
					"slot", slot, "parentRoot", parent.BlockRoot, "err", err)
				continue
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

func buildSlotContext(
	fc slotContextForkChoice,
	beaconCfg *clparams.BeaconChainConfig,
	slot uint64,
	timestamp uint64,
	parent forkchoice.ParentCandidate,
	builderPubkey common.Bytes48,
) (SlotContext, error) {
	parentState, err := fc.GetStateAtBlockRoot(parent.BlockRoot, true)
	if err != nil {
		return SlotContext{}, fmt.Errorf("get parent state %s: %w", parent.BlockRoot, err)
	}
	if parentState == nil {
		return SlotContext{}, fmt.Errorf("parent state %s unavailable", parent.BlockRoot)
	}
	if parentState.Slot() != parent.Slot {
		return SlotContext{}, fmt.Errorf("parent state slot %d does not match parent slot %d", parentState.Slot(), parent.Slot)
	}
	if err := transition.DefaultMachine.ProcessSlots(parentState, slot); err != nil {
		return SlotContext{}, fmt.Errorf("advance parent state to slot %d: %w", slot, err)
	}

	epoch := slot / beaconCfg.SlotsPerEpoch
	if epoch <= beaconCfg.MinSeedLookahead {
		return SlotContext{}, fmt.Errorf("cannot compute proposer dependent root for epoch %d", epoch)
	}
	dependentSlot := (epoch-beaconCfg.MinSeedLookahead)*beaconCfg.SlotsPerEpoch - 1
	dependentRoot := fc.Ancestor(parent.BlockRoot, dependentSlot).Root
	if dependentRoot == (common.Hash{}) {
		return SlotContext{}, fmt.Errorf("proposer dependent root unavailable for parent %s", parent.BlockRoot)
	}
	withdrawals, err := resolveWithdrawalsForParent(parentState, fc, beaconCfg, slot, parent)
	if err != nil {
		return SlotContext{}, err
	}
	builderState, err := builderStateForParent(parentState, fc, parent, builderPubkey)
	if err != nil {
		return SlotContext{}, err
	}
	builderIndex, builderStatus, builderFound := builderStatusForPubkey(builderState, builderPubkey)
	return SlotContext{
		Slot: slot,
		Parent: ParentInfo{
			Slot:          parent.Slot,
			BlockRoot:     parent.BlockRoot,
			ExecutionHash: parent.ExecutionHash,
			ShouldExtend:  parent.ShouldExtend,
		},
		DependentRoot: dependentRoot,
		Timestamp:     timestamp,
		PrevRandao:    parentState.GetRandaoMixes(epoch),
		Withdrawals:   withdrawals,
		BuilderIndex:  builderIndex,
		BuilderStatus: builderStatus,
		BuilderFound:  builderFound,
	}, nil
}

func builderStateForParent(baseState *state.CachingBeaconState, fc slotContextForkChoice, parent forkchoice.ParentCandidate, builderPubkey common.Bytes48) (*state.CachingBeaconState, error) {
	stateCopy, err := baseState.Copy()
	if err != nil {
		return nil, fmt.Errorf("copy target parent state: %w", err)
	}
	if stateCopy.Version() < clparams.GloasVersion {
		return stateCopy, nil
	}
	if parent.ShouldExtend {
		envelope, err := fc.ReadEnvelopeFromDisk(parent.BlockRoot)
		if err != nil {
			return nil, fmt.Errorf("read FULL parent envelope: %w", err)
		}
		if envelope == nil || envelope.Message == nil || envelope.Message.ExecutionRequests == nil {
			return nil, fmt.Errorf("FULL parent %x missing envelope execution requests", parent.BlockRoot)
		}
		if err := (&eth2.Impl{}).ApplyParentExecutionPayload(stateCopy, envelope.Message.ExecutionRequests); err != nil {
			return nil, fmt.Errorf("apply FULL parent execution payload: %w", err)
		}
	}
	if _, _, found := builderStatusForPubkey(stateCopy, builderPubkey); !found {
		return stateCopy, nil
	}
	if err := (&eth2.Impl{}).ProcessWithdrawals(stateCopy, nil); err != nil {
		return nil, fmt.Errorf("process target parent withdrawals: %w", err)
	}
	return stateCopy, nil
}

type slotContextForkChoice interface {
	GetStateAtBlockRoot(common.Hash, bool) (*state.CachingBeaconState, error)
	Ancestor(common.Hash, uint64) forkchoice.ForkChoiceNode
	ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error)
}

func resolveWithdrawalsForParent(
	baseState *state.CachingBeaconState,
	fc slotContextForkChoice,
	beaconCfg *clparams.BeaconChainConfig,
	targetSlot uint64,
	parent forkchoice.ParentCandidate,
) ([]*types.Withdrawal, error) {
	stateVersion := beaconCfg.GetCurrentStateVersion(targetSlot / beaconCfg.SlotsPerEpoch)
	if stateVersion < clparams.GloasVersion {
		expected, err := state.GetExpectedWithdrawals(baseState, targetSlot/beaconCfg.SlotsPerEpoch)
		if err != nil {
			return nil, err
		}
		return withdrawalsToExecution(expected.Withdrawals), nil
	}

	if !parent.ShouldExtend {
		return withdrawalsListToExecution(baseState.GetPayloadExpectedWithdrawals()), nil
	}

	stateCopy, err := baseState.Copy()
	if err != nil {
		return nil, fmt.Errorf("copy state for FULL parent withdrawals: %w", err)
	}
	envelope, err := fc.ReadEnvelopeFromDisk(parent.BlockRoot)
	if err != nil {
		return nil, fmt.Errorf("read FULL parent envelope: %w", err)
	}
	if envelope == nil || envelope.Message == nil || envelope.Message.ExecutionRequests == nil {
		return nil, fmt.Errorf("FULL parent %x missing envelope execution requests", parent.BlockRoot)
	}
	stfMachine := &eth2.Impl{}
	if err := stfMachine.ApplyParentExecutionPayload(stateCopy, envelope.Message.ExecutionRequests); err != nil {
		return nil, fmt.Errorf("apply FULL parent execution payload: %w", err)
	}
	expected, err := state.GetExpectedWithdrawals(stateCopy, targetSlot/beaconCfg.SlotsPerEpoch)
	if err != nil {
		return nil, err
	}
	return withdrawalsToExecution(expected.Withdrawals), nil
}

func withdrawalsToExecution(withdrawals []*cltypes.Withdrawal) []*types.Withdrawal {
	if len(withdrawals) == 0 {
		return nil
	}
	out := make([]*types.Withdrawal, 0, len(withdrawals))
	for _, w := range withdrawals {
		if w == nil {
			continue
		}
		out = append(out, &types.Withdrawal{
			Index:     w.Index,
			Amount:    w.Amount,
			Validator: w.Validator,
			Address:   w.Address,
		})
	}
	return out
}

func withdrawalsListToExecution(withdrawals *solid.ListSSZ[*cltypes.Withdrawal]) []*types.Withdrawal {
	if withdrawals == nil || withdrawals.Len() == 0 {
		return nil
	}
	out := make([]*types.Withdrawal, 0, withdrawals.Len())
	for i := 0; i < withdrawals.Len(); i++ {
		w := withdrawals.Get(i)
		if w == nil {
			continue
		}
		out = append(out, &types.Withdrawal{
			Index:     w.Index,
			Amount:    w.Amount,
			Validator: w.Validator,
			Address:   w.Address,
		})
	}
	return out
}

type importedBlockReader interface {
	GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool)
}

type revealWinningBidFunc func(context.Context, uint64, uint64, common.Hash, common.Hash, common.Hash, common.Hash) error

func runImportedBlockWatcher(ctx context.Context, emitters *beaconevents.EventEmitter, reader *forkchoice.ForkChoiceStore, ethClock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, loop *BuilderLoop) {
	ch := make(chan *beaconevents.EventStream, 16)
	sub := emitters.State().Subscribe(ch)
	defer sub.Unsubscribe()
	workerCtx, cancel := context.WithCancel(ctx)
	scheduler := newRevealScheduler(workerCtx, 4, 128)
	defer func() {
		cancel()
		scheduler.Wait()
	}()
	if head, err := reader.GetHeadNode(); err == nil {
		if header, ok := reader.GetHeader(head.Root); ok && header != nil {
			_ = scheduleImportedBlockReveal(&beaconevents.BlockData{Slot: header.Slot, Block: head.Root}, reader, ethClock, beaconCfg, loop, scheduler)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			if event.Event != beaconevents.StateBlock {
				continue
			}
			data, ok := event.Data.(*beaconevents.BlockData)
			if !ok {
				continue
			}
			if err := scheduleImportedBlockReveal(data, reader, ethClock, beaconCfg, loop, scheduler); err != nil {
				log.Debug("ePBS builder: imported bid unavailable", "slot", data.Slot, "err", err)
			}
		case err := <-sub.Err():
			if err != nil {
				log.Warn("ePBS builder: block subscription error", "err", err)
			}
			return
		}
	}
}

func scheduleImportedBlockReveal(data *beaconevents.BlockData, reader importedBlockReader, ethClock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, loop *BuilderLoop, scheduler *revealScheduler) error {
	bid, err := importedBid(data, reader)
	if err != nil || bid == nil {
		return err
	}
	key, pending := loop.queuePendingBidReveal(bid.Slot, bid.BuilderIndex, bid.ParentBlockHash, bid.ParentBlockRoot, bid.BlockHash, data.Block)
	if !pending {
		return nil
	}
	beaconRoot := data.Block
	deadline := payloadRevealDeadline(ethClock, beaconCfg, bid.Slot)
	if !scheduler.Enqueue(revealTask{
		root: beaconRoot, deadline: deadline,
		reveal: func(revealCtx context.Context) error {
			return loop.OnBidWon(revealCtx, bid.Slot, bid.BuilderIndex, bid.ParentBlockHash, bid.ParentBlockRoot, bid.BlockHash, beaconRoot)
		},
		terminal: func(err error) {
			loop.abandonPendingBidReveal(key, beaconRoot)
			log.Warn("ePBS builder: winning bid reveal failed", "slot", bid.Slot, "err", err)
		},
	}) {
		loop.abandonPendingBidReveal(key, beaconRoot)
		return fmt.Errorf("reveal queue full for beacon root %s", beaconRoot)
	}
	return nil
}

func payloadRevealDeadline(ethClock eth_clock.EthereumClock, beaconCfg *clparams.BeaconChainConfig, slot uint64) time.Time {
	slotDuration := time.Duration(beaconCfg.SecondsPerSlot) * time.Second
	due := slotDuration * time.Duration(beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	return ethClock.GetSlotTime(slot).Add(due)
}

func handleImportedBlock(ctx context.Context, data *beaconevents.BlockData, reader importedBlockReader, reveal revealWinningBidFunc) error {
	bid, err := importedBid(data, reader)
	if err != nil || bid == nil {
		return err
	}
	return reveal(ctx, bid.Slot, bid.BuilderIndex, bid.ParentBlockHash, bid.ParentBlockRoot, bid.BlockHash, data.Block)
}

func importedBid(data *beaconevents.BlockData, reader importedBlockReader) (*cltypes.ExecutionPayloadBid, error) {
	if data == nil {
		return nil, fmt.Errorf("epbs/integration: nil imported block event")
	}
	block, ok := reader.GetBlock(data.Block)
	if !ok || block == nil || block.Block == nil || block.Block.Body == nil {
		return nil, fmt.Errorf("epbs/integration: imported block %s unavailable", data.Block)
	}
	signedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if signedBid == nil || signedBid.Message == nil {
		return nil, nil
	}
	return signedBid.Message, nil
}

// Shutdown cleans up the builder loop's pending payloads.
// Call this when Caplin shuts down (ctx cancellation handles goroutine cleanup).
func (s *BuilderService) Shutdown() {
	if s == nil || s.Loop == nil {
		return
	}
	s.stop.Do(func() {
		if s.pool != nil {
			s.pool.SetPreferencesHandler(nil)
		}
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
		s.Loop.mu.Lock()
		released := make([]uint64, 0, len(s.Loop.pendingPayloads))
		discarded := make([]uint64, 0, len(s.Loop.speculativePayloads))
		for k, pending := range s.Loop.pendingPayloads {
			delete(s.Loop.pendingPayloads, k)
			released = append(released, pending.bidValue)
		}
		for k, payloadID := range s.Loop.speculativePayloads {
			delete(s.Loop.speculativePayloads, k)
			discarded = append(discarded, payloadID)
		}
		s.Loop.mu.Unlock()
		for _, payloadID := range discarded {
			s.Loop.specBuild.Discard(payloadID)
		}
		for _, bidValue := range released {
			s.Loop.manager.ReleaseBid(bidValue)
		}
		log.Info("ePBS builder: shutdown complete")
	})
}
