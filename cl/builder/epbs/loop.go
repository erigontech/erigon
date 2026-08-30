package epbs

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	clservices "github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/hexutil"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

const (
	preferencesTimeout          = 4 * time.Second // wait up to 4s into the slot for preferences
	weiPerGwei                  = 1_000_000_000   // consensus layer uses Gwei; EL blockValue is in wei
	maxSpeculativeBuildsPerSlot = 128
)

// pendingPayload is a completed build result waiting for a bid-won reveal.
type pendingPayload struct {
	slot                uint64
	builderIndex        uint64
	payloadId           uint64
	assembled           *eladapter.AssembledPayload
	execReqs            *cltypes.ExecutionRequests
	parent              ParentInfo
	reveals             map[common.Hash]revealState
	bidValue            uint64
	reservationReleased bool
	columnCellsMu       sync.Mutex
	columnCells         []peerdasutils.CellsAndKZGProofs
	columnCellsErr      error
	columnCellsReady    bool
}

type revealState uint8

const (
	revealQueued revealState = iota
	revealInProgress
	revealComplete
	revealRetryable
	revealExpired
)

// SlotContext holds per-slot context that the caller provides to the builder loop.
// This separates beacon state access (caller responsibility) from build logic.
type SlotContext struct {
	Slot          uint64
	FinalizedSlot uint64
	Parent        ParentInfo
	DependentRoot common.Hash
	Timestamp     uint64      // Unix timestamp for the slot (from ComputeTimestampAtSlot)
	PrevRandao    common.Hash // RANDAO mix for the current epoch
	Withdrawals   []*types.Withdrawal
	BuilderIndex  uint64
	BuilderStatus BalanceStatus
	BuilderFound  bool
	BidDeadline   time.Time
}

// BuilderLoop is the slot-driven build-bid-reveal core loop for the ePBS builder.
type BuilderLoop struct {
	mu sync.Mutex

	manager                  *BuilderManager
	strategy                 BidStrategy
	specBuild                *SpeculativeBuild
	prefsWatch               *PreferencesWatcher
	submitter                BidSubmitter
	beaconCfg                *clparams.BeaconChainConfig
	pendingStore             PendingPayloadStore
	reservationReleaseBefore uint64

	// pendingPayloads stores completed builds keyed by (slot, parentBlockHash)
	// for later reveal when the bid wins.
	pendingPayloads map[pendingPayloadKey]*pendingPayload

	// speculativePayloads tracks speculative build payloadIds keyed by (slot, parentHash).
	speculativePayloads map[speculativeKey]uint64
}

type payloadBroadcastDiscarder interface {
	discardPayloadBroadcast(common.Hash)
}

// pendingPayloadKey includes both EL parentBlockHash and CL parentBlockRoot.
// Different beacon parents can share the same EL parentBlockHash in the EMPTY
// path — using only the hash would let the second build silently overwrite the
// first, and OnBidWon could reveal the wrong payload for the winning root.
type pendingPayloadKey struct {
	slot            uint64
	parentBlockHash common.Hash
	parentBlockRoot common.Hash
	blockHash       common.Hash
}

// speculativeKey also includes parentBlockRoot for the same reason: two
// ParentCandidates in ActiveParents can share the same EL hash.
type speculativeKey struct {
	slot            uint64
	parentBlockHash common.Hash
	parentBlockRoot common.Hash
}

// NewBuilderLoop creates a BuilderLoop.
func NewBuilderLoop(
	manager *BuilderManager,
	strategy BidStrategy,
	assembler PayloadAssembler,
	prefsWatch *PreferencesWatcher,
	submitter BidSubmitter,
	beaconCfg *clparams.BeaconChainConfig,
) *BuilderLoop {
	return &BuilderLoop{
		manager:             manager,
		strategy:            strategy,
		specBuild:           NewSpeculativeBuild(assembler),
		prefsWatch:          prefsWatch,
		submitter:           submitter,
		beaconCfg:           beaconCfg,
		pendingPayloads:     make(map[pendingPayloadKey]*pendingPayload),
		speculativePayloads: make(map[speculativeKey]uint64),
	}
}

// OnNewHead starts a speculative EL build for the given parent.
// Called when the builder observes a new head via fork choice.
func (l *BuilderLoop) OnNewHead(ctx context.Context, sc SlotContext) error {
	l.releaseReservationsBeforeSlot(sc.Slot)
	l.prunePendingBeforeSlot(sc.FinalizedSlot)
	l.pruneSpeculativeBeforeSlot(sc.Slot)
	params := l.buildParams(sc)

	payloadId, err := l.specBuild.StartBuild(ctx, params)
	if err != nil {
		return fmt.Errorf("epbs/loop: OnNewHead speculative build: %w", err)
	}

	key := speculativeKey{slot: sc.Slot, parentBlockHash: sc.Parent.ExecutionHash, parentBlockRoot: sc.Parent.BlockRoot}
	var discarded []uint64
	l.mu.Lock()
	if _, replacing := l.speculativePayloads[key]; !replacing {
		trackedForSlot := 0
		for trackedKey := range l.speculativePayloads {
			if trackedKey.slot == key.slot {
				trackedForSlot++
			}
		}
		if trackedForSlot >= maxSpeculativeBuildsPerSlot {
			for trackedKey, trackedPayloadID := range l.speculativePayloads {
				if trackedKey.slot == key.slot {
					delete(l.speculativePayloads, trackedKey)
					discarded = append(discarded, trackedPayloadID)
					break
				}
			}
		}
	}
	previousPayloadID, replaced := l.speculativePayloads[key]
	l.speculativePayloads[key] = payloadId
	l.mu.Unlock()
	for _, discardedPayloadID := range discarded {
		if discardedPayloadID != payloadId {
			l.specBuild.Discard(discardedPayloadID)
		}
	}
	if replaced && previousPayloadID != payloadId {
		l.specBuild.Discard(previousPayloadID)
	}

	log.Info("ePBS builder: speculative build started",
		"slot", sc.Slot, "parentHash", sc.Parent.ExecutionHash, "payloadId", payloadId)

	return nil
}

// OnSlot is called at the start of each slot. It waits for proposer preferences,
// determines whether the speculative build matches, and submits a bid.
func (l *BuilderLoop) OnSlot(ctx context.Context, sc SlotContext) error {
	l.releaseReservationsBeforeSlot(sc.Slot)
	if !sc.BidDeadline.IsZero() {
		if !time.Now().Before(sc.BidDeadline) {
			return nil
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, sc.BidDeadline)
		defer cancel()
	}
	prefs, err := l.prefsWatch.WaitForPreferences(ctx, sc.Slot, sc.DependentRoot, preferencesTimeout)
	if err != nil {
		log.Debug("ePBS builder: no preferences, skipping slot", "slot", sc.Slot, "err", err)
		return nil // skip path
	}

	return l.buildAndBid(ctx, sc, prefs)
}

func (l *BuilderLoop) prunePendingBeforeSlot(slot uint64) {
	type removedPending struct {
		key     pendingPayloadKey
		pending *pendingPayload
	}
	var removed []removedPending
	l.mu.Lock()
	for key, pending := range l.pendingPayloads {
		if key.slot < slot && !revealInFlight(pending) {
			delete(l.pendingPayloads, key)
			removed = append(removed, removedPending{key: key, pending: pending})
		}
	}
	l.mu.Unlock()
	for _, removed := range removed {
		if submitter, ok := l.submitter.(payloadBroadcastDiscarder); ok {
			for root := range removed.pending.reveals {
				submitter.discardPayloadBroadcast(root)
			}
		}
		if l.pendingStore != nil {
			if err := l.pendingStore.Delete(context.Background(), removed.key); err != nil {
				log.Warn("ePBS builder: delete persisted pending payload failed", "slot", removed.key.slot, "err", err)
			}
		}
		if !removed.pending.reservationReleased {
			removed.pending.reservationReleased = true
			l.manager.ReleaseBid(removed.pending.bidValue)
		}
	}
}

func (l *BuilderLoop) pruneSpeculativeBeforeSlot(slot uint64) {
	var discarded []uint64
	l.mu.Lock()
	for key, payloadID := range l.speculativePayloads {
		if key.slot < slot {
			delete(l.speculativePayloads, key)
			discarded = append(discarded, payloadID)
		}
	}
	l.mu.Unlock()
	for _, payloadID := range discarded {
		l.specBuild.Discard(payloadID)
	}
}

func (l *BuilderLoop) releaseReservationsBeforeSlot(slot uint64) {
	var released []uint64
	l.mu.Lock()
	if slot > l.reservationReleaseBefore {
		l.reservationReleaseBefore = slot
	}
	for key, pending := range l.pendingPayloads {
		if key.slot < slot && !pending.reservationReleased {
			pending.reservationReleased = true
			released = append(released, pending.bidValue)
		}
	}
	l.mu.Unlock()
	for _, bidValue := range released {
		l.manager.ReleaseBid(bidValue)
	}
}

func revealInFlight(pending *pendingPayload) bool {
	for _, reveal := range pending.reveals {
		if reveal == revealQueued || reveal == revealInProgress {
			return true
		}
	}
	return false
}

// buildAndBid implements the speculative match -> calculate value -> bid -> submit flow.
func (l *BuilderLoop) buildAndBid(ctx context.Context, sc SlotContext, prefs *cltypes.SignedProposerPreferences) error {
	slot := sc.Slot
	parent := sc.Parent

	// Try fast path: check if speculative build matches
	l.mu.Lock()
	payloadId, hasSpec := l.speculativePayloads[speculativeKey{slot: slot, parentBlockHash: parent.ExecutionHash, parentBlockRoot: parent.BlockRoot}]
	l.mu.Unlock()

	var assembled *eladapter.AssembledPayload
	var usedPayloadId uint64

	if hasSpec {
		// Fast path: try to get the speculative build result
		result, err := l.specBuild.GetResult(ctx, payloadId)
		if err != nil {
			log.Warn("ePBS builder: speculative build retrieval failed, rebuilding",
				"slot", slot, "err", err)
			hasSpec = false
		} else if result == nil {
			log.Debug("ePBS builder: speculative build not ready, rebuilding", "slot", slot)
			hasSpec = false
		} else if err := validatePayloadForSlot(result, sc); err != nil {
			return fmt.Errorf("epbs/loop: invalid speculative payload: %w", err)
		} else if !speculativeMatchesPrefs(result, prefs, sc.Parent.GasLimit) {
			// Speculative build was started before preferences arrived (OnNewHead
			// uses FeeRecipient=0x0 and default GasLimit). If the proposer wants
			// different values, the speculative payload is invalid — fall through
			// to rebuild with the correct parameters.
			log.Info("ePBS builder: speculative build mismatch, rebuilding",
				"slot", slot,
				"specFeeRecipient", result.Eth1Block.FeeRecipient,
				"prefsFeeRecipient", prefs.Message.FeeRecipient,
				"specGasLimit", result.Eth1Block.GasLimit,
				"prefsGasLimit", prefs.Message.TargetGasLimit)
			hasSpec = false
		} else {
			assembled = result
			usedPayloadId = payloadId
			log.Info("ePBS builder: fast path -- speculative build matched", "slot", slot)
		}
	}

	if !hasSpec {
		// Rebuild path: start a new build with the preferences-derived parameters
		params := l.buildParamsFromPrefs(sc, prefs)
		newPayloadId, err := l.specBuild.StartBuild(ctx, params)
		if err != nil {
			return fmt.Errorf("epbs/loop: rebuild StartBuild: %w", err)
		}
		defer l.specBuild.Discard(newPayloadId)

		buildWindow := time.Duration(l.beaconCfg.SecondsPerSlot) * time.Second / 4
		if buildWindow <= 0 {
			return nil
		}
		buildTimer := time.NewTimer(buildWindow)
		select {
		case <-buildTimer.C:
		case <-ctx.Done():
			buildTimer.Stop()
			return ctx.Err()
		}

		collectionCtx, cancelCollection := context.WithTimeout(ctx, buildWindow)
		defer cancelCollection()
		retry := time.NewTicker(100 * time.Millisecond)
		defer retry.Stop()
		for {
			result, err := l.specBuild.GetResult(collectionCtx, newPayloadId)
			if err != nil {
				if collectionCtx.Err() != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					log.Warn("ePBS builder: rebuild collection timed out", "slot", slot)
					return nil
				}
				return fmt.Errorf("epbs/loop: rebuild GetResult: %w", err)
			}
			if result != nil {
				if err := validatePayloadForSlot(result, sc); err != nil {
					return fmt.Errorf("epbs/loop: invalid rebuilt payload: %w", err)
				}
				assembled = result
				usedPayloadId = newPayloadId
				goto buildDone
			}
			select {
			case <-retry.C:
			case <-collectionCtx.Done():
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Warn("ePBS builder: rebuild collection timed out", "slot", slot)
				return nil
			}
		}
	}

buildDone:
	if assembled == nil {
		return nil
	}
	if prefs == nil || prefs.Message == nil {
		return fmt.Errorf("epbs/loop: missing proposer preferences")
	}
	if assembled.Eth1Block.FeeRecipient != prefs.Message.FeeRecipient {
		return fmt.Errorf("epbs/loop: payload fee recipient does not match proposer preferences")
	}
	if !payloadGasLimitMatchesTarget(assembled.Eth1Block.GasLimit, sc.Parent.GasLimit, prefs.Message.TargetGasLimit) {
		return fmt.Errorf("epbs/loop: payload gas limit does not match proposer preferences")
	}

	// Calculate block value
	blockValue := assembled.BlockValue
	if blockValue == nil {
		blockValue = new(big.Int)
	}

	// Apply bid strategy
	bidAmount := l.strategy.Decide(slot, blockValue)
	if bidAmount == nil {
		log.Info("ePBS builder: strategy decided to skip slot", "slot", slot, "blockValue", blockValue)
		return nil
	}
	bidGwei := new(big.Int).Div(new(big.Int).Set(bidAmount), big.NewInt(weiPerGwei))
	if !bidGwei.IsUint64() {
		return fmt.Errorf("epbs/loop: bid amount exceeds uint64 gwei")
	}
	bidValue := bidGwei.Uint64()
	if !sc.BuilderFound || !l.manager.ReserveBidWithStatus(sc.BuilderStatus, bidValue) {
		log.Debug("ePBS builder: inactive or insufficient available balance, skipping bid", "slot", slot, "bidValue", bidValue)
		return nil
	}
	reserved := true
	defer func() {
		if reserved {
			l.manager.ReleaseBid(bidValue)
		}
	}()

	// Decode execution requests and compute ExecutionRequestsRoot
	execReqs, execReqsRoot, err := l.decodeAndHashRequests(assembled.RequestsBundle)
	if err != nil {
		return fmt.Errorf("epbs/loop: execution requests: %w", err)
	}
	maxBlobs := l.beaconCfg.GetBlobParameters(slot / l.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock
	if err := validateBlobsBundle(assembled.BlobsBundle, maxBlobs); err != nil {
		return fmt.Errorf("epbs/loop: blobs bundle: %w", err)
	}

	// Build KZG commitments from blobs bundle
	blobCommitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	if assembled.BlobsBundle != nil {
		for _, commitment := range assembled.BlobsBundle.Commitments {
			var c cltypes.KZGCommitment
			copy(c[:], commitment)
			blobCommitments.Append(&c)
		}
	}

	// Construct the bid
	// BuilderIndex is stamped by manager.SignBid, but we set it here too for
	// the pending key. If the index isn't resolved yet, skip bidding entirely.
	bidBuilderIndex := sc.BuilderIndex

	bid := &cltypes.ExecutionPayloadBid{
		Slot:                  slot,
		ParentBlockHash:       parent.ExecutionHash,
		ParentBlockRoot:       parent.BlockRoot,
		BlockHash:             assembled.Eth1Block.BlockHash,
		PrevRandao:            assembled.Eth1Block.PrevRandao,
		FeeRecipient:          assembled.Eth1Block.FeeRecipient,
		GasLimit:              assembled.Eth1Block.GasLimit,
		BuilderIndex:          bidBuilderIndex,
		Value:                 bidValue,
		ExecutionPayment:      0,
		BlobKzgCommitments:    *blobCommitments,
		ExecutionRequestsRoot: execReqsRoot,
	}

	// Sign the bid
	signedBid, err := l.manager.SignBidForBuilderIndex(ctx, bid, bidBuilderIndex)
	if err != nil {
		return fmt.Errorf("epbs/loop: sign bid: %w", err)
	}

	key := pendingPayloadKey{
		slot: slot, parentBlockHash: parent.ExecutionHash, parentBlockRoot: parent.BlockRoot,
		blockHash: assembled.Eth1Block.BlockHash,
	}
	pending := &pendingPayload{
		slot:         slot,
		builderIndex: bidBuilderIndex,
		payloadId:    usedPayloadId,
		assembled:    assembled,
		execReqs:     execReqs,
		parent:       parent,
		bidValue:     bidValue,
	}
	l.mu.Lock()
	if slot < l.reservationReleaseBefore {
		l.mu.Unlock()
		return fmt.Errorf("epbs/loop: slot %d collateral window has closed", slot)
	}
	previous := l.pendingPayloads[key]
	l.pendingPayloads[key] = pending
	l.mu.Unlock()
	if l.pendingStore != nil {
		if err := l.pendingStore.Save(ctx, key, pending, l.manager.Pubkey()); err != nil {
			if errors.Is(err, ErrPendingPayloadMayExist) {
				reserved = false
				return fmt.Errorf("epbs/loop: persist pending payload: %w", err)
			}
			l.restorePreviousPending(key, pending, previous)
			return fmt.Errorf("epbs/loop: persist pending payload: %w", err)
		}
	}
	reserved = false

	if err := l.submitter.SubmitBid(ctx, signedBid); err != nil {
		if errors.Is(err, ErrBidNotPublished) {
			var rollbackErr error
			if l.pendingStore != nil {
				if previous == nil {
					rollbackErr = l.pendingStore.Delete(context.Background(), key)
				} else {
					rollbackErr = l.pendingStore.Save(context.Background(), key, previous, l.manager.Pubkey())
				}
			}
			if rollbackErr != nil {
				return errors.Join(fmt.Errorf("epbs/loop: submit bid: %w", err), fmt.Errorf("rollback durable pending payload: %w", rollbackErr))
			}
			l.restorePreviousPending(key, pending, previous)
			l.manager.ReleaseBid(bidValue)
			return fmt.Errorf("epbs/loop: submit bid: %w", err)
		}
		if previous != nil {
			l.manager.ReleaseBid(previous.bidValue)
		}
		return fmt.Errorf("epbs/loop: submit bid: %w", err)
	}

	if previous != nil {
		l.manager.ReleaseBid(previous.bidValue)
	}

	log.Info("ePBS builder: bid submitted",
		"slot", slot,
		"blockHash", bid.BlockHash,
		"value", blockValue,
		"bidAmount", bidAmount,
	)

	return nil
}

func (l *BuilderLoop) restorePreviousPending(key pendingPayloadKey, current, previous *pendingPayload) {
	l.mu.Lock()
	if l.pendingPayloads[key] == current {
		if previous == nil {
			delete(l.pendingPayloads, key)
		} else {
			l.pendingPayloads[key] = previous
		}
	}
	l.mu.Unlock()
}

func (l *BuilderLoop) restorePendingPayloads(ctx context.Context, currentSlot, minSlot uint64) error {
	records, err := l.pendingStore.Load(ctx, minSlot)
	if err != nil {
		return err
	}
	l.reservationReleaseBefore = currentSlot
	for i := range records {
		record := &records[i]
		if record.BuilderPubkey != l.manager.Pubkey() {
			return errors.New("stored pending payload belongs to a different builder key")
		}
		key, pending, err := decodeStoredPendingPayload(*record, l.beaconCfg)
		if err != nil {
			return err
		}
		if l.pendingPayloads[key] != nil {
			return fmt.Errorf("duplicate stored pending payload for slot %d", key.slot)
		}
		l.pendingPayloads[key] = pending
		if key.slot < currentSlot {
			pending.reservationReleased = true
		} else {
			if err := l.manager.RestoreBidReservation(pending.bidValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnBidWon is called when the builder's bid wins (included in a beacon block).
// It constructs the execution payload envelope, signs it, and broadcasts it.
//
// parentBlockRoot is the CL parent root used during building (for pending key
// lookup — matches the ParentBlockRoot in the bid). beaconBlockRoot is the root
// of the beacon block that included the winning bid (set in the envelope per
// spec). These are distinct values: parentBlockRoot identifies which build to
// reveal, while beaconBlockRoot links the envelope to the including block.
func (l *BuilderLoop) OnBidWon(ctx context.Context, slot uint64, builderIndex uint64, parentHash common.Hash, parentBlockRoot common.Hash, blockHash common.Hash, beaconBlockRoot common.Hash) error {
	key := pendingPayloadKey{slot: slot, parentBlockHash: parentHash, parentBlockRoot: parentBlockRoot, blockHash: blockHash}
	l.mu.Lock()
	pending, ok := l.pendingPayloads[key]
	if !ok {
		for candidateKey := range l.pendingPayloads {
			if candidateKey.slot == slot && candidateKey.parentBlockHash == parentHash && candidateKey.parentBlockRoot == parentBlockRoot {
				l.mu.Unlock()
				return fmt.Errorf("epbs/loop: winning block hash %s does not match pending payload", blockHash)
			}
		}
		l.mu.Unlock()
		return fmt.Errorf("epbs/loop: no pending payload for slot %d parentHash %s parentBlockRoot %s", slot, parentHash, parentBlockRoot)
	}
	if pending.builderIndex != builderIndex {
		l.mu.Unlock()
		return nil
	}
	if pending.assembled == nil || pending.assembled.Eth1Block == nil || pending.assembled.Eth1Block.BlockHash != blockHash {
		l.mu.Unlock()
		return fmt.Errorf("epbs/loop: winning block hash %s does not match pending payload", blockHash)
	}
	if pending.reveals == nil {
		pending.reveals = make(map[common.Hash]revealState)
	}
	if state, exists := pending.reveals[beaconBlockRoot]; exists && (state == revealInProgress || state == revealComplete) {
		l.mu.Unlock()
		return nil
	} else if exists && state == revealExpired {
		l.mu.Unlock()
		return ErrRevealExpired
	}
	pending.reveals[beaconBlockRoot] = revealInProgress
	l.mu.Unlock()
	revealed := false
	defer func() {
		if revealed {
			return
		}
		l.mu.Lock()
		if current := l.pendingPayloads[key]; current == pending {
			current.reveals[beaconBlockRoot] = revealQueued
		}
		l.mu.Unlock()
	}()

	// Construct the envelope using the NewExecutionPayloadEnvelope constructor
	envelope := cltypes.NewExecutionPayloadEnvelope(l.beaconCfg)
	envelope.Payload = pending.assembled.Eth1Block
	envelope.ExecutionRequests = pending.execReqs
	envelope.BuilderIndex = pending.builderIndex
	envelope.BeaconBlockRoot = beaconBlockRoot
	envelope.ParentBeaconBlockRoot = parentBlockRoot

	// Sign the envelope
	signedEnvelope, err := l.manager.SignEnvelopeForBuilderIndex(ctx, envelope, slot, pending.builderIndex)
	if err != nil {
		return fmt.Errorf("epbs/loop: sign envelope: %w", err)
	}

	columnSidecars, err := pending.buildDataColumnSidecars(slot, beaconBlockRoot)
	if err != nil {
		return fmt.Errorf("epbs/loop: data column sidecars: %w", err)
	}

	if err := l.broadcastPayload(ctx, signedEnvelope, columnSidecars); err != nil {
		return fmt.Errorf("epbs/loop: broadcast payload: %w", err)
	}

	l.mu.Lock()
	if current := l.pendingPayloads[key]; current == pending {
		current.reveals[beaconBlockRoot] = revealComplete
	}
	l.mu.Unlock()
	revealed = true

	log.Info("ePBS builder: payload revealed",
		"slot", slot,
		"blockHash", pending.assembled.Eth1Block.BlockHash,
		"beaconBlockRoot", beaconBlockRoot,
	)

	return nil
}

func (l *BuilderLoop) queuePendingBidReveal(slot, builderIndex uint64, parentHash, parentBlockRoot, blockHash, beaconRoot common.Hash) (pendingPayloadKey, bool) {
	key := pendingPayloadKey{slot: slot, parentBlockHash: parentHash, parentBlockRoot: parentBlockRoot, blockHash: blockHash}
	l.mu.Lock()
	pending := l.pendingPayloads[key]
	if pending == nil || pending.builderIndex != builderIndex {
		l.mu.Unlock()
		return key, false
	}
	if pending.reveals == nil {
		pending.reveals = make(map[common.Hash]revealState)
	}
	state, exists := pending.reveals[beaconRoot]
	queued := !exists || state == revealRetryable
	if queued {
		pending.reveals[beaconRoot] = revealQueued
	}
	l.mu.Unlock()
	return key, queued
}

func (l *BuilderLoop) unqueuePendingBidReveal(key pendingPayloadKey, beaconRoot common.Hash) {
	l.mu.Lock()
	if pending := l.pendingPayloads[key]; pending != nil && pending.reveals[beaconRoot] == revealQueued {
		pending.reveals[beaconRoot] = revealRetryable
	}
	l.mu.Unlock()
}

func (l *BuilderLoop) hasPendingPayloads() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pendingPayloads) > 0
}

func (l *BuilderLoop) unresolvedPendingPayloadSlots() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	capacity := min(len(l.pendingPayloads), maxPendingPayloadFiles)
	seen := make(map[uint64]struct{}, capacity)
	slots := make([]uint64, 0, capacity)
	for key, pending := range l.pendingPayloads {
		if !pendingNeedsRevealReconciliation(pending) {
			continue
		}
		if _, ok := seen[key.slot]; ok {
			continue
		}
		seen[key.slot] = struct{}{}
		slots = append(slots, key.slot)
		if len(slots) == maxPendingPayloadFiles {
			break
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] > slots[j] })
	return slots
}

func pendingNeedsRevealReconciliation(pending *pendingPayload) bool {
	if len(pending.reveals) == 0 {
		return true
	}
	for _, reveal := range pending.reveals {
		if reveal == revealRetryable {
			return true
		}
	}
	return false
}

func (l *BuilderLoop) abandonPendingBidReveal(key pendingPayloadKey, beaconRoot common.Hash) {
	l.mu.Lock()
	if pending := l.pendingPayloads[key]; pending != nil {
		pending.reveals[beaconRoot] = revealExpired
	}
	l.mu.Unlock()
	if submitter, ok := l.submitter.(payloadBroadcastDiscarder); ok {
		submitter.discardPayloadBroadcast(beaconRoot)
	}
}

func (l *BuilderLoop) broadcastPayload(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, columnSidecars []*cltypes.DataColumnSidecar) error {
	return l.submitter.BroadcastPayload(ctx, envelope, columnSidecars)
}

func validateBlobsBundle(bundle *eladapter.BlobsBundle, maxBlobs uint64) error {
	if bundle == nil {
		return nil
	}
	if uint64(len(bundle.Commitments)) > maxBlobs {
		return fmt.Errorf("%d commitments exceed maximum %d", len(bundle.Commitments), maxBlobs)
	}
	if len(bundle.Commitments) != len(bundle.Blobs) {
		return fmt.Errorf("%d commitments for %d blobs", len(bundle.Commitments), len(bundle.Blobs))
	}
	for i := range bundle.Commitments {
		if len(bundle.Commitments[i]) != len(cltypes.KZGCommitment{}) {
			return fmt.Errorf("commitment %d has length %d", i, len(bundle.Commitments[i]))
		}
		if len(bundle.Blobs[i]) != cltypes.BytesPerBlob {
			return fmt.Errorf("blob %d has length %d", i, len(bundle.Blobs[i]))
		}
	}
	if len(bundle.Proofs) != len(bundle.Blobs) {
		return fmt.Errorf("%d proofs for %d blobs", len(bundle.Proofs), len(bundle.Blobs))
	}
	for i := range bundle.Blobs {
		if len(bundle.Proofs[i]) != len(goethkzg.KZGProof{}) {
			return fmt.Errorf("proof %d has length %d", i, len(bundle.Proofs[i]))
		}
		var blob goethkzg.Blob
		var commitment goethkzg.KZGCommitment
		var proof goethkzg.KZGProof
		copy(blob[:], bundle.Blobs[i])
		copy(commitment[:], bundle.Commitments[i])
		copy(proof[:], bundle.Proofs[i])
		if err := kzg.Ctx().VerifyBlobKZGProof(&blob, commitment, proof); err != nil {
			return fmt.Errorf("blob %d KZG verification failed: %w", i, err)
		}
	}
	return nil
}

func validateAssembledPayload(assembled *eladapter.AssembledPayload) error {
	if assembled == nil {
		return fmt.Errorf("missing assembled payload")
	}
	if assembled.Eth1Block == nil {
		return fmt.Errorf("missing execution payload")
	}
	if assembled.Eth1Block.Extra == nil || assembled.Eth1Block.Transactions == nil || assembled.Eth1Block.Withdrawals == nil {
		return fmt.Errorf("execution payload has uninitialized fields")
	}
	return nil
}

func validatePayloadForSlot(assembled *eladapter.AssembledPayload, slotContext SlotContext) error {
	if err := validateAssembledPayload(assembled); err != nil {
		return err
	}
	payload := assembled.Eth1Block
	if payload.ParentHash != slotContext.Parent.ExecutionHash {
		return fmt.Errorf("execution payload parent hash %s does not match %s", payload.ParentHash, slotContext.Parent.ExecutionHash)
	}
	if payload.Time != slotContext.Timestamp {
		return fmt.Errorf("execution payload timestamp %d does not match %d", payload.Time, slotContext.Timestamp)
	}
	if payload.PrevRandao != slotContext.PrevRandao {
		return fmt.Errorf("execution payload prev_randao does not match slot context")
	}
	if payload.SlotNumber != slotContext.Slot {
		return fmt.Errorf("execution payload slot_number %d does not match %d", payload.SlotNumber, slotContext.Slot)
	}
	if !payloadWithdrawalsMatch(payload.Withdrawals, slotContext.Withdrawals) {
		return fmt.Errorf("execution payload withdrawals do not match slot context")
	}
	return nil
}

func payloadWithdrawalsMatch(payload *solid.ListSSZ[*cltypes.Withdrawal], expected []*types.Withdrawal) bool {
	if payload.Len() != len(expected) {
		return false
	}
	for i, withdrawal := range expected {
		actual := payload.Get(i)
		if actual == nil || withdrawal == nil || actual.Index != withdrawal.Index || actual.Validator != withdrawal.Validator || actual.Address != withdrawal.Address || actual.Amount != withdrawal.Amount {
			return false
		}
	}
	return true
}

var computeCellsAndKZGProofs = peerdasutils.ComputeCellsAndKZGProofs

func buildDataColumnSidecars(blobsBundle *eladapter.BlobsBundle, slot uint64, beaconBlockRoot common.Hash) ([]*cltypes.DataColumnSidecar, error) {
	cellsAndProofsPerBlob, err := deriveDataColumnCells(blobsBundle)
	if err != nil || len(cellsAndProofsPerBlob) == 0 {
		return nil, err
	}
	return peerdasutils.GetDataColumnSidecarsGloas(slot, beaconBlockRoot, cellsAndProofsPerBlob)
}

func deriveDataColumnCells(blobsBundle *eladapter.BlobsBundle) ([]peerdasutils.CellsAndKZGProofs, error) {
	if blobsBundle == nil || len(blobsBundle.Blobs) == 0 {
		return nil, nil
	}

	cellsAndProofsPerBlob := make([]peerdasutils.CellsAndKZGProofs, 0, len(blobsBundle.Blobs))
	for i, blob := range blobsBundle.Blobs {
		cells, proofs, err := computeCellsAndKZGProofs(blob)
		if err != nil {
			return nil, fmt.Errorf("blob %d: %w", i, err)
		}
		cellsAndProofsPerBlob = append(cellsAndProofsPerBlob, peerdasutils.CellsAndKZGProofs{
			Blobs:  cells,
			Proofs: proofs,
		})
	}

	return cellsAndProofsPerBlob, nil
}

func (p *pendingPayload) buildDataColumnSidecars(slot uint64, beaconBlockRoot common.Hash) ([]*cltypes.DataColumnSidecar, error) {
	if p == nil || p.assembled == nil || p.assembled.BlobsBundle == nil || len(p.assembled.BlobsBundle.Blobs) == 0 {
		return nil, nil
	}
	p.columnCellsMu.Lock()
	defer p.columnCellsMu.Unlock()
	if !p.columnCellsReady {
		p.columnCells, p.columnCellsErr = deriveDataColumnCells(p.assembled.BlobsBundle)
		p.columnCellsReady = true
	}
	if p.columnCellsErr != nil {
		return nil, p.columnCellsErr
	}
	return peerdasutils.GetDataColumnSidecarsGloas(slot, beaconBlockRoot, p.columnCells)
}

// buildParams constructs EL builder.Parameters from a SlotContext.
// PrevRandao is always set — the CL validates payload.prev_randao == state RANDAO
// (operations.go:791), so omitting it would make the reveal invalid.
func (l *BuilderLoop) buildParams(sc SlotContext) *builder.Parameters {
	slotNum := sc.Slot
	beaconBlockRoot := sc.Parent.BlockRoot
	return &builder.Parameters{
		ParentHash:            sc.Parent.ExecutionHash,
		Timestamp:             sc.Timestamp,
		PrevRandao:            sc.PrevRandao,
		SuggestedFeeRecipient: common.Address{}, // will be overridden by prefs if available
		ParentBeaconBlockRoot: &beaconBlockRoot,
		SlotNumber:            &slotNum,
		Withdrawals:           sc.Withdrawals,
	}
}

// buildParamsFromPrefs constructs EL builder.Parameters incorporating proposer preferences.
func (l *BuilderLoop) buildParamsFromPrefs(sc SlotContext, prefs *cltypes.SignedProposerPreferences) *builder.Parameters {
	params := l.buildParams(sc)
	if prefs != nil && prefs.Message != nil {
		params.SuggestedFeeRecipient = prefs.Message.FeeRecipient
		gl := prefs.Message.TargetGasLimit
		params.TargetGasLimit = &gl
	}
	return params
}

// speculativeMatchesPrefs checks whether a speculative build result is compatible
// with the proposer preferences. The speculative build was started before prefs
// arrived (OnNewHead uses FeeRecipient=0x0), so if the proposer requires a
// specific fee recipient, the speculative payload's coinbase must match.
func speculativeMatchesPrefs(result *eladapter.AssembledPayload, prefs *cltypes.SignedProposerPreferences, parentGasLimit uint64) bool {
	if prefs == nil || prefs.Message == nil {
		return true // no constraints
	}
	// FeeRecipient is the critical field: the EL sets coinbase from
	// SuggestedFeeRecipient passed in Parameters. If the speculative build
	// used 0x0 and the proposer wants a real address, they won't match.
	wantRecipient := prefs.Message.FeeRecipient
	if result.Eth1Block.FeeRecipient != wantRecipient {
		return false
	}
	if !payloadGasLimitMatchesTarget(result.Eth1Block.GasLimit, parentGasLimit, prefs.Message.TargetGasLimit) {
		return false
	}
	return true
}

func payloadGasLimitMatchesTarget(payloadGasLimit, parentGasLimit, targetGasLimit uint64) bool {
	return clservices.IsGasLimitTargetCompatible(parentGasLimit, payloadGasLimit, targetGasLimit)
}

// decodeAndHashRequests decodes a RequestsBundle into ExecutionRequests and computes
// the hash_tree_root. Returns nil ExecutionRequests and zero hash if bundle is nil.
func (l *BuilderLoop) decodeAndHashRequests(bundle *typesproto.RequestsBundle) (*cltypes.ExecutionRequests, common.Hash, error) {
	var requestList []hexutil.Bytes
	if bundle != nil {
		requests := bundle.GetRequests()
		requestList = make([]hexutil.Bytes, len(requests))
		for i := range requests {
			requestList[i] = requests[i]
		}
	}
	execReqs, err := cltypes.DecodeExecutionRequestsList(l.beaconCfg, requestList, clparams.GloasVersion)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("decode execution requests: %w", err)
	}

	root, err := execReqs.HashSSZ()
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("hash execution requests: %w", err)
	}

	return execReqs, common.Hash(root), nil
}
