package epbs

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

const (
	preferencesTimeout = 4 * time.Second // wait up to 4s into the slot for preferences
	weiPerGwei         = 1_000_000_000   // consensus layer uses Gwei; EL blockValue is in wei
)

// EIP-7685 request type prefixes. These are stable protocol constants duplicated
// here to avoid importing execution/types (keeping the dependency boundary clean).
const (
	depositRequestType       byte = 0x00
	withdrawalRequestType    byte = 0x01
	consolidationRequestType byte = 0x02
)

// pendingPayload is a completed build result waiting for a bid-won reveal.
type pendingPayload struct {
	slot      uint64
	payloadId uint64
	assembled *eladapter.AssembledPayload
	execReqs  *cltypes.ExecutionRequests
	parent    ParentInfo
}

// SlotContext holds per-slot context that the caller provides to the builder loop.
// This separates beacon state access (caller responsibility) from build logic.
type SlotContext struct {
	Slot       uint64
	Parent     ParentInfo
	Timestamp  uint64      // Unix timestamp for the slot (from ComputeTimestampAtSlot)
	PrevRandao common.Hash // RANDAO mix for the current epoch
}

// BuilderLoop is the slot-driven build-bid-reveal core loop for the ePBS builder.
type BuilderLoop struct {
	mu sync.Mutex

	manager    *BuilderManager
	strategy   BidStrategy
	specBuild  *SpeculativeBuild
	prefsWatch *PreferencesWatcher
	submitter  BidSubmitter
	beaconCfg  *clparams.BeaconChainConfig

	// pendingPayloads stores completed builds keyed by (slot, parentBlockHash)
	// for later reveal when the bid wins.
	pendingPayloads map[pendingPayloadKey]*pendingPayload

	// speculativePayloads tracks speculative build payloadIds keyed by (slot, parentHash).
	speculativePayloads map[speculativeKey]uint64
}

// pendingPayloadKey includes both EL parentBlockHash and CL parentBlockRoot.
// Different beacon parents can share the same EL parentBlockHash in the EMPTY
// path — using only the hash would let the second build silently overwrite the
// first, and OnBidWon could reveal the wrong payload for the winning root.
type pendingPayloadKey struct {
	slot            uint64
	parentBlockHash common.Hash
	parentBlockRoot common.Hash
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
	params := l.buildParams(sc)

	payloadId, err := l.specBuild.StartBuild(ctx, params)
	if err != nil {
		return fmt.Errorf("epbs/loop: OnNewHead speculative build: %w", err)
	}

	l.mu.Lock()
	l.speculativePayloads[speculativeKey{slot: sc.Slot, parentBlockHash: sc.Parent.ExecutionHash, parentBlockRoot: sc.Parent.BlockRoot}] = payloadId
	l.mu.Unlock()

	log.Info("ePBS builder: speculative build started",
		"slot", sc.Slot, "parentHash", sc.Parent.ExecutionHash, "payloadId", payloadId)

	return nil
}

// OnSlot is called at the start of each slot. It waits for proposer preferences,
// determines whether the speculative build matches, and submits a bid.
func (l *BuilderLoop) OnSlot(ctx context.Context, sc SlotContext) error {
	prefs, err := l.prefsWatch.WaitForPreferences(sc.Slot, preferencesTimeout)
	if err != nil {
		log.Debug("ePBS builder: no preferences, skipping slot", "slot", sc.Slot, "err", err)
		return nil // skip path
	}

	return l.buildAndBid(ctx, sc, prefs)
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
		} else if !speculativeMatchesPrefs(result, prefs) {
			// Speculative build was started before preferences arrived (OnNewHead
			// uses FeeRecipient=0x0 and default GasLimit). If the proposer wants
			// different values, the speculative payload is invalid — fall through
			// to rebuild with the correct parameters.
			log.Info("ePBS builder: speculative build mismatch, rebuilding",
				"slot", slot,
				"specFeeRecipient", result.Eth1Block.FeeRecipient,
				"prefsFeeRecipient", prefs.Message.FeeRecipient,
				"specGasLimit", result.Eth1Block.GasLimit,
				"prefsGasLimit", prefs.Message.GasLimit)
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

		// Poll for result (simple blocking wait with timeout)
		deadline := time.After(time.Duration(l.beaconCfg.SecondsPerSlot/4) * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-deadline:
				log.Warn("ePBS builder: rebuild timed out", "slot", slot)
				return nil
			case <-ticker.C:
				result, err := l.specBuild.GetResult(ctx, newPayloadId)
				if err != nil {
					return fmt.Errorf("epbs/loop: rebuild GetResult: %w", err)
				}
				if result != nil {
					assembled = result
					usedPayloadId = newPayloadId
					goto buildDone
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

buildDone:
	if assembled == nil {
		return nil
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

	// Decode execution requests and compute ExecutionRequestsRoot
	execReqs, execReqsRoot, err := l.decodeAndHashRequests(assembled.RequestsBundle)
	if err != nil {
		return fmt.Errorf("epbs/loop: execution requests: %w", err)
	}

	// Build KZG commitments from blobs bundle
	blobCommitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	if assembled.BlobsBundle != nil {
		for _, commitment := range assembled.BlobsBundle.Commitments {
			if len(commitment) == 48 {
				var c cltypes.KZGCommitment
				copy(c[:], commitment)
				blobCommitments.Append(&c)
			}
		}
	}

	// Construct the bid
	// BuilderIndex is stamped by manager.SignBid, but we set it here too for
	// the pending key. If the index isn't resolved yet, skip bidding entirely.
	bidBuilderIndex, resolved := l.manager.BuilderIndex()
	if !resolved {
		log.Debug("ePBS builder: builder index not resolved, skipping bid", "slot", slot)
		return nil
	}

	bid := &cltypes.ExecutionPayloadBid{
		Slot:                  slot,
		ParentBlockHash:       parent.ExecutionHash,
		ParentBlockRoot:       parent.BlockRoot,
		BlockHash:             assembled.Eth1Block.BlockHash,
		PrevRandao:            assembled.Eth1Block.PrevRandao,
		FeeRecipient:          assembled.Eth1Block.FeeRecipient,
		GasLimit:              assembled.Eth1Block.GasLimit,
		BuilderIndex:          bidBuilderIndex,
		Value:                 new(big.Int).Div(bidAmount, big.NewInt(weiPerGwei)).Uint64(), // consensus uses Gwei
		ExecutionPayment:      0, // per spec: builder pays nothing in ePBS
		BlobKzgCommitments:    *blobCommitments,
		ExecutionRequestsRoot: execReqsRoot,
	}

	// Sign the bid
	signedBid, err := l.manager.SignBid(ctx, bid)
	if err != nil {
		return fmt.Errorf("epbs/loop: sign bid: %w", err)
	}

	// Submit the bid
	if err := l.submitter.SubmitBid(ctx, signedBid); err != nil {
		return fmt.Errorf("epbs/loop: submit bid: %w", err)
	}

	// Store for reveal
	l.mu.Lock()
	l.pendingPayloads[pendingPayloadKey{slot: slot, parentBlockHash: parent.ExecutionHash, parentBlockRoot: parent.BlockRoot}] = &pendingPayload{
		slot:      slot,
		payloadId: usedPayloadId,
		assembled: assembled,
		execReqs:  execReqs,
		parent:    parent,
	}
	l.mu.Unlock()

	log.Info("ePBS builder: bid submitted",
		"slot", slot,
		"blockHash", bid.BlockHash,
		"value", blockValue,
		"bidAmount", bidAmount,
	)

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
func (l *BuilderLoop) OnBidWon(ctx context.Context, slot uint64, builderIndex uint64, parentHash common.Hash, parentBlockRoot common.Hash, beaconBlockRoot common.Hash) error {
	// Only reveal if the winning bid belongs to us.
	ourIndex, resolved := l.manager.BuilderIndex()
	if !resolved || builderIndex != ourIndex {
		log.Debug("ePBS builder: bid won by another builder or index unresolved, ignoring",
			"slot", slot, "winnerIndex", builderIndex, "ourIndex", ourIndex, "resolved", resolved)
		return nil
	}

	key := pendingPayloadKey{slot: slot, parentBlockHash: parentHash, parentBlockRoot: parentBlockRoot}
	l.mu.Lock()
	pending, ok := l.pendingPayloads[key]
	if ok {
		delete(l.pendingPayloads, key)
	}
	l.mu.Unlock()

	if !ok {
		return fmt.Errorf("epbs/loop: no pending payload for slot %d parentHash %s parentBlockRoot %s", slot, parentHash, parentBlockRoot)
	}

	// Construct the envelope using the NewExecutionPayloadEnvelope constructor
	envelope := cltypes.NewExecutionPayloadEnvelope(l.beaconCfg)
	envelope.Payload = pending.assembled.Eth1Block
	envelope.ExecutionRequests = pending.execReqs
	envelope.BuilderIndex = ourIndex
	envelope.BeaconBlockRoot = beaconBlockRoot

	// Sign the envelope
	signedEnvelope, err := l.manager.SignEnvelope(ctx, envelope, slot)
	if err != nil {
		return fmt.Errorf("epbs/loop: sign envelope: %w", err)
	}

	// Broadcast
	if err := l.submitter.BroadcastPayload(ctx, signedEnvelope); err != nil {
		return fmt.Errorf("epbs/loop: broadcast payload: %w", err)
	}

	log.Info("ePBS builder: payload revealed",
		"slot", slot,
		"blockHash", pending.assembled.Eth1Block.BlockHash,
		"beaconBlockRoot", beaconBlockRoot,
	)

	return nil
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
	}
}

// buildParamsFromPrefs constructs EL builder.Parameters incorporating proposer preferences.
func (l *BuilderLoop) buildParamsFromPrefs(sc SlotContext, prefs *cltypes.SignedProposerPreferences) *builder.Parameters {
	params := l.buildParams(sc)
	if prefs != nil && prefs.Message != nil {
		params.SuggestedFeeRecipient = prefs.Message.FeeRecipient
		if prefs.Message.GasLimit > 0 {
			gl := prefs.Message.GasLimit
			params.GasLimit = &gl
		}
	}
	return params
}

// speculativeMatchesPrefs checks whether a speculative build result is compatible
// with the proposer preferences. The speculative build was started before prefs
// arrived (OnNewHead uses FeeRecipient=0x0), so if the proposer requires a
// specific fee recipient, the speculative payload's coinbase must match.
func speculativeMatchesPrefs(result *eladapter.AssembledPayload, prefs *cltypes.SignedProposerPreferences) bool {
	if prefs == nil || prefs.Message == nil {
		return true // no constraints
	}
	// FeeRecipient is the critical field: the EL sets coinbase from
	// SuggestedFeeRecipient passed in Parameters. If the speculative build
	// used 0x0 and the proposer wants a real address, they won't match.
	wantRecipient := prefs.Message.FeeRecipient
	if wantRecipient != (common.Address{}) && result.Eth1Block.FeeRecipient != wantRecipient {
		return false
	}
	// GasLimit: gossip validation requires bid.gas_limit == prefs.gas_limit
	// (execution_payload_bid_service.go) and the state transition enforces the
	// envelope payload's gas_limit matches (operations.go). If the speculative
	// build used a different gas limit, the bid/reveal will be rejected.
	if prefs.Message.GasLimit > 0 && result.Eth1Block.GasLimit != prefs.Message.GasLimit {
		return false
	}
	return true
}

// decodeAndHashRequests decodes a RequestsBundle into ExecutionRequests and computes
// the hash_tree_root. Returns nil ExecutionRequests and zero hash if bundle is nil.
func (l *BuilderLoop) decodeAndHashRequests(bundle *typesproto.RequestsBundle) (*cltypes.ExecutionRequests, common.Hash, error) {
	execReqs := cltypes.NewExecutionRequests(l.beaconCfg)

	if bundle == nil || len(bundle.GetRequests()) == 0 {
		root, err := execReqs.HashSSZ()
		if err != nil {
			return nil, common.Hash{}, fmt.Errorf("hash empty requests: %w", err)
		}
		return execReqs, common.Hash(root), nil
	}

	stateVersion := int(clparams.GloasVersion)
	for _, request := range bundle.GetRequests() {
		if len(request) == 0 {
			continue
		}
		rType := request[0]
		requestData := request[1:]
		switch rType {
		case depositRequestType:
			if err := execReqs.Deposits.DecodeSSZ(requestData, stateVersion); err != nil {
				return nil, common.Hash{}, fmt.Errorf("decode deposit request: %w", err)
			}
		case withdrawalRequestType:
			if err := execReqs.Withdrawals.DecodeSSZ(requestData, stateVersion); err != nil {
				return nil, common.Hash{}, fmt.Errorf("decode withdrawal request: %w", err)
			}
		case consolidationRequestType:
			if err := execReqs.Consolidations.DecodeSSZ(requestData, stateVersion); err != nil {
				return nil, common.Hash{}, fmt.Errorf("decode consolidation request: %w", err)
			}
		}
	}

	root, err := execReqs.HashSSZ()
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("hash execution requests: %w", err)
	}

	return execReqs, common.Hash(root), nil
}
