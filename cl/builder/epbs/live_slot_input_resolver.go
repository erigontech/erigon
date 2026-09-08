// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

var (
	ErrSlotInputUnavailable = errors.New("slot input unavailable")
	ErrSlotInputStale       = errors.New("slot input stale")
)

type LiveSlotClock interface {
	GetCurrentSlot() uint64
	GenesisValidatorsRoot() common.Hash
}

type LiveHeadStateSource interface {
	ViewHeadStateWithIdentity(synced_data.ViewHeadStateWithIdentityFn) error
}

type LiveForkchoiceSource interface {
	GetHeadNode() (forkchoice.ForkChoiceNode, error)
	HasEnvelope(common.Hash) bool
	ShouldBuildOnFull(forkchoice.ForkChoiceNode, uint64) bool
	ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error)
	GetExecutionPayloadGasLimit(common.Hash) (uint64, bool)
	GetRecentExecutionPayloadStatus(common.Hash) (execution_client.PayloadStatus, bool)
}

type LiveSlotInputResolver struct {
	beaconCfg  *clparams.BeaconChainConfig
	signer     Signer
	clock      LiveSlotClock
	head       LiveHeadStateSource
	forkchoice LiveForkchoiceSource
}

func NewLiveSlotInputResolver(
	beaconCfg *clparams.BeaconChainConfig,
	signer Signer,
	clock LiveSlotClock,
	head LiveHeadStateSource,
	forkchoiceSource LiveForkchoiceSource,
) *LiveSlotInputResolver {
	return &LiveSlotInputResolver{
		beaconCfg:  beaconCfg,
		signer:     signer,
		clock:      clock,
		head:       head,
		forkchoice: forkchoiceSource,
	}
}

func (r *LiveSlotInputResolver) Resolve(
	ctx context.Context,
	preferences *cltypes.SignedProposerPreferences,
) (SlotInput, error) {
	if ctx == nil {
		return SlotInput{}, errors.New("epbs/live resolver: nil context")
	}
	if err := ctx.Err(); err != nil {
		return SlotInput{}, err
	}
	if r == nil || r.beaconCfg == nil || isNilDependency(r.signer) || isNilDependency(r.clock) ||
		isNilDependency(r.head) || isNilDependency(r.forkchoice) {
		return SlotInput{}, errors.New("epbs/live resolver: missing dependency")
	}
	if preferences == nil || preferences.Message == nil {
		return SlotInput{}, errors.New("epbs/live resolver: missing validated proposer preferences")
	}
	if r.beaconCfg.SlotsPerEpoch == 0 {
		return SlotInput{}, errors.New("epbs/live resolver: slots per epoch must be positive")
	}
	if err := r.validateTargetSlot(preferences.Message.ProposalSlot); err != nil {
		return SlotInput{}, err
	}
	input, err := r.resolveCurrent(ctx, preferences)
	if err != nil {
		return SlotInput{}, err
	}
	if err := r.ValidateCurrent(ctx, input); err != nil {
		return SlotInput{}, err
	}
	return input, nil
}

func (r *LiveSlotInputResolver) ValidateCurrent(ctx context.Context, input SlotInput) error {
	if ctx == nil {
		return errors.New("epbs/live resolver: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.beaconCfg == nil || r.beaconCfg.SlotsPerEpoch == 0 || isNilDependency(r.clock) || isNilDependency(r.head) ||
		isNilDependency(r.forkchoice) || input.ValidatedPreferences == nil || input.ValidatedPreferences.Message == nil {
		return ErrSlotInputStale
	}
	if err := r.validateTargetSlot(input.Slot); err != nil {
		return fmt.Errorf("%w: %w", ErrSlotInputStale, err)
	}
	preferenceRoot, err := input.ValidatedPreferences.HashSSZ()
	if err != nil || common.Hash(preferenceRoot) != input.freshness.preferenceRoot ||
		input.ValidatedPreferences.Message.ProposalSlot != input.Slot ||
		input.ValidatedPreferences.Message.DependentRoot != input.DependentRoot {
		return ErrSlotInputStale
	}

	var header cltypes.BeaconBlockHeader
	var parentBid cltypes.ExecutionPayloadBid
	if err := r.head.ViewHeadStateWithIdentity(func(current *state.CachingBeaconState, root common.Hash, slot uint64) error {
		if current == nil || root != input.ParentBlockRoot || slot != input.freshness.headStateSlot ||
			current.Slot() != slot || current.GenesisValidatorsRoot() != input.GenesisValidatorsRoot ||
			current.GenesisValidatorsRoot() != r.clock.GenesisValidatorsRoot() {
			return ErrSlotInputStale
		}
		header = current.LatestBlockHeader()
		if header.Slot != input.freshness.headBlockSlot || input.Slot <= header.Slot || input.Slot < current.Slot() {
			return ErrSlotInputStale
		}
		bid := current.GetLatestExecutionPayloadBid()
		if bid == nil {
			return ErrSlotInputStale
		}
		parentBid = *bid
		return nil
	}); err != nil {
		return fmt.Errorf("%w: head state: %w", ErrSlotInputStale, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	headNode, err := r.forkchoice.GetHeadNode()
	if err != nil || headNode.Root != input.ParentBlockRoot || headNode.PayloadStatus > cltypes.PayloadStatusPending {
		return ErrSlotInputStale
	}
	preGloasParent := header.Slot/r.beaconCfg.SlotsPerEpoch < r.beaconCfg.GloasForkEpoch
	hasEnvelope := r.forkchoice.HasEnvelope(input.ParentBlockRoot)
	buildOnFull := !preGloasParent && headNode.PayloadStatus == cltypes.PayloadStatusFull && hasEnvelope &&
		r.forkchoice.ShouldBuildOnFull(headNode, input.Slot)
	if buildOnFull != input.freshness.buildOnFull {
		return ErrSlotInputStale
	}
	parentHash := parentBid.ParentBlockHash
	if preGloasParent || buildOnFull {
		parentHash = parentBid.BlockHash
	}
	if parentHash == (common.Hash{}) || parentHash != input.ParentBlockHash {
		return ErrSlotInputStale
	}
	status, ok := r.forkchoice.GetRecentExecutionPayloadStatus(parentHash)
	if !ok || status == execution_client.PayloadStatusNone || status == execution_client.PayloadStatusInvalidated {
		return ErrSlotInputStale
	}
	gasLimit, ok := r.forkchoice.GetExecutionPayloadGasLimit(parentHash)
	if !ok || gasLimit == 0 || gasLimit != input.ParentGasLimit {
		return ErrSlotInputStale
	}
	return ctx.Err()
}

func (r *LiveSlotInputResolver) validateTargetSlot(targetSlot uint64) error {
	currentSlot := r.clock.GetCurrentSlot()
	if targetSlot == math.MaxUint64 || targetSlot < currentSlot || targetSlot-currentSlot > 1 {
		return fmt.Errorf("%w: slot %d is not current or next", ErrSlotInputUnavailable, targetSlot)
	}
	if targetSlot <= r.beaconCfg.GenesisSlot {
		return fmt.Errorf("%w: slot %d is not after genesis", ErrSlotInputUnavailable, targetSlot)
	}
	if r.beaconCfg.GetCurrentStateVersion(targetSlot/r.beaconCfg.SlotsPerEpoch) < clparams.GloasVersion {
		return fmt.Errorf("%w: slot %d is before gloas", ErrSlotInputUnavailable, targetSlot)
	}
	return nil
}

func (r *LiveSlotInputResolver) resolveCurrent(
	ctx context.Context,
	preferences *cltypes.SignedProposerPreferences,
) (SlotInput, error) {
	if preferences == nil || preferences.Message == nil || preferences.Signature == (common.Bytes96{}) {
		return SlotInput{}, errors.New("epbs/live resolver: invalid validated proposer preferences")
	}
	if err := r.validateTargetSlot(preferences.Message.ProposalSlot); err != nil {
		return SlotInput{}, err
	}
	targetSlot := preferences.Message.ProposalSlot
	if err := ctx.Err(); err != nil {
		return SlotInput{}, err
	}
	var headState *state.CachingBeaconState
	var headRoot common.Hash
	var headStateSlot uint64
	if err := r.head.ViewHeadStateWithIdentity(func(current *state.CachingBeaconState, root common.Hash, slot uint64) error {
		if current == nil {
			return errors.New("head state is nil")
		}
		var err error
		headState, err = current.Copy()
		if err == nil {
			headRoot, headStateSlot = root, slot
		}
		return err
	}); err != nil {
		return SlotInput{}, fmt.Errorf("%w: head state: %w", ErrSlotInputUnavailable, err)
	}
	if headState == nil || headRoot == (common.Hash{}) || headState.Slot() != headStateSlot {
		return SlotInput{}, fmt.Errorf("%w: inconsistent head state identity", ErrSlotInputUnavailable)
	}
	header := headState.LatestBlockHeader()
	if targetSlot <= header.Slot || targetSlot < headState.Slot() {
		return SlotInput{}, fmt.Errorf("%w: target slot is not after the head", ErrSlotInputUnavailable)
	}
	headNode, err := r.forkchoice.GetHeadNode()
	if err != nil {
		return SlotInput{}, fmt.Errorf("%w: forkchoice head: %w", ErrSlotInputUnavailable, err)
	}
	if headNode.Root != headRoot || headNode.PayloadStatus > cltypes.PayloadStatusPending {
		return SlotInput{}, fmt.Errorf("%w: state and forkchoice heads differ", ErrSlotInputUnavailable)
	}
	parentEpoch := state.Epoch(headState)
	proposalEpoch := targetSlot / r.beaconCfg.SlotsPerEpoch
	if proposalEpoch > parentEpoch && proposalEpoch-parentEpoch > r.beaconCfg.MinSeedLookahead {
		return SlotInput{}, fmt.Errorf("%w: target slot is past the head's proposer lookahead", ErrSlotInputUnavailable)
	}
	parentRandao := headState.GetRandaoMixes(state.Epoch(headState))
	if err := ctx.Err(); err != nil {
		return SlotInput{}, err
	}
	if headState.Slot() != targetSlot {
		if err := transition.DefaultMachine.ProcessSlots(headState, targetSlot); err != nil {
			return SlotInput{}, fmt.Errorf("%w: advance head state: %w", ErrSlotInputUnavailable, err)
		}
	}
	if headState.Version() < clparams.GloasVersion {
		return SlotInput{}, fmt.Errorf("%w: advanced state is before gloas", ErrSlotInputUnavailable)
	}
	proposerIndex, err := headState.GetBeaconProposerIndexForSlot(targetSlot)
	if err != nil {
		return SlotInput{}, fmt.Errorf("%w: proposer index: %w", ErrSlotInputUnavailable, err)
	}
	dependentRoot, err := state.GetProposerDependentRoot(headState, targetSlot/r.beaconCfg.SlotsPerEpoch)
	if err != nil {
		return SlotInput{}, fmt.Errorf("%w: proposer dependent root: %w", ErrSlotInputUnavailable, err)
	}
	if dependentRoot == (common.Hash{}) {
		return SlotInput{}, fmt.Errorf("%w: proposer dependent root is zero", ErrSlotInputUnavailable)
	}
	if preferences.Message.DependentRoot != dependentRoot || preferences.Message.ValidatorIndex != proposerIndex {
		return SlotInput{}, fmt.Errorf("%w: proposer preferences do not match the head", ErrSlotInputUnavailable)
	}
	preferenceRoot, err := preferences.HashSSZ()
	if err != nil {
		return SlotInput{}, fmt.Errorf("epbs/live resolver: hash proposer preferences: %w", err)
	}
	parentBid := headState.GetLatestExecutionPayloadBid()
	if parentBid == nil {
		return SlotInput{}, fmt.Errorf("%w: latest execution payload bid is unavailable", ErrSlotInputUnavailable)
	}
	preGloasParent := header.Slot/r.beaconCfg.SlotsPerEpoch < r.beaconCfg.GloasForkEpoch
	buildOnFull := !preGloasParent && headNode.PayloadStatus == cltypes.PayloadStatusFull &&
		r.forkchoice.HasEnvelope(headRoot) && r.forkchoice.ShouldBuildOnFull(headNode, targetSlot)
	parentHash := parentBid.ParentBlockHash
	if preGloasParent || buildOnFull {
		parentHash = parentBid.BlockHash
	}
	if parentHash == (common.Hash{}) {
		return SlotInput{}, fmt.Errorf("%w: execution parent is unavailable", ErrSlotInputUnavailable)
	}
	parentStatus, ok := r.forkchoice.GetRecentExecutionPayloadStatus(parentHash)
	if !ok || parentStatus == execution_client.PayloadStatusNone || parentStatus == execution_client.PayloadStatusInvalidated {
		return SlotInput{}, fmt.Errorf("%w: execution parent status is unavailable", ErrSlotInputUnavailable)
	}
	parentGasLimit, ok := r.forkchoice.GetExecutionPayloadGasLimit(parentHash)
	if !ok || parentGasLimit == 0 {
		return SlotInput{}, fmt.Errorf("%w: execution parent gas limit is unavailable", ErrSlotInputUnavailable)
	}
	builderIndex, builder, available, err := r.resolveBuilder(headState)
	if err != nil {
		return SlotInput{}, err
	}
	withdrawals, postParentState, err := r.resolveWithdrawals(ctx, headState, headRoot, &header, parentBid, targetSlot, preGloasParent, buildOnFull)
	if err != nil {
		return SlotInput{}, err
	}
	if postParentState != nil {
		postIndex, postBuilder, postAvailable, err := r.resolveBuilder(postParentState)
		if err != nil {
			return SlotInput{}, fmt.Errorf("%w: builder invalid after full parent: %w", ErrSlotInputUnavailable, err)
		}
		if postIndex != builderIndex || postBuilder.Pubkey != builder.Pubkey {
			return SlotInput{}, fmt.Errorf("%w: builder identity changed after full parent", ErrSlotInputUnavailable)
		}
		available = min(available, postAvailable)
	}
	genesisRoot := headState.GenesisValidatorsRoot()
	if genesisRoot == (common.Hash{}) || genesisRoot != r.clock.GenesisValidatorsRoot() {
		return SlotInput{}, fmt.Errorf("%w: genesis validators root mismatch", ErrSlotInputUnavailable)
	}
	timestamp, ok := safeLiveSlotTimestamp(headState.GenesisTime(), r.beaconCfg.GenesisSlot, targetSlot, r.beaconCfg.SecondsPerSlot)
	if !ok {
		return SlotInput{}, fmt.Errorf("%w: slot timestamp overflow", ErrSlotInputUnavailable)
	}
	return SlotInput{
		ValidatedPreferences: preferences.Clone().(*cltypes.SignedProposerPreferences),
		Slot:                 targetSlot, DependentRoot: dependentRoot, ParentBlockRoot: headRoot,
		ParentBlockHash: parentHash, ParentGasLimit: parentGasLimit, PrevRandao: parentRandao,
		Timestamp: timestamp, Withdrawals: withdrawals,
		BuilderIndex: builderIndex, BuilderStatusIndex: builderIndex, BuilderStatusSlot: targetSlot,
		BuilderStatusParentRoot: headRoot, BuilderPubkey: builder.Pubkey,
		GenesisValidatorsRoot: genesisRoot, BuilderActive: true, AvailableBidValueGwei: available,
		freshness: slotInputFreshnessToken{
			preferenceRoot: common.Hash(preferenceRoot),
			headStateSlot:  headStateSlot,
			headBlockSlot:  header.Slot,
			buildOnFull:    buildOnFull,
		},
	}, nil
}

func (r *LiveSlotInputResolver) resolveBuilder(headState *state.CachingBeaconState) (uint64, *cltypes.Builder, uint64, error) {
	builders := headState.GetBuilders()
	if builders == nil {
		return 0, nil, 0, fmt.Errorf("%w: builder registry is unavailable", ErrSlotInputUnavailable)
	}
	pubkey := r.signer.Pubkey()
	if pubkey == (common.Bytes48{}) {
		return 0, nil, 0, errors.New("epbs/live resolver: signer pubkey is zero")
	}
	match := -1
	for i := 0; i < builders.Len(); i++ {
		builder := builders.Get(i)
		if builder == nil {
			return 0, nil, 0, fmt.Errorf("%w: nil builder at index %d", ErrSlotInputUnavailable, i)
		}
		if builder.Pubkey == pubkey {
			if match >= 0 {
				return 0, nil, 0, fmt.Errorf("%w: duplicate builder pubkey", ErrSlotInputUnavailable)
			}
			match = i
		}
	}
	if match < 0 {
		return 0, nil, 0, fmt.Errorf("%w: builder is not registered", ErrSlotInputUnavailable)
	}
	builder := builders.Get(match)
	if builder.Version != r.beaconCfg.PayloadBuilderVersion || !state.IsActiveBuilder(headState, uint64(match)) {
		return 0, nil, 0, fmt.Errorf("%w: builder is not active and supported", ErrSlotInputUnavailable)
	}
	pending, err := checkedPendingBuilderBalance(headState, uint64(match))
	if err != nil {
		return 0, nil, 0, err
	}
	if pending > math.MaxUint64-r.beaconCfg.MinDepositAmount {
		return 0, nil, 0, errors.New("epbs/live resolver: builder collateral overflow")
	}
	reserved := r.beaconCfg.MinDepositAmount + pending
	if builder.Balance <= reserved {
		return 0, nil, 0, fmt.Errorf("%w: builder has no available collateral", ErrSlotInputUnavailable)
	}
	return uint64(match), builder, builder.Balance - reserved, nil
}

func checkedPendingBuilderBalance(headState *state.CachingBeaconState, builderIndex uint64) (uint64, error) {
	var total uint64
	add := func(amount uint64) error {
		if amount > math.MaxUint64-total {
			return errors.New("epbs/live resolver: pending builder collateral overflow")
		}
		total += amount
		return nil
	}
	withdrawals := headState.GetBuilderPendingWithdrawals()
	if withdrawals == nil {
		return 0, errors.New("epbs/live resolver: builder pending withdrawals are unavailable")
	}
	for i := 0; i < withdrawals.Len(); i++ {
		withdrawal := withdrawals.Get(i)
		if withdrawal == nil {
			return 0, fmt.Errorf("epbs/live resolver: nil builder pending withdrawal at index %d", i)
		}
		if withdrawal.BuilderIndex == builderIndex {
			if err := add(withdrawal.Amount); err != nil {
				return 0, err
			}
		}
	}
	payments := headState.GetBuilderPendingPayments()
	if payments == nil {
		return 0, errors.New("epbs/live resolver: builder pending payments are unavailable")
	}
	for i := 0; i < payments.Length(); i++ {
		payment := payments.Get(i)
		if payment == nil || payment.Withdrawal == nil {
			return 0, fmt.Errorf("epbs/live resolver: incomplete builder pending payment at index %d", i)
		}
		if payment.Withdrawal.BuilderIndex == builderIndex {
			if err := add(payment.Withdrawal.Amount); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func (r *LiveSlotInputResolver) resolveWithdrawals(
	ctx context.Context,
	headState *state.CachingBeaconState,
	headRoot common.Hash,
	header *cltypes.BeaconBlockHeader,
	parentBid *cltypes.ExecutionPayloadBid,
	targetSlot uint64,
	preGloasParent bool,
	buildOnFull bool,
) ([]*types.Withdrawal, *state.CachingBeaconState, error) {
	if preGloasParent {
		expected, err := state.GetExpectedWithdrawals(headState, targetSlot/r.beaconCfg.SlotsPerEpoch)
		if err != nil {
			return nil, nil, err
		}
		withdrawals, err := ownedExecutionWithdrawals(expected.Withdrawals)
		return withdrawals, nil, err
	}
	if !buildOnFull {
		cached := headState.GetPayloadExpectedWithdrawals()
		if cached == nil {
			return nil, nil, errors.New("epbs/live resolver: payload expected withdrawals are unavailable")
		}
		consensus := make([]*cltypes.Withdrawal, cached.Len())
		for i := range consensus {
			consensus[i] = cached.Get(i)
		}
		withdrawals, err := ownedExecutionWithdrawals(consensus)
		return withdrawals, nil, err
	}
	if !r.forkchoice.HasEnvelope(headRoot) {
		return nil, nil, fmt.Errorf("%w: full parent envelope is unavailable", ErrSlotInputUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	envelope, err := r.forkchoice.ReadEnvelopeFromDisk(headRoot)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: read full parent envelope: %w", ErrSlotInputUnavailable, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	requests, err := validateLiveParentEnvelope(envelope, headRoot, header.ParentRoot, parentBid)
	if err != nil {
		return nil, nil, err
	}
	withdrawalState, err := headState.Copy()
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := (&eth2.Impl{}).ApplyParentExecutionPayload(withdrawalState, requests); err != nil {
		return nil, nil, fmt.Errorf("epbs/live resolver: apply full parent payload: %w", err)
	}
	expected, err := state.GetExpectedWithdrawals(withdrawalState, targetSlot/r.beaconCfg.SlotsPerEpoch)
	if err != nil {
		return nil, nil, err
	}
	withdrawals, err := ownedExecutionWithdrawals(expected.Withdrawals)
	if err != nil {
		return nil, nil, err
	}
	if err := (&eth2.Impl{}).ProcessWithdrawals(withdrawalState, nil); err != nil {
		return nil, nil, fmt.Errorf("epbs/live resolver: apply full parent withdrawals: %w", err)
	}
	return withdrawals, withdrawalState, nil
}

func validateLiveParentEnvelope(
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	headRoot common.Hash,
	headParentRoot common.Hash,
	parentBid *cltypes.ExecutionPayloadBid,
) (*cltypes.ExecutionRequests, error) {
	if envelope == nil || envelope.Message == nil || envelope.Message.Payload == nil || envelope.Message.ExecutionRequests == nil {
		return nil, errors.New("epbs/live resolver: full parent envelope is incomplete")
	}
	message := envelope.Message
	requests := message.ExecutionRequests
	if requests.Deposits == nil || requests.Withdrawals == nil || requests.Consolidations == nil ||
		requests.BuilderDeposits == nil || requests.BuilderExits == nil {
		return nil, errors.New("epbs/live resolver: full parent execution requests are incomplete")
	}
	if message.BeaconBlockRoot != headRoot || message.ParentBeaconBlockRoot != headParentRoot ||
		message.BuilderIndex != parentBid.BuilderIndex || message.Payload.BlockHash != parentBid.BlockHash ||
		message.Payload.ParentHash != parentBid.ParentBlockHash {
		return nil, errors.New("epbs/live resolver: full parent envelope identity mismatch")
	}
	root, err := requests.HashSSZ()
	if err != nil {
		return nil, err
	}
	if root != parentBid.ExecutionRequestsRoot {
		return nil, errors.New("epbs/live resolver: full parent execution requests root mismatch")
	}
	return requests, nil
}

func ownedExecutionWithdrawals(source []*cltypes.Withdrawal) ([]*types.Withdrawal, error) {
	if source == nil {
		return nil, errors.New("epbs/live resolver: expected withdrawals are nil")
	}
	for i, withdrawal := range source {
		if withdrawal == nil {
			return nil, fmt.Errorf("epbs/live resolver: nil expected withdrawal at index %d", i)
		}
	}
	return cltypes.ConvertConsensusWithdrawalsToExecutionWithdrawals(source), nil
}

func safeLiveSlotTimestamp(genesisTime, genesisSlot, slot, secondsPerSlot uint64) (uint64, bool) {
	if secondsPerSlot == 0 || slot < genesisSlot {
		return 0, false
	}
	delta := slot - genesisSlot
	if delta > math.MaxUint64/secondsPerSlot {
		return 0, false
	}
	offset := delta * secondsPerSlot
	if genesisTime > math.MaxUint64-offset {
		return 0, false
	}
	return genesisTime + offset, true
}
