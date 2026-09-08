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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

type resolverHeadSource struct {
	calls        int
	state        *state.CachingBeaconState
	root         common.Hash
	identitySlot uint64
	err          error
}

func (s *resolverHeadSource) ViewHeadStateWithIdentity(view synced_data.ViewHeadStateWithIdentityFn) error {
	s.calls++
	if s.err != nil {
		return s.err
	}
	if s.state == nil {
		return errors.New("unexpected head access")
	}
	return view(s.state, s.root, s.identitySlot)
}

type resolverForkchoice struct {
	headNode       forkchoice.ForkChoiceNode
	headErr        error
	envelope       *cltypes.SignedExecutionPayloadEnvelope
	envelopeErr    error
	hasEnvelope    bool
	buildOnFull    bool
	gasLimits      map[common.Hash]uint64
	recentStatuses map[common.Hash]execution_client.PayloadStatus
	readEnvelope   func() (*cltypes.SignedExecutionPayloadEnvelope, error)
}

func (f *resolverForkchoice) GetHeadNode() (forkchoice.ForkChoiceNode, error) {
	return f.headNode, f.headErr
}
func (f *resolverForkchoice) HasEnvelope(common.Hash) bool { return f.hasEnvelope }
func (f *resolverForkchoice) ShouldBuildOnFull(forkchoice.ForkChoiceNode, uint64) bool {
	return f.buildOnFull
}
func (f *resolverForkchoice) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if f.readEnvelope != nil {
		return f.readEnvelope()
	}
	return f.envelope, f.envelopeErr
}
func (f *resolverForkchoice) GetExecutionPayloadGasLimit(hash common.Hash) (uint64, bool) {
	gasLimit, ok := f.gasLimits[hash]
	return gasLimit, ok
}
func (f *resolverForkchoice) GetRecentExecutionPayloadStatus(hash common.Hash) (execution_client.PayloadStatus, bool) {
	status, ok := f.recentStatuses[hash]
	return status, ok
}

func TestLiveSlotInputResolverRejectsPastAndFarFutureBeforeHeadAccess(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(64)).Times(2)
	head := new(resolverHeadSource)
	resolver := NewLiveSlotInputResolver(&cfg, new(coordinatorSigner), clock, head, new(resolverForkchoice))

	for _, slot := range []uint64{63, 66} {
		preferences := validCoordinatorSlotInput(cfg).ValidatedPreferences.Clone().(*cltypes.SignedProposerPreferences)
		preferences.Message.ProposalSlot = slot
		_, err := resolver.Resolve(t.Context(), preferences)
		require.Error(t, err)
	}
	require.Zero(t, head.calls)
}

func TestLiveSlotInputResolverValidateCurrentRejectsZeroSlotsPerEpochWithoutPanic(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(cfg)
	cfg.SlotsPerEpoch = 0
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	head := new(resolverHeadSource)
	resolver := NewLiveSlotInputResolver(&cfg, new(coordinatorSigner), clock, head, new(resolverForkchoice))
	var err error

	require.NotPanics(t, func() {
		err = resolver.ValidateCurrent(t.Context(), input)
	})
	require.ErrorIs(t, err, ErrSlotInputStale)
	require.Zero(t, head.calls)
}

func TestLiveSlotInputResolverRejectsTargetPastHeadProposerLookahead(t *testing.T) {
	cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
	require.NoError(t, headState.SetSlot(4))
	headState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 3})
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).Times(2)
	head := &resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()}
	fc := &resolverForkchoice{headNode: forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty}}
	resolver := NewLiveSlotInputResolver(&cfg, new(coordinatorSigner), clock, head, fc)

	_, err := resolver.Resolve(t.Context(), preferences)
	require.ErrorContains(t, err, "past the head's proposer lookahead")
	require.Equal(t, 1, head.calls)
}

func TestLiveSlotInputResolverAcceptsExactHeadProposerLookahead(t *testing.T) {
	cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
	validatorCount := headState.ValidatorLength()
	headState.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, validatorCount))
	headState.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, validatorCount))
	headState.SetInactivityScores(make([]uint64, validatorCount))
	require.NoError(t, headState.SetSlot(11))
	headState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 11})
	require.Equal(t, cfg.MinSeedLookahead, preferences.Message.ProposalSlot/cfg.SlotsPerEpoch-state.Epoch(headState))
	advanced, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, preferences.Message.ProposalSlot))
	preferences.Message.ValidatorIndex, err = advanced.GetBeaconProposerIndexForSlot(preferences.Message.ProposalSlot)
	require.NoError(t, err)
	preferences.Message.DependentRoot, err = state.GetProposerDependentRoot(advanced, preferences.Message.ProposalSlot/cfg.SlotsPerEpoch)
	require.NoError(t, err)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	fc := &resolverForkchoice{
		headNode:  forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty},
		gasLimits: map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
	}
	resolver := NewLiveSlotInputResolver(
		&cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)

	input, err := resolver.Resolve(t.Context(), preferences)
	require.NoError(t, err)
	require.Equal(t, preferences.Message.ProposalSlot, input.Slot)
}

func TestLiveSlotInputResolverResolvesEmptyHeadWithOwnedInput(t *testing.T) {
	cfg, headState, preferences, headRoot, parentHash, randao := liveResolverFixture(t)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	head := &resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()}
	fc := &resolverForkchoice{
		headNode:  forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty},
		gasLimits: map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
	}
	resolver := NewLiveSlotInputResolver(&cfg, new(coordinatorSigner), clock, head, fc)

	input, err := resolver.Resolve(t.Context(), preferences)
	require.NoError(t, err)
	require.Equal(t, preferences.Message.ProposalSlot, input.Slot)
	require.Equal(t, preferences.Message.DependentRoot, input.DependentRoot)
	require.Equal(t, headRoot, input.ParentBlockRoot)
	require.Equal(t, parentHash, input.ParentBlockHash)
	require.Equal(t, uint64(30_000_000), input.ParentGasLimit)
	require.Equal(t, randao, input.PrevRandao)
	require.Equal(t, uint64(1_000)+input.Slot*cfg.SecondsPerSlot, input.Timestamp)
	require.Equal(t, uint64(0), input.BuilderIndex)
	require.Equal(t, uint64(2_000), input.AvailableBidValueGwei)
	require.True(t, input.BuilderActive)
	require.Len(t, input.Withdrawals, 1)
	require.Equal(t, uint64(11), input.Withdrawals[0].Amount)
	require.NotSame(t, preferences, input.ValidatedPreferences)

	preferences.Message.FeeRecipient[0] ^= 1
	headState.GetPayloadExpectedWithdrawals().Get(0).Amount++
	require.NotEqual(t, preferences.Message.FeeRecipient, input.ValidatedPreferences.Message.FeeRecipient)
	require.Equal(t, uint64(11), input.Withdrawals[0].Amount)
}

func TestLiveSlotInputResolverResolvesFullHeadFromEnvelope(t *testing.T) {
	cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
	parentBid := headState.GetLatestExecutionPayloadBid()
	envelope := liveResolverEnvelope(t, &cfg, headState, headRoot)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	fc := &resolverForkchoice{
		headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull},
		envelope:    envelope,
		hasEnvelope: true,
		buildOnFull: true,
		gasLimits:   map[common.Hash]uint64{parentBid.BlockHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentBid.BlockHash: execution_client.PayloadStatusValidated,
		},
	}
	resolver := NewLiveSlotInputResolver(
		&cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)

	input, err := resolver.Resolve(t.Context(), preferences)
	require.NoError(t, err)
	require.Equal(t, parentBid.BlockHash, input.ParentBlockHash)
	require.NotNil(t, input.Withdrawals)
	require.Equal(t, uint64(2_000), input.AvailableBidValueGwei)
}

func TestLiveSlotInputResolverRejectsBuilderExitedByFullParent(t *testing.T) {
	cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
	builder := headState.GetBuilders().Get(0)
	builder.ExecutionAddress = common.HexToAddress("0x71")
	envelope := liveResolverEnvelope(t, &cfg, headState, headRoot)
	envelope.Message.ExecutionRequests.BuilderExits.Append(&solid.BuilderExitRequest{
		SourceAddress: builder.ExecutionAddress,
		PubKey:        builder.Pubkey,
	})
	requestsRoot, err := envelope.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	headState.GetLatestExecutionPayloadBid().ExecutionRequestsRoot = requestsRoot
	parentHash := headState.GetLatestExecutionPayloadBid().BlockHash
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	fc := &resolverForkchoice{
		headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull},
		envelope:    envelope,
		hasEnvelope: true,
		buildOnFull: true,
		gasLimits:   map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
	}
	resolver := NewLiveSlotInputResolver(
		&cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)

	_, err = resolver.Resolve(t.Context(), preferences)
	require.ErrorContains(t, err, "builder invalid after full parent")
}

func TestLiveSlotInputResolverStopsAfterEnvelopeReadCancellation(t *testing.T) {
	cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
	parentHash := headState.GetLatestExecutionPayloadBid().BlockHash
	ctx, cancel := context.WithCancel(t.Context())
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	fc := &resolverForkchoice{
		headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull},
		hasEnvelope: true,
		buildOnFull: true,
		gasLimits:   map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
		readEnvelope: func() (*cltypes.SignedExecutionPayloadEnvelope, error) {
			cancel()
			return nil, nil
		},
	}
	resolver := NewLiveSlotInputResolver(
		&cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)

	_, err := resolver.Resolve(ctx, preferences)
	require.ErrorIs(t, err, context.Canceled)
}

func liveResolverEnvelope(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	headState *state.CachingBeaconState,
	headRoot common.Hash,
) *cltypes.SignedExecutionPayloadEnvelope {
	t.Helper()
	parentBid := headState.GetLatestExecutionPayloadBid()
	header := headState.LatestBlockHeader()
	message := cltypes.NewExecutionPayloadEnvelope(cfg)
	message.BeaconBlockRoot = headRoot
	message.ParentBeaconBlockRoot = header.ParentRoot
	message.BuilderIndex = parentBid.BuilderIndex
	message.Payload.ParentHash = parentBid.ParentBlockHash
	message.Payload.BlockHash = parentBid.BlockHash
	requestsRoot, err := message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	parentBid.ExecutionRequestsRoot = requestsRoot
	return &cltypes.SignedExecutionPayloadEnvelope{Message: message, Signature: common.Bytes96{0: 1}}
}

func liveResolverFixture(t *testing.T) (
	clparams.BeaconChainConfig,
	*state.CachingBeaconState,
	*cltypes.SignedProposerPreferences,
	common.Hash,
	common.Hash,
	common.Hash,
) {
	t.Helper()
	cfg := gloasCoordinatorConfig()
	cfg.SlotsPerEpoch = 4
	cfg.MinSeedLookahead = 1
	cfg.BuilderRegistryLimit = 16
	cfg.BuilderPendingWithdrawalsLimit = 16
	targetSlot := uint64(12)
	headRoot := common.HexToHash("0x30")
	parentHash := common.HexToHash("0x40")
	dependentRoot := common.HexToHash("0x10")
	randao := common.HexToHash("0x50")

	headState := state.New(&cfg)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(targetSlot))
	headState.SetGenesisTime(1_000)
	headState.SetGenesisValidatorsRoot(common.HexToHash("0x60"))
	headState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: targetSlot - 1})
	headState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 2})
	require.NoError(t, headState.SetBlockRootAt(7, dependentRoot))
	require.NoError(t, headState.SetRandaoMixAt(3, randao))
	lookahead := solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch))
	lookahead.Set(0, 7)
	headState.SetProposerLookahead(lookahead)
	for i := range byte(8) {
		require.NoError(t, headState.AddValidator(solid.NewValidatorFromParameters(
			common.Bytes48{0: i + 2},
			common.Hash{},
			cfg.MaxEffectiveBalance,
			false,
			0,
			0,
			cfg.FarFutureEpoch,
			cfg.FarFutureEpoch,
		), cfg.MaxEffectiveBalance))
	}
	headState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentHash,
		BlockHash:       common.HexToHash("0x41"),
		GasLimit:        30_000_000,
		Slot:            targetSlot - 1,
	})
	builders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey:            common.Bytes48{0: 1},
		Version:           cfg.PayloadBuilderVersion,
		Balance:           cfg.MinDepositAmount + 2_000,
		DepositEpoch:      0,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	headState.SetBuilders(builders)
	pendingPayments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * cfg.SlotsPerEpoch))
	for i := 0; i < pendingPayments.Length(); i++ {
		pendingPayments.Set(i, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{}})
	}
	headState.SetBuilderPendingPayments(pendingPayments)
	headState.SetBuilderPendingWithdrawals(solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
		int(cfg.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ(),
	))
	expectedWithdrawals := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), new(cltypes.Withdrawal).EncodingSizeSSZ())
	expectedWithdrawals.Append(&cltypes.Withdrawal{Index: 1, Validator: 2, Address: common.HexToAddress("0x99"), Amount: 11})
	headState.SetPayloadExpectedWithdrawals(expectedWithdrawals)

	preferences := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   targetSlot,
			ValidatorIndex: 7,
			DependentRoot:  dependentRoot,
			FeeRecipient:   common.HexToAddress("0x20"),
			TargetGasLimit: 30_000_000,
		},
		Signature: common.Bytes96{0: 1},
	}
	return cfg, headState, preferences, headRoot, parentHash, randao
}
