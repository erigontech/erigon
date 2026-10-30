// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	mock_services "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestPayloadAttributesEventSkipsStateWithoutSubscriber(t *testing.T) {
	config := payloadPreparationLoopConfig(12, 0, 0)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	store := mock_services.NewForkChoiceStorageMock(t)
	store.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		t.Fatal("payload attributes event copied state without a subscriber")
		return nil, nil
	}
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	handler := &ApiHandler{
		beaconChainCfg:  &config,
		ethClock:        clock,
		forkchoiceStore: store,
		emitters:        emitter,
	}

	err := handler.emitPayloadAttributes()

	require.NoError(t, err)
	require.Empty(t, events)
}

func TestPayloadAttributesEventAtGloasForkBoundary(t *testing.T) {
	_, _, _, _, postState, handler, _, _, store, _ := setupTestingHandler(
		t, clparams.ElectraVersion, log.Root(), false,
	)
	parentRoot := common.Hash{0x41}
	parentHash := common.Hash{0xa1}
	headState, targetState, currentSlot, targetSlot, expectedWithdrawals := setupFirstGloasPayloadStates(
		t, handler, postState, parentHash, 30_000_000,
	)
	handler.beaconChainCfg.GasLimitSchedule = []clparams.GasLimitScheduleEntry{{
		Epoch: targetSlot / handler.beaconChainCfg.SlotsPerEpoch, GasLimit: 200_000_000,
	}}
	store.HeadVal = parentRoot
	store.HeadSlotVal = currentSlot
	store.StateAtBlockRootVal[parentRoot] = headState
	emitter := beaconevents.NewEventEmitter()
	handler.emitters = emitter
	release := emitter.TrackPayloadAttributesSubscriber()
	defer release()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	mockSlotClock(t, handler, currentSlot)

	require.NoError(t, handler.emitPayloadAttributes())

	require.Len(t, events, 1)
	event := (<-events).Data.(*beaconevents.PayloadAttributesData)
	require.Equal(t, "gloas", event.Version)
	require.Equal(t, targetSlot, event.Data.ProposalSlot)
	require.Nil(t, event.Data.ParentBlockNumber)
	require.Equal(t, parentRoot, event.Data.ParentBlockRoot)
	require.Equal(t, parentHash, event.Data.ParentBlockHash)
	require.Equal(t, &parentHash, event.Data.SafeBlockHash)
	require.Equal(t, &parentHash, event.Data.FinalizedBlockHash)
	proposerIndex, err := targetState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	require.Equal(t, proposerIndex, event.Data.ProposerIndex)
	attributes := event.Data.PayloadAttributes
	require.Equal(t, state.ComputeTimestampAtSlot(targetState, targetSlot), attributes.Timestamp)
	require.Equal(t, common.Hash(targetState.GetRandaoMixes(targetSlot/handler.beaconChainCfg.SlotsPerEpoch)), attributes.PrevRandao)
	require.Equal(t, common.Address{}, attributes.SuggestedFeeRecipient)
	require.Equal(t, &parentRoot, attributes.ParentBeaconBlockRoot)
	require.Equal(t, &targetSlot, attributes.SlotNumber)
	targetGasLimit := uint64(200_000_000)
	require.Equal(t, &targetGasLimit, attributes.TargetGasLimit)
	require.NotNil(t, attributes.Withdrawals)
	require.Equal(t, expectedWithdrawals, *attributes.Withdrawals)
}

func mockSlotClock(t *testing.T, handler *ApiHandler, currentSlot uint64) {
	t.Helper()
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(time.Hour)).AnyTimes()
	handler.ethClock = clock
}

func startPayloadAttributesEventLoop(t *testing.T, handler *ApiHandler) chan *beaconevents.EventStream {
	t.Helper()
	emitter := beaconevents.NewEventEmitter()
	handler.emitters = emitter
	release := emitter.TrackPayloadAttributesSubscriber()
	events := make(chan *beaconevents.EventStream, 8)
	subscription := emitter.State().Subscribe(events)
	ctx, cancel := context.WithCancel(t.Context())
	done := handler.StartPayloadAttributes(ctx)
	t.Cleanup(func() {
		cancel()
		<-done
		subscription.Unsubscribe()
		release()
	})
	return events
}

func awaitPayloadAttributesEvent(t *testing.T, handler *ApiHandler, events chan *beaconevents.EventStream, head *beaconevents.HeadV2Data) *beaconevents.PayloadAttributesData {
	t.Helper()
	handler.emitters.State().SendHeadV2(head)
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Event == beaconevents.StatePayloadAttributes {
				return event.Data.(*beaconevents.PayloadAttributesData)
			}
		case <-timer.C:
			t.Fatal("payload attributes event was not emitted")
		}
	}
}

func TestPayloadAttributesEventUsesFullParent(t *testing.T) {
	headState, handler, _, store, validatorParams := setupGloasPreparationTest(t)
	parentRoot := common.Hash{0x41}
	emptyParentHash := common.Hash{0xa1}
	fullParentHash := common.Hash{0xb2}
	targetSlot := headState.Slot() + 1
	header := headState.LatestBlockHeader()
	header.Slot = headState.Slot()
	headState.SetLatestBlockHeader(&header)
	headState.SetLatestBlockHash(emptyParentHash)
	headState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: emptyParentHash,
		BlockHash:       fullParentHash,
		GasLimit:        45_000_000,
		Slot:            headState.Slot(),
	})
	headState.SetFinalizedCheckpoint(solid.Checkpoint{Root: common.Hash{0xf0}})
	headState.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Root: common.Hash{0x50}})
	paymentIndex := int(handler.beaconChainCfg.SlotsPerEpoch + headState.Slot()%handler.beaconChainCfg.SlotsPerEpoch)
	payments := headState.GetBuilderPendingPayments()
	payments.Set(paymentIndex, &cltypes.BuilderPendingPayment{Withdrawal: &cltypes.BuilderPendingWithdrawal{
		FeeRecipient: common.Address{0xbb}, Amount: 12, BuilderIndex: 3,
	}})
	headState.SetBuilderPendingPayments(payments)
	requests := cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion)
	store.SetEnvelope(parentRoot, &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{ExecutionRequests: requests},
	})
	store.HeadVal = parentRoot
	store.HeadSlotVal = headState.Slot()
	store.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	buildOnFull := true
	store.ShouldBuildOnFullVal = &buildOnFull
	store.StateAtBlockRootVal[parentRoot] = headState
	advancedState, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
	require.NoError(t, transition.DefaultMachine.ApplyParentExecutionPayload(advancedState, requests))
	expectedWithdrawals, err := state.GetExpectedWithdrawals(advancedState, targetSlot/handler.beaconChainCfg.SlotsPerEpoch)
	require.NoError(t, err)
	require.NotEmpty(t, expectedWithdrawals.Withdrawals)
	proposerIndex, err := advancedState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	feeRecipient := common.Address{0x11}
	validatorParams.SetFeeRecipient(proposerIndex, feeRecipient)
	finalizedHash := common.Hash{0xf1}
	safeHash := common.Hash{0x51}
	store.Eth1Hashes[advancedState.FinalizedCheckpoint().Root] = finalizedHash
	store.Eth1Hashes[advancedState.CurrentJustifiedCheckpoint().Root] = safeHash
	mockSlotClock(t, handler, headState.Slot())
	events := startPayloadAttributesEventLoop(t, handler)

	event := awaitPayloadAttributesEvent(t, handler, events, &beaconevents.HeadV2Data{
		Version: "gloas",
		Data: beaconevents.HeadV2Content{
			Slot: headState.Slot(), Block: parentRoot, PayloadStatus: "full",
		},
	})

	require.Equal(t, fullParentHash, event.Data.ParentBlockHash)
	require.Equal(t, &safeHash, event.Data.SafeBlockHash)
	require.Equal(t, &finalizedHash, event.Data.FinalizedBlockHash)
	require.Equal(t, proposerIndex, event.Data.ProposerIndex)
	require.Equal(t, targetSlot, event.Data.ProposalSlot)
	targetGasLimit := hexutil.Uint64(45_000_000)
	slotNumber := hexutil.Uint64(targetSlot)
	expectedAttributes := handler.payloadBuildAttributes(
		advancedState,
		parentRoot,
		targetSlot,
		feeRecipient,
		cltypes.ConvertConsensusWithdrawalsToExecutionWithdrawals(expectedWithdrawals.Withdrawals),
		&slotNumber,
		&targetGasLimit,
		clparams.GloasVersion,
	)
	require.Equal(t, beaconevents.NewPayloadAttributes(clparams.GloasVersion, expectedAttributes), event.Data.PayloadAttributes)
}

func TestPayloadAttributesEventUsesEmptyParent(t *testing.T) {
	for _, test := range []struct {
		name               string
		unreadableEnvelope bool
	}{
		{name: "empty head"},
		{name: "full head with unreadable envelope", unreadableEnvelope: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			headState, handler, store, parentRoot := setupEmptyGloasHead(t)
			cachedWithdrawals := solid.NewDynamicListSSZ[*cltypes.Withdrawal](int(handler.beaconChainCfg.MaxWithdrawalsPerPayload))
			cachedWithdrawals.Append(&cltypes.Withdrawal{Index: 7, Validator: 8, Address: common.Address{0xaa}, Amount: 9})
			headState.SetPayloadExpectedWithdrawals(cachedWithdrawals)
			payloadStatus := "empty"
			if test.unreadableEnvelope {
				payloadStatus = "full"
				store.HeadPayloadStatusVal = cltypes.PayloadStatusFull
				buildOnFull := true
				store.ShouldBuildOnFullVal = &buildOnFull
				store.SetEnvelope(parentRoot, &cltypes.SignedExecutionPayloadEnvelope{})
				store.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
					return nil, errors.New("envelope unavailable")
				}
			}
			mockSlotClock(t, handler, headState.Slot())
			events := startPayloadAttributesEventLoop(t, handler)

			event := awaitPayloadAttributesEvent(t, handler, events, &beaconevents.HeadV2Data{
				Version: "gloas",
				Data:    beaconevents.HeadV2Content{Slot: headState.Slot(), Block: parentRoot, PayloadStatus: payloadStatus},
			})

			require.Equal(t, common.Hash{0xa1}, event.Data.ParentBlockHash)
			require.NotNil(t, event.Data.PayloadAttributes.Withdrawals)
			require.Equal(t, []*cltypes.Withdrawal{
				{Index: 7, Validator: 8, Address: common.Address{0xaa}, Amount: 9},
			}, *event.Data.PayloadAttributes.Withdrawals)
		})
	}
}

func TestPayloadAttributesLoopDrainsStateFeedDuringStateWork(t *testing.T) {
	_, _, _, _, postState, handler, _, _, store, _ := setupTestingHandler(
		t, clparams.ElectraVersion, log.Root(), false,
	)
	firstRoot := common.Hash{0x41}
	latestRoot := common.Hash{0x42}
	headState, _, currentSlot, _, _ := setupFirstGloasPayloadStates(
		t, handler, postState, common.Hash{0xa1}, 30_000_000,
	)
	var headRoot atomic.Value
	headRoot.Store(firstRoot)
	store.GetHeadNodeFn = func() (forkchoice.ForkChoiceNode, uint64, error) {
		return forkchoice.ForkChoiceNode{Root: headRoot.Load().(common.Hash), PayloadStatus: cltypes.PayloadStatusPending}, currentSlot, nil
	}
	stateReadStarted := make(chan struct{})
	releaseStateRead := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseStateRead) }) }
	store.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		startOnce.Do(func() {
			close(stateReadStarted)
			<-releaseStateRead
		})
		return headState.Copy()
	}
	mockSlotClock(t, handler, currentSlot)
	emitter := beaconevents.NewEventEmitter()
	handler.emitters = emitter
	defer emitter.TrackPayloadAttributesSubscriber()()
	ctx, cancel := context.WithCancel(t.Context())
	done := handler.StartPayloadAttributes(ctx)
	defer func() {
		release()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("payload attributes loop did not stop")
		}
	}()
	head := &beaconevents.HeadV2Data{Version: "fulu", Data: beaconevents.HeadV2Content{Slot: currentSlot}}
	emitter.State().SendHeadV2(head)
	select {
	case <-stateReadStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("payload attributes loop did not start state work")
	}
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for range 1024 {
			emitter.State().SendBlock(&beaconevents.BlockData{})
		}
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("payload attributes loop blocked the state feed during state work")
	}
	headRoot.Store(latestRoot)
	// SendHeadV2 drops the event for a full subscriber, and the block burst can leave the loop's buffer full.
	require.Eventually(t, func() bool { return emitter.State().SendHeadV2(head) > 0 }, 5*time.Second, time.Millisecond)
	events := make(chan *beaconevents.EventStream, 8)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	release()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Event != beaconevents.StatePayloadAttributes {
				continue
			}
			if event.Data.(*beaconevents.PayloadAttributesData).Data.ParentBlockRoot == latestRoot {
				return
			}
		case <-timeout:
			t.Fatal("payload attributes loop did not emit for the latest head after state work")
		}
	}
}

func setupEmptyGloasHead(t *testing.T) (*state.CachingBeaconState, *ApiHandler, *mock_services.ForkChoiceStorageMock, common.Hash) {
	t.Helper()
	headState, handler, _, store, _ := setupGloasPreparationTest(t)
	parentRoot := common.Hash{0x41}
	headState.SetLatestBlockHash(common.Hash{0xa1})
	headState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xa1},
		BlockHash:       common.Hash{0xb2},
		GasLimit:        45_000_000,
		Slot:            headState.Slot(),
	})
	store.HeadVal = parentRoot
	store.HeadSlotVal = headState.Slot()
	store.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
	store.StateAtBlockRootVal[parentRoot] = headState
	return headState, handler, store, parentRoot
}

func TestPayloadAttributesEventAfterMissedSlotTargetsNextSlot(t *testing.T) {
	headState, handler, _, parentRoot := setupEmptyGloasHead(t)
	currentSlot := headState.Slot() + 1
	mockSlotClock(t, handler, currentSlot)
	events := startPayloadAttributesEventLoop(t, handler)

	event := awaitPayloadAttributesEvent(t, handler, events, &beaconevents.HeadV2Data{
		Version: "gloas",
		Data:    beaconevents.HeadV2Content{Slot: headState.Slot(), Block: parentRoot, PayloadStatus: "empty"},
	})

	proposalSlot := currentSlot + 1
	require.Equal(t, proposalSlot, event.Data.ProposalSlot)
	require.Equal(t, parentRoot, event.Data.ParentBlockRoot)
	require.Equal(t, &proposalSlot, event.Data.PayloadAttributes.SlotNumber)
	require.Equal(t, state.ComputeTimestampAtSlot(headState, proposalSlot), event.Data.PayloadAttributes.Timestamp)
}

func TestPayloadAttributesEventAtPayloadDecisionWithoutHeadChange(t *testing.T) {
	headState, handler, _, parentRoot := setupEmptyGloasHead(t)
	currentSlot := headState.Slot()
	decisionDue := time.Duration(handler.beaconChainCfg.PayloadAttestationDueMs()) * time.Millisecond
	var slotStart time.Time
	var slotStartOnce sync.Once
	currentSlotStart := func() time.Time {
		slotStartOnce.Do(func() { slotStart = time.Now().Add(-decisionDue).Add(200 * time.Millisecond) })
		return slotStart
	}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	clock.EXPECT().GetSlotTime(currentSlot).DoAndReturn(func(uint64) time.Time { return currentSlotStart() }).AnyTimes()
	clock.EXPECT().GetSlotTime(currentSlot + 1).DoAndReturn(func(uint64) time.Time { return currentSlotStart().Add(time.Hour) }).AnyTimes()
	handler.ethClock = clock
	events := startPayloadAttributesEventLoop(t, handler)

	select {
	case event := <-events:
		require.Equal(t, beaconevents.StatePayloadAttributes, event.Event)
		data := event.Data.(*beaconevents.PayloadAttributesData)
		require.Equal(t, currentSlot+1, data.Data.ProposalSlot)
		require.Equal(t, parentRoot, data.Data.ParentBlockRoot)
	case <-time.After(5 * time.Second):
		t.Fatal("payload attributes event was not emitted at the payload decision")
	}
}

func TestPayloadAttributesEventSkipsStateWorkForFarBehindHead(t *testing.T) {
	headState, handler, store, _ := setupEmptyGloasHead(t)
	store.GetStateAtBlockRootFn = func(common.Hash) (*state.CachingBeaconState, error) {
		t.Fatal("payload attributes event copied state for a head beyond the proposer lookahead")
		return nil, nil
	}
	slotsPerEpoch := handler.beaconChainCfg.SlotsPerEpoch
	currentSlot := headState.Slot() + (handler.beaconChainCfg.MinSeedLookahead+1)*slotsPerEpoch
	mockSlotClock(t, handler, currentSlot)
	emitter := beaconevents.NewEventEmitter()
	handler.emitters = emitter
	defer emitter.TrackPayloadAttributesSubscriber()()

	require.NoError(t, handler.emitPayloadAttributes())
}

func TestPayloadAttributesEventDropsHeadChangedBeforePublication(t *testing.T) {
	headState, handler, store, parentRoot := setupEmptyGloasHead(t)
	var headCalls atomic.Int32
	store.GetHeadNodeFn = func() (forkchoice.ForkChoiceNode, uint64, error) {
		if headCalls.Add(1) > 2 {
			return forkchoice.ForkChoiceNode{Root: common.Hash{0x42}, PayloadStatus: cltypes.PayloadStatusEmpty}, headState.Slot(), nil
		}
		return forkchoice.ForkChoiceNode{Root: parentRoot, PayloadStatus: cltypes.PayloadStatusEmpty}, headState.Slot(), nil
	}
	mockSlotClock(t, handler, headState.Slot())
	emitter := beaconevents.NewEventEmitter()
	handler.emitters = emitter
	defer emitter.TrackPayloadAttributesSubscriber()()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()

	require.NoError(t, handler.emitPayloadAttributes())

	require.Empty(t, events)
}
