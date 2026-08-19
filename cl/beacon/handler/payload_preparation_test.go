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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

func TestBlockBuilderWindowTakesPreparedPayloadEarly(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)

	// A primed payload is taken one quarter of the attestation deadline into the slot.
	prepared := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, true)
	require.Equal(t, slotStart.Add(time.Second).Add(-minPayloadPollingWindow), prepared.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), prepared.pollUntil)

	// Without a primed builder nothing changes: the execution layer still needs most of the slot.
	unprepared := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, false)
	require.Equal(t, slotStart.Add(3*time.Second).Add(-minPayloadPollingWindow), unprepared.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), unprepared.pollUntil)

	require.True(t, prepared.firstGetAt.Before(unprepared.firstGetAt))

	late := computeBlockBuilderWindow(slotStart.Add(2*time.Second), slotStart, cfg, clparams.ElectraVersion, true)
	require.Equal(t, slotStart.Add(2*time.Second), late.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), late.pollUntil)
}

func TestPreparedPayloadMatchesOnlyTheSamePrime(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)

	require.False(t, p.matches(10, id, now, 0), "nothing primed yet")

	p.set(10, id, now)
	require.True(t, p.matches(10, id, now, 0))

	// A different payload id means the execution layer started a fresh build — a reorg, a late
	// block, or a changed fee recipient — so the warm builder is gone.
	require.False(t, p.matches(10, []byte{9, 9, 9, 9, 9, 9, 9, 9}, now, 0))
	require.False(t, p.matches(11, id, now, 0), "primed for another slot")
	require.False(t, p.matches(10, nil, now, 0), "no id from the execution layer")
}

func TestPreparedPayloadRequiresMinimumWarmup(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)
	minAge := 2 * time.Second

	p.set(10, id, now.Add(-minAge+time.Nanosecond))
	require.False(t, p.matches(10, id, now, minAge))

	p.set(10, id, now.Add(-minAge))
	require.True(t, p.matches(10, id, now, minAge))
}

func TestPreparedPayloadMinimumWarmupPreservesBuildTime(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}

	require.Equal(t, 2*time.Second, preparedPayloadMinimumAge(cfg, clparams.ElectraVersion))
}

func TestPreparedPayloadRequiresBuilderContinuity(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)
	p.set(10, id, now.Add(-3*time.Second))

	require.True(t, canUsePreparedPayload(&p, true, 10, id, now, 2*time.Second))
	require.False(t, canUsePreparedPayload(&p, false, 10, id, now, 2*time.Second))
}

func TestStartPayloadPreparationSkipsRemoteEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(false)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: true},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	handler.StartPayloadPreparation(ctx)
}

func TestStartPayloadPreparationSkipsWithoutValidatorAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Times(0)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: false},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
	}

	handler.StartPayloadPreparation(t.Context())
}

func TestStartPayloadPreparationSkipsNilEngine(t *testing.T) {
	handler := &ApiHandler{}
	require.NotPanics(t, func() {
		handler.StartPayloadPreparation(t.Context())
	})
}

func TestStartPayloadPreparationStartsLocalEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(true)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: true},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	handler.StartPayloadPreparation(ctx)
}

func TestPreparePayloadLoopRunsImmediatelyWithSlotDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})
	// Preparation only primes on a head fork choice has published, so the loop stands down until
	// one is selected.
	syncedData.(*synced_data.SyncedDataManager).OnSelectedHead(handler.syncedData.HeadRoot(), postState.Slot())

	slotStart := time.Now().Add(6 * time.Second)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1)
	clock.EXPECT().GetSlotTime(targetSlot).Return(slotStart).AnyTimes()
	handler.ethClock = clock

	ctx, cancel := context.WithCancel(t.Context())
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(callCtx context.Context, _, _, _ common.Hash, _ *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			deadline, ok := callCtx.Deadline()
			require.True(t, ok)
			require.Equal(t, slotStart, deadline)
			cancel()
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler.engine = engine

	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopSkipsSlotsTooFarAhead(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// Before genesis the current slot clamps to zero, so the next slot can be hours out.
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(time.Hour)).AnyTimes()
	handler.ethClock = clock

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(true)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	done := handler.StartPayloadPreparation(ctx)
	<-ctx.Done()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("payload preparation did not stop after cancellation")
	}
}

func TestPreparePayloadLoopStandsOffWhileProducing(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).Times(0)
	handler.ethClock = clock

	// Priming would contend with the block being produced for the execution layer's single slot.
	finishProduction := handler.payloadPreparationGate.beginProduction()
	defer finishProduction()
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopSkipsSlotsAboutToStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// Too close to the slot for the prime to ever reach the age production demands of it, so the
	// state copy and the forkchoice update would both be spent for nothing.
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(200 * time.Millisecond)).AnyTimes()
	handler.ethClock = clock

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadForSendsCompleteForkChoiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, fcu, validatorParams := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)
	config := *handler.beaconChainCfg
	targetEpoch := postState.Slot()/config.SlotsPerEpoch + 1
	targetSlot := targetEpoch * config.SlotsPerEpoch
	config.DenebForkEpoch = targetEpoch
	config.InitializeForkSchedule()

	headState := state.New(&config)
	require.NoError(t, postState.CopyInto(headState))
	headState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: targetEpoch - 2, Root: common.Hash{0x31}})
	headState.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: targetEpoch - 1, Root: common.Hash{0x32}})
	currentEpoch := headState.Slot() / config.SlotsPerEpoch
	headState.SetRandaoMixAt(int(currentEpoch%config.EpochsPerHistoricalVector), common.Hash{0x51})
	headState.SetRandaoMixAt(int(targetEpoch%config.EpochsPerHistoricalVector), common.Hash{0x52})
	baseBlockRoot := common.Hash{0x41}
	syncedData := synced_data.NewSyncedDataManager(&config, true)
	require.NoError(t, syncedData.OnHeadStateWithBlockRoot(headState, baseBlockRoot))
	syncedData.OnSelectedHead(baseBlockRoot, headState.Slot())
	handler.beaconChainCfg = &config
	handler.syncedData = syncedData

	proposerIndex, err := headState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)

	feeRecipient := common.Address{0x11}
	validatorParams.SetFeeRecipient(proposerIndex, feeRecipient)

	advancedState, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
	require.Equal(t, clparams.CapellaVersion, headState.Version())
	require.Equal(t, clparams.DenebVersion, advancedState.Version())
	require.NotEqual(t, headState.GetRandaoMixes(targetEpoch), advancedState.GetRandaoMixes(targetEpoch))
	finalizedRoot := advancedState.FinalizedCheckpoint().Root
	justifiedRoot := advancedState.CurrentJustifiedCheckpoint().Root
	require.NotEqual(t, finalizedRoot, justifiedRoot)
	expectedFinalized := common.Hash{0x21}
	expectedSafe := common.Hash{0x22}
	fcu.Eth1Hashes[finalizedRoot] = expectedFinalized
	fcu.Eth1Hashes[justifiedRoot] = expectedSafe
	require.NotEqual(t, expectedFinalized, expectedSafe)

	version := handler.beaconChainCfg.GetCurrentStateVersion(targetSlot / handler.beaconChainCfg.SlotsPerEpoch)
	require.Equal(t, clparams.DenebVersion, version)
	expectedWithdrawals, err := state.GetExpectedWithdrawals(advancedState, targetSlot/handler.beaconChainCfg.SlotsPerEpoch)
	require.NoError(t, err)

	payloadID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, finalized, safe, head common.Hash, attrs *engine_types.PayloadAttributes, gotVersion clparams.StateVersion) ([]byte, error) {
			require.Equal(t, expectedFinalized, finalized)
			require.Equal(t, expectedSafe, safe)
			require.Equal(t, advancedState.LatestExecutionPayloadHeader().BlockHash, head)
			require.Equal(t, version, gotVersion)
			require.Equal(t, hexutil.Uint64(state.ComputeTimestampAtSlot(advancedState, targetSlot)), attrs.Timestamp)
			require.Equal(t, common.Hash(advancedState.GetRandaoMixes(targetSlot/handler.beaconChainCfg.SlotsPerEpoch)), attrs.PrevRandao)
			require.Equal(t, feeRecipient, attrs.SuggestedFeeRecipient)
			require.Equal(t, &baseBlockRoot, attrs.ParentBeaconBlockRoot)
			require.Nil(t, attrs.SlotNumber)
			require.Nil(t, attrs.TargetGasLimit)
			require.NotNil(t, attrs.Withdrawals)
			require.Len(t, attrs.Withdrawals, len(expectedWithdrawals.Withdrawals))
			for i, withdrawal := range expectedWithdrawals.Withdrawals {
				require.Equal(t, withdrawal.Index, attrs.Withdrawals[i].Index)
				require.Equal(t, withdrawal.Amount, attrs.Withdrawals[i].Amount)
				require.Equal(t, withdrawal.Validator, attrs.Withdrawals[i].Validator)
				require.Equal(t, withdrawal.Address, attrs.Withdrawals[i].Address)
			}
			return payloadID, nil
		})
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	primedHead, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
	require.Equal(t, baseBlockRoot, primedHead)
	require.True(t, handler.preparedPayload.matches(targetSlot, payloadID, time.Now(), 0))
}

// The lead is measured before the state copy and the epoch transition. Those are slow enough to
// spend it, and a prime that starts anyway occupies the execution module into the slot it was
// meant to help - the proposal then finds the builder busy with the prime it asked for.
// A busy execution layer must not cost the proposal. Before contention was classified, production
// fell through to retryAssembleBlock and waited it out; classifying it as an error without a retry
// here would turn any forkchoice update over the module's one-second timeout into a missed slot.
func TestProductionRetriesAForkChoiceUpdateThatIsBusy(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1

	engine := execution_client.NewMockExecutionEngine(ctrl)
	gomock.InOrder(
		engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, execution_client.ErrForkChoiceBusy),
		engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return([]byte{9, 9, 9, 9, 9, 9, 9, 9}, nil),
	)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(4 * time.Second)).AnyTimes()
	handler.ethClock = clock

	id, err := handler.forkChoiceUpdateForProposal(t.Context(), targetSlot,
		common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.ElectraVersion)

	require.NoError(t, err)
	require.Equal(t, []byte{9, 9, 9, 9, 9, 9, 9, 9}, id)
}

func TestPreparationGateRefusesActiveProduction(t *testing.T) {
	var gate payloadPreparationGate
	finishProduction := gate.beginProduction()
	defer finishProduction()

	_, _, ok := gate.tryBeginPreparation(t.Context())

	require.False(t, ok, "preparation must stand off a production that has announced itself")
}

func TestProductionDoesNotRetryAForkChoiceUpdateWithoutPayloadID(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, execution_client.ErrForkChoiceUpdateNoPayloadID).Times(1)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(4 * time.Second)).AnyTimes()
	handler.ethClock = clock

	id, err := handler.forkChoiceUpdateForProposal(t.Context(), targetSlot,
		common.Hash{}, common.Hash{}, common.Hash{}, &engine_types.PayloadAttributes{}, clparams.ElectraVersion)

	require.ErrorIs(t, err, execution_client.ErrForkChoiceUpdateNoPayloadID)
	require.Nil(t, id)
}

func TestProductionRetriesBusyForkChoiceUpdateAfterCollectionDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	payloadID := []byte{9, 9, 9, 9, 9, 9, 9, 9}

	engine := execution_client.NewMockExecutionEngine(ctrl)
	gomock.InOrder(
		engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, execution_client.ErrForkChoiceBusy),
		engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(payloadID, nil),
	)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(-10 * time.Second)).AnyTimes()
	handler.ethClock = clock

	id, err := handler.forkChoiceUpdateForProposal(t.Context(), targetSlot,
		common.Hash{}, common.Hash{}, common.Hash{}, &engine_types.PayloadAttributes{}, clparams.ElectraVersion)

	require.NoError(t, err)
	require.Equal(t, payloadID, id)
}

// Pins the wiring, not just the helper: production must go through the retry, so restoring the
// direct engine call fails here even though the helper itself still works.
func TestProduceBeaconBodySurvivesABusyForkChoiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)

	engine := execution_client.NewMockExecutionEngine(ctrl)
	first := true
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
			if first {
				first = false
				return nil, execution_client.ErrForkChoiceBusy
			}
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		}).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil, nil, errors.New("collection stops here")).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	handler.engine = engine

	// The fixture's slot is historical, which would leave the retry no window at all.
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(4 * time.Second)).AnyTimes()
	clock.EXPECT().GetCurrentSlot().Return(postState.Slot()).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().Return(postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch).AnyTimes()
	handler.ethClock = clock

	_, err := handler.produceBlock(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		postState.Slot()+1, common.Bytes96{}, common.Hash{})

	// It fails at collection, which this fixture cannot satisfy - but never on the busy update.
	require.Error(t, err)
	require.NotErrorIs(t, err, execution_client.ErrForkChoiceBusy)
}

func TestProductionUsesTargetSlotRandao(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	targetEpoch := targetSlot / handler.beaconChainCfg.SlotsPerEpoch
	wallClockEpoch := targetEpoch + 1
	targetMix := common.Hash{0x11}
	wallClockMix := common.Hash{0x22}
	postState.SetRandaoMixAt(int(targetEpoch%handler.beaconChainCfg.EpochsPerHistoricalVector), targetMix)
	postState.SetRandaoMixAt(int(wallClockEpoch%handler.beaconChainCfg.EpochsPerHistoricalVector), wallClockMix)

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.Equal(t, targetMix, common.Hash(attrs.PrevRandao))
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil, nil, errors.New("collection stops here")).AnyTimes()
	engine.EXPECT().SupportInsertion().Return(true).AnyTimes()
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(4 * time.Second)).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().Return(wallClockEpoch).AnyTimes()
	handler.ethClock = clock

	_, _, err := handler.produceBeaconBody(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		targetSlot, common.Bytes96{}, common.Hash{})

	require.Error(t, err)
}

// A rejection is not contention: retrying it would burn the slot re-asking a question already
// answered.
func TestProductionDoesNotRetryAForkChoiceRejection(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, execution_client.ErrForkChoiceNotAdopted).Times(1)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(4 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err := handler.forkChoiceUpdateForProposal(t.Context(), targetSlot,
		common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.ElectraVersion)

	require.ErrorIs(t, err, execution_client.ErrForkChoiceNotAdopted)
}

// Contention is not a rejection, so a busy execution layer gets another attempt. Without this the
// prime gives up on the first busy answer and the slot is never primed at all.
func TestPreparePayloadForRetriesWhileTheExecutionLayerIsBusy(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		}).AnyTimes()
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true).AnyTimes()

	engine := execution_client.NewMockExecutionEngine(ctrl)
	gomock.InOrder(
		engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, execution_client.ErrForkChoiceBusy),
		engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil),
	)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	head, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
	require.Equal(t, baseBlockRoot, head)
}

// The head is re-read before every attempt, not only the first: a retry must not assert a parent
// fork choice has already left behind, which is exactly the window a busy answer opens up.
func TestPreparePayloadForRechecksTheHeadBeforeEachRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	movedRoot := common.Hash{0x42}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		}).AnyTimes()
	gomock.InOrder(
		syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true).Times(3),
		syncedDataMock.EXPECT().SelectedHead().Return(movedRoot, postState.Slot(), true),
	)

	engine := execution_client.NewMockExecutionEngine(ctrl)
	// Exactly one: the busy answer earns a retry, and the moved head must stop it before a second.
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, execution_client.ErrForkChoiceBusy).Times(1)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = handler.preparePayloadFor(t.Context(), targetSlot)
	require.ErrorIs(t, err, errPreparationHeadChanged)
}

func TestPreparationDoesNotBlockSelectedHeadPublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := synced_data.NewSyncedDataManager(&clparams.BeaconChainConfig{}, true)
	baseBlockRoot := common.Hash{0x41}
	movedBlockRoot := common.Hash{0x42}
	data.OnSelectedHead(baseBlockRoot, 1)

	updateStarted := make(chan struct{})
	releaseUpdate := make(chan struct{})
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
			close(updateStarted)
			<-releaseUpdate
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler := &ApiHandler{syncedData: data, engine: engine}

	result := make(chan error, 1)
	go func() {
		_, err := handler.forkChoiceUpdateForPreparation(t.Context(), baseBlockRoot,
			common.Hash{}, common.Hash{}, common.Hash{}, nil, clparams.ElectraVersion)
		result <- err
	}()
	<-updateStarted

	headChanged := make(chan struct{})
	go func() {
		data.OnSelectedHead(movedBlockRoot, 2)
		close(headChanged)
	}()
	headPublished := false
	select {
	case <-headChanged:
		headPublished = true
	case <-time.After(time.Second):
	}
	close(releaseUpdate)
	resultErr := <-result
	require.True(t, headPublished, "selected head publication waited for payload preparation")
	require.ErrorIs(t, resultErr, errPreparationHeadChanged)
}

func TestProductionDoesNotWaitForPayloadPreparation(t *testing.T) {
	var gate payloadPreparationGate
	prepareCtx, finishPreparation, ok := gate.tryBeginPreparation(t.Context())
	require.True(t, ok)

	productionStarted := make(chan func(), 1)
	go func() {
		productionStarted <- gate.beginProduction()
	}()

	var finishProduction func()
	startedWithoutWaiting := false
	select {
	case finishProduction = <-productionStarted:
		startedWithoutWaiting = true
	case <-time.After(50 * time.Millisecond):
	}
	finishPreparation()
	if finishProduction == nil {
		finishProduction = <-productionStarted
	}
	finishProduction()

	require.True(t, startedWithoutWaiting, "production waited for speculative preparation")
	require.ErrorIs(t, context.Cause(prepareCtx), errProposalInFlight)
}

func TestSignedBlockStorageCancelsPayloadPreparationBeforeWork(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	prepareCtx, finishPreparation, ok := handler.payloadPreparationGate.tryBeginPreparation(t.Context())
	require.True(t, ok)
	defer finishPreparation()

	storeCtx, cancelStore := context.WithCancel(t.Context())
	cancelStore()
	err := handler.storeBlockAndBlobs(storeCtx, blocks[len(blocks)-1], nil, nil)

	require.Error(t, err)
	require.ErrorIs(t, context.Cause(prepareCtx), errProposalInFlight)
}

func TestExecutionCheckpointHashesKeepUnknownCheckpointsUnset(t *testing.T) {
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)

	safe, finalized := handler.executionCheckpointHashes(postState)

	require.Zero(t, safe)
	require.Zero(t, finalized)
}

func TestPreparePayloadForStopsWhenProductionStartsDuringStateCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	stateCopied := make(chan struct{})
	resumePreparation := make(chan struct{})
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			err := view(postState, baseBlockRoot, postState.Slot())
			close(stateCopied)
			<-resumePreparation
			return err
		})
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true)
	forkChoiceUpdates := 0
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
			forkChoiceUpdates++
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		}).AnyTimes()
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	result := make(chan error, 1)
	go func() {
		_, err := handler.preparePayloadFor(t.Context(), targetSlot)
		result <- err
	}()
	<-stateCopied
	finishProduction := handler.payloadPreparationGate.beginProduction()
	defer finishProduction()
	close(resumePreparation)

	require.ErrorIs(t, <-result, errProposalInFlight)
	require.Zero(t, forkChoiceUpdates)
}

func TestPreparePayloadForStopsWhenTheCopySpentTheLead(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		}).AnyTimes()
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	// Closer to the slot than the warm-up production would demand of the prime, so priming now
	// could only occupy the module without ever being usable.
	tooLate := preparedPayloadMinimumAge(handler.beaconChainCfg, clparams.ElectraVersion) / 2
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(tooLate)).AnyTimes()
	handler.ethClock = clock

	_, err = handler.preparePayloadFor(t.Context(), targetSlot)
	require.ErrorIs(t, err, errPreparationTooLate)
}

func TestPreparePayloadForRejectsChangedHeadBeforeForkChoiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	changedBlockRoot := common.Hash{0x42}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	gomock.InOrder(
		syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
			DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
				return view(postState, baseBlockRoot, postState.Slot())
			}),
		syncedDataMock.EXPECT().SelectedHead().Return(changedBlockRoot, postState.Slot(), true),
	)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = handler.preparePayloadFor(t.Context(), targetSlot)
	require.ErrorIs(t, err, errPreparationHeadChanged)
	require.False(t, handler.preparedPayload.matches(targetSlot, []byte{1}, time.Now(), 0))
}

func TestPreparePayloadForUsesPostEpochProposer(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	config := *handler.beaconChainCfg
	targetEpoch := postState.Slot()/config.SlotsPerEpoch + 1
	targetSlot := targetEpoch * config.SlotsPerEpoch
	config.FuluForkEpoch = targetEpoch + 10
	config.GloasForkEpoch = targetEpoch + 11
	config.InitializeForkSchedule()
	handler.beaconChainCfg = &config

	headState := state.New(&config)
	require.NoError(t, postState.CopyInto(headState))
	headState.SetSlot(targetSlot - 1)
	for i := 0; i < headState.ValidatorLength(); i += 2 {
		headState.SetEffectiveBalanceForValidatorAtIndex(i, 0)
		require.NoError(t, headState.SetValidatorBalance(i, config.MaxEffectiveBalanceElectra))
	}

	mixPosition := (targetEpoch + config.EpochsPerHistoricalVector - config.MinSeedLookahead - 1) % config.EpochsPerHistoricalVector
	var oldProposer, newProposer uint64
	found := false
	for nonce := byte(0); ; nonce++ {
		headState.SetRandaoMixAt(int(mixPosition), common.Hash{nonce})
		var err error
		oldProposer, err = headState.GetBeaconProposerIndexForSlot(targetSlot)
		require.NoError(t, err)

		advanced, err := headState.Copy()
		require.NoError(t, err)
		require.NoError(t, transition.DefaultMachine.ProcessSlots(advanced, targetSlot))
		newProposer, err = advanced.GetBeaconProposerIndexForSlot(targetSlot)
		require.NoError(t, err)
		if oldProposer != newProposer {
			found = true
			break
		}
		if nonce == 255 {
			break
		}
	}
	require.True(t, found, "fixture must expose a proposer change across epoch processing")
	validatorParams.SetFeeRecipient(newProposer, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(headState, baseBlockRoot, headState.Slot())
		})
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, headState.Slot(), true).AnyTimes()
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.Equal(t, common.Address{0x11}, attrs.SuggestedFeeRecipient)
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
}

func TestFuluPreparationRejectsUnregisteredProposerBeforeCopyingState(t *testing.T) {
	_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	currentEpoch := postState.Slot() / handler.beaconChainCfg.SlotsPerEpoch
	handler.beaconChainCfg.FuluForkEpoch = currentEpoch
	handler.beaconChainCfg.GloasForkEpoch = currentEpoch + 2
	handler.beaconChainCfg.InitializeForkSchedule()
	require.NoError(t, postState.UpgradeToFulu())
	targetSlot := (currentEpoch + 1) * handler.beaconChainCfg.SlotsPerEpoch

	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		}).AnyTimes()

	var preparationErr error
	allocations := testing.AllocsPerRun(1, func() {
		_, preparationErr = handler.preparePayloadFor(t.Context(), targetSlot)
	})

	require.ErrorIs(t, preparationErr, errNotOurProposal)
	require.Less(t, allocations, 1000.0, "an unregistered Fulu proposer should not require a full state copy")
}

func TestPreparePayloadForPairsRootAndStateFromOneView(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// The root the view hands over is the only one preparation may use. Reading it separately would
	// let a head update in between pair a parent beacon block root with a different state, priming a
	// builder production can never match.
	viewRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, viewRoot, postState.Slot())
		})
	syncedDataMock.EXPECT().SelectedHead().Return(viewRoot, postState.Slot(), true).AnyTimes()

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.NotNil(t, attrs.ParentBeaconBlockRoot)
			require.Equal(t, viewRoot, *attrs.ParentBeaconBlockRoot)
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	handler.engine = engine

	primedHead, err := handler.preparePayloadFor(t.Context(), targetSlot)
	require.NoError(t, err)
	require.Equal(t, viewRoot, primedHead)
}

func TestShouldPreparePayloadVersion(t *testing.T) {
	for _, tc := range []struct {
		version clparams.StateVersion
		want    bool
	}{
		{clparams.Phase0Version, false},
		{clparams.AltairVersion, false},
		{clparams.BellatrixVersion, false},
		{clparams.CapellaVersion, true},
		{clparams.DenebVersion, true},
		{clparams.FuluVersion, true},
		{clparams.GloasVersion, false},
	} {
		require.Equal(t, tc.want, shouldPreparePayloadVersion(tc.version), tc.version.String())
	}
}

func TestPreparedPayloadKeepsConsecutiveSlots(t *testing.T) {
	var p preparedPayload
	first := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	second := []byte{2, 2, 2, 2, 2, 2, 2, 2}
	now := time.Unix(100, 0)

	// Consecutive proposals: priming slot 11 must not evict slot 10, whose block may still be
	// in production.
	p.set(10, first, now)
	p.set(11, second, now)
	require.True(t, p.matches(10, first, now, 0))
	require.True(t, p.matches(11, second, now, 0))

	// Records old enough that they can no longer be produced are dropped, so the map is bounded.
	p.set(10+preparedPayloadRetainSlots+1, []byte{3, 3, 3, 3, 3, 3, 3, 3}, now)
	require.False(t, p.matches(10, first, now, 0))
}

func TestPreparedPayloadCopiesTheID(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)

	p.set(10, id, now)
	id[0] = 0xff

	// The caller's buffer must not be able to invalidate, or forge, a later match.
	require.True(t, p.matches(10, []byte{1, 2, 3, 4, 5, 6, 7, 8}, now, 0))
	require.False(t, p.matches(10, id, now, 0))
}
