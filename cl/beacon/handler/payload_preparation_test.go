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
	"bytes"
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	mock_services "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func preparedWarmup(p *preparedPayload, slot uint64, payloadID []byte, now time.Time) time.Duration {
	warmup, _, _ := p.warmupAndMismatch(slot, payloadID, now)
	return warmup
}

type payloadBuildEngine struct {
	*execution_client.MockExecutionEngine
	t                 *testing.T
	startPayloadBuild func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error)
}

func newPayloadBuildEngine(t *testing.T, ctrl *gomock.Controller) *payloadBuildEngine {
	return &payloadBuildEngine{
		MockExecutionEngine: execution_client.NewMockExecutionEngine(ctrl),
		t:                   t,
	}
}

func (e *payloadBuildEngine) StartPayloadBuild(
	ctx context.Context,
	head common.Hash,
	attrs *engine_types.PayloadAttributes,
) ([]byte, error) {
	e.t.Helper()
	require.NotNil(e.t, e.startPayloadBuild, "unexpected payload-build attempt")
	return e.startPayloadBuild(ctx, head, attrs)
}

func preparePayloadForTest(t *testing.T, handler *ApiHandler, targetSlot uint64) (common.Hash, error) {
	t.Helper()
	var scratch payloadPreparationScratch
	result, err := handler.preparePayloadForWithScratch(t.Context(), targetSlot, &scratch)
	return result.headRoot, err
}

type payloadPreparationLoopRun struct {
	handler         *ApiHandler
	syncedData      *sync_mock_services.MockSyncedData
	headState       *state.CachingBeaconState
	baseBlockRoot   common.Hash
	selectedSlot    uint64
	currentSlot     uint64
	targetSlot      uint64
	targetSlotStart time.Time
	timeout         time.Duration
	shouldPrepare   bool
	checkBuild      func(common.Hash, *engine_types.PayloadAttributes)
}

func runPayloadPreparationLoop(t *testing.T, ctrl *gomock.Controller, run payloadPreparationLoopRun) {
	t.Helper()
	run.syncedData.EXPECT().SelectedHead().Return(run.baseBlockRoot, run.selectedSlot, true).AnyTimes()
	run.syncedData.EXPECT().HeadRoot().Return(run.baseBlockRoot).AnyTimes()
	viewHead := run.syncedData.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(run.headState, run.baseBlockRoot, run.headState.Slot())
		})
	if run.shouldPrepare {
		viewHead.Times(1)
	} else {
		viewHead.Times(0)
	}

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(run.currentSlot).AnyTimes()
	clock.EXPECT().GetSlotTime(run.targetSlot).Return(run.targetSlotStart).AnyTimes()
	run.handler.ethClock = clock

	timeout := 300 * time.Millisecond
	if run.shouldPrepare {
		timeout = 5 * time.Second
	}
	if run.timeout > 0 {
		timeout = run.timeout
	}
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	buildStarted := false
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(_ context.Context, head common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
		buildStarted = true
		if run.checkBuild != nil {
			run.checkBuild(head, attrs)
		}
		cancel()
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	run.handler.engine = engine

	run.handler.preparePayloadLoop(ctx)
	require.Equal(t, run.shouldPrepare, buildStarted)
}

func TestBlockBuilderWindowTakesPreparedPayloadEarly(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)

	// Two seconds of warmup reaches the earliest prepared collection point.
	prepared := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, 2*time.Second)
	require.Equal(t, slotStart.Add(time.Second).Add(-minPayloadPollingWindow), prepared.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), prepared.pollUntil)

	// Without a primed builder nothing changes: the execution layer still needs most of the slot.
	unprepared := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, 0)
	require.Equal(t, slotStart.Add(3*time.Second).Add(-minPayloadPollingWindow), unprepared.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), unprepared.pollUntil)

	require.True(t, prepared.firstGetAt.Before(unprepared.firstGetAt))
	fullyWarmed := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, 10*time.Second)
	require.Equal(t, prepared, fullyWarmed, "warmup beyond the cap must not collect the payload earlier")

	late := computeBlockBuilderWindow(slotStart.Add(2*time.Second), slotStart, cfg, clparams.ElectraVersion, 2*time.Second)
	require.Equal(t, slotStart.Add(2*time.Second), late.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), late.pollUntil)
}

func TestBlockBuilderWindowUsesPartialWarmup(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}
	slotStart := time.Unix(100, 0)

	window := computeBlockBuilderWindow(slotStart, slotStart, cfg, clparams.ElectraVersion, time.Second)

	// One second of work before production advances collection by one second. It does not have to
	// cross a minimum-age threshold before it becomes useful.
	require.Equal(t, slotStart.Add(2*time.Second).Add(-minPayloadPollingWindow), window.firstGetAt)
	require.Equal(t, slotStart.Add(3*time.Second), window.pollUntil)
}

func TestPreparedPayloadMatchesOnlyTheSamePrime(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)

	require.Zero(t, preparedWarmup(&p, 10, id, now), "nothing primed yet")

	p.set(10, id, common.Hash{}, now.Add(-time.Second))
	require.Equal(t, time.Second, preparedWarmup(&p, 10, id, now))

	// A different payload ID means production selected another build, so the prepared record must
	// not change its collection time.
	require.Zero(t, preparedWarmup(&p, 10, []byte{9, 9, 9, 9, 9, 9, 9, 9}, now))
	require.Zero(t, preparedWarmup(&p, 11, id, now), "primed for another slot")
	require.Zero(t, preparedWarmup(&p, 10, nil, now), "no id from the execution layer")
}

func TestPreparedPayloadKeepsTheEarliestMatchingPrime(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	firstPrime := time.Unix(100, 0)

	p.set(10, id, common.Hash{}, firstPrime)
	p.set(10, id, common.Hash{}, firstPrime.Add(time.Second))

	require.Equal(t, 2*time.Second, preparedWarmup(&p, 10, id, firstPrime.Add(2*time.Second)))
}

func TestMaximumPreparedAdvancePreservesRecentBuildTime(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   12,
		IntervalsPerSlot: 3,
	}

	require.Equal(t, 2*time.Second, maximumPreparedAdvance(cfg, clparams.ElectraVersion))
}

func TestPayloadPreparationScratchReusesState(t *testing.T) {
	_, _, _, _, postState, _, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	var scratch payloadPreparationScratch

	scratch.resetForTargetSlot(10)
	first, err := scratch.copyFrom(postState, postState.BeaconConfig())
	require.NoError(t, err)
	first.SetSlot(first.Slot() + 1)
	scratch.resetForTargetSlot(10)
	second, err := scratch.copyFrom(postState, postState.BeaconConfig())
	require.NoError(t, err)

	require.Same(t, first, second)
	require.Equal(t, postState.Slot(), second.Slot())
}

func TestPayloadPreparationScratchReleasesStateBetweenTargetSlots(t *testing.T) {
	_, _, _, _, postState, _, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	var scratch payloadPreparationScratch

	scratch.resetForTargetSlot(10)
	_, err := scratch.copyFrom(postState, postState.BeaconConfig())
	require.NoError(t, err)
	require.NotNil(t, scratch.state)

	scratch.resetForTargetSlot(11)

	require.Nil(t, scratch.state)
}

func TestShouldWaitForCurrentSlotHead(t *testing.T) {
	currentSlotStart := time.Unix(100, 0)
	attestationDeadline := 4 * time.Second
	tests := []struct {
		name                 string
		selectedSlot         uint64
		blockProcessing      bool
		now                  time.Time
		wantWaitForFreshHead bool
	}{
		{
			name:                 "future head",
			selectedSlot:         11,
			now:                  currentSlotStart.Add(attestationDeadline),
			wantWaitForFreshHead: true,
		},
		{
			name:         "current head",
			selectedSlot: 10,
			now:          currentSlotStart,
		},
		{
			name:                 "before attestation deadline",
			selectedSlot:         9,
			now:                  currentSlotStart.Add(attestationDeadline - time.Millisecond),
			wantWaitForFreshHead: true,
		},
		{
			name:                 "at attestation deadline",
			selectedSlot:         9,
			now:                  currentSlotStart.Add(attestationDeadline),
			wantWaitForFreshHead: false,
		},
		{
			name:                 "past deadline without active block processing",
			selectedSlot:         9,
			now:                  currentSlotStart.Add(attestationDeadline),
			wantWaitForFreshHead: false,
		},
		{
			name:                 "block is still being processed",
			selectedSlot:         9,
			blockProcessing:      true,
			now:                  currentSlotStart.Add(attestationDeadline),
			wantWaitForFreshHead: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantWaitForFreshHead, shouldWaitForCurrentSlotHead(
				10, test.selectedSlot, test.blockProcessing, test.now, currentSlotStart, attestationDeadline,
			))
		})
	}
}

func TestGloasPayloadDecisionDelay(t *testing.T) {
	selectedSlotStart := time.Unix(100, 0)
	deadline := 9 * time.Second

	require.Equal(t, time.Second, gloasPayloadDecisionDelay(
		selectedSlotStart.Add(8*time.Second), selectedSlotStart, deadline,
	))
	require.Zero(t, gloasPayloadDecisionDelay(
		selectedSlotStart.Add(deadline), selectedSlotStart, deadline,
	))
}

func TestStartPayloadPreparationSkipsEngineWithoutDirectBuilder(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	var output bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: true},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
		logger:         logger,
	}

	done := handler.StartPayloadPreparation(t.Context())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("payload preparation did not report that the execution client is unsupported")
	}

	require.Contains(t, output.String(), "execution client does not support direct payload building")
}

func TestStartPayloadPreparationSkipsWithoutValidatorAPI(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := newPayloadBuildEngine(t, ctrl)
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

func TestStartPayloadPreparationStartsWithDirectBuilder(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := newPayloadBuildEngine(t, ctrl)
	handler := &ApiHandler{
		engine:         engine,
		routerCfg:      &beacon_router_configuration.RouterConfiguration{Validator: true},
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 12},
		logger:         log.Root(),
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	handler.StartPayloadPreparation(ctx)
}

func TestPreparePayloadLoopStopsWithZeroSlotDuration(t *testing.T) {
	handler := &ApiHandler{beaconChainCfg: &clparams.BeaconChainConfig{}, logger: log.Root()}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.NotPanics(t, func() {
		handler.preparePayloadLoop(ctx)
	})
}

func TestPreparePayloadLoopWarnsWhenGloasWindowIsTooShort(t *testing.T) {
	var output bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
	handler := &ApiHandler{
		beaconChainCfg: &clparams.BeaconChainConfig{SecondsPerSlot: 2},
		logger:         logger,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	handler.preparePayloadLoop(ctx)

	require.Contains(t, output.String(), "Gloas preparation window is too short")
}

func TestPreparePayloadLoopRunsImmediatelyWithPreparationDeadline(t *testing.T) {
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
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(slotStart).AnyTimes()
	handler.ethClock = clock

	ctx, cancel := context.WithCancel(t.Context())
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(callCtx context.Context, _ common.Hash, _ *engine_types.PayloadAttributes) ([]byte, error) {
		deadline, ok := callCtx.Deadline()
		require.True(t, ok)
		require.Equal(t, slotStart.Add(-minimumPreparationLead), deadline)
		cancel()
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine

	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopWaitsForCurrentSlotHead(t *testing.T) {
	tests := []struct {
		name               string
		elapsed            time.Duration
		producedBlock      bool
		highestSeenCurrent bool
		blockProcessing    bool
		selectedCurrent    bool
		blockWork          bool
		shouldPrepare      bool
	}{
		{name: "before attestation deadline"},
		{name: "after empty-slot deadline", elapsed: 5 * time.Second, shouldPrepare: true},
		{name: "produced block awaiting publication", elapsed: 5 * time.Second, producedBlock: true},
		{name: "rejected current block in highest-seen does not suppress fallback", elapsed: 5 * time.Second, highestSeenCurrent: true, shouldPrepare: true},
		{name: "current block is still being processed", elapsed: 5 * time.Second, blockProcessing: true},
		{name: "current head with block work", selectedCurrent: true, blockWork: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			_, _, _, _, postState, handler, _, syncedData, forkchoiceStore, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
			config := *handler.beaconChainCfg
			config.SecondsPerSlot = 12
			config.IntervalsPerSlot = 3
			handler.beaconChainCfg = &config

			currentSlot := postState.Slot() + 1
			targetSlot := currentSlot + 1
			if test.producedBlock {
				handler.payloadPreparationGate.noteProducedBlock(currentSlot, currentSlot)
			}
			if test.highestSeenCurrent {
				forkchoiceStore.HighestSeenVal = currentSlot
			}
			forkchoiceStore.BlockProcessingVal = test.blockProcessing
			if test.blockWork {
				finishBlockWork := handler.payloadPreparationGate.beginBlockWork()
				defer finishBlockWork()
			}
			advancedState, err := postState.Copy()
			require.NoError(t, err)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
			proposerIndex, err := advancedState.GetBeaconProposerIndexForSlot(targetSlot)
			require.NoError(t, err)
			validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

			baseBlockRoot := common.Hash{0x41}
			selectedSlot := postState.Slot()
			if test.selectedCurrent {
				selectedSlot = currentSlot
			}
			currentSlotStart := time.Now().Add(-test.elapsed)
			runPayloadPreparationLoop(t, ctrl, payloadPreparationLoopRun{
				handler:         handler,
				syncedData:      syncedData.(*sync_mock_services.MockSyncedData),
				headState:       postState,
				baseBlockRoot:   baseBlockRoot,
				selectedSlot:    selectedSlot,
				currentSlot:     currentSlot,
				targetSlot:      targetSlot,
				targetSlotStart: currentSlotStart.Add(12 * time.Second),
				shouldPrepare:   test.shouldPrepare,
			})
		})
	}
}

func TestPreparePayloadLoopPrimesGloasAfterPayloadDecision(t *testing.T) {
	for _, test := range []struct {
		name            string
		elapsed         time.Duration
		staleHead       bool
		invalidatedHead bool
		reorgToEmpty    bool
		timeout         time.Duration
		shouldPrepare   bool
	}{
		{name: "older head after empty-slot deadline", elapsed: 4 * time.Second, staleHead: true, shouldPrepare: true},
		{name: "wakeup at PTC deadline", elapsed: 8900 * time.Millisecond, timeout: 2 * time.Second, shouldPrepare: true},
		{name: "invalidated head after PTC deadline", elapsed: 9100 * time.Millisecond, invalidatedHead: true, shouldPrepare: true},
		{name: "FULL head with EMPTY decision", elapsed: 9100 * time.Millisecond, reorgToEmpty: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			postState, handler, syncedData, forkchoiceStore, validatorParams := setupGloasPreparationTest(t)
			currentSlot := postState.Slot()
			targetSlot := currentSlot + 1
			baseBlockRoot := common.Hash{0x41}
			forkchoiceStore.HeadVal = baseBlockRoot
			parentHash := common.Hash{0xa1}
			postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
				ParentBlockHash: parentHash,
				BlockHash:       common.Hash{0xb2},
				GasLimit:        30_000_000,
				Slot:            currentSlot,
			})
			forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
			forkchoiceStore.HeadPayloadStatusInvalidated = test.invalidatedHead
			if test.reorgToEmpty {
				forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusFull
				buildOnFull := false
				forkchoiceStore.ShouldBuildOnFullVal = &buildOnFull
			}
			proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
			require.NoError(t, err)
			validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

			selectedSlot := currentSlot
			if test.staleHead {
				selectedSlot--
			}
			currentSlotStart := time.Now().Add(-test.elapsed)
			runPayloadPreparationLoop(t, ctrl, payloadPreparationLoopRun{
				handler:         handler,
				syncedData:      syncedData.(*sync_mock_services.MockSyncedData),
				headState:       postState,
				baseBlockRoot:   baseBlockRoot,
				selectedSlot:    selectedSlot,
				currentSlot:     currentSlot,
				targetSlot:      targetSlot,
				targetSlotStart: currentSlotStart.Add(12 * time.Second),
				timeout:         test.timeout,
				shouldPrepare:   test.shouldPrepare,
				checkBuild: func(head common.Hash, _ *engine_types.PayloadAttributes) {
					require.Equal(t, parentHash, head)
				},
			})
		})
	}
}

func TestPreparePayloadLoopUsesCurrentForkTimingAtGloasTransition(t *testing.T) {
	for _, test := range []struct {
		name            string
		elapsed         time.Duration
		currentInGloas  bool
		selectedCurrent bool
		shouldPrepare   bool
	}{
		{name: "current Fulu head", elapsed: 8 * time.Second, selectedCurrent: true, shouldPrepare: true},
		{name: "stale Fulu head before Fulu deadline", elapsed: 4 * time.Second},
		{name: "Fulu head in first Gloas slot", elapsed: 8 * time.Second, currentInGloas: true, shouldPrepare: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(
				t, clparams.ElectraVersion, log.Root(), false,
			)
			config := *handler.beaconChainCfg
			config.SecondsPerSlot = 24
			config.IntervalsPerSlot = 3
			currentEpoch := postState.Slot() / config.SlotsPerEpoch
			config.FuluForkEpoch = currentEpoch
			config.GloasForkEpoch = currentEpoch + 1
			config.InitializeForkSchedule()
			handler.beaconChainCfg = &config

			headState := state.New(&config)
			require.NoError(t, postState.CopyInto(headState))
			require.NoError(t, headState.UpgradeToFulu())
			lastFuluSlot := config.GloasForkEpoch*config.SlotsPerEpoch - 1
			currentSlot := lastFuluSlot
			if test.currentInGloas {
				currentSlot++
			}
			targetSlot := currentSlot + 1
			require.NoError(t, transition.DefaultMachine.ProcessSlots(headState, lastFuluSlot))
			preForkHead := common.Hash{0xa1}
			executionHeader := headState.LatestExecutionPayloadHeader()
			executionHeader.BlockHash = preForkHead
			headState.SetLatestExecutionPayloadHeader(executionHeader)

			proposerIndex, err := headState.GetBeaconProposerIndexForSlot(targetSlot)
			require.NoError(t, err)
			validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})
			baseBlockRoot := common.Hash{0x41}
			selectedSlot := lastFuluSlot - 1
			if test.currentInGloas {
				selectedSlot = lastFuluSlot
			}
			if test.selectedCurrent {
				selectedSlot = currentSlot
			}
			currentSlotStart := time.Now().Add(-test.elapsed)
			runPayloadPreparationLoop(t, ctrl, payloadPreparationLoopRun{
				handler:         handler,
				syncedData:      syncedData.(*sync_mock_services.MockSyncedData),
				headState:       headState,
				baseBlockRoot:   baseBlockRoot,
				selectedSlot:    selectedSlot,
				currentSlot:     currentSlot,
				targetSlot:      targetSlot,
				targetSlotStart: currentSlotStart.Add(24 * time.Second),
				timeout:         time.Second,
				shouldPrepare:   test.shouldPrepare,
				checkBuild: func(head common.Hash, attrs *engine_types.PayloadAttributes) {
					require.Equal(t, preForkHead, head)
					require.NotNil(t, attrs.SlotNumber)
					require.Equal(t, hexutil.Uint64(targetSlot), *attrs.SlotNumber)
				},
			})
		})
	}
}

func TestPreparePayloadLoopMemoizesThePreferenceGenerationItUsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(
		t, clparams.ElectraVersion, log.Root(), false,
	)
	config := *handler.beaconChainCfg
	config.SecondsPerSlot = 4
	config.IntervalsPerSlot = 3
	currentEpoch := postState.Slot() / config.SlotsPerEpoch
	config.FuluForkEpoch = currentEpoch
	config.GloasForkEpoch = currentEpoch + 1
	config.InitializeForkSchedule()
	handler.beaconChainCfg = &config

	headState := state.New(&config)
	require.NoError(t, postState.CopyInto(headState))
	require.NoError(t, headState.UpgradeToFulu())
	currentSlot := config.GloasForkEpoch*config.SlotsPerEpoch - 1
	targetSlot := currentSlot + 1
	require.NoError(t, transition.DefaultMachine.ProcessSlots(headState, currentSlot))
	const defaultGasLimit = uint64(30_000_000)
	executionHeader := headState.LatestExecutionPayloadHeader()
	executionHeader.BlockHash = common.Hash{0xa1}
	executionHeader.GasLimit = defaultGasLimit
	headState.SetLatestExecutionPayloadHeader(executionHeader)

	proposerIndex, err := headState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})
	targetState, err := headState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(targetState, targetSlot))
	dependentRoot, err := state.GetProposerDependentRoot(targetState, targetSlot/config.SlotsPerEpoch)
	require.NoError(t, err)

	handler.epbsPool = pool.NewEpbsPool()
	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, currentSlot, true).AnyTimes()
	syncedDataMock.EXPECT().HeadRoot().Return(baseBlockRoot).AnyTimes()
	viewCount := 0
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			viewCount++
			if viewCount == 1 {
				handler.epbsPool.AddProposerPreference(&cltypes.SignedProposerPreferences{
					Message: &cltypes.ProposerPreferences{
						ProposalSlot:   targetSlot,
						DependentRoot:  dependentRoot,
						ValidatorIndex: proposerIndex,
						TargetGasLimit: 36_000_000,
					},
				})
			}
			return view(headState, baseBlockRoot, currentSlot)
		}).AnyTimes()

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(2500 * time.Millisecond)).AnyTimes()
	handler.ethClock = clock

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var builtGasLimits []uint64
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(_ context.Context, _ common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
		require.NotNil(t, attrs.TargetGasLimit)
		builtGasLimits = append(builtGasLimits, uint64(*attrs.TargetGasLimit))
		if len(builtGasLimits) == 1 {
			// Fill the cache with other slots so the preference used by this attempt is evicted and
			// its generation is pruned back to zero before the attempt settles.
			for offset := uint64(1); offset <= 64; offset++ {
				handler.epbsPool.AddProposerPreference(&cltypes.SignedProposerPreferences{
					Message: &cltypes.ProposerPreferences{
						ProposalSlot:  targetSlot + offset,
						DependentRoot: common.Hash{byte(offset)},
					},
				})
			}
			_, found := handler.epbsPool.GetPreference(targetSlot, dependentRoot)
			require.False(t, found)
			require.Zero(t, handler.epbsPool.ProposerPreferencesGeneration(targetSlot))
		} else {
			cancel()
		}
		return bytes.Repeat([]byte{byte(len(builtGasLimits))}, 8), nil
	}
	handler.engine = engine

	handler.preparePayloadLoop(ctx)

	require.Equal(t, []uint64{36_000_000, defaultGasLimit}, builtGasLimits)
}

func TestPreparePayloadLoopMemoizesTheGloasPathItBuilt(t *testing.T) {
	ctrl := gomock.NewController(t)
	postState, handler, syncedData, forkchoiceStore, validatorParams := setupGloasPreparationTest(t)
	config := *handler.beaconChainCfg
	config.SecondsPerSlot = 6
	config.IntervalsPerSlot = 3
	handler.beaconChainCfg = &config

	currentSlot := postState.Slot()
	targetSlot := currentSlot + 1
	baseBlockRoot := common.Hash{0x41}
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xa1},
		BlockHash:       common.Hash{0xb2},
		GasLimit:        30_000_000,
		Slot:            currentSlot,
	})
	forkchoiceStore.HeadVal = baseBlockRoot
	forkchoiceStore.Envelopes[baseBlockRoot] = &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion),
		},
	}
	statusReads := 0
	forkchoiceStore.GetHeadPayloadStatusFn = func(root common.Hash) (cltypes.PayloadStatus, bool) {
		require.Equal(t, baseBlockRoot, root)
		statusReads++
		if statusReads == 1 {
			return cltypes.PayloadStatusEmpty, true
		}
		return cltypes.PayloadStatusFull, true
	}

	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, currentSlot, true).AnyTimes()
	syncedDataMock.EXPECT().HeadRoot().Return(baseBlockRoot).AnyTimes()
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, currentSlot)
		}).AnyTimes()

	clock := eth_clock.NewMockEthereumClock(ctrl)
	targetSlotStart := time.Now().Add(4200 * time.Millisecond)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(targetSlotStart).AnyTimes()
	handler.ethClock = clock

	ctx, cancel := context.WithTimeout(t.Context(), 3500*time.Millisecond)
	defer cancel()
	buildCount := 0
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		buildCount++
		if buildCount > 1 {
			cancel()
		}
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine

	handler.preparePayloadLoop(ctx)

	require.Equal(t, 1, buildCount, "the settled key must use the path that produced the payload ID")
}

func TestInvalidProductionRequestDoesNotWaitForPreparation(t *testing.T) {
	handler := &ApiHandler{}
	finishPreparation, ok := handler.payloadPreparationGate.tryBeginPreparation()
	require.True(t, ok)

	result := make(chan error, 1)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v3/validator/blocks/1?randao_reveal=invalid", nil)
	go func() {
		_, err := handler.GetEthV3ValidatorBlock(httptest.NewRecorder(), request)
		result <- err
	}()

	select {
	case err := <-result:
		finishPreparation()
		require.ErrorContains(t, err, "invalid randao_reveal")
	case <-time.After(2 * time.Second):
		finishPreparation()
		awaitErrorResult(t, result)
		t.Fatal("request validation waited for payload preparation")
	}
}

func TestPublishedBlockStorageSuppressesStaleHeadPreparation(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage := blob_storage_mock.NewMockBlobStorage(ctrl)
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	finishWrite := sync.OnceFunc(func() { close(releaseWrite) })
	t.Cleanup(finishWrite)
	writeReturned := make(chan struct{})
	persistenceErr := errors.New("stop after persistence")
	storage.EXPECT().WriteBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, common.Hash, []*cltypes.BlobSidecar) error {
			close(writeStarted)
			<-releaseWrite
			close(writeReturned)
			return persistenceErr
		})
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	handler.blobStoage = storage
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.ElectraVersion)

	require.NoError(t, handler.broadcastBlock(t.Context(), block))
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("published block storage did not start")
	}

	finishPreparation, preparationStarted := handler.payloadPreparationGate.tryBeginPreparation()
	if preparationStarted {
		finishPreparation()
	}
	require.False(t, preparationStarted, "published block storage must suppress preparation")
	finishWrite()
	select {
	case <-writeReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("published block storage did not finish")
	}
	require.Eventually(t, func() bool {
		finishPreparation, ok := handler.payloadPreparationGate.tryBeginPreparation()
		if ok {
			finishPreparation()
		}
		return ok
	}, time.Second, 10*time.Millisecond)
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

	engine := newPayloadBuildEngine(t, ctrl)
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
	finishBlockWork := handler.payloadPreparationGate.beginBlockWork()
	defer finishBlockWork()
	engine := newPayloadBuildEngine(t, ctrl)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopSkipsSlotsThatHaveStarted(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// Production owns the target once its slot starts. Preparation must not begin new state or
	// execution work for it.
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(-time.Millisecond)).AnyTimes()
	handler.ethClock = clock

	engine := newPayloadBuildEngine(t, ctrl)
	handler.engine = engine

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadLoopSkipsSlotsWithInsufficientLead(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true).AnyTimes()
	syncedDataMock.EXPECT().HeadRoot().Return(baseBlockRoot).AnyTimes()
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).Times(0)

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(minimumPreparationLead / 2)).AnyTimes()
	handler.ethClock = clock
	handler.engine = newPayloadBuildEngine(t, ctrl)

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

// A deterministic rejection cannot change while the slot, selected head, and validator
// registrations stay fixed. Retrying it on every tick would repeat the expensive state work.
func TestPreparePayloadLoopMemoizesStableFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	// A registration for another validator starts the loop but leaves this proposal unregistered.
	validatorParams.SetFeeRecipient(proposerIndex+1, common.Address{0x11})

	config := *handler.beaconChainCfg
	config.SecondsPerSlot = 1
	config.IntervalsPerSlot = 3
	handler.beaconChainCfg = &config
	baseBlockRoot := common.Hash{0x41}
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true).AnyTimes()
	syncedDataMock.EXPECT().HeadRoot().Return(baseBlockRoot).AnyTimes()
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		}).Times(1)

	engine := newPayloadBuildEngine(t, ctrl)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(targetSlot - 1).AnyTimes()
	clock.EXPECT().GetSlotTime(targetSlot).DoAndReturn(func(uint64) time.Time {
		return time.Now().Add(800 * time.Millisecond)
	}).AnyTimes()
	handler.ethClock = clock

	ctx, cancel := context.WithTimeout(t.Context(), 650*time.Millisecond)
	defer cancel()
	handler.preparePayloadLoop(ctx)
}

func TestPreparePayloadForStartsBuildWithCompleteAttributes(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), true)
	config := *handler.beaconChainCfg
	targetEpoch := postState.Slot()/config.SlotsPerEpoch + 1
	targetSlot := targetEpoch * config.SlotsPerEpoch
	config.DenebForkEpoch = targetEpoch
	config.InitializeForkSchedule()

	headState := state.New(&config)
	require.NoError(t, postState.CopyInto(headState))
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

	require.Equal(t, clparams.DenebVersion,
		handler.beaconChainCfg.GetCurrentStateVersion(targetSlot/handler.beaconChainCfg.SlotsPerEpoch))
	expectedWithdrawals, err := state.GetExpectedWithdrawals(advancedState, targetSlot/handler.beaconChainCfg.SlotsPerEpoch)
	require.NoError(t, err)

	payloadID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(_ context.Context, head common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
		require.Equal(t, advancedState.LatestExecutionPayloadHeader().BlockHash, head)
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
	}
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	var scratch payloadPreparationScratch
	result, err := handler.preparePayloadForWithScratch(t.Context(), targetSlot, &scratch)
	require.NoError(t, err)
	require.Equal(t, baseBlockRoot, result.headRoot)
	require.Positive(t, preparedWarmup(&handler.preparedPayload, targetSlot, payloadID, time.Now().Add(time.Second)))
}

func TestPreparePayloadForStartsGloasSelfBuildOnFullParent(t *testing.T) {
	ctrl := gomock.NewController(t)
	postState, handler, syncedData, forkchoiceStore, validatorParams := setupGloasPreparationTest(t)
	targetSlot := postState.Slot() + 1
	baseBlockRoot := common.Hash{0x41}
	forkchoiceStore.HeadVal = baseBlockRoot
	parentHash := common.Hash{0xa1}
	fullHash := common.Hash{0xb2}
	const defaultGasLimit = uint64(30_000_000)
	const preferredGasLimit = uint64(36_000_000)
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentHash,
		BlockHash:       fullHash,
		GasLimit:        defaultGasLimit,
		Slot:            postState.Slot(),
	})
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	forkchoiceStore.Envelopes[baseBlockRoot] = &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion),
		},
	}

	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	feeRecipient := common.Address{0x11}
	validatorParams.SetFeeRecipient(proposerIndex, feeRecipient)
	advancedState, err := postState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, targetSlot))
	dependentRoot, err := state.GetProposerDependentRoot(advancedState, targetSlot/handler.beaconChainCfg.SlotsPerEpoch)
	require.NoError(t, err)
	handler.epbsPool = pool.NewEpbsPool()
	handler.epbsPool.AddProposerPreference(&cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: targetSlot, DependentRoot: dependentRoot, ValidatorIndex: proposerIndex,
		TargetGasLimit: preferredGasLimit,
	}})

	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		})
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true).AnyTimes()

	payloadID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(_ context.Context, head common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
		require.Equal(t, fullHash, head)
		require.Equal(t, feeRecipient, attrs.SuggestedFeeRecipient)
		require.Equal(t, &baseBlockRoot, attrs.ParentBeaconBlockRoot)
		require.NotNil(t, attrs.Withdrawals)
		require.NotNil(t, attrs.SlotNumber)
		require.NotNil(t, attrs.TargetGasLimit)
		require.Equal(t, hexutil.Uint64(targetSlot), *attrs.SlotNumber)
		require.Equal(t, hexutil.Uint64(preferredGasLimit), *attrs.TargetGasLimit)
		return payloadID, nil
	}
	handler.engine = engine
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	primedHead, err := preparePayloadForTest(t, handler, targetSlot)
	require.NoError(t, err)
	require.Equal(t, baseBlockRoot, primedHead)
	require.Positive(t, preparedWarmup(&handler.preparedPayload, targetSlot, payloadID, time.Now().Add(time.Second)))
}

func TestPreparePayloadForWaitsForResolvedGloasHead(t *testing.T) {
	ctrl := gomock.NewController(t)
	postState, handler, syncedData, forkchoiceStore, validatorParams := setupGloasPreparationTest(t)
	targetSlot := postState.Slot() + 1
	baseBlockRoot := common.Hash{0x41}
	forkchoiceStore.HeadVal = baseBlockRoot
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xa1},
		BlockHash:       common.Hash{0xb2},
		Slot:            postState.Slot(),
	})
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusPending
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		})

	buildStarted := false
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		buildStarted = true
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = preparePayloadForTest(t, handler, targetSlot)
	require.ErrorIs(t, err, errGloasPayloadPending)
	require.False(t, buildStarted)
}

func TestPreparePayloadForSkipsGloasReorgToEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	postState, handler, syncedData, forkchoiceStore, validatorParams := setupGloasPreparationTest(t)
	targetSlot := postState.Slot() + 1
	baseBlockRoot := common.Hash{0x41}
	forkchoiceStore.HeadVal = baseBlockRoot
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xa1},
		BlockHash:       common.Hash{0xb2},
		Slot:            postState.Slot(),
	})
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	buildOnFull := false
	forkchoiceStore.ShouldBuildOnFullVal = &buildOnFull
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, baseBlockRoot, postState.Slot())
		})

	buildStarted := false
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		buildStarted = true
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = preparePayloadForTest(t, handler, targetSlot)
	require.ErrorIs(t, err, errGloasReorgToEmpty)
	require.False(t, buildStarted)
}

func TestExecutionPayloadSourceMarksReorgToEmptyAfterNegativePtcDecision(t *testing.T) {
	postState, handler, _, forkchoiceStore, _ := setupGloasPreparationTest(t)
	targetSlot := postState.Slot() + 1
	baseBlockRoot := common.Hash{0x41}
	forkchoiceStore.HeadVal = baseBlockRoot
	parentHash := common.Hash{0xa1}
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentHash,
		BlockHash:       common.Hash{0xb2},
		Slot:            postState.Slot(),
	})
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	buildOnFull := false
	forkchoiceStore.ShouldBuildOnFullVal = &buildOnFull

	source, err := handler.resolveExecutionPayloadSource(postState, baseBlockRoot, targetSlot, clparams.GloasVersion)

	require.NoError(t, err)
	require.Equal(t, parentHash, source.head)
	require.Equal(t, gloasPayloadPathReorgToEmpty, source.gloasPath)
	require.Nil(t, source.parentExecutionRequests)
}

func TestExecutionPayloadSourceRejectsPendingGloasDecision(t *testing.T) {
	postState, handler, _, forkchoiceStore, _ := setupGloasPreparationTest(t)
	targetSlot := postState.Slot() + 1
	baseBlockRoot := common.Hash{0x41}
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xa1},
		BlockHash:       common.Hash{0xb2},
		Slot:            postState.Slot(),
	})
	forkchoiceStore.HeadVal = baseBlockRoot
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusPending

	source, err := handler.resolveExecutionPayloadSource(postState, baseBlockRoot, targetSlot, clparams.GloasVersion)

	require.ErrorIs(t, err, errGloasPayloadPending)
	require.Equal(t, executionPayloadSource{}, source)
}

func TestExecutionPayloadSourceRefreshesInvalidatedGloasHead(t *testing.T) {
	postState, handler, _, forkchoiceStore, _ := setupGloasPreparationTest(t)
	targetSlot := postState.Slot() + 1
	baseBlockRoot := common.Hash{0x41}
	fullHash := common.Hash{0xb2}
	postState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.Hash{0xa1},
		BlockHash:       fullHash,
		Slot:            postState.Slot(),
	})
	forkchoiceStore.HeadVal = baseBlockRoot
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	forkchoiceStore.HeadPayloadStatusInvalidated = true
	forkchoiceStore.Envelopes[baseBlockRoot] = &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion),
		},
	}

	source, err := handler.resolveExecutionPayloadSource(postState, baseBlockRoot, targetSlot, clparams.GloasVersion)

	require.NoError(t, err)
	require.Equal(t, fullHash, source.head)
	require.Equal(t, gloasPayloadPathFull, source.gloasPath)
}

func setupGloasPreparationTest(t *testing.T) (
	*state.CachingBeaconState,
	*ApiHandler,
	synced_data.SyncedData,
	*mock_services.ForkChoiceStorageMock,
	*validator_params.ValidatorParams,
) {
	t.Helper()
	_, _, _, _, postState, handler, _, syncedData, forkchoiceStore, validatorParams := setupTestingHandler(
		t, clparams.ElectraVersion, log.Root(), false,
	)
	require.NoError(t, postState.UpgradeToFulu())
	require.NoError(t, postState.UpgradeToGloas())
	config := *handler.beaconChainCfg
	currentEpoch := postState.Slot() / config.SlotsPerEpoch
	config.FuluForkEpoch = currentEpoch
	config.GloasForkEpoch = currentEpoch
	config.InitializeForkSchedule()
	handler.beaconChainCfg = &config
	return postState, handler, syncedData, forkchoiceStore, validatorParams
}

func TestPreparationGateRefusesActiveBlockWork(t *testing.T) {
	var gate payloadPreparationGate
	finishBlockWork := gate.beginBlockWork()
	defer finishBlockWork()

	_, ok := gate.tryBeginPreparation()

	require.False(t, ok, "preparation must stand off active block work")
}

func TestPreparationGateBlockWorkReleaseIsIdempotent(t *testing.T) {
	var gate payloadPreparationGate
	finishBlockWork := gate.beginBlockWork()

	finishBlockWork()
	require.NotPanics(t, finishBlockWork)
	finishPreparation, ok := gate.tryBeginPreparation()
	require.True(t, ok)
	finishPreparation()
}

func TestPayloadBuildPanicReleasesPreparationGate(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	baseBlockRoot := common.Hash{0x41}
	syncedData.(*sync_mock_services.MockSyncedData).EXPECT().SelectedHead().Return(baseBlockRoot, uint64(10), true)
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		panic("payload builder panic")
	}
	handler.engine = engine

	require.Panics(t, func() {
		_, _ = handler.startPayloadBuildForPreparation(t.Context(), baseBlockRoot, common.Hash{0x42}, new(engine_types.PayloadAttributes))
	})

	finishPreparation, ok := handler.payloadPreparationGate.tryBeginPreparation()
	require.True(t, ok, "a failed builder attempt must not strand the preparation gate")
	finishPreparation()
}

func TestPreparationGateTracksProducedBlockUntilSlotHeadIsSelected(t *testing.T) {
	var gate payloadPreparationGate
	gate.noteProducedBlock(9, 10)

	require.True(t, gate.producedBlockPending(9, 9), "an early production is pending")
	require.True(t, gate.producedBlockPending(10, 9), "the signing round trip is pending")
	require.False(t, gate.producedBlockPending(10, 10), "a head for the produced slot is selected")
	require.False(t, gate.producedBlockPending(11, 10), "the produced slot has passed")
}

func TestPreparationGateTracksProductionThatFinishesAfterItsSlot(t *testing.T) {
	var gate payloadPreparationGate
	gate.noteProducedBlock(11, 10)

	require.True(t, gate.producedBlockPending(11, 9), "the late signing round trip is pending")
	require.False(t, gate.producedBlockPending(11, 10), "a head for the produced slot is selected")
	require.False(t, gate.producedBlockPending(12, 9), "the signing marker expires after one slot")
}

func TestPreparationGateIgnoresProducedBlocksOutsideCurrentWindow(t *testing.T) {
	var gate payloadPreparationGate
	gate.noteProducedBlock(10, 20)

	require.False(t, gate.producedBlockPending(19, 10), "a far-future request must not schedule a later preparation blackout")
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

	engine := newPayloadBuildEngine(t, ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			require.Equal(t, targetMix, common.Hash(attrs.PrevRandao))
			return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
		})
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil, nil, errors.New("collection stops here")).AnyTimes()
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(-10 * time.Second)).AnyTimes()
	clock.EXPECT().GetCurrentEpoch().Times(0)
	handler.ethClock = clock

	_, _, err := handler.produceBeaconBody(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		targetSlot, common.Bytes96{}, common.Hash{})

	require.Error(t, err)
}

func TestProductionRejectsMissingBlobsBundle(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	payloadID := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(payloadID, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), payloadID, clparams.ElectraVersion).
		Return(cltypes.NewEth1Block(clparams.ElectraVersion, handler.beaconChainCfg), nil, nil, nil, nil)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(-10 * time.Second))
	handler.ethClock = clock

	_, _, err := handler.produceBeaconBody(t.Context(), 1, postState.Slot(), common.Hash{0x41}, postState,
		targetSlot, common.Bytes96{}, common.Hash{})

	require.ErrorContains(t, err, "execution layer returned no blobs bundle")
}

func TestProductionUsesPreparedWarmupForPayloadCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x42})

	config := *handler.beaconChainCfg
	config.SecondsPerSlot = 40
	config.IntervalsPerSlot = 4
	handler.beaconChainCfg = &config
	payloadID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	handler.preparedPayload.set(targetSlot, payloadID, common.Hash{}, time.Now().Add(-10*time.Second))

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	collectionStarted := false
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(payloadID, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), payloadID, clparams.ElectraVersion).
		DoAndReturn(func(context.Context, []byte, clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			collectionStarted = true
			cancel()
			return nil, nil, nil, nil, context.Canceled
		})
	handler.engine = engine

	// With the five-second warmup cap, this slot collects immediately. Ignoring warmup would delay
	// the first poll by more than four seconds, beyond the request context.
	slotStart := time.Now().Add(-3 * time.Second)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(slotStart)
	handler.ethClock = clock

	_, _, err = handler.produceBeaconBody(ctx, 1, postState.Slot(), common.Hash{0x41}, postState,
		targetSlot, common.Bytes96{}, common.Hash{})

	require.ErrorIs(t, err, context.Canceled)
	require.True(t, collectionStarted, "prepared warmup was not applied to the production collection window")
}

func TestProductionLogsPreparedPayloadIDMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	var output bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, logger, false)
	targetSlot := postState.Slot() + 1
	preparedID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	productionID := []byte{8, 7, 6, 5, 4, 3, 2, 1}
	preparedHead := common.Hash{0x51}
	productionHead := common.Hash{0x41}
	handler.preparedPayload.set(targetSlot, preparedID, preparedHead, time.Now().Add(-time.Second))

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(productionID, nil)
	engine.EXPECT().GetAssembledBlock(gomock.Any(), productionID, clparams.ElectraVersion).
		Return(nil, nil, nil, nil, errors.New("collection stops here"))
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(-10 * time.Second))
	handler.ethClock = clock

	_, _, err := handler.produceBeaconBody(t.Context(), 1, postState.Slot(), productionHead, postState,
		targetSlot, common.Bytes96{}, common.Hash{})

	require.Error(t, err)
	require.Contains(t, output.String(), "prepared payload ID did not match production")
	require.Contains(t, output.String(), "preparedHead")
	require.Contains(t, output.String(), preparedHead.Hex())
	require.Contains(t, output.String(), "productionHead")
	require.Contains(t, output.String(), productionHead.Hex())
	require.Contains(t, output.String(), "lvl=info")
}

// Builder startup is non-blocking, so temporary execution contention is retried outside the gate.
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

	engine := newPayloadBuildEngine(t, ctrl)
	buildAttempts := 0
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		buildAttempts++
		if buildAttempts == 1 {
			return nil, chainreader.ErrExecutionBusy
		}
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	head, err := preparePayloadForTest(t, handler, targetSlot)
	require.NoError(t, err)
	require.Equal(t, baseBlockRoot, head)
	require.Equal(t, 2, buildAttempts)
}

func TestPreparePayloadBuildBacksOffWhileExecutionHeadDiffers(t *testing.T) {
	ctrl := gomock.NewController(t)
	handler := &ApiHandler{}
	baseBlockRoot := common.Hash{0x41}
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	syncedData.EXPECT().SelectedHead().Return(baseBlockRoot, uint64(10), true).AnyTimes()
	handler.syncedData = syncedData

	buildAttempts := 0
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		buildAttempts++
		if buildAttempts == 1 {
			return nil, execution_client.ErrPayloadBuildHeadMismatch
		}
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	minimumObservedBackoff := 400 * time.Millisecond
	startedAt := time.Now()

	payloadID, err := handler.startPayloadBuildForPreparation(
		ctx, baseBlockRoot, common.Hash{0x42}, new(engine_types.PayloadAttributes),
	)

	require.NoError(t, err)
	require.Len(t, payloadID, 8)
	require.Equal(t, 2, buildAttempts)
	require.GreaterOrEqual(t, time.Since(startedAt), minimumObservedBackoff)
}

func TestPreparePayloadForStopsRetryWhenTheHeadChanges(t *testing.T) {
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
	movedBlockRoot := common.Hash{0x42}
	gomock.InOrder(
		syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true),
		syncedDataMock.EXPECT().SelectedHead().Return(movedBlockRoot, postState.Slot()+1, true),
	)

	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(context.Context, common.Hash, *engine_types.PayloadAttributes) ([]byte, error) {
		return nil, chainreader.ErrExecutionBusy
	}
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = preparePayloadForTest(t, handler, targetSlot)
	require.ErrorIs(t, err, errPreparationHeadChanged)
}

// Production waits only for a builder-start attempt that has already entered the execution layer.
// The attempt is one non-blocking call; state copying and contention retries happen outside it.
func TestProductionWaitsForActivePreparationAttempt(t *testing.T) {
	var gate payloadPreparationGate
	finishPreparation, ok := gate.tryBeginPreparation()
	require.True(t, ok)

	productionStarted := make(chan func(), 1)
	go func() {
		productionStarted <- gate.beginBlockWork()
	}()

	select {
	case finishBlockWork := <-productionStarted:
		finishBlockWork()
		t.Fatal("production started during a payload-build attempt")
	case <-time.After(50 * time.Millisecond):
	}

	finishPreparation()
	select {
	case finishBlockWork := <-productionStarted:
		finishBlockWork()
	case <-time.After(5 * time.Second):
		t.Fatal("production did not start after payload preparation finished")
	}
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
			select {
			case <-resumePreparation:
			case <-t.Context().Done():
			}
			return err
		})
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true)
	engine := newPayloadBuildEngine(t, ctrl)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	result := make(chan error, 1)
	go func() {
		_, err := preparePayloadForTest(t, handler, targetSlot)
		result <- err
	}()
	select {
	case <-stateCopied:
	case <-time.After(5 * time.Second):
		t.Fatal("state copying did not finish")
	}
	finishBlockWork := handler.payloadPreparationGate.beginBlockWork()
	defer finishBlockWork()
	close(resumePreparation)

	require.ErrorIs(t, awaitErrorResult(t, result), errBlockWorkInFlight)
}

func TestPreparePayloadForStopsWhenStateWorkConsumesMinimumLead(t *testing.T) {
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
	engine := newPayloadBuildEngine(t, ctrl)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	gomock.InOrder(
		clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(minimumPreparationLead+time.Second)),
		clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(minimumPreparationLead/2)),
	)
	handler.ethClock = clock

	var scratch payloadPreparationScratch
	_, err = handler.preparePayloadForWithScratch(t.Context(), targetSlot, &scratch)
	require.ErrorIs(t, err, errPreparationTooLate)
}

func TestPreparePayloadForRejectsChangedHeadBeforeStartingBuilder(t *testing.T) {
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
		})
	syncedDataMock.EXPECT().SelectedHead().Return(common.Hash{0x42}, postState.Slot()+1, true)
	engine := newPayloadBuildEngine(t, ctrl)
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err = preparePayloadForTest(t, handler, targetSlot)
	require.ErrorIs(t, err, errPreparationHeadChanged)
	require.Zero(t, preparedWarmup(&handler.preparedPayload, targetSlot, []byte{1}, time.Now()))
}

func TestPreparePayloadForSkipsStateCopyWhenBlockWorkStarts(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	baseBlockRoot := common.Hash{0x41}
	var finishBlockWork func()
	syncedDataMock := syncedData.(*sync_mock_services.MockSyncedData)
	syncedDataMock.EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			finishBlockWork = handler.payloadPreparationGate.beginBlockWork()
			return view(postState, baseBlockRoot, postState.Slot())
		})
	syncedDataMock.EXPECT().SelectedHead().Return(baseBlockRoot, postState.Slot(), true).AnyTimes()

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock
	handler.engine = newPayloadBuildEngine(t, ctrl)
	var scratch payloadPreparationScratch

	_, err = handler.preparePayloadForWithScratch(t.Context(), targetSlot, &scratch)
	if finishBlockWork != nil {
		finishBlockWork()
	}

	require.ErrorIs(t, err, errBlockWorkInFlight)
	require.Nil(t, scratch.state, "block work announced before copying should avoid the state allocation")
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
	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(_ context.Context, _ common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
		require.Equal(t, common.Address{0x11}, attrs.SuggestedFeeRecipient)
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine

	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(gomock.Any()).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	_, err := preparePayloadForTest(t, handler, targetSlot)
	require.NoError(t, err)
}

func TestFuluPreparationRejectsUnregisteredProposerBeforeCopyingState(t *testing.T) {
	ctrl := gomock.NewController(t)
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
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	var preparationErr error
	allocations := testing.AllocsPerRun(1, func() {
		_, preparationErr = preparePayloadForTest(t, handler, targetSlot)
	})

	require.ErrorIs(t, preparationErr, errNotOurProposal)
	require.Less(t, allocations, 1000.0, "an unregistered Fulu proposer should not require a full state copy")
}

// Memoization must use the head paired with the state that produced the verdict, not an earlier
// selected-head snapshot.
func TestPreparePayloadForReturnsViewedHeadWhenProposalIsNotOurs(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	viewRoot := common.Hash{0x41}
	syncedData.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadStateWithIdentity(gomock.Any()).
		DoAndReturn(func(view synced_data.ViewHeadStateWithIdentityFn) error {
			return view(postState, viewRoot, postState.Slot())
		})
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(targetSlot).Return(time.Now().Add(6 * time.Second)).AnyTimes()
	handler.ethClock = clock

	head, err := preparePayloadForTest(t, handler, targetSlot)

	require.ErrorIs(t, err, errNotOurProposal)
	require.Equal(t, viewRoot, head)
}

func TestPreparePayloadForPairsRootAndStateFromOneView(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, syncedData, _, validatorParams := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)
	targetSlot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(targetSlot)
	require.NoError(t, err)
	validatorParams.SetFeeRecipient(proposerIndex, common.Address{0x11})

	// Root and state must come from one view. Separate reads could pair a new root with an old state
	// and produce payload inputs that production can never reproduce.
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

	engine := newPayloadBuildEngine(t, ctrl)
	engine.startPayloadBuild = func(_ context.Context, _ common.Hash, attrs *engine_types.PayloadAttributes) ([]byte, error) {
		require.NotNil(t, attrs.ParentBeaconBlockRoot)
		require.Equal(t, viewRoot, *attrs.ParentBeaconBlockRoot)
		return []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil
	}
	handler.engine = engine

	primedHead, err := preparePayloadForTest(t, handler, targetSlot)
	require.NoError(t, err)
	require.Equal(t, viewRoot, primedHead)
}

func TestPreparedPayloadKeepsConsecutiveSlots(t *testing.T) {
	var p preparedPayload
	first := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	second := []byte{2, 2, 2, 2, 2, 2, 2, 2}
	now := time.Unix(100, 0)

	// Consecutive proposals: priming slot 11 must not evict slot 10, whose block may still be
	// in production.
	p.set(10, first, common.Hash{}, now)
	p.set(11, second, common.Hash{}, now)
	require.Positive(t, preparedWarmup(&p, 10, first, now.Add(time.Second)))
	require.Positive(t, preparedWarmup(&p, 11, second, now.Add(time.Second)))

	// Records old enough that they can no longer be produced are dropped, so the map is bounded.
	p.set(10+preparedPayloadRetainSlots+1, []byte{3, 3, 3, 3, 3, 3, 3, 3}, common.Hash{}, now)
	require.Zero(t, preparedWarmup(&p, 10, first, now.Add(time.Second)))
}

func TestPreparedPayloadCopiesTheID(t *testing.T) {
	var p preparedPayload
	id := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	now := time.Unix(100, 0)

	p.set(10, id, common.Hash{}, now)
	id[0] = 0xff

	// The caller's buffer must not be able to invalidate, or forge, a later match.
	require.Positive(t, preparedWarmup(&p, 10, []byte{1, 2, 3, 4, 5, 6, 7, 8}, now.Add(time.Second)))
	require.Zero(t, preparedWarmup(&p, 10, id, now.Add(time.Second)))
}
