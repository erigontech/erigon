package services

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	synced_data_mock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func setupProposerPreferencesService(t *testing.T, ctrl *gomock.Controller) (*proposerPreferencesService, *synced_data_mock.MockSyncedData, *eth_clock.MockEthereumClock, *pool.EpbsPool, *forkchoice_mock.ForkChoiceStorageMock) {
	mockSyncedData := synced_data_mock.NewMockSyncedData(ctrl)
	ethClockMock := eth_clock.NewMockEthereumClock(ctrl)
	ethClockMock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
	ethClockMock.EXPECT().GetSlotTime(gomock.Any()).DoAndReturn(func(slot uint64) time.Time {
		return time.Unix(int64(slot*12), 0)
	}).AnyTimes()
	epbsPool := pool.NewEpbsPool()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 32
	beaconCfg.SlotsPerHistoricalRoot = 8192
	beaconCfg.MinSeedLookahead = 1
	beaconCfg.ValidatorRegistryLimit = 1024
	forkChoiceMock := &forkchoice_mock.ForkChoiceStorageMock{
		Headers:             map[common.Hash]*cltypes.BeaconBlockHeader{},
		StateAtBlockRootVal: map[common.Hash]*state2.CachingBeaconState{},
	}
	forkChoiceMock.StateAtBlockRootVal[testDependentRoot] = newProposerPreferencesState(&beaconCfg, map[uint64]uint64{96: 42, 100: 42})
	forkChoiceMock.Headers[testDependentRoot] = &cltypes.BeaconBlockHeader{Slot: 63}
	forkChoiceMock.HeadVal = testDependentRoot
	prevBlsVerify := blsVerify
	blsVerify = func(_ []byte, _ []byte, _ []byte) (bool, error) { return true, nil }
	t.Cleanup(func() { blsVerify = prevBlsVerify })

	service := &proposerPreferencesService{
		syncedDataManager: mockSyncedData,
		forkchoiceStore:   forkChoiceMock,
		ethClock:          ethClockMock,
		beaconCfg:         &beaconCfg,
		epbsPool:          epbsPool,
		now: func() time.Time {
			return ethClockMock.GetSlotTime(ethClockMock.GetCurrentSlot())
		},
	}

	return service, mockSyncedData, ethClockMock, epbsPool, forkChoiceMock
}

// testDependentRoot is a fixed dependent root used across tests.
var testDependentRoot = common.HexToHash("0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

func newProposerPreferencesState(cfg *clparams.BeaconChainConfig, proposers map[uint64]uint64) *state2.CachingBeaconState {
	s := state2.New(cfg)
	s.SetVersion(clparams.GloasVersion)
	if err := s.SetSlot(64); err != nil {
		panic(err)
	}
	for i := range 100 {
		var pk common.Bytes48
		pk[0] = byte(i)
		if err := s.AddValidator(solid.NewValidatorFromParameters(pk, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), 0); err != nil {
			panic(err)
		}
	}
	lookahead := solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch))
	for slot, validatorIndex := range proposers {
		epoch := slot / cfg.SlotsPerEpoch
		stateEpoch := s.Slot() / cfg.SlotsPerEpoch
		index := (epoch-stateEpoch)*cfg.SlotsPerEpoch + slot%cfg.SlotsPerEpoch
		lookahead.Set(int(index), validatorIndex)
	}
	s.SetProposerLookahead(lookahead)
	return s
}

func newTestSignedProposerPreferences(proposalSlot, validatorIndex uint64) *cltypes.SignedProposerPreferences {
	return &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   proposalSlot,
			ValidatorIndex: validatorIndex,
			FeeRecipient:   common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
			TargetGasLimit: 30_000_000,
			DependentRoot:  testDependentRoot,
		},
		Signature: common.Bytes96{},
	}
}

func TestProposerPreferencesServiceNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)

	names := service.Names()
	require.Len(t, names, 1)
	require.Equal(t, "proposer_preferences", names[0])
}

func TestProposerPreferencesServiceNilMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)

	// Test nil message
	err := service.ProcessMessage(context.Background(), nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil proposer preferences message")

	// Test message with nil inner message
	err = service.ProcessMessage(context.Background(), nil, &cltypes.SignedProposerPreferences{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil proposer preferences message")
}

func TestProposerPreferencesServiceRejectsUnrepresentableSlotBeforeState(t *testing.T) {
	for _, test := range []struct {
		name           string
		proposalSlot   uint64
		secondsPerSlot uint64
	}{
		{name: "slot maximum", proposalSlot: math.MaxUint64, secondsPerSlot: 12},
		{name: "unix second overflow", proposalSlot: math.MaxInt64/12 + 1, secondsPerSlot: 12},
		{name: "zero seconds per slot", proposalSlot: 100, secondsPerSlot: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
			cfg := clparams.MainnetBeaconConfig
			cfg.SecondsPerSlot = test.secondsPerSlot
			cfg.SlotsPerEpoch = 32
			cfg.MinSeedLookahead = 1
			stateCalls := 0
			fc := forkchoice_mock.NewForkChoiceStorageMock(t)
			fc.GetStateAtBlockRootFn = func(common.Hash, bool) (*state2.CachingBeaconState, error) {
				stateCalls++
				return nil, nil
			}
			service := &proposerPreferencesService{
				forkchoiceStore: fc,
				ethClock:        clock,
				beaconCfg:       &cfg,
				epbsPool:        pool.NewEpbsPool(),
				now:             func() time.Time { return time.Unix(0, 0) },
			}

			err := service.ProcessMessage(context.Background(), nil, newTestSignedProposerPreferences(test.proposalSlot, 42))
			require.ErrorIs(t, err, ErrIgnore)
			require.Zero(t, stateCalls)
		})
	}
}

func TestProposerPreferencesServiceLookaheadNotKnown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(50))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not yet known")
}

func TestProposerPreferencesServiceCurrentEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)

	// proposalEpoch == currentEpoch (same epoch) → should be accepted now
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90)) // slot not yet passed

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify stored in pool
	stored, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg, stored)
}

func TestProposerPreferencesServiceSlotAlreadyPassed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	// proposalSlot=100, currentSlot=105 → slot already passed → IGNORE
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(105))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already passed")
}

func TestProposerPreferencesServiceCurrentSlotWithinDisparityAccepted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
}

func TestIsPastSlotClockDisparityBoundaries(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)
	start := service.ethClock.GetSlotTime(100)

	past, valid := isPastSlot(service.ethClock, service.beaconCfg, start.Add(gloasMaximumClockDisparity-time.Millisecond), 100, gloasMaximumClockDisparity)
	require.True(t, valid)
	require.False(t, past)
	past, valid = isPastSlot(service.ethClock, service.beaconCfg, start.Add(gloasMaximumClockDisparity), 100, gloasMaximumClockDisparity)
	require.True(t, valid)
	require.False(t, past)
	past, valid = isPastSlot(service.ethClock, service.beaconCfg, start.Add(gloasMaximumClockDisparity+time.Millisecond), 100, gloasMaximumClockDisparity)
	require.True(t, valid)
	require.True(t, past)
}

func TestProposerPreferencesServiceAcceptsEpochRolloverDisparityEdge(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, epbsPool, _ := setupProposerPreferencesService(t, ctrl)
	msg := newTestSignedProposerPreferences(96, 42)
	service.now = func() time.Time {
		return service.ethClock.GetSlotTime(96).Add(gloasMaximumClockDisparity)
	}

	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	stored, ok := epbsPool.GetPreference(96, testDependentRoot)
	require.True(t, ok)
	require.Same(t, msg, stored)
}

func TestProposerPreferencesServiceEmitsEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)
	emitter := beaconevents.NewEventEmitter()
	service.emitters = emitter
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	msg := newTestSignedProposerPreferences(96, 42)
	service.now = func() time.Time { return service.ethClock.GetSlotTime(96).Add(gloasMaximumClockDisparity) }

	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	event := <-events
	require.Equal(t, beaconevents.OpProposerPreferences, event.Event)
	require.Equal(t, &beaconevents.VersionedSignedProposerPreferences{Version: "gloas", Data: msg}, event.Data)
}

func TestProposerPreferencesServiceProgressesWhileEventFeedIsBlocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, epbsPool, _ := setupProposerPreferencesService(t, ctrl)
	emitter := beaconevents.NewEventEmitter()
	service.emitters = emitter
	slow := make(chan *beaconevents.EventStream)
	slowSubscription := emitter.Operation().Subscribe(slow)
	defer slowSubscription.Unsubscribe()
	ready := make(chan *beaconevents.EventStream)
	readySubscription := emitter.Operation().Subscribe(ready)
	defer readySubscription.Unsubscribe()
	blockedSendDone := make(chan struct{})
	go func() {
		emitter.Operation().SendAttestation(&beaconevents.AttestationData{})
		close(blockedSendDone)
	}()
	<-ready

	msg := newTestSignedProposerPreferences(96, 42)
	service.now = func() time.Time { return service.ethClock.GetSlotTime(96).Add(gloasMaximumClockDisparity) }
	processDone := make(chan error, 1)
	ctx := t.Context()
	go func() { processDone <- service.ProcessMessage(ctx, nil, msg) }()
	select {
	case err := <-processDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("proposer preferences processing blocked on the event feed")
	}
	stored, ok := epbsPool.GetPreference(96, testDependentRoot)
	require.True(t, ok)
	require.Same(t, msg, stored)

	slowSubscription.Unsubscribe()
	select {
	case <-blockedSendDone:
	case <-time.After(time.Second):
		t.Fatal("legacy event send remained blocked after unsubscribe")
	}
}

func TestProposerPreferencesServiceLookaheadClockDisparityBoundary(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, epbsPool, _ := setupProposerPreferencesService(t, ctrl)
	msg := newTestSignedProposerPreferences(100, 42)
	lookaheadStart := service.ethClock.GetSlotTime(64)
	now := lookaheadStart.Add(-gloasMaximumClockDisparity - time.Millisecond)
	service.now = func() time.Time { return now }

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "not yet known")
	require.False(t, service.hasSeenPreference(newSeenProposerPreferencesKey(msg.Message)))

	now = lookaheadStart.Add(-gloasMaximumClockDisparity)
	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	stored, ok := epbsPool.GetPreference(100, testDependentRoot)
	require.True(t, ok)
	require.Same(t, msg, stored)
}

func TestProposerPreferencesServiceDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	// First call: epoch OK and slot not passed.
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call: same (validatorIndex, slot, dependentRoot) → IGNORE
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen proposer preferences")
}

func TestProposerPreferencesServiceDuplicateRetainedBeyondFormerCapacity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, ethClock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)
	msg := newTestSignedProposerPreferences(100, 42)
	for i := range uint64(256) {
		root := common.Hash{byte(i), byte(i >> 8)}
		epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: root}, newTestSignedProposerPreferences(100, 42))
	}
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: msg.Message.DependentRoot}, msg)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "already seen proposer preferences")
}

func TestProposerPreferencesServicePrunesSeenMarkerAfterBoundary(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, _, epbsPool, _ := setupProposerPreferencesService(t, ctrl)
	msg := newTestSignedProposerPreferences(100, 42)
	key := pool.ProposerPreferencesKey{Slot: 100, DependentRoot: msg.Message.DependentRoot}
	epbsPool.ProposerPreferences.Add(key, msg)
	preferenceBoundary := time.Unix(100*12, 0).Add(gloasMaximumClockDisparity)
	service.now = func() time.Time { return preferenceBoundary.Add(time.Nanosecond) }

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	_, found := epbsPool.ProposerPreferences.Get(key)
	require.True(t, found)

	bidBoundary := time.Unix(101*12, 0).Add(gloasMaximumClockDisparity)
	service.now = func() time.Time { return bidBoundary }
	err = service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	_, found = epbsPool.ProposerPreferences.Get(key)
	require.True(t, found)

	service.now = func() time.Time { return bidBoundary.Add(time.Nanosecond) }
	err = service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	_, found = epbsPool.ProposerPreferences.Get(key)
	require.False(t, found)
}

func TestProposerPreferencesServiceDependentRootStateMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)
	delete(forkChoiceMock.StateAtBlockRootVal, testDependentRoot)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "state for dependent_root")

	// Should NOT be marked as seen (validation failed)
	seenKey := newSeenProposerPreferencesKey(msg.Message)
	require.False(t, service.hasSeenPreference(seenKey))
}

func TestProposerPreferencesServiceMissingStatePrecedesBoundaryReject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, ethClock, _, fc := setupProposerPreferencesService(t, ctrl)
	msg := newTestSignedProposerPreferences(100, 42)
	fc.Headers[testDependentRoot] = &cltypes.BeaconBlockHeader{Slot: 64}
	delete(fc.StateAtBlockRootVal, testDependentRoot)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "state for dependent_root")
}

func TestProposerPreferencesDependentRootValidity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, _, _, fc := setupProposerPreferencesService(t, ctrl)
	root := testDependentRoot
	boundary := uint64(64)
	fc.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 63}

	fc.HeadVal = root
	require.True(t, service.isValidDependentRoot(root, boundary))
	fc.HeadVal = common.HexToHash("0x99")
	childRoot := common.HexToHash("0x88")
	fc.Headers[childRoot] = &cltypes.BeaconBlockHeader{Slot: boundary, ParentRoot: root}
	require.True(t, service.isValidDependentRoot(root, boundary))
	delete(fc.Headers, childRoot)
	fc.WeightsMock = nil
	require.False(t, service.isValidDependentRoot(root, boundary))
	fc.Headers[root] = &cltypes.BeaconBlockHeader{Slot: boundary}
	require.False(t, service.isValidDependentRoot(root, boundary))
}

func TestProposerPreferencesServiceConcurrentFirstValidCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, ethClock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)
	msg := newTestSignedProposerPreferences(100, 42)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(90)).Times(2)

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { errs <- service.ProcessMessage(context.Background(), nil, msg) })
	}
	wg.Wait()
	close(errs)
	successes := 0
	ignores := 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrIgnore) {
			ignores++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, ignores)
	stored, ok := epbsPool.GetPreference(100, testDependentRoot)
	require.True(t, ok)
	require.Same(t, msg, stored)
}

func TestSeenProposerPreferencesKeyUsesRootAndSlot(t *testing.T) {
	root := common.HexToHash("0x11")
	require.Equal(t,
		seenProposerPreferencesKey{slot: 100, dependentRoot: root},
		newSeenProposerPreferencesKey(&cltypes.ProposerPreferences{ProposalSlot: 100, ValidatorIndex: 42, DependentRoot: root}),
	)
}

func TestProposerPreferencesServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify stored in seen cache
	seenKey := newSeenProposerPreferencesKey(msg.Message)
	require.True(t, service.hasSeenPreference(seenKey))

	// Verify stored in pool
	stored, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg, stored)
}

func TestProposerPreferencesServiceRequestsIndependentDependentRootState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	depState := newProposerPreferencesState(service.beaconCfg, map[uint64]uint64{100: 42})
	forkChoiceMock.StateAtBlockRootVal[testDependentRoot] = depState
	var requestedCopy bool
	var ownedState *state2.CachingBeaconState
	forkChoiceMock.GetStateAtBlockRootFn = func(root common.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
		requestedCopy = alwaysCopy
		var err error
		ownedState, err = forkChoiceMock.StateAtBlockRootVal[root].Copy()
		return ownedState, err
	}

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
	require.True(t, requestedCopy)
	require.NotSame(t, depState, ownedState)
	require.Equal(t, uint64(64), depState.Slot())

	stored, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg, stored)
}

func TestProposerPreferencesValidationStateUsesOwnedStateWithoutSecondCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)
	ownedState := state2.New(service.beaconCfg)
	ownedState.SetVersion(clparams.DenebVersion)
	require.NoError(t, ownedState.AddValidator(solid.NewValidatorFromParameters(common.Bytes48{1}, common.Hash{}, service.beaconCfg.MaxEffectiveBalance, false, 0, 0, service.beaconCfg.FarFutureEpoch, service.beaconCfg.FarFutureEpoch), service.beaconCfg.MaxEffectiveBalance))
	ownedState.SetPreviousEpochParticipationFlags([]cltypes.ParticipationFlags{0})
	ownedState.SetCurrentEpochParticipationFlags([]cltypes.ParticipationFlags{0})
	ownedState.SetInactivityScores([]uint64{0})
	require.NoError(t, ownedState.SetSlot(63))

	validationState, err := service.proposerPreferencesValidationState(ownedState, 3)
	require.NoError(t, err)
	require.Same(t, ownedState, validationState)
	require.Equal(t, uint64(64), ownedState.Slot())
}

func TestProposerPreferencesServiceDifferentValidatorsSameSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	msg1 := newTestSignedProposerPreferences(100, 1)
	msg2 := newTestSignedProposerPreferences(101, 2)
	forkChoiceMock.StateAtBlockRootVal[testDependentRoot] = newProposerPreferencesState(service.beaconCfg, map[uint64]uint64{100: 1, 101: 2})

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90)).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should be seen (different validators and slots)
	require.True(t, service.hasSeenPreference(newSeenProposerPreferencesKey(msg1.Message)))
	require.True(t, service.hasSeenPreference(newSeenProposerPreferencesKey(msg2.Message)))

	stored, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg1, stored)
	stored, ok = epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 101, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg2, stored)
}

func TestProposerPreferencesServiceSameValidatorDifferentSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)

	msg1 := newTestSignedProposerPreferences(96, 42)  // slot 96, epoch 3
	msg2 := newTestSignedProposerPreferences(100, 42) // slot 100, epoch 3

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90)).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should be seen (different slots even though same validator)
	require.True(t, service.hasSeenPreference(newSeenProposerPreferencesKey(msg1.Message)))
	require.True(t, service.hasSeenPreference(newSeenProposerPreferencesKey(msg2.Message)))

	// Both slots should be in pool
	_, ok1 := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 96, DependentRoot: testDependentRoot})
	_, ok2 := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok1)
	require.True(t, ok2)
}

func TestProposerPreferencesServiceDecodeGossipMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)

	original := newTestSignedProposerPreferences(100, 42)
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded, err := service.DecodeGossipMessage("peer123", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, original.Message.ProposalSlot, decoded.Message.ProposalSlot)
	require.Equal(t, original.Message.ValidatorIndex, decoded.Message.ValidatorIndex)
	require.Equal(t, original.Message.FeeRecipient, decoded.Message.FeeRecipient)
	require.Equal(t, original.Message.TargetGasLimit, decoded.Message.TargetGasLimit)
	require.Equal(t, original.Message.DependentRoot, decoded.Message.DependentRoot)
}

func TestProposerPreferencesServiceDecodeGossipMessageInvalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupProposerPreferencesService(t, ctrl)

	_, err := service.DecodeGossipMessage("peer123", []byte{0x00, 0x01, 0x02}, clparams.GloasVersion)
	require.Error(t, err)
}

func TestProposerPreferencesServiceFailedValidationNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	forkChoiceMock.StateAtBlockRootVal[testDependentRoot] = newProposerPreferencesState(service.beaconCfg, map[uint64]uint64{100: 7})

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)

	// Should NOT be in seen cache
	seenKey := newSeenProposerPreferencesKey(msg.Message)
	require.False(t, service.hasSeenPreference(seenKey))

	// Should NOT be in pool
	_, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.False(t, ok)
}
