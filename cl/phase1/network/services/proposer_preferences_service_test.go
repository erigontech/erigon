package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	synced_data_mock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func setupProposerPreferencesService(t *testing.T, ctrl *gomock.Controller) (*proposerPreferencesService, *synced_data_mock.MockSyncedData, *eth_clock.MockEthereumClock, *pool.EpbsPool, *forkchoice_mock.ForkChoiceStorageMock) {
	mockSyncedData := synced_data_mock.NewMockSyncedData(ctrl)
	ethClockMock := eth_clock.NewMockEthereumClock(ctrl)
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
	prevBlsVerify := blsVerify
	blsVerify = func(_ []byte, _ []byte, _ []byte) (bool, error) { return true, nil }
	t.Cleanup(func() { blsVerify = prevBlsVerify })

	seenCache, err := lru.New[seenProposerPreferencesKey, struct{}]("seen_proposer_preferences_test", seenProposerPreferencesCacheSize)
	require.NoError(t, err)

	service := &proposerPreferencesService{
		syncedDataManager: mockSyncedData,
		forkchoiceStore:   forkChoiceMock,
		ethClock:          ethClockMock,
		beaconCfg:         &beaconCfg,
		epbsPool:          epbsPool,
		seenCache:         seenCache,
	}

	return service, mockSyncedData, ethClockMock, epbsPool, forkChoiceMock
}

// testDependentRoot is a fixed dependent root used across tests.
var testDependentRoot = common.HexToHash("0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

func newProposerPreferencesState(cfg *clparams.BeaconChainConfig, proposers map[uint64]uint64) *state2.CachingBeaconState {
	s := state2.New(cfg)
	s.SetSlot(64)
	for i := 0; i < 100; i++ {
		var pk common.Bytes48
		pk[0] = byte(i)
		s.AddValidator(solid.NewValidatorFromParameters(pk, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), 0)
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

func TestProposerPreferencesServiceWrongEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	// proposal_slot=100, SlotsPerEpoch=32 → proposalEpoch = 100/32 = 3
	// currentEpoch = 5 → neither current (5) nor next (6) match proposalEpoch (3)
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(5))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "expected epoch in")
}

func TestProposerPreferencesServiceCurrentEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)

	// proposalEpoch == currentEpoch (same epoch) → should be accepted now
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(3))
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

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(105))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already passed")
}

func TestProposerPreferencesServiceCurrentSlotIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	// proposalSlot == currentSlot → spec says proposal_slot > current_slot, so this should be IGNORED
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already passed")
}

func TestProposerPreferencesServiceDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	// First call: epoch OK and slot not passed.
	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call: same (validatorIndex, slot, dependentRoot) → IGNORE
	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen proposer preferences")
}

func TestProposerPreferencesServiceDependentRootStateMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)
	delete(forkChoiceMock.StateAtBlockRootVal, testDependentRoot)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "state for dependent_root")

	// Should NOT be marked as seen (validation failed)
	seenKey := seenProposerPreferencesKey{validatorIndex: 42, slot: 100, dependentRoot: testDependentRoot}
	require.False(t, service.seenCache.Contains(seenKey))
}

func TestProposerPreferencesServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify stored in seen cache
	seenKey := seenProposerPreferencesKey{validatorIndex: 42, slot: 100, dependentRoot: testDependentRoot}
	require.True(t, service.seenCache.Contains(seenKey))

	// Verify stored in pool
	stored, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg, stored)
}

func TestProposerPreferencesServiceAdvancesDependentRootStateToBoundary(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	depState := newProposerPreferencesState(service.beaconCfg, map[uint64]uint64{100: 42})
	depState.SetSlot(63)
	forkChoiceMock.StateAtBlockRootVal[testDependentRoot] = depState

	msg := newTestSignedProposerPreferences(100, 0)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
	require.Equal(t, uint64(63), depState.Slot())

	stored, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.True(t, ok)
	require.Equal(t, msg, stored)
}

func TestProposerPreferencesServiceDifferentValidatorsSameSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, epbsPool, forkChoiceMock := setupProposerPreferencesService(t, ctrl)

	msg1 := newTestSignedProposerPreferences(100, 1)
	msg2 := newTestSignedProposerPreferences(101, 2)
	forkChoiceMock.StateAtBlockRootVal[testDependentRoot] = newProposerPreferencesState(service.beaconCfg, map[uint64]uint64{100: 1, 101: 2})

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2)).Times(2)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90)).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should be seen (different validators and slots)
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 1, slot: 100, dependentRoot: testDependentRoot}))
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 2, slot: 101, dependentRoot: testDependentRoot}))

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

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2)).Times(2)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90)).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should be seen (different slots even though same validator)
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 42, slot: 96, dependentRoot: testDependentRoot}))
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 42, slot: 100, dependentRoot: testDependentRoot}))

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

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(90))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)

	// Should NOT be in seen cache
	seenKey := seenProposerPreferencesKey{validatorIndex: 42, slot: 100, dependentRoot: testDependentRoot}
	require.False(t, service.seenCache.Contains(seenKey))

	// Should NOT be in pool
	_, ok := epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: testDependentRoot})
	require.False(t, ok)
}
