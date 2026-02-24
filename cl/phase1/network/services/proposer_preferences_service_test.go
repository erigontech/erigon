package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	synced_data_mock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func setupProposerPreferencesService(t *testing.T, ctrl *gomock.Controller) (*proposerPreferencesService, *synced_data_mock.MockSyncedData, *eth_clock.MockEthereumClock, *pool.EpbsPool) {
	mockSyncedData := synced_data_mock.NewMockSyncedData(ctrl)
	ethClockMock := eth_clock.NewMockEthereumClock(ctrl)
	epbsPool := pool.NewEpbsPool()
	beaconCfg := &clparams.BeaconChainConfig{
		SlotsPerEpoch: 32,
	}

	seenCache, err := lru.New[seenProposerPreferencesKey, struct{}]("seen_proposer_preferences_test", seenProposerPreferencesCacheSize)
	require.NoError(t, err)

	service := &proposerPreferencesService{
		syncedDataManager: mockSyncedData,
		ethClock:          ethClockMock,
		beaconCfg:         beaconCfg,
		epbsPool:          epbsPool,
		seenCache:         seenCache,
	}

	return service, mockSyncedData, ethClockMock, epbsPool
}

func newTestSignedProposerPreferences(proposalSlot, validatorIndex uint64) *cltypes.SignedProposerPreferences {
	return &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   proposalSlot,
			ValidatorIndex: validatorIndex,
			FeeRecipient:   common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
			GasLimit:       30_000_000,
		},
		Signature: common.Bytes96{},
	}
}

func TestProposerPreferencesServiceNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _ := setupProposerPreferencesService(t, ctrl)

	names := service.Names()
	require.Len(t, names, 1)
	require.Equal(t, "proposer_preferences", names[0])
}

func TestProposerPreferencesServiceNilMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _ := setupProposerPreferencesService(t, ctrl)

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

	service, _, ethClockMock, _ := setupProposerPreferencesService(t, ctrl)

	// proposal_slot=100, SlotsPerEpoch=32 → proposalEpoch = 100/32 = 3
	// We need proposalEpoch == currentEpoch+1, so set currentEpoch = 5 (mismatch: 3 != 6)
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(5))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "expected next epoch")
}

func TestProposerPreferencesServiceCurrentEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _ := setupProposerPreferencesService(t, ctrl)

	// proposalEpoch == currentEpoch (same epoch, not next) → IGNORE
	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(3))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
}

func TestProposerPreferencesServiceDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	// First call: epoch OK, ViewHeadState succeeds
	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call: same (validatorIndex, slot) → IGNORE
	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))

	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen proposer preferences")
}

func TestProposerPreferencesServiceViewHeadStateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, _ := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))

	// ViewHeadState returns error (e.g. state not synced, or inner validation failed)
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return fmt.Errorf("not synced")
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "proposer preferences validation failed")
	require.Contains(t, err.Error(), "not synced")

	// Should NOT be marked as seen (validation failed)
	seenKey := seenProposerPreferencesKey{validatorIndex: 42, slot: 100}
	require.False(t, service.seenCache.Contains(seenKey))
}

func TestProposerPreferencesServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, epbsPool := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))

	// ViewHeadState succeeds (proposer lookahead + BLS all pass)
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify stored in seen cache
	seenKey := seenProposerPreferencesKey{validatorIndex: 42, slot: 100}
	require.True(t, service.seenCache.Contains(seenKey))

	// Verify stored in pool
	stored, ok := epbsPool.ProposerPreferences.Get(uint64(100))
	require.True(t, ok)
	require.Equal(t, msg, stored)
}

func TestProposerPreferencesServiceDifferentValidatorsSameSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, epbsPool := setupProposerPreferencesService(t, ctrl)

	msg1 := newTestSignedProposerPreferences(100, 1)
	msg2 := newTestSignedProposerPreferences(100, 2)

	// Both calls: epoch OK, ViewHeadState succeeds
	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2)).Times(2)
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3)).Times(2)
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should be seen (different validators)
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 1, slot: 100}))
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 2, slot: 100}))

	// Pool is keyed by slot, so the second one overwrites the first
	stored, ok := epbsPool.ProposerPreferences.Get(uint64(100))
	require.True(t, ok)
	require.Equal(t, msg2, stored)
}

func TestProposerPreferencesServiceSameValidatorDifferentSlots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, epbsPool := setupProposerPreferencesService(t, ctrl)

	msg1 := newTestSignedProposerPreferences(96, 42)  // slot 96, epoch 3
	msg2 := newTestSignedProposerPreferences(100, 42) // slot 100, epoch 3

	// Both calls: epoch OK, ViewHeadState succeeds
	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2)).Times(2)
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(96)).Return(uint64(3))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should be seen (different slots even though same validator)
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 42, slot: 96}))
	require.True(t, service.seenCache.Contains(seenProposerPreferencesKey{validatorIndex: 42, slot: 100}))

	// Both slots should be in pool
	_, ok1 := epbsPool.ProposerPreferences.Get(uint64(96))
	_, ok2 := epbsPool.ProposerPreferences.Get(uint64(100))
	require.True(t, ok1)
	require.True(t, ok2)
}

func TestProposerPreferencesServiceDecodeGossipMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _ := setupProposerPreferencesService(t, ctrl)

	original := newTestSignedProposerPreferences(100, 42)
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded, err := service.DecodeGossipMessage("peer123", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, original.Message.ProposalSlot, decoded.Message.ProposalSlot)
	require.Equal(t, original.Message.ValidatorIndex, decoded.Message.ValidatorIndex)
	require.Equal(t, original.Message.FeeRecipient, decoded.Message.FeeRecipient)
	require.Equal(t, original.Message.GasLimit, decoded.Message.GasLimit)
}

func TestProposerPreferencesServiceDecodeGossipMessageInvalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _ := setupProposerPreferencesService(t, ctrl)

	_, err := service.DecodeGossipMessage("peer123", []byte{0x00, 0x01, 0x02}, clparams.GloasVersion)
	require.Error(t, err)
}

func TestProposerPreferencesServiceFailedValidationNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, epbsPool := setupProposerPreferencesService(t, ctrl)

	msg := newTestSignedProposerPreferences(100, 42)

	ethClockMock.EXPECT().GetCurrentEpoch().Return(uint64(2))
	ethClockMock.EXPECT().GetEpochAtSlot(uint64(100)).Return(uint64(3))

	// ViewHeadState returns error (e.g. wrong proposer or invalid BLS)
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return fmt.Errorf("validator 42 is not the proposer for slot 100")
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)

	// Should NOT be in seen cache
	seenKey := seenProposerPreferencesKey{validatorIndex: 42, slot: 100}
	require.False(t, service.seenCache.Contains(seenKey))

	// Should NOT be in pool
	_, ok := epbsPool.ProposerPreferences.Get(uint64(100))
	require.False(t, ok)
}
