package services

import (
	"context"
	"log"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	mockSync "github.com/ledgerwatch/erigon/cl/beacon/synced_data/mock_services"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	mockCommittee "github.com/ledgerwatch/erigon/cl/validator/committee_subscription/mock_services"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

var (
	mockSlot          = uint64(321)
	mockEpoch         = uint64(10)
	mockSlotsPerEpoch = uint64(32)
	attData           = solid.NewAttestionDataFromParameters(mockSlot, 2, [32]byte{0, 4, 2, 6},
		solid.NewCheckpointFromParameters([32]byte{1, 0}, mockEpoch),
		solid.NewCheckpointFromParameters([32]byte{1, 0}, mockEpoch))

	att = solid.NewAttestionFromParameters(
		[]byte{0b00000001},
		attData,
		[96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
	)
)

type attestationTestSuite struct {
	suite.Suite
	gomockCtrl        *gomock.Controller
	mockForkChoice    *forkchoice.ForkChoiceStorageMock
	syncedData        *mockSync.MockSyncedData
	committeeSubscibe *mockCommittee.MockCommitteeSubscribe
	ethClock          *eth_clock.MockEthereumClock
	attService        AttestationService
}

func (t *attestationTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockForkChoice = &forkchoice.ForkChoiceStorageMock{}
	t.syncedData = mockSync.NewMockSyncedData(t.gomockCtrl)
	t.committeeSubscibe = mockCommittee.NewMockCommitteeSubscribe(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	beaconConfig := &clparams.BeaconChainConfig{SlotsPerEpoch: mockSlotsPerEpoch}
	netConfig := &clparams.NetworkConfig{}
	t.attService = NewAttestationService(t.mockForkChoice, t.committeeSubscibe, t.ethClock, t.syncedData, beaconConfig, netConfig)
}

func (t *attestationTestSuite) TearDownTest() {
	//t.gomockCtrl.Finish()
}

func (t *attestationTestSuite) TestAttestationProcessMessage() {
	type args struct {
		ctx    context.Context
		subnet *uint64
		msg    *solid.Attestation
	}
	tests := []struct {
		name    string
		mock    func()
		args    args
		wantErr bool
	}{
		{
			name: "Test attestation with committee index out of range",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 1
				}
			},
			args: args{
				ctx:    context.Background(),
				subnet: nil,
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "Test attestation with wrong subnet",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 2
				}
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "Test attestation with wrong slot (current_slot < slot)",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(uint64(1)).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "Attestation is aggregated",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: solid.NewAttestionFromParameters(
					[]byte{0b00111111, 0b00000011, 0, 0},
					attData,
					[96]byte{0, 1, 2, 3, 4, 5},
				),
			},
			wantErr: true,
		},
		{
			name: "Attestation is empty",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: solid.NewAttestionFromParameters(
					[]byte{0, 0, 0, 0},
					attData,
					[96]byte{0, 1, 2, 3, 4, 5},
				),
			},
			wantErr: true,
		},
		{
			name: "aggregation bits count mismatch",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 100000
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "block header not found",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "invalid target block",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				t.mockForkChoice = &forkchoice.ForkChoiceStorageMock{
					Headers: map[common.Hash]*cltypes.BeaconBlockHeader{
						att.AttestantionData().BeaconBlockRoot(): {},
					},
				}
				t.attService = NewAttestationService(
					t.mockForkChoice,
					t.committeeSubscibe,
					t.ethClock, t.syncedData,
					&clparams.BeaconChainConfig{SlotsPerEpoch: mockSlotsPerEpoch},
					&clparams.NetworkConfig{})
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "invalid finality checkpoint",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				t.mockForkChoice = &forkchoice.ForkChoiceStorageMock{
					Headers: map[common.Hash]*cltypes.BeaconBlockHeader{
						att.AttestantionData().BeaconBlockRoot(): {},
					},
					Ancestors: map[uint64]common.Hash{
						10 * mockSlotsPerEpoch: att.AttestantionData().Target().BlockRoot(),
						0:                      {},
					},
					FinalizedCheckpointVal: solid.NewCheckpointFromParameters([32]byte{1, 0}, 1),
				}
				t.attService = NewAttestationService(
					t.mockForkChoice,
					t.committeeSubscibe,
					t.ethClock, t.syncedData,
					&clparams.BeaconChainConfig{SlotsPerEpoch: mockSlotsPerEpoch},
					&clparams.NetworkConfig{})
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
			wantErr: true,
		},
		{
			name: "success",
			mock: func() {
				t.syncedData.EXPECT().HeadState().Return(&state.CachingBeaconState{}).Times(1)
				computeCommitteeCountPerSlot = func(_ *state.CachingBeaconState, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				t.mockForkChoice = &forkchoice.ForkChoiceStorageMock{
					Headers: map[common.Hash]*cltypes.BeaconBlockHeader{
						att.AttestantionData().BeaconBlockRoot(): {},
					},
					Ancestors: map[uint64]common.Hash{
						10 * mockSlotsPerEpoch: att.AttestantionData().Target().BlockRoot(),
						1 * mockSlotsPerEpoch:  {1, 0},
					},
					FinalizedCheckpointVal: solid.NewCheckpointFromParameters([32]byte{1, 0}, 1),
				}
				t.attService = NewAttestationService(
					t.mockForkChoice,
					t.committeeSubscibe,
					t.ethClock, t.syncedData,
					&clparams.BeaconChainConfig{SlotsPerEpoch: mockSlotsPerEpoch},
					&clparams.NetworkConfig{})
				t.committeeSubscibe.EXPECT().CheckAggregateAttestation(att).Return(nil).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg:    att,
			},
		},
	}

	for _, tt := range tests {
		log.Printf("test case: %s", tt.name)
		t.SetupTest()
		tt.mock()
		err := t.attService.ProcessMessage(tt.args.ctx, tt.args.subnet, tt.args.msg)
		if tt.wantErr {
			log.Printf("%v", err)
			t.Require().Error(err)
		} else {
			t.Require().NoError(err)
		}
		t.True(t.gomockCtrl.Satisfied())
	}
}

func TestAttestation(t *testing.T) {
	suite.Run(t, &attestationTestSuite{})
}

func uint64Ptr(i uint64) *uint64 {
	return &i
}
