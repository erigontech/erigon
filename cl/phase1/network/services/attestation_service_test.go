// Copyright 2024 The Erigon Authors
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

package services

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	mockCommittee "github.com/erigontech/erigon/cl/validator/committee_subscription/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

var (
	mockSlot          = uint64(321)
	mockEpoch         = uint64(10)
	mockSlotsPerEpoch = uint64(32)
	attData           = &solid.AttestationData{
		Slot:            mockSlot,
		CommitteeIndex:  2,
		BeaconBlockRoot: [32]byte{0, 4, 2, 6},
		Source:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
		Target:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
	}

	att = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000001, 1}, 2048),
		Data:            attData,
		Signature:       [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
	}
)

type attestationTestSuite struct {
	suite.Suite
	gomockCtrl        *gomock.Controller
	mockForkChoice    *mock_services.ForkChoiceStorageMock
	syncedData        synced_data.SyncedData
	committeeSubscibe *mockCommittee.MockCommitteeSubscribe
	ethClock          *eth_clock.MockEthereumClock
	attService        AttestationService
	beaconConfig      *clparams.BeaconChainConfig
}

func (t *attestationTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockForkChoice = &mock_services.ForkChoiceStorageMock{}
	_, st, _ := tests.GetBellatrixRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.syncedData.OnHeadState(st)
	t.committeeSubscibe = mockCommittee.NewMockCommitteeSubscribe(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:    mockSlotsPerEpoch,
		ElectraForkEpoch: 100000,
	}
	netConfig := &clparams.NetworkConfig{}
	emitters := beaconevents.NewEventEmitter()
	computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) { return [32]byte{}, nil }
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	go batchSignatureVerifier.Start()
	ctx, cn := context.WithCancel(context.Background())
	cn()
	t.attService = NewAttestationService(ctx, t.mockForkChoice, t.committeeSubscibe, t.ethClock, t.syncedData, t.beaconConfig, netConfig, emitters, batchSignatureVerifier)
}

func (t *attestationTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func (t *attestationTestSuite) TestAttestationProcessMessage() {
	type args struct {
		ctx    context.Context
		subnet *uint64
		msg    *solid.Attestation
	}
	tests := []struct {
		name    string
		wantErr bool
		mock    func()
		args    args
	}{
		{
			name: "Test attestation with committee index out of range",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
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
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 2
				}
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
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
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
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
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{0b10000001, 1}, 2048),
					Data:            attData,
					Signature:       [96]byte{0, 1, 2, 3, 4, 5},
				},
			},
			wantErr: true,
		},
		{
			name: "Attestation is empty",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{0b0, 1}, 2048),
					Data:            attData,
					Signature:       [96]byte{0, 1, 2, 3, 4, 5},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid signature",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 5
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
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
			name: "block header not found",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
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
			name: "invalid target block",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}
				t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
					att.Data.BeaconBlockRoot: {}, // wrong block root
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
			name: "invalid finality checkpoint",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}
				t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
					att.Data.BeaconBlockRoot: {},
				}
				mockFinalizedCheckPoint := &solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
				t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
					mockEpoch * mockSlotsPerEpoch:                     {Root: att.Data.Target.Root},
					mockFinalizedCheckPoint.Epoch * mockSlotsPerEpoch: {}, // wrong block root
				}
				t.mockForkChoice.FinalizedCheckpointVal = *mockFinalizedCheckPoint
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
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
				computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}
				blsVerifyMultipleSignatures = func(signatures [][]byte, signRoots [][]byte, pks [][]byte) (bool, error) {
					return true, nil
				}
				t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
					att.Data.BeaconBlockRoot: {},
				}

				mockFinalizedCheckPoint := &solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
				t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
					mockEpoch * mockSlotsPerEpoch:                     {Root: att.Data.Target.Root},
					mockFinalizedCheckPoint.Epoch * mockSlotsPerEpoch: {Root: mockFinalizedCheckPoint.Root},
				}
				t.mockForkChoice.FinalizedCheckpointVal = *mockFinalizedCheckPoint
				//t.committeeSubscibe.EXPECT().NeedToAggregate(att).Return(true).Times(1)
				t.committeeSubscibe.EXPECT().AggregateAttestation(att).Return(nil).Times(1)
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
		err := t.attService.ProcessMessage(tt.args.ctx, tt.args.subnet, &AttestationForGossip{
			Attestation:      tt.args.msg,
			ImmediateProcess: true,
		})
		time.Sleep(time.Millisecond * 60)
		if tt.wantErr {
			t.Require().Error(err)
		} else {
			t.Require().NoError(err)
		}

		t.True(t.gomockCtrl.Satisfied())
	}
}

func (t *attestationTestSuite) TestGloasAttestationIndexValidation() {
	// Override beacon config to enable Gloas version
	gloasConfig := &clparams.BeaconChainConfig{
		SlotsPerEpoch:      mockSlotsPerEpoch,
		AltairForkEpoch:    0,
		BellatrixForkEpoch: 0,
		CapellaForkEpoch:   0,
		DenebForkEpoch:     0,
		ElectraForkEpoch:   0,
		FuluForkEpoch:      0,
		GloasForkEpoch:     0,
	}

	type args struct {
		ctx    context.Context
		subnet *uint64
		msg    *solid.SingleAttestation
	}
	tests := []struct {
		name    string
		wantErr bool
		errMsg  string
		mock    func()
		args    args
	}{
		{
			name: "Gloas: reject attestation data index >= 2",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: &solid.SingleAttestation{
					CommitteeIndex: 0,
					AttesterIndex:  0,
					Data: &solid.AttestationData{
						Slot:            mockSlot,
						CommitteeIndex:  2, // index >= 2 should be rejected
						BeaconBlockRoot: [32]byte{0, 4, 2, 6},
						Source:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
						Target:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
					},
					Signature: [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
				},
			},
			wantErr: true,
			errMsg:  "attestation data index must be less than 2",
		},
		{
			name: "Gloas: accept attestation data index 0",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: &solid.SingleAttestation{
					CommitteeIndex: 0,
					AttesterIndex:  0,
					Data: &solid.AttestationData{
						Slot:            mockSlot,
						CommitteeIndex:  0, // index 0 should pass this check
						BeaconBlockRoot: [32]byte{0, 4, 2, 6},
						Source:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
						Target:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
					},
					Signature: [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
				},
			},
			// Will fail later (attester not in committee or block not found), but NOT due to index check
			wantErr: true,
			errMsg:  "", // any error other than "attestation data index must be less than 2"
		},
		{
			name: "Gloas: accept attestation data index 1",
			mock: func() {
				computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
					return 8
				}
				computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
					return 1
				}
				t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
				t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
			},
			args: args{
				ctx:    context.Background(),
				subnet: uint64Ptr(1),
				msg: &solid.SingleAttestation{
					CommitteeIndex: 0,
					AttesterIndex:  0,
					Data: &solid.AttestationData{
						Slot:            mockSlot,
						CommitteeIndex:  1, // index 1 should pass this check
						BeaconBlockRoot: [32]byte{0, 4, 2, 6},
						Source:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
						Target:          solid.Checkpoint{Epoch: mockEpoch, Root: [32]byte{1, 0}},
					},
					Signature: [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
				},
			},
			// Will fail later (attester not in committee or block not found), but NOT due to index check
			wantErr: true,
			errMsg:  "", // any error other than "attestation data index must be less than 2"
		},
	}

	for _, tt := range tests {
		log.Printf("test case: %s", tt.name)
		t.SetupTest()
		t.beaconConfig = gloasConfig
		netConfig := &clparams.NetworkConfig{}
		emitters := beaconevents.NewEventEmitter()
		computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) { return [32]byte{}, nil }
		batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
		go batchSignatureVerifier.Start()
		ctx, cn := context.WithCancel(context.Background())
		cn()
		t.attService = NewAttestationService(ctx, t.mockForkChoice, t.committeeSubscibe, t.ethClock, t.syncedData, gloasConfig, netConfig, emitters, batchSignatureVerifier)

		tt.mock()
		err := t.attService.ProcessMessage(tt.args.ctx, tt.args.subnet, &AttestationForGossip{
			SingleAttestation: tt.args.msg,
			ImmediateProcess:  true,
		})
		if tt.wantErr {
			t.Require().Error(err, "test case: %s", tt.name)
			if tt.errMsg != "" {
				t.Require().Contains(err.Error(), tt.errMsg, "test case: %s", tt.name)
			} else {
				// Should NOT be the index check error
				t.Require().NotContains(err.Error(), "attestation data index must be less than 2", "test case: %s", tt.name)
			}
		} else {
			t.Require().NoError(err, "test case: %s", tt.name)
		}
		t.True(t.gomockCtrl.Satisfied())
	}
}

func TestAttestation(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	suite.Run(t, &attestationTestSuite{})
}

func uint64Ptr(i uint64) *uint64 {
	return &i
}
