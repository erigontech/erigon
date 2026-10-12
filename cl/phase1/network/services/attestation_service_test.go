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
	"errors"
	"fmt"
	"log"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	serviceintf "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	mockCommittee "github.com/erigontech/erigon/cl/validator/committee_subscription/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

var (
	mockSlot          = uint64(64)
	mockEpoch         = uint64(2)
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
	saveSignatureGlobals(t.T())
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockForkChoice = &mock_services.ForkChoiceStorageMock{}
	_, st, _ := tests.GetBellatrixRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.Require().NoError(t.syncedData.OnHeadState(st))
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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

func (t *attestationTestSuite) TestAttestationProcessMessageAllowsNextEpochWhenClockHasReachedIt() {
	nextEpochSlot := mockSlot + mockSlotsPerEpoch
	nextEpoch := mockEpoch + 1
	nextEpochAttData := *attData
	nextEpochAttData.Slot = nextEpochSlot
	nextEpochAttData.Source.Epoch = mockEpoch
	nextEpochAttData.Target.Epoch = nextEpoch
	nextEpochAtt := *att
	nextEpochAtt.Data = &nextEpochAttData

	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	t.ethClock.EXPECT().GetEpochAtSlot(nextEpochSlot).Return(nextEpoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(nextEpochSlot).Times(1)
	t.mockForkChoice.HighestSeenVal = mockSlot
	computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		return [32]byte{}, nil
	}
	blsVerifyMultipleSignatures = func(signatures [][]byte, signRoots [][]byte, pks [][]byte) (bool, error) {
		return true, nil
	}
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		nextEpochAttData.BeaconBlockRoot: {},
	}
	finalizedCheckpoint := solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		nextEpoch * mockSlotsPerEpoch:                 {Root: nextEpochAttData.Target.Root},
		finalizedCheckpoint.Epoch * mockSlotsPerEpoch: {Root: finalizedCheckpoint.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalizedCheckpoint
	t.committeeSubscibe.EXPECT().AggregateAttestation(&nextEpochAtt).Return(nil).Times(1)

	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      &nextEpochAtt,
		ImmediateProcess: true,
	})
	time.Sleep(time.Millisecond * 60)

	t.Require().NoError(err)
}

func (t *attestationTestSuite) TestAttestationProcessMessageAllowsSecondEpochWhenNoLaterBlockSeen() {
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	blsVerifyMultipleSignatures = func(signatures [][]byte, signRoots [][]byte, pks [][]byte) (bool, error) {
		return true, nil
	}

	secondEpoch := mockEpoch + 2
	secondEpochSlot := secondEpoch * mockSlotsPerEpoch
	secondEpochAttData := *attData
	secondEpochAttData.Slot = secondEpochSlot
	secondEpochAttData.Target.Epoch = secondEpoch
	secondEpochAtt := *att
	secondEpochAtt.Data = &secondEpochAttData

	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		secondEpochAttData.BeaconBlockRoot: {},
	}
	finalizedCheckpoint := solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		secondEpochSlot: {Root: secondEpochAttData.Target.Root},
		finalizedCheckpoint.Epoch * mockSlotsPerEpoch: {Root: finalizedCheckpoint.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalizedCheckpoint
	t.committeeSubscibe.EXPECT().AggregateAttestation(&secondEpochAtt).Return(nil).Times(1)

	t.ethClock.EXPECT().GetEpochAtSlot(secondEpochSlot).Return(secondEpoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(secondEpochSlot).Times(1)
	t.mockForkChoice.HighestSeenVal = mockSlot
	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      &secondEpochAtt,
		ImmediateProcess: true,
	})
	t.Require().NoError(err)
	t.Eventually(t.gomockCtrl.Satisfied, time.Second, time.Millisecond)

	thirdEpoch := mockEpoch + 3
	thirdEpochSlot := thirdEpoch * mockSlotsPerEpoch
	thirdEpochAttData := secondEpochAttData
	thirdEpochAttData.Slot = thirdEpochSlot
	thirdEpochAttData.Target.Epoch = thirdEpoch
	thirdEpochAtt := *att
	thirdEpochAtt.Data = &thirdEpochAttData

	t.ethClock.EXPECT().GetEpochAtSlot(thirdEpochSlot).Return(thirdEpoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(thirdEpochSlot).Times(1)
	t.mockForkChoice.HighestSeenVal = mockSlot
	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      &thirdEpochAtt,
		ImmediateProcess: true,
	})
	t.Require().Error(err)
	t.Require().Contains(err.Error(), "too far from attestation epoch")

	t.ethClock.EXPECT().GetEpochAtSlot(secondEpochSlot).Return(secondEpoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(secondEpochSlot).Times(1)
	t.mockForkChoice.HighestSeenVal = mockSlot + 1
	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      &secondEpochAtt,
		ImmediateProcess: true,
	})
	t.Require().Error(err)
	t.Require().Contains(err.Error(), "too far from attestation epoch")
}

// The head state can still carry the previous fork when the attestation targets the first epoch of a new one.
func (t *attestationTestSuite) TestAttestationSignatureDomainUsesTargetEpochForkVersion() {
	t.beaconConfig.DenebForkVersion = 0x04000099 // the fork scheduled for mockEpoch under this config
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).Times(1)
	var gotDomain []byte
	computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		gotDomain = domain
		return [32]byte{}, nil
	}
	blsVerifyMultipleSignatures = func(signatures [][]byte, signRoots [][]byte, pks [][]byte) (bool, error) {
		return true, nil
	}
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		att.Data.BeaconBlockRoot: {},
	}
	finalizedCheckpoint := solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		mockEpoch * mockSlotsPerEpoch:                 {Root: att.Data.Target.Root},
		finalizedCheckpoint.Epoch * mockSlotsPerEpoch: {Root: finalizedCheckpoint.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalizedCheckpoint
	t.committeeSubscibe.EXPECT().AggregateAttestation(att).Return(nil).Times(1)

	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      att,
		ImmediateProcess: true,
	})
	time.Sleep(time.Millisecond * 60)
	t.Require().NoError(err)

	var genesisValidatorsRoot common.Hash
	t.Require().NoError(t.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
		genesisValidatorsRoot = headState.GenesisValidatorsRoot()
		return nil
	}))
	want, err := fork.ComputeDomain(t.beaconConfig.DomainBeaconAttester[:], utils.Uint32ToBytes4(0x04000099), genesisValidatorsRoot)
	t.Require().NoError(err)
	t.Require().Equal(want, gotDomain)
}

// The per-validator seen slot must be claimed only once the signature has been
// verified, otherwise anyone can name a real committee member and censor that
// validator's genuine attestation for the rest of the epoch at no cost.
func (t *attestationTestSuite) TestAttestationSeenOnlyAfterSignatureVerification() {
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		return [32]byte{}, nil
	}
	signatureValid := false
	blsVerifyMultipleSignatures = func(signatures [][]byte, signRoots [][]byte, pks [][]byte) (bool, error) {
		return signatureValid, nil
	}
	t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).AnyTimes()
	t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).AnyTimes()
	t.mockForkChoice.HighestSeenVal = mockSlot

	// The block is known, so validation reaches signature verification.
	finalizedCheckpoint := solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		attData.BeaconBlockRoot: {},
	}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		mockEpoch * mockSlotsPerEpoch:                 {Root: attData.Target.Root},
		finalizedCheckpoint.Epoch * mockSlotsPerEpoch: {Root: finalizedCheckpoint.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalizedCheckpoint
	t.committeeSubscibe.EXPECT().AggregateAttestation(att).Return(nil).Times(1)

	// An invalid signature must not consume the validator's slot.
	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      att,
		ImmediateProcess: true,
	})
	t.Require().Error(err)

	// The validator's genuine attestation must still be accepted.
	signatureValid = true
	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      att,
		ImmediateProcess: true,
	})
	t.Require().NoError(err)

	// ...and having been verified, it now holds the slot against a duplicate.
	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      att,
		ImmediateProcess: true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().ErrorIs(err, ErrAttestationAlreadySeen)
}

func (t *attestationTestSuite) TestAttestationGossipNotAcceptedBeforeSignatureVerification() {
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		return [32]byte{}, nil
	}
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return false, nil
	}
	t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).AnyTimes()
	t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).AnyTimes()
	t.mockForkChoice.HighestSeenVal = mockSlot

	finalizedCheckpoint := solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		attData.BeaconBlockRoot: {},
	}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		mockEpoch * mockSlotsPerEpoch:                 {Root: attData.Target.Root},
		finalizedCheckpoint.Epoch * mockSlotsPerEpoch: {Root: finalizedCheckpoint.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalizedCheckpoint
	t.committeeSubscibe.EXPECT().AggregateAttestation(gomock.Any()).Times(0)

	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      att,
		ImmediateProcess: false,
	})
	t.Require().ErrorIs(err, ErrInvalidBlsSignature)
}

func (t *attestationTestSuite) TestAttestationGossipPreForkAttestationOnPostForkTopicNotAccepted() {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0

	_, st, _ := tests.GetBellatrixRandom()
	slot := st.Slot()
	epoch := slot / cfg.SlotsPerEpoch
	cfg.GloasForkEpoch = epoch + 1
	committee, err := st.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	t.Require().NotEmpty(committee)

	blockRoot, err := st.BlockRoot()
	t.Require().NoError(err)
	targetRoot := common.Hash{1, 2, 3}
	finalizedCheckpoint := solid.Checkpoint{Epoch: 1, Root: common.Hash{4, 5, 6}}
	singleAttestation := &solid.SingleAttestation{
		CommitteeIndex: 0,
		AttesterIndex:  committee[0],
		Data: &solid.AttestationData{
			Slot:            slot,
			BeaconBlockRoot: blockRoot,
			Source:          st.CurrentJustifiedCheckpoint(),
			Target:          solid.Checkpoint{Epoch: epoch, Root: targetRoot},
		},
		Signature: common.Bytes96{1},
	}
	encoded, err := singleAttestation.EncodeSSZ(nil)
	t.Require().NoError(err)

	t.syncedData = synced_data.NewSyncedDataManager(&cfg, true)
	t.Require().NoError(t.syncedData.OnHeadState(st))
	t.beaconConfig = &cfg
	batchSignatureVerifier := NewBatchSignatureVerifier(t.T().Context(), nil)
	batchSignatureVerifier.Start()
	t.attService = NewAttestationService(
		context.Background(),
		t.mockForkChoice,
		t.committeeSubscibe,
		t.ethClock,
		t.syncedData,
		&cfg,
		&clparams.NetworkConfig{},
		beaconevents.NewEventEmitter(),
		batchSignatureVerifier,
	)

	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return st.CommitteeCount(epoch)
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		return [32]byte{}, nil
	}
	blsVerifyMultipleSignatures = func(signatures, signRoots, pks [][]byte) (bool, error) {
		return true, nil
	}
	t.ethClock.EXPECT().GetEpochAtSlot(slot).Return(epoch).AnyTimes()
	t.ethClock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	topicClock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	messageDigest, err := topicClock.ComputeForkDigest(epoch)
	t.Require().NoError(err)
	postForkDigest, err := topicClock.ComputeForkDigest(epoch + 1)
	t.Require().NoError(err)
	t.ethClock.EXPECT().ComputeForkDigest(epoch).Return(messageDigest, nil).Times(2)
	t.mockForkChoice.HighestSeenVal = slot
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		blockRoot: {},
	}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		epoch * cfg.SlotsPerEpoch:                     {Root: targetRoot},
		finalizedCheckpoint.Epoch * cfg.SlotsPerEpoch: {Root: finalizedCheckpoint.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalizedCheckpoint
	t.committeeSubscibe.EXPECT().AggregateAttestation(gomock.Any()).Return(nil).AnyTimes()

	gloasMessage, err := t.attService.DecodeGossipMessage("peer", encoded, clparams.GloasVersion)
	t.Require().NoError(err)
	gloasMessage.SetTopicForkDigest(postForkDigest)
	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), gloasMessage)
	t.Require().ErrorIs(err, ErrIgnore)

	fuluMessage, err := t.attService.DecodeGossipMessage("peer", encoded, clparams.FuluVersion)
	t.Require().NoError(err)
	fuluMessage.SetTopicForkDigest(messageDigest)
	t.Require().NoError(t.attService.ProcessMessage(context.Background(), common.NewUint64(1), fuluMessage))
}

func TestAttestationGossipRejectsDifferentBPOForkDigest(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	cfg.BlobSchedule = []clparams.BlobParameters{
		{Epoch: 1, MaxBlobsPerBlock: 15},
		{Epoch: 2, MaxBlobsPerBlock: 21},
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	oldDigest, err := clock.ComputeForkDigest(1)
	require.NoError(t, err)
	messageDigest, err := clock.ComputeForkDigest(2)
	require.NoError(t, err)
	require.Equal(t, clparams.FuluVersion, cfg.GetCurrentStateVersion(1))
	require.Equal(t, clparams.FuluVersion, cfg.GetCurrentStateVersion(2))
	require.NotEqual(t, oldDigest, messageDigest)

	message := &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{Data: &solid.AttestationData{Slot: 2 * cfg.SlotsPerEpoch}},
		Receiver:          &sentinelproto.Peer{Pid: "peer"},
	}
	message.SetTopicForkDigest(oldDigest)
	service := &attestationService{ethClock: clock, beaconCfg: &cfg}

	err = service.ProcessMessage(context.Background(), nil, message)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "fork digest does not match topic")
}

func (t *attestationTestSuite) TestAttestationProcessMessageRejectsBeyondNextEpochDespiteForkchoiceHavingSeenIt() {
	beyondNextEpochSlot := mockSlot + 2*mockSlotsPerEpoch
	beyondNextEpoch := mockEpoch + 2
	beyondNextEpochAttData := *attData
	beyondNextEpochAttData.Slot = beyondNextEpochSlot
	beyondNextEpochAttData.Source.Epoch = beyondNextEpoch - 1
	beyondNextEpochAttData.Target.Epoch = beyondNextEpoch
	beyondNextEpochAtt := *att
	beyondNextEpochAtt.Data = &beyondNextEpochAttData

	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	t.ethClock.EXPECT().GetEpochAtSlot(beyondNextEpochSlot).Return(beyondNextEpoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(beyondNextEpochSlot).Times(1)
	t.mockForkChoice.HighestSeenVal = beyondNextEpochSlot

	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      &beyondNextEpochAtt,
		ImmediateProcess: true,
	})

	t.Require().Error(err)
	t.Require().Contains(err.Error(), "too far from attestation epoch")
}

func (t *attestationTestSuite) TestAttestationHeadCommitteeRejectionBanDependsOnShuffling() {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0

	var headState *state.CachingBeaconState
	t.Require().NoError(t.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		headState = s
		return nil
	}))
	committee, err := headState.GetBeaconCommitee(mockSlot, 0)
	t.Require().NoError(err)
	var outsider uint64
	activeValidators := headState.GetActiveValidatorsIndices(mockEpoch)
	for _, validatorIndex := range activeValidators {
		if !slices.Contains(committee, validatorIndex) {
			outsider = validatorIndex
			break
		}
	}
	t.Require().Contains(activeValidators, outsider)
	t.Require().NotContains(committee, outsider)

	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 { return 8 }
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 { return 1 }
	t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).AnyTimes()
	t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).AnyTimes()
	verifier := NewBatchSignatureVerifier(context.Background(), nil)
	t.attService = NewAttestationService(
		context.Background(),
		t.mockForkChoice,
		t.committeeSubscibe,
		t.ethClock,
		t.syncedData,
		&cfg,
		&clparams.NetworkConfig{},
		beaconevents.NewEventEmitter(),
		verifier,
	)

	headRoot := t.syncedData.HeadRoot()
	votedRoot := common.Hash{0xc1}
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		votedRoot: {Slot: mockSlot},
	}
	dependentSlot := forkchoice.ComputeShufflingDependentSlot(mockEpoch, cfg.MinSeedLookahead, cfg.SlotsPerEpoch)
	for _, test := range []struct {
		name           string
		otherShuffling bool
	}{
		{name: "other shuffling", otherShuffling: true},
		{name: "same shuffling"},
	} {
		t.Run(test.name, func() {
			headDependentRoot := common.Hash{0xd1}
			votedDependentRoot := headDependentRoot
			if test.otherShuffling {
				votedDependentRoot = common.Hash{0xd2}
			}
			t.mockForkChoice.AncestorFn = func(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
				if slot != dependentSlot {
					return forkchoice.ForkChoiceNode{}
				}
				if root == headRoot {
					return forkchoice.ForkChoiceNode{Root: headDependentRoot}
				}
				return forkchoice.ForkChoiceNode{Root: votedDependentRoot}
			}
			attestation := &AttestationForGossip{
				SingleAttestation: &solid.SingleAttestation{
					CommitteeIndex: 0,
					AttesterIndex:  outsider,
					Data: &solid.AttestationData{
						Slot:            mockSlot,
						BeaconBlockRoot: votedRoot,
						Source:          attData.Source,
						Target:          attData.Target,
					},
				},
			}

			err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), attestation)

			t.Require().EqualError(err, fmt.Sprintf("attester is not a member of the committee. attester index %d committeeIndex %v", outsider, uint64(0)))
			t.Require().Equal(test.otherShuffling, errors.Is(err, serviceintf.ErrRejectWithoutBan))
		})
	}
}

func (t *attestationTestSuite) TestAttestationInvalidSignatureBanDependsOnShufflingBeforeElectra() {
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 { return 8 }
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 { return 1 }
	t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).AnyTimes()
	t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	t.T().Cleanup(cancel)
	verifier := NewBatchSignatureVerifier(ctx, nil)
	cfg := clparams.MainnetBeaconConfig
	t.attService = NewAttestationService(
		ctx,
		t.mockForkChoice,
		t.committeeSubscibe,
		t.ethClock,
		t.syncedData,
		&cfg,
		&clparams.NetworkConfig{},
		beaconevents.NewEventEmitter(),
		verifier,
	)

	otherData := *attData
	sameData := *attData
	otherData.BeaconBlockRoot = common.Hash{0xe3}
	sameData.BeaconBlockRoot = common.Hash{0xe4}
	otherAttestation := *att
	sameAttestation := *att
	otherAttestation.Data = &otherData
	sameAttestation.Data = &sameData
	headRoot := t.syncedData.HeadRoot()
	finalized := solid.Checkpoint{Root: common.Hash{0x01}, Epoch: 1}
	t.mockForkChoice.FinalizedCheckpointVal = finalized
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		otherData.BeaconBlockRoot: {Slot: mockSlot},
		sameData.BeaconBlockRoot:  {Slot: mockSlot},
	}
	dependentSlot := forkchoice.ComputeShufflingDependentSlot(mockEpoch, cfg.MinSeedLookahead, cfg.SlotsPerEpoch)
	headDependentRoot := common.Hash{0xf2}
	t.mockForkChoice.AncestorFn = func(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
		switch slot {
		case dependentSlot:
			if root == otherData.BeaconBlockRoot {
				return forkchoice.ForkChoiceNode{Root: common.Hash{0xf3}}
			}
			if root == headRoot || root == sameData.BeaconBlockRoot {
				return forkchoice.ForkChoiceNode{Root: headDependentRoot}
			}
			return forkchoice.ForkChoiceNode{}
		case finalized.Epoch * cfg.SlotsPerEpoch:
			return forkchoice.ForkChoiceNode{Root: finalized.Root}
		case otherData.Target.Epoch * cfg.SlotsPerEpoch:
			return forkchoice.ForkChoiceNode{Root: otherData.Target.Root}
		default:
			return forkchoice.ForkChoiceNode{}
		}
	}

	for _, test := range []struct {
		name                 string
		attestation          *solid.Attestation
		rejectWithoutPeerBan bool
		wantSendingPeer      bool
	}{
		{name: "other shuffling", attestation: &otherAttestation, rejectWithoutPeerBan: true},
		{name: "same shuffling", attestation: &sameAttestation, wantSendingPeer: true},
	} {
		t.Run(test.name, func() {
			message := &AttestationForGossip{
				Attestation: test.attestation,
				Receiver:    &sentinelproto.Peer{Pid: test.name},
			}
			verification, result := captureQueuedVerification(t.T(), verifier.attVerifyAndExecute, func(ctx context.Context) error {
				return t.attService.ProcessMessage(ctx, common.NewUint64(1), message)
			})
			if test.wantSendingPeer {
				t.Require().Same(message.Receiver, verification.SendingPeer)
			} else {
				t.Require().Nil(verification.SendingPeer)
			}
			t.Require().True(verification.report(ErrInvalidBlsSignature))
			err := <-result

			t.Require().True(errors.Is(err, ErrInvalidBlsSignature))
			t.Require().Equal(test.rejectWithoutPeerBan, errors.Is(err, serviceintf.ErrRejectWithoutBan))
			t.Require().EqualError(err, ErrInvalidBlsSignature.Error())
		})
	}
}

func (t *attestationTestSuite) TestAttestationInvalidSignatureStillBansAfterElectra() {
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 { return 8 }
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 { return 1 }
	t.ethClock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).AnyTimes()
	t.ethClock.EXPECT().GetCurrentSlot().Return(mockSlot).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	t.T().Cleanup(cancel)
	verifier := NewBatchSignatureVerifier(ctx, nil)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	t.attService = NewAttestationService(
		ctx,
		t.mockForkChoice,
		t.committeeSubscibe,
		t.ethClock,
		t.syncedData,
		&cfg,
		&clparams.NetworkConfig{},
		beaconevents.NewEventEmitter(),
		verifier,
	)

	var headState *state.CachingBeaconState
	t.Require().NoError(t.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		headState = s
		return nil
	}))
	committee, err := headState.GetBeaconCommitee(mockSlot, 0)
	t.Require().NoError(err)
	t.Require().NotEmpty(committee)
	headRoot := t.syncedData.HeadRoot()
	votedRoot := common.Hash{0xe5}
	finalized := solid.Checkpoint{Root: common.Hash{0x01}, Epoch: 1}
	t.mockForkChoice.FinalizedCheckpointVal = finalized
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		votedRoot: {Slot: mockSlot},
	}
	dependentSlot := forkchoice.ComputeShufflingDependentSlot(mockEpoch, cfg.MinSeedLookahead, cfg.SlotsPerEpoch)
	t.mockForkChoice.AncestorFn = func(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
		switch slot {
		case dependentSlot:
			if root == headRoot {
				return forkchoice.ForkChoiceNode{Root: common.Hash{0xf4}}
			}
			return forkchoice.ForkChoiceNode{Root: common.Hash{0xf5}}
		case finalized.Epoch * cfg.SlotsPerEpoch:
			return forkchoice.ForkChoiceNode{Root: finalized.Root}
		case mockEpoch * cfg.SlotsPerEpoch:
			return forkchoice.ForkChoiceNode{Root: attData.Target.Root}
		default:
			return forkchoice.ForkChoiceNode{}
		}
	}
	message := &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{
			CommitteeIndex: 0,
			AttesterIndex:  committee[0],
			Data: &solid.AttestationData{
				Slot:            mockSlot,
				BeaconBlockRoot: votedRoot,
				Source:          attData.Source,
				Target:          attData.Target,
			},
		},
		Receiver: &sentinelproto.Peer{Pid: "electra-other-shuffling"},
	}

	verification, result := captureQueuedVerification(t.T(), verifier.attVerifyAndExecute, func(ctx context.Context) error {
		return t.attService.ProcessMessage(ctx, common.NewUint64(1), message)
	})
	t.Require().Same(message.Receiver, verification.SendingPeer)
	t.Require().True(verification.report(ErrInvalidBlsSignature))
	err = <-result
	t.Require().True(errors.Is(err, ErrInvalidBlsSignature))
	t.Require().False(errors.Is(err, serviceintf.ErrRejectWithoutBan))
	t.Require().EqualError(err, ErrInvalidBlsSignature.Error())
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
				subnet: common.NewUint64(1),
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
