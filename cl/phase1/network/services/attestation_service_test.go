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
	"bytes"
	"context"
	"errors"
	"log"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/aggregation"
	mockAggregation "github.com/erigontech/erigon/cl/aggregation/mock_services"
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
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	erigonlog "github.com/erigontech/erigon/common/log/v3"
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
	gomockCtrl      *gomock.Controller
	mockForkChoice  *mock_services.ForkChoiceStorageMock
	syncedData      synced_data.SyncedData
	aggregationPool *mockAggregation.MockAggregationPool
	ethClock        *eth_clock.MockEthereumClock
	attService      AttestationService
	beaconConfig    *clparams.BeaconChainConfig
	emitters        *beaconevents.EventEmitter
}

func (t *attestationTestSuite) SetupTest() {
	saveSignatureGlobals(t.T())
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockForkChoice = &mock_services.ForkChoiceStorageMock{}
	_, st, _ := tests.GetBellatrixRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.Require().NoError(t.syncedData.OnHeadState(st))
	t.aggregationPool = mockAggregation.NewMockAggregationPool(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:    mockSlotsPerEpoch,
		ElectraForkEpoch: 100000,
	}
	netConfig := &clparams.NetworkConfig{}
	emitters := beaconevents.NewEventEmitter()
	t.emitters = emitters
	computeSigningRoot = func(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) { return [32]byte{}, nil }
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	go batchSignatureVerifier.Start()
	ctx, cn := context.WithCancel(context.Background())
	cn()
	t.attService = NewAttestationService(ctx, t.mockForkChoice, t.aggregationPool, t.ethClock, t.syncedData, t.beaconConfig, netConfig, emitters, batchSignatureVerifier)
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
		reject  bool
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
				t.addValidAttestationPath(attData)
			},
			args: args{
				ctx:    context.Background(),
				subnet: nil,
				msg:    att,
			},
			wantErr: true,
			reject:  true,
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
				t.addValidAttestationPath(attData)
			},
			args: args{
				ctx:    context.Background(),
				subnet: common.NewUint64(1),
				msg:    att,
			},
			wantErr: true,
			reject:  true,
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
				t.addValidAttestationPath(attData)
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
			reject:  true,
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
				t.addValidAttestationPath(attData)
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
				t.addValidAttestationPath(attData)
			},
			args: args{
				ctx:    context.Background(),
				subnet: common.NewUint64(1),
				msg:    att,
			},
			wantErr: true,
			reject:  true,
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
				t.aggregationPool.EXPECT().AddAttestation(att).Return(nil).Times(1)
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
			if tt.reject {
				t.Require().NotErrorIs(err, ErrIgnore)
			}
		} else {
			t.Require().NoError(err)
		}

		t.True(t.gomockCtrl.Satisfied())
	}
}

func (t *attestationTestSuite) addValidAttestationPath(data *solid.AttestationData) {
	finalized := solid.Checkpoint{Root: common.Hash{0x33}, Epoch: data.Target.Epoch - 1}
	t.mockForkChoice.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{data.BeaconBlockRoot: {}}
	t.mockForkChoice.Ancestors = map[uint64]forkchoice.ForkChoiceNode{
		data.Target.Epoch * t.beaconConfig.SlotsPerEpoch: {Root: data.Target.Root},
		finalized.Epoch * t.beaconConfig.SlotsPerEpoch:   {Root: finalized.Root},
	}
	t.mockForkChoice.FinalizedCheckpointVal = finalized
}

func (t *attestationTestSuite) setupTargetShufflingAttestation(version clparams.StateVersion) (*state.CachingBeaconState, uint64, uint64) {
	cfg := clparams.MainnetBeaconConfig
	if version >= clparams.ElectraVersion {
		cfg.AltairForkEpoch = 0
		cfg.BellatrixForkEpoch = 0
		cfg.CapellaForkEpoch = 0
		cfg.DenebForkEpoch = 0
		cfg.ElectraForkEpoch = 0
		cfg.FuluForkEpoch = cfg.FarFutureEpoch
		cfg.GloasForkEpoch = cfg.FarFutureEpoch
	}
	_, headState, _ := tests.GetBellatrixRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&cfg, true)
	t.Require().NoError(t.syncedData.OnHeadState(headState))
	t.beaconConfig = &cfg
	t.mockForkChoice = &mock_services.ForkChoiceStorageMock{
		HeadLacksTargetShuffling: true,
		Headers:                  make(map[common.Hash]*cltypes.BeaconBlockHeader),
		Ancestors:                make(map[uint64]forkchoice.ForkChoiceNode),
	}
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	slot := headState.Slot()
	epoch := slot / cfg.SlotsPerEpoch
	t.ethClock.EXPECT().GetEpochAtSlot(slot).Return(epoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(slot).Times(1)
	origCommitteeCount, origSubnet := computeCommitteeCountPerSlot, computeSubnetForAttestation
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, slot, slotsPerEpoch uint64) uint64 {
		return headState.CommitteeCount(slot / slotsPerEpoch)
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 { return 1 }
	t.T().Cleanup(func() { computeCommitteeCountPerSlot, computeSubnetForAttestation = origCommitteeCount, origSubnet })
	computeSigningRoot = func(ssz.HashableSSZ, []byte) ([32]byte, error) { return [32]byte{}, nil }
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) { return true, nil }
	ctx, cancel := context.WithCancel(context.Background())
	t.T().Cleanup(cancel)
	verifier := NewBatchSignatureVerifier(ctx, nil)
	t.attService = NewAttestationService(ctx, t.mockForkChoice, t.aggregationPool, t.ethClock, t.syncedData, &cfg, &clparams.NetworkConfig{}, t.emitters, verifier)
	return headState, slot, epoch
}

func (t *attestationTestSuite) targetShufflingAttestationData(slot, epoch uint64) *solid.AttestationData {
	return &solid.AttestationData{
		Slot:            slot,
		BeaconBlockRoot: common.Hash{0x81},
		Source:          solid.Checkpoint{Epoch: epoch - 1, Root: common.Hash{0x71}},
		Target:          solid.Checkpoint{Epoch: epoch, Root: common.Hash{0x72}},
	}
}

func (t *attestationTestSuite) addTargetShufflingAncestors(data *solid.AttestationData) {
	finalized := solid.Checkpoint{Epoch: data.Target.Epoch - 1, Root: common.Hash{0x73}}
	t.mockForkChoice.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{Slot: data.Slot}
	t.mockForkChoice.Ancestors[data.Target.Epoch*t.beaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: data.Target.Root}
	t.mockForkChoice.Ancestors[finalized.Epoch*t.beaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: finalized.Root}
	t.mockForkChoice.FinalizedCheckpointVal = finalized
}

func validatorOutsideCommittee(t *testing.T, validatorCount int, committee []uint64) uint64 {
	for index := range validatorCount {
		if !slices.Contains(committee, uint64(index)) {
			return uint64(index)
		}
	}
	require.FailNow(t, "committee contains every validator")
	return 0
}

func (t *attestationTestSuite) TestElectraAttestationUsesTargetShuffling() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	var callOrder []string
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		callOrder = append(callOrder, "bls")
		return true, nil
	}
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	singleAttestation := &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data}
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		t.Require().Equal([]string{"bls"}, callOrder)
		callOrder = append(callOrder, "target")
		t.Require().Equal(data.Target, target)
		t.Require().Equal(slot, gotSlot)
		t.Require().Equal(singleAttestation.CommitteeIndex, committeeIndex)
		return []uint64{attester}, headState.CommitteeCount(epoch), nil
	}
	t.addTargetShufflingAncestors(data)
	t.aggregationPool.EXPECT().AddAttestation(gomock.Any()).Return(nil).Times(1)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: singleAttestation,
		ImmediateProcess:  true,
	})
	t.Require().NoError(err)
	t.Require().Equal([]string{"bls", "target", "bls"}, callOrder)
}

func (t *attestationTestSuite) TestElectraAttestationRejectsInvalidSignatureBeforeTargetShuffling() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	targetCalls := 0
	t.mockForkChoice.TargetBeaconCommitteeFn = func(solid.Checkpoint, uint64, uint64, func() bool) ([]uint64, uint64, error) {
		targetCalls++
		return []uint64{attester}, headState.CommitteeCount(epoch), nil
	}
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) { return false, nil }
	t.addTargetShufflingAncestors(data)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data},
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrInvalidBlsSignature)
	t.Require().NotErrorIs(err, ErrIgnore)
	t.Require().Zero(targetCalls)
}

func (t *attestationTestSuite) TestElectraAttestationAlreadySeenBeforeTargetShuffling() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	targetCalls := 0
	t.mockForkChoice.TargetBeaconCommitteeFn = func(solid.Checkpoint, uint64, uint64, func() bool) ([]uint64, uint64, error) {
		targetCalls++
		return []uint64{attester}, headState.CommitteeCount(epoch), nil
	}
	t.addTargetShufflingAncestors(data)
	t.attService.(*attestationService).validatorAttestationSeen.Add(attester, epoch)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data},
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().ErrorIs(err, ErrAttestationAlreadySeen)
	t.Require().Zero(targetCalls)
}

func (t *attestationTestSuite) TestElectraAttestationRejectsNonMemberOfTargetCommittee() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	t.Require().NotEmpty(headCommittee)
	attester := headCommittee[0]
	targetCommittee := []uint64{validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)}
	singleAttestation := &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data}
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		t.Require().Equal(data.Target, target)
		t.Require().Equal(slot, gotSlot)
		t.Require().Equal(singleAttestation.CommitteeIndex, committeeIndex)
		return targetCommittee, headState.CommitteeCount(epoch), nil
	}
	t.addTargetShufflingAncestors(data)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: singleAttestation,
		ImmediateProcess:  true,
	})
	t.Require().ErrorContains(err, "attester is not a member of the committee")
	t.Require().NotErrorIs(err, ErrIgnore)
}

func cachingTargetBeaconCommittee(committee []uint64, committeesPerSlot uint64, builds *int) func(solid.Checkpoint, uint64, uint64, func() bool) ([]uint64, uint64, error) {
	cached := make(map[solid.Checkpoint]bool)
	return func(target solid.Checkpoint, _, _ uint64, claimBuild func() bool) ([]uint64, uint64, error) {
		if !cached[target] {
			if claimBuild != nil && !claimBuild() {
				return nil, 0, errors.New("target state build already claimed")
			}
			*builds++
			cached[target] = true
		}
		return committee, committeesPerSlot, nil
	}
}

func (t *attestationTestSuite) TestElectraAttestationBuildsUncachedTargetOncePerAttesterAndEpoch() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	t.ethClock.EXPECT().GetEpochAtSlot(slot).Return(epoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(slot).Times(1)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	t.Require().NotEmpty(headCommittee)
	attester := headCommittee[0]
	targetCommittee := []uint64{validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)}
	builds := 0
	t.mockForkChoice.TargetBeaconCommitteeFn = cachingTargetBeaconCommittee(targetCommittee, headState.CommitteeCount(epoch), &builds)
	first := t.targetShufflingAttestationData(slot, epoch)
	t.addTargetShufflingAncestors(first)
	second := t.targetShufflingAttestationData(slot, epoch)
	second.BeaconBlockRoot = common.Hash{0x83}
	second.Target.Root = common.Hash{0x84}
	t.mockForkChoice.Headers[second.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{Slot: second.Slot}
	t.mockForkChoice.AncestorFn = func(root common.Hash, gotSlot uint64) forkchoice.ForkChoiceNode {
		if root == second.BeaconBlockRoot && gotSlot == epoch*t.beaconConfig.SlotsPerEpoch {
			return forkchoice.ForkChoiceNode{Root: second.Target.Root}
		}
		return t.mockForkChoice.Ancestors[gotSlot]
	}

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: first},
		ImmediateProcess:  true,
	})
	t.Require().ErrorContains(err, "attester is not a member of the committee")
	t.Require().NotErrorIs(err, ErrIgnore)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: second},
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().Equal(1, builds)
}

func (t *attestationTestSuite) TestElectraAttestationBuildsVotedBlockTargetBeforeRejectingMessageTarget() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	derivedRoot := common.Hash{0x82}
	var gotTarget solid.Checkpoint
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		gotTarget = target
		t.Require().Equal(slot, gotSlot)
		t.Require().Zero(committeeIndex)
		return []uint64{attester}, headState.CommitteeCount(epoch), nil
	}
	t.addTargetShufflingAncestors(data)
	t.mockForkChoice.AncestorFn = func(root common.Hash, gotSlot uint64) forkchoice.ForkChoiceNode {
		if root == data.BeaconBlockRoot && gotSlot == epoch*t.beaconConfig.SlotsPerEpoch {
			return forkchoice.ForkChoiceNode{Root: derivedRoot}
		}
		return t.mockForkChoice.Ancestors[gotSlot]
	}

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data},
		ImmediateProcess:  true,
	})
	t.Require().Equal(solid.Checkpoint{Epoch: epoch, Root: derivedRoot}, gotTarget)
	t.Require().ErrorContains(err, "invalid target block")
	t.Require().NotErrorIs(err, ErrIgnore)
}

func (t *attestationTestSuite) TestElectraAttestationRejectsNonMemberBeforeFinalizedAncestor() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	committee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), committee)
	singleAttestation := &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data}
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		t.Require().Equal(data.Target, target)
		t.Require().Equal(slot, gotSlot)
		t.Require().Equal(singleAttestation.CommitteeIndex, committeeIndex)
		return committee, headState.CommitteeCount(epoch), nil
	}
	t.mockForkChoice.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{Slot: data.Slot}
	t.mockForkChoice.Ancestors[data.Target.Epoch*t.beaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: data.Target.Root}
	finalized := solid.Checkpoint{Epoch: epoch - 1, Root: common.Hash{0x73}}
	t.mockForkChoice.FinalizedCheckpointVal = finalized
	t.mockForkChoice.Ancestors[finalized.Epoch*t.beaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: common.Hash{0x74}}

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: singleAttestation,
		ImmediateProcess:  true,
	})
	t.Require().ErrorContains(err, "attester is not a member of the committee")
	t.Require().NotErrorIs(err, ErrIgnore)
}

func (t *attestationTestSuite) TestAttestationForUnseenBlockIsIgnoredBeforeCommitteeChecks() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	t.mockForkChoice.HeadLacksTargetShuffling = false

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data},
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().ErrorIs(err, ErrBlockNotSeen)
}

func (t *attestationTestSuite) TestAttestationForUnseenBlockWithNilSubnetIsIgnored() {
	_, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	var err error
	t.Require().NotPanics(func() {
		err = t.attService.ProcessMessage(context.Background(), nil, &AttestationForGossip{
			SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, Data: data},
		})
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().ErrorIs(err, ErrBlockNotSeen)
}

func (t *attestationTestSuite) TestAttestationIgnoresUnknownVotedBlockTargetAncestor() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	blsCalls := 0
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		blsCalls++
		return true, nil
	}
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	singleAttestation := &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data}
	targetCalls := 0
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		targetCalls++
		t.Require().Equal(data.Target, target)
		t.Require().Equal(slot, gotSlot)
		t.Require().Equal(singleAttestation.CommitteeIndex, committeeIndex)
		return nil, 0, errors.New("target state unavailable")
	}
	t.addTargetShufflingAncestors(data)
	delete(t.mockForkChoice.Ancestors, data.Target.Epoch*t.beaconConfig.SlotsPerEpoch)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: singleAttestation,
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().Equal([]int{0, 0}, []int{blsCalls, targetCalls})
}

func (t *attestationTestSuite) TestAttestationIgnoresUnavailableTargetCheckpointState() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	blsCalls := 0
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		blsCalls++
		return true, nil
	}
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	singleAttestation := &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data}
	targetCalls := 0
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		targetCalls++
		t.Require().Equal(data.Target, target)
		t.Require().Equal(slot, gotSlot)
		t.Require().Equal(singleAttestation.CommitteeIndex, committeeIndex)
		return nil, 0, errors.New("target state unavailable")
	}
	t.addTargetShufflingAncestors(data)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: singleAttestation,
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().Equal([]int{1, 1}, []int{blsCalls, targetCalls})
}

func (t *attestationTestSuite) TestAttestationKeepsHeadShufflingFastPath() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	blsCalls := 0
	blsVerifyMultipleSignatures = func([][]byte, [][]byte, [][]byte) (bool, error) {
		blsCalls++
		return true, nil
	}
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	t.mockForkChoice.HeadLacksTargetShuffling = false
	t.mockForkChoice.TargetBeaconCommitteeFn = func(solid.Checkpoint, uint64, uint64, func() bool) ([]uint64, uint64, error) {
		t.T().Fatal("target committee lookup used on the head fast path")
		return nil, 0, nil
	}
	t.addTargetShufflingAncestors(data)
	t.aggregationPool.EXPECT().AddAttestation(gomock.Any()).Return(nil).Times(1)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: headCommittee[0], Data: data},
		ImmediateProcess:  true,
	})
	t.Require().NoError(err)
	t.Require().Equal(1, blsCalls)
	t.Require().Equal(int64(2), t.mockForkChoice.AncestorCalls.Load())
}

func (t *attestationTestSuite) TestElectraAttestationAlreadySeenBeforeTargetAncestor() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.ElectraVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	committee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	t.Require().NotEmpty(committee)
	t.mockForkChoice.HeadLacksTargetShuffling = false
	finalized := solid.Checkpoint{Epoch: epoch - 1, Root: common.Hash{0x73}}
	t.mockForkChoice.Headers[data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{Slot: data.Slot}
	t.mockForkChoice.Ancestors[finalized.Epoch*t.beaconConfig.SlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: finalized.Root}
	t.mockForkChoice.FinalizedCheckpointVal = finalized
	attester := committee[0]
	t.attService.(*attestationService).validatorAttestationSeen.Add(attester, epoch)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{CommitteeIndex: 0, AttesterIndex: attester, Data: data},
		ImmediateProcess:  true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().ErrorIs(err, ErrAttestationAlreadySeen)
}

func (t *attestationTestSuite) TestPreElectraAttestationUsesTargetCommitteeMember() {
	headState, slot, epoch := t.setupTargetShufflingAttestation(clparams.DenebVersion)
	data := t.targetShufflingAttestationData(slot, epoch)
	headCommittee, err := headState.GetBeaconCommitee(slot, 0)
	t.Require().NoError(err)
	attester := validatorOutsideCommittee(t.T(), headState.ValidatorLength(), headCommittee)
	t.mockForkChoice.TargetBeaconCommitteeFn = func(target solid.Checkpoint, gotSlot, committeeIndex uint64, _ func() bool) ([]uint64, uint64, error) {
		t.Require().Equal(data.Target, target)
		t.Require().Equal(slot, gotSlot)
		t.Require().Equal(data.CommitteeIndex, committeeIndex)
		return []uint64{attester}, headState.CommitteeCount(epoch), nil
	}
	t.addTargetShufflingAncestors(data)
	wantPubkey, err := headState.ValidatorPublicKey(int(attester))
	t.Require().NoError(err)
	var gotPubkey []byte
	blsVerifyMultipleSignatures = func(_ [][]byte, _ [][]byte, pks [][]byte) (bool, error) {
		gotPubkey = append([]byte(nil), pks[0]...)
		return true, nil
	}
	t.aggregationPool.EXPECT().AddAttestation(gomock.Any()).Return(nil).Times(1)

	err = t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation: &solid.Attestation{
			AggregationBits: solid.BitlistFromBytes([]byte{0b11}, 2048),
			Data:            data,
		},
		ImmediateProcess: true,
	})
	t.Require().NoError(err)
	t.Require().Equal(wantPubkey[:], gotPubkey)
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
	t.aggregationPool.EXPECT().AddAttestation(&nextEpochAtt).Return(nil).Times(1)

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
	t.aggregationPool.EXPECT().AddAttestation(&secondEpochAtt).Return(nil).Times(1)

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
	t.aggregationPool.EXPECT().AddAttestation(att).Return(nil).Times(1)

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
	t.aggregationPool.EXPECT().AddAttestation(att).Return(nil).Times(1)

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

// Copies of one vote can pass the seen check before either is verified, for example one over
// REST and one from gossip. Only the first to be verified may reach the pool and the event stream.
func (t *attestationTestSuite) TestVerifiedAttestationIsClaimedOnce() {
	service := t.attService.(*attestationService)
	t.aggregationPool.EXPECT().AddAttestation(att).Return(nil).Times(1)
	events := make(chan *beaconevents.EventStream, 16)
	sub := t.emitters.Operation().Subscribe(events)
	defer sub.Unsubscribe()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			service.onVerifiedAttestation(7, mockEpoch, att, &AttestationForGossip{Attestation: att})
		})
	}
	wg.Wait()

	t.Len(events, 1)
}

// A pool error other than an already held vote does not make the attestation invalid.
func (t *attestationTestSuite) TestAttestationEventEmittedWhenPoolRejectsVote() {
	service := t.attService.(*attestationService)
	t.aggregationPool.EXPECT().AddAttestation(att).Return(errors.New("pool error")).Times(1)
	events := make(chan *beaconevents.EventStream, 4)
	sub := t.emitters.Operation().Subscribe(events)
	defer sub.Unsubscribe()

	service.onVerifiedAttestation(7, mockEpoch, att, &AttestationForGossip{Attestation: att})

	t.Len(events, 1)
}

// A vote of a member the pool already counts for other data is equivocation that gossip can carry, so
// it is not logged as a warning, and its attestation event is still emitted.
func (t *attestationTestSuite) TestAttestationOfACountedMemberIsNotAWarning() {
	service := t.attService.(*attestationService)
	t.aggregationPool.EXPECT().AddAttestation(att).Return(aggregation.ErrVoterAlreadyCounted).Times(1)
	events := make(chan *beaconevents.EventStream, 4)
	sub := t.emitters.Operation().Subscribe(events)
	defer sub.Unsubscribe()
	var output bytes.Buffer
	previous := erigonlog.Root().GetHandler()
	erigonlog.Root().SetHandler(erigonlog.LvlFilterHandler(erigonlog.LvlWarn, erigonlog.StreamHandler(&output, erigonlog.LogfmtFormat())))
	defer erigonlog.Root().SetHandler(previous)

	service.onVerifiedAttestation(7, mockEpoch, att, &AttestationForGossip{Attestation: att})

	t.Len(events, 1)
	t.Empty(output.String())
}

// The attestation event reports every verified attestation, whether or not the
// aggregation pool already holds its vote.
func (t *attestationTestSuite) TestAttestationEventEmittedWhenPoolAlreadyHasVote() {
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 {
		return 8
	}
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 {
		return 1
	}
	blsVerifyMultipleSignatures = func(signatures [][]byte, signRoots [][]byte, pks [][]byte) (bool, error) {
		return true, nil
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
	t.aggregationPool.EXPECT().AddAttestation(att).Return(aggregation.ErrIsSuperset).Times(1)
	events := make(chan *beaconevents.EventStream, 4)
	sub := t.emitters.Operation().Subscribe(events)
	defer sub.Unsubscribe()

	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		Attestation:      att,
		ImmediateProcess: true,
	})
	t.Require().NoError(err)

	select {
	case event := <-events:
		t.Equal(beaconevents.OpAttestation, event.Event)
	default:
		t.Fail("no attestation event emitted")
	}
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
	t.aggregationPool.EXPECT().AddAttestation(gomock.Any()).Times(0)

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
		t.aggregationPool,
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
	t.aggregationPool.EXPECT().AddAttestation(gomock.Any()).Return(nil).AnyTimes()

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

func (t *attestationTestSuite) TestElectraAttestationChecksEpochRangeBeforeStructuralIndex() {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = cfg.FarFutureEpoch
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	t.beaconConfig = &cfg
	ctx, cancel := context.WithCancel(context.Background())
	t.T().Cleanup(cancel)
	verifier := NewBatchSignatureVerifier(ctx, nil)
	t.attService = NewAttestationService(ctx, t.mockForkChoice, t.aggregationPool, t.ethClock, t.syncedData, &cfg, &clparams.NetworkConfig{}, t.emitters, verifier)

	slot := mockSlot + 2*mockSlotsPerEpoch
	epoch := slot / mockSlotsPerEpoch
	t.ethClock.EXPECT().GetEpochAtSlot(slot).Return(epoch).Times(1)
	t.ethClock.EXPECT().GetCurrentSlot().Return(slot).Times(1)
	t.mockForkChoice.HighestSeenVal = slot

	err := t.attService.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{
		SingleAttestation: &solid.SingleAttestation{
			CommitteeIndex: 0,
			Data: &solid.AttestationData{
				Slot:           slot,
				CommitteeIndex: 1,
				Target:         solid.Checkpoint{Epoch: epoch},
			},
		},
		ImmediateProcess: true,
	})
	t.Require().ErrorIs(err, ErrIgnore)
	t.Require().ErrorContains(err, "too far from attestation epoch")
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
		t.attService = NewAttestationService(ctx, t.mockForkChoice, t.aggregationPool, t.ethClock, t.syncedData, gloasConfig, netConfig, emitters, batchSignatureVerifier)

		tt.mock()
		err := t.attService.ProcessMessage(tt.args.ctx, tt.args.subnet, &AttestationForGossip{
			SingleAttestation: tt.args.msg,
			ImmediateProcess:  true,
		})
		if tt.wantErr {
			t.Require().Error(err, "test case: %s", tt.name)
			if tt.errMsg != "" {
				t.Require().Contains(err.Error(), tt.errMsg, "test case: %s", tt.name)
				t.Require().NotErrorIs(err, ErrIgnore, "test case: %s", tt.name)
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

func TestAttestationForLateBlockIsProcessedOnceBlockArrives(t *testing.T) {
	saveSignatureGlobals(t)
	origCommitteeCount, origSubnet := computeCommitteeCountPerSlot, computeSubnetForAttestation
	t.Cleanup(func() { computeCommitteeCountPerSlot, computeSubnetForAttestation = origCommitteeCount, origSubnet })
	computeCommitteeCountPerSlot = func(_ abstract.BeaconStateReader, _, _ uint64) uint64 { return 8 }
	computeSubnetForAttestation = func(_, _, _, _, _ uint64) uint64 { return 1 }
	computeSigningRoot = func(ssz.HashableSSZ, []byte) ([32]byte, error) { return [32]byte{}, nil }
	blsVerifyMultipleSignatures = func(_, _, _ [][]byte) (bool, error) { return true, nil }

	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetEpochAtSlot(mockSlot).Return(mockEpoch).AnyTimes()
	clock.EXPECT().GetCurrentSlot().Return(mockSlot).AnyTimes()
	processed := make(chan struct{})
	aggregationPool := mockAggregation.NewMockAggregationPool(ctrl)
	aggregationPool.EXPECT().AddAttestation(att).DoAndReturn(func(*solid.Attestation) error {
		close(processed)
		return nil
	}).Times(1)

	_, st, _ := tests.GetBellatrixRandom()
	syncedData := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	require.NoError(t, syncedData.OnHeadState(st))
	finalizedCheckpoint := solid.Checkpoint{Root: [32]byte{1, 0}, Epoch: 1}
	forkChoice := &lateBlockForkChoice{
		ForkChoiceStorageMock: &mock_services.ForkChoiceStorageMock{
			Headers: map[common.Hash]*cltypes.BeaconBlockHeader{attData.BeaconBlockRoot: {}},
			Ancestors: map[uint64]forkchoice.ForkChoiceNode{
				mockEpoch * mockSlotsPerEpoch:                 {Root: attData.Target.Root},
				finalizedCheckpoint.Epoch * mockSlotsPerEpoch: {Root: finalizedCheckpoint.Root},
			},
			FinalizedCheckpointVal: finalizedCheckpoint,
			HighestSeenVal:         mockSlot,
		},
		root: attData.BeaconBlockRoot,
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	verifier := NewBatchSignatureVerifier(ctx, nil)
	verifier.Start()
	beaconCfg := &clparams.BeaconChainConfig{SlotsPerEpoch: mockSlotsPerEpoch, SecondsPerSlot: 12, ElectraForkEpoch: 100000}
	service := NewAttestationService(ctx, forkChoice, aggregationPool, clock, syncedData, beaconCfg, &clparams.NetworkConfig{}, beaconevents.NewEventEmitter(), verifier)

	err := service.ProcessMessage(context.Background(), common.NewUint64(1), &AttestationForGossip{Attestation: att})
	require.ErrorIs(t, err, ErrIgnore)

	forkChoice.arrived.Store(true)
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("attestation was not processed after its block arrived")
	}
}
