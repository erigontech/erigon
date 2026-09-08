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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

type voluntaryExitTestSuite struct {
	suite.Suite
	gomockCtrl           *gomock.Controller
	operationsPool       *pool.OperationsPool
	emitters             *beaconevents.EventEmitter
	syncedData           synced_data.SyncedData
	ethClock             eth_clock.EthereumClock
	beaconCfg            *clparams.BeaconChainConfig
	voluntaryExitService VoluntaryExitService

	mockFuncs *mockFuncs
}

func (t *voluntaryExitTestSuite) SetupTest() {
	saveSignatureGlobals(t.T())
	computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		return [32]byte{}, nil
	}
	t.gomockCtrl = gomock.NewController(t.T())
	t.emitters = beaconevents.NewEventEmitter()
	t.operationsPool = &pool.OperationsPool{
		VoluntaryExitsPool: pool.NewOperationPool[uint64, *cltypes.SignedVoluntaryExit](10, "voluntaryExitsPool"),
	}
	_, st, _ := tests.GetBellatrixRandom()
	t.Require().NoError(st.SetSlot(100 * clparams.MainnetBeaconConfig.SlotsPerEpoch))
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.Require().NoError(t.syncedData.OnHeadState(st))
	t.beaconCfg = &clparams.BeaconChainConfig{SlotsPerEpoch: 32, SecondsPerSlot: 12}
	t.ethClock = eth_clock.NewEthereumClock(0, common.Hash{}, t.beaconCfg)
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	batchSignatureVerifier.Start()
	t.voluntaryExitService = NewVoluntaryExitService(*t.operationsPool, t.emitters, t.syncedData, t.beaconCfg, t.ethClock, batchSignatureVerifier)
	t.voluntaryExitService.(*voluntaryExitService).now = func() time.Time { return t.ethClock.GetSlotTime(100 * t.beaconCfg.SlotsPerEpoch) }
	// mock global functions
	t.mockFuncs = &mockFuncs{
		ctrl: t.gomockCtrl,
	}
	blsVerify = t.mockFuncs.BlsVerify
	blsVerifyMultipleSignatures = t.mockFuncs.BlsVerifyMultipleSignatures
}

func (t *voluntaryExitTestSuite) TearDownTest() {
}

func (t *voluntaryExitTestSuite) TestProcessMessage() {
	curEpoch := uint64(100)
	mockValidatorIndex := uint64(10)
	mockMsg := &SignedVoluntaryExitForGossip{
		SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{
			VoluntaryExit: &cltypes.VoluntaryExit{
				Epoch:          1,
				ValidatorIndex: mockValidatorIndex,
			},
			Signature: [96]byte{},
		},
		ImmediateVerification: true,
	}
	mockMsg2 := &SignedVoluntaryExitForGossip{
		SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{
			VoluntaryExit: &cltypes.VoluntaryExit{
				Epoch:          1,
				ValidatorIndex: 111111111,
			},
			Signature: [96]byte{},
		},
		ImmediateVerification: true,
	}

	_, _, _ = mockMsg, mockMsg2, curEpoch

	tests := []struct {
		name    string
		mock    func()
		msg     *SignedVoluntaryExitForGossip
		wantErr bool
		err     error
	}{
		{
			name: "validator already in pool",
			mock: func() {
				t.operationsPool.VoluntaryExitsPool.Insert(mockValidatorIndex, mockMsg.SignedVoluntaryExit)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "state is nil",
			mock: func() {
				t.syncedData.UnsetHeadState()
			},
			msg:     mockMsg,
			wantErr: true,
			err:     synced_data.ErrNotSynced,
		},
		{
			name: "validator not found",
			mock: func() {
			},
			msg:     mockMsg2,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "validator is not active",
			mock: func() {
				_, st, _ := tests.GetBellatrixRandom()
				t.Require().NoError(st.SetSlot(100 * clparams.MainnetBeaconConfig.SlotsPerEpoch))
				mockValidator := solid.NewValidatorFromParameters(
					[48]byte{},
					[32]byte{},
					0,
					false,
					0,
					0,
					0,
					0,
				)
				st.ValidatorSet().Set(int(mockValidatorIndex), mockValidator)
				t.Require().NoError(t.syncedData.OnHeadState(st))
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "validator has been initialized",
			mock: func() {
				// mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				// mockValidator := solid.NewValidatorFromParameters(
				// 	[48]byte{},
				// 	[32]byte{},
				// 	0,
				// 	false,
				// 	0,
				// 	0,
				// 	curEpoch+1,
				// 	0,
				// )
				// mockState.EXPECT().ValidatorForValidatorIndex(int(mockValidatorIndex)).Return(mockValidator, nil).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "bls verify failed",
			mock: func() {
				mockValidator := solid.NewValidatorFromParameters(
					[48]byte{},
					[32]byte{},
					0,
					false,
					0,
					0,
					curEpoch+1,
					0,
				)
				_, st, _ := tests.GetBellatrixRandom()
				t.Require().NoError(st.SetSlot(100 * clparams.MainnetBeaconConfig.SlotsPerEpoch))
				st.ValidatorSet().Set(int(mockValidatorIndex), mockValidator)
				t.Require().NoError(t.syncedData.OnHeadState(st))
				t.beaconCfg.FarFutureEpoch = mockValidator.ExitEpoch()
				computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}
				t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(2)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "success",
			mock: func() {
				_, st, _ := tests.GetBellatrixRandom()
				t.Require().NoError(st.SetSlot(100 * clparams.MainnetBeaconConfig.SlotsPerEpoch))
				mockValidator := solid.NewValidatorFromParameters(
					[48]byte{},
					[32]byte{},
					0,
					false,
					0,
					0,
					curEpoch+1,
					0,
				)
				st.ValidatorSet().Set(int(mockValidatorIndex), mockValidator)
				t.Require().NoError(t.syncedData.OnHeadState(st))
				t.beaconCfg.FarFutureEpoch = mockValidator.ExitEpoch()
				computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}

				t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
			},
			msg:     mockMsg,
			err:     ErrIgnore,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		log.Printf("VoluntaryExit running test case: %s", tt.name)
		t.SetupTest()
		tt.mock()
		err := t.voluntaryExitService.ProcessMessage(context.Background(), nil, tt.msg)
		if tt.wantErr {
			t.Require().Error(err)
			if tt.err != nil {
				t.Require().Equal(tt.err, err)
			}
			log.Printf("error msg: %v", err.Error())
		} else {
			t.Require().NoError(err)
		}
	}
}

func (t *voluntaryExitTestSuite) TestSeenValidatorIsIgnoredAfterPoolPrune() {
	const (
		currentEpoch   = uint64(100)
		validatorIndex = uint64(10)
	)
	msg := &SignedVoluntaryExitForGossip{
		SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{
			VoluntaryExit: &cltypes.VoluntaryExit{
				Epoch:          1,
				ValidatorIndex: validatorIndex,
			},
		},
		ImmediateVerification: true,
	}
	_, st, _ := tests.GetBellatrixRandom()
	t.Require().NoError(st.SetSlot(100 * clparams.MainnetBeaconConfig.SlotsPerEpoch))
	validator := solid.NewValidatorFromParameters(
		common.Bytes48{},
		common.Hash{},
		0,
		false,
		0,
		0,
		currentEpoch+1,
		0,
	)
	st.ValidatorSet().Set(int(validatorIndex), validator)
	t.Require().NoError(t.syncedData.OnHeadState(st))
	t.beaconCfg.FarFutureEpoch = validator.ExitEpoch()
	t.gomockCtrl.RecordCall(
		t.mockFuncs,
		"BlsVerifyMultipleSignatures",
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(true, nil).Times(1)

	t.Require().NoError(t.voluntaryExitService.ProcessMessage(context.Background(), nil, msg))
	t.Require().True(t.operationsPool.VoluntaryExitsPool.DeleteIfExist(validatorIndex))
	validator.SetExitEpoch(0)
	st.ValidatorSet().Set(int(validatorIndex), validator)
	t.Require().NoError(t.syncedData.OnHeadState(st))

	t.Require().ErrorIs(t.voluntaryExitService.ProcessMessage(context.Background(), nil, msg), ErrIgnore)
	t.Require().True(t.gomockCtrl.Satisfied())
}

func (t *voluntaryExitTestSuite) TestIncompleteMessageReturnsError() {
	for _, msg := range []*SignedVoluntaryExitForGossip{
		nil,
		{},
		{SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{}},
	} {
		t.Require().NotPanics(func() {
			t.Require().Error(t.voluntaryExitService.ProcessMessage(context.Background(), nil, msg))
		})
	}
}

func (t *voluntaryExitTestSuite) TestFutureExitIgnoredBeforeHeadLookup() {
	service := t.voluntaryExitService.(*voluntaryExitService)
	cfg := clparams.MainnetBeaconConfig
	service.beaconCfg = &cfg
	service.ethClock = eth_clock.NewEthereumClock(1000, common.Hash{}, &cfg)
	service.now = func() time.Time {
		return service.ethClock.GetSlotTime(101 * cfg.SlotsPerEpoch).Add(-500*time.Millisecond - time.Millisecond)
	}
	t.syncedData.UnsetHeadState()
	for _, epoch := range []uint64{101, math.MaxUint64} {
		msg := &SignedVoluntaryExitForGossip{SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: epoch, ValidatorIndex: 10}}, ImmediateVerification: true}
		t.Require().ErrorIs(service.ProcessMessage(context.Background(), nil, msg), ErrIgnore)
		t.Require().False(service.seen.Contains(10))
		t.Require().False(t.operationsPool.VoluntaryExitsPool.Has(10))
	}
}

func (t *voluntaryExitTestSuite) TestExitAcceptedAtClockDisparityWithLaggingHead() {
	service := t.voluntaryExitService.(*voluntaryExitService)
	cfg := clparams.MainnetBeaconConfig
	cfg.ShardCommitteePeriod = 100
	service.beaconCfg = &cfg
	_, st, _ := tests.GetBellatrixRandom()
	t.Require().NoError(st.SetSlot(100 * cfg.SlotsPerEpoch))
	st.ValidatorSet().Set(10, solid.NewValidatorFromParameters(common.Bytes48{}, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch))
	t.Require().NoError(t.syncedData.OnHeadState(st))
	clock := eth_clock.NewEthereumClock(1000, common.Hash{}, &cfg)
	service.ethClock = clock
	boundary := clock.GetSlotTime(101 * cfg.SlotsPerEpoch)
	service.now = func() time.Time { return boundary.Add(-500 * time.Millisecond) }
	t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	msg := &SignedVoluntaryExitForGossip{SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: 101, ValidatorIndex: 10}}, ImmediateVerification: true}
	t.Require().NoError(service.ProcessMessage(context.Background(), nil, msg))
	t.Require().True(service.seen.Contains(10))
	t.Require().True(t.operationsPool.VoluntaryExitsPool.Has(10))
}

func (t *voluntaryExitTestSuite) exitAtEpochs(headEpoch, wallEpoch, activationEpoch, committeePeriod, exitEpoch uint64) (*voluntaryExitService, *SignedVoluntaryExitForGossip) {
	service := t.voluntaryExitService.(*voluntaryExitService)
	cfg := clparams.MainnetBeaconConfig
	cfg.ShardCommitteePeriod = committeePeriod
	service.beaconCfg = &cfg
	_, st, _ := tests.GetBellatrixRandom()
	t.Require().NoError(st.SetSlot(headEpoch * cfg.SlotsPerEpoch))
	st.ValidatorSet().Set(10, solid.NewValidatorFromParameters(common.Bytes48{}, common.Hash{}, 0, false, 0, activationEpoch, exitEpoch, cfg.FarFutureEpoch))
	t.Require().NoError(t.syncedData.OnHeadState(st))
	clock := eth_clock.NewEthereumClock(1000, common.Hash{}, &cfg)
	service.ethClock = clock
	service.now = func() time.Time { return clock.GetSlotTime(wallEpoch * cfg.SlotsPerEpoch) }
	t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	return service, &SignedVoluntaryExitForGossip{SignedVoluntaryExit: &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: wallEpoch, ValidatorIndex: 10}}, ImmediateVerification: true}
}

func (t *voluntaryExitTestSuite) TestExitActivityUsesHeadEpoch() {
	service, msg := t.exitAtEpochs(99, 100, 100, 0, math.MaxUint64)
	t.Require().EqualError(service.ProcessMessage(context.Background(), nil, msg), "validator is not active")
	t.Require().False(service.seen.Contains(10))
	t.Require().False(t.operationsPool.VoluntaryExitsPool.Has(10))
}

func (t *voluntaryExitTestSuite) TestExitTenureUsesHeadEpoch() {
	service, msg := t.exitAtEpochs(99, 100, 0, 100, math.MaxUint64)
	t.Require().EqualError(service.ProcessMessage(context.Background(), nil, msg), "verify the validator has been active long enough")
	t.Require().False(service.seen.Contains(10))
	t.Require().False(t.operationsPool.VoluntaryExitsPool.Has(10))
}

func (t *voluntaryExitTestSuite) TestInitiatedExitIgnoredBeforeActivity() {
	service, msg := t.exitAtEpochs(100, 100, 0, 0, 99)
	t.Require().ErrorIs(service.ProcessMessage(context.Background(), nil, msg), ErrIgnore)
	t.Require().False(service.seen.Contains(10))
	t.Require().False(t.operationsPool.VoluntaryExitsPool.Has(10))
}

func TestVoluntaryExit(t *testing.T) {
	//t.Skip("issue #14997")
	suite.Run(t, new(voluntaryExitTestSuite))
}
