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
	"runtime"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	mockState "github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type blsToExecutionChangeTestSuite struct {
	suite.Suite
	gomockCtrl     *gomock.Controller
	operationsPool *pool.OperationsPool
	emitters       *beaconevents.EventEmitter
	syncedData     synced_data.SyncedData
	beaconCfg      *clparams.BeaconChainConfig

	service   BLSToExecutionChangeService
	mockFuncs *mockFuncs
}

func (t *blsToExecutionChangeTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.operationsPool = &pool.OperationsPool{
		BLSToExecutionChangesPool: pool.NewOperationPool[common.Bytes96, *cltypes.SignedBLSToExecutionChange](10, "blsToExecutionChangesPool"),
	}
	_, st, _ := tests.GetCapellaRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.syncedData.OnHeadState(st)
	t.emitters = beaconevents.NewEventEmitter()
	t.beaconCfg = &clparams.BeaconChainConfig{}
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	go batchSignatureVerifier.Start()

	t.service = NewBLSToExecutionChangeService(*t.operationsPool, t.emitters, t.syncedData, t.beaconCfg, batchSignatureVerifier)
	// mock global functions
	t.mockFuncs = &mockFuncs{
		ctrl: t.gomockCtrl,
	}
	computeSigningRoot = t.mockFuncs.ComputeSigningRoot
	blsVerify = t.mockFuncs.BlsVerify
	blsVerifyMultipleSignatures = t.mockFuncs.BlsVerifyMultipleSignatures
}

func (t *blsToExecutionChangeTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func (t *blsToExecutionChangeTestSuite) TestProcessMessage() {
	mockMsg := &SignedBLSToExecutionChangeForGossip{
		SignedBLSToExecutionChange: &cltypes.SignedBLSToExecutionChange{
			Message: &cltypes.BLSToExecutionChange{
				ValidatorIndex: 1,
				From:           common.Bytes48{1, 2, 3, 4, 5, 6},
				To:             common.Address{3, 2, 1},
			},
			Signature: [96]byte{1, 2, 3},
		},
		ImmediateVerification: true,
	}

	tests := []struct {
		name        string
		mock        func()
		msg         *SignedBLSToExecutionChangeForGossip
		wantErr     bool
		specificErr error
	}{
		{
			name: "signature already exists in pool",
			mock: func() {
				t.operationsPool.BLSToExecutionChangesPool.Insert(mockMsg.SignedBLSToExecutionChange.Signature, mockMsg.SignedBLSToExecutionChange)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "version is less than CapellaVersion",
			mock: func() {
				_, st, _ := tests.GetBellatrixRandom()
				t.syncedData.OnHeadState(st)
			},
			msg:         mockMsg,
			wantErr:     true,
			specificErr: ErrIgnore,
		},
		{
			name: "unable to retrieve validator",
			mock: func() {
				mockStateReader := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				mockStateReader.EXPECT().Version().Return(clparams.CapellaVersion).AnyTimes()
				mockStateReader.EXPECT().ValidatorForValidatorIndex(int(mockMsg.SignedBLSToExecutionChange.Message.ValidatorIndex)).Return(nil, errors.New("not found")).AnyTimes()
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "invalid withdrawal credentials prefix",
			mock: func() {
				mockStateReader := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				mockValidator := solid.NewValidator()
				mockValidator.SetWithdrawalCredentials([32]byte{1, 1, 1}) // should be equal to BLS_WITHDRAWAL_PREFIX
				mockStateReader.EXPECT().Version().Return(clparams.CapellaVersion).AnyTimes()
				mockStateReader.EXPECT().ValidatorForValidatorIndex(int(mockMsg.SignedBLSToExecutionChange.Message.ValidatorIndex)).Return(mockValidator, nil).AnyTimes()
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "hashed from is not equal to withdrawal credentials",
			mock: func() {
				mockStateReader := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				mockValidator := solid.NewValidator()
				mockValidator.SetWithdrawalCredentials([32]byte{0}) // first byte is equal to BLS_WITHDRAWAL_PREFIX
				mockStateReader.EXPECT().Version().Return(clparams.CapellaVersion).AnyTimes()
				mockStateReader.EXPECT().ValidatorForValidatorIndex(int(mockMsg.SignedBLSToExecutionChange.Message.ValidatorIndex)).Return(mockValidator, nil).AnyTimes()
			},
			msg:     mockMsg,
			wantErr: true,
		},
		// {
		// 	name: "invalid bls signature",
		// 	mock: func() {
		// 		mockStateReader := mockState.NewMockBeaconStateReader(t.gomockCtrl)
		// 		mockValidator := solid.NewValidator()
		// 		hashedFrom := utils.Sha256(mockMsg.SignedBLSToExecutionChange.Message.From[:])
		// 		wc := [32]byte{0}
		// 		copy(wc[1:], hashedFrom[1:])
		// 		mockValidator.SetWithdrawalCredentials(wc)
		// 		mockStateReader.EXPECT().Version().Return(clparams.CapellaVersion).AnyTimes()
		// 		mockStateReader.EXPECT().ValidatorForValidatorIndex(int(mockMsg.SignedBLSToExecutionChange.Message.ValidatorIndex)).Return(mockValidator, nil).AnyTimes()
		// 		mockStateReader.EXPECT().GenesisValidatorsRoot().Return([32]byte{}).AnyTimes()
		// 		// bls verify
		// 		t.gomockCtrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.SignedBLSToExecutionChange.Message, gomock.Any()).Return([32]byte{}, nil).AnyTimes()
		// 		t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(2)
		// 	},
		// 	msg:         mockMsg,
		// 	specificErr: ErrInvalidBlsSignature,
		// 	wantErr:     true,
		// },
		// {
		// 	name: "pass",
		// 	mock: func() {
		// 		mockStateReader := mockState.NewMockBeaconStateReader(t.gomockCtrl)
		// 		mockValidator := solid.NewValidator()
		// 		hashedFrom := utils.Sha256(mockMsg.SignedBLSToExecutionChange.Message.From[:])
		// 		wc := [32]byte{0}
		// 		copy(wc[1:], hashedFrom[1:])
		// 		mockValidator.SetWithdrawalCredentials(wc)
		// 		mockStateReader.EXPECT().Version().Return(clparams.CapellaVersion).AnyTimes()
		// 		mockStateReader.EXPECT().ValidatorForValidatorIndex(int(mockMsg.SignedBLSToExecutionChange.Message.ValidatorIndex)).Return(mockValidator, nil).AnyTimes()
		// 		mockStateReader.EXPECT().GenesisValidatorsRoot().Return([32]byte{}).AnyTimes()
		// 		// bls verify
		// 		t.gomockCtrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.SignedBLSToExecutionChange.Message, gomock.Any()).Return([32]byte{}, nil).AnyTimes()
		// 		// update withdrawal credentials
		// 		mockNewWc := common.Hash{byte(t.beaconCfg.ETH1AddressWithdrawalPrefixByte)}
		// 		copy(mockNewWc[1:], make([]byte, 11))
		// 		copy(mockNewWc[12:], mockMsg.SignedBLSToExecutionChange.Message.To[:])
		// 		t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerifyMultipleSignatures", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
		// 	},
		// 	msg: mockMsg,
		// 	// specificErr: ErrInvalidBlsSignature,
		// 	// wantErr:     true,
		// },
	}

	for _, tt := range tests {
		log.Printf("Running test case: %s", tt.name)
		t.SetupTest()
		tt.mock()
		err := t.service.ProcessMessage(context.Background(), nil, tt.msg)
		time.Sleep(10 * time.Millisecond)
		if tt.wantErr {
			t.Require().Error(err)
			fmt.Printf("Error: %v\n", err)
			if tt.specificErr != nil {
				t.Require().Equal(tt.specificErr, err)
			}
		} else {
			t.Require().NoError(err)
		}
		t.gomockCtrl.Satisfied()
	}
}

func TestBlsToExecutionChangeTestSuite(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	suite.Run(t, new(blsToExecutionChangeTestSuite))
}
