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

	"github.com/erigontech/erigon-lib/types/ssz"
	mockState "github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	mockSync "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type voluntaryExitTestSuite struct {
	suite.Suite
	gomockCtrl           *gomock.Controller
	operationsPool       *pool.OperationsPool
	emitters             *beaconevents.EventEmitter
	syncedData           *mockSync.MockSyncedData
	ethClock             *eth_clock.MockEthereumClock
	beaconCfg            *clparams.BeaconChainConfig
	voluntaryExitService VoluntaryExitService

	mockFuncs *mockFuncs
}

func (t *voluntaryExitTestSuite) SetupTest() {
	computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
		return [32]byte{}, nil
	}
	t.gomockCtrl = gomock.NewController(t.T())
	t.emitters = beaconevents.NewEventEmitter()
	t.operationsPool = &pool.OperationsPool{
		VoluntaryExitsPool: pool.NewOperationPool[uint64, *cltypes.SignedVoluntaryExit](10, "voluntaryExitsPool"),
	}
	t.syncedData = mockSync.NewMockSyncedData(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconCfg = &clparams.BeaconChainConfig{}
	t.voluntaryExitService = NewVoluntaryExitService(*t.operationsPool, t.emitters, t.syncedData, t.beaconCfg, t.ethClock)
	// mock global functions
	t.mockFuncs = &mockFuncs{
		ctrl: t.gomockCtrl,
	}
	blsVerify = t.mockFuncs.BlsVerify
}

func (t *voluntaryExitTestSuite) TearDownTest() {
}

func (t *voluntaryExitTestSuite) TestProcessMessage() {
	curEpoch := uint64(100)
	mockValidatorIndex := uint64(10)
	mockMsg := &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{
			Epoch:          1,
			ValidatorIndex: mockValidatorIndex,
		},
		Signature: [96]byte{},
	}

	tests := []struct {
		name    string
		mock    func()
		msg     *cltypes.SignedVoluntaryExit
		wantErr bool
		err     error
	}{
		{
			name: "validator already in pool",
			mock: func() {
				t.operationsPool.VoluntaryExitsPool.Insert(mockValidatorIndex, mockMsg)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "state is nil",
			mock: func() {
				t.syncedData.EXPECT().HeadStateReader().Return(nil)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "validator not found",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockValidatorIndex)).Return(nil, errors.New("not found")).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "validator is not active",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
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
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockValidatorIndex)).Return(mockValidator, nil).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
				t.ethClock.EXPECT().GetCurrentEpoch().Return(curEpoch).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "validator has been initialized",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
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
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockValidatorIndex)).Return(mockValidator, nil).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
				t.ethClock.EXPECT().GetCurrentEpoch().Return(curEpoch).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "bls verify failed",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
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
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockValidatorIndex)).Return(mockValidator, nil).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
				t.ethClock.EXPECT().GetCurrentEpoch().Return(curEpoch).Times(1)
				t.beaconCfg.FarFutureEpoch = mockValidator.ExitEpoch()
				mockState.EXPECT().Version().Return(clparams.AltairVersion).Times(1)
				mockState.EXPECT().GetDomain(t.beaconCfg.DomainVoluntaryExit, mockMsg.VoluntaryExit.Epoch).Return([]byte{}, nil).Times(1)
				computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}
				t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "success",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
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
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockValidatorIndex)).Return(mockValidator, nil).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
				t.ethClock.EXPECT().GetCurrentEpoch().Return(curEpoch).Times(1)
				t.beaconCfg.FarFutureEpoch = mockValidator.ExitEpoch()
				mockState.EXPECT().Version().Return(clparams.AltairVersion).Times(1)
				mockState.EXPECT().GetDomain(t.beaconCfg.DomainVoluntaryExit, mockMsg.VoluntaryExit.Epoch).Return([]byte{}, nil).Times(1)
				computeSigningRoot = func(_ ssz.HashableSSZ, domain []byte) ([32]byte, error) {
					return [32]byte{}, nil
				}
				t.gomockCtrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
			},
			msg:     mockMsg,
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

func TestVoluntaryExit(t *testing.T) {
	suite.Run(t, new(voluntaryExitTestSuite))
}
