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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type proposerSlashingTestSuite struct {
	suite.Suite
	gomockCtrl              *gomock.Controller
	operationsPool          *pool.OperationsPool
	syncedData              synced_data.SyncedData
	beaconCfg               *clparams.BeaconChainConfig
	ethClock                *eth_clock.MockEthereumClock
	proposerSlashingService *proposerSlashingService
	mockFuncs               *mockFuncs
}

func (t *proposerSlashingTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.operationsPool = &pool.OperationsPool{
		ProposerSlashingsPool: pool.NewOperationPool[common.Bytes96, *cltypes.ProposerSlashing](10, "proposerSlashingsPool"),
	}
	_, st, _ := tests.GetBellatrixRandom()
	t.syncedData = synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	t.syncedData.OnHeadState(st)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconCfg = &clparams.BeaconChainConfig{
		SlotsPerEpoch: 2,
	}
	emitters := beaconevents.NewEventEmitter()
	t.proposerSlashingService = NewProposerSlashingService(*t.operationsPool, t.syncedData, t.beaconCfg, t.ethClock, emitters)
	// mock global functions
	t.mockFuncs = &mockFuncs{ctrl: t.gomockCtrl}
	computeSigningRoot = t.mockFuncs.ComputeSigningRoot
	blsVerify = t.mockFuncs.BlsVerify
}

func (t *proposerSlashingTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func (t *proposerSlashingTestSuite) TestProcessMessage() {
	mockProposerIndex := uint64(123)
	mockMsg := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: mockProposerIndex,
				Root:          common.Hash{1},
			},
			Signature: common.Bytes96{1, 2, 3},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: mockProposerIndex,
				Root:          common.Hash{2},
			},
			Signature: common.Bytes96{4, 5, 6},
		},
	}
	mockMsg2 := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: 9191991,
				Root:          common.Hash{1},
			},
			Signature: common.Bytes96{1, 2, 3},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: 9191991,
				Root:          common.Hash{2},
			},
			Signature: common.Bytes96{4, 5, 6},
		},
	}
	tests := []struct {
		name    string
		mock    func()
		msg     *cltypes.ProposerSlashing
		wantErr bool
		err     error
	}{
		{
			name: "ignore proposer slashing",
			mock: func() {
				t.proposerSlashingService.cache.Add(mockProposerIndex, struct{}{})
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "ignore proposer slashing in pool",
			mock: func() {
				t.operationsPool.ProposerSlashingsPool.Insert(pool.ComputeKeyForProposerSlashing(mockMsg), mockMsg)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "non-matching slots",
			mock: func() {},
			msg: &cltypes.ProposerSlashing{
				Header1: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: mockProposerIndex,
					},
				},
				Header2: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          2,
						ProposerIndex: mockProposerIndex,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "non-matching proposer indices",
			mock: func() {},
			msg: &cltypes.ProposerSlashing{
				Header1: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: mockProposerIndex,
					},
				},
				Header2: &cltypes.SignedBeaconBlockHeader{
					Header: &cltypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: mockProposerIndex + 1,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty head state",
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
				_, st, _ := tests.GetBellatrixRandom()
				t.syncedData.OnHeadState(st)
			},
			msg:     mockMsg2,
			wantErr: true,
		},
		{
			name: "proposer is not slashable",
			mock: func() {
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
				_, st, _ := tests.GetBellatrixRandom()
				st.ValidatorSet().Set(int(mockProposerIndex), mockValidator)
				t.syncedData.OnHeadState(st)

				t.ethClock.EXPECT().GetCurrentEpoch().Return(uint64(1)).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "pass",
			mock: func() {
				// mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				// mockValidator := solid.NewValidatorFromParameters(
				// 	[48]byte{},
				// 	[32]byte{},
				// 	0,
				// 	false,
				// 	0,
				// 	0,
				// 	2,
				// 	2,
				// )
				t.ethClock.EXPECT().GetCurrentEpoch().Return(uint64(1)).Times(1)

				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.Header1, gomock.Any()).Return([32]byte{}, nil).Times(1)
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.Header2, gomock.Any()).Return([32]byte{}, nil).Times(1)
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(2)
			},
			msg:     mockMsg,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		log.Printf("Running test case: %s", tt.name)
		t.SetupTest()
		tt.mock()
		err := t.proposerSlashingService.ProcessMessage(context.Background(), nil, tt.msg)
		if tt.wantErr {
			t.Error(err)
			if tt.err != nil {
				t.Equal(tt.err, err)
			}
		} else {
			t.NoError(err)
		}
		t.gomockCtrl.Satisfied()
	}
}

func TestProposerSlashing(t *testing.T) {
	suite.Run(t, new(proposerSlashingTestSuite))
}
