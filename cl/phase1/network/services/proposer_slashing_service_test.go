package services

import (
	"context"
	"errors"
	"log"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	mockSync "github.com/ledgerwatch/erigon/cl/beacon/synced_data/mock_services"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	mockState "github.com/ledgerwatch/erigon/cl/phase1/core/state/mock_services"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type mockFuncs struct {
	ctrl *gomock.Controller
}

func (m *mockFuncs) ComputeSigningRoot(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComputeSigningRoot", obj, domain)
	ret0, _ := ret[0].([32]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (m *mockFuncs) BlsVerify(pubkey, message, signature []byte) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlsVerify", pubkey, message, signature)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

type proposerSlashingTestSuite struct {
	suite.Suite
	gomockCtrl              *gomock.Controller
	operationsPool          *pool.OperationsPool
	syncedData              *mockSync.MockSyncedData
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
	t.syncedData = mockSync.NewMockSyncedData(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconCfg = &clparams.BeaconChainConfig{
		SlotsPerEpoch: 2,
	}
	t.proposerSlashingService = NewProposerSlashingService(*t.operationsPool, t.syncedData, t.beaconCfg, t.ethClock)

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
				t.syncedData.EXPECT().HeadStateReader().Return(nil).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
		},
		{
			name: "validator not found",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockProposerIndex)).Return(nil, errors.New("not found")).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "proposer is not slashable",
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
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockProposerIndex)).Return(mockValidator, nil).Times(1)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
				t.ethClock.EXPECT().GetCurrentEpoch().Return(uint64(1)).Times(1)
			},
			msg:     mockMsg,
			wantErr: true,
		},
		{
			name: "pass",
			mock: func() {
				mockState := mockState.NewMockBeaconStateReader(t.gomockCtrl)
				mockValidator := solid.NewValidatorFromParameters(
					[48]byte{},
					[32]byte{},
					0,
					false,
					0,
					0,
					2,
					2,
				)
				t.syncedData.EXPECT().HeadStateReader().Return(mockState).Times(1)
				mockState.EXPECT().ValidatorForValidatorIndex(int(mockProposerIndex)).Return(mockValidator, nil).Times(1)
				t.ethClock.EXPECT().GetCurrentEpoch().Return(uint64(1)).Times(1)

				mockState.EXPECT().GetDomain(t.beaconCfg.DomainBeaconProposer, gomock.Any()).Return([]byte{}, nil).Times(2)
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.Header1, []byte{}).Return([32]byte{}, nil).Times(1)
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "ComputeSigningRoot", mockMsg.Header2, []byte{}).Return([32]byte{}, nil).Times(1)
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
			t.Assert().Error(err)
			if tt.err != nil {
				t.Assert().Equal(tt.err, err)
			}
		} else {
			t.Assert().NoError(err)
		}
		t.gomockCtrl.Satisfied()
	}
}

func TestProposerSlashing(t *testing.T) {
	suite.Run(t, new(proposerSlashingTestSuite))
}
