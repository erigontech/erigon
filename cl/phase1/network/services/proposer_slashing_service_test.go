package services

import (
	"context"
	"log"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	mockSync "github.com/ledgerwatch/erigon/cl/beacon/synced_data/mock_services"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type proposerSlashingTestSuite struct {
	suite.Suite
	gomockCtrl              *gomock.Controller
	operationsPool          *pool.OperationsPool
	syncedData              *mockSync.MockSyncedData
	beaconCfg               *clparams.BeaconChainConfig
	ethClock                *eth_clock.MockEthereumClock
	proposerSlashingService *proposerSlashingService
}

func (t *proposerSlashingTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.operationsPool = &pool.OperationsPool{
		ProposerSlashingsPool: pool.NewOperationPool[common.Bytes96, *cltypes.ProposerSlashing](10, "proposerSlashingsPool"),
	}
	t.syncedData = mockSync.NewMockSyncedData(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconCfg = &clparams.BeaconChainConfig{}
	t.proposerSlashingService = NewProposerSlashingService(*t.operationsPool, t.syncedData, t.beaconCfg, t.ethClock)
}

func (t *proposerSlashingTestSuite) TearDownTest() {
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
				t.syncedData.EXPECT().HeadStateReader().Return(nil)
			},
			msg:     mockMsg,
			wantErr: true,
			err:     ErrIgnore,
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
	}
}

func TestProposerSlashing(t *testing.T) {
	suite.Run(t, new(proposerSlashingTestSuite))
}
