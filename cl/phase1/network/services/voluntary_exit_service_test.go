package services

import (
	"context"
	"log"
	"testing"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	mockSync "github.com/ledgerwatch/erigon/cl/beacon/synced_data/mock_services"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/stretchr/testify/suite"
	gomock "go.uber.org/mock/gomock"
)

type voluntaryExitTestSuite struct {
	suite.Suite
	gomockCtrl           *gomock.Controller
	operationsPool       *pool.OperationsPool
	emitters             *beaconevents.Emitters
	syncedData           *mockSync.MockSyncedData
	ethClock             *eth_clock.MockEthereumClock
	beaconCfg            *clparams.BeaconChainConfig
	voluntaryExitService VoluntaryExitService
}

func (t *voluntaryExitTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.emitters = beaconevents.NewEmitters()
	t.operationsPool = &pool.OperationsPool{
		VoluntaryExitPool: pool.NewOperationPool[uint64, *cltypes.SignedVoluntaryExit](10, "voluntaryExitsPool"),
	}
	t.syncedData = mockSync.NewMockSyncedData(t.gomockCtrl)
	t.ethClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.beaconCfg = &clparams.BeaconChainConfig{}
	t.voluntaryExitService = NewVoluntaryExitService(*t.operationsPool, t.emitters, t.syncedData, t.beaconCfg, t.ethClock)
}

func (t *voluntaryExitTestSuite) TearDownTest() {
}

func (t *voluntaryExitTestSuite) TestProcessMessage() {
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
				t.operationsPool.VoluntaryExitPool.Insert(mockValidatorIndex, mockMsg)
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
		err := t.voluntaryExitService.ProcessMessage(context.Background(), nil, tt.msg)
		if tt.wantErr {
			t.Require().Error(err)
			if tt.err != nil {
				t.Require().Equal(tt.err, err)
			}
		} else {
			t.Require().NoError(err)
		}
	}
}

func TestVoluntaryExit(t *testing.T) {
	suite.Run(t, new(voluntaryExitTestSuite))
}
