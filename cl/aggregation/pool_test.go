package aggregation

import (
	"context"
	"log"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

var (
	// mock attestations with attestation data 1
	attData1 = solid.NewAttestionDataFromParameters(1, 1, [32]byte{0, 4, 2, 6},
		solid.NewCheckpointFromParameters([32]byte{0}, 4),
		solid.NewCheckpointFromParameters([32]byte{0}, 4))
	attData1Root, _ = attData1.HashSSZ()

	att1_1 = solid.NewAttestionFromParameters(
		[]byte{0b00000001, 0, 0, 0},
		attData1,
		[96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
	)
	att1_2 = solid.NewAttestionFromParameters(
		[]byte{0b00001011, 0, 0, 0},
		attData1,
		[96]byte{'d', 'e', 'f', 'g', 'h', 'i'},
	)
	att1_3 = solid.NewAttestionFromParameters(
		[]byte{0b00000100, 0b00000011, 0, 0},
		attData1,
		[96]byte{'g', 'h', 'i', 'j', 'k', 'l'},
	)
	att1_4 = solid.NewAttestionFromParameters(
		[]byte{0b00110000, 0, 0, 0b00000011},
		attData1,
		[96]byte{'m', 'n', 'o', 'p', 'q', 'r'},
	)
	// mock attestations with attestation data 2
	attData2 = solid.NewAttestionDataFromParameters(3, 1, [32]byte{5, 5, 6, 6},
		solid.NewCheckpointFromParameters([32]byte{0}, 4),
		solid.NewCheckpointFromParameters([32]byte{0}, 4))
	att2_1 = solid.NewAttestionFromParameters(
		[]byte{0b00000001, 0, 0, 0},
		attData2,
		[96]byte{'t', 'e', 's', 't', 'i', 'n'},
	)

	mockAggrResult = [96]byte{'m', 'o', 'c', 'k'}
)

type mockFuncs struct {
	ctrl *gomock.Controller
}

func (m *mockFuncs) BlsAggregate(sigs [][]byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlsAggregate", sigs)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

type PoolTestSuite struct {
	suite.Suite
	gomockCtrl *gomock.Controller
	mockFuncs  *mockFuncs
}

func (t *PoolTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockFuncs = &mockFuncs{ctrl: t.gomockCtrl}
	blsAggregate = t.mockFuncs.BlsAggregate
}

func (t *PoolTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func (t *PoolTestSuite) TestAddAttestation() {
	testcases := []struct {
		name     string
		mock     func()
		atts     []*solid.Attestation
		hashRoot [32]byte
		expect   *solid.Attestation
	}{
		{
			name: "simple, different hashRoot",
			atts: []*solid.Attestation{
				att1_1,
				att2_1,
			},
			hashRoot: attData1Root,
			expect:   att1_1,
		},
		{
			name: "att1_2 is a super set of att1_1. skip att1_1",
			atts: []*solid.Attestation{
				att1_2,
				att1_1,
				att2_1, // none of its business
			},
			hashRoot: attData1Root,
			expect:   att1_2,
		},
		{
			name: "merge att1_2, att1_3, att1_4",
			mock: func() {
				t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsAggregate", gomock.Any()).Return(mockAggrResult[:], nil).Times(3)
			},
			atts: []*solid.Attestation{
				att1_1, // no merge
				att1_2, // no merge becasue overlapped with att1_1
				att1_3, // merge with att1_2
				att1_4, // merge with att1_2 and att1_3
			},
			hashRoot: attData1Root,
			expect: solid.NewAttestionFromParameters(
				[]byte{0b00111111, 0b00000011, 0, 0b00000011}, // merge of att1_2, att1_3 and att1_4
				attData1,
				mockAggrResult),
		},
	}

	for _, tc := range testcases {
		log.Printf("test case: %s", tc.name)
		t.SetupTest()
		if tc.mock != nil {
			tc.mock()
		}
		pool := NewAggregationPool(context.Background(), nil, nil, nil)
		for _, att := range tc.atts {
			pool.AddAttestation(att)
		}
		att := pool.GetAggregatationByRoot(tc.hashRoot)
		t.Equal(tc.expect, att, tc.name)
		t.gomockCtrl.Satisfied()
	}
}

func TestPool(t *testing.T) {
	suite.Run(t, new(PoolTestSuite))
}
