package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	mockaggregation "github.com/ledgerwatch/erigon/cl/aggregation/mock_services"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type validatorTestSuite struct {
	suite.Suite
	apiHandler   *ApiHandler
	mockAggrPool *mockaggregation.MockAggregationPool
	gomockCtrl   *gomock.Controller
}

func (t *validatorTestSuite) SetupTest() {
	gomockCtrl := gomock.NewController(t.T())
	t.mockAggrPool = mockaggregation.NewMockAggregationPool(gomockCtrl)
	t.apiHandler = NewApiHandler(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		pool.OperationsPool{},
		nil,
		nil,
		nil,
		nil,
		"0",
		&beacon_router_configuration.RouterConfiguration{Validator: true},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		t.mockAggrPool,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	t.gomockCtrl = gomockCtrl
}

func (t *validatorTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func (t *validatorTestSuite) TestGetEthV1ValidatorAggregateAttestation() {
	mockDataRoot := libcommon.HexToHash("0x123").String()

	tests := []struct {
		name    string
		method  string
		url     string
		mock    func()
		expCode int
		expBody any
	}{
		{
			name:    "empty attestation data root",
			method:  http.MethodGet,
			url:     "/eth/v1/validator/aggregate_attestation?slot=1",
			mock:    func() {},
			expCode: http.StatusBadRequest,
			expBody: map[string]any{
				"code":    float64(http.StatusBadRequest),
				"message": "attestation_data_root is required",
			},
		},
		{
			name:   "slot mismatch",
			method: http.MethodGet,
			url:    "/eth/v1/validator/aggregate_attestation?attestation_data_root=" + mockDataRoot + "&slot=1",
			mock: func() {
				ret := *solid.NewAttestionFromParameters(
					[]byte{},
					solid.NewAttestionDataFromParameters(
						123456,
						1,
						libcommon.HexToHash(mockDataRoot),
						solid.NewCheckpointFromParameters(libcommon.Hash{}, 1),
						solid.NewCheckpointFromParameters(libcommon.Hash{}, 1),
					),
					[96]byte{},
				)
				t.mockAggrPool.EXPECT().GetAggregatationByRoot(libcommon.HexToHash(mockDataRoot)).Return(&ret).Times(1)
			},
			expCode: http.StatusBadRequest,
			expBody: map[string]any{
				"code":    float64(http.StatusBadRequest),
				"message": "attestation slot mismatch",
			},
		},
		{
			name:   "pass",
			method: http.MethodGet,
			url:    "/eth/v1/validator/aggregate_attestation?attestation_data_root=" + mockDataRoot + "&slot=1",
			mock: func() {
				ret := *solid.NewAttestionFromParameters(
					[]byte{0b00111111, 0b00000011, 0, 0},
					solid.NewAttestionDataFromParameters(
						1,
						1,
						libcommon.HexToHash(mockDataRoot),
						solid.NewCheckpointFromParameters(libcommon.Hash{}, 1),
						solid.NewCheckpointFromParameters(libcommon.Hash{}, 1),
					),
					[96]byte{0, 1, 2, 3, 4, 5},
				)
				t.mockAggrPool.EXPECT().GetAggregatationByRoot(libcommon.HexToHash(mockDataRoot)).Return(&ret).Times(1)
			},
			expCode: http.StatusOK,
			expBody: map[string]any{
				"data": map[string]any{
					"aggregation_bits": "0x" + common.Bytes2Hex([]byte{0b00111111, 0b00000011, 0, 0}),
					"signature":        "0x" + common.Bytes2Hex([][96]byte{{0, 1, 2, 3, 4, 5}}[0][:]),
					"data": map[string]any{
						"slot":              "1",
						"index":             "1",
						"beacon_block_root": mockDataRoot,
						"source": map[string]any{
							"epoch": "1",
							"root":  libcommon.Hash{}.String(),
						},
						"target": map[string]any{
							"epoch": "1",
							"root":  libcommon.Hash{}.String(),
						},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		log.Printf("test case: %s", tc.name)
		tc.mock()
		req, err := http.NewRequest(tc.method, tc.url, nil)
		t.NoError(err)
		rr := httptest.NewRecorder()
		t.apiHandler.ServeHTTP(rr, req)
		t.Equal(tc.expCode, rr.Code)

		// check body by comparing map
		jsonResp := map[string]any{}
		err = json.Unmarshal(rr.Body.Bytes(), &jsonResp)
		t.NoError(err)
		t.Equal(tc.expBody, jsonResp)

		t.True(t.gomockCtrl.Satisfied(), "mock expectations were not met")
	}
}

func TestValidator(t *testing.T) {
	suite.Run(t, new(validatorTestSuite))
}
