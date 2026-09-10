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

package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	mockaggregation "github.com/erigontech/erigon/cl/aggregation/mock_services"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
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
		nil,
		t.mockAggrPool,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
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
	mockDataRoot := common.HexToHash("0x123").String()

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
				// ret := *solid.NewAttestionFromParameters(
				// 	[]byte{},
				// 	solid.NewAttestionDataFromParameters(
				// 		123456,
				// 		1,
				// 		common.HexToHash(mockDataRoot),
				// 		solid.NewCheckpointFromParameters(common.Hash{}, 1),
				// 		solid.NewCheckpointFromParameters(common.Hash{}, 1),
				// 	),
				// 	[96]byte{},
				// )
				ret := &solid.Attestation{
					AggregationBits: solid.NewBitList(0, 2048),
					Data: &solid.AttestationData{
						Slot:            123456,
						CommitteeIndex:  1,
						BeaconBlockRoot: common.HexToHash(mockDataRoot),
						Source:          solid.Checkpoint{Epoch: 1},
						Target:          solid.Checkpoint{Epoch: 1},
					},
				}
				t.mockAggrPool.EXPECT().GetAggregatationByRoot(common.HexToHash(mockDataRoot)).Return(ret).Times(1)
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
				// ret := *solid.NewAttestionFromParameters(
				// 	[]byte{0b00111111, 0b00000011, 0, 0},
				// 	solid.NewAttestionDataFromParameters(
				// 		1,
				// 		1,
				// 		common.HexToHash(mockDataRoot),
				// 		solid.NewCheckpointFromParameters(common.Hash{}, 1),
				// 		solid.NewCheckpointFromParameters(common.Hash{}, 1),
				// 	),
				// 	[96]byte{0, 1, 2, 3, 4, 5},
				// )
				ret := &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{0b00111111, 0b00000011, 0, 0}, 2048),
					Data: &solid.AttestationData{
						Slot:            1,
						CommitteeIndex:  1,
						BeaconBlockRoot: common.HexToHash(mockDataRoot),
						Source:          solid.Checkpoint{Epoch: 1},
						Target:          solid.Checkpoint{Epoch: 1},
					},
					Signature: [96]byte{0, 1, 2, 3, 4, 5},
				}
				t.mockAggrPool.EXPECT().GetAggregatationByRoot(common.HexToHash(mockDataRoot)).Return(ret).Times(1)
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
							"root":  common.Hash{}.String(),
						},
						"target": map[string]any{
							"epoch": "1",
							"root":  common.Hash{}.String(),
						},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		log.Printf("test case: %s", tc.name)
		tc.mock()
		req, err := http.NewRequestWithContext(t.T().Context(), tc.method, tc.url, nil)
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

func (t *validatorTestSuite) TestGetEthV2ValidatorAggregateAttestationUsesPreElectraPool() {
	clock := eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.apiHandler.ethClock = clock
	t.apiHandler.beaconChainCfg = &clparams.BeaconChainConfig{SlotsPerEpoch: 8}

	dataRoot := common.HexToHash("0x123")
	attestation := &solid.Attestation{Data: &solid.AttestationData{Slot: 1}}
	clock.EXPECT().StateVersionByEpoch(uint64(0)).Return(clparams.DenebVersion)
	t.mockAggrPool.EXPECT().GetAggregatationByRoot(dataRoot).Return(attestation)

	req := httptest.NewRequestWithContext(t.T().Context(), http.MethodGet, "/eth/v2/validator/aggregate_attestation?attestation_data_root="+dataRoot.String()+"&slot=1&committee_index=0", nil)
	response, err := t.apiHandler.GetEthV2ValidatorAggregateAttestation(httptest.NewRecorder(), req)
	t.Require().NoError(err)
	t.Require().Same(attestation, response.Data)
}

func (t *validatorTestSuite) TestGetEthV2ValidatorAggregateAttestationRejectsPreElectraCommitteeMismatch() {
	clock := eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.apiHandler.ethClock = clock
	t.apiHandler.beaconChainCfg = &clparams.BeaconChainConfig{SlotsPerEpoch: 8}

	dataRoot := common.HexToHash("0x123")
	attestation := &solid.Attestation{Data: &solid.AttestationData{Slot: 1, CommitteeIndex: 0}}
	clock.EXPECT().StateVersionByEpoch(uint64(0)).Return(clparams.DenebVersion)
	t.mockAggrPool.EXPECT().GetAggregatationByRoot(dataRoot).Return(attestation)

	req := httptest.NewRequestWithContext(t.T().Context(), http.MethodGet, "/eth/v2/validator/aggregate_attestation?attestation_data_root="+dataRoot.String()+"&slot=1&committee_index=3", nil)
	_, err := t.apiHandler.GetEthV2ValidatorAggregateAttestation(httptest.NewRecorder(), req)
	var endpointErr *beaconhttp.EndpointError
	t.Require().ErrorAs(err, &endpointErr)
	t.Require().Equal(http.StatusBadRequest, endpointErr.Code)
}

func (t *validatorTestSuite) TestGetEthV2ValidatorAggregateAttestationUsesCommitteePoolAfterElectra() {
	clock := eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.apiHandler.ethClock = clock
	t.apiHandler.beaconChainCfg = &clparams.BeaconChainConfig{SlotsPerEpoch: 8}

	dataRoot := common.HexToHash("0x123")
	attestation := &solid.Attestation{Data: &solid.AttestationData{Slot: 8}}
	clock.EXPECT().StateVersionByEpoch(uint64(1)).Return(clparams.ElectraVersion)
	t.mockAggrPool.EXPECT().GetAggregatationByRootAndCommittee(dataRoot, uint64(3)).Return(attestation)

	req := httptest.NewRequestWithContext(t.T().Context(), http.MethodGet, "/eth/v2/validator/aggregate_attestation?attestation_data_root="+dataRoot.String()+"&slot=8&committee_index=3", nil)
	response, err := t.apiHandler.GetEthV2ValidatorAggregateAttestation(httptest.NewRecorder(), req)
	t.Require().NoError(err)
	t.Require().Same(attestation, response.Data)
}

func TestValidator(t *testing.T) {
	suite.Run(t, new(validatorTestSuite))
}
