// Copyright 2026 The Erigon Authors
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
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	builder_mock "github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	clservices "github.com/erigontech/erigon/cl/phase1/network/services"
	network_services_mock "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func producedBuilderIndex(t *testing.T, handler *ApiHandler, fixture gloasBidSelectionFixture, body string) uint64 {
	t.Helper()
	response, err := handler.PostEthV4ValidatorBlock(httptest.NewRecorder(), postV4BlockRequest(t.Context(), fixture.block.Slot, false, "", body))
	require.NoError(t, err)
	block, ok := response.Data.(*cltypes.BeaconBlock)
	require.True(t, ok)
	return block.Body.SignedExecutionPayloadBid.Message.BuilderIndex
}

func TestPostEthV4ValidatorBlockBuilderBoostFactorDoesNotOverflow(t *testing.T) {
	for _, tc := range []struct {
		name          string
		boostFactor   uint64
		wantExternal  bool
		localValueWei *big.Int
	}{
		{"default boost keeps the larger local value", 100, false, big.NewInt(1_000_000_000_000)},
		{"maximum boost lets the external bid win", math.MaxUint64, true, big.NewInt(1_000_000_000_000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{slotOffset: 1})
			handler, _ := newGloasProductionHandler(t, &fixture, 1, tc.localValueWei)
			handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)

			index := producedBuilderIndex(t, handler, fixture, v4RequestBody(0, tc.boostFactor))

			if tc.wantExternal {
				require.Equal(t, fixture.externalBid.Message.BuilderIndex, index)
			} else {
				require.Equal(t, uint64(clparams.BuilderIndexSelfBuild), index)
			}
		})
	}
}

func TestPostEthV4ValidatorBlockMinBidAppliesToP2PBid(t *testing.T) {
	for _, tc := range []struct {
		name         string
		minBidOffset int64
		wantExternal bool
	}{
		{"below the bid value", -1, true},
		{"equal to the bid value", 0, true},
		{"above the bid value", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{slotOffset: 1})
			handler, _ := newGloasProductionHandler(t, &fixture, 1, big.NewInt(1))
			handler.epbsPool.StoreHighestBid(fixture.bidKey, fixture.externalBid)

			index := producedBuilderIndex(t, handler, fixture, v4RequestBody(uint64(int64(fixture.externalBid.Message.Value)+tc.minBidOffset), 100))

			if tc.wantExternal {
				require.Equal(t, fixture.externalBid.Message.BuilderIndex, index)
			} else {
				require.Equal(t, uint64(clparams.BuilderIndexSelfBuild), index)
			}
		})
	}
}

func TestPostEthV4ValidatorBlockSetsBuilderURLWhenBuilderAPIBidWins(t *testing.T) {
	const builderURL = "https://builder.example"
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{
		slotOffset: 1,
		mutateBid: func(bid *cltypes.ExecutionPayloadBid) {
			bid.FeeRecipient = common.Address{}
		},
	})
	handler, forkchoiceStore := newGloasProductionHandler(t, &fixture, 1, big.NewInt(1))
	forkchoiceStore.ExecutionPayloadGasLimitMap[fixture.bidKey.ParentBlockHash] = fixture.externalBid.Message.GasLimit
	builderClient := builder_mock.NewMockBuilderClient(gomock.NewController(t))
	builderClient.EXPECT().RequestExecutionPayloadBid(
		gomock.Any(), builderURL, fixture.block.Slot, fixture.bidKey.ParentBlockHash, fixture.bidKey.ParentBlockRoot, gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(fixture.externalBid, nil)
	handler.builderClient = builderClient
	handler.executionPayloadBidService = acceptingExecutionPayloadBidService{}
	entry, err := json.Marshal(&cltypes.BuilderEntry{
		URL: builderURL,
		Auth: &cltypes.SignedBuilderRequestAuth{Message: &cltypes.BuilderRequestAuth{
			Data: []byte(builderURL), Slot: fixture.block.Slot,
		}},
		BuilderPubkeys:     []common.Bytes48{},
		BuilderBoostFactor: 100,
	})
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	response, err := handler.PostEthV4ValidatorBlock(recorder, postV4BlockRequest(t.Context(), fixture.block.Slot, false, "", v4RequestBody(0, 100, string(entry))))

	require.NoError(t, err)
	block, ok := response.Data.(*cltypes.BeaconBlock)
	require.True(t, ok)
	require.Equal(t, fixture.externalBid.Message.BuilderIndex, block.Body.SignedExecutionPayloadBid.Message.BuilderIndex)
	require.Equal(t, builderURL, recorder.Header().Get("Eth-Builder-Url"))
}

func TestPostEthV4ValidatorBlockWithoutPayloadServesEnvelopeAfterPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	fixture := newGloasBidSelectionFixture(t, gloasBidSelectionOptions{slotOffset: 1})
	handler, forkchoiceStore := newGloasProductionHandler(t, &fixture, 1, big.NewInt(1))
	slot := fixture.block.Slot
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	clock.EXPECT().GetSlotTime(slot).Return(time.Now()).AnyTimes()
	handler.ethClock = clock

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, postV4BlockRequest(t.Context(), slot, false, "", v4RequestBody(0, 100)))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "false", recorder.Header().Get("Eth-Execution-Payload-Included"))
	var produced struct {
		Data json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &produced))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(produced.Data, &fields))
	require.Contains(t, fields, "slot", "include_payload=false returns a bare BeaconBlock")
	require.NotContains(t, fields, "execution_payload_envelope")
	require.NotContains(t, recorder.Body.String(), `"execution_payload_envelope"`)

	signedBlock := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	require.NoError(t, json.Unmarshal(produced.Data, signedBlock.Block))
	blockRoot, err := signedBlock.Block.HashSSZ()
	require.NoError(t, err)
	root := common.Hash(blockRoot)

	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.gossipManager = gossipManager
	blockService := network_services_mock.NewMockBlockService(ctrl)
	blockService.EXPECT().ValidateGossip(gomock.Any(), gomock.Any()).Return(nil)
	blockService.EXPECT().CommitGossipReservation(gomock.Any())
	blockService.EXPECT().SchedulePublishedBlockForLaterProcessing(gomock.Any(), gomock.Any()).Return(clservices.PublishedBlockJob(completedPublishedBlockJob{}))
	handler.blockService = blockService
	signedJSON := fmt.Sprintf(`{"message":%s,"signature":%q}`, produced.Data, common.Bytes96{}.String())
	publish := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", strings.NewReader(signedJSON))
	publish.Header.Set("Content-Type", "application/json")
	publish.Header.Set("Eth-Consensus-Version", "gloas")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, publish)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	forkchoiceStore.HeadVal = root
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("/eth/v1/validator/execution_payload_envelopes/%d/%s", slot, root), http.NoBody))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), fmt.Sprintf(`"beacon_block_root":%q`, root.String()))
}
