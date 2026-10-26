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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type apiEnvelopeRebuilder struct {
	envelope *cltypes.SignedExecutionPayloadEnvelope
}

type failingBeginRoDB struct {
	kv.RwDB
}

func (failingBeginRoDB) BeginRo(context.Context) (kv.Tx, error) {
	return nil, errors.New("index database unavailable")
}

func (r *apiEnvelopeRebuilder) Rebuild(context.Context, kv.Tx, uint64, common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return r.envelope, nil
}

func TestGetExecutionPayloadEnvelopeRebuildsPrunedBlock(t *testing.T) {
	handler, root, envelope, forkchoiceStore := newHistoricalEnvelopeHandler(t, false)
	handler.envelopeRebuilder = &apiEnvelopeRebuilder{envelope: envelope}
	forkchoiceStore.IsRootOptimisticVal = true

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.NoError(t, err)
	require.Same(t, envelope, response.Data)
	require.Equal(t, clparams.GloasVersion, *response.Version)
	require.True(t, *response.ExecutionOptimistic)
	require.True(t, *response.Finalized)
}

func TestGetExecutionPayloadEnvelopeUsesFinalizedCheckpointStart(t *testing.T) {
	for _, test := range []struct {
		name          string
		slot          uint64
		wantFinalized bool
	}{
		{name: "at checkpoint start", slot: 64, wantFinalized: true},
		{name: "after checkpoint start", slot: 69, wantFinalized: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, root, envelope, _ := newHistoricalEnvelopeHandlerAtSlot(t, false, test.slot)
			handler.envelopeRebuilder = &apiEnvelopeRebuilder{envelope: envelope}

			response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
			require.NoError(t, err)
			require.Equal(t, test.wantFinalized, *response.Finalized)
		})
	}
}

func TestGetExecutionPayloadEnvelopeReadsAnchorBlockFromDatabase(t *testing.T) {
	handler, root, envelope, forkchoiceStore := newHistoricalEnvelopeHandler(t, true)
	forkchoiceStore.SetEnvelope(root, envelope)

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.NoError(t, err)
	require.Same(t, envelope, response.Data)
	require.Equal(t, clparams.GloasVersion, *response.Version)
}

func TestGetExecutionPayloadEnvelopeForkGraphPathDoesNotReadIndices(t *testing.T) {
	handler, root, envelope, forkchoiceStore := newHistoricalEnvelopeHandler(t, false)
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	block.Block.Slot = 64
	rebuiltRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, common.Hash(rebuiltRoot))
	forkchoiceStore.Blocks[root] = block
	forkchoiceStore.SetEnvelope(root, envelope)
	handler.indiciesDB = failingBeginRoDB{RwDB: handler.indiciesDB}

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.NoError(t, err)
	require.Same(t, envelope, response.Data)
}

func TestGetExecutionPayloadEnvelopeReturnsNotFoundWhenUnavailable(t *testing.T) {
	handler, root, _, _ := newHistoricalEnvelopeHandler(t, false)
	handler.envelopeRebuilder = &apiEnvelopeRebuilder{}

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.Nil(t, response)
	endpointErr := beaconhttp.WrapEndpointError(err)
	require.Equal(t, http.StatusNotFound, endpointErr.Code)
}

func newHistoricalEnvelopeHandler(t *testing.T, anchor bool) (*ApiHandler, common.Hash, *cltypes.SignedExecutionPayloadEnvelope, *mock_services.ForkChoiceStorageMock) {
	return newHistoricalEnvelopeHandlerAtSlot(t, anchor, 64)
}

func newHistoricalEnvelopeHandlerAtSlot(t *testing.T, anchor bool, slot uint64) (*ApiHandler, common.Hash, *cltypes.SignedExecutionPayloadEnvelope, *mock_services.ForkChoiceStorageMock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	reader := tests.NewMockBlockReader()
	reader.U[block.Block.Slot] = block
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, block.Block.Slot, root)
	}))
	forkchoiceStore := mock_services.NewForkChoiceStorageMock(t)
	forkchoiceStore.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2}
	forkchoiceStore.FinalizedSlotVal = 95
	if anchor {
		forkchoiceStore.AnchorRootVal = root
	}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = root
	return &ApiHandler{
		blockReader:     reader,
		indiciesDB:      db,
		beaconChainCfg:  &cfg,
		forkchoiceStore: forkchoiceStore,
	}, root, envelope, forkchoiceStore
}

func envelopeRequest(t *testing.T, root common.Hash) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/execution_payload_envelope/"+root.Hex(), http.NoBody)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("block_id", fmt.Sprintf("%#x", root))
	return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, routeContext))
}
