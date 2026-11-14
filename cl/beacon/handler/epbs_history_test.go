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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/phase1/envelope_history"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type apiEnvelopeRebuilder struct {
	envelope     *cltypes.SignedExecutionPayloadEnvelope
	rebuild      func(context.Context) error
	prepareCalls int
}

type executionBodyPrunedBlockReader struct {
	*tests.MockBlockReader
}

func (executionBodyPrunedBlockReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return nil, snapshot_format.ErrExecutionBodyNotFound
}

type failingBeginRoDB struct {
	kv.RwDB
}

type readTrackingDB struct {
	kv.RwDB
	open atomic.Int32
}

type readTrackingTx struct {
	kv.Tx
	open *atomic.Int32
}

func (failingBeginRoDB) BeginRo(context.Context) (kv.Tx, error) {
	return nil, errors.New("index database unavailable")
}

func (db *readTrackingDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	tx, err := db.RwDB.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	db.open.Add(1)
	return &readTrackingTx{Tx: tx, open: &db.open}, nil
}

func (db *readTrackingDB) View(ctx context.Context, f func(kv.Tx) error) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

func (tx *readTrackingTx) Rollback() {
	tx.Tx.Rollback()
	tx.open.Add(-1)
}

func (r *apiEnvelopeRebuilder) Prepare(context.Context, kv.Tx, uint64, common.Hash) (*envelope_history.PreparedRebuild, error) {
	r.prepareCalls++
	return new(envelope_history.PreparedRebuild), nil
}

func (r *apiEnvelopeRebuilder) Rebuild(ctx context.Context, _ *envelope_history.PreparedRebuild) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if r.rebuild != nil {
		if err := r.rebuild(ctx); err != nil {
			return nil, err
		}
	}
	return r.envelope, nil
}

func TestGetExecutionPayloadEnvelopeRebuildsPrunedBlock(t *testing.T) {
	t.Parallel()
	handler, root, envelope, forkchoiceStore := newHistoricalEnvelopeHandlerAtSlot(t, 64)
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
	t.Parallel()
	for _, test := range []struct {
		name          string
		slot          uint64
		wantFinalized bool
	}{
		{name: "at checkpoint start", slot: 64, wantFinalized: true},
		{name: "after checkpoint start", slot: 69, wantFinalized: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, root, envelope, _ := newHistoricalEnvelopeHandlerAtSlot(t, test.slot)
			handler.envelopeRebuilder = &apiEnvelopeRebuilder{envelope: envelope}

			response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
			require.NoError(t, err)
			require.Equal(t, test.wantFinalized, *response.Finalized)
		})
	}
}

func TestGetExecutionPayloadEnvelopeReadsBlockFromDatabase(t *testing.T) {
	t.Parallel()
	handler, root, envelope, forkchoiceStore := newHistoricalEnvelopeHandlerAtSlot(t, 64)
	forkchoiceStore.SetEnvelope(root, envelope)

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.NoError(t, err)
	require.Same(t, envelope, response.Data)
	require.Equal(t, clparams.GloasVersion, *response.Version)
}

func TestGetExecutionPayloadEnvelopeForkGraphPathDoesNotReadIndices(t *testing.T) {
	t.Parallel()
	handler, root, envelope, forkchoiceStore := newHistoricalEnvelopeHandlerAtSlot(t, 64)
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
	t.Parallel()
	handler, root, _, _ := newHistoricalEnvelopeHandlerAtSlot(t, 64)
	handler.envelopeRebuilder = &apiEnvelopeRebuilder{}

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.Nil(t, response)
	endpointErr := beaconhttp.WrapEndpointError(err)
	require.Equal(t, http.StatusNotFound, endpointErr.Code)
}

func TestGetExecutionPayloadEnvelopeDoesNotRebuildRejectedRoot(t *testing.T) {
	t.Parallel()
	handler, root, envelope, fc := newHistoricalEnvelopeHandlerAtSlot(t, 64)
	rebuilder := &apiEnvelopeRebuilder{envelope: envelope}
	handler.envelopeRebuilder = rebuilder
	fc.IsEnvelopeRejectedFunc = func(blockRoot common.Hash) bool { return blockRoot == root }

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	require.Nil(t, response)
	require.Equal(t, http.StatusNotFound, beaconhttp.WrapEndpointError(err).Code)
	require.Zero(t, rebuilder.prepareCalls)
}

func TestGetExecutionPayloadEnvelopeRechecksRejectionAfterLookup(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"fork graph", "history", "rebuild"} {
		t.Run(source, func(t *testing.T) {
			handler, root, envelope, fc := newHistoricalEnvelopeHandlerAtSlot(t, 64)
			rejected := false
			fc.IsEnvelopeRejectedFunc = func(blockRoot common.Hash) bool { return blockRoot == root && rejected }
			if source == "rebuild" {
				handler.envelopeRebuilder = &apiEnvelopeRebuilder{
					envelope: envelope,
					rebuild: func(context.Context) error {
						rejected = true
						return nil
					},
				}
			} else {
				fc.SetEnvelope(root, envelope)
				if source == "fork graph" {
					fc.Blocks[root] = handler.blockReader.(*tests.MockBlockReader).U[64]
				}
				fc.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
					rejected = true
					return envelope, nil
				}
			}

			response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
			require.True(t, rejected)
			require.Nil(t, response)
			require.Equal(t, http.StatusNotFound, beaconhttp.WrapEndpointError(err).Code)
		})
	}
}

func TestGetExecutionPayloadEnvelopeReadError(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		rejected bool
		wantCode int
	}{
		{name: "rejected during read", rejected: true, wantCode: http.StatusNotFound},
		{name: "unrelated read error", wantCode: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, root, envelope, fc := newHistoricalEnvelopeHandlerAtSlot(t, 64)
			rebuilder := &apiEnvelopeRebuilder{envelope: envelope}
			handler.envelopeRebuilder = rebuilder
			fc.SetEnvelope(root, envelope)
			rejected := false
			fc.IsEnvelopeRejectedFunc = func(blockRoot common.Hash) bool { return blockRoot == root && rejected }
			readErr := errors.New("injected envelope read error")
			fc.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
				rejected = test.rejected
				return nil, readErr
			}

			response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
			require.Nil(t, response)
			require.Error(t, err)
			require.Equal(t, test.wantCode, beaconhttp.WrapEndpointError(err).Code)
			require.Zero(t, rebuilder.prepareCalls)
			if !test.rejected {
				require.ErrorIs(t, err, readErr)
			}
		})
	}
}

func TestGetExecutionPayloadEnvelopeChecksGloasForkBeforeReadingBlock(t *testing.T) {
	t.Parallel()
	t.Run("pre-Gloas block with pruned execution body", func(t *testing.T) {
		handler, root, _, _ := newHistoricalEnvelopeHandlerAtSlot(t, 64)
		handler.beaconChainCfg.GloasForkEpoch = 3
		handler.blockReader = executionBodyPrunedBlockReader{MockBlockReader: handler.blockReader.(*tests.MockBlockReader)}
		rebuilder := &apiEnvelopeRebuilder{}
		handler.envelopeRebuilder = rebuilder

		response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
		require.Nil(t, response)
		require.Equal(t, http.StatusNotFound, beaconhttp.WrapEndpointError(err).Code)
		require.Zero(t, rebuilder.prepareCalls)
	})
	t.Run("first slot of the Gloas fork epoch", func(t *testing.T) {
		handler, root, envelope, _ := newHistoricalEnvelopeHandlerAtSlot(t, 64)
		handler.beaconChainCfg.GloasForkEpoch = 2
		rebuilder := &apiEnvelopeRebuilder{envelope: envelope}
		handler.envelopeRebuilder = rebuilder

		response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
		require.NoError(t, err)
		require.Same(t, envelope, response.Data)
		require.Equal(t, 1, rebuilder.prepareCalls)
	})
}

func TestGetExecutionPayloadEnvelopeBoundsRebuildWait(t *testing.T) {
	t.Parallel()
	handler, root, _, _ := newHistoricalEnvelopeHandlerAtSlot(t, 64)
	trackedDB := &readTrackingDB{RwDB: handler.indiciesDB}
	handler.indiciesDB = trackedDB

	var hasDeadline bool
	var deadlineWithinLimit bool
	var openDuringRebuild int32
	handler.envelopeRebuilder = &apiEnvelopeRebuilder{rebuild: func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		hasDeadline = ok
		deadlineWithinLimit = ok && time.Until(deadline) <= executionPayloadEnvelopeRebuildTimeout
		openDuringRebuild = trackedDB.open.Load()
		return context.DeadlineExceeded
	}}

	response, err := handler.GetEthV1BeaconExecutionPayloadEnvelope(httptest.NewRecorder(), envelopeRequest(t, root))
	assert.Nil(t, response)
	assert.Equal(t, http.StatusInternalServerError, beaconhttp.WrapEndpointError(err).Code)
	assert.True(t, hasDeadline)
	assert.True(t, deadlineWithinLimit)
	assert.Zero(t, openDuringRebuild)
}

func newHistoricalEnvelopeHandlerAtSlot(t *testing.T, slot uint64) (*ApiHandler, common.Hash, *cltypes.SignedExecutionPayloadEnvelope, *mock_services.ForkChoiceStorageMock) {
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
