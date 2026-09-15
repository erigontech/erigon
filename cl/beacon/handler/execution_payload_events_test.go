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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestImportedEnvelopeEventsSurvivePartialColumnStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	currentSlot := handler.ethClock.GetCurrentSlot()
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	handler.ethClock = clock
	handler.beaconChainCfg.GloasForkEpoch = 0

	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	_, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
	require.NoError(t, err)
	require.Len(t, proofs, int(handler.beaconChainCfg.NumberOfColumns))
	bundleProofs := make([]common.Bytes48, len(proofs))
	for i := range proofs {
		bundleProofs[i] = common.Bytes48(proofs[i])
	}

	executionRequests := cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion)
	executionRequestsRoot, err := executionRequests.HashSSZ()
	require.NoError(t, err)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)
	payload.BlockHash = common.HexToHash("0x1234")
	payload.SlotNumber = currentSlot

	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	block.Block.Slot = currentSlot
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BuilderIndex = 3
	bid.Slot = currentSlot
	bid.BlockHash = payload.BlockHash
	bid.ExecutionRequestsRoot = common.Hash(executionRequestsRoot)
	bid.BlobKzgCommitments.Append((*cltypes.KZGCommitment)(&commitment))
	_, ok := handler.pendingBuilderPayloads.Add(currentSlot, bid, &selfBuildPayload{
		Payload: payload, ExecutionRequests: executionRequests, BlobBundles: []BlobBundle{{
			Commitment: common.Bytes48(commitment), Blob: (*cltypes.Blob)(&blob), KzgProofs: bundleProofs,
		}},
	})
	require.True(t, ok)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Blocks[common.Hash(blockRoot)] = block

	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 8)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	var columnsWritten atomic.Int32
	stored := make(map[uint64]bool)
	failed := false
	imports := 0
	columnStorage := blob_storage_mock.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().WriteColumnSidecars(gomock.Any(), common.Hash(blockRoot), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ common.Hash, _ int64, column *cltypes.DataColumnSidecar) error {
			columnsWritten.Add(1)
			if !failed && column.Index == handler.beaconChainCfg.NumberOfColumns-1 {
				failed = true
				return errors.New("injected final column write failure")
			}
			stored[column.Index] = true
			return nil
		}).Times(2 * int(handler.beaconChainCfg.NumberOfColumns))
	handler.columnStorage = columnStorage
	fcu.OnExecutionPayloadFn = func(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
		for i := range uint64(4) {
			if !stored[i] {
				return forkchoice.ErrEIP7594ColumnDataNotAvailable
			}
		}
		if fcu.Envelopes[envelope.Message.BeaconBlockRoot] != nil {
			return forkchoice.ErrIgnore
		}
		imports++
		fcu.Envelopes[envelope.Message.BeaconBlockRoot] = envelope
		return nil
	}

	signedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload: payload, ExecutionRequests: executionRequests, BuilderIndex: bid.BuilderIndex,
		BeaconBlockRoot: common.Hash(blockRoot), ParentBeaconBlockRoot: block.Block.ParentRoot,
	}}
	body, err := json.Marshal(signedEnvelope)
	require.NoError(t, err)
	for attempt := range 2 {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Blob-Data-Included", "false")
		request.Header.Set("Eth-Consensus-Version", "gloas")
		recorder := httptest.NewRecorder()
		handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
		expected := http.StatusAccepted
		if attempt == 1 {
			expected = http.StatusOK
		}
		require.Equal(t, expected, recorder.Code, recorder.Body.String())
	}
	require.Equal(t, 1, imports)
	require.Len(t, stored, int(handler.beaconChainCfg.NumberOfColumns))
	counts := make(map[beaconevents.EventTopic]int)
	for len(events) > 0 {
		counts[(<-events).Event]++
	}
	t.Logf("imports=%d stored=%d writes=%d events=%v", imports, len(stored), columnsWritten.Load(), counts)
	require.Equal(t, 1, counts[beaconevents.OpExecutionPayload], "a successfully imported envelope must emit its imported event even after partial supplied-column storage")
	require.Equal(t, 1, counts[beaconevents.OpExecutionPayloadAvailable])
}
