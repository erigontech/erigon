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
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	antiquarytests "github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type gatedBeaconHeaderReader struct {
	freezeblocks.BeaconSnapshotReader
	entered chan struct{}
	release chan struct{}
}

func (r *gatedBeaconHeaderReader) ReadHeaderByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	close(r.entered)
	<-r.release
	return nil, nil
}

type failingBeaconHeaderReader struct {
	freezeblocks.BeaconSnapshotReader
	err error
}

func (r *failingBeaconHeaderReader) ReadHeaderByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlockHeader, error) {
	return nil, r.err
}

func TestGetHeadersIncludesFinalized(t *testing.T) {
	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	blockRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	canonicalOnlyBlockRoot, err := blocks[1].Block.HashSSZ()
	require.NoError(t, err)
	parentRoot := blocks[0].Block.ParentRoot
	canonicalOnlyParentRoot := blocks[1].Block.ParentRoot
	missingParentRoot := common.Hash{0xbe, 0xef}
	nonCanonicalBlock := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.Phase0Version)
	nonCanonicalBlock.Block.Slot = blocks[len(blocks)-1].Block.Slot + 1
	nonCanonicalBlock.Block.ParentRoot = parentRoot
	nonCanonicalBlock.Block.StateRoot = common.Hash{0xca, 0xfe}
	nonCanonicalRoot, err := nonCanonicalBlock.Block.HashSSZ()
	require.NoError(t, err)
	blockReader := handler.blockReader.(*antiquarytests.MockBlockReader)
	blockReader.U[nonCanonicalBlock.Block.Slot] = nonCanonicalBlock
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(context.Background(), tx, nonCanonicalBlock, false))
	require.NoError(t, tx.Commit())

	type expectedHeader struct {
		root      common.Hash
		canonical bool
	}

	for _, tc := range []struct {
		name          string
		parentRoot    common.Hash
		finalizedSlot uint64
		finalized     bool
		expected      []expectedHeader
	}{
		{
			name:          "not finalized",
			parentRoot:    parentRoot,
			finalizedSlot: 0,
			finalized:     false,
			expected: []expectedHeader{
				{root: common.Hash(blockRoot), canonical: true},
				{root: common.Hash(nonCanonicalRoot), canonical: false},
			},
		},
		{
			name:          "finalized",
			parentRoot:    canonicalOnlyParentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     true,
			expected: []expectedHeader{
				{root: common.Hash(canonicalOnlyBlockRoot), canonical: true},
			},
		},
		{
			name:          "non canonical child keeps envelope not finalized",
			parentRoot:    parentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     false,
			expected: []expectedHeader{
				{root: common.Hash(blockRoot), canonical: true},
				{root: common.Hash(nonCanonicalRoot), canonical: false},
			},
		},
		{
			name:          "no matching headers",
			parentRoot:    missingParentRoot,
			finalizedSlot: math.MaxUint64,
			finalized:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcu.FinalizedSlotVal = tc.finalizedSlot
			server := httptest.NewServer(handler.mux)
			defer server.Close()

			req, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/headers?parent_root=0x"+common.Bytes2Hex(tc.parentRoot[:]), nil)
			require.NoError(t, err)
			resp, err := server.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var body struct {
				Finalized           *bool `json:"finalized"`
				ExecutionOptimistic *bool `json:"execution_optimistic"`
				Data                []struct {
					Root      common.Hash `json:"root"`
					Canonical bool        `json:"canonical"`
				} `json:"data"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			require.NotNil(t, body.ExecutionOptimistic)
			require.NotNil(t, body.Finalized)
			require.Equal(t, tc.finalized, *body.Finalized)
			require.Len(t, body.Data, len(tc.expected))
			for i, expected := range tc.expected {
				require.Equal(t, expected.root, body.Data[i].Root)
				require.Equal(t, expected.canonical, body.Data[i].Canonical)
			}
		})
	}
}

func TestGetHeadHeaderIsCanonicalBeforeDatabasePromotion(t *testing.T) {
	db, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	head := blocks[len(blocks)-1]
	headRoot, err := head.Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = head.Block.Slot

	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return beacon_indicies.TruncateCanonicalChain(context.Background(), tx, head.Block.Slot)
	}))

	server := httptest.NewServer(handler.mux)
	defer server.Close()
	req, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/headers/head", nil)
	require.NoError(t, err)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Data headerResponse `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, common.Hash(headRoot), body.Data.Root)
	require.True(t, body.Data.Canonical)
}

func TestGetHeaderByRootUsesImportedBlockBeforePersistence(t *testing.T) {
	_, blocks, _, _, _, handler, _, syncData, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[root] = block
	forkchoiceStore.Headers[root] = block.SignedBeaconBlockHeader().Header
	handler.enableMemoizedHeadState = true
	syncData.(*synced_data.SyncedDataManager).OnSelectedHead(root, block.Block.Slot)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/headers/0x"+common.Bytes2Hex(root[:]), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response struct {
		Data headerResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, common.Hash(root), response.Data.Root)
	require.True(t, response.Data.Canonical)
	require.Equal(t, block.Block.Slot, response.Data.Header.Header.Slot)
}

func TestGetHeaderByRootKeepsLiveSideBranchNonCanonical(t *testing.T) {
	_, blocks, _, _, _, handler, _, syncData, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[root] = block
	forkchoiceStore.Headers[root] = block.SignedBeaconBlockHeader().Header
	handler.enableMemoizedHeadState = true
	syncData.(*synced_data.SyncedDataManager).OnSelectedHead(common.Hash{0xaa}, block.Block.Slot+1)
	forkchoiceStore.Ancestors[block.Block.Slot] = forkchoice.ForkChoiceNode{Root: common.Hash{0xbb}}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/headers/0x"+common.Bytes2Hex(root[:]), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response struct {
		Data headerResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.False(t, response.Data.Canonical)
}

func TestGetHeaderByRootRejectsUnavailableForkchoiceBlock(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	unvalidatedRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[unvalidatedRoot] = block
	mismatchedRoot := common.Hash{0xff}
	forkchoiceStore.Blocks[mismatchedRoot] = block
	forkchoiceStore.Headers[mismatchedRoot] = block.SignedBeaconBlockHeader().Header
	headerOnlyRoot := common.Hash{0xdd}
	forkchoiceStore.Headers[headerOnlyRoot] = block.SignedBeaconBlockHeader().Header

	for _, testCase := range []struct {
		name string
		root common.Hash
	}{
		{name: "unknown", root: common.Hash{0xee}},
		{name: "unvalidated", root: unvalidatedRoot},
		{name: "mismatched", root: mismatchedRoot},
		{name: "header only", root: headerOnlyRoot},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/headers/0x"+common.Bytes2Hex(testCase.root[:]), http.NoBody)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
		})
	}
}

func TestGetHeaderByRootPreservesDatabaseReaderError(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[root] = block
	forkchoiceStore.Headers[root] = block.SignedBeaconBlockHeader().Header
	databaseErr := errors.New("header database unavailable")
	handler.blockReader = &failingBeaconHeaderReader{BeaconSnapshotReader: handler.blockReader, err: databaseErr}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/headers/0x"+common.Bytes2Hex(root[:]), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), databaseErr.Error())
}

func TestGetHeaderByRootSamplesOptimisticStatusAfterFallback(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	reader := &gatedBeaconHeaderReader{
		BeaconSnapshotReader: handler.blockReader,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	handler.blockReader = reader
	response := make(chan *httptest.ResponseRecorder, 1)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/headers/0x"+common.Bytes2Hex(root[:]), http.NoBody)

	go func() {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		response <- recorder
	}()

	<-reader.entered
	forkchoiceStore.IsRootOptimisticVal = true
	forkchoiceStore.Blocks[root] = block
	forkchoiceStore.Headers[root] = block.SignedBeaconBlockHeader().Header
	close(reader.release)
	recorder := <-response

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"execution_optimistic":true`)
}
