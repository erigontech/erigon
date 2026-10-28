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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type gatedBeaconBlockReader struct {
	freezeblocks.BeaconSnapshotReader
	entered chan struct{}
	release chan struct{}
}

func (r *gatedBeaconBlockReader) ReadBlockByRoot(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
	close(r.entered)
	<-r.release
	return nil, nil
}

func TestGetBeaconBlockByRootUsesImportedBlockBeforePersistence(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[root] = block
	forkchoiceStore.Headers[root] = block.SignedBeaconBlockHeader().Header

	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/%s/beacon/blocks/0x%x", version, root), http.NoBody)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), fmt.Sprintf(`"slot":"%d"`, block.Block.Slot))
		})
	}
}

func TestGetBeaconBlockByRootSamplesOptimisticStatusAfterFallback(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	reader := &gatedBeaconBlockReader{
		BeaconSnapshotReader: handler.blockReader,
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	handler.blockReader = reader
	response := make(chan *httptest.ResponseRecorder, 1)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/v2/beacon/blocks/0x%x", root), http.NoBody)

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

func TestGetBeaconBlockByRootKeepsPersistedBlockFastPath(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1]
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/%s/beacon/blocks/0x%x", version, root), http.NoBody)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), fmt.Sprintf(`"slot":"%d"`, block.Block.Slot))
		})
	}
}

func TestGetBeaconBlockByRootRejectsUnvalidatedForkchoiceBlock(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[root] = block

	for _, version := range []string{"v1", "v2"} {
		t.Run(version, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/%s/beacon/blocks/0x%x", version, root), http.NoBody)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
		})
	}
}

func TestGetBeaconBlockByRootRejectsUnavailableForkchoiceBlock(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	block := blocks[len(blocks)-1].Clone().(*cltypes.SignedBeaconBlock)
	block.Block.Slot++
	mismatchedRoot := common.Hash{0xff}
	forkchoiceStore.Blocks[mismatchedRoot] = block
	forkchoiceStore.Headers[mismatchedRoot] = block.SignedBeaconBlockHeader().Header
	headerOnlyRoot := common.Hash{0xdd}
	forkchoiceStore.Headers[headerOnlyRoot] = block.SignedBeaconBlockHeader().Header
	nilBlockRoot := common.Hash{0xcc}
	forkchoiceStore.Blocks[nilBlockRoot] = &cltypes.SignedBeaconBlock{}
	forkchoiceStore.Headers[nilBlockRoot] = block.SignedBeaconBlockHeader().Header
	nilBodyRoot := common.Hash{0xbb}
	nilBodyBlock := block.Clone().(*cltypes.SignedBeaconBlock)
	nilBodyBlock.Block.Body = nil
	forkchoiceStore.Blocks[nilBodyRoot] = nilBodyBlock
	forkchoiceStore.Headers[nilBodyRoot] = block.SignedBeaconBlockHeader().Header

	for _, testCase := range []struct {
		name string
		root common.Hash
	}{
		{name: "unknown", root: common.Hash{0xee}},
		{name: "mismatched", root: mismatchedRoot},
		{name: "header only", root: headerOnlyRoot},
		{name: "nil block", root: nilBlockRoot},
		{name: "nil body", root: nilBodyRoot},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, version := range []string{"v1", "v2"} {
				t.Run(version, func(t *testing.T) {
					request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/%s/beacon/blocks/0x%x", version, testCase.root), http.NoBody)
					recorder := httptest.NewRecorder()

					handler.ServeHTTP(recorder, request)

					require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
				})
			}
		})
	}
}
