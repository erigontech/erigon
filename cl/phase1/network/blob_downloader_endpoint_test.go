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

package network

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type endpointBlock struct {
	block    *cltypes.SignedBeaconBlock
	root     common.Hash
	sidecars []*cltypes.BlobSidecar
}

func newEndpointBlock(t *testing.T, slot uint64) endpointBlock {
	t.Helper()
	block, sidecars := validDenebRecoverySidecars(t, slot, 1)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return endpointBlock{block: block, root: root, sidecars: sidecars}
}

// blobEndpoint serves the sidecars of served and answers 400, as a pruned beacon node does, for every other root.
func blobEndpoint(t *testing.T, served ...endpointBlock) (string, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		id := common.HexToHash(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		for _, b := range served {
			if b.root == id {
				if err := json.NewEncoder(w).Encode(map[string]any{"data": b.sidecars}); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, requests
}

func newEndpointDownloader(t *testing.T, endpoints []string, workers int, peer blobPeerClient) *BlobHistoryDownloader {
	t.Helper()
	downloader := newBoundaryDownloader(t, 100, 0, 100, &boundaryBlockReader{})
	downloader.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	downloader.rpc = peer
	downloader.SetBlobEndpoints(endpoints, workers)
	return downloader
}

func storedSidecars(t *testing.T, downloader *BlobHistoryDownloader, b endpointBlock) int {
	t.Helper()
	sidecars, _, err := downloader.blobStorage.ReadBlobSidecars(t.Context(), b.block.Block.Slot, b.root)
	require.NoError(t, err)
	return len(sidecars)
}

func TestBlobHistoryDownloaderFillsABlockFromTheEndpoint(t *testing.T) {
	b := newEndpointBlock(t, 100)
	url, requests := blobEndpoint(t, b)
	peer := &countingBlobPeerClient{}
	downloader := newEndpointDownloader(t, []string{url}, 2, peer)

	require.True(t, downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}}))
	require.Equal(t, 1, storedSidecars(t, downloader, b))
	require.Equal(t, int32(1), requests.Load())
	require.Zero(t, peer.requests)
}

func TestBlobHistoryDownloaderAsksPeersForABlockTheEndpointDoesNotServe(t *testing.T) {
	b := newEndpointBlock(t, 100)
	url, requests := blobEndpoint(t)
	peer := &countingBlobPeerClient{responses: b.sidecars}
	downloader := newEndpointDownloader(t, []string{url}, 2, peer)

	require.True(t, downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}}))
	require.Equal(t, 1, storedSidecars(t, downloader, b))
	require.Equal(t, int32(1), requests.Load())
	require.Equal(t, 1, peer.requests)
}

func TestBlobHistoryDownloaderTriesTheNextEndpoint(t *testing.T) {
	b := newEndpointBlock(t, 100)
	pruned, prunedRequests := blobEndpoint(t)
	archive, archiveRequests := blobEndpoint(t, b)
	peer := &countingBlobPeerClient{}
	downloader := newEndpointDownloader(t, []string{pruned, archive}, 2, peer)

	require.True(t, downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}}))
	require.Equal(t, 1, storedSidecars(t, downloader, b))
	require.Equal(t, int32(1), prunedRequests.Load())
	require.Equal(t, int32(1), archiveRequests.Load())
	require.Zero(t, peer.requests)
}

// The backfill walks down from the head, and a pruned endpoint serves only the slots above its window, so once it
// does not serve a slot it is not asked for older slots in the later batches of the same pass.
func TestBlobHistoryDownloaderStopsAskingAnEndpointBelowASlotItDoesNotServe(t *testing.T) {
	newer, older := newEndpointBlock(t, 100), newEndpointBlock(t, 99)
	url, requests := blobEndpoint(t)
	downloader := newEndpointDownloader(t, []string{url}, 1, &countingBlobPeerClient{})

	downloader.processBatch([]incompleteBlock{{block: newer.block, root: newer.root}})
	downloader.processBatch([]incompleteBlock{{block: older.block, root: older.root}})
	require.Equal(t, int32(1), requests.Load())

	downloader.resetEndpointWindows()
	downloader.processBatch([]incompleteBlock{{block: older.block, root: older.root}})
	require.Equal(t, int32(2), requests.Load(), "a new pass asks the endpoint again")
}

func TestBlobEndpointWorkersDefaultToAQuarterOfTheCPUs(t *testing.T) {
	for _, tc := range []struct{ configured, cpus, want int }{
		{0, 1, 1}, {0, 2, 1}, {0, 4, 1}, {0, 8, 2}, {0, 16, 4}, {0, 32, 8}, {3, 16, 3}, {64, 16, 64},
	} {
		require.Equal(t, tc.want, blobEndpointWorkers(tc.configured, tc.cpus), "configured %d, %d cores", tc.configured, tc.cpus)
	}
}
