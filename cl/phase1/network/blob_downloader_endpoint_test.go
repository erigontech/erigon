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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/blobfetch"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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

// blobEndpoint serves the sidecars of served and answers 400, as a pruned beacon node does, for every other root. Its
// request count leaves out the blobs requests, which a fetch makes only after a blob_sidecars request was not served.
func blobEndpoint(t *testing.T, served ...endpointBlock) (string, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/eth/v1/beacon/blobs/") {
			requests.Add(1)
		}
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

// blobsOnlyEndpoint answers 404 on blob_sidecars and serves the blobs of served. Its request count is the number of
// blob_sidecars requests.
func blobsOnlyEndpoint(t *testing.T, served ...endpointBlock) (string, *atomic.Int32) {
	t.Helper()
	requests := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/eth/v1/beacon/blobs/") {
			requests.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		id := common.HexToHash(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		for _, b := range served {
			if b.root != id {
				continue
			}
			blobs := make([]*cltypes.Blob, len(b.sidecars))
			for i, sidecar := range b.sidecars {
				blobs[i] = &sidecar.Blob
			}
			if err := json.NewEncoder(w).Encode(map[string]any{"data": blobs}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, requests
}

// servingPeer answers every request with the sidecars of the requested root, so blocks the endpoint leaves to peers
// are filled at once instead of waiting out the peer request deadline.
type servingPeer struct {
	sidecars map[common.Hash][]*cltypes.BlobSidecar
}

func (servingPeer) Peers() (uint64, error) { return 1, nil }

func (p servingPeer) SendBlobsSidecarByIdentifierReq(_ context.Context, ids *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	var out []*cltypes.BlobSidecar
	for i := 0; i < ids.Len(); i++ {
		if ids.Get(i).Index == 0 {
			out = append(out, p.sidecars[ids.Get(i).BlockRoot]...)
		}
	}
	return out, "peer", nil
}

func endpointBlocks(t *testing.T, slots ...uint64) ([]endpointBlock, servingPeer) {
	t.Helper()
	peer := servingPeer{sidecars: map[common.Hash][]*cltypes.BlobSidecar{}}
	blocks := make([]endpointBlock, len(slots))
	for i, slot := range slots {
		blocks[i] = newEndpointBlock(t, slot)
		peer.sidecars[blocks[i].root] = blocks[i].sidecars
	}
	return blocks, peer
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

func TestBlobHistoryDownloaderFillsABlockFromTheBlobsOfTheEndpoint(t *testing.T) {
	b := newEndpointBlock(t, 100)
	url, _ := blobsOnlyEndpoint(t, b)
	peer := &countingBlobPeerClient{}
	downloader := newEndpointDownloader(t, []string{url}, 2, peer)
	downloader.blockReader = &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{100: b.block}}

	require.True(t, downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}}))
	require.Equal(t, 1, storedSidecars(t, downloader, b))
	require.Zero(t, peer.requests)
}

func TestBlobHistoryDownloaderStoredBlockReadFailureIsNotAnEndpointFailure(t *testing.T) {
	slots := make([]uint64, endpointFailuresBeforePause+1)
	for i := range slots {
		slots[i] = uint64(100 - i)
	}
	blocks, peer := endpointBlocks(t, slots...)
	first, firstRequests := blobsOnlyEndpoint(t, blocks...)
	second, secondRequests := blobEndpoint(t, blocks...)
	downloader := newEndpointDownloader(t, []string{first, second}, 1, peer)
	downloader.blockReader = &boundaryBlockReader{blindedErr: errors.New("block read failed")}
	seeded := endpointState{failures: endpointFailuresBeforePause - 1, misses: 1, highestMiss: 200}
	downloader.endpointStates[0] = seeded

	for _, b := range blocks {
		require.Empty(t, downloader.fetchFromEndpoints([]incompleteBlock{{block: b.block, root: b.root}}))
		require.Equal(t, 1, storedSidecars(t, downloader, b))
	}

	require.Equal(t, int32(len(blocks)), firstRequests.Load(), "the first endpoint is asked for every block, it was not paused")
	require.Equal(t, int32(len(blocks)), secondRequests.Load())
	require.Equal(t, seeded, downloader.endpointStates[0])
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
// has not served several slots in a row it is not asked for older slots in the later batches of the same pass.
func TestBlobHistoryDownloaderStopsAskingAnEndpointBelowTheSlotsItDoesNotServe(t *testing.T) {
	slots := make([]uint64, endpointMissesBeforeCutoff+1)
	for i := range slots {
		slots[i] = uint64(100 - i)
	}
	blocks, peer := endpointBlocks(t, slots...)
	url, requests := blobEndpoint(t)
	downloader := newEndpointDownloader(t, []string{url}, 1, peer)

	for _, b := range blocks[:endpointMissesBeforeCutoff] {
		downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}})
	}
	older := blocks[endpointMissesBeforeCutoff]
	downloader.processBatch([]incompleteBlock{{block: older.block, root: older.root}})
	require.Equal(t, int32(endpointMissesBeforeCutoff), requests.Load())

	downloader.resetEndpointWindows()
	downloader.processBatch([]incompleteBlock{{block: older.block, root: older.root}})
	require.Equal(t, int32(endpointMissesBeforeCutoff+1), requests.Load(), "a new pass asks the endpoint again")
}

// Misses only end the window when they come in a row: a served slot in between starts the count again.
func TestBlobHistoryDownloaderKeepsAskingAnEndpointAfterMissedSlots(t *testing.T) {
	// served, then endpointMissesBeforeCutoff-1 misses, served, one more miss, served
	slots := make([]uint64, endpointMissesBeforeCutoff+4)
	for i := range slots {
		slots[i] = uint64(100 - i)
	}
	blocks, peer := endpointBlocks(t, slots...)
	last := len(blocks) - 1
	url, requests := blobEndpoint(t, blocks[0], blocks[endpointMissesBeforeCutoff], blocks[last])
	downloader := newEndpointDownloader(t, []string{url}, 1, peer)

	for _, b := range blocks {
		downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}})
	}
	require.Equal(t, int32(len(blocks)), requests.Load())
	require.Equal(t, 1, storedSidecars(t, downloader, blocks[last]))
}

// An endpoint that is down, times out or rate-limits costs a full retry cycle per block, so after a few failures in a
// row it is paused for a while.
func TestBlobHistoryDownloaderPausesAnEndpointThatKeepsFailing(t *testing.T) {
	retryWait := blobfetch.RetryWait
	blobfetch.RetryWait = time.Millisecond
	t.Cleanup(func() { blobfetch.RetryWait = retryWait })
	requests := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	slots := make([]uint64, endpointFailuresBeforePause+2)
	for i := range slots {
		slots[i] = uint64(100 - i)
	}
	blocks, peer := endpointBlocks(t, slots...)
	downloader := newEndpointDownloader(t, []string{srv.URL}, 1, peer)
	now := time.Now()
	downloader.endpointNow = func() time.Time { return now }

	ask := func(b endpointBlock) { downloader.processBatch([]incompleteBlock{{block: b.block, root: b.root}}) }
	for _, b := range blocks[:endpointFailuresBeforePause] {
		ask(b)
	}
	asked := requests.Load()
	ask(blocks[endpointFailuresBeforePause])
	require.Equal(t, asked, requests.Load(), "a paused endpoint is not asked")

	now = now.Add(endpointPause)
	ask(blocks[endpointFailuresBeforePause+1])
	require.Greater(t, requests.Load(), asked, "the endpoint is asked again after the pause")
}

func TestBlobEndpointWorkersDefaultToAQuarterOfTheCPUs(t *testing.T) {
	for _, tc := range []struct{ configured, cpus, want int }{
		{0, 1, 1}, {0, 2, 1}, {0, 4, 1}, {0, 8, 2}, {0, 16, 4}, {0, 32, 8}, {3, 16, 3}, {64, 16, 64},
	} {
		require.Equal(t, tc.want, blobEndpointWorkers(tc.configured, tc.cpus), "configured %d, %d cores", tc.configured, tc.cpus)
	}
}

// Requests of a batch finish in any order: a slow served slot between fast misses must still break the run of misses.
func TestBlobHistoryDownloaderCountsMissesInSlotOrder(t *testing.T) {
	blocks, peer := endpointBlocks(t, 100, 99, 98, 97, 96, 95)
	slow, older := blocks[2], blocks[5]
	requests := &atomic.Int32{}
	misses := make(chan struct{}, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallback := strings.Contains(r.URL.Path, "/eth/v1/beacon/blobs/")
		if !fallback {
			requests.Add(1)
		}
		id := common.HexToHash(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		for _, b := range []endpointBlock{slow, older} {
			if b.root == id {
				if b.root == slow.root {
					// Answer the served slot only after the four misses around it, so they finish first.
					for range 4 {
						select {
						case <-misses:
						case <-time.After(5 * time.Second):
						}
					}
				}
				if err := json.NewEncoder(w).Encode(map[string]any{"data": b.sidecars}); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
		}
		w.WriteHeader(http.StatusBadRequest)
		if !fallback {
			return
		}
		// A miss is over once its blobs fallback is answered.
		select {
		case misses <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(srv.Close)
	downloader := newEndpointDownloader(t, []string{srv.URL}, 4, peer)

	batch := make([]incompleteBlock, 0, 5)
	for _, b := range blocks[:5] {
		batch = append(batch, incompleteBlock{block: b.block, root: b.root})
	}
	downloader.processBatch(batch)
	downloader.processBatch([]incompleteBlock{{block: older.block, root: older.root}})
	require.Equal(t, int32(6), requests.Load(), "two misses on each side of a served slot do not end the window")
}

// The endpoint does not need peers, so a pass runs without them.
func TestBlobHistoryDownloaderFillsFromTheEndpointWithoutPeers(t *testing.T) {
	b := newEndpointBlock(t, 100)
	url, _ := blobEndpoint(t, b)
	downloader := newBoundaryDownloader(t, 100, 0, 100, &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{100: b.block}})
	downloader.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	downloader.rpc = boundaryPeerCounter(0)
	downloader.SetBlobEndpoints([]string{url}, 1)

	require.NoError(t, downloader.downloadOnce(false))
	require.Equal(t, 1, storedSidecars(t, downloader, b))
}

func failingEndpoint(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	retryWait := blobfetch.RetryWait
	blobfetch.RetryWait = time.Millisecond
	t.Cleanup(func() { blobfetch.RetryWait = retryWait })
	requests := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, requests
}

// Within one batch, requests that start after the pause see it.
func TestBlobHistoryDownloaderPausesAFailingEndpointWithinABatch(t *testing.T) {
	slots := make([]uint64, 8)
	for i := range slots {
		slots[i] = uint64(100 - i)
	}
	blocks, peer := endpointBlocks(t, slots...)
	url, requests := failingEndpoint(t)
	downloader := newEndpointDownloader(t, []string{url}, 1, peer)

	batch := make([]incompleteBlock, 0, len(blocks))
	for _, b := range blocks {
		batch = append(batch, incompleteBlock{block: b.block, root: b.root})
	}
	downloader.processBatch(batch)
	attemptsPerBlock := int32(3)
	require.LessOrEqual(t, requests.Load(), attemptsPerBlock*(endpointFailuresBeforePause+1),
		"at most the blocks in flight when the pause starts are asked after it")
}

// Retries visit scattered slots in their own order, so their misses must not end the window of the scan.
func TestBlobHistoryDownloaderRetryMissesDoNotEndTheScanWindow(t *testing.T) {
	retrySlots := []uint64{90, 300, 150, 200, 120}
	blocks, peer := endpointBlocks(t, append(slices.Clone(retrySlots), 60)...)
	scanned := blocks[len(blocks)-1]
	url, requests := blobEndpoint(t, scanned)
	reader := &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{}}
	for _, b := range blocks {
		reader.blocks[b.block.Block.Slot] = b.block
	}
	downloader := newBoundaryDownloader(t, 300, 0, 0, reader)
	downloader.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	downloader.rpc = peer
	downloader.SetBlobEndpoints([]string{url}, 1)
	for _, slot := range retrySlots {
		downloader.addRetrySlot(slot)
	}

	require.NoError(t, downloader.retryFailedRecoveries(0, 0))
	asked := requests.Load()
	require.Equal(t, int32(len(retrySlots)), asked)
	downloader.processBatch([]incompleteBlock{{block: scanned.block, root: scanned.root}})
	require.Equal(t, asked+1, requests.Load(), "the scan still asks the endpoint below the retried slots")
}

// Without peers, a block the endpoint leaves ends the pass, as a pass without peers did before endpoints: queueing it
// and every older block as a failed recovery would only refill the retry list.
func TestBlobHistoryDownloaderEndsAPassWithoutPeersAtABlockTheEndpointLeaves(t *testing.T) {
	newer, older := newEndpointBlock(t, 100), newEndpointBlock(t, 99)
	url, requests := blobEndpoint(t)
	downloader := newBoundaryDownloader(t, 100, 0, 90, &boundaryBlockReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{100: newer.block, 99: older.block}})
	downloader.blobStorage = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	downloader.rpc = boundaryPeerCounter(0)
	downloader.SetBlobEndpoints([]string{url}, 1)

	require.NoError(t, downloader.downloadOnce(false))
	require.Empty(t, downloader.retryRanges)
	require.Positive(t, requests.Load())
}
