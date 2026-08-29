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
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// makeGloasBlock creates a GLOAS SignedBeaconBlock with the given bid hashes.
func makeGloasBlock(slot uint64, blockHash, parentBlockHash common.Hash) *cltypes.SignedBeaconBlock {
	blk := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	blk.Block.Slot = slot
	blk.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	bid := blk.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BlockHash = blockHash
	bid.Message.ParentBlockHash = parentBlockHash
	return blk
}

// makeDenebBlock creates a pre-GLOAS (Deneb) SignedBeaconBlock.
func makeDenebBlock(slot uint64) *cltypes.SignedBeaconBlock {
	blk := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blk.Block.Slot = slot
	blk.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	return blk
}

func hash(b byte) common.Hash {
	var h common.Hash
	h[0] = b
	return h
}

// TestDetermineGloasFullRoots_EmptyBatch verifies that an empty batch returns no roots.
func TestDetermineGloasFullRoots_EmptyBatch(t *testing.T) {
	roots := determineGloasFullRoots(nil, nil)
	assert.Empty(t, roots)

	roots = determineGloasFullRoots([]*cltypes.SignedBeaconBlock{}, nil)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_AllPreGloas verifies that pre-GLOAS blocks are ignored.
func TestDetermineGloasFullRoots_AllPreGloas(t *testing.T) {
	responses := []*cltypes.SignedBeaconBlock{
		makeDenebBlock(100),
		makeDenebBlock(101),
		makeDenebBlock(102),
	}
	roots := determineGloasFullRoots(responses, nil)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_SingleBlock_NilLookahead verifies that a single GLOAS block
// with no prevBatchTopBlock (first batch ever) is treated optimistically as FULL.
func TestDetermineGloasFullRoots_SingleBlock_NilLookahead(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, nil)
	require.Len(t, roots, 1)

	expected, err := blk.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expected, roots[0])
}

// TestDetermineGloasFullRoots_InBatch_Full verifies that a GLOAS block is identified as FULL
// when the next block's bid.ParentBlockHash matches this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Full(t *testing.T) {
	// blk0 is FULL: blk1.ParentBlockHash == blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xAA)) // ParentBlockHash = blk0.BlockHash
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	roots := determineGloasFullRoots(responses, nil)
	// blk0 is FULL, blk1 is highest with nil prevBatchTopBlock → optimistic
	require.Len(t, roots, 2)

	root0, err := blk0.Block.HashSSZ()
	require.NoError(t, err)
	root1, err := blk1.Block.HashSSZ()
	require.NoError(t, err)
	assert.Contains(t, roots, root0)
	assert.Contains(t, roots, root1)
}

// TestDetermineGloasFullRoots_InBatch_Empty verifies that a GLOAS block is identified as EMPTY
// when the next block's bid.ParentBlockHash does NOT match this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Empty(t *testing.T) {
	// blk0 is EMPTY: blk1.ParentBlockHash != blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xCC)) // ParentBlockHash != blk0.BlockHash
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	roots := determineGloasFullRoots(responses, nil)
	// blk0 is EMPTY, blk1 is highest with nil prevBatchTopBlock → optimistic
	require.Len(t, roots, 1)

	root1, err := blk1.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, root1, roots[0])
}

// TestDetermineGloasFullRoots_CrossBatch_Full verifies the cross-batch lookahead:
// the highest block in a batch is confirmed FULL by prevBatchTopBlock.
func TestDetermineGloasFullRoots_CrossBatch_Full(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// prevBatchTopBlock is from the previous (higher-slot) batch; its ParentBlockHash = blk.BlockHash
	prevTop := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, prevTop)
	require.Len(t, roots, 1)

	expected, err := blk.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expected, roots[0])
}

// TestDetermineGloasFullRoots_CrossBatch_Empty verifies the cross-batch lookahead:
// the highest block is identified as EMPTY when prevBatchTopBlock doesn't reference it.
func TestDetermineGloasFullRoots_CrossBatch_Empty(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// prevBatchTopBlock's ParentBlockHash != blk.BlockHash → blk is EMPTY
	prevTop := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, prevTop)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_Mixed verifies a batch with both FULL and EMPTY blocks.
func TestDetermineGloasFullRoots_Mixed(t *testing.T) {
	// Chain: blk0(FULL) → blk1(EMPTY) → blk2(FULL) → blk3(highest, optimistic)
	//   blk1.ParentBlockHash == blk0.BlockHash → blk0 FULL
	//   blk2.ParentBlockHash != blk1.BlockHash → blk1 EMPTY
	//   blk3.ParentBlockHash == blk2.BlockHash → blk2 FULL
	//   blk3 is highest, prevBatchTopBlock=nil → blk3 optimistic
	blk0 := makeGloasBlock(100, hash(0x10), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0x20), hash(0x10)) // parent = blk0.hash → blk0 FULL
	blk2 := makeGloasBlock(102, hash(0x30), hash(0xFF)) // parent != blk1.hash → blk1 EMPTY
	blk3 := makeGloasBlock(103, hash(0x40), hash(0x30)) // parent = blk2.hash → blk2 FULL
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1, blk2, blk3}

	roots := determineGloasFullRoots(responses, nil)
	require.Len(t, roots, 3) // blk0, blk2, blk3(optimistic)

	root0, _ := blk0.Block.HashSSZ()
	root1, _ := blk1.Block.HashSSZ()
	root2, _ := blk2.Block.HashSSZ()
	root3, _ := blk3.Block.HashSSZ()
	assert.Contains(t, roots, root0)
	assert.NotContains(t, roots, root1)
	assert.Contains(t, roots, root2)
	assert.Contains(t, roots, root3)
}

// TestDetermineGloasFullRoots_MixedVersions verifies that pre-GLOAS blocks in a mixed
// batch are ignored and GLOAS blocks are processed correctly.
func TestDetermineGloasFullRoots_MixedVersions(t *testing.T) {
	deneb := makeDenebBlock(99)
	gloasFull := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// lookahead confirms gloasFull is FULL
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	responses := []*cltypes.SignedBeaconBlock{deneb, gloasFull, lookahead}

	roots := determineGloasFullRoots(responses, nil)
	// gloasFull FULL, lookahead optimistic (highest with nil prevBatchTopBlock)
	require.Len(t, roots, 2)

	rootFull, _ := gloasFull.Block.HashSSZ()
	rootLookahead, _ := lookahead.Block.HashSSZ()
	assert.Contains(t, roots, rootFull)
	assert.Contains(t, roots, rootLookahead)
}

func TestBackwardBeaconDownloaderHTTPPreferredEmptyResponseFallsBack(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	downloader := &BackwardBeaconDownloader{
		reqInterval:     time.NewTicker(time.Hour),
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	defer downloader.reqInterval.Stop()
	downloader.slotToDownload.Store(63)
	downloader.httpPreferred.Store(true)

	blocks, err := downloader.fetchBlockRange(ctx)
	if err == nil {
		t.Fatalf("fetchBlockRange returned nil error for %d blocks, want fallback instead of empty HTTP success", len(blocks))
	}
	if downloader.httpPreferred.Load() {
		t.Fatal("httpPreferred remained true after empty HTTP response")
	}
}

func TestFetchBlockFromBeaconAPIByRootRejectsDifferentBlock(t *testing.T) {
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	otherRoot := common.HexToHash("0xdead")
	fetched, err := fetchBlockFromBeaconAPIByRoot(t.Context(), server.URL, otherRoot, gloasFromGenesisConfig())
	require.ErrorContains(t, err, "root")
	require.Nil(t, fetched)
}

func TestBackwardBeaconDownloaderFetchEnvelopeUsesRootAndRejectsIdentityMismatch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0xdead")
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	requestedPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath <- r.URL.Path
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()
	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: cfg}

	fetched, err := downloader.fetchSingleEnvelope(t.Context(), block)
	require.ErrorContains(t, err, "root mismatch")
	require.Nil(t, fetched)
	require.Equal(t, "/eth/v1/beacon/execution_payload_envelopes/"+common.Hash(blockRoot).Hex(), <-requestedPath)
}

func TestBackwardBeaconDownloaderRequestMoreDoesNotFinishWhenMatchedCallbackFails(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedBlock, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = targetRoot
	encodedEnvelope, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	var httpRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpRequests.Add(1)
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/10":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedEnvelope)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		neverSkip:         false,
		reqInterval:       time.NewTicker(time.Hour),
		prevBatchTopBlock: child,
	}
	defer downloader.reqInterval.Stop()
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	sentinelErr := errors.New("callback failed after deciding to finish")
	failCallback := true
	callbackAttempts := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		callbackAttempts++
		if failCallback {
			return true, sentinelErr
		}
		return true, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.False(t, downloader.Finished())
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
	require.Equal(t, 1, callbackAttempts)
	require.False(t, downloader.httpPreferred.Load())
	requestsBeforeRetry := httpRequests.Load()
	retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
	require.Equal(t, requestsBeforeRetry, httpRequests.Load())
	require.Equal(t, 1, callbackAttempts)

	failCallback = false
	downloader.httpPreferred.Store(true)
	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, 2, callbackAttempts)
	require.True(t, downloader.Finished())
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
	require.Equal(t, target.Block.Slot-1, downloader.Progress())
}

func TestBackwardBeaconDownloaderRequestMoreDoesNotFinishWhenRootFallbackCallbackFails(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	wrong := makeGloasBlock(10, hash(0xbb), common.Hash{0x99})
	wrongRoot, err := wrong.Block.HashSSZ()
	require.NoError(t, err)
	encodedTarget, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedWrong, err := wrong.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedEnvelopes := make(map[string][]byte)
	for _, root := range []common.Hash{targetRoot, wrongRoot} {
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
		envelope.Message.BeaconBlockRoot = root
		encoded, encodeErr := envelope.EncodeSSZ(nil)
		require.NoError(t, encodeErr)
		encodedEnvelopes[root.Hex()] = encoded
	}
	var httpRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpRequests.Add(1)
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/10":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedWrong)
		case r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(targetRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedTarget)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			root := strings.TrimPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/")
			encoded, ok := encodedEnvelopes[root]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encoded)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		neverSkip:         false,
		reqInterval:       time.NewTicker(time.Hour),
		prevBatchTopBlock: child,
	}
	defer downloader.reqInterval.Stop()
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	sentinelErr := errors.New("root callback failed after deciding to finish")
	failCallback := true
	callbackAttempts := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		callbackAttempts++
		if failCallback {
			return true, sentinelErr
		}
		return true, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.False(t, downloader.Finished())
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
	require.Equal(t, 1, callbackAttempts)
	require.False(t, downloader.httpPreferred.Load())
	requestsBeforeRetry := httpRequests.Load()
	retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
	require.Equal(t, requestsBeforeRetry, httpRequests.Load())
	require.Equal(t, 1, callbackAttempts)

	failCallback = false
	downloader.httpPreferred.Store(true)
	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, 2, callbackAttempts)
	require.True(t, downloader.Finished())
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
	require.Equal(t, target.Block.Slot-1, downloader.Progress())
}

func TestBackwardBeaconDownloaderRequestMoreRetainsLookaheadAfterPartialCallbackFailure(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "empty"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			lower := makeGloasBlock(100, hash(0xaa), common.Hash{0x42})
			parentBlockHash := hash(0xcc)
			if full {
				parentBlockHash = hash(0xaa)
			}
			higher := makeGloasBlock(101, hash(0xbb), parentBlockHash)
			linkBeaconBlocks(t, lower, higher)
			batchSuccessor := makeGloasBlock(102, hash(0xdd), hash(0xbb))
			linkBeaconBlocks(t, higher, batchSuccessor)
			lowerRoot, err := lower.Block.HashSSZ()
			require.NoError(t, err)
			higherRoot, err := higher.Block.HashSSZ()
			require.NoError(t, err)
			encodedLower, err := lower.EncodeSSZ(nil)
			require.NoError(t, err)
			encodedHigher, err := higher.EncodeSSZ(nil)
			require.NoError(t, err)
			encodedEnvelopes := make(map[string][]byte)
			for _, root := range []common.Hash{higherRoot, lowerRoot} {
				envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
				envelope.Message.BeaconBlockRoot = root
				encoded, encodeErr := envelope.EncodeSSZ(nil)
				require.NoError(t, encodeErr)
				encodedEnvelopes[root.Hex()] = encoded
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/eth/v2/beacon/blocks/100":
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encodedLower)
				case "/eth/v2/beacon/blocks/101":
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encodedHigher)
				default:
					prefix := "/eth/v1/beacon/execution_payload_envelopes/"
					if !strings.HasPrefix(r.URL.Path, prefix) {
						http.NotFound(w, r)
						return
					}
					root := strings.TrimPrefix(r.URL.Path, prefix)
					if !full && root == common.Hash(lowerRoot).Hex() {
						http.NotFound(w, r)
						return
					}
					encoded, ok := encodedEnvelopes[root]
					if !ok {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encoded)
				}
			}))
			defer server.Close()

			downloader := &BackwardBeaconDownloader{
				expectedRoot:      higherRoot,
				httpFallbackURL:   server.URL,
				beaconCfg:         cfg,
				neverSkip:         false,
				prevBatchTopBlock: batchSuccessor,
			}
			downloader.httpPreferred.Store(true)
			downloader.slotToDownload.Store(higher.Block.Slot)
			lowerAttempts := 0
			downloader.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				switch block.Block.Slot {
				case higher.Block.Slot:
					require.NotNil(t, envelope)
					return false, nil
				case lower.Block.Slot:
					lowerAttempts++
					if full {
						require.NotNil(t, envelope)
					} else {
						require.Nil(t, envelope)
					}
					if lowerAttempts == 1 {
						return true, errors.New("lower callback failed")
					}
					return true, nil
				default:
					t.Fatalf("unexpected block slot %d", block.Block.Slot)
					return false, nil
				}
			})

			require.NoError(t, downloader.RequestMore(t.Context()))
			require.False(t, downloader.Finished())
			require.Equal(t, common.Hash(lowerRoot), downloader.expectedRoot)
			require.Equal(t, lower.Block.Slot, downloader.Progress())
			require.NotNil(t, downloader.prevBatchTopBlock)
			retainedRoot, err := downloader.prevBatchTopBlock.Block.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, higherRoot, retainedRoot)

			downloader.httpPreferred.Store(true)
			require.NoError(t, downloader.RequestMore(t.Context()))
			require.Equal(t, 2, lowerAttempts)
			require.True(t, downloader.Finished())
			require.Equal(t, lower.Block.ParentRoot, downloader.expectedRoot)
			require.Equal(t, lower.Block.Slot-1, downloader.Progress())
		})
	}
}

func TestBackwardBeaconDownloaderRootFallbackAdvancesOnlyForEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedBlock, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	wrongEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	wrongEnvelope.Message.BeaconBlockRoot = common.HexToHash("0xdead")
	encodedWrongEnvelope, err := wrongEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)

	tests := []struct {
		name          string
		writeEnvelope func(http.ResponseWriter)
		wantProcessed bool
	}{
		{
			name: "not found is not an empty proof",
			writeEnvelope: func(w http.ResponseWriter) {
				http.NotFound(w, nil)
			},
		},
		{
			name: "server error",
			writeEnvelope: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		},
		{
			name: "malformed body",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write([]byte{1, 2, 3})
			},
		},
		{
			name: "wrong version",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "fulu")
				_, _ = w.Write(encodedWrongEnvelope)
			},
		},
		{
			name: "wrong root",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(encodedWrongEnvelope)
			},
		},
		{
			name: "oversized body",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(bytes.Repeat([]byte{'x'}, int(clparams.MaxChunkSize)+1))
			},
		},
		{
			name: "transport failure",
			writeEnvelope: func(http.ResponseWriter) {
				panic(http.ErrAbortHandler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var httpRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests.Add(1)
				switch {
				case strings.HasPrefix(r.URL.Path, "/eth/v2/beacon/blocks/"):
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encodedBlock)
				case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
					tt.writeEnvelope(w)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			downloader := &BackwardBeaconDownloader{
				expectedRoot:      targetRoot,
				httpFallbackURL:   server.URL,
				beaconCfg:         cfg,
				reqInterval:       time.NewTicker(time.Hour),
				prevBatchTopBlock: child,
			}
			defer downloader.reqInterval.Stop()
			downloader.httpPreferred.Store(true)
			downloader.slotToDownload.Store(target.Block.Slot)
			processed := 0
			downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				processed++
				require.Nil(t, envelope)
				return false, nil
			})

			require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{makeDenebBlock(11)}))
			if tt.wantProcessed {
				require.Equal(t, 1, processed)
				require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
				require.Equal(t, target.Block.Slot-1, downloader.Progress())
			} else {
				require.Zero(t, processed)
				require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
				require.Equal(t, target.Block.Slot, downloader.Progress())
				require.False(t, downloader.httpPreferred.Load())
			}
			if tt.name == "not found is not an empty proof" {
				requestsBeforeRetry := httpRequests.Load()
				retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
				require.Equal(t, requestsBeforeRetry, httpRequests.Load())
			}
		})
	}
}

func TestBackwardBeaconDownloaderMissingRequiredEnvelopeSurvivesRestart(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	lookahead := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, lookahead)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	lookaheadRoot, err := lookahead.Block.HashSSZ()
	require.NoError(t, err)
	targetEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	targetEnvelope.Message.BeaconBlockRoot = targetRoot
	encodedTargetEnvelope, err := targetEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	lookaheadEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	lookaheadEnvelope.Message.BeaconBlockRoot = lookaheadRoot
	encodedLookaheadEnvelope, err := lookaheadEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)

	recovered := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch {
		case strings.HasSuffix(r.URL.Path, common.Hash(lookaheadRoot).Hex()):
			_, _ = w.Write(encodedLookaheadEnvelope)
		case strings.HasSuffix(r.URL.Path, common.Hash(targetRoot).Hex()) && recovered:
			_, _ = w.Write(encodedTargetEnvelope)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return false, nil
	})

	for range 3 {
		downloader.httpPreferred.Store(true)
		require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}))
	}
	require.Equal(t, 3, downloader.consecutiveEnvelopeFailures)
	require.Zero(t, processed)
	require.False(t, downloader.Finished())
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
	require.Empty(t, downloader.SkippedFullBlocks())

	recovered = true
	restarted := &BackwardBeaconDownloader{
		expectedRoot:                targetRoot,
		httpFallbackURL:             server.URL,
		beaconCfg:                   cfg,
		consecutiveEnvelopeFailures: 2,
	}
	restarted.httpPreferred.Store(true)
	restarted.slotToDownload.Store(target.Block.Slot)
	restartedProcessed := 0
	restarted.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		restartedProcessed++
		require.Equal(t, targetEnvelope.Message.BeaconBlockRoot, envelope.Message.BeaconBlockRoot)
		return true, nil
	})

	require.NoError(t, restarted.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}))
	require.Zero(t, restarted.consecutiveEnvelopeFailures)
	require.Equal(t, 1, restartedProcessed)
	require.True(t, restarted.Finished())
	require.Equal(t, target.Block.ParentRoot, restarted.expectedRoot)
}

func TestBackwardBeaconDownloaderInitial404RetainsUnresolvedBlock(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return false, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
}

func TestBackwardBeaconDownloaderInitialCheckpointUsesFetchedSuccessor(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "empty"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
			parentBlockHash := hash(0xcc)
			if full {
				parentBlockHash = hash(0xaa)
			}
			successor := makeGloasBlock(12, hash(0xbb), parentBlockHash)
			linkBeaconBlocks(t, target, successor)
			targetRoot, err := target.Block.HashSSZ()
			require.NoError(t, err)
			successorEncoded, err := successor.EncodeSSZ(nil)
			require.NoError(t, err)
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			envelope.Message.BeaconBlockRoot = targetRoot
			envelopeEncoded, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)

			var successorRequests atomic.Int32
			var envelopeRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/eth/v2/beacon/blocks/11":
					successorRequests.Add(1)
					http.NotFound(w, r)
				case r.URL.Path == "/eth/v2/beacon/blocks/12":
					successorRequests.Add(1)
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(successorEncoded)
				case strings.HasSuffix(r.URL.Path, common.Hash(targetRoot).Hex()):
					envelopeRequests.Add(1)
					if !full {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(envelopeEncoded)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			downloader := &BackwardBeaconDownloader{
				expectedRoot:    targetRoot,
				httpFallbackURL: server.URL,
				beaconCfg:       cfg,
			}
			downloader.SetCurrentSlotSampler(func() uint64 { return 13 })
			downloader.httpPreferred.Store(true)
			downloader.slotToDownload.Store(target.Block.Slot)
			processed := 0
			downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, got *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				processed++
				if full {
					require.NotNil(t, got)
				} else {
					require.Nil(t, got)
				}
				return true, nil
			})

			require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
			require.Equal(t, int32(2), successorRequests.Load())
			expectedEnvelopeRequests := int32(0)
			if full {
				expectedEnvelopeRequests = 1
			}
			require.Equal(t, expectedEnvelopeRequests, envelopeRequests.Load())
			require.Equal(t, 1, processed)
			require.True(t, downloader.Finished())
			require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
		})
	}
}

func TestBackwardBeaconDownloaderRetriesOmittedSuccessorRange(t *testing.T) {
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)

	var serveSuccessor atomic.Bool
	downloader := &BackwardBeaconDownloader{
		expectedRoot: targetRoot,
		beaconCfg:    gloasFromGenesisConfig(),
		currentSlot:  func() uint64 { return 13 },
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			if serveSuccessor.Load() && start <= successor.Block.Slot && successor.Block.Slot < start+count {
				return []*cltypes.SignedBeaconBlock{successor}, "block-peer", nil
			}
			return []*cltypes.SignedBeaconBlock{}, "block-peer", nil
		},
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)

	serveSuccessor.Store(true)
	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Equal(t, 1, processed)
	require.True(t, downloader.Finished())
}

func TestBackwardBeaconDownloaderAdvancesPastSuccessfulEmptyFallback(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(75, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/75" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(successorEncoded)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		currentSlot:     func() uint64 { return 76 },
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", peers.ErrNoPeers
		},
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Equal(t, 1, processed)
	require.True(t, downloader.Finished())
}

func TestBackwardBeaconDownloaderRejectsDisconnectedSuccessorBatch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	staleChild := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	canonicalChild := makeGloasBlock(12, hash(0xdd), hash(0xaa))
	linkBeaconBlocks(t, target, staleChild)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	canonicalChild.Block.ParentRoot = common.Hash{0xee}
	staleEncoded, err := staleChild.EncodeSSZ(nil)
	require.NoError(t, err)
	canonicalEncoded, err := canonicalChild.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/11":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(staleEncoded)
		case "/eth/v2/beacon/blocks/12":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(canonicalEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		currentSlot:     func() uint64 { return 13 },
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderRootFallbackRejectsDisconnectedSuccessorBatch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	wrong := makeGloasBlock(10, hash(0x99), common.Hash{0x24})
	staleChild := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	disconnected := makeGloasBlock(12, hash(0xdd), hash(0xaa))
	linkBeaconBlocks(t, target, staleChild)
	disconnected.Block.ParentRoot = common.Hash{0xee}
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encoded := make(map[string][]byte)
	for path, block := range map[string]*cltypes.SignedBeaconBlock{
		"/eth/v2/beacon/blocks/10":                               wrong,
		"/eth/v2/beacon/blocks/11":                               staleChild,
		"/eth/v2/beacon/blocks/12":                               disconnected,
		"/eth/v2/beacon/blocks/" + common.Hash(targetRoot).Hex(): target,
	} {
		encoded[path], err = block.EncodeSSZ(nil)
		require.NoError(t, err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := encoded[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		currentSlot:     func() uint64 { return 13 },
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{wrong, staleChild, disconnected}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderRootFallbackUsesRetainedChildToClassifyEmpty(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, child)
	wrong := makeGloasBlock(10, hash(0xdd), common.Hash{0x99})
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	targetEncoded, err := target.EncodeSSZ(nil)
	require.NoError(t, err)

	var envelopeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(targetRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(targetEncoded)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			envelopeRequests.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		prevBatchTopBlock: child,
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(got *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		gotRoot, hashErr := got.Block.HashSSZ()
		require.NoError(t, hashErr)
		require.Equal(t, targetRoot, gotRoot)
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{wrong}))
	require.Zero(t, envelopeRequests.Load())
	require.Equal(t, 1, processed)
	require.True(t, downloader.Finished())
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderIgnoresDisconnectedHTTPLookahead(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	canonicalChild := makeGloasBlock(12, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, canonicalChild)
	disconnected := makeGloasBlock(11, hash(0xcc), hash(0xdd))
	disconnected.Block.ParentRoot = common.Hash{0xff}
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		beaconCfg:         cfg,
		httpFallbackURL:   server.URL,
		prevBatchTopBlock: canonicalChild,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, disconnected}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderPacesRepeatedUnresolvedEnvelopeRetry(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedTarget, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	var envelopeRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/10":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedTarget)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			envelopeRequests.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		neverSkip:         false,
		reqInterval:       time.NewTicker(time.Hour),
		prevBatchTopBlock: child,
	}
	defer downloader.reqInterval.Stop()
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		t.Fatal("unresolved envelope must not reach callback")
		return false, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, int32(1), envelopeRequests.Load())
	require.False(t, downloader.httpPreferred.Load())

	retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(started), 25*time.Millisecond)
	require.Equal(t, int32(1), envelopeRequests.Load())
}

func TestBackwardBeaconDownloaderProvenFullDoesNotAdvanceOnHTTP404(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	lookahead := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, lookahead)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return false, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
}

func TestBackwardBeaconDownloaderDoesNotSkipUnknownGloasPayload(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		withoutEngine := &BackwardBeaconDownloader{
			beaconCfg:    gloasFromGenesisConfig(),
			expectedRoot: common.Hash{0xaa},
		}
		require.False(t, withoutEngine.canSkipSlot(t.Context(), tx, 0, 0, 10))

		engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
		withEngine := &BackwardBeaconDownloader{
			engine:       engine,
			beaconCfg:    gloasFromGenesisConfig(),
			expectedRoot: common.Hash{0xaa},
		}
		require.False(t, withEngine.canSkipSlot(t.Context(), tx, 0, 0, 10))
		return nil
	}))
}
