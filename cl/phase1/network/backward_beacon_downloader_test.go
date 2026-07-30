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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
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

func TestDetermineGloasFullRoots_IncompleteBlocks(t *testing.T) {
	incomplete := &cltypes.SignedBeaconBlock{}

	assert.NotPanics(t, func() {
		roots := determineGloasFullRoots(
			[]*cltypes.SignedBeaconBlock{nil, incomplete},
			nil,
		)
		assert.Empty(t, roots)
	})
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

func TestDetermineGloasFullRoots_SingleBlock_NilLookaheadDoesNotGuess(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, nil)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_InBatch_Full verifies that a GLOAS block is identified as FULL
// when the next block's bid.ParentBlockHash matches this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Full(t *testing.T) {
	// blk0 is FULL: blk1.ParentBlockHash == blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xAA)) // ParentBlockHash = blk0.BlockHash
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	roots := determineGloasFullRoots(responses, nil)
	require.Len(t, roots, 1)

	root0, err := blk0.Block.HashSSZ()
	require.NoError(t, err)
	assert.Contains(t, roots, root0)
}

// TestDetermineGloasFullRoots_InBatch_Empty verifies that a GLOAS block is identified as EMPTY
// when the next block's bid.ParentBlockHash does NOT match this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Empty(t *testing.T) {
	// blk0 is EMPTY: blk1.ParentBlockHash != blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xCC)) // ParentBlockHash != blk0.BlockHash
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	roots := determineGloasFullRoots(responses, nil)
	assert.Empty(t, roots)
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
	require.Len(t, roots, 2)

	root0, _ := blk0.Block.HashSSZ()
	root1, _ := blk1.Block.HashSSZ()
	root2, _ := blk2.Block.HashSSZ()
	root3, _ := blk3.Block.HashSSZ()
	assert.Contains(t, roots, root0)
	assert.NotContains(t, roots, root1)
	assert.Contains(t, roots, root2)
	assert.NotContains(t, roots, root3)
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
	require.Len(t, roots, 1)

	rootFull, _ := gloasFull.Block.HashSSZ()
	assert.Contains(t, roots, rootFull)
}

func TestBackwardBeaconDownloaderFirstBatchUsesLookaheadAfterMissedSlot(t *testing.T) {
	anchor := makeGloasBlock(100, hash(0xAA), hash(0x10))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)

	lookahead := makeGloasBlock(102, hash(0xBB), hash(0x10))
	lookahead.Block.ParentRoot = anchorRoot
	encodedLookahead, err := lookahead.EncodeSSZ(nil)
	require.NoError(t, err)

	var lookaheadRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/102" {
			lookaheadRequests.Add(1)
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedLookahead)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	var processed atomic.Bool
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    anchorRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Store(true)
			assert.Nil(t, envelope)
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{anchor}))
	assert.True(t, processed.Load())
	assert.Equal(t, int32(1), lookaheadRequests.Load())
}

func TestBackwardBeaconDownloaderFirstBatchFullBlockWaitsForEnvelope(t *testing.T) {
	anchor := makeGloasBlock(100, hash(0xAA), hash(0x10))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)

	lookahead := makeGloasBlock(102, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = anchorRoot
	encodedLookahead, err := lookahead.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/102" {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedLookahead)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	var processed atomic.Bool
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    anchorRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Store(true)
			return true, nil
		},
	}
	downloader.httpPreferred.Store(true)

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{anchor}))
	assert.False(t, processed.Load())
	assert.Equal(t, 1, downloader.consecutiveEnvelopeFailures)
}

func TestBackwardBeaconDownloaderHTTPPreferredMissingEnvelopeTracksFailure(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	lookahead := makeGloasBlock(102, hash(0xBB), hash(0xAA))
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	downloader.httpPreferred.Store(true)

	envelopes, fullRoots := downloader.fetchGloasEnvelopes(
		context.Background(),
		[]*cltypes.SignedBeaconBlock{block, lookahead},
	)

	assert.Empty(t, envelopes)
	require.Len(t, fullRoots, 1)
	assert.Equal(t, 1, downloader.consecutiveEnvelopeFailures)
}

func TestSelectGloasLookaheadRejectsUnlinkedAndIncompleteBlocks(t *testing.T) {
	anchor := makeGloasBlock(100, hash(0xAA), hash(0x10))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)

	unlinked := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	unlinked.Block.ParentRoot = hash(0xFF)
	linked := makeGloasBlock(102, hash(0xCC), hash(0xAA))
	linked.Block.ParentRoot = anchorRoot
	laterLinked := makeGloasBlock(103, hash(0xDD), hash(0xCC))
	laterLinked.Block.ParentRoot = anchorRoot

	selected := selectGloasLookahead(
		anchor,
		anchorRoot,
		[]*cltypes.SignedBeaconBlock{nil, unlinked, laterLinked, linked},
	)
	assert.Same(t, linked, selected)
}

func TestFetchEnvelopesFromBeaconAPIIncompleteBlock(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)

	assert.NotPanics(t, func() {
		fetched := fetchEnvelopesFromBeaconAPI(
			context.Background(),
			server.URL,
			[]*cltypes.SignedBeaconBlock{nil, block},
			[][32]byte{blockRoot},
			received,
			&clparams.MainnetBeaconConfig,
		)
		assert.Zero(t, fetched)
	})
}

func TestBackwardBeaconDownloaderPreGloasFirstBatchNeedsNoLookahead(t *testing.T) {
	block := makeDenebBlock(100)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	var processed atomic.Bool
	downloader := &BackwardBeaconDownloader{
		expectedRoot: blockRoot,
		beaconCfg:    &clparams.MainnetBeaconConfig,
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Store(true)
			assert.Nil(t, envelope)
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	assert.True(t, processed.Load())
}

func TestBackwardBeaconDownloaderSkipsIncompleteResponse(t *testing.T) {
	block := makeDenebBlock(100)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		expectedRoot: blockRoot,
		beaconCfg:    &clparams.MainnetBeaconConfig,
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			return false, nil
		},
	}

	assert.NotPanics(t, func() {
		require.NoError(t, downloader.processResponses(
			context.Background(),
			[]*cltypes.SignedBeaconBlock{nil, block},
		))
	})
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
