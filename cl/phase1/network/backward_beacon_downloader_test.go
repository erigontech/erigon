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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
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

func linkGloasChild(t *testing.T, parent, child *cltypes.SignedBeaconBlock) common.Hash {
	t.Helper()
	root, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	child.Block.ParentRoot = root
	return root
}

func makeGloasEnvelopeForBlock(t *testing.T, block *cltypes.SignedBeaconBlock) (*cltypes.SignedExecutionPayloadEnvelope, common.Hash) {
	t.Helper()
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.Payload.Extra = solid.NewExtraData()
	envelope.Message.Payload.Transactions = solid.NewProgressiveTransactionsSSZ()
	envelope.Message.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	envelope.Message.ParentBeaconBlockRoot = block.Block.ParentRoot
	envelope.Message.Payload.SlotNumber = block.Block.Slot
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	envelope.Message.Payload.ParentHash = bid.ParentBlockHash
	envelope.Message.Payload.PrevRandao = bid.PrevRandao
	envelope.Message.Payload.FeeRecipient = bid.FeeRecipient
	envelope.Message.Payload.GasLimit = bid.GasLimit
	envelope.Message.BuilderIndex = bid.BuilderIndex
	requestsRoot, err := envelope.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	bid.ExecutionRequestsRoot = requestsRoot
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(&clparams.MainnetBeaconConfig, envelope.Message.ExecutionRequests))
	envelope.Message.Payload.BlockHash, err = envelope.Message.Payload.ComputeBlockHash(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	bid.BlockHash = envelope.Message.Payload.BlockHash
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = root
	return envelope, root
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

func TestDetermineGloasEnvelopeRootsSingleBlockWithoutLookaheadIsOnlyOptimistic(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	responses := []*cltypes.SignedBeaconBlock{blk}

	fetchRoots, fullRoots := determineGloasEnvelopeRoots(responses, nil)
	require.Len(t, fetchRoots, 1)
	require.Empty(t, fullRoots)

	expected, err := blk.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expected, fetchRoots[0])
}

func TestDetermineGloasEnvelopeRootsNonChildCannotConfirmFull(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x00))
	nonChild := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	nonChild.Block.ParentRoot = hash(0xCC)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	_, fullRoots := determineGloasEnvelopeRoots([]*cltypes.SignedBeaconBlock{block, nonChild}, nil)

	assert.NotContains(t, fullRoots, [32]byte(blockRoot))
}

func TestDetermineGloasEnvelopeRootsFindsNonAdjacentDirectChild(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x00))
	unrelated := makeGloasBlock(101, hash(0xCC), hash(0xDD))
	child := makeGloasBlock(102, hash(0xBB), hash(0xAA))
	blockRoot := linkGloasChild(t, block, child)

	_, fullRoots := determineGloasEnvelopeRoots([]*cltypes.SignedBeaconBlock{block, unrelated, child}, nil)

	assert.Contains(t, fullRoots, [32]byte(blockRoot))
}

func TestBackwardEnvelopeLookaheadFollowsAcceptedChild(t *testing.T) {
	parent := makeGloasBlock(100, hash(0xAA), hash(0x00))
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	sideChild := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	sideChild.Block.ParentRoot = parentRoot
	acceptedChild := makeGloasBlock(101, hash(0xCC), hash(0xDD))
	acceptedChild.Block.ParentRoot = parentRoot
	acceptedChildRoot, err := acceptedChild.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    acceptedChildRoot,
		httpFallbackURL: "://invalid",
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			return false, nil
		},
	}
	downloader.SetInitialBlockEnvelopeDeferred(acceptedChildRoot)
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(acceptedChild.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{parent, sideChild, acceptedChild}))
	require.Empty(t, downloader.SkippedFullBlocks())
}

// TestDetermineGloasFullRoots_InBatch_Full verifies that a GLOAS block is identified as FULL
// when the next block's bid.ParentBlockHash matches this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Full(t *testing.T) {
	// blk0 is FULL: blk1.ParentBlockHash == blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xAA)) // ParentBlockHash = blk0.BlockHash
	linkGloasChild(t, blk0, blk1)
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	fetchRoots, roots := determineGloasEnvelopeRoots(responses, nil)
	require.Len(t, fetchRoots, 2)
	require.Len(t, roots, 1)

	root0, err := blk0.Block.HashSSZ()
	require.NoError(t, err)
	root1, err := blk1.Block.HashSSZ()
	require.NoError(t, err)
	assert.Contains(t, roots, root0)
	assert.NotContains(t, roots, root1)
}

// TestDetermineGloasFullRoots_InBatch_Empty verifies that a GLOAS block is identified as EMPTY
// when the next block's bid.ParentBlockHash does NOT match this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Empty(t *testing.T) {
	// blk0 is EMPTY: blk1.ParentBlockHash != blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xCC)) // ParentBlockHash != blk0.BlockHash
	linkGloasChild(t, blk0, blk1)
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	fetchRoots, roots := determineGloasEnvelopeRoots(responses, nil)
	require.Len(t, fetchRoots, 1)
	require.Empty(t, roots)

	root1, err := blk1.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, root1, fetchRoots[0])
}

// TestDetermineGloasFullRoots_CrossBatch_Full verifies the cross-batch lookahead:
// the highest block in a batch is confirmed FULL by prevBatchTopBlock.
func TestDetermineGloasFullRoots_CrossBatch_Full(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// prevBatchTopBlock is from the previous (higher-slot) batch; its ParentBlockHash = blk.BlockHash
	prevTop := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	linkGloasChild(t, blk, prevTop)
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
	linkGloasChild(t, blk, prevTop)
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, prevTop)
	assert.Empty(t, roots)
}

func TestDetermineGloasEnvelopeRootsCrossBatchNonChildCannotConfirmFull(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	previousTop := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	previousTop.Block.ParentRoot = hash(0xCC)

	_, fullRoots := determineGloasEnvelopeRootsWithDeferredRoot([]*cltypes.SignedBeaconBlock{block}, previousTop, blockRoot, nil)

	assert.NotContains(t, fullRoots, [32]byte(blockRoot))
}

func TestDetermineGloasEnvelopeRootsCrossBatchUsesAcceptedPreviousChild(t *testing.T) {
	parent := makeGloasBlock(100, hash(0xAA), hash(0x00))
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	sideChild := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	sideChild.Block.ParentRoot = parentRoot
	acceptedPreviousChild := makeGloasBlock(101, hash(0xCC), hash(0xDD))
	acceptedPreviousChild.Block.ParentRoot = parentRoot

	_, fullRoots := determineGloasEnvelopeRootsWithDeferredRoot([]*cltypes.SignedBeaconBlock{parent, sideChild}, acceptedPreviousChild, parentRoot, nil)

	assert.NotContains(t, fullRoots, [32]byte(parentRoot))
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
	linkGloasChild(t, blk0, blk1)
	linkGloasChild(t, blk1, blk2)
	linkGloasChild(t, blk2, blk3)
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1, blk2, blk3}

	fetchRoots, roots := determineGloasEnvelopeRoots(responses, nil)
	require.Len(t, fetchRoots, 3)
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
	linkGloasChild(t, gloasFull, lookahead)
	responses := []*cltypes.SignedBeaconBlock{deneb, gloasFull, lookahead}

	fetchRoots, roots := determineGloasEnvelopeRoots(responses, nil)
	require.Len(t, fetchRoots, 2)
	require.Len(t, roots, 1)

	rootFull, _ := gloasFull.Block.HashSSZ()
	rootLookahead, _ := lookahead.Block.HashSSZ()
	assert.Contains(t, roots, rootFull)
	assert.NotContains(t, roots, rootLookahead)
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

func TestBackwardRootFallbackConfirmedFullEnvelopeMissTransfersToRecovery(t *testing.T) {
	block := makeGloasBlock(7, hash(1), hash(2))
	child := makeGloasBlock(8, hash(3), hash(1))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	child.Block.ParentRoot = root
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/0x"+common.Bytes2Hex(root[:]) {
			w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			_, _ = w.Write(encoded)
			return
		}
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	defer server.Close()

	processed := 0
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		expectedRoot:      root,
		prevBatchTopBlock: child,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed++
			return false, nil
		},
	}
	downloader.slotToDownload.Store(block.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), nil))
	require.Equal(t, 1, processed)
	require.Equal(t, common.Hash(block.Block.ParentRoot), downloader.expectedRoot)
	require.Equal(t, block.Block.Slot-1, downloader.slotToDownload.Load())
	require.Equal(t, []SkippedFullBlock{{Block: block, Root: root}}, downloader.SkippedFullBlocks())
}

func TestBackwardRootFallbackDeferredAnchorAdvancesWithoutRecovery(t *testing.T) {
	block := makeGloasBlock(7, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	envelopeRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/0x"+common.Bytes2Hex(root[:]) {
			w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			_, _ = w.Write(encoded)
			return
		}
		envelopeRequests++
		http.NotFound(w, r)
	}))
	defer server.Close()

	processed := 0
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    root,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed++
			return false, nil
		},
	}
	downloader.SetInitialBlockEnvelopeDeferred(root)
	downloader.slotToDownload.Store(block.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), nil))
	require.Equal(t, 1, processed)
	require.Zero(t, envelopeRequests)
	require.Equal(t, common.Hash(block.Block.ParentRoot), downloader.expectedRoot)
	require.Empty(t, downloader.SkippedFullBlocks())
}

func TestBackwardRootFallbackUnseededEnvelopeFailureConservesProgress(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			block := makeGloasBlock(7, hash(1), hash(2))
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			encodedBlock, err := block.EncodeSSZ(nil)
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/eth/v2/beacon/blocks/0x"+common.Bytes2Hex(root[:]) {
					w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
					_, _ = w.Write(encodedBlock)
					return
				}
				http.Error(w, http.StatusText(status), status)
			}))
			defer server.Close()

			processed := 0
			downloader := &BackwardBeaconDownloader{
				httpFallbackURL: server.URL,
				beaconCfg:       &clparams.MainnetBeaconConfig,
				expectedRoot:    root,
				onNewBlock: func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
					require.Nil(t, envelope)
					processed++
					return false, nil
				},
			}
			downloader.slotToDownload.Store(block.Block.Slot)

			require.NoError(t, downloader.processResponses(context.Background(), nil))
			require.Zero(t, processed)
			require.Equal(t, common.Hash(root), downloader.expectedRoot)
			require.Empty(t, downloader.SkippedFullBlocks())
		})
	}
}

func TestBackwardBatchUnseededEnvelopeFailureConservesProgress(t *testing.T) {
	block := makeGloasBlock(7, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	defer server.Close()

	processed := 0
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    root,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed++
			return false, nil
		},
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(block.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(root), downloader.expectedRoot)
	require.Empty(t, downloader.SkippedFullBlocks())
}

func TestBackwardInitialAnchorDefersEnvelopeAndAdvances(t *testing.T) {
	anchor := makeGloasBlock(8, hash(2), hash(1))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)

	processed := 0
	downloader := &BackwardBeaconDownloader{
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    anchorRoot,
		httpFallbackURL: "://invalid",
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			require.Same(t, anchor, block)
			require.Nil(t, envelope)
			processed++
			return false, nil
		},
	}
	downloader.SetInitialBlockEnvelopeDeferred(anchorRoot)
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(anchor.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, processed)
	require.Equal(t, common.Hash(anchor.Block.ParentRoot), downloader.expectedRoot)
	require.Equal(t, anchor.Block.Slot-1, downloader.slotToDownload.Load())
	require.Empty(t, downloader.SkippedFullBlocks())
}

func TestBackwardInitialAnchorBecomesLookaheadForParent(t *testing.T) {
	parent := makeGloasBlock(7, hash(1), hash(0))
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	anchor := makeGloasBlock(8, hash(2), hash(1))
	anchor.Block.ParentRoot = parentRoot
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)

	processed := make([]common.Hash, 0, 2)
	downloader := &BackwardBeaconDownloader{
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    anchorRoot,
		httpFallbackURL: "://invalid",
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			require.Nil(t, envelope)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			processed = append(processed, root)
			return false, nil
		},
	}
	downloader.SetInitialBlockEnvelopeDeferred(anchorRoot)
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(anchor.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{parent, anchor}))
	require.Equal(t, []common.Hash{anchorRoot, parentRoot}, processed)
	require.Equal(t, []SkippedFullBlock{{Block: parent, Root: parentRoot}}, downloader.SkippedFullBlocks())
}

func TestBackwardConfirmedMissingEnvelopesTransferToBatchRecoveryInOneTraversal(t *testing.T) {
	grandparent := makeGloasBlock(7, hash(1), hash(2))
	parent := makeGloasBlock(8, hash(3), hash(1))
	child := makeGloasBlock(9, hash(4), hash(3))
	grandparentRoot, err := grandparent.Block.HashSSZ()
	require.NoError(t, err)
	parent.Block.ParentRoot = grandparentRoot
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	child.Block.ParentRoot = parentRoot
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)

	requests := map[string]int{}
	var requestsMu sync.Mutex
	envelopePaths := map[string]struct{}{}
	for _, root := range []common.Hash{childRoot, parentRoot, grandparentRoot} {
		envelopePaths["/eth/v1/beacon/execution_payload_envelope/0x"+common.Bytes2Hex(root[:])] = struct{}{}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsMu.Lock()
		requests[r.URL.Path]++
		requestsMu.Unlock()
		if _, ok := envelopePaths[r.URL.Path]; ok {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	defer server.Close()

	processed := make([]common.Hash, 0, 3)
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    childRoot,
		onNewBlock: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			require.Nil(t, envelope)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			processed = append(processed, root)
			return false, nil
		},
	}
	downloader.SetInitialBlockEnvelopeDeferred(childRoot)
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(child.Block.Slot)
	responses := []*cltypes.SignedBeaconBlock{grandparent, parent, child}

	require.NoError(t, downloader.processResponses(context.Background(), responses))
	require.Equal(t, []common.Hash{childRoot, parentRoot, grandparentRoot}, processed)
	require.Equal(t, common.Hash(grandparent.Block.ParentRoot), downloader.expectedRoot)
	require.Equal(t, []SkippedFullBlock{
		{Block: parent, Root: parentRoot},
		{Block: grandparent, Root: grandparentRoot},
	}, downloader.SkippedFullBlocks())
	for root, want := range map[common.Hash]int{childRoot: 0, parentRoot: 1, grandparentRoot: 1} {
		path := "/eth/v1/beacon/execution_payload_envelope/0x" + common.Bytes2Hex(root[:])
		requestsMu.Lock()
		count := requests[path]
		requestsMu.Unlock()
		require.Equal(t, want, count)
	}
}

func TestBackwardRestartZeroExecutionHashCannotSkip(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	beaconCfg := clparams.MainnetBeaconConfig
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().SupportInsertion().Return(true)
	downloader := &BackwardBeaconDownloader{
		engine:       engine,
		beaconCfg:    &beaconCfg,
		expectedRoot: hash(1),
	}

	require.False(t, downloader.canSkipSlot(context.Background(), tx, 0, 0, 1))
}

func TestBackwardExistingBlockSkipStopsAtGloasBoundary(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 2
	downloader := &BackwardBeaconDownloader{
		beaconCfg:    &beaconCfg,
		expectedRoot: hash(1),
	}

	require.True(t, downloader.canSkipSlot(context.Background(), tx, 0, 0, 63))
	require.False(t, downloader.canSkipSlot(context.Background(), tx, 0, 0, 64))
}

func TestBackwardBeaconDownloaderRejectsOversizedEnvelopeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, clparams.MaxChunkSize+1))
	}))
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}

	_, err := downloader.fetchSingleEnvelope(context.Background(), makeGloasBlock(1, hash(1), hash(2)))
	require.ErrorContains(t, err, "too large")
}

func TestForwardBeaconDownloaderRejectsOversizedEnvelopeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, clparams.MaxChunkSize+1))
	}))
	defer server.Close()
	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}

	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}

func TestEnvelopeHTTPFallbackRejectsMismatchedBlockRoot(t *testing.T) {
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = hash(0xff)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "block root")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}

func TestEnvelopeHTTPFallbackRejectsBidCommitmentMismatch(t *testing.T) {
	block := makeGloasBlock(9, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.ParentBeaconBlockRoot = block.Block.ParentRoot
	envelope.Message.Payload.SlotNumber = block.Block.Slot
	envelope.Message.Payload.BlockHash = hash(3)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "block hash")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}

func TestInvalidP2PEnvelopeDoesNotSuppressHealthyHTTPFallback(t *testing.T) {
	validP2PBlock := makeGloasBlock(9, hash(1), hash(2))
	validP2PEnvelope, validP2PRoot := makeGloasEnvelopeForBlock(t, validP2PBlock)
	httpBlock := makeGloasBlock(8, hash(3), hash(4))
	httpEnvelope, httpRoot := makeGloasEnvelopeForBlock(t, httpBlock)

	invalidP2PEnvelope := httpEnvelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	invalidP2PEnvelope.Message.Payload.GasUsed++
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
		validP2PRoot: validP2PEnvelope,
		httpRoot:     invalidP2PEnvelope,
	}
	filterEnvelopesByBlockCommitments(
		&clparams.MainnetBeaconConfig,
		received,
		[]*cltypes.SignedBeaconBlock{validP2PBlock, httpBlock},
	)
	require.Contains(t, received, validP2PRoot)
	require.NotContains(t, received, httpRoot)

	encoded, err := httpEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	httpRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpRequests++
		require.Contains(t, r.URL.Path, common.Bytes2Hex(httpRoot[:]))
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(),
		server.URL,
		[]*cltypes.SignedBeaconBlock{validP2PBlock, httpBlock},
		[][32]byte{validP2PRoot, httpRoot},
		received,
		&clparams.MainnetBeaconConfig,
	)
	require.Equal(t, 1, fetched)
	require.Equal(t, 1, httpRequests)
	require.Same(t, validP2PEnvelope, received[validP2PRoot])
	require.Equal(t, httpEnvelope.Message.Payload.BlockHash, received[httpRoot].Message.Payload.BlockHash)
}

func TestEnvelopeHTTPFallbackRejectsConfiguredRequestLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &cfg}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "builder deposits")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	require.Zero(t, fetchEnvelopesFromBeaconAPI(context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &cfg))
	require.Empty(t, received)
}

func TestEnvelopeHTTPFallbackFetchesByBlockRoot(t *testing.T) {
	block := makeGloasBlock(9, hash(1), hash(2))
	envelope, root := makeGloasEnvelopeForBlock(t, block)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	wantPath := "/eth/v1/beacon/execution_payload_envelope/0x" + common.Bytes2Hex(root[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	fetchedEnvelope, err := downloader.fetchSingleEnvelope(context.Background(), block)
	require.NoError(t, err)
	require.NotNil(t, fetchedEnvelope)

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	require.Equal(t, 1, fetchEnvelopesFromBeaconAPI(context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig))
	require.Contains(t, received, common.Hash(root))
}

func TestEnvelopeHTTPFallbackRejectsPreGloasVersion(t *testing.T) {
	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = root
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", clparams.FuluVersion.String())
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "consensus version")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}
