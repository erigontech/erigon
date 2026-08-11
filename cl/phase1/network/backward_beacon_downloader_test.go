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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
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

func linkGloasBlocks(t *testing.T, parent, child *cltypes.SignedBeaconBlock) {
	root, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	child.Block.ParentRoot = root
}

func makeValidGloasEnvelope(t *testing.T, block *cltypes.SignedBeaconBlock) ([32]byte, *cltypes.SignedExecutionPayloadEnvelope) {
	requests := cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.ExecutionRequestsRoot = requestsRoot
	bid.Slot = block.Block.Slot
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.Payload.Extra = solid.NewExtraData()
	envelope.Message.Payload.Transactions = &solid.TransactionsSSZ{}
	envelope.Message.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	envelope.Message.Payload.BlockAccessList = solid.NewByteListSSZ(clparams.MainnetBeaconConfig.MaxBytesPerTransaction)
	envelope.Message.Payload.SlotNumber = bid.Slot
	envelope.Message.Payload.ParentHash = bid.ParentBlockHash
	envelope.Message.Payload.PrevRandao = bid.PrevRandao
	envelope.Message.Payload.FeeRecipient = bid.FeeRecipient
	envelope.Message.Payload.GasLimit = bid.GasLimit
	envelope.Message.BuilderIndex = bid.BuilderIndex
	envelope.Message.Payload.BlockHash = common.HexToHash("0x1da54a16ef5d8bd1d1559378bbdea3b084b58d1ff1e3db53c276a3ecd6c3ceb6")
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(&clparams.MainnetBeaconConfig, envelope.Message.ExecutionRequests))
	header, err := envelope.Message.Payload.RlpHeader(&envelope.Message.ParentBeaconBlockRoot, requestsHash)
	if err != nil {
		parts := strings.Fields(err.Error())
		require.Greater(t, len(parts), 6)
		envelope.Message.Payload.BlockHash = common.HexToHash(parts[6])
		header, err = envelope.Message.Payload.RlpHeader(&envelope.Message.ParentBeaconBlockRoot, requestsHash)
	}
	require.NoError(t, err)
	bid.BlockHash = header.Hash()
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = blockRoot
	require.NoError(t, ValidateFetchedEnvelope(&clparams.MainnetBeaconConfig, block, blockRoot, envelope))
	return blockRoot, envelope
}

func determineGloasFullRoots(responses []*cltypes.SignedBeaconBlock, lookahead *cltypes.SignedBeaconBlock) [][32]byte {
	anchor := lastCompleteBlock(responses)
	if anchor == nil {
		return nil
	}
	expectedRoot, err := anchor.Block.HashSSZ()
	if err != nil {
		return nil
	}
	fullRoots, _ := determineGloasAvailability(responses, lookahead, expectedRoot)
	return fullRoots
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
	linkGloasBlocks(t, blk0, blk1)
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
	linkGloasBlocks(t, blk0, blk1)
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
	linkGloasBlocks(t, blk, prevTop)
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
	linkGloasBlocks(t, blk, prevTop)
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
	linkGloasBlocks(t, blk0, blk1)
	linkGloasBlocks(t, blk1, blk2)
	linkGloasBlocks(t, blk2, blk3)
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
	linkGloasBlocks(t, gloasFull, lookahead)
	responses := []*cltypes.SignedBeaconBlock{deneb, gloasFull, lookahead}

	roots := determineGloasFullRoots(responses, nil)
	require.Len(t, roots, 1)

	rootFull, _ := gloasFull.Block.HashSSZ()
	assert.Contains(t, roots, rootFull)
}

func TestBackwardBeaconDownloaderFirstBatchHTTPChildCannotProveEmpty(t *testing.T) {
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
	assert.False(t, processed.Load())
	assert.Equal(t, int32(1), lookaheadRequests.Load())
	assert.True(t, downloader.prevBatchTopBlockUntrusted)
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
		validateLookahead: func(*cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock) error {
			return nil
		},
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

func TestBackwardBeaconDownloaderRejectsUnauthenticatedHTTPlookaheadBeforeEnvelopeFetch(t *testing.T) {
	anchor := makeGloasBlock(100, hash(0xAA), hash(0x10))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(102, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = anchorRoot
	encodedLookahead, err := lookahead.EncodeSSZ(nil)
	require.NoError(t, err)
	var envelopeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/102":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedLookahead)
		case "/eth/v1/beacon/execution_payload_envelope/" + common.Hash(anchorRoot).Hex():
			envelopeRequests.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var processed atomic.Bool
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    anchorRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		validateBlock:   func(*cltypes.SignedBeaconBlock) error { return nil },
		validateLookahead: func(*cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock) error {
			return errors.New("invalid lookahead proposer")
		},
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Store(true)
			return true, nil
		},
	}
	downloader.httpPreferred.Store(true)

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Zero(t, envelopeRequests.Load())
	require.False(t, processed.Load())
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

	linkGloasBlocks(t, block, lookahead)
	lookaheadRoot, err := lookahead.Block.HashSSZ()
	require.NoError(t, err)
	downloader.expectedRoot = lookaheadRoot
	envelopes, fullRoots, _ := downloader.fetchGloasEnvelopes(
		context.Background(),
		[]*cltypes.SignedBeaconBlock{block, lookahead},
	)

	assert.Empty(t, envelopes)
	require.Len(t, fullRoots, 1)
	assert.Equal(t, 1, downloader.consecutiveEnvelopeFailures)
}

func TestBackwardBeaconDownloaderRejectsInvalidRootedSignatureBeforeEnvelopeFetch(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = blockRoot

	var envelopeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/beacon/execution_payload_envelope/"+common.Hash(blockRoot).Hex() {
			envelopeRequests.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	var processed atomic.Bool
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: lookahead,
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		validateBlock: func(*cltypes.SignedBeaconBlock) error {
			return errors.New("invalid proposer signature")
		},
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Store(true)
			return true, nil
		},
	}
	downloader.httpPreferred.Store(true)

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{block}))
	require.Zero(t, envelopeRequests.Load())
	require.False(t, processed.Load())
}

func TestFetchGloasEnvelopesCanonicalEmptyRejectsLateEnvelope(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/beacon/execution_payload_envelope/"+common.Hash(blockRoot).Hex() {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	lookaheadRoot, err := lookahead.Block.HashSSZ()
	require.NoError(t, err)
	downloader.expectedRoot = lookaheadRoot
	downloader.httpPreferred.Store(true)

	envelopes, fullRoots, knownRoots := downloader.fetchGloasEnvelopes(
		context.Background(),
		[]*cltypes.SignedBeaconBlock{block, lookahead},
	)

	assert.NotContains(t, envelopes, common.Hash(blockRoot))
	assert.NotContains(t, fullRoots, common.Hash(blockRoot))
	assert.Contains(t, knownRoots, common.Hash(blockRoot))
}

func TestP2POnlyDownloadTracksLookaheadInferredEmptyBlock(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: lookahead,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		onNewBlock: func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			require.Nil(t, envelope)
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	require.True(t, processed)
	assert.Empty(t, downloader.skippedFullBlocks)
}

func TestUntrustedP2PLookaheadCannotProveEmpty(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:               blockRoot,
		prevBatchTopBlock:          lookahead,
		prevBatchTopBlockUntrusted: true,
		beaconCfg:                  &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	require.False(t, processed)
	require.False(t, downloader.Finished())
}

func TestUntrustedP2PLookaheadCannotProveFullWithoutEnvelope(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = blockRoot

	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		expectedRoot:               blockRoot,
		prevBatchTopBlock:          lookahead,
		prevBatchTopBlockUntrusted: true,
		httpFallbackURL:            server.URL,
		beaconCfg:                  &clparams.MainnetBeaconConfig,
	}
	downloader.httpPreferred.Store(true)

	envelopes, fullRoots, knownRoots := downloader.fetchGloasEnvelopes(context.Background(), []*cltypes.SignedBeaconBlock{block})
	require.Empty(t, envelopes)
	require.NotContains(t, fullRoots, common.Hash(blockRoot))
	require.NotContains(t, knownRoots, common.Hash(blockRoot))
}

func TestUntrustedP2PLookaheadCanProveFullWithMatchingEnvelope(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	lookahead := makeGloasBlock(101, hash(0xBB), block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash)
	lookahead.Block.ParentRoot = blockRoot
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(encoded)
	}))
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		expectedRoot:               blockRoot,
		prevBatchTopBlock:          lookahead,
		prevBatchTopBlockUntrusted: true,
		httpFallbackURL:            server.URL,
		beaconCfg:                  &clparams.MainnetBeaconConfig,
		validateLookahead: func(*cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock) error {
			return nil
		},
	}
	downloader.httpPreferred.Store(true)

	envelopes, fullRoots, knownRoots := downloader.fetchGloasEnvelopes(context.Background(), []*cltypes.SignedBeaconBlock{block})
	require.NoError(t, ValidateFetchedEnvelope(&clparams.MainnetBeaconConfig, block, common.Hash(blockRoot), envelopes[common.Hash(blockRoot)]))
	require.Contains(t, fullRoots, common.Hash(blockRoot))
	require.Contains(t, knownRoots, common.Hash(blockRoot))
}

func TestUntrustedP2PEmptyLookaheadEntersBoundedRecovery(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:               blockRoot,
		prevBatchTopBlock:          lookahead,
		prevBatchTopBlockUntrusted: true,
		beaconCfg:                  &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return true, nil
		},
	}

	for range 3 {
		require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	}
	require.True(t, processed)
	require.True(t, downloader.envelopesSkipped)
	require.Len(t, downloader.skippedFullBlocks, 1)
	lookaheadRoot, err := lookahead.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, lookahead.Block.Slot, downloader.skippedFullBlocks[0].ChildSlot)
	require.Equal(t, lookaheadRoot, downloader.skippedFullBlocks[0].ChildRoot)
}

func TestDirectFirstBatchP2PLookaheadRemainsUntrusted(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot: blockRoot,
		beaconCfg:    &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block, lookahead}))
	require.False(t, processed)
	require.True(t, downloader.prevBatchTopBlockUntrusted)
}

func TestUnavailableLookaheadEntersBoundedUnresolvedRecovery(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:                 blockRoot,
		beaconCfg:                    &clparams.MainnetBeaconConfig,
		consecutiveLookaheadFailures: 5,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(ctx, []*cltypes.SignedBeaconBlock{block}))
	require.True(t, processed)
	require.True(t, downloader.envelopesSkipped)
	require.Len(t, downloader.skippedFullBlocks, 1)
}

func TestCanonicalEmptyDoesNotDependOnEnvelopeSource(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot
	requests := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: lookahead,
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	require.True(t, processed)
	require.False(t, downloader.envelopesSkipped)
	require.Empty(t, downloader.skippedFullBlocks)
	require.Zero(t, requests.Load())
}

func TestFetchEnvelopesFromBeaconAPIUsesBlockRoot(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	require.Equal(t, 1, fetchEnvelopesFromBeaconAPI(
		context.Background(),
		server.URL,
		[]*cltypes.SignedBeaconBlock{block},
		[][32]byte{blockRoot},
		received,
		&clparams.MainnetBeaconConfig,
	))
	assert.Equal(t, "/eth/v1/beacon/execution_payload_envelope/"+common.Hash(blockRoot).Hex(), requestedPath)
}

func TestFetchGloasEnvelopesSkipsNetworkAfterFailureThreshold(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	linkGloasBlocks(t, block, lookahead)

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL:  server.URL,
		beaconCfg:        &clparams.MainnetBeaconConfig,
		envelopesSkipped: true,
	}
	lookaheadRoot, err := lookahead.Block.HashSSZ()
	require.NoError(t, err)
	downloader.expectedRoot = lookaheadRoot
	downloader.httpPreferred.Store(true)

	envelopes, fullRoots, _ := downloader.fetchGloasEnvelopes(
		context.Background(),
		[]*cltypes.SignedBeaconBlock{block, lookahead},
	)

	assert.Empty(t, envelopes)
	require.Len(t, fullRoots, 1)
	assert.Zero(t, requests.Load())
}

func TestPartialEnvelopeSuccessDoesNotResetDegradedModeFailures(t *testing.T) {
	downloader := &BackwardBeaconDownloader{consecutiveEnvelopeFailures: 2}

	downloader.recordEnvelopeFetchResult(2, 1)

	require.Equal(t, 3, downloader.consecutiveEnvelopeFailures)
	require.True(t, downloader.envelopesSkipped)
}

func TestCompleteEnvelopeSuccessResetsDegradedModeFailures(t *testing.T) {
	downloader := &BackwardBeaconDownloader{consecutiveEnvelopeFailures: 2, envelopesSkipped: true}

	downloader.recordEnvelopeFetchResult(2, 2)

	require.Zero(t, downloader.consecutiveEnvelopeFailures)
	require.False(t, downloader.envelopesSkipped)
}

func TestPartialEmptyProbeSuccessDoesNotResetExpectedRootFailure(t *testing.T) {
	downloader := &BackwardBeaconDownloader{consecutiveProbeFailures: 2}

	downloader.recordEmptyProbeResult(2, 1)

	require.Equal(t, uint8(3), downloader.consecutiveProbeFailures)
	require.True(t, downloader.envelopesSkipped)
}

func TestResolvedExpectedRootResetsProbeFailures(t *testing.T) {
	downloader := &BackwardBeaconDownloader{consecutiveProbeFailures: 2}

	downloader.recordEmptyProbeResult(1, 1)

	require.Zero(t, downloader.consecutiveProbeFailures)
}

func TestPartialEnvelopeSuccessStillReachesRecoveryForMissingExpectedRoot(t *testing.T) {
	older := makeGloasBlock(100, hash(0xAA), hash(0x10))
	olderRoot, olderEnvelope := makeValidGloasEnvelope(t, older)
	newer := makeGloasBlock(101, hash(0xBB), older.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash)
	newer.Block.ParentRoot = olderRoot
	newerRoot, err := newer.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(102, hash(0xCC), newer.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash)
	lookahead.Block.ParentRoot = newerRoot
	encodedOlder, err := olderEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v1/beacon/execution_payload_envelope/"+common.Hash(olderRoot).Hex() {
			_, _ = w.Write(encodedOlder)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	var processed atomic.Int32
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      newerRoot,
		prevBatchTopBlock: lookahead,
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Add(1)
			return false, nil
		},
	}
	downloader.httpPreferred.Store(true)

	for range 3 {
		require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{older, newer}))
	}

	require.True(t, downloader.envelopesSkipped)
	require.Equal(t, int32(2), processed.Load())
}

func TestResolvedEmptyProbeDoesNotResetUnknownExpectedRoot(t *testing.T) {
	older := makeGloasBlock(100, hash(0xAA), hash(0x10))
	olderRoot, err := older.Block.HashSSZ()
	require.NoError(t, err)
	newer := makeGloasBlock(101, hash(0xBB), hash(0x20))
	newer.Block.ParentRoot = olderRoot
	newerRoot, err := newer.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(102, hash(0xCC), hash(0x30))
	lookahead.Block.ParentRoot = newerRoot

	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	var processed atomic.Int32
	downloader := &BackwardBeaconDownloader{
		expectedRoot:                newerRoot,
		prevBatchTopBlock:           lookahead,
		prevBatchTopBlockUntrusted:  true,
		httpFallbackURL:             server.URL,
		beaconCfg:                   &clparams.MainnetBeaconConfig,
		consecutiveEnvelopeFailures: 0,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed.Add(1)
			return false, nil
		},
	}
	downloader.httpPreferred.Store(true)

	for range 3 {
		require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{older, newer}))
	}

	require.True(t, downloader.envelopesSkipped)
	require.Equal(t, int32(2), processed.Load())
}

func TestDegradedDownloadDoesNotTrackCanonicallyProvenEmptyBlock(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: lookahead,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		envelopesSkipped:  true,
		onNewBlock: func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			require.Nil(t, envelope)
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block}))
	require.Empty(t, downloader.skippedFullBlocks)
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

func TestGloasBlockAvailabilityRejectsUnlinkedLookahead(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = hash(0xFF)

	full, known := gloasBlockAvailability(block, lookahead)
	assert.False(t, full)
	assert.False(t, known)
}

func TestDetermineFullGloasRootsRequiresCanonicalChild(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	child := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	child.Block.ParentRoot = hash(0xFF)

	assert.Empty(t, determineFullGloasRoots([]*cltypes.SignedBeaconBlock{block, child}, 1))
	linkGloasBlocks(t, block, child)
	require.Len(t, determineFullGloasRoots([]*cltypes.SignedBeaconBlock{block, child}, 1), 1)

	child.Block.Slot = block.Block.Slot
	assert.Empty(t, determineFullGloasRoots([]*cltypes.SignedBeaconBlock{block, child}, 1))
}

func TestForwardGloasHelpersIgnoreIncompleteBlocks(t *testing.T) {
	incomplete := []*cltypes.SignedBeaconBlock{nil, {}}
	assert.NotPanics(t, func() {
		assert.False(t, anyGloasBlock(incomplete))
		assert.Empty(t, determineFullGloasRoots(incomplete, len(incomplete)))
	})
}

func TestCompleteBeaconBlocksFiltersBeforeForwardProcessing(t *testing.T) {
	valid := makeDenebBlock(100)
	blocks := []*cltypes.SignedBeaconBlock{nil, {}, {Block: &cltypes.BeaconBlock{}}, valid}

	got := completeBeaconBlocks(blocks)
	require.Len(t, got, 1)
	assert.Same(t, valid, got[0])
}

func TestFetchGloasLookaheadAdvancesPastFirstWindow(t *testing.T) {
	anchor := makeGloasBlock(100, hash(0xAA), hash(0x10))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(165, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = anchorRoot
	encoded, err := lookahead.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/165" {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encoded)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	_, err = downloader.fetchGloasLookahead(context.Background(), anchor, anchorRoot)
	require.Error(t, err)
	require.Equal(t, uint64(64), downloader.lookaheadSearchOffset)
	got, err := downloader.fetchGloasLookahead(context.Background(), anchor, anchorRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(165), got.Block.Slot)
}

func TestFetchGloasLookaheadRescansEarlierWindow(t *testing.T) {
	anchor := makeGloasBlock(100, hash(0xAA), hash(0x10))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	lookahead.Block.ParentRoot = anchorRoot
	encoded, err := lookahead.EncodeSSZ(nil)
	require.NoError(t, err)

	var available atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if available.Load() && r.URL.Path == "/eth/v2/beacon/blocks/101" {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encoded)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	_, err = downloader.fetchGloasLookahead(context.Background(), anchor, anchorRoot)
	require.Error(t, err)
	available.Store(true)
	_, err = downloader.fetchGloasLookahead(context.Background(), anchor, anchorRoot)
	require.Error(t, err)
	got, err := downloader.fetchGloasLookahead(context.Background(), anchor, anchorRoot)
	require.NoError(t, err)
	require.Equal(t, uint64(101), got.Block.Slot)
}

func TestLookaheadSearchRetriesTransientLaterWindowBeforeAdvancing(t *testing.T) {
	downloader := &BackwardBeaconDownloader{}

	downloader.advanceLookaheadSearch()
	require.Equal(t, gloasLookaheadWindow, downloader.lookaheadSearchOffset)
	downloader.advanceLookaheadSearch()
	require.True(t, downloader.lookaheadRescan)
	downloader.advanceLookaheadSearch()

	require.False(t, downloader.lookaheadRescan)
	require.Equal(t, gloasLookaheadWindow, downloader.lookaheadSearchOffset)
	downloader.advanceLookaheadSearch()
	require.Equal(t, 2*gloasLookaheadWindow, downloader.lookaheadSearchOffset)
}

func TestBlockByRootFindsExpectedBlockInMiddle(t *testing.T) {
	first := makeGloasBlock(100, hash(0x10), hash(0x00))
	expected := makeGloasBlock(101, hash(0x20), hash(0x10))
	last := makeGloasBlock(102, hash(0x30), hash(0x20))
	expectedRoot, err := expected.Block.HashSSZ()
	require.NoError(t, err)
	require.Same(t, expected, blockByRoot([]*cltypes.SignedBeaconBlock{first, expected, last}, expectedRoot))
}

func TestRootFallbackMissingEnvelopeEntersBoundedRecovery(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	child := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	linkGloasBlocks(t, block, child)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(blockRoot).Hex() {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encoded)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	processed := 0
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: child,
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		onNewBlock: func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed++
			assert.Nil(t, envelope)
			return false, nil
		},
	}

	for range 3 {
		require.NoError(t, downloader.processResponses(context.Background(), nil))
	}
	assert.Equal(t, 1, processed)
	require.Len(t, downloader.skippedFullBlocks, 1)
	assert.Equal(t, common.Hash(blockRoot), common.Hash(downloader.skippedFullBlocks[0].Root))
}

func TestRootFallbackCanonicalEmptyRejectsLateEnvelope(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	child := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	child.Block.ParentRoot = blockRoot
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedEnvelope, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/" + common.Hash(blockRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
		case "/eth/v1/beacon/execution_payload_envelope/" + common.Hash(blockRoot).Hex():
			_, _ = w.Write(encodedEnvelope)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var processedEnvelope *cltypes.SignedExecutionPayloadEnvelope
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: child,
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		onNewBlock: func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processedEnvelope = envelope
			return true, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), nil))
	require.Nil(t, processedEnvelope)
}

func TestRootFallbackLookaheadFailureEntersBoundedRecovery(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(blockRoot).Hex() {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:                 blockRoot,
		httpFallbackURL:              server.URL,
		beaconCfg:                    &clparams.MainnetBeaconConfig,
		consecutiveLookaheadFailures: maxLookaheadFailures - 1,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return false, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), nil))
	require.True(t, processed)
	require.True(t, downloader.envelopesSkipped)
	require.Len(t, downloader.skippedFullBlocks, 1)
}

func TestRootFallbackUntrustedP2PEmptyLookaheadEntersBoundedRecovery(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(blockRoot).Hex() {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:               blockRoot,
		prevBatchTopBlock:          lookahead,
		prevBatchTopBlockUntrusted: true,
		httpFallbackURL:            server.URL,
		beaconCfg:                  &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return false, nil
		},
	}

	for range 3 {
		require.NoError(t, downloader.processResponses(context.Background(), nil))
	}
	require.True(t, processed)
	require.True(t, downloader.envelopesSkipped)
	require.Len(t, downloader.skippedFullBlocks, 1)
}

func TestRootFallbackFailingProbeDoesNotTrackCanonicallyProvenEmptyBlock(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	lookahead.Block.ParentRoot = blockRoot
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/" + common.Hash(blockRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
		case "/eth/v1/beacon/execution_payload_envelope/" + common.Hash(blockRoot).Hex():
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	processed := false
	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: lookahead,
		httpFallbackURL:   server.URL,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed = true
			return false, nil
		},
	}

	require.NoError(t, downloader.processResponses(context.Background(), nil))
	require.True(t, processed)
	require.False(t, downloader.envelopesSkipped)
	require.Empty(t, downloader.skippedFullBlocks)
}

func TestValidateFetchedEnvelopeRejectsDifferentBeaconRoot(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	mutations := map[string]func(*cltypes.ExecutionPayloadEnvelope){
		"beacon root":        func(e *cltypes.ExecutionPayloadEnvelope) { e.BeaconBlockRoot[0]++ },
		"parent beacon root": func(e *cltypes.ExecutionPayloadEnvelope) { e.ParentBeaconBlockRoot[0]++ },
		"builder index":      func(e *cltypes.ExecutionPayloadEnvelope) { e.BuilderIndex++ },
		"parent hash":        func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.ParentHash[0]++ },
		"prev randao":        func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.PrevRandao[0]++ },
		"fee recipient":      func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.FeeRecipient[0]++ },
		"gas limit":          func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.GasLimit++ },
		"slot":               func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.SlotNumber++ },
		"payload contents":   func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.GasUsed++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
			mutate(changed.Message)
			require.Error(t, ValidateFetchedEnvelope(&clparams.MainnetBeaconConfig, block, blockRoot, changed))
		})
	}
}

func TestMalformedP2PEnvelopeFallsBackToValidHTTPEnvelope(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, valid := makeValidGloasEnvelope(t, block)
	encoded, err := valid.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/beacon/execution_payload_envelope/"+common.Hash(blockRoot).Hex() {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	malformed := valid.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	malformed.Message.Payload.GasUsed++
	got := validateAndFetchMissingEnvelopes(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{blockRoot},
		map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(blockRoot): malformed}, &clparams.MainnetBeaconConfig,
	)
	require.Len(t, got, 1)
	require.NoError(t, ValidateFetchedEnvelope(&clparams.MainnetBeaconConfig, block, common.Hash(blockRoot), got[common.Hash(blockRoot)]))
}

func TestMalformedHTTPRecoveryEnvelopeRemainsMissing(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, malformed := makeValidGloasEnvelope(t, block)
	malformed.Message.Payload.GasUsed++
	encoded, err := malformed.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	got := downloader.RecoverSkippedEnvelopes(
		context.Background(),
		[]SkippedFullBlock{{Slot: block.Block.Slot, Root: blockRoot}},
		map[common.Hash]*cltypes.SignedBeaconBlock{common.Hash(blockRoot): block},
	)
	require.Empty(t, got.Envelopes)
}

func TestHTTPRecoveryKeepsNotFoundUnresolved(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	got := downloader.RecoverSkippedEnvelopes(context.Background(), []SkippedFullBlock{{Slot: block.Block.Slot, Root: blockRoot}}, map[common.Hash]*cltypes.SignedBeaconBlock{common.Hash(blockRoot): block})

	require.Empty(t, got.Envelopes)
}

func TestHTTPRecoveryKeepsServerFailureUnresolved(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	got := downloader.RecoverSkippedEnvelopes(context.Background(), []SkippedFullBlock{{Slot: block.Block.Slot, Root: blockRoot}}, map[common.Hash]*cltypes.SignedBeaconBlock{common.Hash(blockRoot): block})

	require.Empty(t, got.Envelopes)
}

func TestHTTPRecoveryDoesNotConfirmEmptyWithoutCanonicalBlock(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &clparams.MainnetBeaconConfig}
	got := downloader.RecoverSkippedEnvelopes(context.Background(), []SkippedFullBlock{{Slot: block.Block.Slot, Root: blockRoot}}, nil)

	require.Empty(t, got.Envelopes)
}

func TestHTTPRecoveryUsesBlockRoot(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	got := downloader.RecoverSkippedEnvelopes(
		context.Background(),
		[]SkippedFullBlock{{Slot: block.Block.Slot, Root: blockRoot}},
		map[common.Hash]*cltypes.SignedBeaconBlock{common.Hash(blockRoot): block},
	)

	require.NoError(t, ValidateFetchedEnvelope(&clparams.MainnetBeaconConfig, block, common.Hash(blockRoot), got.Envelopes[common.Hash(blockRoot)]))
	assert.Equal(t, "/eth/v1/beacon/execution_payload_envelope/"+common.Hash(blockRoot).Hex(), requestedPath)
}

func TestValidateFetchedEnvelopesDropsMalformedSameRoot(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = blockRoot

	got := validateFetchedEnvelopes(&clparams.MainnetBeaconConfig, []*cltypes.SignedBeaconBlock{block}, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{blockRoot: envelope})
	assert.Empty(t, got)
}

func TestSkippedFullBlockMemoryBudget(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	downloader := &BackwardBeaconDownloader{skippedFullBlocks: make([]SkippedFullBlock, maxSkippedFullBlocks-1)}
	require.True(t, downloader.canTrackSkippedFullBlock(block))
	downloader.skippedFullBlocks = append(downloader.skippedFullBlocks, SkippedFullBlock{})
	require.False(t, downloader.canTrackSkippedFullBlock(block))
}

func TestBackwardBeaconDownloaderCapacityStopsInsteadOfWaitingForFinish(t *testing.T) {
	block := makeGloasBlock(100, hash(0xAA), hash(0x10))
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	linkGloasBlocks(t, block, lookahead)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      blockRoot,
		prevBatchTopBlock: lookahead,
		beaconCfg:         &clparams.MainnetBeaconConfig,
		envelopesSkipped:  true,
		skippedFullBlocks: make([]SkippedFullBlock, maxSkippedFullBlocks),
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			t.Fatal("block processing must stop before unresolved recovery state is lost")
			return false, nil
		},
	}

	err = downloader.processResponses(context.Background(), []*cltypes.SignedBeaconBlock{block})
	require.ErrorContains(t, err, "skipped envelope recovery capacity")
	require.False(t, downloader.Finished())
}

func TestBackwardBeaconDownloaderRestartDoesNotSkipUnresolvedGloasBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().SupportInsertion().Return(true)
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	downloader := &BackwardBeaconDownloader{
		engine:       engine,
		beaconCfg:    &beaconCfg,
		expectedRoot: hash(0x42),
	}

	require.False(t, downloader.canSkipSlot(context.Background(), tx, ^uint64(0), 0, 1))
}

func TestBackwardBeaconDownloaderRestartWithoutELDoesNotSkipUnresolvedGloasBlock(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	downloader := &BackwardBeaconDownloader{beaconCfg: &beaconCfg, expectedRoot: hash(0x42)}

	require.False(t, downloader.canSkipSlot(context.Background(), tx, ^uint64(0), 0, 1))
}

func TestTrySkipToExistingBlockRefreshesLookahead(t *testing.T) {
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	parent := makeGloasBlock(99, hash(0x10), hash(0x01))
	child := makeGloasBlock(100, hash(0x20), hash(0x10))
	linkGloasBlocks(t, parent, child)
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(context.Background(), tx, parent, false))
	require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(context.Background(), tx, child, false))
	require.NoError(t, beacon_indicies.WriteExecutionBlockHash(tx, childRoot, hash(0x20)))
	require.NoError(t, tx.Commit())

	downloader := &BackwardBeaconDownloader{
		ctx:            context.Background(),
		db:             db,
		beaconCfg:      &beaconCfg,
		expectedRoot:   childRoot,
		slotToDownload: atomic.Uint64{},
	}
	require.NoError(t, downloader.trySkipToExistingBlock(context.Background()))
	require.Equal(t, common.Hash(parentRoot), downloader.expectedRoot)
	require.NotNil(t, downloader.prevBatchTopBlock)
	gotRoot, err := downloader.prevBatchTopBlock.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(childRoot), common.Hash(gotRoot))
	require.False(t, downloader.prevBatchTopBlockUntrusted)
}

func TestReadBoundedBeaconAPIResponseRejectsOversize(t *testing.T) {
	_, err := readBoundedBeaconAPIResponse(bytes.NewReader(make([]byte, 9)), 8)
	require.Error(t, err)
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
