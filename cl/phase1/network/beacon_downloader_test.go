package network

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestShouldBanIncompleteBlockResponse(t *testing.T) {
	require.False(t, shouldBanIncompleteBlockResponse("peer", 0, 0))
	require.True(t, shouldBanIncompleteBlockResponse("peer", 1, 0))
	require.False(t, shouldBanIncompleteBlockResponse("peer", 2, 1))
	require.False(t, shouldBanIncompleteBlockResponse("", 1, 0))
	require.False(t, shouldBanIncompleteBlockResponse("http-fallback", 1, 0))
}

func TestForwardBeaconDownloaderHTTPRetainsSingleGloasTipUntilLookahead(t *testing.T) {
	block := makeGloasBlock(100, hash(0xaa), common.Hash{})
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	child := makeGloasBlock(101, hash(0xbb), block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash)
	linkGloasBlocks(t, block, child)
	blockBytes, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	childBytes, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	var childAvailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/100":
			_, _ = w.Write(blockBytes)
		case "/eth/v2/beacon/blocks/101":
			if !childAvailable.Load() {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(childBytes)
		case "/eth/v1/beacon/execution_payload_envelope/" + common.Hash(blockRoot).Hex():
			_, _ = w.Write(envelopeBytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := NewForwardBeaconDownloader(t.Context(), nil, &clparams.MainnetBeaconConfig)
	downloader.SetHighestProcessedSlot(99)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	downloader.SetValidateFunction(func(blocks []*cltypes.SignedBeaconBlock) (int, error) { return len(blocks), nil })
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		require.Len(t, blocks, 1)
		require.Equal(t, uint64(100), blocks[0].Block.Slot)
		require.Contains(t, envelopes, common.Hash(blockRoot))
		return blocks[0].Block.Slot, nil
	})

	downloader.RequestMore(context.Background())
	require.Zero(t, processCalls.Load())
	require.Equal(t, uint64(99), downloader.GetHighestProcessedSlot())

	childAvailable.Store(true)
	downloader.RequestMore(context.Background())
	require.Equal(t, int32(1), processCalls.Load())
	require.Equal(t, uint64(100), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderRejectsUnauthenticatedLookaheadBeforeHTTPEnvelopeFetch(t *testing.T) {
	block := makeGloasBlock(100, hash(0xaa), common.Hash{})
	blockRoot, envelope := makeValidGloasEnvelope(t, block)
	child := makeGloasBlock(101, hash(0xbb), block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash)
	linkGloasBlocks(t, block, child)
	blockBytes, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	childBytes, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	var envelopeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/100":
			_, _ = w.Write(blockBytes)
		case "/eth/v2/beacon/blocks/101":
			_, _ = w.Write(childBytes)
		case "/eth/v1/beacon/execution_payload_envelope/" + common.Hash(blockRoot).Hex():
			envelopeRequests.Add(1)
			_, _ = w.Write(envelopeBytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := NewForwardBeaconDownloader(t.Context(), nil, &clparams.MainnetBeaconConfig)
	downloader.SetHighestProcessedSlot(99)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	downloader.SetValidateFunction(func([]*cltypes.SignedBeaconBlock) (int, error) {
		return 0, errors.New("invalid proposer signature")
	})
	downloader.SetProcessFunction(func(uint64, []*cltypes.SignedBeaconBlock, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		t.Fatal("rejected response must not be processed")
		return 0, nil
	})

	downloader.RequestMore(t.Context())

	require.Zero(t, envelopeRequests.Load())
	require.Equal(t, uint64(99), downloader.GetHighestProcessedSlot())
}

func TestFetchBlocksFromBeaconAPIRejectsNonCanonicalSSZ(t *testing.T) {
	block := makeGloasBlock(100, hash(0xaa), hash(0x01))
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	encoded = append(encoded, 0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		if r.URL.Path != "/eth/v2/beacon/blocks/100" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	_, err = fetchBlocksFromBeaconAPI(t.Context(), server.URL, 100, 1, &clparams.MainnetBeaconConfig)
	require.Error(t, err)
}
