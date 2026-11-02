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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type noPeersSentinel struct {
	sentinelproto.SentinelClient
	calls atomic.Int32
}

func (s *noPeersSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls.Add(1)
	return nil, errors.New("no peers")
}

func (*noPeersSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func newHeldTipDownloader(t *testing.T, cfg *clparams.BeaconChainConfig, client *rpc.BeaconRpcP2P, tip *cltypes.SignedBeaconBlock, tipPeer string) *ForwardBeaconDownloader {
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(tip.Block.Slot - 1)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = tipPeer
	downloader.gloasNextUnscanned = tip.Block.Slot + 1
	return downloader
}

func newNoPeersRPC(t *testing.T, cfg *clparams.BeaconChainConfig) (*rpc.BeaconRpcP2P, *noPeersSentinel) {
	cfg.InitializeForkSchedule()
	sentinel := &noPeersSentinel{}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	return rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock), sentinel
}

func newEnvelopeServer(t *testing.T, root common.Hash, envelopeBytes []byte) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/eth/v1/beacon/execution_payload_envelopes/0x%x", root) {
			http.NotFound(w, r)
			return
		}
		if envelopeBytes == nil {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(envelopeBytes)
	}))
	t.Cleanup(server.Close)
	return server
}

func makeFullGloasTipAndEnvelope(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope, common.Hash) {
	t.Helper()
	block := makeGloasBlock(slot, common.Hash{}, common.Hash{})
	bid := block.Block.Body.SignedExecutionPayloadBid.Message
	bid.Slot = slot
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.Payload.SlotNumber = slot
	_, err := envelope.Message.Payload.EncodeSSZ(nil)
	require.NoError(t, err)
	bid.ExecutionRequestsRoot, err = envelope.Message.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, envelope.Message.ExecutionRequests))
	envelope.Message.Payload.BlockHash = common.HexToHash("0xefd641f283ac9ad546ad48be6207db401e3f70fe94adc9a10d6c858d845b67e3")
	header, err := envelope.Message.Payload.RlpHeader(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	envelope.Message.Payload.BlockHash = header.Hash()
	bid.BlockHash = header.Hash()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.Message.BeaconBlockRoot = root
	require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, block, envelope))
	return block, envelope, root
}

func requestMoreWithin(t *testing.T, downloader *ForwardBeaconDownloader) bool {
	t.Helper()
	done := make(chan bool, 1)
	go func() { done <- downloader.RequestMore(t.Context()) }()
	select {
	case atTip := <-done:
		return atTip
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not return")
		return false
	}
}

func TestForwardBeaconDownloaderReturnsAtScannedGloasTip(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	for _, currentSlot := range []uint64{11, 12, 50} {
		t.Run(fmt.Sprint(currentSlot), func(t *testing.T) {
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
			downloader.SetCurrentSlotSampler(func() uint64 { return currentSlot })
			var rangeCalls atomic.Int32
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return []*cltypes.SignedBeaconBlock{}, fmt.Sprintf("range-peer-%d", rangeCalls.Add(1)), nil
			}
			var processed atomic.Bool
			downloader.SetProcessFunction(func(uint64, []*cltypes.SignedBeaconBlock, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				processed.Store(true)
				return 10, nil
			})

			require.True(t, requestMoreWithin(t, downloader))
			require.False(t, processed.Load())
			require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
			require.Same(t, tip, downloader.gloasLookahead)
		})
	}
}

// The answer that brings the tip says nothing about the slots after it, so it does not report the tip.
func TestForwardBeaconDownloaderNeedsEmptyAnswerAfterTip(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	downloader := NewForwardBeaconDownloader(t.Context(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	response := []*cltypes.SignedBeaconBlock{tip}
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return response, "range-peer", nil
	}
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})

	require.False(t, requestMoreWithin(t, downloader))
	require.Same(t, tip, downloader.gloasLookahead)
	response = []*cltypes.SignedBeaconBlock{}
	require.True(t, requestMoreWithin(t, downloader))
}

// No completed slot follows a tip in the current slot, so there is nothing to ask about.
func TestForwardBeaconDownloaderReportsGloasTipInCurrentSlot(t *testing.T) {
	tip := makeGloasBlock(11, hash(0xaa), common.Hash{})
	downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		t.Error("requested a range after a tip in the current slot")
		return nil, "", errors.New("unexpected request")
	}

	require.True(t, requestMoreWithin(t, downloader))
}

// A tip that came from the beacon API gets its envelope from there, without waiting for P2P.
func TestForwardBeaconDownloaderImportsHeldGloasTipWithHTTPEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, envelope, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	client, sentinel := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "http-fallback")
	downloader.SetHTTPFallbackURL(newEnvelopeServer(t, root, envelopeBytes).URL)
	var processedBlocks []*cltypes.SignedBeaconBlock
	var processedEnvelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processedBlocks, processedEnvelopes = blocks, envelopes
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	importedRoot, imported := downloader.ImportHeldGloasTip(t.Context(), time.Second)
	require.True(t, imported)
	require.Equal(t, root, importedRoot)
	require.Zero(t, sentinel.calls.Load())
	require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, processedBlocks)
	require.NotNil(t, processedEnvelopes[root])
	processedEnvelopeBytes, err := processedEnvelopes[root].EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, envelopeBytes, processedEnvelopeBytes)
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderImportsHeldGloasTipWithoutEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	processed := 0
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed++
		require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
		require.Empty(t, envelopes)
		return 10, nil
	})

	_, imported := downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond)
	require.True(t, imported)
	require.Equal(t, 1, processed)
}

// P2P and the beacon API share one envelope budget.
func TestForwardBeaconDownloaderBoundsHeldGloasTipEnvelopeFetch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, _, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	downloader.SetHTTPFallbackURL(newEnvelopeServer(t, root, nil).URL)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	const budget = 200 * time.Millisecond
	start := time.Now()
	_, imported := downloader.ImportHeldGloasTip(t.Context(), budget)
	require.True(t, imported)
	require.Less(t, time.Since(start), budget+budget/2)
}

func TestForwardBeaconDownloaderRetriesHeldGloasTipInNextSlot(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	client, _ := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	var currentSlot atomic.Uint64
	currentSlot.Store(11)
	downloader.SetCurrentSlotSampler(currentSlot.Load)
	processed := 0
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed++
		return highest, nil
	})
	importTip := func() bool {
		_, imported := downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond)
		return imported
	}

	require.False(t, importTip())
	require.False(t, importTip())
	require.Equal(t, 1, processed)
	currentSlot.Store(12)
	require.False(t, importTip())
	require.Equal(t, 2, processed)
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
	require.Same(t, tip, downloader.gloasLookahead)
}

// A tip that processing rejected is dropped like a rejected batch: its peer is banned unless the error cannot be
// attributed to it.
func TestForwardBeaconDownloaderDropsRejectedHeldGloasTip(t *testing.T) {
	for name, test := range map[string]struct {
		err        error
		wantBanned []string
	}{
		"invalid block":    {err: errors.New("invalid block"), wantBanned: []string{"tip-peer"}},
		"invalid envelope": {err: fmt.Errorf("%w: invalid envelope", ErrUnattributableProcess)},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			client, _ := newNoPeersRPC(t, cfg)
			downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
			downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
			var banned []string
			downloader.banPeer = func(peer string) { banned = append(banned, peer) }
			downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				return highest, test.err
			})

			_, imported := downloader.ImportHeldGloasTip(t.Context(), 50*time.Millisecond)
			require.False(t, imported)
			require.Equal(t, test.wantBanned, banned)
			require.Nil(t, downloader.gloasLookahead)
			require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
		})
	}
}

// When P2P does not serve the tip's envelope, the beacon API still gets part of the budget.
func TestForwardBeaconDownloaderFetchesHeldGloasTipEnvelopeFromHTTPAfterP2P(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, envelope, root := makeFullGloasTipAndEnvelope(t, cfg, 10)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	client, sentinel := newNoPeersRPC(t, cfg)
	downloader := newHeldTipDownloader(t, cfg, client, tip, "tip-peer")
	downloader.SetHTTPFallbackURL(newEnvelopeServer(t, root, envelopeBytes).URL)
	var processedEnvelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processedEnvelopes = envelopes
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	_, imported := downloader.ImportHeldGloasTip(t.Context(), 400*time.Millisecond)
	require.True(t, imported)
	require.NotZero(t, sentinel.calls.Load())
	require.NotNil(t, processedEnvelopes[root])
}

// In HTTP-preferred mode every empty beacon API answer waits like a P2P retry, so a tip that cannot be imported, or a
// gap that is scanned again and again, does not flood the beacon API. At the current slot the answer reports the tip.
func TestForwardBeaconDownloaderHTTPPreferredPacesEmptyAnswers(t *testing.T) {
	for _, test := range []struct {
		currentSlot uint64
		wantAtTip   bool
	}{{currentSlot: 12, wantAtTip: true}, {currentSlot: 100}} {
		t.Run(fmt.Sprint(test.currentSlot), func(t *testing.T) {
			tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
			server := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(server.Close)
			downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "http-fallback")
			downloader.SetCurrentSlotSampler(func() uint64 { return test.currentSlot })
			downloader.SetHTTPFallbackURL(server.URL)
			downloader.httpPreferred.Store(true)

			start := time.Now()
			require.Equal(t, test.wantAtTip, requestMoreWithin(t, downloader))
			require.GreaterOrEqual(t, time.Since(start), forwardRequestRetryInterval)
			require.Greater(t, downloader.gloasNextUnscanned, uint64(11))
		})
	}
}

// The P2P requests fail, and the HTTP fallback answers empty for the slots after the tip.
func TestForwardBeaconDownloaderHTTPFallbackReportsGloasTip(t *testing.T) {
	previousInterval, previousDelay := forwardBeaconRequestInterval, forwardBeaconFallbackDelay
	forwardBeaconRequestInterval, forwardBeaconFallbackDelay = time.Millisecond, time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval, forwardBeaconFallbackDelay = previousInterval, previousDelay })

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	downloader := newHeldTipDownloader(t, gloasFromGenesisConfig(), nil, tip, "tip-peer")
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return nil, "", errors.New("no peers")
	}

	require.True(t, requestMoreWithin(t, downloader))
}

func TestForwardBeaconDownloaderReportsNoTipWithoutGloasLookahead(t *testing.T) {
	previousInterval, previousTimeout := forwardBeaconRequestInterval, forwardBeaconRequestTimeout
	forwardBeaconRequestInterval, forwardBeaconRequestTimeout = time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval, forwardBeaconRequestTimeout = previousInterval, previousTimeout })

	downloader := NewForwardBeaconDownloader(t.Context(), nil, &clparams.MainnetBeaconConfig)
	downloader.SetHighestProcessedSlot(9)
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, "range-peer", nil
	}

	require.False(t, requestMoreWithin(t, downloader))
}

// A beacon API batch asked through the current slot; its block can appear later, so the scan asks for that slot
// again even after the clock moved on.
func TestForwardBeaconDownloaderRescansSlotInProgressAtHTTPRequest(t *testing.T) {
	first := makeGloasBlock(9, hash(0x09), hash(0x01))
	tip := makeGloasBlock(10, hash(0x10), hash(0x02))
	successor := makeGloasBlock(12, hash(0x12), hash(0x03))
	linkBeaconBlocks(t, first, tip, successor)
	encoded := map[uint64][]byte{}
	for _, block := range []*cltypes.SignedBeaconBlock{first, tip, successor} {
		data, err := block.EncodeSSZ(nil)
		require.NoError(t, err)
		encoded[block.Block.Slot] = data
	}
	var successorPublished atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var slot uint64
		_, _ = fmt.Sscanf(r.URL.Path, "/eth/v2/beacon/blocks/%d", &slot)
		if data, ok := encoded[slot]; ok && (slot != successor.Block.Slot || successorPublished.Load()) {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)

	var currentSlot atomic.Uint64
	currentSlot.Store(12)
	downloader := NewForwardBeaconDownloader(t.Context(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(8)
	downloader.SetCurrentSlotSampler(currentSlot.Load)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	var processedSlots []uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedSlots = append(processedSlots, block.Block.Slot)
			highest = block.Block.Slot
		}
		currentSlot.Store(14)
		successorPublished.Store(true)
		return highest, nil
	})

	require.False(t, requestMoreWithin(t, downloader))
	require.Equal(t, tip.Block.Slot, downloader.gloasLookahead.Block.Slot)
	require.False(t, requestMoreWithin(t, downloader))
	require.Equal(t, []uint64{9, 10}, processedSlots)
	require.Equal(t, successor.Block.Slot, downloader.gloasLookahead.Block.Slot)
}
