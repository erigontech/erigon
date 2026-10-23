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
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type invalidEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	response []byte
	calls    atomic.Int32
}

type blockingEnvelopeSentinel struct {
	sentinelproto.SentinelClient
}

func (s *invalidEnvelopeSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls.Add(1)
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: "envelope-peer"}}, nil
}

func (s *invalidEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func (s *blockingEnvelopeSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *blockingEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func makeFullGloasBlockAndEnvelope(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope, common.Hash) {
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

func TestForwardBeaconDownloaderProcessesTipEnvelopeAfterEmptyRange(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	for _, currentSlot := range []uint64{11, 12} {
		t.Run(fmt.Sprint(currentSlot), func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			cfg.InitializeForkSchedule()
			clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			tip, envelope, root := makeFullGloasBlockAndEnvelope(t, cfg, 10)
			envelopeBytes, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)
			var wire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, digest[:]...))
			sentinel := &invalidEnvelopeSentinel{response: wire.Bytes()}
			client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)
			downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
			downloader.SetHighestProcessedSlot(9)
			downloader.gloasLookahead = tip
			downloader.gloasLookaheadPeer = "tip-peer"
			downloader.gloasNextUnscanned = 11
			downloader.SetCurrentSlotSampler(func() uint64 { return currentSlot })
			var rangeCalls atomic.Int32
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return []*cltypes.SignedBeaconBlock{}, fmt.Sprintf("range-peer-%d", rangeCalls.Add(1)%2), nil
			}
			type processResult struct {
				blocks    []*cltypes.SignedBeaconBlock
				envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
			}
			processed := make(chan processResult, 1)
			downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				processed <- processResult{blocks: blocks, envelopes: envelopes}
				return tip.Block.Slot, nil
			})

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				downloader.RequestMore(ctx)
			}()
			select {
			case result := <-processed:
				cancel()
				<-done
				require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, result.blocks)
				require.Equal(t, envelopeBytes, mustEncodeEnvelope(t, result.envelopes[root]))
				require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
				require.True(t, downloader.CaughtUpTo(currentSlot))
			case <-time.After(time.Second):
				cancel()
				<-done
				t.Fatalf("tip was not processed; highest=%d caught_up=%t", downloader.GetHighestProcessedSlot(), downloader.CaughtUpTo(currentSlot))
			}
		})
	}
}

func TestForwardBeaconDownloaderRequiresDistinctPeersForEmptyTipRange(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTipTimeout := forwardTipEnvelopeTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardTipEnvelopeTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardTipEnvelopeTimeout = previousTipTimeout
	})

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	rpcClient, _ := newContextBlockingBeaconRPC(t.Context(), &immediateEnvelopeSentinel{empty: true})
	downloader := NewForwardBeaconDownloader(t.Context(), rpcClient, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	var useConfirmingPeer atomic.Bool
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		if useConfirmingPeer.Load() {
			return []*cltypes.SignedBeaconBlock{}, "peer-b", nil
		}
		return []*cltypes.SignedBeaconBlock{}, "peer-a", nil
	}
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	downloader.RequestMore(ctx)
	cancel()
	require.Zero(t, processCalls.Load())
	require.False(t, downloader.CaughtUpTo(12))
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
	start, pending := downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(11), start)

	useConfirmingPeer.Store(true)
	downloader.RequestMore(t.Context())
	require.Equal(t, int32(1), processCalls.Load())
	require.True(t, downloader.CaughtUpTo(12))
}

func TestForwardBeaconDownloaderRequiresDistinctPeersForEmptyFullRange(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTipTimeout := forwardTipEnvelopeTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardTipEnvelopeTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardTipEnvelopeTimeout = previousTipTimeout
	})

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	rpcClient, _ := newContextBlockingBeaconRPC(t.Context(), &immediateEnvelopeSentinel{empty: true})
	downloader := NewForwardBeaconDownloader(t.Context(), rpcClient, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 50 })
	var fullRangePeer, tipRangePeer atomic.Value
	fullRangePeer.Store("lagging-peer")
	tipRangePeer.Store("peer-b")
	var tipRangeCalls atomic.Int32
	downloader.requestBlocksByRange = func(_ context.Context, start, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		if start == 11 {
			return []*cltypes.SignedBeaconBlock{}, fullRangePeer.Load().(string), nil
		}
		if tipRangeCalls.Add(1)%2 == 0 {
			return []*cltypes.SignedBeaconBlock{}, "peer-c", nil
		}
		return []*cltypes.SignedBeaconBlock{}, tipRangePeer.Load().(string), nil
	}
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return blocks[len(blocks)-1].Block.Slot, nil
	})
	requestMore := func() {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		downloader.RequestMore(ctx)
	}

	requestMore()
	require.Zero(t, tipRangeCalls.Load())
	require.Zero(t, processCalls.Load())
	require.False(t, downloader.CaughtUpTo(50))
	start, pending := downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(11), start)

	fullRangePeer.Store("peer-a")
	tipRangePeer.Store("peer-c")
	requestMore()
	require.Zero(t, processCalls.Load())
	require.False(t, downloader.CaughtUpTo(50))
	start, pending = downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(44), start)

	tipRangePeer.Store("peer-b")
	downloader.RequestMore(t.Context())
	require.Equal(t, int32(1), processCalls.Load())
	require.True(t, downloader.CaughtUpTo(50))
}

func TestForwardBeaconDownloaderRequiresMatchingEmptyTipRanges(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTipTimeout := forwardTipEnvelopeTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardTipEnvelopeTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardTipEnvelopeTimeout = previousTipTimeout
	})

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	rpcClient, _ := newContextBlockingBeaconRPC(t.Context(), &immediateEnvelopeSentinel{empty: true})
	downloader := NewForwardBeaconDownloader(t.Context(), rpcClient, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	var currentSlot atomic.Uint64
	currentSlot.Store(11)
	downloader.SetCurrentSlotSampler(currentSlot.Load)
	var peerStage atomic.Int32
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		if peerStage.Load() == 1 {
			return []*cltypes.SignedBeaconBlock{}, "peer-b", nil
		}
		return []*cltypes.SignedBeaconBlock{}, "peer-a", nil
	}
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	downloader.RequestMore(ctx)
	cancel()
	require.Zero(t, processCalls.Load())

	currentSlot.Store(12)
	peerStage.Store(1)
	ctx, cancel = context.WithTimeout(t.Context(), 200*time.Millisecond)
	downloader.RequestMore(ctx)
	cancel()
	require.Zero(t, processCalls.Load())
	require.False(t, downloader.CaughtUpTo(12))

	peerStage.Store(2)
	downloader.RequestMore(t.Context())
	require.Equal(t, int32(1), processCalls.Load())
	require.True(t, downloader.CaughtUpTo(12))
}

func TestForwardBeaconDownloaderResetsEmptyTipPeerWhenLookaheadAdvances(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTipTimeout := forwardTipEnvelopeTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardTipEnvelopeTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardTipEnvelopeTimeout = previousTipTimeout
	})

	tip := makeGloasBlock(10, hash(0xaa), common.Hash{})
	nextTip := makeGloasBlock(11, hash(0xbb), common.Hash{})
	linkBeaconBlocks(t, tip, nextTip)
	rpcClient, _ := newContextBlockingBeaconRPC(t.Context(), &immediateEnvelopeSentinel{empty: true})
	downloader := NewForwardBeaconDownloader(t.Context(), rpcClient, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	var stage atomic.Int32
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		switch stage.Load() {
		case 1:
			return []*cltypes.SignedBeaconBlock{nextTip}, "block-peer", nil
		case 2:
			return []*cltypes.SignedBeaconBlock{}, "peer-b", nil
		case 3:
			return []*cltypes.SignedBeaconBlock{}, "peer-c", nil
		default:
			return []*cltypes.SignedBeaconBlock{}, "peer-a", nil
		}
	}
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return blocks[len(blocks)-1].Block.Slot, nil
	})
	requestMore := func() {
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		downloader.RequestMore(ctx)
	}

	requestMore()
	require.Zero(t, processCalls.Load())

	stage.Store(1)
	requestMore()
	require.Equal(t, int32(1), processCalls.Load())
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
	start, pending := downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(12), start)

	stage.Store(2)
	requestMore()
	require.Equal(t, int32(1), processCalls.Load())
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
	require.False(t, downloader.CaughtUpTo(12))

	stage.Store(3)
	requestMore()
	require.Equal(t, int32(2), processCalls.Load())
	require.Equal(t, uint64(11), downloader.GetHighestProcessedSlot())
	require.True(t, downloader.CaughtUpTo(12))
}

func TestForwardBeaconDownloaderProcessesTipWithoutAvailableEnvelope(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardTipEnvelopeTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardTipEnvelopeTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardTipEnvelopeTimeout = previousTimeout
	})

	cfg := gloasFromGenesisConfig()
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	tip, _, _ := makeFullGloasBlockAndEnvelope(t, cfg, 10)
	client := rpc.NewBeaconRpcP2P(t.Context(), &invalidEnvelopeSentinel{}, cfg, clock)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	var rangeCalls atomic.Int32
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, fmt.Sprintf("range-peer-%d", rangeCalls.Add(1)%2), nil
	}
	processed := make(chan map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, 1)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) == 0 {
			return highest, nil
		}
		processed <- envelopes
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	select {
	case envelopes := <-processed:
		cancel()
		<-done
		require.Empty(t, envelopes)
		require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
		require.True(t, downloader.CaughtUpTo(11))
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatalf("tip was not processed before its envelope budget; highest=%d", downloader.GetHighestProcessedSlot())
	}
}

func TestForwardBeaconDownloaderUsesHTTPForTipAfterP2PEnvelopeTimeout(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardTipEnvelopeTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardTipEnvelopeTimeout = time.Second
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardTipEnvelopeTimeout = previousTimeout
	})

	cfg := gloasFromGenesisConfig()
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	tip, envelope, root := makeFullGloasBlockAndEnvelope(t, cfg, 10)
	envelopeBytes, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	sentinel := &blockingEnvelopeSentinel{}
	client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)
	var httpCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpCalls.Add(1)
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(envelopeBytes)
	}))
	t.Cleanup(server.Close)

	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	downloader.SetHTTPFallbackURL(server.URL)
	var rangeCalls atomic.Int32
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, fmt.Sprintf("range-peer-%d", rangeCalls.Add(1)%2), nil
	}
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if envelopes[root] == nil {
			return highest, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())
	require.Equal(t, int32(1), httpCalls.Load())
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
	require.True(t, downloader.CaughtUpTo(11))
}

func TestForwardBeaconDownloaderHTTPPreferredProcessesTipAfterEmptyRange(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	tip, _, _ := makeFullGloasBlockAndEnvelope(t, cfg, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	downloader := NewForwardBeaconDownloader(t.Context(), nil, cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "http-fallback"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	processed := make(chan []*cltypes.SignedBeaconBlock, 1)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed <- blocks
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())
	select {
	case blocks := <-processed:
		require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
		require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
		require.True(t, downloader.CaughtUpTo(11))
	default:
		t.Fatalf("tip was not processed; highest=%d", downloader.GetHighestProcessedSlot())
	}
}

func TestForwardBeaconDownloaderHTTPFallbackProcessesTipAfterEmptyRange(t *testing.T) {
	previousFallbackDelay := forwardBeaconFallbackDelay
	forwardBeaconFallbackDelay = time.Millisecond
	t.Cleanup(func() { forwardBeaconFallbackDelay = previousFallbackDelay })

	cfg := gloasFromGenesisConfig()
	tip, _, _ := makeFullGloasBlockAndEnvelope(t, cfg, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	downloader := NewForwardBeaconDownloader(t.Context(), nil, cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "http-fallback"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.requestBlocksByRange = func(ctx context.Context, _, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	processed := make(chan []*cltypes.SignedBeaconBlock, 1)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed <- blocks
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	downloader.RequestMore(ctx)
	select {
	case blocks := <-processed:
		require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
		require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
		require.True(t, downloader.CaughtUpTo(11))
	default:
		t.Fatalf("tip was not processed; highest=%d", downloader.GetHighestProcessedSlot())
	}
}

func TestForwardBeaconDownloaderRetainsTipWhenProcessingDoesNotAdvance(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	forwardBeaconRequestInterval = time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestInterval = previousInterval })

	cfg := gloasFromGenesisConfig()
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
	digest, err := clock.ComputeForkDigest(0)
	require.NoError(t, err)
	tip, envelope, _ := makeFullGloasBlockAndEnvelope(t, cfg, 10)
	var wire bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, digest[:]...))
	client := rpc.NewBeaconRpcP2P(t.Context(), &invalidEnvelopeSentinel{response: wire.Bytes()}, cfg, clock)
	downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
	downloader.SetHighestProcessedSlot(9)
	downloader.gloasLookahead = tip
	downloader.gloasLookaheadPeer = "tip-peer"
	downloader.gloasNextUnscanned = 11
	downloader.SetCurrentSlotSampler(func() uint64 { return 11 })
	var rangeCalls atomic.Int32
	downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		return []*cltypes.SignedBeaconBlock{}, fmt.Sprintf("range-peer-%d", rangeCalls.Add(1)%2), nil
	}
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	start, pending := downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(11), start)
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
	require.False(t, downloader.CaughtUpTo(11))
}

func mustEncodeEnvelope(t *testing.T, envelope *cltypes.SignedExecutionPayloadEnvelope) []byte {
	t.Helper()
	require.NotNil(t, envelope)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	return encoded
}

func TestForwardInvalidEnvelopeUsesAvailableFallback(t *testing.T) {
	for _, httpAvailable := range []bool{false, true} {
		t.Run(fmt.Sprint(httpAvailable), func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			cfg.InitializeForkSchedule()
			clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			first := makeGloasBlock(10, common.Hash{}, common.Hash{})
			bid := first.Block.Body.SignedExecutionPayloadBid.Message
			bid.Slot = 10
			good := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			good.Message.Payload.SlotNumber = 10
			_, err = good.Message.Payload.EncodeSSZ(nil)
			require.NoError(t, err)
			bid.ExecutionRequestsRoot, err = good.Message.ExecutionRequests.HashSSZ()
			require.NoError(t, err)
			requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, good.Message.ExecutionRequests))
			good.Message.Payload.BlockHash = common.HexToHash("0xefd641f283ac9ad546ad48be6207db401e3f70fe94adc9a10d6c858d845b67e3")
			header, err := good.Message.Payload.RlpHeader(&good.Message.ParentBeaconBlockRoot, requestsHash, nil)
			require.NoError(t, err)
			good.Message.Payload.BlockHash = header.Hash()
			bid.BlockHash = header.Hash()
			root, err := first.Block.HashSSZ()
			require.NoError(t, err)
			good.Message.BeaconBlockRoot = root
			require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, first, good))
			goodBytes, err := good.EncodeSSZ(nil)
			require.NoError(t, err)
			bad := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			require.NoError(t, bad.DecodeSSZ(goodBytes, int(clparams.GloasVersion)))
			bad.Message.Payload.BlockHash = hash(0xff)
			require.ErrorContains(t, ValidateDownloadedGloasEnvelope(cfg, first, bad), "block hash mismatch")
			var wire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, bad, digest[:]...))
			sentinel := &invalidEnvelopeSentinel{response: wire.Bytes()}
			client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)
			second := makeGloasBlock(11, hash(2), bid.BlockHash)
			linkBeaconBlocks(t, first, second)
			var httpCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpCalls.Add(1)
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(goodBytes)
			}))
			defer server.Close()
			probe := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
			httpResult := fetchEnvelopesFromBeaconAPI(t.Context(), server.URL, []*cltypes.SignedBeaconBlock{first}, [][32]byte{root}, probe, cfg)
			require.Equal(t, 1, httpResult.fetched)
			require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, first, probe[root]))
			httpCalls.Store(0)
			downloader := NewForwardBeaconDownloader(t.Context(), client, cfg)
			downloader.SetHighestProcessedSlot(9)
			downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
			if httpAvailable {
				downloader.SetHTTPFallbackURL(server.URL)
			}
			downloader.requestBlocksByRange = func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
				return []*cltypes.SignedBeaconBlock{first, second}, "block-peer", nil
			}
			var rejected int
			downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
				if len(blocks) == 0 {
					return highest, nil
				}
				if err := ValidateDownloadedGloasEnvelope(cfg, first, envelopes[root]); err != nil {
					rejected++
					return highest, fmt.Errorf("%w: %w", ErrUnattributableProcess, err)
				}
				return first.Block.Slot, nil
			})
			for range 2 {
				downloader.RequestMore(t.Context())
			}
			t.Logf("P2P envelope calls=%d invalid callbacks=%d HTTP calls=%d highest=%d", sentinel.calls.Load(), rejected, httpCalls.Load(), downloader.GetHighestProcessedSlot())
			require.Equal(t, int32(2), sentinel.calls.Load())
			require.Zero(t, rejected)
			if httpAvailable {
				require.Equal(t, int32(2), httpCalls.Load())
				require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
			} else {
				require.Zero(t, httpCalls.Load())
				require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
			}
		})
	}
}
