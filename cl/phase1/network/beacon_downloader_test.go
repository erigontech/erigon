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
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func gloasFromGenesisConfig() *clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	return &cfg
}

func linkBeaconBlocks(t *testing.T, blocks ...*cltypes.SignedBeaconBlock) {
	t.Helper()
	for i := 1; i < len(blocks); i++ {
		root, err := blocks[i-1].Block.HashSSZ()
		require.NoError(t, err)
		blocks[i].Block.ParentRoot = root
	}
}

func TestForwardBeaconDownloaderEmptyGloasRangeStopsAtForkBoundary(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 10
	cfg.AltairForkEpoch = math.MaxUint64
	cfg.BellatrixForkEpoch = math.MaxUint64
	cfg.CapellaForkEpoch = math.MaxUint64
	cfg.DenebForkEpoch = math.MaxUint64
	cfg.ElectraForkEpoch = math.MaxUint64
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.GloasForkEpoch = 11

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 1)
	downloader := &ForwardBeaconDownloader{
		beaconCfg:          &cfg,
		gloasLookahead:     makeGloasBlock(98, hash(0xaa), common.Hash{}),
		gloasNextUnscanned: 98,
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			return []*cltypes.SignedBeaconBlock{}, "block-peer", nil
		},
	}
	downloader.SetHighestProcessedSlot(97)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	request := <-requests
	require.Eventually(t, func() bool {
		start, pending := downloader.nextRequestStart(false)
		return pending && start == 110
	}, time.Second, 10*time.Millisecond)
	cancel()
	<-done

	require.Equal(t, cltypes.BeaconBlocksByRangeRequest{StartSlot: 98, Count: 12}, request)
	require.Equal(t, uint64(97), downloader.GetHighestProcessedSlot())
}

type countingBlockingSentinel struct {
	sentinelproto.SentinelClient
	active    atomic.Int32
	maximum   atomic.Int32
	overLimit chan struct{}
	drained   chan struct{}
	overOnce  sync.Once
	drainOnce sync.Once
}

type rotatingProbeSentinel struct {
	sentinelproto.SentinelClient
	response   []byte
	calls      atomic.Int32
	active     atomic.Int32
	maximum    atomic.Int32
	canceled   chan struct{}
	cancelOnce sync.Once
}

type switchableProbeSentinel struct {
	sentinelproto.SentinelClient
	response []byte
	healthy  atomic.Bool
	calls    atomic.Int32
}

type emptyThenBlockSentinel struct {
	sentinelproto.SentinelClient
	response      []byte
	emptyReturned chan struct{}
	allowBlock    chan struct{}
	calls         atomic.Int32
}

func (s *emptyThenBlockSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if s.calls.Add(1) == 1 {
		close(s.emptyReturned)
		return &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "empty-peer"}}, nil
	}
	select {
	case <-s.allowBlock:
		return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: "block-peer"}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *switchableProbeSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls.Add(1)
	if !s.healthy.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: "healthy-peer"}}, nil
}

func (s *rotatingProbeSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	call := s.calls.Add(1)
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maximum.Load()
		if active <= maximum || s.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	if call <= 2 {
		<-ctx.Done()
		if call == 2 {
			s.cancelOnce.Do(func() { close(s.canceled) })
		}
		return nil, ctx.Err()
	}
	return &sentinelproto.ResponseData{
		Data: s.response,
		Peer: &sentinelproto.Peer{Pid: "healthy-peer"},
	}, nil
}

func newCountingBlockingSentinel() *countingBlockingSentinel {
	return &countingBlockingSentinel{
		overLimit: make(chan struct{}),
		drained:   make(chan struct{}),
	}
}

func (s *countingBlockingSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	active := s.active.Add(1)
	for {
		maximum := s.maximum.Load()
		if active <= maximum || s.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	if active > 2 {
		s.overOnce.Do(func() { close(s.overLimit) })
	}
	<-ctx.Done()
	if s.active.Add(-1) == 0 {
		s.drainOnce.Do(func() { close(s.drained) })
	}
	return nil, ctx.Err()
}

type contextBlockingSentinel struct {
	sentinelproto.SentinelClient
	started  chan struct{}
	canceled chan struct{}
	start    sync.Once
	stop     sync.Once
}

type emptyAfterFallbackStartsSentinel struct {
	sentinelproto.SentinelClient
	fallbackStarted <-chan struct{}
}

func (s *emptyAfterFallbackStartsSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	select {
	case <-s.fallbackStarted:
		return &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "empty-peer"}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newContextBlockingSentinel() *contextBlockingSentinel {
	return &contextBlockingSentinel{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
}

func (s *contextBlockingSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.start.Do(func() { close(s.started) })
	<-ctx.Done()
	s.stop.Do(func() { close(s.canceled) })
	return nil, ctx.Err()
}

func newContextBlockingBeaconRPC(ctx context.Context, sentinel sentinelproto.SentinelClient) (*rpc.BeaconRpcP2P, *clparams.BeaconChainConfig) {
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, &cfg)
	return rpc.NewBeaconRpcP2P(ctx, sentinel, &cfg, clock, nil), &cfg
}

func TestForwardRequestMoreCancelsOutstandingRequestsWhenBatchExpires(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newContextBlockingSentinel()
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, cfg)

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()

	select {
	case <-sentinel.started:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the Sentinel boundary")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not return after its batch expired")
	}

	select {
	case <-sentinel.canceled:
	case <-time.After(time.Second):
		t.Fatal("batch expiration returned without canceling the outstanding request")
	}
}

func TestForwardRequestMoreEmptyResponseKeepsUnknownRange(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = time.Second
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
	})

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, &cfg)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 11
	digest, err := clock.ComputeForkDigest(cfg.GenesisEpoch)
	require.NoError(t, err)
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, block, digest[:]...))

	sentinel := &emptyThenBlockSentinel{
		response:      response.Bytes(),
		emptyReturned: make(chan struct{}),
		allowBlock:    make(chan struct{}),
	}
	rpcClient, rpcCfg := newContextBlockingBeaconRPC(t.Context(), sentinel)
	downloader := NewForwardBeaconDownloader(t.Context(), rpcClient, rpcCfg)
	downloader.SetHighestProcessedSlot(10)
	processed := make(chan struct{}, 1)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed <- struct{}{}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(t.Context())
		close(done)
	}()
	<-sentinel.emptyReturned
	require.Eventually(t, func() bool { return sentinel.calls.Load() >= 3 }, time.Second, time.Millisecond)
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot())
	close(sentinel.allowBlock)
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("valid response for the unknown range was not processed")
	}
	<-done
	require.Equal(t, uint64(11), downloader.GetHighestProcessedSlot())
}

func TestForwardRequestMoreBoundsActiveProbes(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newCountingBlockingSentinel()
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, cfg)

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()

	select {
	case <-sentinel.overLimit:
		t.Fatal("forward downloader exceeded two active probes")
	case <-done:
	}
	select {
	case <-sentinel.drained:
	case <-time.After(time.Second):
		t.Fatal("active probes did not drain after the request window closed")
	}
	require.LessOrEqual(t, sentinel.maximum.Load(), int32(2))
	require.Zero(t, sentinel.active.Load())
}

func TestForwardRequestMoreRotatesSlowP2PProbes(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousProbeTimeout := forwardBeaconProbeTimeout
	previousResponsePoll := forwardBeaconResponsePoll
	forwardBeaconRequestInterval = 3 * time.Millisecond
	forwardBeaconRequestTimeout = 300 * time.Millisecond
	forwardBeaconProbeTimeout = 210 * time.Millisecond
	forwardBeaconResponsePoll = time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconProbeTimeout = previousProbeTimeout
		forwardBeaconResponsePoll = previousResponsePoll
	})

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, &cfg)
	digest, err := clock.ComputeForkDigest(cfg.GenesisEpoch)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 1
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, block, digest[:]...))

	sentinel := &rotatingProbeSentinel{response: response.Bytes(), canceled: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rpcClient := rpc.NewBeaconRpcP2P(ctx, sentinel, &cfg, clock, nil)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, &cfg)
	processed := make(chan int, 1)
	var processOnce sync.Once
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processOnce.Do(func() { processed <- len(blocks) })
		return highest, nil
	})

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()

	select {
	case blockCount := <-processed:
		require.Equal(t, 1, blockCount)
	case <-done:
		t.Fatal("request window ended before a healthy probe was processed")
	case <-time.After(time.Second):
		t.Fatal("healthy probe was not processed")
	}
	select {
	case <-sentinel.canceled:
	case <-time.After(time.Second):
		t.Fatal("slow probes were not canceled before the request window ended")
	}
	<-done
	require.GreaterOrEqual(t, sentinel.calls.Load(), int32(3))
	require.LessOrEqual(t, sentinel.maximum.Load(), int32(maxConcurrentForwardBeaconRequests))
	require.Zero(t, sentinel.active.Load())
}

func TestForwardRequestMoreStartsHTTPFallbackWhileProbesAreSlow(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousFallbackDelay := forwardBeaconFallbackDelay
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 200 * time.Millisecond
	forwardBeaconFallbackDelay = 20 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconFallbackDelay = previousFallbackDelay
	})

	fallbackStarted := make(chan struct{})
	var fallbackOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackOnce.Do(func() { close(fallbackStarted) })
		http.NotFound(w, nil)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newCountingBlockingSentinel()
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, cfg)
	downloader.SetHTTPFallbackURL(server.URL)

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()

	select {
	case <-fallbackStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("slow P2P probes prevented the HTTP fallback from starting")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish after its request window")
	}
}

func TestForwardRequestMoreDoesNotPreferHTTPWithoutProgress(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousFallbackDelay := forwardBeaconFallbackDelay
	previousResponsePoll := forwardBeaconResponsePoll
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	forwardBeaconFallbackDelay = 5 * time.Millisecond
	forwardBeaconResponsePoll = time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconFallbackDelay = previousFallbackDelay
		forwardBeaconResponsePoll = previousResponsePoll
	})

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, &cfg)
	digest, err := clock.ComputeForkDigest(cfg.GenesisEpoch)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 1
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	var p2pResponse bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&p2pResponse, block, digest[:]...))

	var httpCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		if r.URL.Path != "/eth/v2/beacon/blocks/1" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "phase0")
		_, _ = w.Write(encodedBlock)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := &switchableProbeSentinel{response: p2pResponse.Bytes()}
	rpcClient := rpc.NewBeaconRpcP2P(ctx, sentinel, &cfg, clock, nil)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, &cfg)
	downloader.SetHTTPFallbackURL(server.URL)
	var firstProcessCalls atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		firstProcessCalls.Add(1)
		return highest, nil
	})

	downloader.RequestMore(ctx)
	require.Positive(t, httpCalls.Load())
	require.Equal(t, int32(1), firstProcessCalls.Load(), "HTTP response did not reach processing")
	require.False(t, sentinel.healthy.Load())
	p2pCalls := sentinel.calls.Load()
	firstHTTPCalls := httpCalls.Load()

	sentinel.healthy.Store(true)
	downloader.SetProcessFunction(func(_ uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return blocks[len(blocks)-1].Block.Slot, nil
	})
	downloader.RequestMore(ctx)

	require.Greater(t, sentinel.calls.Load(), p2pCalls, "second request did not return to P2P")
	require.Equal(t, firstHTTPCalls, httpCalls.Load(), "second request incorrectly preferred HTTP")
	require.Equal(t, uint64(1), downloader.GetHighestProcessedSlot())
}

func TestForwardRequestMoreEmptyP2PDoesNotInvalidateHTTPFallback(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousFallbackDelay := forwardBeaconFallbackDelay
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	forwardBeaconFallbackDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconFallbackDelay = previousFallbackDelay
	})

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 11
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	fallbackStarted := make(chan struct{})
	releaseFallback := make(chan struct{})
	var fallbackOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFallback) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackOnce.Do(func() { close(fallbackStarted) })
		<-releaseFallback
		if r.URL.Path != "/eth/v2/beacon/blocks/11" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "phase0")
		_, _ = w.Write(encodedBlock)
	}))
	t.Cleanup(func() {
		release()
		server.Close()
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := &emptyAfterFallbackStartsSentinel{fallbackStarted: fallbackStarted}
	rpcClient, rpcCfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, rpcCfg)
	downloader.SetHighestProcessedSlot(10)
	downloader.SetHTTPFallbackURL(server.URL)
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return highest, nil
	})

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()
	select {
	case <-fallbackStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP fallback did not start")
	}
	require.Equal(t, uint64(10), downloader.GetHighestProcessedSlot(), "empty P2P response advanced the cursor")
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish")
	}
	require.Equal(t, int32(1), processCalls.Load(), "valid HTTP fallback was discarded after an empty P2P response")
}

func TestForwardRequestMoreBoundsPreferredHTTPFallback(t *testing.T) {
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestTimeout = 50 * time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestTimeout = previousTimeout })

	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newContextBlockingSentinel()
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, cfg)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("preferred HTTP fallback exceeded the request window")
	}
}

func TestForwardRequestMoreRejectsStalePreferredHTTPFallback(t *testing.T) {
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() { forwardBeaconRequestTimeout = previousTimeout })

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 11
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	fallbackStarted := make(chan struct{})
	releaseFallback := make(chan struct{})
	var fallbackOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackOnce.Do(func() { close(fallbackStarted) })
		<-releaseFallback
		if r.URL.Path != "/eth/v2/beacon/blocks/11" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "phase0")
		_, _ = w.Write(encodedBlock)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newContextBlockingSentinel()
	rpcClient, rpcCfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, rpcCfg)
	downloader.SetHighestProcessedSlot(10)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return highest, nil
	})

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()
	select {
	case <-fallbackStarted:
	case <-time.After(time.Second):
		t.Fatal("preferred HTTP fallback did not start")
	}
	downloader.SetHighestProcessedSlot(20)
	close(releaseFallback)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish")
	}
	require.Zero(t, processCalls.Load(), "stale preferred HTTP result was processed after the frontier advanced")
}

func TestForwardRequestMoreRejectsHTTPFallbackWhenFrontierAdvancesDuringFetch(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousFallbackDelay := forwardBeaconFallbackDelay
	previousResponsePoll := forwardBeaconResponsePoll
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 300 * time.Millisecond
	forwardBeaconFallbackDelay = 5 * time.Millisecond
	forwardBeaconResponsePoll = 100 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconFallbackDelay = previousFallbackDelay
		forwardBeaconResponsePoll = previousResponsePoll
	})

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = 11
	encodedBlock, err := block.EncodeSSZ(nil)
	require.NoError(t, err)

	requestsCompleted := make(chan struct{})
	var completedOnce sync.Once
	var completedRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if completedRequests.Add(1) == 42 {
				completedOnce.Do(func() { close(requestsCompleted) })
			}
		}()
		if r.URL.Path != "/eth/v2/beacon/blocks/11" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "phase0")
		_, _ = w.Write(encodedBlock)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newContextBlockingSentinel()
	rpcClient, rpcCfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, rpcCfg)
	downloader.SetHighestProcessedSlot(10)
	downloader.SetHTTPFallbackURL(server.URL)
	var processCalls atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processCalls.Add(1)
		return highest, nil
	})

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()
	select {
	case <-requestsCompleted:
	case <-time.After(time.Second):
		t.Fatal("HTTP fallback did not complete")
	}
	downloader.SetHighestProcessedSlot(20)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish")
	}
	require.Zero(t, processCalls.Load(), "HTTP response was not revalidated before processing")
}

func TestForwardRequestMoreDropsHTTPPreferenceWhenFrontierAdvancesDuringEnvelopeFetch(t *testing.T) {
	const testTimeout = 5 * time.Second

	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 2 * time.Second
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
	})

	cfg := *gloasFromGenesisConfig()
	first := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	first.Block.Slot = 11
	first.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{1}
	second := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	second.Block.Slot = 12
	second.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{1}
	linkBeaconBlocks(t, first, second)
	firstEncoded, err := first.EncodeSSZ(nil)
	require.NoError(t, err)
	secondEncoded, err := second.EncodeSSZ(nil)
	require.NoError(t, err)

	envelopeStarted := make(chan struct{})
	releaseEnvelope := make(chan struct{})
	var envelopeOnce sync.Once
	var blockSecondHTTP atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/11":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(firstEncoded)
		case r.URL.Path == "/eth/v2/beacon/blocks/12":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(secondEncoded)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			envelopeOnce.Do(func() { close(envelopeStarted) })
			<-releaseEnvelope
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			if r.URL.Path == "/eth/v2/beacon/blocks/21" && blockSecondHTTP.Load() {
				<-r.Context().Done()
				return
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newContextBlockingSentinel()
	rpcClient, _ := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(10)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)

	firstDone := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(firstDone)
	}()
	select {
	case <-envelopeStarted:
	case <-time.After(testTimeout):
		t.Fatal("preferred HTTP response did not start envelope fetch")
	}
	downloader.SetHighestProcessedSlot(20)
	close(releaseEnvelope)
	select {
	case <-firstDone:
	case <-time.After(testTimeout):
		t.Fatal("stale preferred HTTP request did not finish")
	}
	blockSecondHTTP.Store(true)

	secondDone := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(secondDone)
	}()
	select {
	case <-sentinel.started:
		cancel()
		<-secondDone
	case <-secondDone:
		t.Fatal("next request skipped P2P after the preferred HTTP result became stale")
	case <-time.After(testTimeout):
		t.Fatal("next request did not choose a transport")
	}
}

func TestForwardRequestMoreSynchronizesConcurrentProgressUpdates(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newCountingBlockingSentinel()
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, cfg)

	done := make(chan struct{})
	go func() {
		downloader.RequestMore(ctx)
		close(done)
	}()

	stopUpdates := make(chan struct{})
	updatesDone := make(chan struct{})
	go func() {
		defer close(updatesDone)
		for slot := uint64(1); ; slot++ {
			select {
			case <-stopUpdates:
				return
			default:
				downloader.SetHighestProcessedSlot(slot)
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish")
	}
	close(stopUpdates)
	<-updatesDone
	require.Positive(t, downloader.GetHighestProcessedSlot())
}

func TestShouldBanProcessPeer(t *testing.T) {
	processErr := errors.New("process failed")

	require.False(t, shouldBanProcessPeer("block-peer", fmt.Errorf("%w: %w", ErrUnattributableProcess, processErr)))
	require.True(t, shouldBanProcessPeer("block-peer", processErr))
	require.False(t, shouldBanProcessPeer("http-fallback", processErr))
}

func TestFetchBlocksFromBeaconAPIRejectsMissingConsensusVersion(t *testing.T) {
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	blocks, err := fetchBlocksFromBeaconAPI(t.Context(), server.URL, 10, 1, gloasFromGenesisConfig())
	require.Error(t, err)
	require.Empty(t, blocks)
}

func TestFetchBlocksFromBeaconAPIRejectsWrongSlot(t *testing.T) {
	block := makeGloasBlock(11, hash(0xaa), common.Hash{})
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	blocks, err := fetchBlocksFromBeaconAPI(t.Context(), server.URL, 10, 1, gloasFromGenesisConfig())
	require.Error(t, err)
	require.Empty(t, blocks)
}

func TestFetchBlocksFromBeaconAPIRejectsVersionOutsideRequestedSlot(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.FuluVersion)
	block.Block.Slot = 10
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "fulu")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	blocks, err := fetchBlocksFromBeaconAPI(t.Context(), server.URL, 10, 1, cfg)
	require.ErrorContains(t, err, "version mismatch")
	require.Empty(t, blocks)
}

func TestFetchBlocksFromBeaconAPIRejectsTrailingSSZData(t *testing.T) {
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	encoded = append(encoded, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	blocks, err := fetchBlocksFromBeaconAPI(t.Context(), server.URL, 10, 1, gloasFromGenesisConfig())
	require.ErrorContains(t, err, "decode")
	require.Empty(t, blocks)
}

func TestFetchBlocksFromBeaconAPIBoundsErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(bytes.Repeat([]byte{'x'}, int(clparams.MaxChunkSize)+1))
	}))
	defer server.Close()

	_, err := fetchBlocksFromBeaconAPI(t.Context(), server.URL, 10, 1, gloasFromGenesisConfig())
	require.ErrorContains(t, err, "exceeds")
}

func TestFetchEnvelopesFromBeaconAPIUsesRootAndRejectsIdentityMismatch(t *testing.T) {
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

	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	fetched := fetchEnvelopesFromBeaconAPI(t.Context(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{blockRoot}, received, cfg)
	require.Zero(t, fetched)
	require.Empty(t, received)
	require.Equal(t, "/eth/v1/beacon/execution_payload_envelope/0x"+common.Hash(blockRoot).Hex()[2:], <-requestedPath)
}

func TestForwardBeaconDownloaderHTTPFailureWithoutP2PReturnsSafely(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	downloader := NewForwardBeaconDownloader(t.Context(), nil, gloasFromGenesisConfig())
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	downloader.SetHighestProcessedSlot(9)
	downloader.SetCurrentSlotSampler(func() uint64 { return 10 })

	require.NotPanics(t, func() { downloader.RequestMore(t.Context()) })
}

func TestForwardBeaconDownloaderRetainsFrontierWhenHTTPFullEnvelopeIsMissing(t *testing.T) {
	first := makeGloasBlock(10, hash(0xaa), common.Hash{})
	lookahead := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, first, lookahead)
	firstEncoded, err := first.EncodeSSZ(nil)
	require.NoError(t, err)
	lookaheadEncoded, err := lookahead.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch {
		case strings.HasSuffix(r.URL.Path, "/beacon/blocks/10"):
			_, _ = w.Write(firstEncoded)
		case strings.HasSuffix(r.URL.Path, "/beacon/blocks/11"):
			_, _ = w.Write(lookaheadEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := NewForwardBeaconDownloader(context.Background(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	processed := 0
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed += len(blocks)
		if len(blocks) == 0 {
			return highest, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())

	require.Zero(t, processed)
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderRetainsSingleHTTPGloasBlockUntilLookahead(t *testing.T) {
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/beacon/blocks/10") {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encoded)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	downloader := NewForwardBeaconDownloader(context.Background(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(9)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	processed := 0
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed += len(blocks)
		if len(blocks) == 0 {
			return highest, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())

	require.Zero(t, processed)
	require.Equal(t, uint64(9), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderScansPastGloasLookaheadGap(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xaa), common.Hash{})
	successor := makeGloasBlock(frontier+66, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, first, successor)

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 3)
	allowSuccessor := make(chan struct{})
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			switch requestCount.Add(1) {
			case 1:
				return []*cltypes.SignedBeaconBlock{first}, "block-peer", nil
			case 2:
				return []*cltypes.SignedBeaconBlock{}, "block-peer", nil
			case 3:
				<-allowSuccessor
				return []*cltypes.SignedBeaconBlock{successor}, "block-peer", nil
			default:
				return nil, "block-peer", errors.New("unexpected request")
			}
		},
	}
	downloader.SetCurrentSlotSampler(func() uint64 { return frontier + 66 })
	downloader.SetHighestProcessedSlot(frontier)
	var processedSlots []uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedSlots = append(processedSlots, block.Block.Slot)
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	firstRequest := <-requests
	require.Equal(t, cltypes.BeaconBlocksByRangeRequest{StartSlot: frontier - 2, Count: 33}, firstRequest)
	require.Empty(t, processedSlots)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(t.Context())
	}()
	emptyRequest := <-requests
	require.Equal(t, cltypes.BeaconBlocksByRangeRequest{StartSlot: frontier + 2, Count: 33}, emptyRequest)
	farRequest := <-requests
	require.Equal(t, cltypes.BeaconBlocksByRangeRequest{StartSlot: frontier + 35, Count: 32}, farRequest)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())
	close(allowSuccessor)
	<-done
	require.Equal(t, []uint64{frontier + 1}, processedSlots)
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderRechecksNewSlotAfterNearHeadLookahead(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xaa), common.Hash{})
	successor := makeGloasBlock(frontier+2, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, first, successor)

	var currentSlot atomic.Uint64
	currentSlot.Store(frontier + 1)
	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			switch requestCount.Add(1) {
			case 1:
				return []*cltypes.SignedBeaconBlock{first}, "block-peer", nil
			case 2:
				return []*cltypes.SignedBeaconBlock{successor}, "block-peer", nil
			default:
				return nil, "block-peer", errors.New("unexpected request")
			}
		},
	}
	downloader.SetCurrentSlotSampler(currentSlot.Load)
	downloader.SetHighestProcessedSlot(frontier)
	var processedSlots []uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedSlots = append(processedSlots, block.Block.Slot)
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	require.Equal(t, cltypes.BeaconBlocksByRangeRequest{StartSlot: frontier - 2, Count: 4}, <-requests)
	require.Empty(t, processedSlots)
	downloader.RequestMore(t.Context())
	select {
	case request := <-requests:
		t.Fatalf("requested future range before the slot existed: %+v", request)
	default:
	}

	currentSlot.Store(frontier + 2)
	downloader.RequestMore(t.Context())
	require.Equal(t, cltypes.BeaconBlocksByRangeRequest{StartSlot: frontier + 2, Count: 1}, <-requests)
	require.Equal(t, []uint64{frontier + 1}, processedSlots)
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderPacesUnavailableNextSlot(t *testing.T) {
	const frontier = uint64(100)
	ctx, cancel := context.WithCancel(t.Context())
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			t.Fatal("requested a range whose first slot does not exist")
			return nil, "", nil
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	downloader.gloasLookahead = makeGloasBlock(frontier+1, hash(0xaa), common.Hash{})
	downloader.gloasNextUnscanned = frontier + 2
	downloader.SetCurrentSlotSampler(func() uint64 { return frontier + 1 })

	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	select {
	case <-done:
		t.Fatal("unavailable next slot returned without pacing")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	<-done
}

func TestForwardBeaconDownloaderDoesNotSendZeroCountAfterSlotSampleMovesBack(t *testing.T) {
	const frontier = uint64(100)
	var samples atomic.Int32
	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 1)
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			return nil, "peer-a", nil
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetCurrentSlotSampler(func() uint64 {
		if samples.Add(1) == 1 {
			return frontier
		}
		return frontier - 3
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	select {
	case request := <-requests:
		cancel()
		<-done
		require.NotZero(t, request.Count)
	case <-time.After(400 * time.Millisecond):
		cancel()
		<-done
	}
}

func TestForwardBeaconDownloaderHTTPScansPastGloasLookaheadGap(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xaa), common.Hash{})
	successor := makeGloasBlock(frontier+45, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, first, successor)
	firstEncoded, err := first.EncodeSSZ(nil)
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)

	requestedSlots := make(chan uint64, 84)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var slot uint64
		_, _ = fmt.Sscanf(r.URL.Path, "/eth/v2/beacon/blocks/%d", &slot)
		requestedSlots <- slot
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch slot {
		case first.Block.Slot:
			_, _ = w.Write(firstEncoded)
		case successor.Block.Slot:
			_, _ = w.Write(successorEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := NewForwardBeaconDownloader(context.Background(), nil, gloasFromGenesisConfig())
	downloader.SetCurrentSlotSampler(func() uint64 { return frontier + 45 })
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	var processedSlots []uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedSlots = append(processedSlots, block.Block.Slot)
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	firstRange := make([]uint64, 42)
	for i := range firstRange {
		firstRange[i] = <-requestedSlots
	}
	require.Contains(t, firstRange, frontier+1)
	require.Contains(t, firstRange, frontier+42)
	require.Empty(t, processedSlots)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	downloader.RequestMore(t.Context())
	secondRange := make([]uint64, 3)
	for i := range secondRange {
		secondRange[i] = <-requestedSlots
	}
	require.Contains(t, secondRange, frontier+43)
	require.Contains(t, secondRange, frontier+45)
	require.NotContains(t, secondRange, frontier+1)
	require.Equal(t, []uint64{frontier + 1}, processedSlots)
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderRetainsGloasScanAcrossDuplicateAndCancellation(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xaa), common.Hash{})
	successor := makeGloasBlock(frontier+66, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, first, successor)
	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 4)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			switch requestCount.Add(1) {
			case 1:
				return []*cltypes.SignedBeaconBlock{first}, "block-peer", nil
			case 2:
				return []*cltypes.SignedBeaconBlock{first}, "block-peer", nil
			case 3:
				<-ctx.Done()
				return nil, "block-peer", ctx.Err()
			case 4:
				return []*cltypes.SignedBeaconBlock{successor}, "block-peer", nil
			default:
				return nil, "block-peer", errors.New("unexpected request")
			}
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	var processedSlots []uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			processedSlots = append(processedSlots, block.Block.Slot)
			highest = block.Block.Slot
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	require.Equal(t, frontier-2, (<-requests).StartSlot)
	downloader.RequestMore(t.Context())
	require.Equal(t, frontier+2, (<-requests).StartSlot)
	require.Empty(t, processedSlots)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	cancelCtx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(cancelCtx)
	}()
	canceledRequest := <-requests
	cancel()
	<-done
	require.Equal(t, frontier+2, canceledRequest.StartSlot)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	downloader.RequestMore(t.Context())
	retryRequest := <-requests
	require.Equal(t, canceledRequest, retryRequest)
	require.Equal(t, []uint64{frontier + 1}, processedSlots)
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderPendingGloasScanDoesNotWrapAtMaxSlot(t *testing.T) {
	frontier := uint64(math.MaxUint64 - 1)
	last := makeGloasBlock(math.MaxUint64, hash(0xaa), common.Hash{})
	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			if requestCount.Add(1) == 1 {
				return []*cltypes.SignedBeaconBlock{last}, "block-peer", nil
			}
			<-ctx.Done()
			return nil, "block-peer", ctx.Err()
		},
	}
	downloader.SetCurrentSlotSampler(func() uint64 { return math.MaxUint64 })
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	firstRequest := <-requests
	require.Equal(t, frontier-2, firstRequest.StartSlot)
	require.Equal(t, uint64(4), firstRequest.Count)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	secondRequest := <-requests
	cancel()
	<-done
	require.Equal(t, uint64(math.MaxUint64), secondRequest.StartSlot)
	require.Equal(t, uint64(1), secondRequest.Count)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderRetainsUnprocessedGloasSuffix(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	second := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	third := makeGloasBlock(frontier+3, hash(0xa3), hash(0xb2))
	fourth := makeGloasBlock(frontier+4, hash(0xa4), hash(0xb3))
	linkBeaconBlocks(t, first, second, third, fourth)
	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			if requestCount.Add(1) == 1 {
				return []*cltypes.SignedBeaconBlock{first, second, third}, "block-peer", nil
			}
			return []*cltypes.SignedBeaconBlock{third, fourth}, "block-peer", nil
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	var processedBatches [][]uint64
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		batch := make([]uint64, len(blocks))
		for i, block := range blocks {
			batch[i] = block.Block.Slot
		}
		processedBatches = append(processedBatches, batch)
		if len(processedBatches) == 1 {
			return first.Block.Slot, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())
	<-requests
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())
	require.Equal(t, [][]uint64{{frontier + 1, frontier + 2}}, processedBatches)

	downloader.RequestMore(t.Context())
	secondRequest := <-requests
	require.Equal(t, frontier+3, secondRequest.StartSlot)
	require.Equal(t, [][]uint64{{frontier + 1, frontier + 2}, {frontier + 2, frontier + 3}}, processedBatches)
	require.Equal(t, frontier+3, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderPartialGloasRangeResumesAfterReturnedLookahead(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	lookahead := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	linkBeaconBlocks(t, first, lookahead)

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			if start == frontier-2 {
				return []*cltypes.SignedBeaconBlock{first, lookahead}, "peer-a", nil
			}
			<-ctx.Done()
			return nil, "peer-b", ctx.Err()
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) > 0 {
			return blocks[len(blocks)-1].Block.Slot, nil
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	require.Equal(t, frontier-2, (<-requests).StartSlot)
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	next := <-requests
	cancel()
	<-done
	require.Equal(t, frontier+3, next.StartSlot)
}

func TestForwardBeaconDownloaderMixedOwnerProcessErrorRestartsOverlap(t *testing.T) {
	const frontier = uint64(100)
	cached := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	successor := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	cachedRoot, err := cached.Block.HashSSZ()
	require.NoError(t, err)
	successor.Block.ParentRoot = cachedRoot

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 3)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			switch requestCount.Add(1) {
			case 1:
				return []*cltypes.SignedBeaconBlock{cached}, "peer-a", nil
			case 2:
				return []*cltypes.SignedBeaconBlock{successor}, "peer-b", nil
			default:
				<-ctx.Done()
				return nil, "peer-c", ctx.Err()
			}
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) == 0 {
			return highest, nil
		}
		return highest, errors.New("cached block failed")
	})

	downloader.RequestMore(t.Context())
	<-requests
	downloader.RequestMore(t.Context())
	<-requests
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	retry := <-requests
	cancel()
	<-done
	require.Equal(t, frontier-2, retry.StartSlot)
}

func TestForwardBeaconDownloaderSingleOwnerProcessErrorRestartsOverlap(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	successor := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	firstRoot, err := first.Block.HashSSZ()
	require.NoError(t, err)
	successor.Block.ParentRoot = firstRoot

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			if requestCount.Add(1) == 1 {
				return []*cltypes.SignedBeaconBlock{first, successor}, "peer-a", nil
			}
			<-ctx.Done()
			return nil, "peer-b", ctx.Err()
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) == 0 {
			return highest, nil
		}
		return highest, fmt.Errorf("%w: state gap", ErrUnattributableProcess)
	})

	downloader.RequestMore(t.Context())
	<-requests
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	retry := <-requests
	cancel()
	<-done
	require.Equal(t, frontier-2, retry.StartSlot)
}

func TestForwardBeaconDownloaderNoProgressRestartsOverlap(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	successor := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	firstRoot, err := first.Block.HashSSZ()
	require.NoError(t, err)
	successor.Block.ParentRoot = firstRoot

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(_ context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			return []*cltypes.SignedBeaconBlock{first, successor}, fmt.Sprintf("peer-%d", requestCount.Add(1)), nil
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	var processCount atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) == 0 || processCount.Add(1) == 1 {
			return highest, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())
	firstRequest := <-requests
	require.Equal(t, frontier-2, firstRequest.StartSlot)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	downloader.RequestMore(t.Context())
	secondRequest := <-requests
	require.Equal(t, frontier-2, secondRequest.StartSlot)
	require.Equal(t, frontier+1, downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderOverlapOnlyProgressRetainsLookahead(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier-2, hash(0xa1), common.Hash{})
	second := makeGloasBlock(frontier-1, hash(0xa2), hash(0xb1))
	third := makeGloasBlock(frontier, hash(0xa3), hash(0xb2))
	lookahead := makeGloasBlock(frontier+1, hash(0xa4), hash(0xb3))
	linkBeaconBlocks(t, first, second, third, lookahead)

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 2)
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			if requestCount.Add(1) == 1 {
				return []*cltypes.SignedBeaconBlock{first, second, third, lookahead}, "peer-a", nil
			}
			<-ctx.Done()
			return nil, "peer-b", ctx.Err()
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		for _, block := range blocks {
			require.LessOrEqual(t, block.Block.Slot, highest)
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	firstRequest := <-requests
	require.Equal(t, frontier-2, firstRequest.StartSlot)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	next := <-requests
	cancel()
	<-done
	require.Equal(t, frontier+2, next.StartSlot)
}

func TestInvalidGloasResponseAction(t *testing.T) {
	require.Equal(t, "ban-peer", invalidGloasResponseAction("peer-a"))
	require.Equal(t, "disable-http", invalidGloasResponseAction("http-fallback"))
}

func TestForwardBeaconDownloaderRejectsDisconnectedGloasLookahead(t *testing.T) {
	const frontier = uint64(100)
	cached := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	disconnected := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	disconnected.Block.ParentRoot = hash(0xff)

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 3)
	var requestCount atomic.Int32
	var banned atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		banPeer: func(string) {
			banned.Add(1)
		},
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			switch requestCount.Add(1) {
			case 1:
				return []*cltypes.SignedBeaconBlock{cached}, "peer-a", nil
			case 2:
				return []*cltypes.SignedBeaconBlock{disconnected}, "peer-b", nil
			default:
				<-ctx.Done()
				return nil, "peer-c", ctx.Err()
			}
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	var processed atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed.Add(int32(len(blocks)))
		if len(blocks) == 0 {
			return highest, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())
	<-requests
	downloader.RequestMore(t.Context())
	<-requests
	require.Zero(t, processed.Load())
	require.Zero(t, banned.Load())
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	retry := <-requests
	cancel()
	<-done
	require.Equal(t, frontier-2, retry.StartSlot)
}

func TestForwardBeaconDownloaderRejectsDisconnectedGloasResponseSuffix(t *testing.T) {
	const frontier = uint64(100)
	cached := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	linked := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	disconnected := makeGloasBlock(frontier+3, hash(0xa3), hash(0xb2))
	cachedRoot, err := cached.Block.HashSSZ()
	require.NoError(t, err)
	linked.Block.ParentRoot = cachedRoot
	disconnected.Block.ParentRoot = hash(0xff)

	requests := make(chan cltypes.BeaconBlocksByRangeRequest, 3)
	var requestCount atomic.Int32
	banned := make(chan string, 1)
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		banPeer: func(pid string) {
			banned <- pid
		},
		requestBlocksByRange: func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
			switch requestCount.Add(1) {
			case 1:
				return []*cltypes.SignedBeaconBlock{cached}, "peer-a", nil
			case 2:
				return []*cltypes.SignedBeaconBlock{linked, disconnected}, "peer-b", nil
			default:
				<-ctx.Done()
				return nil, "peer-c", ctx.Err()
			}
		},
	}
	downloader.SetHighestProcessedSlot(frontier)
	var processed atomic.Int32
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		processed.Add(int32(len(blocks)))
		if len(blocks) == 0 {
			return highest, nil
		}
		return blocks[len(blocks)-1].Block.Slot, nil
	})

	downloader.RequestMore(t.Context())
	<-requests
	downloader.RequestMore(t.Context())
	<-requests
	require.Zero(t, processed.Load())
	require.Equal(t, "peer-b", <-banned)
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	retry := <-requests
	cancel()
	<-done
	require.Equal(t, frontier+2, retry.StartSlot)
}

func TestForwardBeaconDownloaderDisconnectedHTTPResponseFallsBackToP2P(t *testing.T) {
	const frontier = uint64(100)
	first := makeGloasBlock(frontier+1, hash(0xa1), common.Hash{})
	second := makeGloasBlock(frontier+2, hash(0xa2), hash(0xb1))
	second.Block.ParentRoot = hash(0xff)
	firstEncoded, err := first.EncodeSSZ(nil)
	require.NoError(t, err)
	secondEncoded, err := second.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch {
		case strings.HasSuffix(r.URL.Path, "/beacon/blocks/101"):
			_, _ = w.Write(firstEncoded)
		case strings.HasSuffix(r.URL.Path, "/beacon/blocks/102"):
			_, _ = w.Write(secondEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := NewForwardBeaconDownloader(context.Background(), nil, gloasFromGenesisConfig())
	downloader.SetHighestProcessedSlot(frontier)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.httpPreferred.Store(true)
	p2pRequests := make(chan cltypes.BeaconBlocksByRangeRequest, 1)
	downloader.requestBlocksByRange = func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		p2pRequests <- cltypes.BeaconBlocksByRangeRequest{StartSlot: start, Count: count}
		<-ctx.Done()
		return nil, "peer-a", ctx.Err()
	}
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	require.False(t, downloader.httpPreferred.Load())
	require.Equal(t, frontier, downloader.GetHighestProcessedSlot())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()
	request := <-p2pRequests
	cancel()
	<-done
	require.Equal(t, frontier-2, request.StartSlot)
}

func TestForwardBeaconDownloaderHTTPEmptyWaitsForOverlappingGloasP2PResponse(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousFallbackDelay := forwardBeaconFallbackDelay
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 2 * time.Second
	forwardBeaconFallbackDelay = time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconFallbackDelay = previousFallbackDelay
	})

	lookahead := makeGloasBlock(101, hash(0xaa), common.Hash{})
	successor := makeGloasBlock(103, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, lookahead, successor)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)

	p2pStarted := make(chan struct{})
	releaseP2P := make(chan struct{})
	httpComplete := make(chan struct{})
	var startOnce sync.Once
	var httpCompleteOnce sync.Once
	downloader := &ForwardBeaconDownloader{
		beaconCfg:          gloasFromGenesisConfig(),
		gloasLookahead:     lookahead,
		gloasNextUnscanned: 102,
		onRequestComplete: func(_ *peerAndBlocks, empty bool) {
			if empty {
				httpCompleteOnce.Do(func() { close(httpComplete) })
			}
		},
		requestBlocksByRange: func(ctx context.Context, _, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			startOnce.Do(func() { close(p2pStarted) })
			select {
			case <-releaseP2P:
				return []*cltypes.SignedBeaconBlock{successor}, "p2p-peer", nil
			case <-ctx.Done():
				return nil, "p2p-peer", ctx.Err()
			}
		},
	}
	downloader.SetHighestProcessedSlot(100)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) > 0 {
			return blocks[len(blocks)-1].Block.Slot, nil
		}
		return highest, nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(t.Context())
	}()
	<-p2pStarted
	<-httpComplete
	start, pending := downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(102), start)

	close(releaseP2P)
	<-done
	start, pending = downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(104), start)
	require.Equal(t, uint64(101), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderHTTPResponseWinsBeforeOverlappingGloasP2PResponse(t *testing.T) {
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	previousFallbackDelay := forwardBeaconFallbackDelay
	previousResponsePoll := forwardBeaconResponsePoll
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 2 * time.Second
	forwardBeaconFallbackDelay = time.Millisecond
	forwardBeaconResponsePoll = 200 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
		forwardBeaconFallbackDelay = previousFallbackDelay
		forwardBeaconResponsePoll = previousResponsePoll
	})

	lookahead := makeGloasBlock(101, hash(0xaa), common.Hash{})
	httpSuccessor := makeGloasBlock(103, hash(0xbb), hash(0xcc))
	p2pSuccessor := makeGloasBlock(104, hash(0xdd), hash(0xee))
	linkBeaconBlocks(t, lookahead, httpSuccessor)
	linkBeaconBlocks(t, lookahead, p2pSuccessor)
	httpEncoded, err := httpSuccessor.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/beacon/blocks/103") {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(httpEncoded)
		} else {
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	httpComplete := make(chan struct{})
	var httpCompleteOnce sync.Once
	downloader := &ForwardBeaconDownloader{
		beaconCfg:          gloasFromGenesisConfig(),
		gloasLookahead:     lookahead,
		gloasNextUnscanned: 102,
		onRequestComplete: func(response *peerAndBlocks, _ bool) {
			if response != nil && response.fromHTTP {
				httpCompleteOnce.Do(func() { close(httpComplete) })
			}
		},
		requestBlocksByRange: func(ctx context.Context, _, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			select {
			case <-httpComplete:
			case <-ctx.Done():
				return nil, "p2p-peer", ctx.Err()
			}
			return []*cltypes.SignedBeaconBlock{p2pSuccessor}, "p2p-peer", nil
		},
	}
	downloader.SetHighestProcessedSlot(100)
	downloader.SetHTTPFallbackURL(server.URL)
	downloader.SetProcessFunction(func(highest uint64, blocks []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		if len(blocks) > 0 {
			return blocks[len(blocks)-1].Block.Slot, nil
		}
		return highest, nil
	})

	downloader.RequestMore(t.Context())
	start, pending := downloader.nextRequestStart(false)
	require.True(t, pending)
	require.Equal(t, uint64(144), start)
	require.Equal(t, uint64(101), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderOverlappingEmptyDoesNotSkipGloasLookahead(t *testing.T) {
	block := makeGloasBlock(101, hash(0xaa), common.Hash{})
	firstStarted := make(chan struct{})
	secondReturned := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg: &clparams.MainnetBeaconConfig,
		requestBlocksByRange: func(_ context.Context, _, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			switch requestCount.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseFirst
				return []*cltypes.SignedBeaconBlock{block}, "block-peer", nil
			case 2:
				close(secondReturned)
				return []*cltypes.SignedBeaconBlock{}, "block-peer", nil
			default:
				return nil, "block-peer", errors.New("unexpected request")
			}
		},
	}
	downloader.SetHighestProcessedSlot(100)
	downloader.SetProcessFunction(func(highest uint64, _ []*cltypes.SignedBeaconBlock, _ map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (uint64, error) {
		return highest, nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(t.Context())
	}()

	<-firstStarted
	<-secondReturned
	require.Equal(t, uint64(100), downloader.GetHighestProcessedSlot())
	close(releaseFirst)
	<-done
	require.Equal(t, uint64(100), downloader.GetHighestProcessedSlot())
}

func TestForwardBeaconDownloaderAdvancesPendingGloasScanAfterOverlappingError(t *testing.T) {
	firstStarted := make(chan struct{})
	emptyReturned := make(chan struct{})
	releaseError := make(chan struct{})
	var requestCount atomic.Int32
	downloader := &ForwardBeaconDownloader{
		beaconCfg:          &clparams.MainnetBeaconConfig,
		gloasLookahead:     makeGloasBlock(101, hash(0xaa), common.Hash{}),
		gloasNextUnscanned: 102,
		requestBlocksByRange: func(ctx context.Context, _, _ uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			switch requestCount.Add(1) {
			case 1:
				close(firstStarted)
				<-releaseError
				return nil, "block-peer", errors.New("request failed")
			case 2:
				close(emptyReturned)
				return []*cltypes.SignedBeaconBlock{}, "block-peer", nil
			default:
				<-ctx.Done()
				return nil, "block-peer", ctx.Err()
			}
		},
	}
	downloader.SetHighestProcessedSlot(100)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		downloader.RequestMore(ctx)
	}()

	<-firstStarted
	<-emptyReturned
	require.Equal(t, uint64(100), downloader.GetHighestProcessedSlot())
	close(releaseError)
	require.Eventually(t, func() bool {
		start, pending := downloader.nextRequestStart(false)
		return pending && start == 135
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(100), downloader.GetHighestProcessedSlot())
	cancel()
	<-done
}

func TestRetainBlocksBeforeMissingGloasEnvelopeKeepsCompletePrefix(t *testing.T) {
	first := makeGloasBlock(10, hash(0xaa), common.Hash{})
	second := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	lookahead := makeGloasBlock(12, hash(0xcc), hash(0xbb))
	blocks := []*cltypes.SignedBeaconBlock{first, second}
	fullRoots := determineFullGloasRoots([]*cltypes.SignedBeaconBlock{first, second, lookahead}, len(blocks))
	firstRoot, err := first.Block.HashSSZ()
	require.NoError(t, err)

	retained := retainBlocksBeforeMissingGloasEnvelope(
		blocks,
		fullRoots,
		map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{firstRoot: {}},
	)

	require.Equal(t, blocks[:1], retained)
}
