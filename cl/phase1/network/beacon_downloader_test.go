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
	"net/http"
	"net/http/httptest"
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
	previousInterval := forwardBeaconRequestInterval
	previousTimeout := forwardBeaconRequestTimeout
	forwardBeaconRequestInterval = time.Millisecond
	forwardBeaconRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		forwardBeaconRequestInterval = previousInterval
		forwardBeaconRequestTimeout = previousTimeout
	})

	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	first := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	first.Block.Slot = 11
	first.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{1}
	second := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	second.Block.Slot = 12
	second.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{1}
	firstEncoded, err := first.EncodeSSZ(nil)
	require.NoError(t, err)
	secondEncoded, err := second.EncodeSSZ(nil)
	require.NoError(t, err)

	envelopeStarted := make(chan struct{})
	releaseEnvelope := make(chan struct{})
	var envelopeOnce sync.Once
	var blockSecondHTTP atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/11":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(firstEncoded)
		case "/eth/v2/beacon/blocks/12":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(secondEncoded)
		case "/eth/v1/beacon/execution_payload_envelope/11":
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
	rpcClient, rpcCfg := newContextBlockingBeaconRPC(ctx, sentinel)
	downloader := NewForwardBeaconDownloader(ctx, rpcClient, rpcCfg)
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
	case <-time.After(time.Second):
		t.Fatal("preferred HTTP response did not start envelope fetch")
	}
	downloader.SetHighestProcessedSlot(20)
	close(releaseEnvelope)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
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
	case <-time.After(time.Second):
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
