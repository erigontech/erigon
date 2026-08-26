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

type countingBlockingSentinel struct {
	sentinelproto.SentinelClient
	active    atomic.Int32
	maximum   atomic.Int32
	overLimit chan struct{}
	drained   chan struct{}
	overOnce  sync.Once
	drainOnce sync.Once
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

func TestForwardRequestMoreDoesNotApplyStaleHTTPFallbackAfterP2PProgress(t *testing.T) {
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
	require.Eventually(t, func() bool {
		return downloader.GetHighestProcessedSlot() > 10
	}, time.Second, time.Millisecond, "empty P2P response did not advance the cursor")
	close(releaseFallback)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish")
	}
	require.Zero(t, processCalls.Load(), "stale HTTP result was processed after P2P advanced the cursor")
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

func TestForwardRequestMoreRechecksHTTPFrontierBeforeProcessing(t *testing.T) {
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

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	require.Eventually(t, downloader.httpPreferred.Load, time.Second, time.Millisecond, "HTTP response was not committed")
	downloader.SetHighestProcessedSlot(20)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestMore did not finish")
	}
	require.Zero(t, processCalls.Load(), "HTTP response was not revalidated before processing")
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
