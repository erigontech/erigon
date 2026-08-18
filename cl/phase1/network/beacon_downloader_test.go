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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
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
