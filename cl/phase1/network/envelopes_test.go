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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type slowEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	first       sync.Once
	byRange     chan struct{}
	byRangeOnce sync.Once
}

type blockedRangeEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	byRootCalls   atomic.Int32
	byRange       chan struct{}
	laterByRoot   chan struct{}
	byRangeOnce   sync.Once
	laterRootOnce sync.Once
}

type immediateEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	empty bool
}

func (s *immediateEnvelopeSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if s.empty {
		return &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "empty-peer"}}, nil
	}
	return nil, errors.New("request failed")
}

func (s *blockedRangeEnvelopeSentinel) SendRequest(ctx context.Context, req *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if req.Topic == communication.ExecutionPayloadEnvelopesByRangeProtocolV1 {
		s.byRangeOnce.Do(func() { close(s.byRange) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.byRootCalls.Add(1) > 3 {
		s.laterRootOnce.Do(func() { close(s.laterByRoot) })
	}
	return nil, context.Canceled
}

func (s *slowEnvelopeSentinel) SendRequest(ctx context.Context, req *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	first := false
	s.first.Do(func() { first = true })
	if first {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if req.Topic == communication.ExecutionPayloadEnvelopesByRangeProtocolV1 {
		s.byRangeOnce.Do(func() { close(s.byRange) })
	}
	return nil, context.Canceled
}

func TestRequestEnvelopesFranticallyCancelsBlockedRequestWhenBatchExpires(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	requestEnvelopeBatchExpiration = 100 * time.Millisecond
	t.Cleanup(func() { requestEnvelopeBatchExpiration = previousExpiration })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := newContextBlockingSentinel()
	rpcClient, _ := newContextBlockingBeaconRPC(ctx, sentinel)

	done := make(chan error, 1)
	go func() {
		_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{{1}})
		done <- err
	}()

	select {
	case <-sentinel.started:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the Sentinel boundary")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("batch expiration did not stop the blocked envelope request")
	}
	select {
	case <-sentinel.canceled:
	case <-time.After(time.Second):
		t.Fatal("batch expiration returned without canceling the blocked envelope request")
	}
}

func TestRequestEnvelopesFranticallyReturnsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	sentinel := newContextBlockingSentinel()
	rpcClient, _ := newContextBlockingBeaconRPC(ctx, sentinel)

	done := make(chan error, 1)
	go func() {
		_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{{1}})
		done <- err
	}()

	select {
	case <-sentinel.started:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the Sentinel boundary")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not stop the blocked envelope request")
	}
}

func TestRequestEnvelopesFranticallyInterruptsRetryDelay(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	previousRetryInterval := requestEnvelopeRetryInterval
	requestEnvelopeBatchExpiration = 20 * time.Millisecond
	requestEnvelopeRetryInterval = time.Second
	t.Cleanup(func() {
		requestEnvelopeBatchExpiration = previousExpiration
		requestEnvelopeRetryInterval = previousRetryInterval
	})

	for _, tc := range []struct {
		name  string
		empty bool
	}{
		{name: "request_error"},
		{name: "empty_response", empty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rpcClient, _ := newContextBlockingBeaconRPC(ctx, &immediateEnvelopeSentinel{empty: tc.empty})
			done := make(chan error, 1)
			go func() {
				_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{{1}})
				done <- err
			}()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(200 * time.Millisecond):
				t.Fatal("batch expiration did not interrupt the retry delay")
			}
		})
	}
}

func TestRequestEnvelopesFranticallyReservesTimeForRangeFallback(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	previousAttemptTimeout := requestEnvelopeAttemptTimeout
	requestEnvelopeBatchExpiration = 200 * time.Millisecond
	requestEnvelopeAttemptTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		requestEnvelopeBatchExpiration = previousExpiration
		requestEnvelopeAttemptTimeout = previousAttemptTimeout
	})

	ctx, cancel := context.WithCancel(t.Context())
	sentinel := &slowEnvelopeSentinel{byRange: make(chan struct{})}
	rpcClient, _ := newContextBlockingBeaconRPC(ctx, sentinel)
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1}}

	done := make(chan error, 1)
	go func() {
		_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{{1}}, block)
		done <- err
	}()

	select {
	case <-sentinel.byRange:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("slow by-root peer consumed the range fallback budget")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRequestEnvelopesFranticallyBoundsRangeFallbackAttempt(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	previousAttemptTimeout := requestEnvelopeAttemptTimeout
	previousRetryInterval := requestEnvelopeRetryInterval
	requestEnvelopeBatchExpiration = 200 * time.Millisecond
	requestEnvelopeAttemptTimeout = 20 * time.Millisecond
	requestEnvelopeRetryInterval = time.Millisecond
	t.Cleanup(func() {
		requestEnvelopeBatchExpiration = previousExpiration
		requestEnvelopeAttemptTimeout = previousAttemptTimeout
		requestEnvelopeRetryInterval = previousRetryInterval
	})

	ctx, cancel := context.WithCancel(t.Context())
	sentinel := &blockedRangeEnvelopeSentinel{
		byRange:     make(chan struct{}),
		laterByRoot: make(chan struct{}),
	}
	rpcClient, _ := newContextBlockingBeaconRPC(ctx, sentinel)
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1}}

	done := make(chan error, 1)
	go func() {
		_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{{1}}, block)
		done <- err
	}()

	select {
	case <-sentinel.byRange:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("range fallback did not start")
	}
	select {
	case <-sentinel.laterByRoot:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked range fallback consumed the remaining by-root budget")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestAcceptEnvelopeResponsesKeepsOnlyRequestedRoots(t *testing.T) {
	requestedRoot := common.Hash{1}
	unsolicitedRoot := common.Hash{2}
	requestedRoots := map[common.Hash]struct{}{
		requestedRoot: {},
	}
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	requestedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: requestedRoot},
	}

	acceptEnvelopeResponses([]*cltypes.SignedExecutionPayloadEnvelope{
		requestedEnvelope,
		nil,
		{},
		{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: unsolicitedRoot}},
	}, requestedRoots, received)

	require.Same(t, requestedEnvelope, received[requestedRoot])
	require.NotContains(t, received, unsolicitedRoot)
}
