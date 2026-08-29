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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
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

type countingRangeEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	byRange atomic.Int32
	cancel  context.CancelFunc
}

func (s *countingRangeEnvelopeSentinel) SendRequest(_ context.Context, req *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if req.Topic == communication.ExecutionPayloadEnvelopesByRangeProtocolV1 && s.byRange.Add(1) == 5 {
		s.cancel()
	}
	return &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "empty-peer"}}, nil
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
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 1
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{root}, block)
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
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 1
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := RequestEnvelopesFrantically(ctx, rpcClient, [][32]byte{root}, block)
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

type envelopeResponseSentinel struct {
	sentinelproto.SentinelClient
	response []byte
}

func (s *envelopeResponseSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	return &sentinelproto.ResponseData{
		Data: s.response,
		Peer: &sentinelproto.Peer{Pid: "mixed-response-peer"},
	}, nil
}

func (s *envelopeResponseSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
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

func TestRequestEnvelopesByRangeRetainsValidatedPrefixOnError(t *testing.T) {
	client, requestedRoot, block := newMixedEnvelopeResponseClient(t)
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}

	requestEnvelopesByRange(
		context.Background(),
		client,
		[]*cltypes.SignedBeaconBlock{block},
		map[common.Hash]struct{}{requestedRoot: {}},
		received,
	)

	require.Contains(t, received, requestedRoot)
	require.Equal(t, requestedRoot, received[requestedRoot].Message.BeaconBlockRoot)
}

func TestRequestEnvelopesByRootRetainsValidatedPrefixOnError(t *testing.T) {
	client, requestedRoot, _ := newMixedEnvelopeResponseClient(t)

	envelopes, err := requestEnvelopesByRoot(context.Background(), client, [][32]byte{requestedRoot, {2}})

	require.Error(t, err)
	require.Len(t, envelopes, 1)
	require.Equal(t, requestedRoot, envelopes[0].Message.BeaconBlockRoot)
}

func TestEnvelopeRequestSlotRangeUsesRequestedRootsIndependentOfOrder(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	low := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	low.Block.Slot = 37
	high := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	high.Block.Slot = 99
	unsolicited := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	unsolicited.Block.Slot = 100
	lowRoot, err := low.Block.HashSSZ()
	require.NoError(t, err)
	highRoot, err := high.Block.HashSSZ()
	require.NoError(t, err)

	requested := map[common.Hash]struct{}{lowRoot: {}, highRoot: {}}
	for _, blocks := range [][]*cltypes.SignedBeaconBlock{{low, high}, {high, low}} {
		start, count, ok := envelopeRequestSlotRange(blocks, requested)
		require.True(t, ok)
		require.Equal(t, uint64(37), start)
		require.Equal(t, uint64(63), count)
	}

	start, count, ok := envelopeRequestSlotRange([]*cltypes.SignedBeaconBlock{unsolicited, low}, map[common.Hash]struct{}{lowRoot: {}})
	require.True(t, ok)
	require.Equal(t, uint64(37), start)
	require.Equal(t, uint64(1), count)
}

func TestEnvelopeRequestSlotRangeRejectsOverflow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	low := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	low.Block.Slot = 0
	high := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	high.Block.Slot = ^uint64(0)
	lowRoot, err := low.Block.HashSSZ()
	require.NoError(t, err)
	highRoot, err := high.Block.HashSSZ()
	require.NoError(t, err)

	_, _, ok := envelopeRequestSlotRange(
		[]*cltypes.SignedBeaconBlock{high, low},
		map[common.Hash]struct{}{lowRoot: {}, highRoot: {}},
	)
	require.False(t, ok)
}

func TestRequestEnvelopesByRangeBoundsCallsForSparseRequestedSlots(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sentinel := &countingRangeEnvelopeSentinel{cancel: cancel}
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	cfg.MaxRequestPayloads = 1
	low := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	low.Block.Slot = 0
	high := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	high.Block.Slot = 1 << 40
	lowRoot, err := low.Block.HashSSZ()
	require.NoError(t, err)
	highRoot, err := high.Block.HashSSZ()
	require.NoError(t, err)

	requestEnvelopesByRange(
		ctx,
		rpcClient,
		[]*cltypes.SignedBeaconBlock{low, high},
		map[common.Hash]struct{}{lowRoot: {}, highRoot: {}},
		map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{},
	)

	require.Equal(t, int32(2), sentinel.byRange.Load())
}

func TestRequestEnvelopesFranticallyPreservesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		_, err := requestEnvelopesFranticallyWithValidator(ctx, nil, [][32]byte{{1}}, nil)
		require.ErrorIs(t, err, context.Canceled)
	}
}

func newMixedEnvelopeResponseClient(t *testing.T) (*rpc.BeaconRpcP2P, common.Hash, *cltypes.SignedBeaconBlock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	cfg.InitializeForkSchedule()
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	gloasDigest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = 1
	requestedRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	valid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	valid.Message.BeaconBlockRoot = requestedRoot
	invalid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	invalid.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	invalid.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})

	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, valid, gloasDigest[:]...))
	require.NoError(t, response.WriteByte(0))
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, invalid, gloasDigest[:]...))
	client := rpc.NewBeaconRpcP2P(
		context.Background(),
		&envelopeResponseSentinel{response: response.Bytes()},
		&cfg,
		clock,
		nil,
	)
	return client, requestedRoot, block
}
