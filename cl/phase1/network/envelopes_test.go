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
			var bans atomic.Int32
			done := make(chan error, 1)
			go func() {
				_, err := requestEnvelopesFranticallyWithValidator(ctx, rpcClient, [][32]byte{{1}}, nil, func(context.Context, string) { bans.Add(1) })
				done <- err
			}()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(200 * time.Millisecond):
				t.Fatal("batch expiration did not interrupt the retry delay")
			}
			require.Zero(t, bans.Load())
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
	banned   []string
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

func (s *envelopeResponseSentinel) BanPeer(_ context.Context, peer *sentinelproto.Peer, _ ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.banned = append(s.banned, peer.Pid)
	return &sentinelproto.EmptyMessage{}, nil
}

func (s *envelopeResponseSentinel) BanPeerForInvalidData(ctx context.Context, peer *sentinelproto.Peer, opts ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return s.BanPeer(ctx, peer, opts...)
}

type retryEnvelopeSentinel struct {
	envelopeResponseSentinel
	malformedProtocol string
	sentMalformed     bool
	validRequests     int
}

func (s *retryEnvelopeSentinel) SendRequest(ctx context.Context, req *sentinelproto.RequestData, opts ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if !s.sentMalformed {
		response := &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "malformed-response-peer"}}
		if req.Topic == s.malformedProtocol {
			response.Data = []byte{1}
			s.sentMalformed = true
		}
		return response, nil
	}
	s.validRequests++
	return s.envelopeResponseSentinel.SendRequest(ctx, req, opts...)
}

func TestRequestEnvelopesFranticallyRetriesMalformedResponseWithoutBan(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	previousRetryInterval := requestEnvelopeRetryInterval
	requestEnvelopeBatchExpiration = time.Second
	requestEnvelopeRetryInterval = time.Millisecond
	t.Cleanup(func() {
		requestEnvelopeBatchExpiration = previousExpiration
		requestEnvelopeRetryInterval = previousRetryInterval
	})

	for _, tc := range []struct {
		name     string
		protocol string
		validate bool
	}{
		{name: "by-root", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1},
		{name: "by-range", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1},
		{name: "by-root with validator", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, validate: true},
		{name: "by-range with validator", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, validate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			cfg.InitializeForkSchedule()
			_, block, envelope := backfilledGloasEnvelopeFixture(t, cfg, 10, clparams.BuilderIndexSelfBuild, 0, backfillProposerKey)
			clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			var wire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, digest[:]...))
			sentinel := &retryEnvelopeSentinel{
				envelopeResponseSentinel: envelopeResponseSentinel{response: wire.Bytes()},
				malformedProtocol:        tc.protocol,
			}
			client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)
			roots := [][32]byte{envelope.Message.BeaconBlockRoot}
			var received map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
			if tc.validate {
				validate := newEnvelopeValidator([]*cltypes.SignedBeaconBlock{block}, func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
					return ValidateDownloadedGloasEnvelope(cfg, block, envelope)
				})
				received, err = requestEnvelopesFranticallyWithValidator(t.Context(), client, roots, validate, nil, block)
			} else {
				received, err = RequestEnvelopesFrantically(t.Context(), client, roots, block)
			}
			require.NoError(t, err)
			require.True(t, sentinel.sentMalformed)
			require.Equal(t, 1, sentinel.validRequests)
			require.Empty(t, sentinel.banned)
			require.Len(t, received, 1)
			require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, block, received[envelope.Message.BeaconBlockRoot]))
		})
	}
}

type backfillEnvelopeSentinel struct {
	envelopeResponseSentinel
	invalidResponse []byte
	invalidProtocol string
}

type rawEnvelopeSSZ []byte

func (r rawEnvelopeSSZ) EncodeSSZ(dst []byte) ([]byte, error) { return append(dst, r...), nil }
func (r rawEnvelopeSSZ) EncodingSizeSSZ() int                 { return len(r) }

func (s *backfillEnvelopeSentinel) SendRequest(ctx context.Context, req *sentinelproto.RequestData, opts ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if len(s.banned) > 0 {
		return s.envelopeResponseSentinel.SendRequest(ctx, req, opts...)
	}
	response := &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "invalid-envelope-peer"}}
	if req.Topic == s.invalidProtocol {
		response.Data = s.invalidResponse
	}
	return response, nil
}

func (s *backfillEnvelopeSentinel) BanPeer(context.Context, *sentinelproto.Peer, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return &sentinelproto.EmptyMessage{}, nil
}

type blockedEnvelopeBanSentinel struct {
	backfillEnvelopeSentinel
	banStarted chan struct{}
	banOnce    sync.Once
}

func (s *blockedEnvelopeBanSentinel) BanPeer(ctx context.Context, peer *sentinelproto.Peer, opts ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return s.BanPeerForInvalidData(ctx, peer, opts...)
}

func (s *blockedEnvelopeBanSentinel) BanPeerForInvalidData(ctx context.Context, _ *sentinelproto.Peer, _ ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	s.banOnce.Do(func() { close(s.banStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestBackwardBeaconDownloaderCancelsBlockedEnvelopeBan(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	previousRetryInterval := requestEnvelopeRetryInterval
	requestEnvelopeRetryInterval = time.Millisecond
	t.Cleanup(func() {
		requestEnvelopeBatchExpiration = previousExpiration
		requestEnvelopeRetryInterval = previousRetryInterval
	})

	for _, tc := range []struct {
		name         string
		protocol     string
		cancelCaller bool
		excessChunks bool
	}{
		{name: "by-root batch expiration", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1},
		{name: "by-range batch expiration", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1},
		{name: "by-root caller cancellation", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, cancelCaller: true},
		{name: "by-range caller cancellation", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, cancelCaller: true},
		{name: "by-root excess chunks batch expiration", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, excessChunks: true},
		{name: "by-range excess chunks caller cancellation", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, excessChunks: true, cancelCaller: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestEnvelopeBatchExpiration = 100 * time.Millisecond
			if tc.cancelCaller {
				requestEnvelopeBatchExpiration = time.Minute
			}
			cfg := gloasFromGenesisConfig()
			cfg.InitializeForkSchedule()
			_, block, envelope := backfilledGloasEnvelopeFixture(t, cfg, 10, clparams.BuilderIndexSelfBuild, 0, backfillProposerKey)
			envelope.Message.BuilderIndex++
			require.ErrorContains(t, ValidateDownloadedGloasEnvelope(cfg, block, envelope), "builder index mismatch")
			clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			var wire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, digest[:]...))
			if tc.excessChunks {
				require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, append([]byte{0}, digest[:]...)...))
			}
			sentinel := &blockedEnvelopeBanSentinel{
				backfillEnvelopeSentinel: backfillEnvelopeSentinel{
					invalidResponse: wire.Bytes(),
					invalidProtocol: tc.protocol,
				},
				banStarted: make(chan struct{}),
			}
			clientCtx, cancelClient := context.WithCancel(t.Context())
			fetchCtx, cancelFetch := context.WithCancel(clientCtx)
			client := rpc.NewBeaconRpcP2P(clientCtx, sentinel, cfg, clock)
			downloader := NewBackwardBeaconDownloader(clientCtx, client, nil, nil, nil, cfg)
			t.Cleanup(downloader.reqInterval.Stop)
			done := make(chan struct{})
			var fetchErr error
			go func() {
				_, fetchErr = downloader.requestEnvelopes(fetchCtx, [][32]byte{envelope.Message.BeaconBlockRoot}, block)
				close(done)
			}()
			defer func() {
				cancelFetch()
				cancelClient()
				<-done
			}()
			select {
			case <-sentinel.banStarted:
			case <-time.After(time.Second):
				t.Fatal("envelope fetch did not reach the ban RPC")
			}
			if tc.cancelCaller {
				cancelFetch()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("envelope fetch remained blocked in the ban RPC")
			}
			if tc.cancelCaller {
				require.ErrorIs(t, fetchErr, context.Canceled)
			} else {
				require.NoError(t, fetchErr)
			}
			require.NoError(t, clientCtx.Err())
		})
	}
}

func TestBackwardBeaconDownloaderBansInvalidEnvelopePeerAndRecovers(t *testing.T) {
	previousExpiration := requestEnvelopeBatchExpiration
	previousRetryInterval := requestEnvelopeRetryInterval
	requestEnvelopeBatchExpiration = 200 * time.Millisecond
	requestEnvelopeRetryInterval = time.Millisecond
	t.Cleanup(func() {
		requestEnvelopeBatchExpiration = previousExpiration
		requestEnvelopeRetryInterval = previousRetryInterval
	})

	for _, tc := range []struct {
		name        string
		protocol    string
		invalidKind string
		single      bool
	}{
		{name: "by-root commitment", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1},
		{name: "by-range commitment", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1},
		{name: "by-root signature", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "signature"},
		{name: "by-range signature", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, invalidKind: "signature"},
		{name: "by-root config", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "config"},
		{name: "by-range config", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, invalidKind: "config"},
		{name: "by-root SSZ", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "ssz"},
		{name: "by-range SSZ", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, invalidKind: "ssz"},
		{name: "by-root framing", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "framing"},
		{name: "by-range framing", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, invalidKind: "framing"},
		{name: "by-root fork digest", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "fork"},
		{name: "by-range fork digest", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, invalidKind: "fork"},
		{name: "by-root excess chunks", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "excess"},
		{name: "by-range excess chunks", protocol: communication.ExecutionPayloadEnvelopesByRangeProtocolV1, invalidKind: "excess"},
		{name: "single commitment", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, single: true},
		{name: "single signature", protocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1, invalidKind: "signature", single: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			cfg.MaxBuilderDepositRequestsPerPayload = 1
			cfg.InitializeForkSchedule()
			clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			anchorState, block, valid := backfilledGloasEnvelopeFixture(t, cfg, 10, clparams.BuilderIndexSelfBuild, 0, backfillProposerKey)
			validBytes, err := valid.EncodeSSZ(nil)
			require.NoError(t, err)
			invalid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			require.NoError(t, invalid.DecodeSSZ(validBytes, int(clparams.GloasVersion)))
			switch tc.invalidKind {
			case "signature":
				signBackfilledGloasEnvelope(t, cfg, anchorState, invalid, backfillOtherKey)
				require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, block, invalid))
				require.ErrorContains(t, validateBackfilledGloasEnvelopeSignature(anchorState, block, invalid), "invalid signature")
			case "config":
				invalid.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
				invalid.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
				require.ErrorContains(t, invalid.ValidateForConfig(cfg), "builder deposits")
			case "":
				invalid.Message.BuilderIndex++
				require.ErrorContains(t, ValidateDownloadedGloasEnvelope(cfg, block, invalid), "builder index mismatch")
			}
			if tc.invalidKind != "config" {
				require.NoError(t, invalid.ValidateForConfig(cfg))
			}

			var validWire, invalidWire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&validWire, valid, digest[:]...))
			switch tc.invalidKind {
			case "ssz":
				require.NoError(t, ssz_snappy.EncodeAndWrite(&invalidWire, rawEnvelopeSSZ{0}, digest[:]...))
			case "framing":
				_, err = invalidWire.Write(digest[:1])
				require.NoError(t, err)
			case "fork":
				require.NoError(t, ssz_snappy.EncodeAndWrite(&invalidWire, invalid, 0, 0, 0, 0))
			case "excess":
				require.NoError(t, ssz_snappy.EncodeAndWrite(&invalidWire, valid, digest[:]...))
				require.NoError(t, ssz_snappy.EncodeAndWrite(&invalidWire, valid, append([]byte{0}, digest[:]...)...))
			default:
				require.NoError(t, ssz_snappy.EncodeAndWrite(&invalidWire, invalid, digest[:]...))
			}
			sentinel := &backfillEnvelopeSentinel{
				envelopeResponseSentinel: envelopeResponseSentinel{response: validWire.Bytes()},
				invalidResponse:          invalidWire.Bytes(),
				invalidProtocol:          tc.protocol,
			}
			client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)
			downloader := NewBackwardBeaconDownloader(t.Context(), client, nil, nil, nil, cfg)
			t.Cleanup(downloader.reqInterval.Stop)
			downloader.SetGloasEnvelopeSignatureValidator(anchorState)
			validate := downloader.validateGloasEnvelope
			validations := 0
			downloader.validateGloasEnvelope = func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
				err := validate(block, envelope)
				if err == nil {
					validations++
				}
				return err
			}
			downloader.SetExpectedRoot(valid.Message.BeaconBlockRoot)
			downloader.SetSlotToDownload(block.Block.Slot)
			child := makeGloasBlock(11, hash(2), valid.Message.Payload.BlockHash)
			linkBeaconBlocks(t, block, child)
			downloader.prevBatchTopBlock = child
			downloader.SetOnNewBlock(func(got *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				require.Same(t, block, got)
				require.NoError(t, validate(got, envelope))
				return true, nil
			})

			if tc.single {
				envelope, err := downloader.fetchSingleEnvelopeP2P(t.Context(), block)
				require.NoError(t, err)
				require.NoError(t, validate(block, envelope))
			} else {
				require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{block}))
				require.True(t, downloader.Finished())
				require.Equal(t, uint64(1), downloader.Processed())
				require.Equal(t, block.Block.ParentRoot, downloader.expectedRoot)
			}
			require.Equal(t, []string{"invalid-envelope-peer"}, sentinel.banned)
			require.Equal(t, 1, validations)
			require.Zero(t, downloader.consecutiveEnvelopeFailures)
		})
	}
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

	rejected := acceptEnvelopeResponsesWithValidator([]*cltypes.SignedExecutionPayloadEnvelope{
		requestedEnvelope,
		nil,
		{},
		{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: unsolicitedRoot}},
	}, nil, requestedRoots, received, func(envelope *cltypes.SignedExecutionPayloadEnvelope) error {
		require.Same(t, requestedEnvelope, envelope)
		return nil
	}, "peer", func(pid string) { t.Errorf("unexpected ban of %s", pid) })

	require.False(t, rejected)
	require.Same(t, requestedEnvelope, received[requestedRoot])
	require.NotContains(t, received, unsolicitedRoot)
}

func TestRequestEnvelopesByRangeRetainsValidatedPrefixOnError(t *testing.T) {
	client, requestedRoot, block := newMixedEnvelopeResponseClient(t)
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	var banned []string

	rejected := requestEnvelopesByRangeWithValidator(
		context.Background(),
		client,
		[]*cltypes.SignedBeaconBlock{block},
		map[common.Hash]struct{}{requestedRoot: {}},
		received,
		nil,
		func(pid string) { banned = append(banned, pid) },
	)

	require.True(t, rejected)
	require.Equal(t, []string{"mixed-response-peer"}, banned)
	require.Contains(t, received, requestedRoot)
	require.Equal(t, requestedRoot, received[requestedRoot].Message.BeaconBlockRoot)
}

func TestRequestEnvelopesByRootRetainsValidatedPrefixOnError(t *testing.T) {
	client, requestedRoot, _ := newMixedEnvelopeResponseClient(t)

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	var banned []string
	rejected, err := requestEnvelopesByRoot(context.Background(), client, [][32]byte{requestedRoot, {2}}, map[common.Hash]struct{}{requestedRoot: {}}, received, nil, func(pid string) { banned = append(banned, pid) })

	require.Error(t, err)
	require.True(t, rejected)
	require.Equal(t, []string{"mixed-response-peer"}, banned)
	require.Len(t, received, 1)
	require.Equal(t, requestedRoot, received[requestedRoot].Message.BeaconBlockRoot)
}

func TestAcceptEnvelopeResponsesRejectsInvalidCandidateBeforeProgress(t *testing.T) {
	root := common.Hash{1}
	invalid := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	invalid.Message.BeaconBlockRoot = root
	valid := invalid.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	valid.Message.Payload.BlockHash = common.Hash{2}
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	requested := map[common.Hash]struct{}{root: {}}
	validate := func(envelope *cltypes.SignedExecutionPayloadEnvelope) error {
		if envelope.Message.Payload.BlockHash == (common.Hash{}) {
			return errors.New("invalid candidate")
		}
		return nil
	}
	var banned []string
	banPeer := func(pid string) { banned = append(banned, pid) }

	require.True(t, acceptEnvelopeResponsesWithValidator([]*cltypes.SignedExecutionPayloadEnvelope{invalid}, nil, requested, received, validate, "invalid-peer", banPeer))
	require.Equal(t, [][32]byte{root}, filterReceived([][32]byte{root}, received))
	require.Equal(t, []string{"invalid-peer"}, banned)

	require.False(t, acceptEnvelopeResponsesWithValidator([]*cltypes.SignedExecutionPayloadEnvelope{valid}, nil, requested, received, validate, "valid-peer", banPeer))
	require.Same(t, valid, received[root])
	require.Equal(t, []string{"invalid-peer"}, banned)

	delete(received, root)
	require.True(t, acceptEnvelopeResponsesWithValidator([]*cltypes.SignedExecutionPayloadEnvelope{valid, invalid, invalid}, rpc.ErrInvalidResponse, requested, received, validate, "mixed-peer", banPeer))
	require.Same(t, valid, received[root])
	require.Equal(t, []string{"invalid-peer", "mixed-peer"}, banned)
}

func TestRequestEnvelopesByRangeReportsInvalidCandidate(t *testing.T) {
	client, requestedRoot, block := newMixedEnvelopeResponseClient(t)
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}

	rejected := requestEnvelopesByRangeWithValidator(
		context.Background(),
		client,
		[]*cltypes.SignedBeaconBlock{block},
		map[common.Hash]struct{}{requestedRoot: {}},
		received,
		func(*cltypes.SignedExecutionPayloadEnvelope) error { return errors.New("invalid candidate") },
		nil,
	)

	require.True(t, rejected)
	require.Empty(t, received)
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

	requestEnvelopesByRangeWithValidator(
		ctx,
		rpcClient,
		[]*cltypes.SignedBeaconBlock{low, high},
		map[common.Hash]struct{}{lowRoot: {}, highRoot: {}},
		map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{},
		nil,
		nil,
	)

	require.Equal(t, int32(2), sentinel.byRange.Load())
}

func TestRequestEnvelopesFranticallyPreservesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		_, err := requestEnvelopesFranticallyWithValidator(ctx, nil, [][32]byte{{1}}, nil, nil)
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
	)
	return client, requestedRoot, block
}
