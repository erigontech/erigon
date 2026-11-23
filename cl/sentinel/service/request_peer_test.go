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

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

func TestPeerRequestUsesCallerContext(t *testing.T) {
	requestStarted, requestCancelled := make(chan struct{}), make(chan struct{})
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{
		handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			close(requestStarted)
			<-request.Context().Done()
			close(requestCancelled)
		}),
	}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := server.requestPeer(ctx, peer.ID("peer"), &sentinelproto.RequestData{Data: []byte("request")})
		done <- err
	}()

	receivePeerRequestTestValue(t, requestStarted)
	cancel()
	require.ErrorIs(t, receivePeerRequestTestValue(t, done), context.Canceled)
	receivePeerRequestTestValue(t, requestCancelled)
}

func TestPeerProtocolErrorBodyClosesOnCancellation(t *testing.T) {
	body := &blockingResponseBody{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	stopClose := closeBodyOnCancel(ctx, body)
	defer stopClose()
	done := make(chan error, 1)
	go func() {
		_, err := httpreqresp.ResponseCodeServerError.ErrorMessage(&http.Response{Body: body})
		done <- err
	}()

	cancel()
	require.Error(t, receivePeerRequestTestValue(t, done))
	receivePeerRequestTestValue(t, body.closed)
}

type blockingResponseBody struct {
	closed chan struct{}
}

func (b *blockingResponseBody) Read([]byte) (int, error) {
	<-b.closed
	return 0, errors.New("response body closed")
}

func (b *blockingResponseBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

var _ io.ReadCloser = (*blockingResponseBody)(nil)

type peerRequestBackendStub struct {
	handler       http.Handler
	active        int
	lostDuplicate *atomic.Bool
}

func (p peerRequestBackendStub) GetPeersCount() (int, int, int) { return p.active, 0, 0 }
func (p peerRequestBackendStub) ConsumeDuplicateLoss(peer.ID) bool {
	return p.lostDuplicate != nil && p.lostDuplicate.CompareAndSwap(true, false)
}

func (p peerRequestBackendStub) Config() *sentinel.SentinelConfig {
	config := &sentinel.SentinelConfig{}
	config.MaxPeerCount = 1
	return config
}
func (p peerRequestBackendStub) ReqRespHandler() http.Handler { return p.handler }

// A request cut by a duplicate connection closing is not the peer's fault, and dropping the peer
// would close the connection it still has.
func TestFailedRequestKeepsAPeerThatJustLostADuplicateConnection(t *testing.T) {
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{active: 1}}
	require.True(t, server.dropOnFailure(peer.ID("peer"), nil))
	lost := &atomic.Bool{}
	lost.Store(true)
	server.peerRequestBackend = peerRequestBackendStub{active: 0, lostDuplicate: lost}
	require.False(t, server.dropOnFailure(peer.ID("peer"), nil), "below the peer limit failures never drop")
	require.True(t, lost.Load(), "a failure that drops nothing leaves the loss for the next one")
	server.peerRequestBackend = peerRequestBackendStub{active: 1, lostDuplicate: lost}
	require.False(t, server.dropOnFailure(peer.ID("peer"), nil))
	require.True(t, server.dropOnFailure(peer.ID("peer"), nil), "one loss excuses one failure")
}

func TestOverCapResponseIsNotExcusedByADuplicateLoss(t *testing.T) {
	lost := &atomic.Bool{}
	lost.Store(true)
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{active: 1, lostDuplicate: lost}}
	require.True(t, server.dropOnFailure(peer.ID("peer"), fmt.Errorf("read body: %w", httpreqresp.ErrResponseTooLarge)))
	require.True(t, lost.Load(), "the loss stays for a failure it can excuse")
}

// A well-formed error response arrived complete, so a cut stream did not cause it.
func TestPeerErrorResponseIsNotExcusedByADuplicateLoss(t *testing.T) {
	lost := &atomic.Bool{}
	lost.Store(true)
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{active: 1, lostDuplicate: lost}}
	require.True(t, server.dropOnFailure(peer.ID("peer"), &PeerResponseError{Code: httpreqresp.ResponseCodeServerError}))
	require.True(t, lost.Load(), "the loss stays for a failure it can excuse")
}

// The duplicate can close while the request runs, so the drop decision is taken when the request
// fails. The server has no sentinel, so a drop would panic.
func TestRequestCutByADuplicateCloseKeepsThePeer(t *testing.T) {
	lost := &atomic.Bool{}
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{
		active:        1,
		lostDuplicate: lost,
		handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			lost.Store(true)
			w.WriteHeader(http.StatusBadRequest)
		}),
	}}
	require.NotPanics(t, func() {
		_, err := server.requestPeer(t.Context(), peer.ID("peer"), &sentinelproto.RequestData{Data: []byte("request")})
		var httpErr *HTTPError
		require.ErrorAs(t, err, &httpErr)
	})
}

func receivePeerRequestTestValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for peer request")
		var zero T
		return zero
	}
}

// Requests to one peer share MAX_CONCURRENT_REQUESTS open streams per request type, across protocol versions and alternatives lists.
func TestPeerRequestLimitsConcurrentStreamsPerPeerAndRequestType(t *testing.T) {
	release := make(chan struct{})
	reached := make(chan string, 8)
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{
		handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			reached <- request.Header.Get("REQRESP-PEER-ID") + " " + request.Header.Get("REQRESP-TOPIC")
			<-release
		}),
	}}
	request := func(pid, topic string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := server.requestPeer(t.Context(), peer.ID(pid), &sentinelproto.RequestData{Topic: topic, Data: []byte("request")})
			done <- err
		}()
		return done
	}

	blocksByRange := communication.BeaconBlocksByRangeProtocolV2 + "," + communication.BeaconBlocksByRangeProtocolV1
	first, second := request("a", blocksByRange), request("a", blocksByRange)
	receivePeerRequestTestValue(t, reached)
	receivePeerRequestTestValue(t, reached)

	require.ErrorIs(t, receivePeerRequestTestValue(t, request("a", communication.BeaconBlocksByRangeProtocolV2)), ErrPeerBusy)
	otherPeer := request("b", communication.BeaconBlocksByRangeProtocolV1)
	otherRequestType := request("a", communication.DataColumnSidecarsByRootProtocolV1)
	receivePeerRequestTestValue(t, reached)
	receivePeerRequestTestValue(t, reached)

	close(release)
	for _, done := range []<-chan error{first, second, otherPeer, otherRequestType} {
		require.NotErrorIs(t, receivePeerRequestTestValue(t, done), ErrPeerBusy)
	}
	require.NotErrorIs(t, receivePeerRequestTestValue(t, request("a", communication.BeaconBlocksByRangeProtocolV2)), ErrPeerBusy, "finished streams free their slots")
}

func TestSmallRequestDoesNotWaitBehindBulkRequests(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	reached := make(chan struct{}, 4)
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{
		handler: http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			reached <- struct{}{}
			<-release
		}),
	}}
	bulk := func(pid string) {
		go func() {
			_, _ = server.requestPeer(t.Context(), peer.ID(pid), &sentinelproto.RequestData{Topic: communication.BeaconBlocksByRangeProtocolV2, Data: []byte{1}, MaxResponseBytes: math.MaxUint64})
		}()
	}
	bulk("a")
	bulk("b")
	receivePeerRequestTestValue(t, reached)
	receivePeerRequestTestValue(t, reached)
	bulk("c")
	time.Sleep(100 * time.Millisecond) // the third bulk request is now queued

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	go func() {
		_, _ = server.requestPeer(ctx, peer.ID("d"), &sentinelproto.RequestData{Topic: communication.MetadataProtocolV2, Data: []byte{1}})
	}()
	receivePeerRequestTestValue(t, reached)
}

// Responses are buffered whole, so max-size requests in flight share one byte budget: a third must wait, without a stream.
func TestPeerRequestsShareAResponseBudget(t *testing.T) {
	release := make(chan struct{})
	reached := make(chan struct{}, 4)
	server := &SentinelServer{peerRequestBackend: peerRequestBackendStub{
		handler: http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			reached <- struct{}{}
			<-release
		}),
	}}
	blocksByRange := " " + communication.BeaconBlocksByRangeProtocolV2 + " , " + communication.BeaconBlocksByRangeProtocolV1 // the handler trims each alternative
	request := func(pid string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := server.requestPeer(t.Context(), peer.ID(pid), &sentinelproto.RequestData{Topic: blocksByRange, Data: []byte("request"), MaxResponseBytes: math.MaxUint64})
			done <- err
		}()
		return done
	}

	first, second, third := request("a"), request("a"), request("a")
	receivePeerRequestTestValue(t, reached)
	receivePeerRequestTestValue(t, reached)
	select {
	case <-reached:
		t.Fatal("a third max-size request ran while two held the budget")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	receivePeerRequestTestValue(t, reached)
	for _, done := range []<-chan error{first, second, third} {
		require.NotErrorIs(t, receivePeerRequestTestValue(t, done), ErrPeerBusy, "a request waiting for budget holds no stream")
	}
}
