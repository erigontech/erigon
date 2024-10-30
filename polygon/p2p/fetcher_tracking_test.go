// Copyright 2024 The Erigon Authors
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

package p2p

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/v3/core/types"
)

func TestTrackingFetcherFetchHeadersUpdatesPeerTracker(t *testing.T) {
	t.Parallel()

	peerId1 := PeerIdFromUint64(1)
	requestId1 := uint64(1234)
	mockInboundMessages1 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId1.H512(),
			Data:   newMockBlockHeadersPacket66Bytes(t, requestId1, 2),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId1,
		wantRequestOriginNumber:     1,
		wantRequestAmount:           2,
	}
	requestId2 := uint64(1235)
	mockInboundMessages2 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_HEADERS_66,
			PeerId: peerId1.H512(),
			// peer returns 0 headers for requestId2 - peer does not have this header range
			Data: newMockBlockHeadersPacket66Bytes(t, requestId2, 0),
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId1,
		wantRequestOriginNumber:     3,
		wantRequestAmount:           2,
	}

	test := newTrackingFetcherTest(t, newMockRequestGenerator(requestId1, requestId2))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2)
	test.run(func(ctx context.Context, t *testing.T) {
		test.simulateDefaultPeerEvents()
		var peerIds []*PeerId // peers which may have blocks 1 and 2
		require.Eventuallyf(t, func() bool {
			peerIds = test.peerTracker.ListPeersMayHaveBlockNum(2)
			return len(peerIds) == 2
		}, time.Second, 100*time.Millisecond, "expected number of initial peers never satisfied: want=2, have=%d", len(peerIds))

		headers, err := test.trackingFetcher.FetchHeaders(ctx, 1, 3, peerId1) // fetch headers 1 and 2
		headersData := headers.Data
		require.NoError(t, err)
		require.Len(t, headersData, 2)
		require.Equal(t, uint64(1), headersData[0].Number.Uint64())
		require.Equal(t, uint64(2), headersData[1].Number.Uint64())

		peerIds = test.peerTracker.ListPeersMayHaveBlockNum(4) // peers which may have blocks 1,2,3,4
		require.Len(t, peerIds, 2)

		var errIncompleteHeaders *ErrIncompleteHeaders
		headers, err = test.trackingFetcher.FetchHeaders(ctx, 3, 5, peerId1) // fetch headers 3 and 4
		require.ErrorAs(t, err, &errIncompleteHeaders)                       // peer 1 does not have headers 3 and 4
		require.Equal(t, uint64(3), errIncompleteHeaders.start)
		require.Equal(t, uint64(2), errIncompleteHeaders.requested)
		require.Equal(t, uint64(0), errIncompleteHeaders.received)
		require.Equal(t, uint64(3), errIncompleteHeaders.LowestMissingBlockNum())
		require.Nil(t, headers.Data)

		// should be one peer less now given that we know that peer 1 does not have block num 4
		peerIds = test.peerTracker.ListPeersMayHaveBlockNum(4)
		require.Len(t, peerIds, 1)
	})
}

func TestTrackingFetcherFetchBodiesUpdatesPeerTracker(t *testing.T) {
	t.Parallel()

	peerId1 := PeerIdFromUint64(1)
	peerId2 := PeerIdFromUint64(2)
	requestId1 := uint64(1234)
	requestId2 := uint64(1235)
	requestId3 := uint64(1236)
	mockHeaders := []*types.Header{{Number: big.NewInt(1)}}
	mockHashes := []common.Hash{mockHeaders[0].Hash()}
	mockInboundMessages1 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_BODIES_66,
			PeerId: peerId1.H512(),
			Data:   newMockBlockBodiesPacketBytes(t, requestId1),
		},
	}
	mockRequestResponse1 := requestResponseMock{
		requestId:                   requestId1,
		mockResponseInboundMessages: mockInboundMessages1,
		wantRequestPeerId:           peerId1,
		wantRequestHashes:           mockHashes,
	}
	mockInboundMessages2 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_BODIES_66,
			PeerId: peerId2.H512(),
			Data:   nil, // response timeout
		},
	}
	mockRequestResponse2 := requestResponseMock{
		requestId:                   requestId2,
		mockResponseInboundMessages: mockInboundMessages2,
		wantRequestPeerId:           peerId2,
		wantRequestHashes:           mockHashes,
		responseDelay:               600 * time.Millisecond,
	}
	mockInboundMessages3 := []*sentry.InboundMessage{
		{
			Id:     sentry.MessageId_BLOCK_BODIES_66,
			PeerId: peerId2.H512(),
			Data:   nil, // response timeout
		},
	}
	mockRequestResponse3 := requestResponseMock{
		requestId:                   requestId3,
		mockResponseInboundMessages: mockInboundMessages3,
		wantRequestPeerId:           peerId2,
		wantRequestHashes:           mockHashes,
		responseDelay:               600 * time.Millisecond,
	}

	test := newTrackingFetcherTest(t, newMockRequestGenerator(requestId1, requestId2, requestId3))
	test.mockSentryStreams(mockRequestResponse1, mockRequestResponse2, mockRequestResponse3)
	test.run(func(ctx context.Context, t *testing.T) {
		test.simulateDefaultPeerEvents()
		var peerIds []*PeerId // peers which may have block 1
		require.Eventuallyf(t, func() bool {
			peerIds = test.peerTracker.ListPeersMayHaveBlockNum(1)
			return len(peerIds) == 2
		}, time.Second, 100*time.Millisecond, "expected number of initial peers never satisfied: want=2, have=%d", len(peerIds))

		bodies, err := test.trackingFetcher.FetchBodies(ctx, mockHeaders, peerId1)
		require.ErrorIs(t, err, &ErrMissingBodies{})
		require.Nil(t, bodies.Data)

		peerIds = test.peerTracker.ListPeersMayHaveBlockNum(1) // only peerId2 may have block 1, peerId does not
		require.Len(t, peerIds, 1)

		bodies, err = test.trackingFetcher.FetchBodies(ctx, mockHeaders, peerId2)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, bodies.Data)

		peerIds = test.peerTracker.ListPeersMayHaveBlockNum(1) // neither peerId1 nor peerId2 have block num 1
		require.Len(t, peerIds, 0)
	})
}

func newTrackingFetcherTest(t *testing.T, requestIdGenerator RequestIdGenerator) *trackingFetcherTest {
	fetcherTest := newFetcherTest(t, requestIdGenerator)
	logger := fetcherTest.logger
	sentryClient := fetcherTest.sentryClient
	messageListener := fetcherTest.messageListener
	peerTracker := NewPeerTracker(logger, sentryClient, messageListener, WithPreservingPeerShuffle)
	trackingFetcher := newTrackingFetcher(fetcherTest.fetcher, peerTracker)
	return &trackingFetcherTest{
		fetcherTest:     fetcherTest,
		trackingFetcher: trackingFetcher,
		peerTracker:     peerTracker,
	}
}

type trackingFetcherTest struct {
	*fetcherTest
	trackingFetcher        *trackingFetcher
	peerTracker            PeerTracker
	peerTrackerInitialised atomic.Bool
}

func (tft *trackingFetcherTest) run(f func(ctx context.Context, t *testing.T)) {
	var done atomic.Bool
	tft.t.Run("start", func(t *testing.T) {
		go func() {
			defer done.Store(true)
			eg, ctx := errgroup.WithContext(tft.ctx)
			eg.Go(func() error {
				return tft.peerTracker.Run(ctx)
			})
			eg.Go(func() error {
				// wait for the tracker to be initialised before simulating peer events,
				// otherwise the tests may be flake-y because simulated test events in each test
				// may be consumed by the listener background loop before the tracker has
				// registered its peer event observer which makes the test unreliable
				require.Eventually(tft.t, func() bool {
					return tft.peerTrackerInitialised.Load()
				}, time.Second, 100*time.Millisecond, "expected peer tracker to be initialised")

				return tft.messageListener.Run(ctx)
			})
			err := eg.Wait()
			require.ErrorIs(t, err, context.Canceled)
		}()
	})

	tft.t.Run("test", func(t *testing.T) {
		f(tft.ctx, t)
	})

	tft.t.Run("stop", func(t *testing.T) {
		tft.ctxCancel()
		require.Eventually(t, done.Load, time.Second, 5*time.Millisecond)
	})
}

func (tft *trackingFetcherTest) mockSentryStreams(mocks ...requestResponseMock) {
	tft.fetcherTest.mockSentryStreams(mocks...)

	tft.sentryClient.EXPECT().
		Peers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *emptypb.Empty, ...grpc.CallOption) (*sentry.PeersReply, error) {
			tft.peerTrackerInitialised.Store(true)
			return &sentry.PeersReply{}, nil
		}).
		Times(1)
}

func (tft *trackingFetcherTest) simulateDefaultPeerEvents() {
	tft.simulatePeerEvents([]*sentry.PeerEvent{
		{
			EventId: sentry.PeerEvent_Connect,
			PeerId:  PeerIdFromUint64(1).H512(),
		},
		{
			EventId: sentry.PeerEvent_Connect,
			PeerId:  PeerIdFromUint64(2).H512(),
		},
	})
}

func (tft *trackingFetcherTest) simulatePeerEvents(peerEvents []*sentry.PeerEvent) {
	for _, peerEvent := range peerEvents {
		tft.peerEvents <- &delayedMessage[*sentry.PeerEvent]{
			message: peerEvent,
		}
	}
}
