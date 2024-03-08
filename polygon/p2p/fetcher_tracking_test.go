package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

func TestTrackingFetcherUpdatesPeerTracker(t *testing.T) {
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
		var peerIds []*PeerId // peers which may have blocks 1 and 2
		require.Eventuallyf(t, func() bool {
			peerIds = test.peerTracker.ListPeersMayHaveBlockNum(2)
			return len(peerIds) == 2
		}, time.Second, 100*time.Millisecond, "expected number of initial peers never satisfied: want=2, have=%d", len(peerIds))

		headers, err := test.trackingFetcher.FetchHeaders(ctx, 1, 3, peerId1) // fetch headers 1 and 2
		require.NoError(t, err)
		require.Len(t, headers, 2)
		require.Equal(t, uint64(1), headers[0].Number.Uint64())
		require.Equal(t, uint64(2), headers[1].Number.Uint64())

		peerIds = test.peerTracker.ListPeersMayHaveBlockNum(4) // peers which may have blocks 1,2,3,4
		require.Len(t, peerIds, 2)

		var errIncompleteHeaders *ErrIncompleteHeaders
		headers, err = test.trackingFetcher.FetchHeaders(ctx, 3, 5, peerId1) // fetch headers 3 and 4
		require.ErrorAs(t, err, &errIncompleteHeaders)                       // peer 1 does not have headers 3 and 4
		require.Equal(t, uint64(3), errIncompleteHeaders.start)
		require.Equal(t, uint64(2), errIncompleteHeaders.requested)
		require.Equal(t, uint64(0), errIncompleteHeaders.received)
		require.Equal(t, uint64(3), errIncompleteHeaders.LowestMissingBlockNum())
		require.Nil(t, headers)

		// should be one peer less now given that we know that peer 1 does not have block num 4
		peerIds = test.peerTracker.ListPeersMayHaveBlockNum(4)
		require.Len(t, peerIds, 1)
	})
}

func newTrackingFetcherTest(t *testing.T, requestIdGenerator RequestIdGenerator) *trackingFetcherTest {
	fetcherTest := newFetcherTest(t, requestIdGenerator)
	peerTracker := NewPeerTracker()
	unregister := fetcherTest.messageListener.RegisterPeerEventObserver(NewPeerEventObserver(peerTracker))
	t.Cleanup(unregister)
	trackingFetcher := newTrackingFetcher(fetcherTest.fetcher, peerTracker)
	return &trackingFetcherTest{
		fetcherTest:     fetcherTest,
		trackingFetcher: trackingFetcher,
		peerTracker:     peerTracker,
	}
}

type trackingFetcherTest struct {
	*fetcherTest
	trackingFetcher *trackingFetcher
	peerTracker     PeerTracker
}
