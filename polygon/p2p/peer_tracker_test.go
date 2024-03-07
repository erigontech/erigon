package p2p

import (
	"context"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
)

func TestPeerTracker(t *testing.T) {
	t.Parallel()

	peerTracker := newPeerTracker()
	peerIds := peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 0)

	peerTracker.PeerConnected(PeerIdFromUint64(1))
	peerTracker.PeerConnected(PeerIdFromUint64(2))
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 2)
	sortPeerIdsAssumingUints(peerIds)
	require.Equal(t, PeerIdFromUint64(1), peerIds[0])
	require.Equal(t, PeerIdFromUint64(2), peerIds[1])

	peerTracker.BlockNumMissing(PeerIdFromUint64(1), 50)
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 1)
	require.Equal(t, PeerIdFromUint64(2), peerIds[0])

	peerTracker.BlockNumPresent(PeerIdFromUint64(1), 100)
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 2)
	sortPeerIdsAssumingUints(peerIds)
	require.Equal(t, PeerIdFromUint64(1), peerIds[0])
	require.Equal(t, PeerIdFromUint64(2), peerIds[1])

	peerTracker.PeerDisconnected(PeerIdFromUint64(2))
	peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
	require.Len(t, peerIds, 1)
	require.Equal(t, PeerIdFromUint64(1), peerIds[0])
}

func TestPeerTrackerPeerEventObserver(t *testing.T) {
	t.Parallel()

	peerTracker := newPeerTracker()
	peerTrackerPeerEventObserver := NewPeerEventObserver(peerTracker)
	messageListenerTest := newMessageListenerTest(t)
	messageListenerTest.mockSentryStreams()
	messageListenerTest.run(func(ctx context.Context, t *testing.T) {
		unregister := messageListenerTest.messageListener.RegisterPeerEventObserver(peerTrackerPeerEventObserver)
		t.Cleanup(unregister)

		messageListenerTest.peerEventsStream <- &delayedMessage[*sentry.PeerEvent]{
			message: &sentry.PeerEvent{
				PeerId:  PeerIdFromUint64(1).H512(),
				EventId: sentry.PeerEvent_Connect,
			},
		}

		messageListenerTest.peerEventsStream <- &delayedMessage[*sentry.PeerEvent]{
			message: &sentry.PeerEvent{
				PeerId:  PeerIdFromUint64(2).H512(),
				EventId: sentry.PeerEvent_Connect,
			},
		}

		var peerIds []*PeerId
		waitCond := func(wantPeerIdsLen int) func() bool {
			return func() bool {
				peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
				return len(peerIds) == wantPeerIdsLen
			}
		}
		require.Eventually(t, waitCond(2), time.Second, 5*time.Millisecond)
		require.Len(t, peerIds, 2)
		sortPeerIdsAssumingUints(peerIds)
		require.Equal(t, PeerIdFromUint64(1), peerIds[0])
		require.Equal(t, PeerIdFromUint64(2), peerIds[1])

		messageListenerTest.peerEventsStream <- &delayedMessage[*sentry.PeerEvent]{
			message: &sentry.PeerEvent{
				PeerId:  PeerIdFromUint64(1).H512(),
				EventId: sentry.PeerEvent_Disconnect,
			},
		}

		peerIds = peerTracker.ListPeersMayHaveBlockNum(100)
		require.Eventually(t, waitCond(1), time.Second, 5*time.Millisecond)
		require.Len(t, peerIds, 1)
		require.Equal(t, PeerIdFromUint64(2), peerIds[0])
	})
}

// sortPeerIdsAssumingUints is a hacky way for us to sort peer ids in tests - assuming they are all created
// by using PeerIdFromUint64 and so uint64 value is sorted in first 8 bytes. Sorting is needed since
// ListPeersMayHaveBlockNum returns peer ids from a map and order is non-deterministic.
func sortPeerIdsAssumingUints(peerIds []*PeerId) {
	sort.Slice(peerIds, func(i, j int) bool {
		bytesI := peerIds[i][:8]
		bytesJ := peerIds[j][:8]
		numI := binary.BigEndian.Uint64(bytesI)
		numJ := binary.BigEndian.Uint64(bytesJ)
		return numI < numJ
	})
}
