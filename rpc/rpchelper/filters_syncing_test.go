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

package rpchelper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func syncingEvent(t *testing.T, reply *remoteproto.SyncingReply) *remoteproto.SubscribeReply {
	t.Helper()
	data, err := proto.Marshal(reply)
	require.NoError(t, err)
	return &remoteproto.SubscribeReply{Type: remoteproto.Event_SYNCING, Data: data}
}

func TestSyncingEventReachesSubscribers(t *testing.T) {
	f := newTestFilters(t)
	ch, id := f.SubscribeSyncing(8, ProtocolWS)
	defer f.UnsubscribeSyncing(id)

	f.OnNewEvent(syncingEvent(t, &remoteproto.SyncingReply{Syncing: true, CurrentBlock: 42, LastNewBlockSeen: 100}))

	select {
	case got := <-ch:
		require.True(t, got.Syncing)
		require.Equal(t, uint64(42), got.CurrentBlock)
		require.Equal(t, uint64(100), got.LastNewBlockSeen)
	default:
		t.Fatal("expected a syncing notification on the subscription channel")
	}
}

func TestSyncingEventAfterUnsubscribeIsNotDelivered(t *testing.T) {
	f := newTestFilters(t)
	ch, id := f.SubscribeSyncing(8, ProtocolWS)
	require.True(t, f.UnsubscribeSyncing(id))

	f.OnNewEvent(syncingEvent(t, &remoteproto.SyncingReply{Syncing: false}))

	select {
	case _, ok := <-ch:
		require.False(t, ok, "channel must be closed with no pending notifications")
	default:
	}
}

// Only the most recent sync state matters, so an overloaded subscriber must
// drop stale progress updates, never the newest event: the final synced
// transition is not re-published and would otherwise be lost for good.
func TestSyncingSlowSubscriberStillGetsLatestEvent(t *testing.T) {
	f := newTestFilters(t)
	ch, id := f.SubscribeSyncing(8, ProtocolWS)
	defer f.UnsubscribeSyncing(id)

	for block := uint64(1); block <= 12; block++ {
		f.OnNewEvent(syncingEvent(t, &remoteproto.SyncingReply{Syncing: true, CurrentBlock: block, LastNewBlockSeen: 100}))
	}
	f.OnNewEvent(syncingEvent(t, &remoteproto.SyncingReply{Syncing: false, CurrentBlock: 100, LastNewBlockSeen: 100}))

	var last *remoteproto.SyncingReply
drain:
	for {
		select {
		case reply := <-ch:
			last = reply
		default:
			break drain
		}
	}
	require.NotNil(t, last)
	require.False(t, last.Syncing, "the newest event must survive buffer overflow")
}

// A new subscriber is seeded with the last state seen on the shared event
// stream, so a transition that happened before subscribing is never lost:
// the producer only republishes on changes.
func TestSubscribeSyncingSeedsFromLastStreamEvent(t *testing.T) {
	f := newTestFilters(t)
	f.OnNewEvent(syncingEvent(t, &remoteproto.SyncingReply{Syncing: false, CurrentBlock: 100, LastNewBlockSeen: 100}))

	ch, id := f.SubscribeSyncing(8, ProtocolWS)
	defer f.UnsubscribeSyncing(id)

	select {
	case got := <-ch:
		require.False(t, got.Syncing)
		require.Equal(t, uint64(100), got.CurrentBlock)
	default:
		t.Fatal("expected the last stream state as seed on the new subscription")
	}
}

func TestSubscribeSyncingBeforeAnyEventHasNoSeed(t *testing.T) {
	f := newTestFilters(t)
	ch, id := f.SubscribeSyncing(8, ProtocolWS)
	defer f.UnsubscribeSyncing(id)

	select {
	case got := <-ch:
		t.Fatalf("no event seen yet, no seed expected, got %v", got)
	default:
	}
}

func TestSubscribeSyncingIsTracked(t *testing.T) {
	f := newTestFilters(t)
	_, id := f.SubscribeSyncing(8, ProtocolWS)
	require.True(t, f.hasTrackedSub(SubscriptionID(id)))

	require.True(t, f.UnsubscribeSyncing(id))
	require.False(t, f.hasTrackedSub(SubscriptionID(id)))
}

func TestEvictStaleSubscriptionsRemovesIdleSyncingFilter(t *testing.T) {
	f := newTestFilters(t)
	ch, id := f.SubscribeSyncing(8, ProtocolHTTP)

	sub, ok := f.syncingSubs.Get(id)
	require.True(t, ok)
	backdateSub[*remoteproto.SyncingReply](t, sub, 2*time.Hour)

	f.evictStaleSubscriptions(time.Hour)

	_, ok = f.syncingSubs.Get(id)
	require.False(t, ok)
	require.False(t, f.hasTrackedSub(SubscriptionID(id)))
	_, open := <-ch
	require.False(t, open)
}

func TestSyncingEventWithCorruptPayloadIsDropped(t *testing.T) {
	f := newTestFilters(t)
	ch, id := f.SubscribeSyncing(8, ProtocolWS)
	defer f.UnsubscribeSyncing(id)

	f.OnNewEvent(&remoteproto.SubscribeReply{Type: remoteproto.Event_SYNCING, Data: []byte{0xff, 0xff, 0xff}})

	select {
	case <-ch:
		t.Fatal("corrupt payload must not produce a notification")
	default:
	}
}
