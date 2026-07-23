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

package shards

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestSyncStateSubscriptionReceivesPublishedReply(t *testing.T) {
	events := NewEvents()
	ch, unsubscribe := events.AddSyncStateSubscription()
	defer unsubscribe()

	published := &remoteproto.SyncingReply{Syncing: true, CurrentBlock: 42, LastNewBlockSeen: 100}
	events.OnNewSyncState(published)

	select {
	case got := <-ch:
		require.Equal(t, published, got)
	default:
		t.Fatal("expected a sync state notification on the subscription channel")
	}
}

func TestSyncStateSubscriptionUnsubscribeStopsDelivery(t *testing.T) {
	events := NewEvents()
	ch, unsubscribe := events.AddSyncStateSubscription()
	unsubscribe()

	events.OnNewSyncState(&remoteproto.SyncingReply{Syncing: false})

	if _, ok := <-ch; ok {
		t.Fatal("expected closed channel with no pending notifications after unsubscribe")
	}
}
