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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc/filters"
)

func newTestFilters(t *testing.T) *Filters {
	return New(t.Context(), DefaultFiltersConfig, nil, nil, nil, func() {}, log.New(), nil)
}

func backdateSub[T any](t *testing.T, sub Sub[T], age time.Duration) {
	t.Helper()
	cs, ok := sub.(*chan_sub[T])
	require.True(t, ok)
	cs.lock.Lock()
	cs.lastAccess = time.Now().Add(-age)
	cs.lock.Unlock()
}

func TestEvictStaleSubscriptionsRemovesIdleFilters(t *testing.T) {
	f := newTestFilters(t)

	headsCh, headsID := f.SubscribeNewHeads(8)
	f.TrackSubscription(SubscriptionID(headsID), FilterTypeHeads, ProtocolHTTP)
	txsCh, txsID := f.SubscribePendingTxs(8)
	f.TrackSubscription(SubscriptionID(txsID), FilterTypePendingTxs, ProtocolHTTP)
	logsCh, logsID := f.SubscribeLogs(8, filters.FilterCriteria{})
	f.TrackSubscription(SubscriptionID(logsID), FilterTypeLogs, ProtocolHTTP)

	headsSub, ok := f.headsSubs.Get(headsID)
	require.True(t, ok)
	backdateSub(t, headsSub, 2*time.Hour)
	txsSub, ok := f.pendingTxsSubs.Get(txsID)
	require.True(t, ok)
	backdateSub(t, txsSub, 2*time.Hour)
	logsFilter, ok := f.logsSubs.logsFilters.Get(logsID)
	require.True(t, ok)
	backdateSub(t, logsFilter.sender, 2*time.Hour)

	f.evictStaleSubscriptions(time.Hour)

	require.False(t, f.HasHeadsSubscription(headsID))
	require.False(t, f.HasPendingTxsSubscription(txsID))
	require.False(t, f.HasSubscription(logsID))

	_, open := <-headsCh
	require.False(t, open)
	_, open = <-txsCh
	require.False(t, open)
	_, open = <-logsCh
	require.False(t, open)
}

func TestTouchSubscriptionPreventsEviction(t *testing.T) {
	f := newTestFilters(t)

	_, touchedID := f.SubscribePendingTxs(8)
	f.TrackSubscription(SubscriptionID(touchedID), FilterTypePendingTxs, ProtocolHTTP)
	_, idleID := f.SubscribePendingTxs(8)
	f.TrackSubscription(SubscriptionID(idleID), FilterTypePendingTxs, ProtocolHTTP)

	for _, id := range []PendingTxsSubID{touchedID, idleID} {
		sub, ok := f.pendingTxsSubs.Get(id)
		require.True(t, ok)
		backdateSub(t, sub, 2*time.Hour)
	}

	f.TouchSubscription(SubscriptionID(touchedID), FilterTypePendingTxs)
	f.evictStaleSubscriptions(time.Hour)

	require.True(t, f.HasPendingTxsSubscription(touchedID))
	require.False(t, f.HasPendingTxsSubscription(idleID))
}

func TestEvictStaleSubscriptionsSkipsWebSocketSubscriptions(t *testing.T) {
	f := newTestFilters(t)

	_, id := f.SubscribeNewHeads(8)
	f.SetSubscriptionProtocol(SubscriptionID(id), FilterTypeHeads, ProtocolWS)

	f.evictStaleSubscriptions(time.Nanosecond)

	require.True(t, f.HasHeadsSubscription(id))
}

func TestConcurrentTouchAndEviction(t *testing.T) {
	f := newTestFilters(t)

	_, id := f.SubscribePendingTxs(8)
	f.TrackSubscription(SubscriptionID(id), FilterTypePendingTxs, ProtocolHTTP)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.TouchSubscription(SubscriptionID(id), FilterTypePendingTxs)
			}
		}
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		f.evictStaleSubscriptions(50 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	require.True(t, f.HasPendingTxsSubscription(id))
}
