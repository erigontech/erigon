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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc/filters"
)

func newLimitedFilters(t *testing.T, maxSubscriptions int) *Filters {
	config := FiltersConfig{RpcSubscriptionFiltersMaxSubscriptions: maxSubscriptions}
	return New(t.Context(), config, nil, nil, nil, func() {}, log.New(), nil)
}

type subscriptionKind struct {
	name      string
	subscribe func(f *Filters) (unsubscribe func() bool, err error)
}

var subscriptionKinds = []subscriptionKind{
	{"heads", func(f *Filters) (func() bool, error) {
		_, id, err := f.SubscribeNewHeads(8, ProtocolHTTP)
		return func() bool { return f.UnsubscribeHeads(id) }, err
	}},
	{"pendingTxs", func(f *Filters) (func() bool, error) {
		_, id, err := f.SubscribePendingTxs(8, ProtocolWS)
		return func() bool { return f.UnsubscribePendingTxs(id) }, err
	}},
	{"syncing", func(f *Filters) (func() bool, error) {
		_, id, err := f.SubscribeSyncing(8, ProtocolWS)
		return func() bool { return f.UnsubscribeSyncing(id) }, err
	}},
	{"logs", func(f *Filters) (func() bool, error) {
		_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{}, ProtocolHTTP)
		return func() bool { return f.UnsubscribeLogs(id) }, err
	}},
	{"receipts", func(f *Filters) (func() bool, error) {
		_, id, err := f.SubscribeReceipts(8, filters.ReceiptsFilterCriteria{})
		return func() bool { return f.UnsubscribeReceipts(id) }, err
	}},
}

// One budget covers every kind of subscription, and an unsubscribe of any kind frees its slot.
func TestSubscriptionLimitSharedAcrossKindsAndReleasedOnUnsubscribe(t *testing.T) {
	f := newLimitedFilters(t, 1)

	for _, kind := range subscriptionKinds {
		unsubscribe, err := kind.subscribe(f)
		require.NoError(t, err, kind.name)
		for _, other := range subscriptionKinds {
			_, err := other.subscribe(f)
			require.ErrorIs(t, err, ErrTooManySubscriptions, "%s while %s holds the slot", other.name, kind.name)
		}
		require.True(t, unsubscribe(), kind.name)
	}

	_, _, err := f.SubscribeNewHeads(8, ProtocolHTTP)
	require.NoError(t, err)
}

func TestSubscriptionLimitReleasedOnEviction(t *testing.T) {
	f := newLimitedFilters(t, 1)

	_, id, err := f.SubscribeNewHeads(8, ProtocolHTTP)
	require.NoError(t, err)
	sub, ok := f.headsSubs.Get(id)
	require.True(t, ok)
	backdateSub(t, sub, 2*time.Hour)
	f.evictStaleSubscriptions(time.Hour)

	_, _, err = f.SubscribeNewHeads(8, ProtocolHTTP)
	require.NoError(t, err)
}

// A subscription that fails to install must not keep its slot.
func TestSubscriptionLimitReleasedWhenRemoteUpdateFails(t *testing.T) {
	f := newLimitedFilters(t, 1)
	remoteDown := errors.New("remote down")
	f.logsRequestor.Store(func(*remoteproto.LogsFilterRequest) error { return remoteDown })
	f.receiptsRequestor.Store(func(*remoteproto.ReceiptsFilterRequest) error { return remoteDown })

	_, _, err := f.SubscribeLogs(8, filters.FilterCriteria{}, ProtocolHTTP)
	require.ErrorIs(t, err, remoteDown)
	_, _, err = f.SubscribeReceipts(8, filters.ReceiptsFilterCriteria{})
	require.ErrorIs(t, err, remoteDown)

	_, _, err = f.SubscribeNewHeads(8, ProtocolHTTP)
	require.NoError(t, err)
}

func TestSubscriptionLimitZeroDisablesTheLimit(t *testing.T) {
	f := newLimitedFilters(t, 0)

	for range 3 {
		_, _, err := f.SubscribeNewHeads(8, ProtocolHTTP)
		require.NoError(t, err)
	}
}
