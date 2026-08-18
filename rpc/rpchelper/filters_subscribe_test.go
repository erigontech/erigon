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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc/filters"
)

// A filter id must never be handed out for a subscription that is not installed:
// when the remote filter update fails, Subscribe* reports the error instead.
func TestSubscribeLogsRemoteUpdateFailureReturnsError(t *testing.T) {
	f := newTestFilters(t)
	f.logsRequestor.Store(func(*remoteproto.LogsFilterRequest) error {
		return errors.New("remote logs source unavailable")
	})

	_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{}, ProtocolHTTP)
	require.Error(t, err)
	_, ok := f.logsSubs.logsFilters.Get(id)
	require.False(t, ok)
	require.False(t, f.hasTrackedSub(SubscriptionID(id)))
}

func TestSubscribeLogsStoresBoundedCriteriaForHTTP(t *testing.T) {
	address1 := common.Address{1}
	address2 := common.Address{2}
	topic1 := common.Hash{1}
	topic2 := common.Hash{2}
	topic3 := common.Hash{3}
	criteria := filters.FilterCriteria{
		Addresses: []common.Address{address1, address2},
		Topics:    [][]common.Hash{{topic1, topic2}, {topic3}},
	}
	addressStorage := &criteria.Addresses[0]
	topicStorage := &criteria.Topics[0][0]
	config := FiltersConfig{
		RpcSubscriptionFiltersMaxAddresses: 1,
		RpcSubscriptionFiltersMaxTopics:    2,
	}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New(), nil)

	_, id, err := f.SubscribeLogs(8, criteria, ProtocolHTTP)
	require.NoError(t, err)
	t.Cleanup(func() { f.UnsubscribeLogs(id) })

	stored, ok := f.LogFilterCriteria(id)
	require.True(t, ok)
	require.Equal(t, []common.Address{address1}, stored.Addresses)
	require.Equal(t, [][]common.Hash{{topic1, topic2}, {}}, stored.Topics)
	require.Same(t, addressStorage, &stored.Addresses[0])
	require.Same(t, topicStorage, &stored.Topics[0][0])
	require.Equal(t, len(stored.Addresses), cap(stored.Addresses))
	require.Equal(t, len(stored.Topics), cap(stored.Topics))
	for _, topics := range stored.Topics {
		require.Equal(t, len(topics), cap(topics))
	}
}

func TestSubscribeLogsDoesNotStoreCriteriaForWebSocket(t *testing.T) {
	f := newTestFilters(t)

	_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{
		Addresses: []common.Address{{1}},
		Topics:    [][]common.Hash{{{1}}},
	}, ProtocolWS)
	require.NoError(t, err)
	t.Cleanup(func() { f.UnsubscribeLogs(id) })

	_, ok := f.LogFilterCriteria(id)
	require.False(t, ok)
}

func TestSubscribeReceiptsRemoteUpdateFailureReturnsError(t *testing.T) {
	f := newTestFilters(t)
	f.receiptsRequestor.Store(func(*remoteproto.ReceiptsFilterRequest) error {
		return errors.New("remote receipts source unavailable")
	})

	_, id, err := f.SubscribeReceipts(8, filters.ReceiptsFilterCriteria{})
	require.Error(t, err)
	require.False(t, f.receiptsSubs.removeReceiptsFilter(id))
}
