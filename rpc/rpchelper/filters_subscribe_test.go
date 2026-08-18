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
	"github.com/erigontech/erigon/rpc"
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

func TestSubscribeLogsRejectsTooManyAddresses(t *testing.T) {
	for _, protocol := range []SubProtocol{ProtocolHTTP, ProtocolWS} {
		t.Run(string(protocol), func(t *testing.T) {
			config := FiltersConfig{
				RpcSubscriptionFiltersMaxAddresses: 1,
			}
			f := New(t.Context(), config, nil, nil, nil, func() {}, log.New(), nil)

			_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{
				Addresses: []common.Address{{1}, {2}},
			}, protocol)
			var invalidParams *rpc.InvalidParamsError
			require.ErrorAs(t, err, &invalidParams)
			require.EqualError(t, err, "log filter has 2 addresses, maximum is 1")
			require.Empty(t, id)
		})
	}
}

func TestSubscribeLogsRejectsTooManyTopics(t *testing.T) {
	config := FiltersConfig{
		RpcSubscriptionFiltersMaxTopics: 2,
	}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New(), nil)

	_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{
		Topics: [][]common.Hash{{{1}, {2}}, {{3}}},
	}, ProtocolHTTP)
	var invalidParams *rpc.InvalidParamsError
	require.ErrorAs(t, err, &invalidParams)
	require.EqualError(t, err, "log filter has 3 topic alternatives, maximum is 2")
	require.Empty(t, id)
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
