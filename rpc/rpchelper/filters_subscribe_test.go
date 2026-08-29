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
	"math/big"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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

func TestSubscribeLogsRejectsTooManyTopicPositions(t *testing.T) {
	for _, protocol := range []SubProtocol{ProtocolHTTP, ProtocolWS} {
		t.Run(string(protocol), func(t *testing.T) {
			f := newTestFilters(t)

			_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{
				Topics: make([][]common.Hash, filters.MaxTopicPositions+1),
			}, protocol)
			var invalidParams *rpc.InvalidParamsError
			require.ErrorAs(t, err, &invalidParams)
			require.EqualError(t, err, "query exceeds the maximum of 4 topics")
			require.Empty(t, id)
		})
	}
}

func TestSubscribeLogsAcceptsTopicAlternativesInFourPositions(t *testing.T) {
	criteria := filters.FilterCriteria{
		Topics: [][]common.Hash{
			{{1}, {2}},
			{{3}, {4}},
			{{5}, {6}},
			{{7}, {8}},
		},
	}
	for _, protocol := range []SubProtocol{ProtocolHTTP, ProtocolWS} {
		t.Run(string(protocol), func(t *testing.T) {
			f := newTestFilters(t)
			_, id, err := f.SubscribeLogs(8, criteria, protocol)
			require.NoError(t, err)
			t.Cleanup(func() { f.UnsubscribeLogs(id) })
		})
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

func TestSubscribeLogsOwnsStoredCriteria(t *testing.T) {
	f := newTestFilters(t)
	criteria := filters.FilterCriteria{
		FromBlock: big.NewInt(1),
		ToBlock:   big.NewInt(2),
		Addresses: common.Addresses{{1}},
		Topics:    [][]common.Hash{{{2}}},
	}

	_, id, err := f.SubscribeLogs(8, criteria, ProtocolHTTP)
	require.NoError(t, err)
	t.Cleanup(func() { f.UnsubscribeLogs(id) })

	criteria.FromBlock.SetInt64(10)
	criteria.ToBlock.SetInt64(20)
	criteria.Addresses[0] = common.Address{3}
	criteria.Topics[0][0] = common.Hash{4}

	stored, ok := f.LogFilterCriteria(id)
	require.True(t, ok)
	require.Equal(t, int64(1), stored.FromBlock.Int64())
	require.Equal(t, int64(2), stored.ToBlock.Int64())
	require.Equal(t, []common.Address{{1}}, stored.Addresses)
	require.Equal(t, [][]common.Hash{{{2}}}, stored.Topics)
}

func TestSubscribeLogsOwnsWebSocketTopics(t *testing.T) {
	f := newTestFilters(t)
	criteria := filters.FilterCriteria{
		Topics: [][]common.Hash{{{99, 99}}},
	}

	logs, id, err := f.SubscribeLogs(8, criteria, ProtocolWS)
	require.NoError(t, err)
	t.Cleanup(func() { f.UnsubscribeLogs(id) })

	criteria.Topics[0][0] = common.Hash{1}
	f.OnNewLogs(createLog())
	require.Len(t, logs, 1)
}

func TestSubscribeLogsIncludesBlockTimestamp(t *testing.T) {
	f := newTestFilters(t)
	logs, id, err := f.SubscribeLogs(8, filters.FilterCriteria{}, ProtocolWS)
	require.NoError(t, err)
	t.Cleanup(func() { f.UnsubscribeLogs(id) })
	event := createLog()
	event.BlockTimestamp = 123

	f.OnNewLogs(event)

	require.Equal(t, hexutil.Uint64(123), (<-logs).BlockTimestamp)
}

func TestSubscribeLogsPublishesInitializedFilter(t *testing.T) {
	f := newTestFilters(t)
	stop := make(chan struct{})
	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				f.OnNewLogs(createLog())
			}
		}
	})
	defer func() {
		close(stop)
		wg.Wait()
	}()
	<-started

	for range 1000 {
		_, id, err := f.SubscribeLogs(8, filters.FilterCriteria{}, ProtocolWS)
		require.NoError(t, err)
		require.True(t, f.UnsubscribeLogs(id))
	}
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
