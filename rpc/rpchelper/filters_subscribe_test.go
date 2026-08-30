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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces"
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

// distributeLog hands the same log to every subscriber, and the mutation it used to do
// wrote identical values back, so delivery succeeds either way: without -race this test
// asserts nothing.
func TestDistributeLogsLeavesDeliveredLogsUntouched(t *testing.T) {
	f := newTestFilters(t)
	var delivered atomic.Int64
	var wg sync.WaitGroup
	ids := make([]LogsSubID, 0, 8)
	for range 8 {
		logs, id, err := f.SubscribeLogs(64, filters.FilterCriteria{}, ProtocolWS)
		require.NoError(t, err)
		// Covers the FailNow path only, where wg.Wait() is never reached and the
		// consumers would block on a channel nobody closes.
		t.Cleanup(func() { f.UnsubscribeLogs(id) })
		ids = append(ids, id)
		wg.Go(func() {
			for lg := range logs {
				for _, topic := range lg.Topics {
					_ = topic
				}
				_ = lg.Address
				delivered.Add(1)
			}
		})
	}

	for range 2000 {
		f.OnNewLogs(createLog())
	}
	for _, id := range ids {
		f.UnsubscribeLogs(id)
	}
	wg.Wait()
	require.Positive(t, delivered.Load())
}

// The last LogsFilterRequest delivered to the remote log source must reflect every
// installed subscription: the remote replaces its filter with each request, so a
// stale request sent last silently drops the newer subscription's events.
func TestSubscribeLogsConcurrentSubscribersDoNotSendStaleRequest(t *testing.T) {
	f := newTestFilters(t)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	requestAddresses := func(r *remoteproto.LogsFilterRequest) map[common.Address]bool {
		addresses := make(map[common.Address]bool, len(r.GetAddresses()))
		for _, h160 := range r.GetAddresses() {
			addresses[gointerfaces.ConvertH160toAddress(h160)] = true
		}
		return addresses
	}

	var reqMu sync.Mutex
	var lastRequest *remoteproto.LogsFilterRequest
	var firstSend atomic.Bool
	firstSend.Store(true)
	firstSendEntered := make(chan struct{})
	releaseFirstSend := make(chan struct{})
	secondRequestSeen := make(chan struct{})
	var secondRequestSeenOnce sync.Once
	f.logsRequestor.Store(func(r *remoteproto.LogsFilterRequest) error {
		if firstSend.CompareAndSwap(true, false) {
			close(firstSendEntered)
			<-releaseFirstSend
		}
		reqMu.Lock()
		defer reqMu.Unlock()
		lastRequest = r
		if requestAddresses(r)[addr2] {
			secondRequestSeenOnce.Do(func() { close(secondRequestSeen) })
		}
		return nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, _, err := f.SubscribeLogs(8, filters.FilterCriteria{Addresses: []common.Address{addr1}}, ProtocolHTTP)
		firstDone <- err
	}()
	select {
	case <-firstSendEntered:
	case err := <-firstDone:
		t.Fatalf("first subscriber finished without sending a filter request: %v", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, _, err := f.SubscribeLogs(8, filters.FilterCriteria{Addresses: []common.Address{addr2}}, ProtocolHTTP)
		secondDone <- err
	}()

	// Unblock the first send only once the second subscriber's request has been
	// delivered (the racy interleaving), or after a grace period if sends are
	// serialized and the second subscriber is waiting for the first to finish.
	select {
	case <-secondRequestSeen:
	case <-time.After(300 * time.Millisecond):
	}
	close(releaseFirstSend)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	reqMu.Lock()
	defer reqMu.Unlock()
	finalAddresses := requestAddresses(lastRequest)
	require.True(t, finalAddresses[addr1], "last delivered request lost addr1: %v", finalAddresses)
	require.True(t, finalAddresses[addr2], "last delivered request lost addr2: %v", finalAddresses)
}

// Same invariant as the logs variant above, for the receipts filter stream.
func TestSubscribeReceiptsConcurrentSubscribersDoNotSendStaleRequest(t *testing.T) {
	f := newTestFilters(t)

	hash1 := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	hash2 := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")

	requestHashes := func(r *remoteproto.ReceiptsFilterRequest) map[common.Hash]bool {
		hashes := make(map[common.Hash]bool, len(r.GetTransactionHashes()))
		for _, h256 := range r.GetTransactionHashes() {
			hashes[gointerfaces.ConvertH256ToHash(h256)] = true
		}
		return hashes
	}

	var reqMu sync.Mutex
	var lastRequest *remoteproto.ReceiptsFilterRequest
	var firstSend atomic.Bool
	firstSend.Store(true)
	firstSendEntered := make(chan struct{})
	releaseFirstSend := make(chan struct{})
	secondRequestSeen := make(chan struct{})
	var secondRequestSeenOnce sync.Once
	f.receiptsRequestor.Store(func(r *remoteproto.ReceiptsFilterRequest) error {
		if firstSend.CompareAndSwap(true, false) {
			close(firstSendEntered)
			<-releaseFirstSend
		}
		reqMu.Lock()
		defer reqMu.Unlock()
		lastRequest = r
		if requestHashes(r)[hash2] {
			secondRequestSeenOnce.Do(func() { close(secondRequestSeen) })
		}
		return nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, _, err := f.SubscribeReceipts(8, filters.ReceiptsFilterCriteria{TransactionHashes: []common.Hash{hash1}})
		firstDone <- err
	}()
	select {
	case <-firstSendEntered:
	case err := <-firstDone:
		t.Fatalf("first subscriber finished without sending a filter request: %v", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, _, err := f.SubscribeReceipts(8, filters.ReceiptsFilterCriteria{TransactionHashes: []common.Hash{hash2}})
		secondDone <- err
	}()

	select {
	case <-secondRequestSeen:
	case <-time.After(300 * time.Millisecond):
	}
	close(releaseFirstSend)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	reqMu.Lock()
	defer reqMu.Unlock()
	finalHashes := requestHashes(lastRequest)
	require.True(t, finalHashes[hash1], "last delivered request lost hash1: %v", finalHashes)
	require.True(t, finalHashes[hash2], "last delivered request lost hash2: %v", finalHashes)
}
