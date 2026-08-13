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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces"
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

func TestSubscribeReceiptsRemoteUpdateFailureReturnsError(t *testing.T) {
	f := newTestFilters(t)
	f.receiptsRequestor.Store(func(*remoteproto.ReceiptsFilterRequest) error {
		return errors.New("remote receipts source unavailable")
	})

	_, id, err := f.SubscribeReceipts(8, filters.ReceiptsFilterCriteria{})
	require.Error(t, err)
	require.False(t, f.receiptsSubs.removeReceiptsFilter(id))
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
