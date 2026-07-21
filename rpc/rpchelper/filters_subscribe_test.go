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
