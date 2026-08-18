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

package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func logsWithIndexes(n int) types.Logs {
	logs := make(types.Logs, n)
	for i := range logs {
		logs[i] = &types.Log{Index: hexutil.Uint(i)}
	}
	return logs
}

func erigonLogsWithIndexes(n int) []*types.ErigonLog {
	logs := make([]*types.ErigonLog, n)
	for i, l := range logsWithIndexes(n) {
		logs[i] = &types.ErigonLog{Log: *l}
	}
	return logs
}

func TestAppendErigonLogs(t *testing.T) {
	const blockTime = 42

	cases := []struct {
		name       string
		logs       []*types.ErigonLog
		filtered   types.Logs
		maxResults int
		wantLen    int
		wantErr    bool
	}{
		{name: "unlimited", filtered: logsWithIndexes(3), maxResults: 0, wantLen: 3},
		{name: "below limit", filtered: logsWithIndexes(3), maxResults: 5, wantLen: 3},
		{name: "at limit", filtered: logsWithIndexes(3), maxResults: 3, wantLen: 3},
		{name: "above limit", filtered: logsWithIndexes(4), maxResults: 3, wantErr: true},
		{name: "limit counts logs appended earlier", logs: erigonLogsWithIndexes(2), filtered: logsWithIndexes(2), maxResults: 3, wantErr: true},
		{name: "nothing to append at limit", logs: erigonLogsWithIndexes(2), maxResults: 2, wantLen: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := appendErigonLogs(tc.logs, tc.filtered, blockTime, tc.maxResults)
			if tc.wantErr {
				require.Nil(t, got)
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				assert.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
				assert.Equal(t, fmt.Sprintf("%s: %d", errExceedLogResults, tc.maxResults), rpcErr.Error())
				return
			}
			require.NoError(t, err)
			require.Len(t, got, tc.wantLen)
			for i, l := range got[len(tc.logs):] {
				assert.Equal(t, tc.filtered[i].Index, l.Log.Index)
				assert.Equal(t, hexutil.Uint64(blockTime), l.Timestamp)
			}
		})
	}
}

var _ bridgeReader = mockBridgeReader{}

type mockBridgeReader struct {
	events []*types.Message
	err    error
	// stateSyncBlock is the block a state sync txn hash resolves to, mimicking the
	// bridge index that is the only place such a txn can be looked up.
	stateSyncBlock uint64
	stateSyncFound bool
}

func (b mockBridgeReader) Events(context.Context, common.Hash, uint64) ([]*types.Message, error) {
	return b.events, b.err
}

func (b mockBridgeReader) EventTxnLookup(context.Context, common.Hash) (uint64, bool, error) {
	return b.stateSyncBlock, b.stateSyncFound, b.err
}

func TestBorStateSyncLogs_NoEvents(t *testing.T) {
	api := &BaseAPI{bridgeReader: mockBridgeReader{}}
	logs, err := api.borStateSyncLogs(context.Background(), nil, nil, &types.Header{Number: *uint256.NewInt(1)}, 0, 0)
	require.NoError(t, err)
	assert.Empty(t, logs)
}

func TestBorStateSyncLogs_EventsError(t *testing.T) {
	wantErr := errors.New("bridge down")
	api := &BaseAPI{bridgeReader: mockBridgeReader{err: wantErr}}
	_, err := api.borStateSyncLogs(context.Background(), nil, nil, &types.Header{Number: *uint256.NewInt(1)}, 0, 0)
	require.ErrorIs(t, err, wantErr)
}
