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

package engineapitester

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc"
)

type dataError struct{ data any }

func (dataError) Error() string    { return "payload rejected" }
func (dataError) ErrorCode() int   { return -38005 }
func (e dataError) ErrorData() any { return e.data }

func TestRPCErrorString(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{errors.New("plain"), fmt.Sprintf("%d: plain", rpc.ErrCodeDefault)},
		{&rpc.InvalidParamsError{Message: "bad params"}, "-32602: bad params"},
		{dataError{nil}, "-38005: payload rejected"},
		{dataError{"the cause"}, "-38005: payload rejected: the cause"},
		{dataError{map[string]string{"err": "the cause"}}, `-38005: payload rejected: {"err":"the cause"}`},
		{fmt.Errorf("wrapped: %w", dataError{"the cause"}), "-38005: wrapped: payload rejected: the cause"},
	} {
		require.Equal(t, tc.want, rpcErrorString(tc.err))
	}
}

// answers returns a call that answers SYNCING syncing times, then VALID. A
// negative syncing answers SYNCING forever.
func answers(syncing int) func() (*enginetypes.PayloadStatus, error) {
	return func() (*enginetypes.PayloadStatus, error) {
		if syncing != 0 {
			syncing--
			return &enginetypes.PayloadStatus{Status: enginetypes.SyncingStatus}, nil
		}
		return &enginetypes.PayloadStatus{Status: enginetypes.ValidStatus}, nil
	}
}

func TestRetrySyncingWaitsForAnAnswer(t *testing.T) {
	status, err := retrySyncing(context.Background(), time.Minute, nil, answers(3))
	require.NoError(t, err)
	require.Equal(t, enginetypes.ValidStatus, status.Status)
}

func TestRetrySyncingFailsAfterTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	start := time.Now()
	_, err := retrySyncing(context.Background(), timeout, nil, answers(-1))
	require.ErrorIs(t, err, errStillSyncing)
	require.GreaterOrEqual(t, time.Since(start), timeout)
}

func TestRetrySyncingReturnsSyncingWhenStuck(t *testing.T) {
	status, err := retrySyncing(context.Background(), time.Minute, func() bool { return true }, answers(-1))
	require.NoError(t, err)
	require.Equal(t, enginetypes.SyncingStatus, status.Status)
}
