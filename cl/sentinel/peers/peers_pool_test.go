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

package peers

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRecordHandshakeSuccessClearsHandshakeFailuresAndUndialableStatus(t *testing.T) {
	t.Run("failure count", func(t *testing.T) {
		pool := NewPool(nil)
		pid := peer.ID("failure-count")
		pool.RecordHandshakeFailure(pid)
		pool.RecordHandshakeFailure(pid)

		pool.RecordHandshakeSuccess(pid)
		pool.RecordHandshakeFailure(pid)

		require.True(t, pool.Dialable(pid))
	})

	t.Run("undialable status", func(t *testing.T) {
		pool := NewPool(nil)
		pid := peer.ID("undialable-status")
		for range 3 {
			pool.RecordHandshakeFailure(pid)
		}
		require.False(t, pool.Dialable(pid))

		pool.RecordHandshakeSuccess(pid)

		require.True(t, pool.Dialable(pid))
	})

	t.Run("connection refusal", func(t *testing.T) {
		pool := NewPool(nil)
		pid := peer.ID("connection-refusal")
		for range 10 {
			pool.RecordHandshakeFailure(pid)
		}
		require.True(t, pool.RefuseConnections(pid))

		pool.RecordHandshakeSuccess(pid)

		require.False(t, pool.RefuseConnections(pid))
		require.True(t, pool.Dialable(pid))
	})
}

func TestHandshakeFailureThresholds(t *testing.T) {
	pool := NewPool(nil)
	pid := peer.ID("failure-thresholds")

	for failure := 1; failure <= 12; failure++ {
		count, becameUndialable := pool.RecordHandshakeFailure(pid)

		require.Equal(t, failure, count)
		require.Equal(t, failure == 3, becameUndialable)
		require.Equal(t, failure >= 10, pool.RefuseConnections(pid))
		require.Equal(t, failure < 3, pool.Dialable(pid))
	}
}

func TestHandshakeFailureRenewsUndialableMark(t *testing.T) {
	pool := NewPool(nil)
	pid := peer.ID("undialable-renewal")
	for range 3 {
		pool.RecordHandshakeFailure(pid)
	}
	pool.undialable.Remove(pid)

	pool.RecordHandshakeFailure(pid)

	require.False(t, pool.Dialable(pid))
}
