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

package temporaltest

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

type HistoryEntry struct {
	Key    string
	TxNum  uint64
	Before string
}

// ReadDomain returns non-empty latest values, with keys and values encoded as hex.
func ReadDomain(tb testing.TB, tx kv.TemporalTx, domain kv.Domain) map[string]string {
	tb.Helper()
	entries, err := tx.Debug().RangeLatest(domain, nil, nil, kv.Unlim)
	require.NoError(tb, err)
	defer entries.Close()
	values := make(map[string]string)
	for entries.HasNext() {
		key, value, err := entries.Next()
		require.NoError(tb, err)
		if len(value) != 0 {
			values[hex.EncodeToString(key)] = hex.EncodeToString(value)
		}
	}
	return values
}

// ReadHistory returns all history entries, with keys and previous values encoded
// as hex. The scan has no upper bound, so it includes history left above the head.
func ReadHistory(tb testing.TB, tx kv.TemporalTx, domain kv.Domain) []HistoryEntry {
	tb.Helper()
	entries, err := tx.Debug().HistoryKeyTxNumRange(domain, -1, -1, order.Asc, kv.Unlim)
	require.NoError(tb, err)
	defer entries.Close()
	var history []HistoryEntry
	for entries.HasNext() {
		key, txNum, err := entries.Next()
		require.NoError(tb, err)
		before, ok, err := tx.HistorySeek(domain, key, txNum)
		require.NoError(tb, err)
		require.True(tb, ok, "%s history index must have a matching value", domain)
		history = append(history, HistoryEntry{
			Key: hex.EncodeToString(key), TxNum: txNum, Before: hex.EncodeToString(before),
		})
	}
	return history
}
