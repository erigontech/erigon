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

package prune_test

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
)

func BenchmarkHistoryPrunePrefixVal(b *testing.B) {
	const totalDups = 1 << 16
	for _, dups := range []int{2, 4, 6, 8, 64, 1024} {
		b.Run(fmt.Sprintf("dups=%d", dups), func(b *testing.B) {
			benchHistoryPrune(b, totalDups/dups, dups)
		})
	}
}

func benchHistoryPrune(b *testing.B, keys, dups int) {
	db := openTestDupSortDB(b)
	defer db.Close()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()
	logger := log.New()

	b.ResetTimer()
	for range b.N {
		historyPruneOnce(b, db, keys, dups, logEvery, logger)
	}
}

func historyPruneOnce(b *testing.B, db kv.RwDB, keys, dups int, logEvery *time.Ticker, logger log.Logger) {
	b.StopTimer()
	tx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()

	key := make([]byte, 52)
	val := make([]byte, 8+32)
	for k := range keys {
		binary.BigEndian.PutUint64(key[44:], uint64(k))
		for d := range dups {
			binary.BigEndian.PutUint64(val, uint64(d*keys+k))
			require.NoError(b, tx.Put(testDupSortTable, key, val))
		}
	}
	cur := openDupSortPseudoCursor(b, tx)
	defer cur.Close()
	b.StartTimer()

	stat, err := prune.TableScanningPrune(
		b.Context(), "bench", "storage",
		0, uint64(keys*dups/2), 16, logEvery, logger,
		nil, cur, &prune.Stat{}, prune.PrefixValStorageMode,
	)

	b.StopTimer()
	require.NoError(b, err)
	require.EqualValues(b, keys*dups/2, stat.PruneCountValues)
}
