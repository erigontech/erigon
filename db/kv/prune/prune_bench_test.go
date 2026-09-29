// Copyright 2024 The Erigon Authors
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/prune"
)

func BenchmarkTableScanningPrune(b *testing.B) {
	db := openTestDB(b)
	defer db.Close()

	const N = 10_000
	tx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()
	insertEntries(b, tx, N, 0) // txNums 0..N-1; prune [0, N/2)

	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()
	logger := log.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur := openPseudoCursor(b, tx)
		prune.TableScanningPrune( //nolint:errcheck
			b.Context(), "bench", "txlookup",
			0, N/2, 1, logEvery, logger,
			nil, cur, false, &prune.Stat{}, prune.ValueOffset8StorageMode,
		)
		cur.Close()
	}
}
