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

package membatchwithdb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
)

func BenchmarkMemoryMutationReadViewForAmount(b *testing.B) {
	for _, markerTable := range []string{"none", kv.MaxTxNum, kv.HeaderNumber} {
		b.Run("markers="+markerTable, func(b *testing.B) {
			db, rwTx := newTestTx(b)
			const rows = 200
			for i := range rows {
				require.NoError(b, rwTx.Put(kv.HeaderNumber, fmt.Appendf(nil, "key-%04d", i), []byte("db-value")))
			}
			require.NoError(b, rwTx.Commit())
			roTx, err := db.BeginTemporalRo(b.Context())
			require.NoError(b, err)
			defer roTx.Rollback()
			batch, err := membatchwithdb.NewMemoryBatch(roTx)
			require.NoError(b, err)
			defer batch.Close()
			if markerTable != "none" {
				require.NoError(b, batch.Delete(markerTable, []byte("absent")))
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				tx, err := db.BeginRo(b.Context())
				if err != nil {
					b.Error(err)
					return
				}
				defer tx.Rollback()
				view := batch.NewReadView(tx)
				for pb.Next() {
					count := 0
					err := view.ForAmount(kv.HeaderNumber, nil, rows, func(k, v []byte) error {
						count++
						return nil
					})
					if err != nil {
						b.Error(err)
						return
					}
					if count != rows {
						b.Errorf("read %d rows, want %d", count, rows)
						return
					}
				}
			})
		})
	}
}
