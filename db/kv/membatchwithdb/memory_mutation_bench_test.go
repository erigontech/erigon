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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
)

func BenchmarkMemoryMutationReadViewForAmount(b *testing.B) {
	for _, markers := range []struct {
		name, table string
		count       int
	}{
		{"none", "", 0},
		{kv.MaxTxNum, kv.MaxTxNum, 1},
		{kv.Headers, kv.Headers, 1},
		{"many", kv.Headers, 10_000},
	} {
		b.Run("markers="+markers.name, func(b *testing.B) {
			db, rwTx := newTestTx(b)
			const rows = 200
			for i := range rows {
				require.NoError(b, rwTx.Put(kv.Headers, dbutils.HeaderKey(uint64(i+1), common.Hash{}), []byte("db-value")))
			}
			require.NoError(b, rwTx.Commit())
			roTx, err := db.BeginTemporalRo(b.Context())
			require.NoError(b, err)
			defer roTx.Rollback()
			batch, err := membatchwithdb.NewMemoryBatch(roTx)
			require.NoError(b, err)
			defer batch.Close()
			for i := range markers.count {
				key := dbutils.HeaderKey(uint64(rows+i+1), common.Hash{})
				if markers.table == kv.MaxTxNum {
					key = dbutils.EncodeBlockNumber(uint64(rows + i + 1))
				}
				require.NoError(b, batch.Delete(markers.table, key))
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
					err := view.ForAmount(kv.Headers, nil, rows, func(k, v []byte) error {
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

func BenchmarkMemoryMutationDeleteBatch(b *testing.B) {
	for _, rows := range []int{1, 200, 10_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			_, tx := newTestTx(b)
			keys := make([][]byte, rows)
			for i := range keys {
				keys[i] = dbutils.HeaderKey(uint64(i+1), common.Hash{})
			}
			for b.Loop() {
				batch, err := membatchwithdb.NewMemoryBatch(tx)
				if err != nil {
					b.Fatal(err)
				}
				for _, key := range keys {
					if err := batch.Delete(kv.Headers, key); err != nil {
						b.Fatal(err)
					}
				}
				batch.Close()
			}
		})
	}
}

func BenchmarkMemoryMutationReadViewGetOne(b *testing.B) {
	for _, table := range []string{kv.MaxTxNum, kv.Headers} {
		b.Run("markers="+table, func(b *testing.B) {
			db, tx := newTestTx(b)
			key, value := dbutils.HeaderKey(1, common.Hash{}), []byte("value")
			require.NoError(b, tx.Put(kv.Headers, key, value))
			require.NoError(b, tx.Commit())
			roTx, err := db.BeginTemporalRo(b.Context())
			require.NoError(b, err)
			defer roTx.Rollback()
			batch, err := membatchwithdb.NewMemoryBatch(roTx)
			require.NoError(b, err)
			defer batch.Close()
			marker := dbutils.HeaderKey(2, common.Hash{})
			if table == kv.MaxTxNum {
				marker = dbutils.EncodeBlockNumber(2)
			}
			require.NoError(b, batch.Delete(table, marker))
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
					got, err := view.GetOne(kv.Headers, key)
					if err != nil {
						b.Error(err)
						return
					}
					if !bytes.Equal(got, value) {
						b.Errorf("read %q, want %q", got, value)
						return
					}
				}
			})
		})
	}
}
