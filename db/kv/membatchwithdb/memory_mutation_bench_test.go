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

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
)

func BenchmarkReadViewForAmount(b *testing.B) {
	for _, deleted := range []int{0, 1000} {
		b.Run(fmt.Sprintf("deleted=%d", deleted), func(b *testing.B) {
			db, tx := newTestTx(b)
			const rows = 200
			for i := range rows {
				require.NoError(b, tx.Put(kv.EthTx, hexutil.EncodeTs(uint64(i)), make([]byte, 128)))
			}
			require.NoError(b, tx.Commit())
			base, err := db.BeginTemporalRo(b.Context())
			require.NoError(b, err)
			defer base.Rollback()
			overlay, err := membatchwithdb.NewMemoryBatch(base)
			require.NoError(b, err)
			defer overlay.Close()
			for i := range deleted {
				require.NoError(b, overlay.Delete(kv.EthTx, hexutil.EncodeTs(uint64(rows+i))))
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				roTx, err := db.BeginTemporalRo(b.Context())
				if err != nil {
					b.Error(err)
					return
				}
				defer roTx.Rollback()
				view := overlay.NewReadView(roTx)
				for pb.Next() {
					var count int
					err := view.ForAmount(kv.EthTx, nil, rows, func(_, _ []byte) error {
						count++
						return nil
					})
					if err != nil || count != rows {
						b.Errorf("ForAmount: count=%d, err=%v", count, err)
						return
					}
				}
			})
			b.StopTimer()
		})
	}
}
