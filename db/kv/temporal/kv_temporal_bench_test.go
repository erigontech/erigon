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

package temporal_test

import (
	"testing"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

// BenchmarkBeginTemporalRo measures the per-tx open cost. On this branch a tx
// also pins a block-files view (blocktx); the bench guards that hot path.
func BenchmarkBeginTemporalRo(b *testing.B) {
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic // benchmark loop; explicit Rollback below
		if err != nil {
			b.Fatal(err)
		}
		tx.Rollback()
	}
}
