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

package execctx_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

func commitmentPutCorpus(n int) []commitment.BranchDelta {
	rng := rand.New(rand.NewPCG(1, 2))
	writes := make([]commitment.BranchDelta, n)
	for i := range writes {
		key := make([]byte, 37)
		key[0] = 0x41
		for j := 1; j < len(key); j++ {
			key[j] = byte(rng.Uint32())
		}
		data := make([]byte, 300+rng.IntN(150))
		prev := make([]byte, len(data))
		for j := range data {
			data[j] = byte(rng.Uint32())
			prev[j] = byte(rng.Uint32())
		}
		writes[i] = commitment.BranchDelta{Key: key, Data: data, Prev: prev}
	}
	return writes
}

func BenchmarkCommitmentPut(b *testing.B) {
	const records = 900_000
	writes := commitmentPutCorpus(records)
	var parts [][]commitment.BranchDelta
	for rest := writes; len(rest) > 0; {
		n := min(1+len(parts)%20, len(rest))
		parts = append(parts, rest[:n])
		rest = rest[n:]
	}
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()))
	ctx := b.Context()

	run := func(b *testing.B, put func(sd *execctx.SharedDomains, tx kv.TemporalTx) error) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
			require.NoError(b, err)
			sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
			require.NoError(b, err)
			b.StartTimer()
			require.NoError(b, put(sd, tx))
			b.StopTimer()
			sd.Close()
			tx.Rollback()
			b.StartTimer()
		}
	}

	b.Run("batch", func(b *testing.B) {
		run(b, func(sd *execctx.SharedDomains, tx kv.TemporalTx) error {
			return sd.PutCommitmentBranches(tx, parts, 1, nil)
		})
	})

	b.Run("per-record", func(b *testing.B) {
		run(b, func(sd *execctx.SharedDomains, tx kv.TemporalTx) error {
			for _, w := range writes {
				if err := sd.DomainPutCommitmentDiff(tx, w.Key, w.Data, 1, w.Prev, nil); err != nil {
					return err
				}
			}
			return nil
		})
	})
}
