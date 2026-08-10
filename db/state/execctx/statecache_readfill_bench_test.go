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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

// benchSeedDb commits one account so the domain tables are non-empty for
// cold-negative probes.
func benchSeedDb(b *testing.B) kv.TemporalRwDB {
	b.Helper()
	const stepSize = uint64(16)
	ctx := b.Context()
	db := newTestDb(b, stepSize)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(b, err)
	defer sd.Close()
	written := make([]byte, 20)
	written[0] = 0x01
	sd.SetTxNum(100)
	require.NoError(b, sd.DomainPut(kv.AccountsDomain, rwTx, written, encAccount(7), 100, nil))
	require.NoError(b, sd.Commit(ctx, rwTx))
	return db
}

// benchColdNegativeReads drives the full cold-negative SD read: the whole
// miss stack plus the generation-checked fill when a cache is wired.
func benchColdNegativeReads(b *testing.B, withCache, writable bool) {
	db := benchSeedDb(b)
	ctx := b.Context()
	var tx kv.TemporalTx
	var err error
	if writable {
		tx, err = db.BeginTemporalRw(ctx)
	} else {
		tx, err = db.BeginTemporalRo(ctx)
	}
	require.NoError(b, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(b, err)
	defer sd.Close()
	if withCache {
		stateCache := newSmallStateCache()
		defer stateCache.Close()
		sd.SetStateCacheForTest(stateCache)
	}

	key := make([]byte, 20)
	key[0] = 0x02
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[12:], uint64(i)+1)
		v, _, err := sd.GetLatest(kv.AccountsDomain, tx, key)
		if err != nil {
			b.Fatal(err)
		}
		if len(v) != 0 {
			b.Fatalf("expected a negative, got %x", v)
		}
	}
}

func BenchmarkGetLatestColdNegative(b *testing.B) { benchColdNegativeReads(b, true, false) }

// The baseline for the generation check and fill.
func BenchmarkGetLatestColdNegativeNoCache(b *testing.B) {
	benchColdNegativeReads(b, false, false)
}

func BenchmarkGetLatestColdNegativeRw(b *testing.B) { benchColdNegativeReads(b, true, true) }

func BenchmarkGetLatestColdNegativeRwNoCache(b *testing.B) {
	benchColdNegativeReads(b, false, true)
}

var benchmarkTemporalGetter kv.TemporalGetter

func benchmarkCacheGetterConstruction(b *testing.B, resolveVisibleEnds bool) {
	db := benchSeedDb(b)
	ctx := b.Context()
	baseTx, err := db.BeginTemporalRo(ctx)
	require.NoError(b, err)
	defer baseTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, baseTx, log.New())
	require.NoError(b, err)
	defer sd.Close()
	stateCache := newSmallStateCache()
	defer stateCache.Close()
	sd.SetStateCacheForTest(stateCache)

	domains := [...]kv.Domain{
		kv.AccountsDomain,
		kv.StorageDomain,
		kv.CodeDomain,
		kv.CommitmentDomain,
	}
	b.ResetTimer()
	b.StopTimer()
	for range b.N {
		tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic // benchmark loop; explicit Rollback below
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if resolveVisibleEnds {
			debug := tx.Debug()
			for _, domain := range domains {
				debug.DomainVisibleEnd(domain)
			}
		}
		benchmarkTemporalGetter = sd.AsGetter(tx)
		b.StopTimer()
		tx.Rollback()
	}
}

func BenchmarkCacheGetterConstruction(b *testing.B) {
	b.Run("exactness_check", func(b *testing.B) {
		benchmarkCacheGetterConstruction(b, false)
	})
	b.Run("visible_end_resolution", func(b *testing.B) {
		benchmarkCacheGetterConstruction(b, true)
	})
}
