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

// BenchmarkDomainVisibleEnd isolates the transaction-local cached frontier
// lookup used by repeated cache fills.
func BenchmarkDomainVisibleEnd(b *testing.B) {
	db := benchSeedDb(b)
	roTx, err := db.BeginTemporalRo(b.Context())
	require.NoError(b, err)
	defer roTx.Rollback()
	_, _ = roTx.Debug().DomainVisibleEnd(kv.AccountsDomain)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = roTx.Debug().DomainVisibleEnd(kv.AccountsDomain)
	}
}

// benchColdNegativeReads drives the full cold-negative SD read: the whole
// miss stack, plus — when a cache is wired — the exact-frontier lookup and
// freshness-checked fill.
func benchColdNegativeReads(b *testing.B, withCache bool) {
	db := benchSeedDb(b)
	ctx := b.Context()
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(b, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(b, err)
	defer sd.Close()
	if withCache {
		sd.SetStateCacheForTest(newSmallStateCache())
	}

	key := make([]byte, 20)
	key[0] = 0x02
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[12:], uint64(i)+1)
		v, _, err := sd.GetLatest(kv.AccountsDomain, roTx, key)
		if err != nil {
			b.Fatal(err)
		}
		if len(v) != 0 {
			b.Fatalf("expected a negative, got %x", v)
		}
	}
}

func BenchmarkGetLatestColdNegative(b *testing.B) { benchColdNegativeReads(b, true) }

// The baseline the stamp+fill cost adds to.
func BenchmarkGetLatestColdNegativeNoCache(b *testing.B) { benchColdNegativeReads(b, false) }
