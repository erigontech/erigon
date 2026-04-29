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

package freezeblocks

// Benchmarks for BlockReader.CanonicalHash hot path.
//
// The LRU cache introduced by PR #19173 only populates from snapshot data
// (immutable, post-finalized blocks). For blocks still in the DB the code
// path is identical on main and on this branch.
//
// The three benchmarks below isolate the components that change:
//
//   BenchmarkCanonicalHash_DBPath          – raw MDBX lookup; same on both branches.
//   BenchmarkCanonicalHash_SnapshotNoCache – header.Hash() per call; what main
//                                            pays for every snapshot-range lookup.
//   BenchmarkCanonicalHash_SnapshotCacheHit – LRU Get() per call; what this branch
//                                             pays after the first snapshot lookup.
//
// Running on main vs. this branch:
//
//   go test -bench=BenchmarkCanonicalHash -benchmem ./db/snapshotsync/freezeblocks/

import (
	"context"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	uint256 "github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

const benchBlockCount = 1_000

// BenchmarkCanonicalHash_DBPath measures a raw MDBX canonical-hash lookup.
// This path is taken when the block is in the DB (not yet in snapshots) and
// is identical on main and on this branch — the cache is never populated from
// DB data.
func BenchmarkCanonicalHash_DBPath(b *testing.B) {
	db := memdb.NewTestDB(b, dbcfg.ChainDB)

	rwTx, err := db.BeginRw(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(0); i < benchBlockCount; i++ {
		h := &types.Header{Number: *uint256.NewInt(i)}
		if err := rawdb.WriteCanonicalHash(rwTx, h.Hash(), i); err != nil {
			b.Fatal(err)
		}
	}
	if err := rwTx.Commit(); err != nil {
		b.Fatal(err)
	}

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := rawdb.ReadCanonicalHash(tx, uint64(i%benchBlockCount)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCanonicalHash_SnapshotNoCache measures the per-call cost of
// computing a canonical hash from a header object. On main, CanonicalHash
// calls headerFromSnapshot + header.Hash() on every invocation for blocks
// that live in snapshots (not in the DB). This benchmark isolates the
// header.Hash() cost only; in production the full per-call cost also includes
// headerFromSnapshot (mmap read + decompression + RLP decode), and header.Hash()
// itself is more expensive because real headers carry more fields.
func BenchmarkCanonicalHash_SnapshotNoCache(b *testing.B) {
	headers := make([]*types.Header, benchBlockCount)
	for i := uint64(0); i < benchBlockCount; i++ {
		headers[i] = &types.Header{Number: *uint256.NewInt(i)}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = headers[i%benchBlockCount].Hash()
	}
}

// BenchmarkCanonicalHash_SnapshotCacheHit measures the steady-state cost on
// this branch: after the first request populates the LRU cache, every
// subsequent CanonicalHash call for a snapshot-range block is a single
// cache lookup. Compare this against BenchmarkCanonicalHash_SnapshotNoCache
// to see the per-call saving.
func BenchmarkCanonicalHash_SnapshotCacheHit(b *testing.B) {
	cache, err := lru.New[uint64, common.Hash](10_000)
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(0); i < benchBlockCount; i++ {
		h := &types.Header{Number: *uint256.NewInt(i)}
		cache.Add(i, h.Hash())
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = cache.Get(uint64(i % benchBlockCount))
	}
}
