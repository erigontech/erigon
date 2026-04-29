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
// Context: CanonicalHash is called on every eth_getBlockByNumber / eth_getTransactionByHash
// RPC request. For blocks in snapshots (the vast majority of historical blocks),
// the code path on main is:
//
//   1. rawdb.ReadCanonicalHash (MDBX miss)        ~200 ns
//   2. headerFromSnapshot (decompress + RLP)      ~several µs (from memory-mapped file)
//   3. header.Hash() (keccak256 of full RLP)      ~300–500 ns on a real header
//
// On this branch (PR #19173), after the first call the cache is populated and
// steps 2+3 are replaced by a single LRU lookup (~30–40 ns).
//
// These benchmarks isolate each component so the saving is measurable
// without needing real snapshot files:
//
//   BenchmarkCanonicalHash_MDBXLookup         – step 1: raw MDBX read; identical on both branches.
//   BenchmarkCanonicalHash_HeaderHash_Minimal  – step 3 on a trivial header (not representative).
//   BenchmarkCanonicalHash_HeaderHash_Realistic– step 3 on a full mainnet-like header.
//   BenchmarkCanonicalHash_LRUCacheHit         – LRU lookup replacing steps 2+3 on this branch.
//
// Run with:
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

// realisticHeader returns a header that resembles a post-Merge mainnet block:
// all hash/bloom fields are non-zero so the RLP encoding is representative
// of real data (~500 bytes), making header.Hash() cost comparable to production.
func realisticHeader(blockNum uint64) *types.Header {
	seed := blockNum + 1
	fill32 := func(offset uint64) (h common.Hash) {
		for i := range h {
			h[i] = byte(seed + offset + uint64(i))
		}
		return
	}
	h := &types.Header{
		Number:      *uint256.NewInt(blockNum),
		ParentHash:  fill32(0),
		UncleHash:   fill32(7),
		Coinbase:    common.BytesToAddress(fill32(1).Bytes()),
		Root:        fill32(2),
		TxHash:      fill32(8),
		ReceiptHash: fill32(3),
		GasLimit:    30_000_000,
		GasUsed:     seed % 30_000_000,
		Time:        1_700_000_000 + seed,
		Extra:       fill32(4).Bytes(),
		MixDigest:   fill32(5),
		BaseFee:     uint256.NewInt(seed % 1_000_000_000),
	}
	copy(h.Bloom[:], fill32(6).Bytes())
	return h
}

// BenchmarkCanonicalHash_MDBXLookup measures a raw MDBX canonical-hash read.
// This is the first step of CanonicalHash on every call; it is identical on
// main and on this branch.
func BenchmarkCanonicalHash_MDBXLookup(b *testing.B) {
	db := memdb.NewTestDB(b, dbcfg.ChainDB)

	rwTx, err := db.BeginRw(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(0); i < benchBlockCount; i++ {
		if err := rawdb.WriteCanonicalHash(rwTx, realisticHeader(i).Hash(), i); err != nil {
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

// BenchmarkCanonicalHash_HeaderHash_Realistic measures the cost of computing a
// canonical hash from a full mainnet-like header (~500-byte RLP). On main,
// CanonicalHash pays this cost on EVERY call for snapshot-range blocks because
// headerFromSnapshot produces a fresh *Header each time (no per-object caching
// across calls). CalcHash is used here instead of Hash to bypass the per-object
// atomic cache and reflect the true first-call cost.
// The real per-call cost on main also includes headerFromSnapshot (decompress +
// RLP decode from the memory-mapped snapshot file), which is not measured here.
func BenchmarkCanonicalHash_HeaderHash_Realistic(b *testing.B) {
	headers := make([]*types.Header, benchBlockCount)
	for i := uint64(0); i < benchBlockCount; i++ {
		headers[i] = realisticHeader(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = headers[i%benchBlockCount].CalcHash()
	}
}

// BenchmarkCanonicalHash_LRUCacheHit measures the steady-state cost of a cache
// lookup on this branch. After the first CanonicalHash call for a snapshot-range
// block, all subsequent calls return here instead of calling headerFromSnapshot
// + header.Hash(). Compare this against BenchmarkCanonicalHash_HeaderHash_Realistic
// to see the per-call saving (headerFromSnapshot decompression is additional).
func BenchmarkCanonicalHash_LRUCacheHit(b *testing.B) {
	cache, err := lru.New[uint64, common.Hash](10_000)
	if err != nil {
		b.Fatal(err)
	}
	for i := uint64(0); i < benchBlockCount; i++ {
		cache.Add(i, realisticHeader(i).Hash())
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = cache.Get(uint64(i % benchBlockCount))
	}
}
