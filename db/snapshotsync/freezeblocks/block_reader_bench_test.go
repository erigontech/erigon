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
//   BenchmarkCanonicalHash_MDBXLookup          – step 1: raw MDBX read; identical on both branches.
//   BenchmarkCanonicalHash_HeaderHash_Realistic – step 3 on a full mainnet-like header.
//   BenchmarkCanonicalHash_LRUCacheHit          – LRU lookup replacing steps 2+3 on this branch.
//
// Run with:
//   go test -bench=BenchmarkCanonicalHash -benchmem ./db/snapshotsync/freezeblocks/

import (
	"context"
	"os"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	uint256 "github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
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
	defer rwTx.Rollback() //nolint:gocritic
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

// BenchmarkCanonicalHash_RealSnapshot is an end-to-end benchmark using real
// snapshot files from disk. It measures the full CanonicalHash call path
// including MDBX lookup (miss) + snapshot decompression + RLP decode +
// header.Hash() on main, vs. MDBX lookup (miss) + LRU cache hit on this branch.
//
// Requires a local Erigon mainnet datadir with snapshots. Set ERIGON_SNAP_DIR
// to the snapshots directory, or the benchmark is skipped.
//
//	ERIGON_SNAP_DIR=/home/user/.local/share/erigon/snapshots \
//	  go test -bench=BenchmarkCanonicalHash_RealSnapshot -benchmem -benchtime=5s \
//	  ./db/snapshotsync/freezeblocks/
//
// Run on both main and this branch to compare:
//
//	main:        every call pays full snapshot I/O + decompression + hash
//	this branch: after warmup, every call is a single LRU lookup
func BenchmarkCanonicalHash_RealSnapshot(b *testing.B) {
	snapDir := os.Getenv("ERIGON_SNAP_DIR")
	if snapDir == "" {
		b.Skip("set ERIGON_SNAP_DIR to a mainnet snapshots directory to run this benchmark")
	}
	if _, err := os.Stat(snapDir); err != nil {
		b.Skipf("snapshot dir not accessible: %v", err)
	}

	logger := log.New()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, snapDir, logger)
	if err := snapshots.OpenFolder(); err != nil {
		b.Fatal(err)
	}
	defer snapshots.Close()

	available := snapshots.BlocksAvailable()
	if available == 0 {
		b.Skip("no blocks available in snapshot dir")
	}

	blockReader := NewBlockReader(snapshots, nil)

	// Use an empty memdb so every lookup misses the DB and falls through to snapshots.
	db := memdb.NewTestDB(b, dbcfg.ChainDB)
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	// Pick blocks well inside the available range.
	const startBlock = 500_000
	const numBlocks = 1_000
	if available < startBlock+numBlocks {
		b.Skipf("need at least %d blocks in snapshots, got %d", startBlock+numBlocks, available)
	}

	// Warmup: on this branch this populates the cache; on main it is a no-op.
	for i := uint64(startBlock); i < startBlock+numBlocks; i++ {
		if _, _, err := blockReader.CanonicalHash(context.Background(), tx, i); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blockNum := uint64(startBlock + (i % numBlocks))
		if _, _, err := blockReader.CanonicalHash(context.Background(), tx, blockNum); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCanonicalHash_RealSnapshot_Cold measures the full snapshot path with
// no cache warmup. This is what every call costs on main (which has no LRU cache)
// and what the first call costs on this branch. It cycles through enough distinct
// block numbers to exceed the LRU cache capacity, forcing cache misses.
//
//	ERIGON_SNAP_DIR=/datadisk/erigon34/datadir/snapshots \
//	  go test -bench=BenchmarkCanonicalHash_RealSnapshot_Cold -benchmem -benchtime=5s \
//	  ./db/snapshotsync/freezeblocks/
func BenchmarkCanonicalHash_RealSnapshot_Cold(b *testing.B) {
	snapDir := os.Getenv("ERIGON_SNAP_DIR")
	if snapDir == "" {
		b.Skip("set ERIGON_SNAP_DIR to a mainnet snapshots directory to run this benchmark")
	}
	if _, err := os.Stat(snapDir); err != nil {
		b.Skipf("snapshot dir not accessible: %v", err)
	}

	logger := log.New()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := NewRoSnapshots(cfg, snapDir, logger)
	if err := snapshots.OpenFolder(); err != nil {
		b.Fatal(err)
	}
	defer snapshots.Close()

	available := snapshots.BlocksAvailable()
	if available == 0 {
		b.Skip("no blocks available in snapshot dir")
	}

	blockReader := NewBlockReader(snapshots, nil)

	db := memdb.NewTestDB(b, dbcfg.ChainDB)
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()

	const startBlock = 500_000
	// Use a range larger than the LRU cache size (10_000) to guarantee cache misses.
	const coldRange = 20_000
	if available < startBlock+coldRange {
		b.Skipf("need at least %d blocks in snapshots, got %d", startBlock+coldRange, available)
	}

	// No warmup — every call is a cold snapshot read.
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blockNum := uint64(startBlock + (i % coldRange))
		if _, _, err := blockReader.CanonicalHash(context.Background(), tx, blockNum); err != nil {
			b.Fatal(err)
		}
	}
}
