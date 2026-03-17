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

package test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

// lifecycleConfig controls the benchmark dimensions.
type lifecycleConfig struct {
	// name is the sub-benchmark label.
	name string
	// totalSteps controls how many aggregator steps to drive through.
	// Merge triggers at 64 steps with default stepsInFrozenFile.
	totalSteps int
	// keysPerTx is the number of storage DomainPut operations per transaction.
	// Bloatnet density uses ~500 keys/tx vs mainnet ~50.
	keysPerTx int
	// txsPerBlock simulates block gas usage via transaction count.
	txsPerBlock int
	// blocksPerStep groups blocks into an aggregator step boundary.
	blocksPerStep int
	// hotAccounts controls the number of frequently-written accounts (Zipf head).
	hotAccounts int
	// coldAccounts controls the long-tail of less-frequently-written accounts.
	coldAccounts int
}

// keyGenerator produces deterministic keys with a Zipf-like distribution:
// hotAccounts get ~70% of writes (DeFi contracts), coldAccounts get the rest.
type keyGenerator struct {
	rnd          *rndGen
	hotAddrs     [][]byte // 20-byte account addresses
	coldAddrs    [][]byte
	hotWeight    int // out of 100
	slotCounters []uint64
}

func newKeyGenerator(rnd *rndGen, nHot, nCold int) *keyGenerator {
	g := &keyGenerator{
		rnd:          rnd,
		hotAddrs:     make([][]byte, nHot),
		coldAddrs:    make([][]byte, nCold),
		hotWeight:    70,
		slotCounters: make([]uint64, nHot+nCold),
	}
	for i := range g.hotAddrs {
		g.hotAddrs[i] = make([]byte, length.Addr)
		rnd.Read(g.hotAddrs[i])
	}
	for i := range g.coldAddrs {
		g.coldAddrs[i] = make([]byte, length.Addr)
		rnd.Read(g.coldAddrs[i])
	}
	return g
}

// nextStorageKey returns a composite key (addr + slot hash) for a storage DomainPut.
func (g *keyGenerator) nextStorageKey() []byte {
	var addr []byte
	var idx int
	if g.rnd.IntN(100) < g.hotWeight && len(g.hotAddrs) > 0 {
		idx = g.rnd.IntN(len(g.hotAddrs))
		addr = g.hotAddrs[idx]
	} else {
		ci := g.rnd.IntN(len(g.coldAddrs))
		idx = len(g.hotAddrs) + ci
		addr = g.coldAddrs[ci]
	}

	// Generate a slot hash deterministically from account + counter.
	g.slotCounters[idx]++
	slot := make([]byte, length.Hash)
	binary.BigEndian.PutUint64(slot[24:], g.slotCounters[idx])
	// Mix in account index for uniqueness across accounts.
	binary.BigEndian.PutUint32(slot[0:4], uint32(idx))

	return composite(addr, slot)
}

// nextAccountKey returns a 20-byte account key and serialised account value.
func (g *keyGenerator) nextAccountKey(nonce uint64) ([]byte, []byte) {
	var addr []byte
	if g.rnd.IntN(100) < g.hotWeight && len(g.hotAddrs) > 0 {
		addr = g.hotAddrs[g.rnd.IntN(len(g.hotAddrs))]
	} else {
		addr = g.coldAddrs[g.rnd.IntN(len(g.coldAddrs))]
	}

	acc := accounts3.Account{
		Nonce:    nonce,
		Balance:  *uint256.NewInt(nonce*1e9 + uint64(g.rnd.IntN(1e9))),
		CodeHash: accounts3.EmptyCodeHash,
	}
	return addr, accounts3.SerialiseV3(&acc)
}

// lifecycleTimings holds per-phase duration accumulators.
type lifecycleTimings struct {
	execute time.Duration
	commit  time.Duration
	flush   time.Duration
	collate time.Duration
	prune   time.Duration
}

func (t *lifecycleTimings) report(b *testing.B) {
	b.ReportMetric(float64(t.execute.Milliseconds()), "ms/execute")
	b.ReportMetric(float64(t.commit.Milliseconds()), "ms/commit")
	b.ReportMetric(float64(t.flush.Milliseconds()), "ms/flush")
	b.ReportMetric(float64(t.collate.Milliseconds()), "ms/collate")
	b.ReportMetric(float64(t.prune.Milliseconds()), "ms/prune")
}

// initAccounts writes all hot+cold account entries into the domain so that
// storage subtrees under these addresses are valid in the commitment trie.
// Returns the txNum after initialisation (always 1).
func initAccounts(b *testing.B, domains *execctx.SharedDomains, rwTx kv.TemporalRwTx, keyGen *keyGenerator) uint64 {
	b.Helper()
	var txNum uint64 = 1
	for _, addr := range keyGen.hotAddrs {
		acc := accounts3.Account{Nonce: 1, Balance: *uint256.NewInt(1e18), CodeHash: accounts3.EmptyCodeHash}
		err := domains.DomainPut(kv.AccountsDomain, rwTx, addr, accounts3.SerialiseV3(&acc), txNum, nil)
		require.NoError(b, err)
	}
	for _, addr := range keyGen.coldAddrs {
		acc := accounts3.Account{Nonce: 1, Balance: *uint256.NewInt(1e18), CodeHash: accounts3.EmptyCodeHash}
		err := domains.DomainPut(kv.AccountsDomain, rwTx, addr, accounts3.SerialiseV3(&acc), txNum, nil)
		require.NoError(b, err)
	}
	return txNum
}

// runLifecycle drives the full aggregator lifecycle for a given config
// and returns the accumulated timings plus the final state for read benchmarks.
func runLifecycle(b *testing.B, cfg lifecycleConfig) (*lifecycleTimings, kv.TemporalRwDB, *state.Aggregator) {
	b.Helper()

	stepSize := uint64(cfg.txsPerBlock * cfg.blocksPerStep)
	db, agg := testDbAndAggregatorBench(b, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(b, err)
	defer domains.Close()

	rnd := newRnd(42)
	keyGen := newKeyGenerator(rnd, cfg.hotAccounts, cfg.coldAccounts)

	var timings lifecycleTimings
	var blockNum uint64

	// Initialise all accounts so the commitment trie has account nodes
	// before storage subtrees are populated.
	txNum := initAccounts(b, domains, rwTx, keyGen)
	blockNum++
	_, err = domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
	require.NoError(b, err)

	for step := 0; step < cfg.totalSteps; step++ {
		for blk := 0; blk < cfg.blocksPerStep; blk++ {
			// === EXECUTE: DomainPut for all txs in this block ===
			execStart := time.Now()
			for tx := 0; tx < cfg.txsPerBlock; tx++ {
				txNum++

				// ~80% storage writes, ~20% account updates
				storageKeys := cfg.keysPerTx * 4 / 5
				accountKeys := cfg.keysPerTx - storageKeys

				for k := 0; k < storageKeys; k++ {
					sKey := keyGen.nextStorageKey()
					val := make([]byte, 32)
					binary.BigEndian.PutUint64(val[24:], txNum)
					err := domains.DomainPut(kv.StorageDomain, rwTx, sKey, val, txNum, nil)
					require.NoError(b, err)
				}

				for k := 0; k < accountKeys; k++ {
					aKey, aVal := keyGen.nextAccountKey(txNum)
					err := domains.DomainPut(kv.AccountsDomain, rwTx, aKey, aVal, txNum, nil)
					require.NoError(b, err)
				}
			}
			timings.execute += time.Since(execStart)

			// === COMMIT: ComputeCommitment at block boundary ===
			commitStart := time.Now()
			blockNum++
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(b, err)
			timings.commit += time.Since(commitStart)
		}

		// === FLUSH: write in-memory batch to MDBX ===
		flushStart := time.Now()
		err := domains.Flush(ctx, rwTx)
		require.NoError(b, err)
		timings.flush += time.Since(flushStart)

		// === COLLATE + BUILD: create .seg files from MDBX history ===
		// Lag by 2 steps to ensure data is fully written.
		if step > 3 {
			collateStart := time.Now()
			buildTo := txNum - 2*stepSize
			err = agg.BuildFiles(buildTo)
			require.NoError(b, err)
			timings.collate += time.Since(collateStart)
		}

		// === PRUNE: remove MDBX history already covered by files ===
		// Run every 10 steps to amortise overhead (matches production pattern).
		if step > 5 && step%10 == 0 {
			pruneStart := time.Now()
			at := agg.BeginFilesRo()
			_, err := at.PruneSmallBatches(ctx, 5*time.Second, rwTx)
			at.Close()
			require.NoError(b, err)
			timings.prune += time.Since(pruneStart)
		}
	}

	// Final commitment + flush for remaining data.
	_, err = domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
	require.NoError(b, err)
	err = domains.Flush(ctx, rwTx)
	require.NoError(b, err)
	err = rwTx.Commit()
	require.NoError(b, err)

	return &timings, db, agg
}

// BenchmarkLifecycle runs the full aggregator lifecycle at different scales.
// Each sub-benchmark drives data through execute→commit→flush→collate→prune→merge
// with realistic key distribution and independently timed phases.
func BenchmarkLifecycle(b *testing.B) {
	configs := []lifecycleConfig{
		{
			name:          "Small",
			totalSteps:    50,
			keysPerTx:     10,
			txsPerBlock:   5,
			blocksPerStep: 2,
			hotAccounts:   10,
			coldAccounts:  100,
		},
		{
			name:          "Medium",
			totalSteps:    200,
			keysPerTx:     20,
			txsPerBlock:   10,
			blocksPerStep: 2,
			hotAccounts:   50,
			coldAccounts:  500,
		},
		{
			name:          "Large",
			totalSteps:    500,
			keysPerTx:     50,
			txsPerBlock:   10,
			blocksPerStep: 2,
			hotAccounts:   100,
			coldAccounts:  1000,
		},
		{
			name:          "Bloatnet",
			totalSteps:    200,
			keysPerTx:     500,
			txsPerBlock:   10,
			blocksPerStep: 2,
			hotAccounts:   100,
			coldAccounts:  1000,
		},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			for b.Loop() {
				timings, _, _ := runLifecycle(b, cfg)
				timings.report(b)
			}
		})
	}
}

// BenchmarkLifecycle_PhaseIsolation runs each lifecycle phase independently
// to measure the ceiling of each phase without interaction effects.
func BenchmarkLifecycle_PhaseIsolation(b *testing.B) {
	cfg := lifecycleConfig{
		totalSteps:    200,
		keysPerTx:     50,
		txsPerBlock:   10,
		blocksPerStep: 2,
		hotAccounts:   100,
		coldAccounts:  1000,
	}
	stepSize := uint64(cfg.txsPerBlock * cfg.blocksPerStep)

	b.Run("Execute_Only", func(b *testing.B) {
		db, _ := testDbAndAggregatorBench(b, stepSize)
		ctx := context.Background()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(b, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(b, err)
		defer domains.Close()

		rnd := newRnd(42)
		keyGen := newKeyGenerator(rnd, cfg.hotAccounts, cfg.coldAccounts)
		txNum := initAccounts(b, domains, rwTx, keyGen)

		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			for step := 0; step < cfg.totalSteps; step++ {
				for blk := 0; blk < cfg.blocksPerStep; blk++ {
					for tx := 0; tx < cfg.txsPerBlock; tx++ {
						txNum++
						sKey := keyGen.nextStorageKey()
						val := make([]byte, 32)
						binary.BigEndian.PutUint64(val[24:], txNum)
						err := domains.DomainPut(kv.StorageDomain, rwTx, sKey, val, txNum, nil)
						require.NoError(b, err)
					}
				}
			}
		}
	})

	b.Run("Commit_Only", func(b *testing.B) {
		db, _ := testDbAndAggregatorBench(b, stepSize)
		ctx := context.Background()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(b, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(b, err)
		defer domains.Close()

		rnd := newRnd(42)
		keyGen := newKeyGenerator(rnd, cfg.hotAccounts, cfg.coldAccounts)
		txNum := initAccounts(b, domains, rwTx, keyGen)

		// Pre-populate: write one block worth of storage data
		for tx := 0; tx < cfg.txsPerBlock; tx++ {
			txNum++
			for k := 0; k < cfg.keysPerTx; k++ {
				sKey := keyGen.nextStorageKey()
				val := make([]byte, 32)
				binary.BigEndian.PutUint64(val[24:], txNum)
				err := domains.DomainPut(kv.StorageDomain, rwTx, sKey, val, txNum, nil)
				require.NoError(b, err)
			}
		}

		b.ResetTimer()
		b.ReportAllocs()
		var blockNum uint64
		for b.Loop() {
			blockNum++
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(b, err)
		}
	})

	b.Run("Flush_Only", func(b *testing.B) {
		db, _ := testDbAndAggregatorBench(b, stepSize)
		ctx := context.Background()

		rnd := newRnd(42)
		keyGen := newKeyGenerator(rnd, cfg.hotAccounts, cfg.coldAccounts)

		// Pre-populate data for flushing
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(b, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(b, err)
		defer domains.Close()

		txNum := initAccounts(b, domains, rwTx, keyGen)

		for tx := 0; tx < cfg.txsPerBlock*cfg.blocksPerStep; tx++ {
			txNum++
			sKey := keyGen.nextStorageKey()
			val := make([]byte, 32)
			binary.BigEndian.PutUint64(val[24:], txNum)
			err := domains.DomainPut(kv.StorageDomain, rwTx, sKey, val, txNum, nil) //nolint:gocritic
			require.NoError(b, err)
		}
		_, err = domains.ComputeCommitment(ctx, rwTx, true, 1, txNum, "", nil)
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			err = domains.Flush(ctx, rwTx)
			require.NoError(b, err)
		}
	})

	b.Run("Collate_Only", func(b *testing.B) {
		// Build up enough data in MDBX, then benchmark only the BuildFiles call.
		db, agg := testDbAndAggregatorBench(b, stepSize)
		ctx := context.Background()
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(b, err)
		defer rwTx.Rollback()

		domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
		require.NoError(b, err)
		defer domains.Close()

		rnd := newRnd(42)
		keyGen := newKeyGenerator(rnd, cfg.hotAccounts, cfg.coldAccounts)

		txNum := initAccounts(b, domains, rwTx, keyGen)
		var blockNum uint64
		blockNum++
		_, err = domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
		require.NoError(b, err)

		// Write 10 steps of data to MDBX without building files
		targetSteps := 10
		for step := 0; step < targetSteps; step++ {
			for blk := 0; blk < cfg.blocksPerStep; blk++ {
				for tx := 0; tx < cfg.txsPerBlock; tx++ {
					txNum++
					sKey := keyGen.nextStorageKey()
					val := make([]byte, 32)
					binary.BigEndian.PutUint64(val[24:], txNum)
					err := domains.DomainPut(kv.StorageDomain, rwTx, sKey, val, txNum, nil)
					require.NoError(b, err)
				}
				blockNum++
				_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
				require.NoError(b, err)
			}
			err := domains.Flush(ctx, rwTx)
			require.NoError(b, err)
		}
		err = rwTx.Commit()
		require.NoError(b, err)

		b.ResetTimer()
		b.ReportAllocs()
		for b.Loop() {
			err := agg.BuildFiles(txNum - 2*stepSize)
			require.NoError(b, err)
		}
	})
}

// BenchmarkReadAfterLifecycle runs the lifecycle to build state with files,
// then benchmarks read operations against the resulting persisted state.
func BenchmarkReadAfterLifecycle(b *testing.B) {
	cfg := lifecycleConfig{
		totalSteps:    200,
		keysPerTx:     20,
		txsPerBlock:   10,
		blocksPerStep: 2,
		hotAccounts:   50,
		coldAccounts:  500,
	}

	_, db, _ := runLifecycle(b, cfg)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	// Regenerate the same keys for reading.
	rnd := newRnd(42)
	keyGen := newKeyGenerator(rnd, cfg.hotAccounts, cfg.coldAccounts)

	// Collect a sample of storage keys that were written.
	sampleSize := 200
	storageKeys := make([][]byte, sampleSize)
	for i := range storageKeys {
		storageKeys[i] = keyGen.nextStorageKey()
	}

	// Collect account keys.
	accountKeys := make([][]byte, cfg.hotAccounts+cfg.coldAccounts)
	copy(accountKeys[:cfg.hotAccounts], keyGen.hotAddrs)
	copy(accountKeys[cfg.hotAccounts:], keyGen.coldAddrs)

	stepSize := uint64(cfg.txsPerBlock * cfg.blocksPerStep)
	maxTx := uint64(cfg.totalSteps) * stepSize

	b.Run("GetLatest/Storage", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, key := range storageKeys {
				_, _, err := rwTx.GetLatest(kv.StorageDomain, key)
				require.NoError(b, err)
			}
		}
	})

	b.Run("GetLatest/Account", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, key := range accountKeys {
				_, _, err := rwTx.GetLatest(kv.AccountsDomain, key)
				require.NoError(b, err)
			}
		}
	})

	b.Run("HistorySeek/Recent", func(b *testing.B) {
		b.ReportAllocs()
		readRnd := newRnd(99)
		recentStart := maxTx - maxTx/10 // last 10% of tx range
		for b.Loop() {
			for _, key := range storageKeys[:50] {
				ts := recentStart + uint64(readRnd.IntN(int(maxTx/10)))
				_, _, err := rwTx.HistorySeek(kv.StorageDomain, key, ts)
				require.NoError(b, err)
			}
		}
	})

	b.Run("HistorySeek/Deep", func(b *testing.B) {
		b.ReportAllocs()
		readRnd := newRnd(99)
		for b.Loop() {
			for _, key := range storageKeys[:50] {
				ts := uint64(readRnd.IntN(int(maxTx / 2))) // first half of history
				_, _, err := rwTx.HistorySeek(kv.StorageDomain, key, ts)
				require.NoError(b, err)
			}
		}
	})
}

// BenchmarkLifecycle_KeyDensity benchmarks commitment cost as a function of
// keys-per-block, keeping total steps constant. This reveals the commitment
// scaling curve relevant to bloatnet (33k keys/block vs mainnet ~1.4k).
func BenchmarkLifecycle_KeyDensity(b *testing.B) {
	for _, keysPerTx := range []int{10, 50, 200, 500} {
		b.Run(fmt.Sprintf("keys=%d", keysPerTx), func(b *testing.B) {
			cfg := lifecycleConfig{
				totalSteps:    100,
				keysPerTx:     keysPerTx,
				txsPerBlock:   10,
				blocksPerStep: 2,
				hotAccounts:   50,
				coldAccounts:  500,
			}
			for b.Loop() {
				timings, _, _ := runLifecycle(b, cfg)
				timings.report(b)
			}
		})
	}
}
