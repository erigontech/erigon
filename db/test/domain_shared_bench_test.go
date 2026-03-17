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
	randOld "math/rand"
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
)

type rndGen struct {
	*rand.Rand
	oldGen *randOld.Rand
}

func newRnd(seed uint64) *rndGen {
	return &rndGen{
		Rand:   rand.New(rand.NewChaCha8([32]byte{byte(seed)})),
		oldGen: randOld.New(randOld.NewSource(int64(seed))),
	}
}
func (r *rndGen) IntN(n int) int                   { return int(r.Uint64N(uint64(n))) }
func (r *rndGen) Read(p []byte) (n int, err error) { return r.oldGen.Read(p) } // seems `go1.22` doesn't have `Read` method on `math/v2` generator

func testDbAndAggregatorBench(b *testing.B, aggStep uint64) (kv.TemporalRwDB, *state.Aggregator) {
	b.Helper()
	dirs := datadir.New(b.TempDir())
	db := temporaltest.NewTestDBWithStepSize(b, dirs, aggStep)
	return db, db.(state.HasAgg).Agg().(*state.Aggregator)
}

func composite(k, k2 []byte) []byte {
	return append(common.Copy(k), k2...)
}

func Benchmark_SharedDomains_GetLatest(t *testing.B) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorBench(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(context.Background(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	maxTx := stepSize * 258

	rnd := newRnd(4500)

	keys := make([][]byte, 8)
	for i := 0; i < len(keys); i++ {
		keys[i] = make([]byte, length.Addr)
		rnd.Read(keys[i])
	}

	var txNum, blockNum uint64
	for i := uint64(0); i < maxTx; i++ {
		txNum = i
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, i)
		for j := 0; j < len(keys); j++ {
			err := domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], v, txNum, nil)
			require.NoError(t, err)
		}

		if i%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			err = domains.Flush(ctx, rwTx)
			require.NoError(t, err)
			if i/stepSize > 3 {
				err = agg.BuildFiles(i - (2 * stepSize))
				require.NoError(t, err)
			}
		}
	}
	_, err = domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
	require.NoError(t, err)
	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	latest := make([]byte, 8)
	binary.BigEndian.PutUint64(latest, maxTx-1)

	t.Run("GetLatest", func(b *testing.B) {
		t.ReportAllocs()
		for ik := 0; ik < t.N; ik++ {
			for i := 0; i < len(keys); i++ {
				v, _, err := rwTx.GetLatest(kv.AccountsDomain, keys[i])
				require.Equalf(t, latest, v, "unexpected %d, wanted %d", binary.BigEndian.Uint64(v), maxTx-1)
				require.NoError(t, err)
			}
		}
	})
	t.Run("HistorySeek", func(b *testing.B) {
		t.ReportAllocs()
		for ik := 0; ik < t.N; ik++ {
			for i := 0; i < len(keys); i++ {
				ts := uint64(rnd.IntN(int(maxTx)))
				v, ok, err := rwTx.HistorySeek(kv.AccountsDomain, keys[i], ts)

				require.True(t, ok)
				require.NotNil(t, v)
				//require.EqualValuesf(t, latest, v, "unexpected %d, wanted %d", binary.BigEndian.Uint64(v), maxTx-1)
				require.NoError(t, err)
			}
		}
	})
}

func BenchmarkSharedDomains_ComputeCommitment(b *testing.B) {
	stepSize := uint64(100)
	db, _ := testDbAndAggregatorBench(b, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(context.Background(), rwTx, log.New())
	require.NoError(b, err)
	defer domains.Close()

	maxTx := stepSize * 17
	data := generateTestDataForDomainCommitment(b, length.Addr, length.Addr+length.Hash, maxTx, 15, 100)
	require.NotNil(b, data)

	var txNum, blockNum uint64
	for domName, d := range data {
		fom := kv.AccountsDomain
		if domName == "storage" {
			fom = kv.StorageDomain
		}
		for key, upd := range d {
			for _, u := range upd {
				txNum = u.txNum
				err := domains.DomainPut(fom, rwTx, []byte(key), u.value, txNum, nil)
				require.NoError(b, err)
			}
		}
	}

	b.Run("ComputeCommitment", func(b *testing.B) {
		for b.Loop() {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(b, err)
		}
	})
}

type upd struct {
	txNum uint64
	value []byte
}

func generateTestDataForDomainCommitment(tb testing.TB, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit uint64) map[string]map[string][]upd {
	tb.Helper()

	doms := make(map[string]map[string][]upd)
	r := newRnd(31)

	accs := make(map[string][]upd)
	stor := make(map[string][]upd)
	if keyLimit == 1 {
		key1 := generateRandomKey(r, keySize1)
		accs[key1] = generateAccountUpdates(r, totalTx, keyTxsLimit)
		doms["accounts"] = accs
		return doms
	}

	for i := uint64(0); i < keyLimit/2; i++ {
		key1 := generateRandomKey(r, keySize1)
		accs[key1] = generateAccountUpdates(r, totalTx, keyTxsLimit)
		key2 := key1 + generateRandomKey(r, keySize2-keySize1)
		stor[key2] = generateArbitraryValueUpdates(r, totalTx, keyTxsLimit, 32)
	}
	doms["accounts"] = accs
	doms["storage"] = stor

	return doms
}
func generateRandomKey(r *rndGen, size uint64) string {
	return string(generateRandomKeyBytes(r, size))
}

func generateRandomKeyBytes(r *rndGen, size uint64) []byte {
	key := make([]byte, size)
	r.Read(key)
	return key
}

func generateAccountUpdates(r *rndGen, totalTx, keyTxsLimit uint64) []upd {
	updates := make([]upd, 0)
	usedTxNums := make(map[uint64]bool)

	for i := uint64(0); i < keyTxsLimit; i++ {
		txNum := generateRandomTxNum(r, totalTx, usedTxNums)
		jitter := r.IntN(10e7)
		acc := accounts3.Account{
			Nonce:       i,
			Balance:     *uint256.NewInt(i*10e4 + uint64(jitter)),
			CodeHash:    accounts3.EmptyCodeHash,
			Incarnation: 0,
		}
		value := accounts3.SerialiseV3(&acc)

		updates = append(updates, upd{txNum: txNum, value: value})
		usedTxNums[txNum] = true
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].txNum < updates[j].txNum })

	return updates
}

func generateArbitraryValueUpdates(r *rndGen, totalTx, keyTxsLimit, maxSize uint64) []upd {
	updates := make([]upd, 0)
	usedTxNums := make(map[uint64]bool)
	//maxStorageSize := 24 * (1 << 10) // limit on contract code

	for i := uint64(0); i < keyTxsLimit; i++ {
		txNum := generateRandomTxNum(r, totalTx, usedTxNums)

		value := make([]byte, r.IntN(int(maxSize)))
		r.Read(value)

		updates = append(updates, upd{txNum: txNum, value: value})
		usedTxNums[txNum] = true
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].txNum < updates[j].txNum })

	return updates
}
func generateRandomTxNum(r *rndGen, maxTxNum uint64, usedTxNums map[uint64]bool) uint64 {
	txNum := uint64(r.IntN(int(maxTxNum)))
	for usedTxNums[txNum] {
		txNum = uint64(r.IntN(int(maxTxNum)))
	}

	return txNum
}

// BenchmarkPruneSmallBatches benchmarks the PruneSmallBatches path.
// It populates domains with data across multiple steps, builds snapshot files,
// then measures the time to prune the data covered by those snapshots.
func BenchmarkPruneSmallBatches(b *testing.B) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorBench(b, stepSize)

	ctx := context.Background()
	rnd := newRnd(0)

	// Populate data: write enough txs to span several steps
	maxTx := stepSize * 50
	keysCount := uint64(100)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(b, err)

	usedKeys := make(map[string]struct{}, keysCount*maxTx)
	for txNum := uint64(1); txNum <= maxTx; txNum++ {
		generateSharedDomainsUpdatesForBench(b, domains, rwTx, txNum, rnd, usedKeys, length.Addr, keysCount)
		if txNum%(stepSize/2) == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum/(stepSize/2), txNum, "", nil)
			require.NoError(b, err)
		}
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(b, err)
	err = rwTx.Commit()
	require.NoError(b, err)
	domains.Close()

	// Build snapshot files so there is data to prune
	err = agg.BuildFiles(maxTx)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()
		pruneTx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
		require.NoError(b, err)
		b.StartTimer()

		for {
			more, err := pruneTx.PruneSmallBatches(ctx, 10*time.Hour)
			require.NoError(b, err)
			if !more {
				break
			}
		}

		b.StopTimer()
		pruneTx.Rollback() // rollback so next iteration has data to prune again
		b.StartTimer()
	}
}

// generateSharedDomainsUpdatesForBench is a benchmark-compatible version of generateSharedDomainsUpdatesForTx.
func generateSharedDomainsUpdatesForBench(b *testing.B, domains *execctx.SharedDomains, tx kv.TemporalTx, txNum uint64, rnd *rndGen, prevKeys map[string]struct{}, keyMaxLen, keysCount uint64) {
	b.Helper()

	getKey := func() ([]byte, bool) {
		r := rnd.IntN(100)
		if r < 50 && len(prevKeys) > 0 {
			ri := rnd.IntN(len(prevKeys))
			for k := range prevKeys {
				if ri == 0 {
					return []byte(k), true
				}
				ri--
			}
		}
		return []byte(generateRandomKey(rnd, keyMaxLen)), false
	}

	for j := uint64(0); j < keysCount; j++ {
		key, existed := getKey()

		r := rnd.IntN(101)
		switch {
		case r <= 33:
			acc := accounts3.Account{
				Nonce:       txNum,
				Balance:     *uint256.NewInt(txNum * 100_000),
				CodeHash:    accounts3.EmptyCodeHash,
				Incarnation: 0,
			}
			buf := accounts3.SerialiseV3(&acc)
			prev, _, err := domains.GetLatest(kv.AccountsDomain, tx, key)
			require.NoError(b, err)

			prevKeys[string(key)] = struct{}{}

			err = domains.DomainPut(kv.AccountsDomain, tx, key, buf, txNum, prev)
			require.NoError(b, err)

		case r > 33 && r <= 66:
			codeUpd := make([]byte, rnd.IntN(24576))
			rnd.Read(codeUpd)
			for limit := 1000; len(key) > length.Addr && limit > 0; limit-- {
				key, existed = getKey() //nolint
				if !existed {
					continue
				}
			}
			prevKeys[string(key)] = struct{}{}

			prev, _, err := domains.GetLatest(kv.CodeDomain, tx, key)
			require.NoError(b, err)

			err = domains.DomainPut(kv.CodeDomain, tx, key, codeUpd, txNum, prev)
			require.NoError(b, err)

		case r > 80:
			if !existed {
				continue
			}
			prevKeys[string(key)] = struct{}{}

			err := domains.DomainDel(kv.AccountsDomain, tx, key, txNum, nil)
			require.NoError(b, err)

		case r > 66 && r <= 80:
			if len(key) > length.Addr {
				key = key[:length.Addr]
			}

			prev, _, err := domains.GetLatest(kv.AccountsDomain, tx, key)
			require.NoError(b, err)
			if prev == nil {
				prevKeys[string(key)] = struct{}{}
				acc := accounts3.Account{
					Nonce:       txNum,
					Balance:     *uint256.NewInt(txNum * 100_000),
					CodeHash:    accounts3.EmptyCodeHash,
					Incarnation: 0,
				}
				buf := accounts3.SerialiseV3(&acc)
				err = domains.DomainPut(kv.AccountsDomain, tx, key, buf, txNum, prev)
				require.NoError(b, err)
			}

			sk := make([]byte, length.Addr+length.Hash)
			copy(sk, key)
			rnd.Read(sk[length.Addr:])

			prev, _, err = domains.GetLatest(kv.StorageDomain, tx, sk)
			require.NoError(b, err)

			prevKeys[string(sk)] = struct{}{}

			err = domains.DomainPut(kv.StorageDomain, tx, sk, common.Append(prev, []byte("v")), txNum, prev)
			require.NoError(b, err)
		}
	}
}
