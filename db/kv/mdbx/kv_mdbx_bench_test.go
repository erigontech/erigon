// Copyright 2022 The Erigon Authors
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

package mdbx_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func BenchmarkDB_BeginRO(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	db := _db.(*mdbx.MdbxKV)

	for b.Loop() {
		tx, _ := db.BeginRo(b.Context())
		tx.Rollback()
	}
}

func BenchmarkDB_BeginROParallel(b *testing.B) {
	db := BaseCaseDBForBenchmark(b)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := db.BeginRo(b.Context()) //nolint:gocritic
			if err != nil {
				b.Error(err)
				return
			}
			tx.Rollback()
		}
	})
}

func BenchmarkDB_Get(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*mdbx.MdbxKV)

	// buffered so we never leak goroutines
	err := db.Update(b.Context(), func(tx kv.RwTx) error {
		return tx.Put(table, u64tob(uint64(1)), u64tob(uint64(1)))
	})
	if err != nil {
		b.Fatal(err)
	}

	// Ensure data is correct.
	if err := db.View(b.Context(), func(tx kv.Tx) error {
		key := u64tob(uint64(1))
		for b.Loop() {
			v, err := tx.GetOne(table, key)
			if err != nil {
				return err
			}
			if v == nil {
				b.Errorf("key not found: %d", 1)
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_Put(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*mdbx.MdbxKV)

	const keyCount = 10000
	keys := make([][]byte, keyCount)
	for i := 1; i <= keyCount; i++ {
		keys[i-1] = u64tob(uint64(i))
	}

	if err := db.Update(b.Context(), func(tx kv.RwTx) error {
		var idx int
		for b.Loop() {
			err := tx.Put(table, keys[idx%len(keys)], keys[idx%len(keys)])
			if err != nil {
				return err
			}
			idx++
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_PutRandom(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*mdbx.MdbxKV)

	// Ensure data is correct.
	if err := db.Update(b.Context(), func(tx kv.RwTx) error {
		keys := make(map[string]struct{}, b.N)
		for len(keys) < b.N {
			keys[string(u64tob(uint64(rand.Intn(1e10))))] = struct{}{}
		}
		b.ResetTimer()
		for key := range keys {
			err := tx.Put(table, []byte(key), []byte(key))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_Delete(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	db := _db.(*mdbx.MdbxKV)

	const keyCount = 10000
	keys := make([][]byte, keyCount)
	for i := 1; i <= keyCount; i++ {
		keys[i-1] = u64tob(uint64(i))
	}

	if err := db.Update(b.Context(), func(tx kv.RwTx) error {
		for i := range keys {
			err := tx.Put(table, keys[i], keys[i])
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	if err := db.Update(b.Context(), func(tx kv.RwTx) error {
		var idx int
		for b.Loop() {
			err := tx.Delete(table, keys[idx%len(keys)])
			if err != nil {
				return err
			}
			idx++
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDB_ResetSequence(b *testing.B) {
	_db := BaseCaseDBForBenchmark(b)
	table := "Table"
	//db := _db.(*mdbx.MdbxKV)
	ctx := b.Context()

	tx, err := _db.BeginRw(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	for i := 0; b.Loop(); i++ {
		err = tx.ResetSequence(table, uint64(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BaseCaseDBForBenchmark(b *testing.B) kv.RwDB {
	b.Helper()
	path := b.TempDir()
	logger := log.New()
	table := "Table"
	db := mdbxtest.InMem(b, mdbx.New(dbcfg.ChainDB, logger), path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table:       kv.TableCfgItem{Flags: kv.DupSort},
			kv.Sequence: kv.TableCfgItem{},
		}
	}).MapSize(128 * datasize.MB).MustOpen()
	b.Cleanup(db.Close)
	return db
}

func BenchmarkRangeDupSortIntersect(b *testing.B) {
	const bigN = 200_000
	db := BaseCaseDBForBenchmark(b)
	rwTx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer rwTx.Rollback()
	rnd := rand.New(rand.NewSource(1))
	for n := range uint64(bigN) {
		require.NoError(b, rwTx.Put("Table", []byte("big"), binary.BigEndian.AppendUint64(nil, n*10)))
	}
	ratios := []int{1, 1000}
	for _, r := range ratios {
		for range bigN / r {
			v := uint64(rnd.Intn(bigN)) * 10
			require.NoError(b, rwTx.Put("Table", fmt.Appendf(nil, "small%d", r), binary.BigEndian.AppendUint64(nil, v)))
		}
	}
	require.NoError(b, rwTx.Commit())
	tx, err := db.BeginRo(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()
	decode := func(_, v []byte) (uint64, error) { return binary.BigEndian.Uint64(v), nil }
	rangeU64 := func(key string, seek bool) stream.U64 {
		it, err := tx.RangeDupSort("Table", []byte(key), nil, nil, order.Asc, -1)
		require.NoError(b, err)
		if seek {
			return stream.TransformKV2U64Seekable(it, decode, binary.BigEndian.AppendUint64)
		}
		return stream.TransformKV2U64(it, decode)
	}
	for _, r := range ratios {
		for _, seek := range []bool{false, true} {
			b.Run(fmt.Sprintf("1:%d/seek=%t", r, seek), func(b *testing.B) {
				for b.Loop() {
					it := stream.Intersect[uint64](rangeU64(fmt.Sprintf("small%d", r), seek), rangeU64("big", seek), order.Asc, -1)
					for it.HasNext() {
						if _, err := it.Next(); err != nil {
							b.Fatal(err)
						}
					}
					it.Close()
				}
			})
		}
	}
}
