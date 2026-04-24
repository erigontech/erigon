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

package state

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

// BenchmarkDomainFlush measures DomainBufferedWriter.Flush throughput for two
// workload shapes:
//
//   - insert: all keys are brand-new (PutNoOverwrite fast-path fires every time, 1 CGo call)
//   - update: all keys already have a dup entry (falls back to SeekBothRange+PutCurrent, 2 CGo calls)
//
// Run with:
//
//	go test ./db/state/ -run=^$ -bench=BenchmarkDomainFlush -benchtime=5s
func BenchmarkDomainFlush(b *testing.B) {
	const nKeys = 1_000

	logger := log.New()
	ctx := context.Background()

	generateKV := func(i int) (key, val []byte) {
		key = make([]byte, 20)
		binary.BigEndian.PutUint64(key[12:], uint64(i))
		val = make([]byte, 32)
		binary.BigEndian.PutUint64(val, uint64(i))
		return key, val
	}

	b.Run("insert", func(b *testing.B) {
		// All keys are new on every iteration (tx is rolled back so the DB
		// stays empty across iterations — PutNoOverwrite wins every time).
		db, d := testDbAndDomain(b, logger)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginRw(ctx) //nolint:gocritic
			if err != nil {
				b.Fatal(err)
			}

			dc := d.beginForTests()
			w := dc.NewWriter()

			for j := 0; j < nKeys; j++ {
				k, v := generateKV(j)
				if err := w.PutWithPrev(k, v, uint64(j), nil); err != nil {
					b.Fatal(err)
				}
			}

			if err := w.Flush(ctx, tx); err != nil {
				b.Fatal(err)
			}
			w.Close()
			dc.Close()
			tx.Rollback() // keep DB empty for next iteration
		}
		b.StopTimer()
	})

	b.Run("update", func(b *testing.B) {
		// Pre-populate all keys in a committed tx so every Flush call hits
		// the update path (PutNoOverwrite returns inserted=false → SeekBothRange+PutCurrent).
		db, d := testDbAndDomain(b, logger)

		seedTx, err := db.BeginRw(ctx)
		if err != nil {
			b.Fatal(err)
		}
		defer seedTx.Rollback()
		{
			dc := d.beginForTests()
			w := dc.NewWriter()
			for j := 0; j < nKeys; j++ {
				k, v := generateKV(j)
				if err := w.PutWithPrev(k, v, uint64(j), nil); err != nil {
					b.Fatal(err)
				}
			}
			if err := w.Flush(ctx, seedTx); err != nil {
				b.Fatal(err)
			}
			w.Close()
			dc.Close()
		}
		if err := seedTx.Commit(); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginRw(ctx) //nolint:gocritic
			if err != nil {
				b.Fatal(err)
			}

			dc := d.beginForTests()
			w := dc.NewWriter()

			for j := 0; j < nKeys; j++ {
				k, v := generateKV(j)
				binary.BigEndian.PutUint64(v, uint64(i*nKeys+j)) // vary value each round
				if err := w.PutWithPrev(k, v, uint64(j), nil); err != nil {
					b.Fatal(err)
				}
			}

			if err := w.Flush(ctx, tx); err != nil {
				b.Fatal(err)
			}
			w.Close()
			dc.Close()
			tx.Rollback() // keep seed data intact for next iteration
		}
		b.StopTimer()
	})
}

// BenchmarkDomainFlushCursorOps isolates the per-entry cursor cost for insert
// vs update in a tight loop (no ETL overhead, direct cursor calls).
func BenchmarkDomainFlushCursorOps(b *testing.B) {
	const nKeys = 1_000

	logger := log.New()
	ctx := context.Background()

	db, d := testDbAndDomain(b, logger)
	valsTable := d.ValuesTable

	makeKey := func(i int) []byte {
		k := make([]byte, 20)
		binary.BigEndian.PutUint64(k[12:], uint64(i))
		return k
	}
	makeVal := func(step, payload int) []byte {
		v := make([]byte, 40)
		binary.BigEndian.PutUint64(v, ^uint64(step)) // inverted step prefix
		binary.BigEndian.PutUint64(v[8:], uint64(payload))
		return v
	}

	b.Run("PutNoOverwrite-insert", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginRw(ctx) //nolint:gocritic
			if err != nil {
				b.Fatal(err)
			}
			c, err := tx.RwCursorDupSort(valsTable) //nolint:gocritic
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
			for j := 0; j < nKeys; j++ {
				if _, err := c.PutNoOverwrite(makeKey(j), makeVal(0, j)); err != nil {
					c.Close()
					tx.Rollback()
					b.Fatal(err)
				}
			}
			c.Close()
			tx.Rollback() // DB stays empty: each iteration is fresh inserts
		}
	})

	// Seed the DB once so the update benchmark always hits existing keys.
	seedTx, err := db.BeginRw(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer seedTx.Rollback()
	sc, err := seedTx.RwCursorDupSort(valsTable)
	if err != nil {
		b.Fatal(err)
	}
	defer sc.Close()
	for j := 0; j < nKeys; j++ {
		if _, err := sc.PutNoOverwrite(makeKey(j), makeVal(0, j)); err != nil {
			sc.Close()
			seedTx.Rollback()
			b.Fatal(err)
		}
	}
	sc.Close()
	if err := seedTx.Commit(); err != nil {
		b.Fatal(err)
	}

	b.Run("SeekBothRange+PutCurrent-update", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.BeginRw(ctx) //nolint:gocritic
			if err != nil {
				b.Fatal(err)
			}
			c, err := tx.RwCursorDupSort(valsTable) //nolint:gocritic
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
			for j := 0; j < nKeys; j++ {
				v := makeVal(0, i*nKeys+j) // same step prefix, new payload
				fv, err := c.SeekBothRange(makeKey(j), v[:8])
				if err != nil {
					c.Close()
					tx.Rollback()
					b.Fatal(err)
				}
				if len(fv) > 0 && fv[0] == v[0] {
					if err := c.PutCurrent(makeKey(j), v); err != nil {
						c.Close()
						tx.Rollback()
						b.Fatal(err)
					}
				} else {
					if err := c.Put(makeKey(j), v); err != nil {
						c.Close()
						tx.Rollback()
						b.Fatal(err)
					}
				}
			}
			c.Close()
			tx.Rollback()
		}
	})
}
