// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package enode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"

	mdbxgo "github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

// BenchmarkNodeDBGeometry for `dbSyncBytesThreshold` constant
//   - must use `-cpu=` because even 1-core overloading disk write IO. Multi-core bench is unreliable here (degradating with time).
//   - same about `-benchtime=30s` and -benchtime can't be smaller than SyncPeriod
//   - target is: to improve throughput without big ms_worst
//   - params to choose: pageSize, syncPeriod, syncBytes
//
// go test -bench=BenchmarkNodeDBGeometry/20kb      -run=BenchmarkNodeDBGeometry -cpu=1 -count=1 -benchtime=10s ./p2p/enode > old.txt
// go test -bench=BenchmarkNodeDBGeometry/10mb_1sec -run=BenchmarkNodeDBGeometry -cpu=1 -count=1 -benchtime=10s ./p2p/enode > new.txt
// benchstat old.txt new.txt
func BenchmarkNodeDBGeometry(b *testing.B) {
	keys, vals := make([][]byte, 100_000), make([][]byte, 100_000)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key %d", i)
		vals[i] = fmt.Appendf(nil, "val %d", i)
	}
	cfg := mdbx.New(dbcfg.ChainDB, log.New()).
		PageSize(4 * datasize.KB).
		MapSize(8 * datasize.GB).
		GrowthStep(2 * datasize.MB).
		Flags(func(f uint) uint {
			return mdbxgo.SafeNoSync | mdbxgo.WriteMap
		}).
		SyncBytes(20 * datasize.MB).
		SyncPeriod(2 * time.Second).
		DirtySpace(uint64(32 * datasize.MB))

	doBench := func(b *testing.B, db kv.RwDB) {
		//b.ReportAllocs()
		time.Sleep(200 * time.Millisecond) // give for OS a bit time between bench runs - to reduce cumulative effect
		b.ResetTimer()
		var worst time.Duration
		var i int
		for b.Loop() {
			i++
			tx, _ := db.BeginRw(context.Background()) //nolint:gocritic

			v := vals[i%len(vals)]
			v[0]++ // modify value a bit on every update
			_ = tx.Put(kv.Headers, keys[i%len(keys)], v)
			t := time.Now()
			_ = tx.Commit()
			worst = max(worst, time.Since(t))
		}
		b.ReportMetric(float64(worst.Milliseconds()), "ms_worst")
		db.Close()
		dir.RemoveFile(db.Path())
	}

	b.Run("20kb", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(20 * datasize.KB).SyncPeriod(1 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("200kb", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(200 * datasize.KB).SyncPeriod(1 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("1mb 1sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(1 * datasize.MB).SyncPeriod(1 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("1mb 2sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(1 * datasize.MB).SyncPeriod(2 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("5mb 1sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(5 * datasize.MB).SyncPeriod(1 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("5mb 2sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(5 * datasize.MB).SyncPeriod(2 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})

	b.Run("10mb 1sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(10 * datasize.MB).SyncPeriod(1 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})

	b.Run("10mb 2sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(10 * datasize.MB).SyncPeriod(2 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("10mb 5sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(10 * datasize.MB).SyncPeriod(5 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})

	b.Run("20mb 1sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(20 * datasize.MB).SyncPeriod(1 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})

	b.Run("20mb 2sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(20 * datasize.MB).SyncPeriod(2 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
	b.Run("20mb 5sec", func(b *testing.B) {
		db := cfg.Path(b.TempDir()).SyncBytes(20 * datasize.MB).SyncPeriod(5 * time.Second).MustOpen()
		defer db.Close()
		doBench(b, db)
	})
}
