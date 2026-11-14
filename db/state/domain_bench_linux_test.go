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

//go:build linux

package state

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
)

// BenchmarkAbsentKeyLatestLookup compares, per absent key over several domain files, the existence-filter pass that
// PrefetchLatest runs first (a mapped filter keeps every key) with the probed lookup it used to run for every key.
func BenchmarkAbsentKeyLatestLookup(b *testing.B) {
	prev := domainExistenceForceRandom
	domainExistenceForceRandom = []string{kv.AccountsDomain.String()} // build mapped filters
	b.Cleanup(func() { domainExistenceForceRandom = prev })
	db, d := testDbAndDomain(b, log.New())
	tx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()
	const files, keysPerFile = 8, 4000
	domainTx := d.beginForTests()
	writer := domainTx.NewWriter(db)
	for step := range files {
		for i := range keysPerFile {
			require.NoError(b, writer.PutWithPrev(fmt.Appendf(nil, "present-%d-%d", step, i), []byte("v"), uint64(step)*d.stepSize+1, nil))
		}
	}
	require.NoError(b, writer.Flush(b.Context(), tx))
	writer.Close()
	domainTx.Close()
	for step := range files {
		require.NoError(b, d.collateBuildIntegrate(b.Context(), kv.Step(step), tx, background.NewProgressSet()))
	}
	dt := d.beginForTests()
	defer dt.Close()
	require.Len(b, dt.files, files)
	absent := make([][]byte, 1<<20) // distinct keys: a repeated key would hit the file cache
	for i := range absent {
		absent[i] = fmt.Appendf(nil, "absent-%d", i)
	}
	b.Run("mapped-filter-pass", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			dt.mayContainInFiles(absent[i%len(absent)])
		}
	})
	for _, f := range dt.files {
		f.src.existence.ForceInMem()
	}
	b.Run("in-heap-filter-pass", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			dt.mayContainInFiles(absent[i%len(absent)])
		}
	})
	b.Run("probed-lookup", func(b *testing.B) {
		defer func(prev bool) { mmap.BatchPrefetch = prev }(mmap.BatchPrefetch)
		mmap.BatchPrefetch = true
		if dt.probe = mmap.NewProbe(); dt.probe == nil {
			b.Fatal("no probe: the probed lookup needs io_uring")
		}
		defer func() { dt.probe = nil }()
		for i := 0; b.Loop(); i++ {
			probeLatestFromFiles(dt, absent[i%len(absent)])
		}
	})
}
