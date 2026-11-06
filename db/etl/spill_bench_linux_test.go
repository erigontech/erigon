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

package etl

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
)

func cachedMiB(tb testing.TB, f *os.File) float64 {
	var st unix.Cachestat_t
	require.NoError(tb, unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &st, 0))
	return float64(st.Cache*uint64(os.Getpagesize())) / (1 << 20)
}

func BenchmarkCollectorSpillAndLoad(b *testing.B) {
	logger := log.New()
	key, val := make([]byte, 32), make([]byte, 32)
	noop := func(_, _ []byte, _ CurrentTableReader, _ LoadNextFunc) error { return nil }
	var cached float64
	for b.Loop() {
		c := NewCollector("bench", b.TempDir(), NewSortableBuffer(64*datasize.MB), logger)
		for i := range 8_000_000 {
			binary.BigEndian.PutUint64(key, uint64(i)*0x9e3779b97f4a7c15)
			require.NoError(b, c.Collect(key, val))
		}
		require.NoError(b, c.Flush())
		for _, p := range c.dataProviders {
			require.NoError(b, p.Wait())
			if fp, ok := p.(*fileDataProvider); ok {
				cached += cachedMiB(b, fp.file)
			}
		}
		require.NoError(b, c.Load(nil, "", noop, TransformArgs{}))
		c.Close()
	}
	b.ReportMetric(cached/float64(b.N), "spill-cached-MiB/op")
}
