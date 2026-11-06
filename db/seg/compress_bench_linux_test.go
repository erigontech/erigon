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

package seg

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
)

func cachedMiB(tb testing.TB, path string) float64 {
	f, err := os.Open(path)
	require.NoError(tb, err)
	defer f.Close()
	var st unix.Cachestat_t
	require.NoError(tb, unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &st, 0))
	return float64(st.Cache*uint64(os.Getpagesize())) / (1 << 20)
}

func BenchmarkCompressThenRead(b *testing.B) {
	logger := log.New()
	tmp := b.TempDir()
	out := filepath.Join(tmp, "bench.seg")
	word := make([]byte, 128)
	var cached float64
	for b.Loop() {
		c, err := NewCompressor(b.Context(), "bench", out, tmp, DefaultCfg, log.LvlDebug, logger)
		require.NoError(b, err)
		for i := range 4_000_000 {
			binary.BigEndian.PutUint64(word, uint64(i)*0x9e3779b97f4a7c15)
			require.NoError(b, c.AddUncompressedWord(word))
		}
		require.NoError(b, c.Compress())
		c.Close()
		cached += cachedMiB(b, out)

		d, err := NewDecompressor(out)
		require.NoError(b, err)
		for g := d.MakeGetter(); g.HasNext(); {
			g.Skip()
		}
		d.Close()
	}
	b.ReportMetric(cached/float64(b.N), "output-cached-MiB/op")
}
