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

package btindex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func cachedMiB(tb testing.TB, path string) float64 {
	f, err := os.Open(path)
	require.NoError(tb, err)
	defer f.Close()
	var st unix.Cachestat_t
	require.NoError(tb, unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &st, 0))
	return float64(st.Cache*uint64(os.Getpagesize())) / (1 << 20)
}

func BenchmarkBuildBtreeIndex(b *testing.B) {
	logger := log.New()
	tmp := b.TempDir()
	decomp, err := seg.NewDecompressor(generateKV(b, tmp, 32, 64, 4_000_000, logger, seg.CompressNone))
	require.NoError(b, err)
	defer decomp.Close()
	for g := decomp.MakeGetter(); g.HasNext(); {
		g.Skip()
	}

	indexPath := filepath.Join(tmp, "bench.bt")
	var cached float64
	for b.Loop() {
		r := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
		require.NoError(b, BuildBtreeIndexWithDecompressor(indexPath, "", r, background.NewProgressSet(), tmp, 1, logger, false, statecfg.AccessorBTree))
		cached += cachedMiB(b, indexPath)
	}
	b.ReportMetric(cached/float64(b.N), "index-cached-MiB/op")
}
