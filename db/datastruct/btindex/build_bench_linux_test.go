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
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func cachedMiB(tb testing.TB, path string) (float64, bool) {
	f, err := os.Open(path)
	require.NoError(tb, err)
	defer f.Close()
	cached, err := cachedPages(f)
	if err != nil {
		return 0, false
	}
	return float64(cached*uint64(os.Getpagesize())) / (1 << 20), true
}

func BenchmarkBuildBtreeIndex(b *testing.B) {
	logger := log.New()
	tmp := b.TempDir()
	decomp, err := seg.NewDecompressor(generateKV(b, tmp, 32, 64, 4_000_000, logger, seg.CompressNone))
	require.NoError(b, err)
	defer decomp.Close()
	var keys [][]byte
	for g := decomp.MakeGetter(); g.HasNext(); {
		k, _ := g.Next(nil)
		keys = append(keys, k)
		g.Skip()
	}
	rnd := rand.New(rand.NewPCG(1, 2))

	indexPath := filepath.Join(tmp, "bench.bt")
	var cached float64
	measured := cacheMeasurable(b, tmp)
	var lookup time.Duration
	for b.Loop() {
		r := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
		require.NoError(b, BuildBtreeIndexWithDecompressor(indexPath, "", r, background.NewProgressSet(), tmp, 1, logger, false, statecfg.AccessorBTree))
		b.StopTimer()
		if measured {
			mib, ok := cachedMiB(b, indexPath)
			measured = measured && ok
			cached += mib
		}

		start := time.Now()
		bt, err := OpenBtreeIndexWithDecompressor(indexPath, seg.NewReader(decomp.MakeGetter(), seg.CompressNone))
		require.NoError(b, err)
		gr := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
		for range 200_000 {
			_, _, _, found, err := bt.Get(keys[rnd.IntN(len(keys))], nil, gr)
			require.NoError(b, err)
			require.True(b, found)
		}
		bt.Close()
		lookup += time.Since(start)
		b.StartTimer()
	}
	if measured {
		b.ReportMetric(cached/float64(b.N), "index-cached-MiB/op")
	}
	b.ReportMetric(float64(lookup.Nanoseconds())/float64(b.N), "lookup-ns/op")
}
