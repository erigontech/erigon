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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
)

func spillCachedPages(files []uint) (uint64, error) {
	var total uint64
	for _, fd := range files {
		var st unix.Cachestat_t
		if err := unix.Cachestat(fd, &unix.CachestatRange{}, &st, 0); err != nil {
			return 0, err
		}
		total += st.Cache
	}
	return total, nil
}

func BenchmarkCollectorSpillAndLoad(b *testing.B) {
	require.NotEqual(b, "false", os.Getenv("FILE_BUILD_DONTCACHE"))
	require.True(b, cacheMeasurable(b, b.TempDir()), "cachestat is required")
	probe, openErr := os.CreateTemp(b.TempDir(), "dontcache-probe-")
	require.NoError(b, openErr)
	defer probe.Close()
	page := make([]byte, os.Getpagesize())
	for {
		n, err := unix.Pwritev2(int(probe.Fd()), [][]byte{page}, 0, unix.RWF_DONTCACHE)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		require.NoError(b, err, "RWF_DONTCACHE writes are required")
		require.Equal(b, len(page), n)
		break
	}
	logger := log.New()
	key, val := make([]byte, 32), make([]byte, 32)
	noop := func(_, _ []byte, _ CurrentTableReader, _ LoadNextFunc) error { return nil }
	var cached, peakPages uint64
	var loadTime time.Duration
	var samples int
	for b.Loop() {
		c := NewCollector("bench", b.TempDir(), NewSortableBuffer(64*datasize.MB), logger)
		for i := range 8_000_000 {
			binary.BigEndian.PutUint64(key, uint64(i)*0x9e3779b97f4a7c15)
			require.NoError(b, c.Collect(key, val))
		}
		require.NoError(b, c.Flush())
		files := make([]uint, 0, len(c.dataProviders))
		for _, p := range c.dataProviders {
			require.NoError(b, p.Wait())
			fp, ok := p.(*fileDataProvider)
			require.True(b, ok)
			files = append(files, uint(fp.file.Fd()))
		}
		initial, initialErr := spillCachedPages(files)
		require.NoError(b, initialErr)
		cached += initial
		peak := initial
		count := 1
		stop, done := make(chan struct{}), make(chan struct{})
		var sampleErr error
		ticker := time.NewTicker(time.Millisecond)
		go func() {
			defer close(done)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
				case <-stop:
					return
				}
				pages, err := spillCachedPages(files)
				if err != nil {
					sampleErr = err
					return
				}
				peak = max(peak, pages)
				count++
			}
		}()
		start := time.Now()
		loadErr := c.Load(nil, "", noop, TransformArgs{})
		loadTime += time.Since(start)
		close(stop)
		<-done
		last, lastErr := spillCachedPages(files)
		peakPages += max(peak, last)
		samples += count + 1
		c.Close()
		require.NoError(b, loadErr)
		require.NoError(b, sampleErr)
		require.NoError(b, lastErr)
		require.Greater(b, count, 1, "Load completed without a periodic cache sample")
	}
	mibPerPage := float64(os.Getpagesize()) / (1 << 20)
	b.ReportMetric(float64(cached)*mibPerPage/float64(b.N), "spill-cached-MiB/op")
	b.ReportMetric(float64(peakPages)*mibPerPage/float64(b.N), "peak-cached-MiB/op")
	b.ReportMetric(float64(loadTime.Nanoseconds())/float64(b.N), "load-ns/op")
	b.ReportMetric(float64(samples)/float64(b.N), "cache-samples/op")
}
