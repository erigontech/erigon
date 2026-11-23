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

package dir

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func dontCacheSupported(t *testing.T, dir string) bool {
	return probeDontCacheSupported(t, dir, unix.Pwritev2)
}

type pwritev2Func func(int, [][]byte, int64, int) (int, error)

func probeDontCacheSupported(t *testing.T, dir string, write pwritev2Func) bool {
	f, err := os.CreateTemp(dir, "probe-")
	require.NoError(t, err)
	defer f.Close()
	for {
		_, err = write(int(f.Fd()), [][]byte{make([]byte, os.Getpagesize())}, 0, unix.RWF_DONTCACHE)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			t.Logf("page cache residency check did not run: RWF_DONTCACHE probe on %s returned %v", dir, err)
			return false
		}
		return true
	}
}

func TestDontCacheSupportedTreatsProbeErrorsAsUnsupported(t *testing.T) {
	for _, probeErr := range []error{unix.EOPNOTSUPP, unix.ENOSYS, unix.EPERM} {
		t.Run(probeErr.Error(), func(t *testing.T) {
			supported := probeDontCacheSupported(t, t.TempDir(), func(int, [][]byte, int64, int) (int, error) {
				return 0, probeErr
			})
			require.False(t, supported)
		})
	}
}

func cachedPages(f *os.File) (uint64, error) {
	var st unix.Cachestat_t
	err := unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &st, 0)
	return st.Cache, err
}

func cacheMeasurable(t *testing.T, dir string) bool {
	f, err := os.CreateTemp(dir, "cached-probe-")
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write(make([]byte, os.Getpagesize()))
	require.NoError(t, err)
	cached, err := cachedPages(f)
	if err != nil || cached == 0 {
		t.Logf("page cache residency check did not run: plain-write control on %s returned %d cached pages, error %v", dir, cached, err)
		return false
	}
	return true
}

func TestUncachedWriterLeavesWrittenPagesOutOfPageCache(t *testing.T) {
	prev := dontCacheWrites
	dontCacheWrites = true
	t.Cleanup(func() { dontCacheWrites = prev })

	tmp := t.TempDir()
	measurable := dontCacheSupported(t, tmp) && cacheMeasurable(t, tmp)

	f, err := os.CreateTemp(tmp, "uncached-")
	require.NoError(t, err)
	defer f.Close()

	data := bytes.Repeat([]byte{0xa5}, 16<<20)
	bw := bufio.NewWriterSize(UncachedWriter(f), 512<<10)
	_, err = bw.Write(data)
	require.NoError(t, err)
	require.NoError(t, bw.Flush())
	require.NoError(t, f.Sync())

	if measurable {
		pages := uint64(len(data) / os.Getpagesize())
		var cached uint64
		deadline := time.Now().Add(5 * time.Second)
		for {
			cached, err = cachedPages(f)
			require.NoError(t, err)
			if cached < pages/10 || !time.Now().Before(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.Less(t, cached, pages/10, "kernel supports RWF_DONTCACHE on %s: %d of %d pages still cached", tmp, cached, pages)
	}

	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got))
}
