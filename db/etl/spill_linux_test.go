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
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
)

func dontCacheSupported(t *testing.T, dir string) bool {
	f, err := os.CreateTemp(dir, "probe-")
	require.NoError(t, err)
	defer f.Close()
	for {
		_, err = unix.Pwritev2(int(f.Fd()), [][]byte{make([]byte, os.Getpagesize())}, 0, unix.RWF_DONTCACHE)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EOPNOTSUPP) {
			t.Logf("page cache residency check did not run: RWF_DONTCACHE is unsupported on %s", dir)
			return false
		}
		require.NoError(t, err)
		return true
	}
}

func cachedPages(f *os.File) (uint64, error) {
	var st unix.Cachestat_t
	err := unix.Cachestat(uint(f.Fd()), &unix.CachestatRange{}, &st, 0)
	return st.Cache, err
}

func cacheMeasurable(t testing.TB, dir string) bool {
	f, err := os.CreateTemp(dir, "cached-probe-")
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write(make([]byte, os.Getpagesize()))
	require.NoError(t, err)
	cached, err := cachedPages(f)
	if err != nil || cached == 0 {
		t.Logf("page cache measurement unavailable: plain-write control on %s returned %d cached pages, error %v", dir, cached, err)
		return false
	}
	return true
}

func TestSortAndFlushLeavesWrittenPagesOutOfPageCache(t *testing.T) {
	tmp := t.TempDir()
	measurable := dontCacheSupported(t, tmp) && cacheMeasurable(t, tmp)
	if !dbg.EnvBool("FILE_BUILD_DONTCACHE", true) {
		t.Logf("page cache residency check did not run: FILE_BUILD_DONTCACHE is false")
		measurable = false
	}

	buffer := NewSortableBuffer(8 * datasize.MB)
	defer buffer.Reset()
	buffer.Put([]byte("key"), bytes.Repeat([]byte{0xa5}, 4<<20))
	f, err := sortAndFlush(buffer, tmp)
	if f != nil {
		t.Cleanup(func() { require.NoError(t, dir.RemoveFile(f.Name())) })
		defer f.Close()
	}
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	st, err := f.Stat()
	require.NoError(t, err)

	if measurable {
		pageSize := int64(os.Getpagesize())
		pages := uint64((st.Size() + pageSize - 1) / pageSize)
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
		require.Less(t, cached, pages/10, "kernel supports RWF_DONTCACHE on %s: %d of %d spill pages still cached", tmp, cached, pages)
	}

	var want bytes.Buffer
	require.NoError(t, buffer.Write(&want))
	got, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, want.Len(), len(got))
	require.True(t, bytes.Equal(want.Bytes(), got))
}
