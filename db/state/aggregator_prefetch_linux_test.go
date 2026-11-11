// Copyright 2022 The Erigon Authors
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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/commitment"
)

// A batch prefetch over cold domain files changes no latest value and leaves no probe on the readers.
func TestPrefetchLatestOverColdFilesKeepsLatestValues(t *testing.T) {
	prev := mmap.BatchPrefetch
	mmap.BatchPrefetch = true
	t.Cleanup(func() { mmap.BatchPrefetch = prev })
	db, d, txs := filledDomain(t, log.New())
	require.NoError(t, db.UpdateNosync(t.Context(), func(tx kv.RwTx) error {
		collateAndMerge(t, tx, d, txs)
		return nil
	}))
	maps := fileMappings(t, d.dirs.SnapDomain)
	require.NotEmpty(t, maps)
	pageOut(t, d.dirs.SnapDomain, maps)
	var kvMaps []fileMapping
	for _, m := range maps {
		if strings.HasSuffix(m.path, ".kv") {
			kvMaps = append(kvMaps, m)
		}
	}
	require.NotEmpty(t, kvMaps)
	before, kvBefore := residentPages(t, maps), residentPages(t, kvMaps)

	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	keys := make([][]byte, 0, 31)
	for keyNum := uint64(1); keyNum <= 31; keyNum++ {
		keys = append(keys, binary.BigEndian.AppendUint64(nil, keyNum))
	}
	dt := d.beginForTests()
	defer dt.Close()
	at := &AggregatorRoTx{}
	at.d[kv.AccountsDomain] = dt
	pages, err := at.PrefetchLatest(t.Context(), kv.AccountsDomain, keys)
	require.NoError(t, err)
	require.Positive(t, pages)
	require.Greater(t, residentPages(t, maps), before, "the batch read cold file pages")
	require.Greater(t, residentPages(t, kvMaps), kvBefore, "the batch read cold .kv pages")
	require.Nil(t, dt.probe)
	for _, r := range dt.dataReaders {
		if r != nil {
			require.Nil(t, r.Probe())
		}
	}

	ref := d.beginForTests()
	defer ref.Close()
	for _, k := range keys {
		want, _, wantOK, err := ref.GetLatest(k, roTx)
		require.NoError(t, err)
		got, _, gotOK, err := dt.GetLatest(k, roTx)
		require.NoError(t, err)
		require.Equal(t, wantOK, gotOK, "%x", k)
		require.Equal(t, want, got, "%x", k)
	}
}

type fileMapping struct {
	start, end uintptr
	path       string
}

// fileMappings returns this process's mappings of the files under dir.
func fileMappings(t *testing.T, dir string) []fileMapping {
	t.Helper()
	raw, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	var maps []fileMapping
	for line := range strings.SplitSeq(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 || !strings.HasPrefix(fields[5], dir) {
			continue
		}
		m := fileMapping{path: fields[5]}
		_, err := fmt.Sscanf(fields[0], "%x-%x", &m.start, &m.end)
		require.NoError(t, err)
		maps = append(maps, m)
	}
	return maps
}

func residentPages(t *testing.T, maps []fileMapping) int {
	t.Helper()
	pg := uintptr(os.Getpagesize())
	n := 0
	for _, m := range maps {
		vec := make([]byte, (m.end-m.start+pg-1)/pg)
		_, _, errno := unix.Syscall(unix.SYS_MINCORE, m.start, m.end-m.start, uintptr(unsafe.Pointer(&vec[0])))
		require.Zero(t, errno)
		for _, v := range vec {
			n += int(v & 1)
		}
	}
	return n
}

// pageOut writes the domain files back, since the fixture skips fsync and dirty pages stay resident, then pages out
// this process's mappings of them, which the domain keeps from building its files.
func pageOut(t *testing.T, dir string, maps []fileMapping) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		f, err := os.OpenFile(filepath.Join(dir, e.Name()), os.O_RDWR, 0)
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())
	}
	for _, m := range maps {
		_, _, errno := unix.Syscall(unix.SYS_MADVISE, m.start, m.end-m.start, unix.MADV_PAGEOUT)
		require.Zero(t, errno)
	}
}

// A prefetch that stops at its wave cap with lookups pending reports it, so a caller does not treat those keys as read.
func TestPrefetchLatestReportsLookupsLeftAtTheWaveCap(t *testing.T) {
	prev, prevWaves := mmap.BatchPrefetch, prefetchMaxWaves
	mmap.BatchPrefetch, prefetchMaxWaves = true, 1
	t.Cleanup(func() { mmap.BatchPrefetch, prefetchMaxWaves = prev, prevWaves })
	db, d, txs := filledDomain(t, log.New())
	require.NoError(t, db.UpdateNosync(t.Context(), func(tx kv.RwTx) error {
		collateAndMerge(t, tx, d, txs)
		return nil
	}))
	maps := fileMappings(t, d.dirs.SnapDomain)
	require.NotEmpty(t, maps)
	pageOut(t, d.dirs.SnapDomain, maps)

	keys := make([][]byte, 0, 31)
	for keyNum := uint64(1); keyNum <= 31; keyNum++ {
		keys = append(keys, binary.BigEndian.AppendUint64(nil, keyNum))
	}
	dt := d.beginForTests()
	defer dt.Close()
	at := &AggregatorRoTx{}
	at.d[kv.AccountsDomain] = dt
	_, err := at.PrefetchLatest(t.Context(), kv.AccountsDomain, keys)
	require.ErrorIs(t, err, kv.ErrPrefetchIncomplete)
}

// Expanding a branch reads the cold file pages its shortened keys point to in one batch, and leaves no probe behind.
func TestReadReferencePagesReadsColdReferencedPages(t *testing.T) {
	prev := mmap.BatchPrefetch
	mmap.BatchPrefetch = true
	t.Cleanup(func() { mmap.BatchPrefetch = prev })
	dir := t.TempDir()
	path := filepath.Join(dir, "refs.kv")
	c, err := seg.NewCompressor(t.Context(), t.Name(), path, dir, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	key := make([]byte, length.Addr)
	const words = 4096
	for i := range words {
		binary.BigEndian.PutUint32(key, uint32(i))
		require.NoError(t, c.AddUncompressedWord(key))
	}
	require.NoError(t, c.Compress())
	c.Close()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()

	offsets := make([]uint64, words)
	g := d.MakeGetter()
	for i := 1; i < words; i++ {
		offsets[i], _ = g.Skip()
	}
	const accountKeyField = 2 // commitment.fieldAccountAddr
	branch := []byte{0, 0x0f, 0, 0x0f}
	for _, word := range []int{1, 1000, 2000, 3000} {
		ref := EncodeReferenceKey(nil, offsets[word])
		branch = append(branch, accountKeyField)
		branch = binary.AppendUvarint(branch, uint64(len(ref)))
		branch = append(branch, ref...)
	}

	maps := fileMappings(t, dir)
	require.NotEmpty(t, maps)
	pageOut(t, dir, maps)
	before := residentPages(t, maps)
	r := seg.NewReader(d.MakeGetter(), seg.CompressNone)
	require.Equal(t, 4, readReferencePages(commitment.BranchData(branch), r, r))
	require.GreaterOrEqual(t, residentPages(t, maps), before+4, "the referenced pages were read")
	require.Nil(t, r.Probe())
}
