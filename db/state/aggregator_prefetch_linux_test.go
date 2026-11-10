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
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
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

// A lookup stopped by a cold page continues in that page's file and does not walk the newer files again.
func TestPrefetchLookupContinuesInTheFileOfItsColdPage(t *testing.T) {
	prev := mmap.BatchPrefetch
	mmap.BatchPrefetch = true
	t.Cleanup(func() { mmap.BatchPrefetch = prev })
	cfg := statecfg.Schema.AccountsDomain
	cfg.Accessors = statecfg.AccessorHashMap
	cfg.FileVersion = statecfg.DomainVersionTypes{DataKV: version.V1_0_standart, AccessorBT: version.V1_0_standart, AccessorKVEI: version.V1_0_standart, AccessorKVI: version.V1_0_standart}
	db, d := testDbAndDomainOfStep(t, cfg, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	const files, keysPerFile = 3, 64
	writerTx := d.beginForTests()
	writer := writerTx.NewWriter(db)
	for step := range files {
		for i := range keysPerFile {
			require.NoError(t, writer.PutWithPrev(fmt.Appendf(nil, "key-%d-%d", step, i), []byte("value"), uint64(step)*d.stepSize+1, nil))
		}
	}
	require.NoError(t, writer.Flush(t.Context(), tx))
	writer.Close()
	writerTx.Close()
	for step := range files {
		require.NoError(t, d.collateBuildIntegrate(t.Context(), kv.Step(step), tx, background.NewProgressSet()))
	}
	dt := d.beginForTests()
	defer dt.Close()
	require.Len(t, dt.files, files)

	var oldest, newer []fileMapping
	for _, m := range fileMappings(t, d.dirs.SnapDomain) {
		if strings.Contains(m.path, ".0-1.") {
			oldest = append(oldest, m)
		} else {
			newer = append(newer, m)
		}
	}
	require.NotEmpty(t, oldest)
	require.NotEmpty(t, newer)
	for _, m := range newer {
		_, err := os.ReadFile(m.path)
		require.NoError(t, err)
	}
	evictFiles(t, d.dirs.SnapDomain, oldest)

	dt.probe = mmap.NewProbe()
	require.NotNil(t, dt.probe)
	defer func() { dt.probe = nil }()
	lookup := newPrefetchLookup(dt, []byte("key-0-0"))
	page, cold := lookup.probe(dt)
	require.True(t, cold)
	require.True(t, inMappings(page, oldest), "the warm newer files pass, the first cold page is in the oldest file")

	evictFiles(t, d.dirs.SnapDomain, newer)
	dt.probe = mmap.NewProbe()
	page, cold = lookup.probe(dt)
	require.True(t, cold)
	require.True(t, inMappings(page, oldest), "the next wave does not walk the newer files again")
}

func inMappings(page uintptr, maps []fileMapping) bool {
	return slices.ContainsFunc(maps, func(m fileMapping) bool { return m.start <= page && page < m.end })
}

// evictFiles pages out the mappings and drops their clean pages from the page cache.
func evictFiles(t *testing.T, dir string, maps []fileMapping) {
	t.Helper()
	pageOut(t, dir, maps)
	for _, m := range maps {
		f, err := os.Open(m.path)
		require.NoError(t, err)
		require.NoError(t, unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED))
		require.NoError(t, f.Close())
	}
}
