// Copyright 2025 The Erigon Authors
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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

// regionAround returns the page-aligned mmap slice covering [offset, offset+window)
// within the getter's data, mirroring what the residency gate probes.
func regionAround(g *Getter, offset uint64, window int) []byte {
	full := g.d.mmapHandle1
	base := int(uintptr(unsafe.Pointer(&g.data[0])) - uintptr(unsafe.Pointer(&full[0])))
	absStart := base + int(offset)
	pg := os.Getpagesize()
	aligned := absStart &^ (pg - 1)
	end := min(absStart+window, len(full))
	return full[aligned:end]
}

func TestResidencyGateWarmsOnReset(t *testing.T) {
	tmp := t.TempDir()
	file := filepath.Join(tmp, "test.kv")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	c, err := NewCompressor(t.Context(), "t", file, tmp, cfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	const n = 40000
	for i := 0; i < n; i++ {
		require.NoError(t, c.AddWord(fmt.Appendf(nil, "residency-word-%d-padding-aaaaaaaaaaaaaaaaaaaa", i)))
	}
	require.NoError(t, c.Compress())
	c.Close()

	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()

	// Collect (offset, word) pairs; iterating warms the whole file.
	type ent struct {
		off uint64
		w   []byte
	}
	var ents []ent
	gi := d.MakeGetter()
	for gi.HasNext() {
		off := gi.dataP
		w, _ := gi.Next(nil)
		ents = append(ents, ent{off, append([]byte(nil), w...)})
	}
	require.Greater(t, len(ents), n/2)
	pick := ents[len(ents)*3/4]

	// Probe just the word's start page; the gate warms at least this,
	// independent of the configured window size.
	window := os.Getpagesize()
	evict := func() {
		require.NoError(t, unix.Madvise(d.mmapHandle1, unix.MADV_DONTNEED))
		require.NoError(t, d.f.Sync())
		require.NoError(t, unix.Fadvise(int(d.f.Fd()), 0, int64(len(d.mmapHandle1)), unix.FADV_DONTNEED))
	}
	isResident := func(g *Getter) bool {
		r, err := mmap.Resident(regionAround(g, pick.off, window))
		require.NoError(t, err)
		return r
	}

	// Ungated getter: Reset must not warm anything.
	evict()
	gu := d.MakeGetter()
	require.False(t, isResident(gu), "region must be cold right after eviction")
	gu.Reset(pick.off)
	require.False(t, isResident(gu), "ungated Reset must not warm the page")

	// Gated getter: Reset warms the word's pages before the mmap touch.
	evict()
	gg := d.MakeGetter()
	gg.EnableResidencyGate()
	require.False(t, isResident(gg), "region must be cold right after eviction")
	gg.Reset(pick.off)
	require.True(t, isResident(gg), "gated Reset should warm the word's pages")

	w, _ := gg.Next(nil)
	require.Equal(t, pick.w, w, "gated read must still return the correct word")
}
