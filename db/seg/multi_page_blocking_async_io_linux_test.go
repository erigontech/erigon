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
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/iouring"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

func TestMultiPageBlockingAsyncReadRange(t *testing.T) {
	page := uint64(os.Getpagesize())
	tests := []struct {
		name       string
		offset     uint64
		length     uint64
		wantOffset uint64
		wantLength uint64
		wantOK     bool
	}{
		{name: "empty"},
		{name: "one aligned page", offset: page, length: page},
		{name: "small boundary crossing", offset: page - 32, length: 64},
		{name: "one unaligned page", offset: page / 2, length: page},
		{name: "aligned large literal", offset: page, length: page + 1, wantOffset: page, wantLength: page + 1, wantOK: true},
		{name: "skip metadata-touched prefix", offset: page + page/2, length: 2 * page, wantOffset: 2 * page, wantLength: page + page/2, wantOK: true},
		{name: "overflow", offset: math.MaxUint64 - page/2, length: page + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, length, ok := multiPageBlockingAsyncReadRange(tt.offset, tt.length)
			require.Equal(t, tt.wantOffset, offset)
			require.Equal(t, tt.wantLength, length)
			require.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestMultiPageBlockingAsyncIOSkipsPageSizedWord(t *testing.T) {
	tmpDir := t.TempDir()
	fileName := filepath.Join(tmpDir, "page-sized.kv")
	cfg := DefaultCfg
	cfg.MinPatternScore = ^uint64(0)
	c, err := NewCompressor(t.Context(), t.Name(), fileName, tmpDir, cfg, log.LvlDebug, log.New())
	require.NoError(t, err)

	word := make([]byte, os.Getpagesize())
	require.NoError(t, c.AddWord(word))
	require.NoError(t, c.Compress())
	c.Close()

	d, err := NewDecompressor(fileName)
	require.NoError(t, err)
	defer d.Close()

	g := d.MakeGetter()
	g.EnableMultiPageBlockingAsyncIO()
	called := false
	g.multiPageBlockingAsyncRead = func(_ *Getter, _, _ uint64) {
		called = true
	}
	got, _ := g.Next(nil)

	require.Equal(t, word, got)
	require.False(t, called)
}

func TestMultiPageBlockingAsyncIO(t *testing.T) {
	page := os.Getpagesize()
	file, err := os.CreateTemp(t.TempDir(), "multi-page-blocking-async-io-*.kv")
	require.NoError(t, err)
	defer file.Close()

	payload := make([]byte, 4*page)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, err = file.Write(payload)
	require.NoError(t, err)
	require.NoError(t, file.Sync())

	mapping, err := mmap.OpenRo(file, len(payload))
	require.NoError(t, err)
	defer func() { require.NoError(t, mapping.Unmap()) }()

	region := mapping[:2*page]
	require.NoError(t, unix.Madvise(mapping, unix.MADV_DONTNEED))
	require.NoError(t, unix.Fadvise(int(file.Fd()), 0, int64(len(mapping)), unix.FADV_DONTNEED))
	resident, err := mmap.Resident(region)
	require.NoError(t, err)
	require.False(t, resident)

	g := &Getter{d: &Decompressor{f: file}}
	g.EnableMultiPageBlockingAsyncIO()
	require.NotNil(t, g.multiPageBlockingAsyncRead)
	g.multiPageBlockingAsyncRead(g, 0, uint64(len(region)))

	resident, err = mmap.Resident(region)
	require.NoError(t, err)
	require.Equal(t, iouring.Available(), resident)
}
