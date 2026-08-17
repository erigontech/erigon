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
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/iouring"
	"github.com/erigontech/erigon/common/mmap"
)

func TestShouldWarmLiteral(t *testing.T) {
	page := uint64(os.Getpagesize())

	require.False(t, shouldWarmLiteral(0, 0))
	require.False(t, shouldWarmLiteral(page, page))
	require.True(t, shouldWarmLiteral(page/2, page))
	require.True(t, shouldWarmLiteral(page, page+1))
	require.False(t, shouldWarmLiteral(math.MaxUint64-page/2, page))
}

func TestWarmLiteral(t *testing.T) {
	page := os.Getpagesize()
	file, err := os.CreateTemp(t.TempDir(), "literal-warm-*.kv")
	require.NoError(t, err)
	defer file.Close()

	payload := make([]byte, 4*page)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, err = file.Write(payload)
	require.NoError(t, err)
	require.NoError(t, file.Sync())

	mapping, mappingHandle, err := mmap.Mmap(file, len(payload))
	require.NoError(t, err)
	defer mmap.Munmap(mapping, mappingHandle)

	region := mapping[:2*page]
	require.NoError(t, unix.Madvise(mapping, unix.MADV_DONTNEED))
	require.NoError(t, unix.Fadvise(int(file.Fd()), 0, int64(len(mapping)), unix.FADV_DONTNEED))
	resident, err := mmap.Resident(region)
	require.NoError(t, err)
	require.False(t, resident)

	g := &Getter{d: &Decompressor{f: file}}
	g.EnableAsyncLiteralWarm()
	require.NotNil(t, g.literalWarmer)
	g.literalWarmer(g, 0, uint64(len(region)))

	resident, err = mmap.Resident(region)
	require.NoError(t, err)
	require.Equal(t, iouring.Available(), resident)
}
