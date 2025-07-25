// Copyright 2021 The Erigon Authors
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

package bitmapdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
)

func TestFixedSizeBitmaps(t *testing.T) {

	tmpDir, require := t.TempDir(), require.New(t)
	must := require.NoError
	idxPath := filepath.Join(tmpDir, "idx.tmp")
	wr, err := NewFixedSizeBitmapsWriter(idxPath, 14, 0, 7, log.New())
	require.NoError(err)
	defer wr.Close()

	must(wr.AddArray(0, []uint64{3, 9, 11}))
	must(wr.AddArray(1, []uint64{1, 2, 3}))
	must(wr.AddArray(2, []uint64{4, 8, 13}))
	must(wr.AddArray(3, []uint64{1, 13}))
	must(wr.AddArray(4, []uint64{1, 13}))
	must(wr.AddArray(5, []uint64{1, 13}))
	must(wr.AddArray(6, []uint64{0, 9, 13}))
	must(wr.AddArray(7, []uint64{7}))

	require.Error(wr.AddArray(8, []uint64{8}))
	err = wr.Build()
	require.NoError(err)

	bm, err := OpenFixedSizeBitmaps(idxPath)
	require.NoError(err)
	defer bm.Close()

	at := func(item uint64) []uint64 {
		n, err := bm.At(item)
		require.NoError(err)
		return n
	}

	require.Equal([]uint64{3, 9, 11}, at(0))
	require.Equal([]uint64{1, 2, 3}, at(1))
	require.Equal([]uint64{4, 8, 13}, at(2))
	require.Equal([]uint64{1, 13}, at(3))
	require.Equal([]uint64{1, 13}, at(4))
	require.Equal([]uint64{1, 13}, at(5))
	require.Equal([]uint64{0, 9, 13}, at(6))
	require.Equal([]uint64{7}, at(7))

	fst, snd, ok, ok2, err := bm.First2At(7, 0)
	require.NoError(err)
	require.Equal(uint64(7), fst)
	require.Equal(uint64(0), snd)
	require.True(ok)
	require.False(ok2)

	fst, snd, ok, ok2, err = bm.First2At(2, 8)
	require.NoError(err)
	require.Equal(uint64(8), fst)
	require.Equal(uint64(13), snd)
	require.True(ok)
	require.True(ok2)

	fst, snd, ok, ok2, err = bm.First2At(2, 9)
	require.NoError(err)
	require.Equal(uint64(13), fst)
	require.Equal(uint64(0), snd)
	require.True(ok)
	require.False(ok2)

	_, err = bm.At(8)
	require.Error(err)
}

func TestPageAlined(t *testing.T) {
	tmpDir, require := t.TempDir(), require.New(t)
	idxPath := filepath.Join(tmpDir, "idx.tmp")

	bm2, err := NewFixedSizeBitmapsWriter(idxPath, 128, 0, 100, log.New())
	require.NoError(err)
	require.Equal((128/8*100/os.Getpagesize()+1)*os.Getpagesize(), bm2.size)
	defer bm2.Close()
	bm2.Close()

	bm3, err := NewFixedSizeBitmapsWriter(idxPath, 128, 0, 1000, log.New())
	require.NoError(err)
	require.Equal((128/8*1000/os.Getpagesize()+1)*os.Getpagesize(), bm3.size)
	defer bm3.Close()
}
