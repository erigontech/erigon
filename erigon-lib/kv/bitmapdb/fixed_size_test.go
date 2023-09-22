/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bitmapdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestFixedSizeBitmaps(t *testing.T) {

	tmpDir, require := t.TempDir(), require.New(t)
	must := require.NoError
	idxPath := filepath.Join(tmpDir, "idx.tmp")
	wr, err := NewFixedSizeBitmapsWriter(idxPath, 14, 7, log.New())
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

	bm, err := OpenFixedSizeBitmaps(idxPath, 14)
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
	require.Equal(true, ok)
	require.Equal(false, ok2)

	fst, snd, ok, ok2, err = bm.First2At(2, 8)
	require.NoError(err)
	require.Equal(uint64(8), fst)
	require.Equal(uint64(13), snd)
	require.Equal(true, ok)
	require.Equal(true, ok2)

	fst, snd, ok, ok2, err = bm.First2At(2, 9)
	require.NoError(err)
	require.Equal(uint64(13), fst)
	require.Equal(uint64(0), snd)
	require.Equal(true, ok)
	require.Equal(false, ok2)

	_, err = bm.At(8)
	require.Error(err)
}

func TestPageAlined(t *testing.T) {
	tmpDir, require := t.TempDir(), require.New(t)
	idxPath := filepath.Join(tmpDir, "idx.tmp")

	bm2, err := NewFixedSizeBitmapsWriter(idxPath, 128, 100, log.New())
	require.NoError(err)
	require.Equal((128/8*100/os.Getpagesize()+1)*os.Getpagesize(), bm2.size)
	defer bm2.Close()
	bm2.Close()

	bm3, err := NewFixedSizeBitmapsWriter(idxPath, 128, 1000, log.New())
	require.NoError(err)
	require.Equal((128/8*1000/os.Getpagesize()+1)*os.Getpagesize(), bm3.size)
	defer bm3.Close()
}
