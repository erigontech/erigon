/*
   Copyright 2022 Erigon contributors

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

package recsplit

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReWriteIndex(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,
		StartSeed: []uint64{0x106393c187cae21a, 0x6453cec3f7376937, 0x643e521ddbd2be98, 0x3740c6412f6572cb, 0x717d47562f1ce470, 0x4cd6eb4c63befb7c, 0x9bfd8c5e18c8da73,
			0x082f20e10092a9a3, 0x2ada2ce68d21defc, 0xe33cb4f3e7c6466b, 0x3980be458c509c59, 0xc466fd9584828e8c, 0x45f0aabe1a61ede6, 0xf6e7b8b33ad9b98d,
			0x4ef95e25f4b4983d, 0x81175195173b92d3, 0x4e50927d8dd15978, 0x1ea2099d1fafae7f, 0x425c8a06fbaaa815, 0xcd4216006c74052a},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = rs.AddKey([]byte(fmt.Sprintf("key %d", i)), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rs.Build(); err != nil {
		t.Fatal(err)
	}
	idx := MustOpen(indexFile)
	defer idx.Close()
	offsets := idx.ExtractOffsets()
	for i := 0; i < 100; i++ {
		_, ok := offsets[uint64(i*17)]
		require.True(t, ok)
		offsets[uint64(i*17)] = uint64(i * 3965)
	}
	reindexFile := filepath.Join(tmpDir, "reindex")
	f, err := os.Create(reindexFile)
	require.NoError(t, err)
	defer f.Close()
	w := bufio.NewWriter(f)
	err = idx.RewriteWithOffsets(w, offsets)
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
	reidx := MustOpen(reindexFile)
	defer reidx.Close()
	for i := 0; i < 100; i++ {
		reader := NewIndexReader(reidx)
		offset := reader.Lookup([]byte(fmt.Sprintf("key %d", i)))
		if offset != uint64(i*3965) {
			t.Errorf("expected offset: %d, looked up: %d", i*3965, offset)
		}
	}
}
