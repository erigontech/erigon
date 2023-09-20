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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestReWriteIndex(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = rs.AddKey([]byte(fmt.Sprintf("key %d", i)), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rs.Build(context.Background()); err != nil {
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
