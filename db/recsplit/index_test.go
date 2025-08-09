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

package recsplit

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
)

func TestReWriteIndex(t *testing.T) {
	logger := log.New()
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	salt := uint32(1)
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   100,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
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
		offset, _ := reader.Lookup([]byte(fmt.Sprintf("key %d", i)))
		if offset != uint64(i*3965) {
			t.Errorf("expected offset: %d, looked up: %d", i*3965, offset)
		}
	}
}

func TestForwardCompatibility(t *testing.T) {
	t.Run("features_are_optional", func(t *testing.T) {
		var features Features
		err := onlyKnownFeatures(features)
		require.NoError(t, err)
	})
	t.Run("allow_known", func(t *testing.T) {
		features := No | Enums
		err := onlyKnownFeatures(features)
		require.NoError(t, err)
		assert.Equal(t, No|Enums, features) //no side-effects
	})
	t.Run("disallow_unknown", func(t *testing.T) {
		features := Features(0xff)
		err := onlyKnownFeatures(features)
		assert.ErrorIs(t, err, IncompatibleErr)
	})
}
