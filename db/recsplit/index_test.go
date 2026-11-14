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
	"fmt"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestExtractOffsets(t *testing.T) {
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
	for i := range 100 {
		if err = rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rs.Build(t.Context()); err != nil {
		t.Fatal(err)
	}
	idx := MustOpen(indexFile)
	defer idx.Close()
	offsets := idx.ExtractOffsets()
	require.Len(t, offsets, 100)
	for i := range 100 {
		_, ok := offsets[uint64(i*17)]
		require.True(t, ok)
	}
}

func TestForceHashInRAM(t *testing.T) {
	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index")
	salt := uint32(1)
	const keys = 1000
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   keys,
		BucketSize: 100,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  indexFile,
		LeafSize:   8,
	}, log.New())
	require.NoError(t, err)
	defer rs.Close()
	for i := range keys {
		require.NoError(t, rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i*17)))
	}
	require.NoError(t, rs.Build(t.Context()))

	idx := MustOpen(indexFile)
	defer idx.Close()
	require.Positive(t, idx.ForceHashInRAM())

	mapStart := uintptr(unsafe.Pointer(&idx.mmapHandle1[0]))
	mapEnd := mapStart + uintptr(len(idx.mmapHandle1))
	gr := uintptr(unsafe.Pointer(&idx.grData[0]))
	require.False(t, gr >= mapStart && gr < mapEnd, "Golomb-Rice codes still read from the mapped file")

	reader := NewIndexReader(idx)
	for i := range keys {
		offset, ok := reader.Lookup(fmt.Appendf(nil, "key %d", i))
		require.True(t, ok)
		require.Equal(t, uint64(i*17), offset)
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
		assert.Equal(t, No|Enums, features) // no side-effects
	})
	t.Run("disallow_unknown", func(t *testing.T) {
		features := Features(0xff)
		err := onlyKnownFeatures(features)
		assert.ErrorIs(t, err, ErrIncompatible)
	})
}
