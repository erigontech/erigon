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

package recsplit

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func buildSharded(t *testing.T, cfg RecSplitArgs, n int, offsetOf func(i int) uint64) *ShardedIndex {
	t.Helper()
	logger := log.New()
	rs, err := NewShardedRecSplit(cfg, logger)
	require.NoError(t, err)
	defer rs.Close()
	for i := 0; i < n; i++ {
		require.NoError(t, rs.AddKey(fmt.Appendf(nil, "key %d", i), offsetOf(i)))
	}
	require.NoError(t, rs.Build(t.Context()))
	idx, err := OpenShardedIndex(cfg.IndexFile)
	require.NoError(t, err)
	return idx
}

func TestShardedIndexLookupNoEnums(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	const n = 20000
	offsetOf := func(i int) uint64 { return uint64(i * 17) }
	idx := buildSharded(t, RecSplitArgs{
		KeyCount:           n,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          filepath.Join(tmpDir, "index"),
		Enums:              false,
		LessFalsePositives: true,
	}, n, offsetOf)
	defer idx.Close()

	require.EqualValues(t, n, idx.KeyCount())
	require.False(t, idx.Enums())

	r := idx.Reader()
	for i := 0; i < n; i++ {
		offset, ok := r.Lookup(fmt.Appendf(nil, "key %d", i))
		require.Truef(t, ok, "key %d not found", i)
		require.Equalf(t, offsetOf(i), offset, "key %d", i)
	}
}

func TestShardedIndexTwoLayerLookupEnums(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	const n = 20000
	offsetOf := func(i int) uint64 { return uint64(i * 17) } // monotonic, required by enums
	idx := buildSharded(t, RecSplitArgs{
		KeyCount:           n,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          filepath.Join(tmpDir, "index"),
		Enums:              true,
		LessFalsePositives: true,
		BaseDataID:         42,
	}, n, offsetOf)
	defer idx.Close()

	require.EqualValues(t, n, idx.KeyCount())
	require.True(t, idx.Enums())
	require.EqualValues(t, 42, idx.BaseDataID())

	r := idx.Reader()
	for i := 0; i < n; i++ {
		offset, ok := r.TwoLayerLookup(fmt.Appendf(nil, "key %d", i))
		require.Truef(t, ok, "key %d not found", i)
		require.Equalf(t, offsetOf(i), offset, "key %d", i)
	}
}

func TestShardedIndexOrdinalLookup(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	const n = 20000
	offsetOf := func(i int) uint64 { return uint64(i*17 + 3) } // monotonic
	idx := buildSharded(t, RecSplitArgs{
		KeyCount:           n,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          filepath.Join(tmpDir, "index"),
		Enums:              true,
		LessFalsePositives: true,
	}, n, offsetOf)
	defer idx.Close()

	r := idx.Reader()
	prev := uint64(0)
	for i := 0; i < n; i++ {
		got := r.OrdinalLookup(uint64(i))
		require.Equalf(t, offsetOf(i), got, "OrdinalLookup(%d)", i)
		if i > 0 {
			require.GreaterOrEqualf(t, got, prev, "OrdinalLookup must be monotone at %d", i)
		}
		prev = got
	}
}

func TestShardedIndexAbsentKeysFiltered(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	const n = 20000
	idx := buildSharded(t, RecSplitArgs{
		KeyCount:           n,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          filepath.Join(tmpDir, "index"),
		Enums:              false,
		LessFalsePositives: true,
	}, n, func(i int) uint64 { return uint64(i * 17) })
	defer idx.Close()

	r := idx.Reader()
	falsePositives := 0
	const probes = 20000
	for i := n; i < n+probes; i++ {
		if _, ok := r.Lookup(fmt.Appendf(nil, "key %d", i)); ok {
			falsePositives++
		}
	}
	// FuseFilter is ~0.4% FP; allow generous slack for the test.
	require.Lessf(t, falsePositives, probes/20, "too many false positives: %d/%d", falsePositives, probes)
}

func TestShardedIndexSmallNonEnum(t *testing.T) {
	// Small key count → many shards end up with exactly one key, exercising the
	// Index.Lookup keyCount==1 path which must still return the real offset.
	tmpDir := t.TempDir()
	salt := uint32(1)
	const n = 300
	offsetOf := func(i int) uint64 { return uint64(i*31 + 7) }
	idx := buildSharded(t, RecSplitArgs{
		KeyCount:           n,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          filepath.Join(tmpDir, "index"),
		Enums:              false,
		LessFalsePositives: true,
	}, n, offsetOf)
	defer idx.Close()

	r := idx.Reader()
	for i := 0; i < n; i++ {
		offset, ok := r.Lookup(fmt.Appendf(nil, "key %d", i))
		require.Truef(t, ok, "key %d not found", i)
		require.Equalf(t, offsetOf(i), offset, "key %d", i)
	}
}

func TestShardedIndexSingleKey(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	idx := buildSharded(t, RecSplitArgs{
		KeyCount:           1,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          filepath.Join(tmpDir, "index"),
		Enums:              true,
		LessFalsePositives: true,
	}, 1, func(i int) uint64 { return 777 })
	defer idx.Close()

	require.EqualValues(t, 1, idx.KeyCount())
	offset, ok := idx.Reader().TwoLayerLookup([]byte("key 0"))
	require.True(t, ok)
	require.EqualValues(t, 777, offset)
}

func TestShardedRecSplitKeyCountMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	rs, err := NewShardedRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		LeafSize:   8,
		Salt:       &salt,
		TmpDir:     tmpDir,
		IndexFile:  filepath.Join(tmpDir, "index"),
	}, log.New())
	require.NoError(t, err)
	defer rs.Close()
	require.NoError(t, rs.AddKey([]byte("only_key"), 0))
	require.Error(t, rs.Build(t.Context()))
}

func TestShardedRecSplitResetNextSalt(t *testing.T) {
	tmpDir := t.TempDir()
	salt := uint32(1)
	indexFile := filepath.Join(tmpDir, "index")
	const n = 5000
	rs, err := NewShardedRecSplit(RecSplitArgs{
		KeyCount:           n,
		BucketSize:         10,
		LeafSize:           8,
		Salt:               &salt,
		TmpDir:             tmpDir,
		IndexFile:          indexFile,
		LessFalsePositives: true,
	}, log.New())
	require.NoError(t, err)
	defer rs.Close()

	add := func() {
		for i := 0; i < n; i++ {
			require.NoError(t, rs.AddKey(fmt.Appendf(nil, "key %d", i), uint64(i)))
		}
	}
	add()
	saltBefore := rs.Salt()
	rs.ResetNextSalt()
	require.Equal(t, saltBefore+1, rs.Salt())
	add() // re-add after reset, as the collision-retry loop would
	require.NoError(t, rs.Build(t.Context()))

	idx, err := OpenShardedIndex(indexFile)
	require.NoError(t, err)
	defer idx.Close()
	require.Equal(t, rs.Salt(), idx.Salt())
	r := idx.Reader()
	for i := 0; i < n; i++ {
		offset, ok := r.Lookup(fmt.Appendf(nil, "key %d", i))
		require.Truef(t, ok, "key %d", i)
		require.EqualValues(t, uint64(i), offset)
	}
}

// go test -trimpath -v -fuzz=FuzzShardedRecSplit -fuzztime=10s ./db/recsplit
func FuzzShardedRecSplit(f *testing.F) {
	logger := log.New()
	f.Add(2, []byte("1stkey2ndkey"))
	f.Fuzz(func(t *testing.T, count int, in []byte) {
		if count < 1 || len(in) < count {
			t.Skip()
		}
		l := (len(in) + count - 1) / count
		keys := make([][]byte, 0, count)
		dups := make(map[string]struct{})
		var i int
		for i = 0; i < len(in)-l; i += l {
			keys = append(keys, in[i:i+l])
			dups[string(in[i:i+l])] = struct{}{}
		}
		keys = append(keys, in[i:])
		dups[string(in[i:])] = struct{}{}
		if len(dups) != count || len(keys) != count {
			t.Skip()
		}

		tmpDir := t.TempDir()
		indexFile := filepath.Join(tmpDir, "index")
		salt := uint32(1)
		rs, err := NewShardedRecSplit(RecSplitArgs{
			KeyCount:           count,
			Enums:              true,
			LessFalsePositives: true,
			BucketSize:         10,
			LeafSize:           8,
			Salt:               &salt,
			TmpDir:             tmpDir,
			IndexFile:          indexFile,
		}, logger)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		for off, k := range keys {
			if err := rs.AddKey(k, uint64(off)); err != nil {
				t.Fatal(err)
			}
		}
		if err = rs.Build(t.Context()); err != nil {
			t.Fatal(err)
		}

		idx, err := OpenShardedIndex(indexFile)
		if err != nil {
			t.Fatal(err)
		}
		defer idx.Close()
		r := idx.Reader()
		for off, k := range keys {
			got, ok := r.TwoLayerLookup(k)
			if !ok {
				t.Fatalf("key %d not found", off)
			}
			if got != uint64(off) {
				t.Fatalf("key %d: got offset %d, want %d", off, got, off)
			}
		}
	})
}
