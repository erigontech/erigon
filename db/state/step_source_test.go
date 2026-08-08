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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// staticStepSource is an in-memory stepSource for testing merges
// without any file/MDBX plumbing.
type staticStepSource struct {
	pairs [][2][]byte
	idx   int
}

func newStaticStepSource(pairs [][2][]byte) *staticStepSource {
	return &staticStepSource{pairs: pairs, idx: 0}
}

func (s *staticStepSource) Current() (key, value []byte, ok bool) {
	if s.idx >= len(s.pairs) {
		return nil, nil, false
	}
	return s.pairs[s.idx][0], s.pairs[s.idx][1], true
}

func (s *staticStepSource) Advance() error {
	if s.idx < len(s.pairs) {
		s.idx++
	}
	return nil
}

func (s *staticStepSource) Close() {}

// TestMergedStepSources_PriorityOnDuplicate is the load-bearing
// property behind merge-on-retire: MDBX (source[0]) wins over a v4
// boundary file (source[1]) whenever both hold the same key. The
// output is sorted-by-key with duplicates collapsed to the priority
// winner's value.
func TestMergedStepSources_PriorityOnDuplicate(t *testing.T) {
	t.Parallel()

	// source[0] simulates MDBX (post-target writes) — priority winner.
	mdbx := newStaticStepSource([][2][]byte{
		{[]byte("aaa"), []byte("mdbx-aaa")},
		{[]byte("ccc"), []byte("mdbx-ccc")},
		{[]byte("eee"), []byte("mdbx-eee")},
	})
	// source[1] simulates v4 boundary file (pre-target snapshot).
	// Overlaps on "ccc" — mdbx must win. Contributes unique "bbb"
	// and "ddd" that MDBX doesn't have.
	v4 := newStaticStepSource([][2][]byte{
		{[]byte("aaa"), []byte("v4-aaa-loses")},
		{[]byte("bbb"), []byte("v4-bbb")},
		{[]byte("ccc"), []byte("v4-ccc-loses")},
		{[]byte("ddd"), []byte("v4-ddd")},
	})

	merged := newMergedStepSources([]stepSource{mdbx, v4})

	type kv struct{ k, v string }
	var got []kv
	for {
		k, v, ok, err := merged.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kv{string(k), string(v)})
	}
	require.Equal(t, []kv{
		{"aaa", "mdbx-aaa"}, // duplicate → MDBX wins
		{"bbb", "v4-bbb"},   // v4-only
		{"ccc", "mdbx-ccc"}, // duplicate → MDBX wins
		{"ddd", "v4-ddd"},   // v4-only
		{"eee", "mdbx-eee"}, // MDBX-only
	}, got, "merged output must be sorted by key with MDBX (source[0]) winning duplicates")
}

// TestMergedStepSources_EmptySources: no sources, or all sources
// empty, yields no output.
func TestMergedStepSources_EmptySources(t *testing.T) {
	t.Parallel()

	empty := newMergedStepSources(nil)
	_, _, ok, err := empty.Next()
	require.NoError(t, err)
	require.False(t, ok)

	oneEmpty := newMergedStepSources([]stepSource{newStaticStepSource(nil)})
	_, _, ok, err = oneEmpty.Next()
	require.NoError(t, err)
	require.False(t, ok)
}

// TestMergedStepSources_SingleSource — with one source, output
// matches its stream verbatim.
func TestMergedStepSources_SingleSource(t *testing.T) {
	t.Parallel()

	src := newStaticStepSource([][2][]byte{
		{[]byte("k1"), []byte("v1")},
		{[]byte("k2"), []byte("v2")},
	})
	merged := newMergedStepSources([]stepSource{src})
	k, v, ok, err := merged.Next()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "k1", string(k))
	require.Equal(t, "v1", string(v))
	k, v, ok, err = merged.Next()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "k2", string(k))
	require.Equal(t, "v2", string(v))
	_, _, ok, err = merged.Next()
	require.NoError(t, err)
	require.False(t, ok)
}

// TestMergedStepSources_ThreeSourcesPriority extends the two-source
// case: earlier index wins over both later indices; latest source
// contributes only its unique keys.
func TestMergedStepSources_ThreeSourcesPriority(t *testing.T) {
	t.Parallel()

	first := newStaticStepSource([][2][]byte{
		{[]byte("a"), []byte("first-a")},
	})
	second := newStaticStepSource([][2][]byte{
		{[]byte("a"), []byte("second-a-loses")},
		{[]byte("b"), []byte("second-b")},
	})
	third := newStaticStepSource([][2][]byte{
		{[]byte("a"), []byte("third-a-loses")},
		{[]byte("b"), []byte("third-b-loses")},
		{[]byte("c"), []byte("third-c")},
	})

	merged := newMergedStepSources([]stepSource{first, second, third})
	var got [][2]string
	for {
		k, v, ok, err := merged.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, [2]string{string(k), string(v)})
	}
	require.Equal(t, [][2]string{
		{"a", "first-a"},
		{"b", "second-b"},
		{"c", "third-c"},
	}, got)
}
