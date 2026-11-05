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

package stream_test

import (
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// seekArr is a sorted array stream that can seek, counting the elements it returns.
type seekArr struct {
	arr   []uint64
	i     int
	asc   bool
	nexts *int
}

func (s *seekArr) HasNext() bool { return s.i < len(s.arr) }
func (s *seekArr) Next() (uint64, error) {
	*s.nexts++
	v := s.arr[s.i]
	s.i++
	return v, nil
}
func (s *seekArr) Close()        {}
func (s *seekArr) CanSeek() bool { return true }
func (s *seekArr) Seek(v uint64) error {
	for s.i < len(s.arr) && ((s.asc && s.arr[s.i] < v) || (!s.asc && s.arr[s.i] > v)) {
		s.i++ // a real seeker searches; the elements skipped are not counted
	}
	return nil
}

func randomSet(rng *rand.Rand, n, max int, asc bool) []uint64 {
	set := map[uint64]bool{}
	for range n {
		set[uint64(rng.Intn(max))] = true
	}
	out := make([]uint64, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	slices.Sort(out)
	if !asc {
		slices.Reverse(out)
	}
	return out
}

func intersect(a, b []uint64) []uint64 {
	var out []uint64
	for _, v := range a {
		if slices.Contains(b, v) {
			out = append(out, v)
		}
	}
	return out
}

func union(a, b []uint64, asc bool) []uint64 {
	out := slices.Clone(a)
	for _, v := range b {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	if !asc {
		slices.Reverse(out)
	}
	return out
}

func TestIntersectWithSeekMatchesTheSetIntersection(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, asc := range []bool{true, false} {
		for range 500 {
			big, small, other := randomSet(rng, 300, 1000, asc), randomSet(rng, rng.Intn(10), 1000, asc), randomSet(rng, 200, 1000, asc)
			nexts := 0
			mk := func(arr []uint64, seek bool) stream.Uno[uint64] {
				if seek {
					return &seekArr{arr: arr, asc: asc, nexts: &nexts}
				}
				return stream.Array(arr) // the arrays are already in the stream order
			}
			// (big ∪ other) ∩ small, then ∩ big again: unions and nested intersections seek through
			it := stream.Intersect[uint64](stream.Intersect[uint64](stream.Union[uint64](mk(big, true), mk(other, false), order.By(asc), -1), mk(small, true), order.By(asc), -1), mk(big, true), order.By(asc), -1)
			got, err := stream.ToArrayU64(it)
			require.NoError(t, err)
			want := intersect(intersect(union(big, other, asc), small), big)
			if len(want) == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, want, got)
			}
		}
	}
}

func TestIntersectSeeksPastTheLargeStream(t *testing.T) {
	large := make([]uint64, 100_000)
	for i := range large {
		large[i] = uint64(i)
	}
	nexts := 0
	it := stream.Intersect[uint64](&seekArr{arr: large, asc: true, nexts: &nexts}, stream.Array([]uint64{10, 50_000, 99_999}), order.Asc, -1)
	got, err := stream.ToArrayU64(it)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 50_000, 99_999}, got)
	require.Less(t, nexts, 40, "the large stream should be skipped, not iterated")
}

func seekRange(n uint64) *seekArr {
	arr := make([]uint64, n)
	for i := range arr {
		arr[i] = uint64(i)
	}
	return &seekArr{arr: arr, asc: true, nexts: new(int)}
}

func TestIntersectDoesNotSeekPastTheUnionLimit(t *testing.T) {
	limited := stream.Union[uint64](seekRange(12), stream.Array([]uint64{100}), order.Asc, 3) // [0,1,2]
	got, err := stream.ToArrayU64(stream.Intersect[uint64](limited, stream.Array([]uint64{10}), order.Asc, -1))
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestIntersectDoesNotSeekPastTheNestedIntersectLimit(t *testing.T) {
	limited := stream.Intersect[uint64](seekRange(12), seekRange(12), order.Asc, 3) // [0,1,2]
	got, err := stream.ToArrayU64(stream.Intersect[uint64](limited, stream.Array([]uint64{10}), order.Asc, -1))
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestIntersectNaN(t *testing.T) {
	nan := math.NaN()
	tests := []struct {
		name string
		x, y []float64
		asc  order.By
		want []float64
	}{
		{"x only NaN", []float64{nan}, []float64{1}, order.Asc, nil},
		{"y only NaN", []float64{1}, []float64{nan}, order.Asc, nil},
		{"x asc", []float64{nan, 1, 2}, []float64{2}, order.Asc, []float64{2}},
		{"y asc", []float64{2}, []float64{nan, 1, 2}, order.Asc, []float64{2}},
		{"x desc", []float64{nan, 2, 1}, []float64{1}, order.Desc, []float64{1}},
		{"y desc", []float64{1}, []float64{nan, 2, 1}, order.Desc, []float64{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stream.ToArray(stream.Intersect[float64](stream.Array(tt.x), stream.Array(tt.y), tt.asc, -1))
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
