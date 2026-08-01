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

package patricia

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// TestEdgeLookup pins swarEdge and bsearchEdge against a plain scan for every
// byte and every fanout, including the boundaries the word-at-a-time load can
// get wrong: a run ending mid-word, a hit inside the tail padding, and a hit
// that belongs to the next state's labels.
func TestEdgeLookup(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for fanout := 2; fanout <= 40; fanout++ {
		// three runs back to back so lookups in the first two can bleed into
		// their neighbour, and the last one sits against the padding
		var labels []byte
		var children []int32
		bounds := make([][2]int32, 0, 3)
		for range 3 {
			perm := r.Perm(256)[:fanout]
			ls := make([]byte, fanout)
			for i, v := range perm {
				ls[i] = byte(v)
			}
			slices.Sort(ls)
			start := int32(len(labels))
			for _, l := range ls {
				labels = append(labels, l)
				children = append(children, int32(len(children))+1000)
			}
			bounds = append(bounds, [2]int32{start, int32(len(labels))})
		}
		labels = append(labels, make([]byte, swarPad)...)

		for run, lohi := range bounds {
			lo, hi := lohi[0], lohi[1]
			for b := range 256 {
				want := int32(-1)
				for i := lo; i < hi; i++ {
					if labels[i] == byte(b) {
						want = children[i]
						break
					}
				}
				name := fmt.Sprintf("fanout=%d run=%d b=%d", fanout, run, b)
				if got := swarEdge(labels, children, lo, hi, byte(b)); got != want {
					t.Fatalf("swarEdge %s: got %d, want %d", name, got, want)
				}
				if got := bsearchEdge(labels, children, lo, hi, byte(b)); got != want {
					t.Fatalf("bsearchEdge %s: got %d, want %d", name, got, want)
				}
			}
		}
	}
}

// TestWideFanout drives the automaton itself across the wideBsearchMin
// threshold, so both edge-lookup strategies are reached through the scan loop.
func TestWideFanout(t *testing.T) {
	for _, fanout := range []int{2, 8, 16, wideBsearchMin, wideBsearchMin + 1, 200, 256} {
		t.Run(fmt.Sprintf("fanout=%d", fanout), func(t *testing.T) {
			ac := NewAhoCorasick()
			// "a" + <one of fanout distinct bytes> + "z" gives a depth-1 state
			// with exactly `fanout` edges
			for i := range fanout {
				ac.Insert([]byte{'a', byte(i), 'z'}, i)
			}
			m := NewACMatcher(ac)
			for i := range fanout {
				data := []byte{'x', 'a', byte(i), 'z', 'x'}
				got := m.FindLongestMatches(data)
				if len(got) != 1 {
					t.Fatalf("byte %d: got %d matches, want 1", i, len(got))
				}
				if got[0].Start != 1 || got[0].End != 4 || got[0].Val.(int) != i {
					t.Fatalf("byte %d: got %+v", i, got[0])
				}
			}
			// a byte with no edge must not match
			if fanout < 256 {
				if got := m.FindLongestMatches([]byte{'a', byte(fanout), 'z'}); len(got) != 0 {
					t.Fatalf("unexpected match %+v", got)
				}
			}
		})
	}
}
