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

package v4

import (
	"bytes"
	"testing"
)

func TestKeySetForEachMissing(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after []string
		want          []string
	}{
		{"empty", nil, nil, nil},
		{"all missing", []string{"b", "a"}, nil, []string{"a", "b"}},
		{"none missing", []string{"a", "b"}, []string{"b", "a", "c"}, nil},
		{"duplicates collapse", []string{"a", "a", "b"}, []string{"b"}, []string{"a"}},
		{"after exhausted first", []string{"a", "c", "e"}, []string{"a"}, []string{"c", "e"}},
		{"before exhausted first", []string{"c"}, []string{"a", "b", "c", "d"}, nil},
		{"interleaved", []string{"a", "c", "e"}, []string{"b", "c", "d"}, []string{"a", "e"}},
		{"shared prefix", []string{"ab", "abc"}, []string{"abc"}, []string{"ab"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, after := new(keySet), new(keySet)
			for _, k := range tc.before {
				before.add([]byte(k))
			}
			for _, k := range tc.after {
				after.add([]byte(k))
			}
			var got []string
			if err := before.forEachMissing(after, func(key []byte) error {
				got = append(got, string(key))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %q want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %q want %q", got, tc.want)
				}
			}
		})
	}
}

func TestKeySetAddNodeKeyMatchesNodeKey(t *testing.T) {
	g := storageGraph(bytes.Repeat([]byte{0x7e}, 32))
	s := new(keySet)
	paths := [][]byte{nil, {0x01}, {0x0a, 0x0b, 0x0c}}
	for _, p := range paths {
		s.addNodeKey(g, p)
	}
	for i, p := range paths {
		if want := g.nodeKey(p, nil); !bytes.Equal(s.at(i), want) {
			t.Fatalf("path %x: got %x want %x", p, s.at(i), want)
		}
	}
}
