// Copyright 2021 The Erigon Authors
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
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
)

// go test -trimpath -v -fuzz=FuzzLongestMatch -fuzztime=10s ./patricia

type oracleNode struct {
	next  map[byte]*oracleNode
	isKey bool
}

func newOracleNode() *oracleNode { return &oracleNode{next: make(map[byte]*oracleNode)} }

func FuzzLongestMatch(f *testing.F) {
	f.Fuzz(func(t *testing.T, build []byte, test []byte) {
		keyMap := make(map[string][]byte)
		i := 0
		for i < len(build) {
			keyLen := int(build[i]>>4) + 1
			valLen := int(build[i]&15) + 1
			i++
			var key []byte
			var val []byte
			for keyLen > 0 && i < len(build) {
				key = append(key, build[i])
				i++
				keyLen--
			}
			for valLen > 0 && i < len(build) {
				val = append(val, build[i])
				i++
				valLen--
			}
			keyMap[string(key)] = val
		}
		var keys []string
		for key := range keyMap {
			keys = append(keys, key)
		}
		if len(keys) == 0 {
			return
		}
		// keys indexes the generated match data below, so map iteration order
		// would make the same fuzz input build a different test string each run.
		slices.Sort(keys)
		var data []byte
		for i := 0; i < 4*(len(test)/4); i += 4 {
			keyIdx := int(binary.BigEndian.Uint32(test[i : i+4]))
			keyIdx %= len(keys)
			key := []byte(keys[keyIdx])
			data = append(data, key...)
			for j := range key {
				data = append(data, key[len(key)-1-j])
			}
		}
		// Validate AC against an independent trie oracle; the walk is
		// O(len(data)*maxKeyLen) so the fuzzer can't drive it to a timeout.
		oracleRoot := newOracleNode()
		for key := range keyMap {
			nd := oracleRoot
			for j := 0; j < len(key); j++ {
				c := nd.next[key[j]]
				if c == nil {
					c = newOracleNode()
					nd.next[key[j]] = c
				}
				nd = c
			}
			nd.isKey = true
		}
		var oracle Matches
		lastEnd := 0
		for s := 0; s < len(data); s++ {
			best := 0
			nd := oracleRoot
			for d := 0; s+d < len(data); d++ {
				c := nd.next[data[s+d]]
				if c == nil {
					break
				}
				nd = c
				if nd.isKey {
					best = d + 1
				}
			}
			if best > 0 && s+best > lastEnd {
				oracle = append(oracle, Match{Start: s, End: s + best})
				lastEnd = s + best
			}
		}
		ac := NewAhoCorasick()
		for key, val := range keyMap {
			ac.Insert([]byte(key), val)
		}
		sameMatches := func(what string, got, want Matches) {
			t.Helper()
			if len(got) != len(want) {
				t.Errorf("AC warm/fresh %s: %d matches, want %d", what, len(got), len(want))
				return
			}
			for i := range got {
				if got[i].Start != want[i].Start || got[i].End != want[i].End {
					t.Errorf("AC warm/fresh %s at %d: %+v, want %+v", what, i, got[i], want[i])
				}
			}
		}
		// exercise the prefix-resume path: a reused matcher must give the same
		// result as a fresh one, for words that grow and that shrink
		acm := NewACMatcher(ac)
		if len(data) > 1 {
			half := data[:len(data)/2]
			sameMatches("half word", acm.FindLongestMatches(half), NewACMatcher(ac).FindLongestMatches(half))
		}
		m4 := slices.Clone(acm.FindLongestMatches(data))
		sameMatches("full word", m4, NewACMatcher(ac).FindLongestMatches(data))
		if len(data) > 2 {
			// shorter than the cached word: states is re-sliced down, and every
			// match comes from carried-over states with no fresh scan at all
			third := data[:len(data)/3]
			sameMatches("shrunk word", acm.FindLongestMatches(third), NewACMatcher(ac).FindLongestMatches(third))
		}
		if len(oracle) == len(m4) {
			for i, m := range oracle {
				mm := m4[i]
				if m.Start != mm.Start || m.End != mm.End {
					t.Errorf("AC mismatch, expected %+v, got %+v", m, mm)
				}
			}
		} else {
			t.Errorf("AC matches %d, expected %d", len(m4), len(oracle))
			for _, m := range oracle {
				fmt.Printf("%+v, oracle: [%x]\n", m, data[m.Start:m.End])
			}
			for _, m := range m4 {
				fmt.Printf("%+v, match4: [%x]\n", m, data[m.Start:m.End])
			}
		}
	})
}
