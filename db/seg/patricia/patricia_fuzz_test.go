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
	"testing"
)

// go test -trimpath -v -fuzz=FuzzLongestMatch -fuzztime=10s ./patricia

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
		var data []byte
		for i := 0; i < 4*(len(test)/4); i += 4 {
			keyIdx := int(binary.BigEndian.Uint32(test[i : i+4]))
			keyIdx = keyIdx % len(keys)
			key := []byte(keys[keyIdx])
			data = append(data, key...)
			for j := 0; j < len(key); j++ {
				data = append(data, key[len(key)-1-j])
			}
		}
		// Brute-force oracle: longest dictionary key at each position, then drop
		// matches subsumed by an earlier, further-reaching one (the maximal set).
		var oracle Matches
		lastEnd := 0
		for s := 0; s < len(data); s++ {
			best := 0
			for key := range keyMap {
				kl := len(key)
				if kl > best && s+kl <= len(data) && string(data[s:s+kl]) == key {
					best = kl
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
		acm := NewACMatcher(ac)
		// exercise the prefix-resume path: a reused matcher must give the same
		// result as a fresh one, for the same word and for suffix/other words
		if len(data) > 1 {
			pre := acm.FindLongestMatches(data[:len(data)/2])
			fresh := NewACMatcher(ac).FindLongestMatches(data[:len(data)/2])
			if len(pre) != len(fresh) {
				t.Errorf("AC warm/fresh mismatch on half word: %d vs %d", len(pre), len(fresh))
			}
		}
		m4 := acm.FindLongestMatches(data)
		m4again := NewACMatcher(ac).FindLongestMatches(data)
		if len(m4) != len(m4again) {
			t.Errorf("AC warm/fresh mismatch: %d vs %d", len(m4), len(m4again))
		} else {
			for i := range m4 {
				if m4[i].Start != m4again[i].Start || m4[i].End != m4again[i].End {
					t.Errorf("AC warm/fresh mismatch at %d: %+v vs %+v", i, m4[i], m4again[i])
				}
			}
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
