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
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// go test -trimpath -v -fuzz=FuzzPatricia -fuzztime=10s ./patricia

func FuzzPatricia(f *testing.F) {
	f.Fuzz(func(t *testing.T, build []byte, test []byte) {
		var n node
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
			n.insert(key, val)
			keyMap[string(key)] = val
		}
		var testKeys [][]byte
		i = 0
		for i < len(test) {
			keyLen := int(test[i]>>4) + 1
			i++
			var key []byte
			for keyLen > 0 && i < len(test) {
				key = append(key, test[i])
				i++
				keyLen--
			}
			if _, ok := keyMap[string(key)]; !ok {
				testKeys = append(testKeys, key)
			}
		}
		// Test for keys
		for key, vals := range keyMap {
			v, ok := n.get([]byte(key))
			if ok {
				if !bytes.Equal(vals, v.([]byte)) {
					t.Errorf("for key %x expected value %x, got %x", key, vals, v.([]byte))
				}
			}
		}
		// Test for non-existent keys
		for _, key := range testKeys {
			_, ok := n.get(key)
			if ok {
				t.Errorf("unexpected key found [%x]", key)
			}
		}
	})
}

func FuzzLongestMatch(f *testing.F) {
	f.Fuzz(func(t *testing.T, build []byte, test []byte) {
		var pt PatriciaTree
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
			pt.Insert(key, val)
			keyMap[string(key)] = val
		}
		var keys []string
		for key := range keyMap {
			keys = append(keys, key)
		}
		if len(keys) == 0 {
			t.Skip()
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
		mf := NewMatchFinder(&pt)
		m1 := mf.FindLongestMatches(data)
		mf2 := NewMatchFinder2(&pt)
		m2 := mf2.FindLongestMatches(data)
		ft := pt.Flatten()
		mf3 := NewMatchFinder3(ft)
		m3 := mf3.FindLongestMatches(data)
		if len(m1) == len(m2) {
			for i, m := range m1 {
				mm := m2[i]
				if m.Start != mm.Start || m.End != mm.End {
					t.Errorf("mismatch, expected %+v, got %+v", m, mm)
				}
			}
		} else {
			t.Errorf("matches %d, expected %d", len(m2), len(m1))
			for _, m := range m1 {
				fmt.Printf("%+v, match1: [%x]\n", m, data[m.Start:m.End])
			}
			for _, m := range m2 {
				fmt.Printf("%+v, match2: [%x]\n", m, data[m.Start:m.End])
			}
		}
		// AC is validated against a brute-force oracle instead of MatchFinder:
		// patricia.Insert loses an existing key when inserting its proper
		// prefix, so MF1/MF2/MF3 under-report matches on prefix-nested dicts.
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
		if len(m1) == len(m3) {
			for i, m := range m1 {
				mm := m3[i]
				if m.Start != mm.Start || m.End != mm.End {
					t.Errorf("MF3 mismatch, expected %+v, got %+v", m, mm)
				}
			}
		} else {
			t.Errorf("MF3 matches %d, expected %d", len(m3), len(m1))
			for _, m := range m1 {
				fmt.Printf("%+v, match1: [%x]\n", m, data[m.Start:m.End])
			}
			for _, m := range m3 {
				fmt.Printf("%+v, match3: [%x]\n", m, data[m.Start:m.End])
			}
		}
	})
}
