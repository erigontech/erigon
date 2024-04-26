//go:build !nofuzz

/*
Copyright 2021 Erigon contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
	})
}
