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
		for key, vals := range keyMap {
			v, ok := n.get([]byte(key))
			if ok {
				if !bytes.Equal(vals, v.([]byte)) {
					t.Errorf("for key %x expected value %x, got %x", key, vals, v.([]byte))
				}
			}
		}
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
		ft := pt.Flatten()
		mf3 := NewMatchFinder3(ft)
		_ = mf3.FindLongestMatches(data)
	})
}
