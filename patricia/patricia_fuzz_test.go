//go:build gofuzzbeta
// +build gofuzzbeta

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
	"testing"
)

// gotip test -trimpath -v -fuzz=FuzzPatricia -fuzztime=10s ./patricia

func FuzzPatricia(f *testing.F) {
	f.Fuzz(func(t *testing.T, build []byte, test []byte) {
		var n node
		keyMap := make(map[string][][]byte)
		i := 0
		for i < len(build) {
			keyLen := int(build[i]>>16) + 1
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
			//if len(key) == 0 {
			//	continue
			//}
			n.insert(key, val)
			m, _ := keyMap[string(key)]
			m = append(m, val)
			keyMap[string(key)] = m
		}
		var testKeys [][]byte
		i = 0
		for i < len(test) {
			keyLen := int(test[i]>>16) + 1
			i++
			var key []byte
			for keyLen > 0 && i < len(test) {
				key = append(key, test[i])
				i++
				keyLen--
			}
			//if len(key) == 0 {
			//	continue
			//}
			if _, ok := keyMap[string(key)]; !ok {
				testKeys = append(testKeys, key)
			}
		}
		//fmt.Printf("\n%s", &n)
		// Test for keys
		for key, vals := range keyMap {
			//fmt.Printf("key [%x], vals %x\n", key, vals)
			v, ok := n.get([]byte(key))
			if ok {
				if len(vals) == len(v) {
					for j := 0; j < len(vals); j++ {
						if !bytes.Equal(vals[j], v[j]) {
							t.Errorf("for key %x expected value[%d] %x, got %x", key, j, vals[j], v[j])
						}
					}
				} else {
					t.Errorf("for key %x expected values %d, got %d", key, len(vals), len(v))
				}
			} else {
				t.Errorf("expected key not found %x", key)
			}
		}
		// Test for non-existent keys
		for _, key := range testKeys {
			//fmt.Printf("testkey [%x]\n", key)
			_, ok := n.get(key)
			if ok {
				t.Errorf("unexpected key found [%x]", key)
			}
		}
	})
}
