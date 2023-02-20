/*
   Copyright 2022 Erigon-Lightclient contributors
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

package utils

import (
	"crypto/sha256"
	"hash"
	"sync"
)

type HashFunc func(data []byte, extras ...[]byte) [32]byte

var hasherPool = sync.Pool{
	New: func() interface{} {
		return sha256.New()
	},
}

// General purpose Keccak256
func Keccak256(data []byte, extras ...[]byte) [32]byte {
	h, ok := hasherPool.Get().(hash.Hash)
	if !ok {
		h = sha256.New()
	}
	defer hasherPool.Put(h)
	h.Reset()

	var b [32]byte

	h.Write(data)
	for _, extra := range extras {
		h.Write(extra)
	}
	h.Sum(b[:0])
	return b
}

// Optimized Keccak256, avoid pool.put/pool.get, meant for intensive operations.
func OptimizedKeccak256() HashFunc {
	h := sha256.New()
	return func(data []byte, extras ...[]byte) [32]byte {
		h.Reset()
		var b [32]byte
		h.Write(data)
		for _, extra := range extras {
			h.Write(extra)
		}
		h.Sum(b[:0])
		return b
	}
}
