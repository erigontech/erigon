// Copyright 2022 The Erigon Authors
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

// General purpose Sha256
func Sha256(data []byte, extras ...[]byte) [32]byte {
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

// Optimized Sha256, avoid pool.put/pool.get, meant for intensive operations.
// this version is not thread safe
func OptimizedSha256NotThreadSafe() HashFunc {
	h := sha256.New()
	var b [32]byte
	return func(data []byte, extras ...[]byte) [32]byte {
		h.Reset()
		h.Write(data)
		for _, extra := range extras {
			h.Write(extra)
		}
		h.Sum(b[:0])
		return b
	}
}
