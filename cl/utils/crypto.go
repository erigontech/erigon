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
	New: func() any {
		return sha256.New()
	},
}

// sha256StackBuf bounds the concatenation of data+extras, sized for the SSZ calls
// that join two 32-byte roots. Longer concatenations go to the pooled hasher
// instead; a lone data argument needs no buffer and is hashed at any size.
const sha256StackBuf = 64

// General purpose Sha256
func Sha256(data []byte, extras ...[]byte) [32]byte {
	if len(extras) == 0 {
		return sha256.Sum256(data)
	}
	total := len(data)
	for _, extra := range extras {
		total += len(extra)
	}
	if total > sha256StackBuf {
		return sha256Streamed(data, extras...)
	}
	var buf [sha256StackBuf]byte
	n := copy(buf[:], data)
	for _, extra := range extras {
		n += copy(buf[n:], extra)
	}
	return sha256.Sum256(buf[:n])
}

// sha256Streamed hashes inputs too large for Sha256's stack buffer.
func sha256Streamed(data []byte, extras ...[]byte) [32]byte {
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
