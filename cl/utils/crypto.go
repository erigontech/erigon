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
	"sync"

	"github.com/erigontech/erigon/common"
)

// joinBufPool holds scratch buffers for joins too large for the stack buffer.
var joinBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 2*sha256StackBuf)
		return &b
	},
}

// joinBufKeepCap bounds what a join returns to the pool, so one outsized call
// does not pin a large buffer per processor for the process lifetime.
const joinBufKeepCap = 1 << 16

type HashFunc func(data []byte, extras ...[]byte) common.Hash

// sha256StackBuf sizes the join buffer for the SSZ calls that join two 32-byte
// roots; larger joins use the heap.
const sha256StackBuf = 64

// Sha256 returns the SHA-256 of data followed by extras.
//
// It hashes through the concrete sha256.Sum256, never a hash.Hash. The interface is
// what costs, not the pooling:
//   - Write and Sum are interface methods, so the compiler must assume they leak.
//     That alone heap-allocates the digest, and since escape analysis is
//     flow-insensitive, one such branch also pushes every caller's buffer out,
//     including callers that never reach it.
//   - So a hash.Hash cannot reach zero allocations however it is pooled. On
//     Sha256(32B, 32B) it costs 1-3 allocs and several times the time, and the gap
//     widens with core count, because those allocations bound the scaling.
//   - Pooling itself is fine. Joins above sha256StackBuf pool their scratch buffer,
//     which append copies into, so that path stays allocation-free too.
func Sha256(data []byte, extras ...[]byte) common.Hash {
	if len(extras) == 0 {
		return common.Hash(sha256.Sum256(data))
	}
	total := len(data)
	for _, extra := range extras {
		total += len(extra)
	}
	if total > sha256StackBuf {
		return sha256Joined(data, extras, total)
	}
	var buf [sha256StackBuf]byte
	n := copy(buf[:], data)
	for _, extra := range extras {
		n += copy(buf[n:], extra)
	}
	return common.Hash(sha256.Sum256(buf[:n]))
}

// sha256Joined hashes the concatenation of data and extras. It pools the scratch
// buffer rather than a hash.Hash: Write is an interface method, so a pooled hasher
// would leak the arguments and push every caller's buffer onto the heap. append
// copies, so the pooled buffer costs nothing in escape analysis.
func sha256Joined(data []byte, extras [][]byte, total int) common.Hash {
	p := joinBufPool.Get().(*[]byte)
	buf := append((*p)[:0], data...)
	for _, extra := range extras {
		buf = append(buf, extra...)
	}
	out := common.Hash(sha256.Sum256(buf))
	if cap(buf) <= joinBufKeepCap {
		*p = buf
		joinBufPool.Put(p)
	}
	return out
}
