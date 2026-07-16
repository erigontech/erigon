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

	"github.com/erigontech/erigon/common"
)

type HashFunc func(data []byte, extras ...[]byte) common.Hash

// sha256StackBuf sizes the join buffer for the SSZ calls that join two 32-byte
// roots; larger joins use the heap.
const sha256StackBuf = 64

// General purpose Sha256
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

// sha256Joined hashes the concatenation of data and extras. Hashing through a pooled
// hash.Hash would leak every caller's buffer to the heap, since Write is an interface method.
func sha256Joined(data []byte, extras [][]byte, total int) common.Hash {
	buf := make([]byte, 0, total)
	buf = append(buf, data...)
	for _, extra := range extras {
		buf = append(buf, extra...)
	}
	return common.Hash(sha256.Sum256(buf))
}
