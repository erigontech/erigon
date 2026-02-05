// Copyright 2024 The Erigon Authors
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

package vm

import (
	"math/bits"
	"sync"
)

const (
	returnPoolMinSize  = 64
	returnPoolMaxSize  = 1 << 20 // 1 MiB
	returnPoolMinShift = 6       // log2(64)
	returnPoolMaxShift = 20      // log2(1<<20)
)

var returnDataPools [returnPoolMaxShift - returnPoolMinShift + 1]sync.Pool

func getPooledBytes(size uint64) ([]byte, bool) {
	if size == 0 {
		return nil, false
	}
	if size > returnPoolMaxSize {
		return make([]byte, size), false
	}
	classSize := uint64(1) << uint(bits.Len64(size-1))
	if classSize < returnPoolMinSize {
		classSize = returnPoolMinSize
	}
	if classSize > returnPoolMaxSize {
		return make([]byte, size), false
	}
	idx := int(bits.Len64(classSize)-1) - returnPoolMinShift
	pool := &returnDataPools[idx]
	if v := pool.Get(); v != nil {
		buf := v.([]byte)
		return buf[:size], true
	}
	return make([]byte, classSize)[:size], true
}

func putPooledBytes(buf []byte) {
	if len(buf) == 0 {
		return
	}
	capSize := cap(buf)
	if capSize < returnPoolMinSize || capSize > returnPoolMaxSize {
		return
	}
	if capSize&(capSize-1) != 0 {
		return
	}
	idx := int(bits.Len(uint(capSize))-1) - returnPoolMinShift
	if idx < 0 || idx >= len(returnDataPools) {
		return
	}
	returnDataPools[idx].Put(buf[:capSize])
}
