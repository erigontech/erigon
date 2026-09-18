// Copyright 2026 The Erigon Authors
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

//go:build linux

package seg

/*
#include <stddef.h>

static void erigon_prefault(const char *p, size_t n, size_t step) {
	volatile char sink = 0;
	for (size_t i = 0; i < n; i += step) {
		sink ^= p[i];
	}
	(void)sink;
}
*/
import "C"

import (
	"unsafe"

	"github.com/erigontech/erigon/common/dbg"
)

var residencyCgoPrefault = dbg.EnvBool("RESIDENCY_CGO_PREFAULT", false)

// cgoPrefault faults a mapped range in from C. A page fault taken in Go code
// keeps the goroutine's P for the whole ~70us of the read, because the runtime
// is never told the thread blocked; cgocall runs entersyscall first, so the same
// fault taken from C releases the P and another goroutine can run. Concurrent
// reads are otherwise capped at GOMAXPROCS.
func (d *Decompressor) cgoPrefault(fileOffset int64, n int) bool {
	if !residencyCgoPrefault || n <= 0 {
		return false
	}
	off := int(fileOffset)
	if off < 0 || off+n > len(d._mmapHandle) {
		return false
	}
	C.erigon_prefault((*C.char)(unsafe.Pointer(&d._mmapHandle[off])), C.size_t(n), C.size_t(pageSize))
	return true
}
