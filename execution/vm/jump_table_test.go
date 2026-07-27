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

package vm

import (
	"testing"
	"unsafe"
)

// The interpreter reads several operation fields per executed opcode; a
// 32-byte entry keeps the whole read set on one cache line and the table at
// 8 KiB. Growing the struct is a silent interpreter regression — shrink or
// repack instead.
func TestOperationEntryIs32Bytes(t *testing.T) {
	if size := unsafe.Sizeof(operation{}); size != 32 {
		t.Fatalf("operation struct is %d bytes, want 32", size)
	}
}
