// Copyright 2025 The Erigon Authors
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

package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Resident reports whether every page spanned by m is present in this process's
// page tables (i.e. touching it will not take a major fault). m must start on a
// page boundary. It never triggers I/O.
func Resident(m []byte) (bool, error) {
	if len(m) == 0 {
		return true, nil
	}
	pageSize := os.Getpagesize()
	vec := make([]byte, (len(m)+pageSize-1)/pageSize)
	_, _, errno := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&m[0])),
		uintptr(len(m)),
		uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return false, errno
	}
	for _, v := range vec {
		if v&1 == 0 {
			return false, nil
		}
	}
	return true, nil
}
