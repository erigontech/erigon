// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Platform-specific functions of the Etheruem database package
package ethdb

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// mmap memory maps a HexHeap's data file.
func mmap(f *os.File, sz int) ([]byte, error) {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(f.Fd()), 0, sz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
	}

	return b, nil
}

// munmap unmaps a DB's data file from memory.
func munmap(h []byte) error {
	// Ignore the unmap if we have no mapped data.
	if h == nil {
		return nil
	}

	// Unmap using the original byte slice.
	return syscall.Munmap(h)
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
