// Copyright 2021 The Erigon Authors
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

package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const MaxMapSize = 0xFFFFFFFFFFFF

func Mmap(f *os.File, size int) ([]byte, *[MaxMapSize]byte, error) {
	// Open a file mapping handle.
	sizelo := uint32(size >> 32)
	sizehi := uint32(size) & 0xffffffff
	h, errno := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, sizelo, sizehi, nil)
	if h == 0 {
		return nil, nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		return nil, nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := windows.CloseHandle(h); err != nil {
		return nil, nil, os.NewSyscallError("CloseHandle", err)
	}

	// Convert to a byte array.
	mmapHandle2 := ((*[MaxMapSize]byte)(unsafe.Pointer(addr)))
	return mmapHandle2[:size], mmapHandle2, nil
}

func MadviseSequential(mmapHandle1 []byte) error { return nil }
func MadviseNormal(mmapHandle1 []byte) error     { return nil }
func MadviseWillNeed(mmapHandle1 []byte) error   { return nil }
func MadviseRandom(mmapHandle1 []byte) error     { return nil }

func Munmap(_ []byte, mmapHandle2 *[MaxMapSize]byte) error {
	if mmapHandle2 == nil {
		return nil
	}

	addr := (uintptr)(unsafe.Pointer(&mmapHandle2[0]))
	if err := windows.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
