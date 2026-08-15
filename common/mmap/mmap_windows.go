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

func Mmap(f *os.File, size int) ([]byte, error) {
	// Open a file mapping handle.
	maxSizeHigh := uint32(size >> 32)
	maxSizeLow := uint32(size) & 0xffffffff
	h, errno := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, maxSizeHigh, maxSizeLow, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map. The view keeps the section alive, so the mapping
	// handle is closed either way.
	addr, errno := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		_ = windows.CloseHandle(h)
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}
	if err := windows.CloseHandle(h); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), nil
}

func MadviseSequential(mmapHandle1 []byte) error { return nil }
func MadviseNormal(mmapHandle1 []byte) error     { return nil }
func MadviseWillNeed(mmapHandle1 []byte) error   { return nil }
func MadviseRandom(mmapHandle1 []byte) error     { return nil }

// Munmap accepts only the slice returned by Mmap: UnmapViewOfFile needs the
// base address of the view, which a re-slice no longer carries.
func Munmap(m []byte) error {
	if len(m) == 0 {
		return nil
	}

	addr := uintptr(unsafe.Pointer(&m[0]))
	if err := windows.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
