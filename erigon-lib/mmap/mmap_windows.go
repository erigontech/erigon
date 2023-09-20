/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
	if err := windows.CloseHandle(windows.Handle(h)); err != nil {
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
