//go:build !windows

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
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const MaxMapSize = 0xFFFFFFFFFFFF

// mmap memory maps a DB's data file.
func MmapRw(f *os.File, size int) ([]byte, *[MaxMapSize]byte, error) {
	// Map the data file to memory.
	mmapHandle1, err := unix.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	err = unix.Madvise(mmapHandle1, syscall.MADV_RANDOM)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return nil, nil, fmt.Errorf("madvise: %w", err)
	}
	mmapHandle2 := (*[MaxMapSize]byte)(unsafe.Pointer(&mmapHandle1[0]))
	return mmapHandle1, mmapHandle2, nil
}
func Mmap(f *os.File, size int) ([]byte, *[MaxMapSize]byte, error) {
	// Map the data file to memory.
	mmapHandle1, err := unix.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	err = unix.Madvise(mmapHandle1, syscall.MADV_RANDOM)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return nil, nil, fmt.Errorf("madvise: %w", err)
	}
	mmapHandle2 := (*[MaxMapSize]byte)(unsafe.Pointer(&mmapHandle1[0]))
	return mmapHandle1, mmapHandle2, nil
}

func MadviseSequential(mmapHandle1 []byte) error {
	err := unix.Madvise(mmapHandle1, syscall.MADV_SEQUENTIAL)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return fmt.Errorf("madvise: %w", err)
	}
	return nil
}

func MadviseNormal(mmapHandle1 []byte) error {
	err := unix.Madvise(mmapHandle1, syscall.MADV_NORMAL)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return fmt.Errorf("madvise: %w", err)
	}
	return nil
}

func MadviseWillNeed(mmapHandle1 []byte) error {
	err := unix.Madvise(mmapHandle1, syscall.MADV_WILLNEED)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return fmt.Errorf("madvise: %w", err)
	}
	return nil
}

func MadviseRandom(mmapHandle1 []byte) error {
	err := unix.Madvise(mmapHandle1, syscall.MADV_RANDOM)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return fmt.Errorf("madvise: %w", err)
	}
	return nil
}

// munmap unmaps a DB's data file from memory.
func Munmap(mmapHandle1 []byte, _ *[MaxMapSize]byte) error {
	// Ignore the unmap if we have no mapped data.
	if mmapHandle1 == nil {
		return nil
	}
	// Unmap using the original byte slice.
	err := unix.Munmap(mmapHandle1)
	return err
}
