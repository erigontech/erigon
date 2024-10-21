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

//go:build !windows

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
