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
	"reflect"
	"syscall"

	"golang.org/x/sys/unix"

	_ "github.com/erigontech/erigon/common/race"
)

var osPageSize = uintptr(os.Getpagesize())

// pageAligned returns the sub-slice of m covering only whole pages:
// start rounded up, end rounded down. Returns nil when m spans no full page.
func pageAligned(m []byte) []byte {
	if len(m) == 0 {
		return nil
	}
	start := reflect.ValueOf(m).Pointer()
	skip := int((osPageSize - start%osPageSize) % osPageSize)
	trim := int((start + uintptr(len(m))) % osPageSize)
	if skip+trim >= len(m) {
		return nil
	}
	return m[skip : len(m)-trim]
}

func Mmap(f *os.File, size int) ([]byte, error) {
	m, err := unix.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	// Advise the kernel that the mmap is accessed randomly.
	// Ignore not implemented error in kernel because it still works.
	if err := unix.Madvise(m, syscall.MADV_RANDOM); err != nil && !errors.Is(err, syscall.ENOSYS) {
		_ = unix.Munmap(m)
		return nil, fmt.Errorf("madvise: %w", err)
	}
	return m, nil
}

func madvise(m []byte, advice int) error {
	if aligned := pageAligned(m); len(aligned) > 0 {
		if err := unix.Madvise(aligned, advice); err != nil && !errors.Is(err, syscall.ENOSYS) {
			return fmt.Errorf("madvise: %w", err)
		}
	}
	return nil
}

func MadviseSequential(m []byte) error { return madvise(m, syscall.MADV_SEQUENTIAL) }
func MadviseNormal(m []byte) error     { return madvise(m, syscall.MADV_NORMAL) }
func MadviseWillNeed(m []byte) error   { return madvise(m, syscall.MADV_WILLNEED) }
func MadviseRandom(m []byte) error     { return madvise(m, syscall.MADV_RANDOM) }

// Munmap accepts only the slice returned by Mmap: a re-slice has a different
// base address and length, which munmap(2) rejects.
func Munmap(m []byte) error {
	if len(m) == 0 {
		return nil
	}
	return unix.Munmap(m)
}
