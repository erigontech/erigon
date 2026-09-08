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

// pageAligned returns the sub-slice of m starting at a page boundary, which is
// what madvise(2) requires of its address. The tail is left alone: the kernel
// rounds the length up to a whole page, and mmap only ever hands out whole pages.
// Rounding the tail down instead would skip a mapping shorter than one page
// entirely, and split the VMA of any mapping with a partial last page.
func pageAligned(m []byte) []byte {
	if len(m) == 0 {
		return nil
	}
	start := reflect.ValueOf(m).Pointer()
	skip := int((osPageSize - start%osPageSize) % osPageSize)
	if skip >= len(m) {
		return nil
	}
	return m[skip:]
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
