//go:build linux

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

package eliasfano32

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// fallocate extends f to size bytes and, where supported, allocates disk
// blocks so ENOSPC surfaces as an error instead of SIGBUS on an RDWR mmap write.
func fallocate(f *os.File, size int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	for errors.Is(err, unix.EINTR) {
		err = unix.Fallocate(int(f.Fd()), 0, 0, size)
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		_, err = f.WriteAt([]byte{0}, size-1)
	}
	return err
}
