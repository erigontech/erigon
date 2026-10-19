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
package state

import (
	"os"

	"golang.org/x/sys/unix"
)

// writeBack starts the writeback of [off, off+n) and waits for the previous chunk, so at most two chunks are dirty.
func writeBack(f *os.File, off, n, chunk int64) error {
	fd := int(f.Fd())
	if err := unix.SyncFileRange(fd, off, n, unix.SYNC_FILE_RANGE_WRITE); err != nil {
		return err
	}
	if off == 0 {
		return nil
	}
	return unix.SyncFileRange(fd, off-chunk, chunk, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
}
