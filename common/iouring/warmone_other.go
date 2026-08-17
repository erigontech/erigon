// Copyright 2025 The Erigon Authors
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

//go:build !linux

package iouring

// WarmOne is unreachable off Linux: the residency probe (mmap.Resident) is a
// no-op there, so the gate never warms. It panics rather than silently no-op
// because there is no fallback path — io_uring is Linux-only.
func WarmOne(fd int, off int64, length int) {
	panic("iouring: io_uring warming is only available on linux")
}

// Available reports io_uring support: always false off Linux.
func Available() bool { return false }
