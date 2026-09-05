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

//go:build !linux

package mdbx

// ResidencyGate needs io_uring and /proc/self/maps, so it is linux-only.
const ResidencyGate = false

type dataMapping struct{}

func (db *MdbxKV) ValueFileRegion([]byte) (fd uintptr, off int64, ok bool) { return 0, 0, false }

func (db *MdbxKV) WarmValue([]byte) {}
