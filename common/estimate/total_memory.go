// Copyright 2024 The Erigon Authors
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

package estimate

import (
	"runtime/debug"

	"github.com/pbnjay/memory"
)

func TotalMemory() uint64 {
	mem := memory.TotalMemory()

	if cgroupsMemLimit, err := cgroupsMemoryLimit(); (err == nil) && (cgroupsMemLimit > 0) {
		mem = min(mem, cgroupsMemLimit)
	}

	if goMemLimit := debug.SetMemoryLimit(-1); goMemLimit > 0 {
		mem = min(mem, uint64(goMemLimit))
	}

	return mem
}
