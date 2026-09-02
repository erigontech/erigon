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
	"sync"

	"github.com/shirou/gopsutil/v4/mem"
)

var (
	totalMemoryOnce   sync.Once
	totalMemoryCached uint64
)

// SystemMemory reports the machine's physical memory, ignoring any cgroup
// confinement. TotalMemory folds the cgroup in; this is the number to compare a
// cgroup limit against to decide whether it constrains anything.
func SystemMemory() uint64 {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return vm.Total
}

// CgroupsMemoryLimit reports the memory limit this process is confined to, or 0
// when it is not confined. Callers that only want a sizing input should use
// TotalMemory; this is for deciding whether a cgroup is the binding constraint.
func CgroupsMemoryLimit() uint64 {
	limit, err := cgroupsMemoryLimit()
	if err != nil {
		return 0
	}
	return limit
}

func TotalMemory() uint64 {
	totalMemoryOnce.Do(func() {
		var total uint64
		if vm, err := mem.VirtualMemory(); err == nil {
			total = vm.Total
		}

		if cgroupsMemLimit, err := cgroupsMemoryLimit(); (err == nil) && (cgroupsMemLimit > 0) {
			total = min(total, cgroupsMemLimit)
		}

		if goMemLimit := debug.SetMemoryLimit(-1); goMemLimit > 0 {
			total = min(total, uint64(goMemLimit))
		}

		totalMemoryCached = total
	})
	return totalMemoryCached
}
