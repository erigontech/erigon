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

var startupGoMemLimit = debug.SetMemoryLimit(-1)

// memoryBound folds the probes into the tightest one. A zero means the probe
// failed, not a bound of zero, so it is skipped — otherwise an unreadable
// /proc/meminfo would discard a cgroup limit that was read fine.
func memoryBound(bounds ...uint64) uint64 {
	var total uint64
	for _, b := range bounds {
		if b > 0 && (total == 0 || b < total) {
			total = b
		}
	}
	return total
}

func TotalMemory() uint64 {
	totalMemoryOnce.Do(func() {
		var system uint64
		if vm, err := mem.VirtualMemory(); err == nil {
			system = vm.Total
		}

		var cgroup uint64
		if cgroupsMemLimit, err := cgroupsMemoryLimit(); err == nil {
			cgroup = cgroupsMemLimit
		}

		var goMemLimit uint64
		if startupGoMemLimit > 0 {
			goMemLimit = uint64(startupGoMemLimit)
		}

		totalMemoryCached = memoryBound(system, cgroup, goMemLimit)
	})
	return totalMemoryCached
}
