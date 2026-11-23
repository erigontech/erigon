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

package mem

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

const VirtualMemStatsAge = 30 * time.Minute

var (
	vmStatsMu   sync.Mutex
	vmStats     process.MemoryMapsStat
	vmStatsRead time.Time
)

// RefreshVirtualMemStats reports fresh only when this call read the stats successfully.
func RefreshVirtualMemStats(maxAge time.Duration) (stats VirtualMemStat, fresh bool) {
	vmStatsMu.Lock()
	defer vmStatsMu.Unlock()
	if time.Since(vmStatsRead) >= maxAge {
		if vm, err := ReadVirtualMemStats(); err == nil {
			vmStats = vm
			UpdatePrometheusVirtualMemStats(vm)
			fresh = true
		}
		vmStatsRead = time.Now()
	}
	return VirtualMemStat{vmStats}, fresh
}

type VirtualMemStat struct {
	process.MemoryMapsStat
}

// Fields converts VirtualMemStat to slice
func (m VirtualMemStat) Fields() []any {
	typ := reflect.TypeFor[process.MemoryMapsStat]()
	val := reflect.ValueOf(m.MemoryMapsStat)

	var s []any
	for i := 0; i < typ.NumField(); i++ {
		t := typ.Field(i).Name
		if t == "Path" || t == "Size" { // smaps_rollup omits per-mapping fields
			continue
		}

		value := val.Field(i).Interface()
		if uint64Val, ok := value.(uint64); ok {
			value = common.ByteCount(uint64Val)
		}

		s = append(s, t, value)
	}

	return s
}

func LogMemStats(ctx context.Context, logger log.Logger) {
	logEvery := time.NewTicker(180 * time.Second)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			updateProcessMemGauges()
			v, fresh := RefreshVirtualMemStats(VirtualMemStatsAge + 5*time.Minute)

			alloc, sys := dbg.MemUsage()

			var l []any
			if fresh {
				l = v.Fields()
			}
			l = append(l, "alloc", common.ByteCount(alloc), "sys", common.ByteCount(sys))

			logger.Info("[mem] memory stats", l...)
		}
	}
}
