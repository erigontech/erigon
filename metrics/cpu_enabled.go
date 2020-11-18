// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// +build !ios

package metrics

import (
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/process"
)

func ReadCPUStats(p *process.Process, stats *CPUStats) {
	if m, _ := p.Times(); m != nil {
		// requesting all cpu times will always return an array with only one time stats entry
		stats.GlobalTime = int64((m.User + m.Nice + m.System) * cpu.ClocksPerSec)
		stats.GlobalWait = int64((m.Iowait) * cpu.ClocksPerSec)
	if len(timeStats) == 0 {
		log.Error("Empty cpu stats")
		return
	}
}
