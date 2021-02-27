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
	"syscall"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/shirou/gopsutil/v3/cpu"
)

// ReadCPUStats retrieves the current CPU stats.
func ReadCPUStats(stats *CPUStats) {
	// passing false to request all cpu times
	timeStats, err := cpu.Times(false)
	if err != nil {
		log.Error("Could not read cpu stats", "err", err)
		return
	}

	// requesting all cpu times will always return an array with only one time stats entry
	timeStat := timeStats[0]
	stats.GlobalTime = int64((timeStat.User + timeStat.Nice + timeStat.System) * cpu.ClocksPerSec)
	stats.GlobalWait = int64((timeStat.Iowait) * cpu.ClocksPerSec)

	stats.Usage = getRUsage()
	stats.LocalTime = cpuTimeFromUsage(stats.Usage)
}

func ReadCPUStats2(in *cpu.TimesStat, stats *CPUStats) {
	// requesting all cpu times will always return an array with only one time stats entry
	stats.GlobalTime = int64((in.User + in.Nice + in.System) * cpu.ClocksPerSec)
	stats.GlobalWait = int64((in.Iowait) * cpu.ClocksPerSec)
	stats.LocalTime = int64(in.Total())
	stats.Usage = getRUsage()
}

func cpuTimeFromUsage(usage syscall.Rusage) int64 {
	return int64(usage.Utime.Sec+usage.Stime.Sec)*100 + int64(usage.Utime.Usec+usage.Stime.Usec)/10000 //nolint:unconvert
}
