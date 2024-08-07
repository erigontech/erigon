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

package sysutils

import (
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/shirou/gopsutil/v4/process"
)

type ProcessInfo struct {
	Pid      int32
	Name     string
	CPUUsage float64
	Memory   float32
}

type ProcessMerge struct {
	CPUUsage float64
	Memory   float32
	Times    int
	Name     string
}

const (
	iterations     = 5
	sleepSeconds   = 2
	usageThreshold = 0.05
)

func GetProcessesInfo() []*ProcessInfo {
	procs, err := process.Processes()
	if err != nil {
		log.Debug("[Sysutil] Error retrieving processes: %v", err)
	}

	return averageProceses(procs)
}

func AverageProceses(procs []*process.Process) []*ProcessInfo {
	return averageProceses(procs)
}

func averageProceses(procs []*process.Process) []*ProcessInfo {
	// Collect processes and calculate average stats.
	allProcsRepeats := make([][]*ProcessInfo, 0, iterations)

	// Collect all processes N times with a delay of N seconds to calculate average stats.
	for i := 0; i < iterations; i++ {
		processes := allProcesses(procs)
		allProcsRepeats = append(allProcsRepeats, processes)
		time.Sleep(sleepSeconds * time.Second)
	}

	// Calculate average stats.
	averageProcs := mergeProcesses(allProcsRepeats)
	averageProcs = removeProcessesAboveThreshold(averageProcs, usageThreshold)

	return averageProcs
}

func RemoveProcessesAboveThreshold(processes []*ProcessInfo, treshold float64) []*ProcessInfo {
	return removeProcessesAboveThreshold(processes, treshold)
}

func removeProcessesAboveThreshold(processes []*ProcessInfo, treshold float64) []*ProcessInfo {
	// remove processes with CPU or Memory usage less than threshold
	for i := 0; i < len(processes); i++ {
		if processes[i].CPUUsage < treshold && processes[i].Memory < float32(treshold) {
			processes = append(processes[:i], processes[i+1:]...)
			i--
		}
	}
	return processes
}

func MergeProcesses(allProcsRepeats [][]*ProcessInfo) []*ProcessInfo {
	return mergeProcesses(allProcsRepeats)
}

func mergeProcesses(allProcsRepeats [][]*ProcessInfo) []*ProcessInfo {
	if len(allProcsRepeats) == 0 || len(allProcsRepeats[0]) == 0 {
		return nil
	}

	repeats := len(allProcsRepeats)
	if repeats == 1 {
		return allProcsRepeats[0]
	}

	prcmap := make(map[int32]*ProcessMerge)

	for _, procList := range allProcsRepeats {
		for _, proc := range procList {
			if prc, exists := prcmap[proc.Pid]; exists {
				prc.CPUUsage += proc.CPUUsage
				prc.Memory += proc.Memory
				prc.Times++
			} else {
				prcmap[proc.Pid] = &ProcessMerge{
					CPUUsage: proc.CPUUsage,
					Memory:   proc.Memory,
					Times:    1,
					Name:     proc.Name,
				}
			}
		}
	}

	resultArray := make([]*ProcessInfo, 0, len(prcmap))

	for pid, prc := range prcmap {
		resultArray = append(resultArray, &ProcessInfo{
			Pid:      pid,
			Name:     prc.Name,
			CPUUsage: prc.CPUUsage / float64(prc.Times),
			Memory:   prc.Memory / float32(prc.Times),
		})
	}

	return resultArray
}

func allProcesses(procs []*process.Process) []*ProcessInfo {
	processes := make([]*ProcessInfo, 0)

	for _, proc := range procs {
		pid := proc.Pid
		name, err := proc.Name()
		if err != nil {
			name = "Unknown"
		}

		//remove gopls process as it is what we use to get info
		if name == "gopls" {
			continue
		}

		cpuPercent, err := proc.CPUPercent()
		if err != nil {
			log.Trace("[Sysutil] Error retrieving CPU percent for PID %d: %v Name: %s", pid, err, name)
			continue
		}

		memPercent, err := proc.MemoryPercent()
		if err != nil {
			log.Trace("[Sysutil] Error retrieving memory percent for PID %d: %v Name: %s", pid, err, name)
			continue
		}

		processes = append(processes, &ProcessInfo{Pid: pid, Name: name, CPUUsage: cpuPercent, Memory: memPercent})
	}

	return processes
}
