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
	"fmt"
	"sort"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/process"
)

type ProcessInfo struct {
	Pid       int32
	Name      string
	CPUUsage  float64
	Memory    float32
	StartTime int64
}

const (
	iterations     = 5
	sleepSeconds   = 2
	usageThreshold = 0.05
)

func AllProcessesCPU() {
	procs, err := process.Processes()
	if err != nil {
		log.Fatalf("Error retrieving processes: %v", err)
	}

	// Collect processes and calculate average stats.
	allProcsRepeats := make([][]ProcessInfo, 0, iterations)

	// Collect all processes 5 times with a delay of 2 seconds to calculate average stats.
	for i := 0; i < iterations; i++ {
		processCPUUsage := allProcesses(procs)
		allProcsRepeats = append(allProcsRepeats, processCPUUsage)
		time.Sleep(sleepSeconds * time.Second)
	}

	// Calculate average stats.
	averageProcs := mergeProcesses(allProcsRepeats)

	// Sort by CPU usage in descending order and remove processes with negligible CPU and memory usage.
	sort.Slice(averageProcs, func(i, j int) bool {
		return averageProcs[i].CPUUsage > averageProcs[j].CPUUsage
	})

	// Filter out processes with negligible CPU or memory usage.
	toDisplay := make([]ProcessInfo, 0, len(averageProcs))
	for _, proc := range averageProcs {
		if proc.CPUUsage > usageThreshold || proc.Memory > usageThreshold {
			toDisplay = append(toDisplay, proc)
		}
	}

	//print all
	for idx, proc := range toDisplay {
		fmt.Printf("#:%d PID: %d Name: %s CPU Usage: %.2f%% Memory: %.2f%% Start Time: %s\n", idx, proc.Pid, proc.Name, proc.CPUUsage, proc.Memory, time.Unix(proc.StartTime, 0))
	}

	// Optionally, you can also print total CPU usage for reference
	totalCPUPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Fatalf("Error retrieving total CPU usage: %v", err)
	}
	fmt.Printf("Total CPU Usage: %.2f%%\n", totalCPUPercent[0])
}

// merge all processes infos into one array with average values
func mergeProcesses(allProcsRepeats [][]ProcessInfo) []ProcessInfo {
	if len(allProcsRepeats) == 0 || len(allProcsRepeats[0]) == 0 {
		return nil
	}

	repeats := len(allProcsRepeats)
	allProcessLength := len(allProcsRepeats[0])
	resultArray := make([]ProcessInfo, 0, allProcessLength)

	for i := 0; i < allProcessLength; i++ {
		firstProcess := allProcsRepeats[0][i]
		totalCPUUsage := firstProcess.CPUUsage
		totalMemory := firstProcess.Memory

		if repeats > 1 {
			for j := 1; j < repeats; j++ {
				totalCPUUsage += allProcsRepeats[j][i].CPUUsage
				totalMemory += allProcsRepeats[j][i].Memory
			}

			totalCPUUsage /= float64(repeats)
			totalMemory /= float32(repeats)
		}

		resultArray = append(resultArray, ProcessInfo{
			Pid:       firstProcess.Pid,
			Name:      firstProcess.Name,
			CPUUsage:  totalCPUUsage,
			Memory:    totalMemory,
			StartTime: firstProcess.StartTime,
		})
	}

	return resultArray
}

func allProcesses(procs []*process.Process) []ProcessInfo {
	// add process cpu usage data to array in order to sort it later
	processCPUUsage := make([]ProcessInfo, 0)

	for _, proc := range procs {
		// Get process ID
		pid := proc.Pid
		// Get process name
		name, err := proc.Name()
		if err != nil {
			name = "Unknown"
		}

		// Get CPU percent
		cpuPercent, err := proc.CPUPercent()
		if err != nil {
			//log.Printf("Error retrieving CPU percent for PID %d: %v Name: %s", pid, err, name)
			continue
		}

		// Get memory percent
		memPercent, err := proc.MemoryPercent()
		if err != nil {
			//log.Printf("Error retrieving memory percent for PID %d: %v Name: %s", pid, err, name)
			continue
		}

		// Get process start time
		createTime, err := proc.CreateTime()
		if err != nil {
			//log.Printf("Error retrieving create time for PID %d: %v Name: %s", pid, err, name)
			continue
		}

		// memory info

		processCPUUsage = append(processCPUUsage, ProcessInfo{Pid: pid, Name: name, CPUUsage: cpuPercent, Memory: memPercent, StartTime: createTime})
	}

	return processCPUUsage
}
