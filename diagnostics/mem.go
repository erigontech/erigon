package diagnostics

import (
	"github.com/shirou/gopsutil/v3/process"
	"os"
)

func getMemUsage() (process.MemoryMapsStat, error) {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))

	if err != nil {
		return process.MemoryMapsStat{}, err
	}

	memoryMaps, err := proc.MemoryMaps(true)

	if err != nil {
		return process.MemoryMapsStat{}, err
	}

	return (*memoryMaps)[0], nil
}
