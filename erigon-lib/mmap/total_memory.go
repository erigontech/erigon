package mmap

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
