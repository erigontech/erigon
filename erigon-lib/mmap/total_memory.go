package mmap

import (
	"runtime/debug"

	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/pbnjay/memory"
)

func TotalMemory() uint64 {
	mem := memory.TotalMemory()

	if cgroupsMemLimit, err := cgroupsMemoryLimit(); (err == nil) && (cgroupsMemLimit > 0) {
		mem = cmp.Min(mem, cgroupsMemLimit)
	}

	if goMemLimit := debug.SetMemoryLimit(-1); goMemLimit > 0 {
		mem = cmp.Min(mem, uint64(goMemLimit))
	}

	return mem
}
