//go:build !linux

package mem

import (
	"errors"

	"github.com/shirou/gopsutil/v3/process"
)

func ReadVirtualMemStats() (process.MemoryMapsStat, error) {
	return process.MemoryMapsStat{}, errors.New("unsupported platform")
}

func UpdatePrometheusVirtualMemStats(p process.MemoryMapsStat) {}
