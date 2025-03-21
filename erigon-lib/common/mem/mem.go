//go:build !linux

package mem

import (
	"errors"

	"github.com/shirou/gopsutil/v4/process"
)

func ReadVirtualMemStats() (process.MemoryMapsStat, error) {
	return process.MemoryMapsStat{}, errors.New("unsupported platform")
}

func UpdatePrometheusVirtualMemStats(p process.MemoryMapsStat) {}
