//go:build !linux

package mem

import (
	"context"
	"errors"

	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v3/process"
)

func ReadVirtualMemStats() (process.MemoryMapsStat, error) {
	return process.MemoryMapsStat{}, errors.New("unsupported platform")
}

func UpdatePrometheusVirtualMemStats(p process.MemoryMapsStat) {}

func LogVirtualMemStats(ctx context.Context, logger log.Logger) {}
