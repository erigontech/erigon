//go:build !linux

package mem

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
)

func ReadVirtualMemStats() (process.MemoryMapsStat, error) {
	return process.MemoryMapsStat{}, errors.New("unsupported platform")
}

func UpdatePrometheusVirtualMemStats(p process.MemoryMapsStat) {}

func LogMemStats(ctx context.Context, logger log.Logger) {
	logEvery := time.NewTicker(1 * time.Second)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			var m runtime.MemStats
			dbg.ReadMemStats(&m)

			logger.Info("[mem] memory stats", "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
		}
	}
}
