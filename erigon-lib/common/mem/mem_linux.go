//go:build linux

package mem

import (
	"context"
	"os"
	"reflect"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v3/process"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

type VirtualMemStat struct {
	process.MemoryMapsStat
}

// Fields converts VirtualMemStat to slice
func (m VirtualMemStat) Fields() []interface{} {
	typ := reflect.TypeOf(m.MemoryMapsStat)
	val := reflect.ValueOf(m.MemoryMapsStat)

	var s []interface{}
	for i := 0; i < typ.NumField(); i++ {
		t := typ.Field(i).Name
		if t == "Path" { // always empty for aggregated smap statistics
			continue
		}

		s = append(s, t, val.Field(i).Interface())
	}

	return s
}

var (
	memRssGauge          = metrics.NewGauge(`mem_rss`)
	memSizeGauge         = metrics.NewGauge(`mem_size`)
	memPssGauge          = metrics.NewGauge(`mem_pss`)
	memSharedCleanGauge  = metrics.NewGauge(`mem_shared{type="clean"}`)
	memSharedDirtyGauge  = metrics.NewGauge(`mem_shared{type="dirty"}`)
	memPrivateCleanGauge = metrics.NewGauge(`mem_private{type="clean"}`)
	memPrivateDirtyGauge = metrics.NewGauge(`mem_private{type="dirty"}`)
	memReferencedGauge   = metrics.NewGauge(`mem_referenced`)
	memAnonymousGauge    = metrics.NewGauge(`mem_anonymous`)
	memSwapGauge         = metrics.NewGauge(`mem_swap`)
)

func ReadVirtualMemStats() (process.MemoryMapsStat, error) {
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

func UpdatePrometheusVirtualMemStats(p process.MemoryMapsStat) {
	memRssGauge.SetUint64(p.Rss)
	memSizeGauge.SetUint64(p.Size)
	memPssGauge.SetUint64(p.Pss)
	memSharedCleanGauge.SetUint64(p.SharedClean)
	memSharedDirtyGauge.SetUint64(p.SharedDirty)
	memPrivateCleanGauge.SetUint64(p.PrivateClean)
	memPrivateDirtyGauge.SetUint64(p.PrivateDirty)
	memReferencedGauge.SetUint64(p.Referenced)
	memAnonymousGauge.SetUint64(p.Anonymous)
	memSwapGauge.SetUint64(p.Swap)
}

func LogVirtualMemStats(ctx context.Context, logger log.Logger) {
	logEvery := time.NewTicker(180 * time.Second)
	defer logEvery.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-logEvery.C:
			memStats, err := ReadVirtualMemStats()
			if err != nil {
				logger.Warn("[mem] error reading virtual memory stats", "err", err)
				continue
			}

			v := VirtualMemStat{memStats}
			logger.Info("[mem] virtual memory stats", v.Fields()...)
			UpdatePrometheusVirtualMemStats(memStats)
		}
	}
}
