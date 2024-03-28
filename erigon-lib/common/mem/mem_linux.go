//go:build linux

package mem

import (
	"os"
	"reflect"

	"github.com/shirou/gopsutil/v3/process"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

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

	m := (*memoryMaps)[0]

	// convert from kilobytes to bytes
	val := reflect.ValueOf(&m).Elem()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)

		if field.Kind() == reflect.Uint64 {
			field.SetUint(field.Interface().(uint64) * 1024)
		}
	}

	return m, nil
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
