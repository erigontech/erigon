package dbg

import (
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/shirou/gopsutil/v3/process"
	"os"
)

var (
	memRssGauge          = metrics.NewGauge(`mem_rss`)
	memSizeGauge         = metrics.NewGauge(`mem_size`)
	memPssGauge          = metrics.NewGauge(`mem_pss`)
	memSharedCleanGauge  = metrics.NewGauge(`mem_shared_clean`)
	memSharedDirtyGauge  = metrics.NewGauge(`mem_shared_dirty`)
	memPrivateCleanGauge = metrics.NewGauge(`mem_private_clean`)
	memPrivateDirtyGauge = metrics.NewGauge(`mem_private_dirty`)
	memReferencedGauge   = metrics.NewGauge(`mem_referenced`)
	memAnonymousGauge    = metrics.NewGauge(`mem_anonymous`)
	memSwapGauge         = metrics.NewGauge(`mem_swap`)
)

func GetMemUsage() (process.MemoryMapsStat, error) {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))

	if err != nil {
		return process.MemoryMapsStat{}, err
	}

	memoryMaps, err := proc.MemoryMaps(true)

	if err != nil {
		return process.MemoryMapsStat{}, err
	}

	memRssGauge.SetUint64((*memoryMaps)[0].Rss)
	memSizeGauge.SetUint64((*memoryMaps)[0].Size)
	memPssGauge.SetUint64((*memoryMaps)[0].Pss)
	memSharedCleanGauge.SetUint64((*memoryMaps)[0].SharedClean)
	memSharedDirtyGauge.SetUint64((*memoryMaps)[0].SharedDirty)
	memPrivateCleanGauge.SetUint64((*memoryMaps)[0].PrivateClean)
	memPrivateDirtyGauge.SetUint64((*memoryMaps)[0].PrivateDirty)
	memReferencedGauge.SetUint64((*memoryMaps)[0].Referenced)
	memAnonymousGauge.SetUint64((*memoryMaps)[0].Anonymous)
	memSwapGauge.SetUint64((*memoryMaps)[0].Swap)

	return (*memoryMaps)[0], nil
}
