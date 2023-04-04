package estimate

import (
	"os"
	"runtime"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/pbnjay/memory"
	"github.com/shirou/gopsutil/v3/docker"
)

type estimatedRamPerWorker datasize.ByteSize

// Workers - return max workers amount based on total Memory/CPU's and estimated RAM per worker
func (r estimatedRamPerWorker) Workers() int {
	// 50% of TotalMemory. Better don't count on 100% because OOM Killer may have aggressive defaults and other software may need RAM
	maxWorkersForGivenMemory := (totalMemory() / 2) / uint64(r)
	return cmp.Min(AlmostAllCPUs(), int(maxWorkersForGivenMemory))
}
func (r estimatedRamPerWorker) WorkersHalf() int    { return cmp.Max(1, r.Workers()/2) }
func (r estimatedRamPerWorker) WorkersQuarter() int { return cmp.Max(1, r.Workers()/4) }

const (
	IndexSnapshot     = estimatedRamPerWorker(2 * datasize.GB)   //elias-fano index building is single-threaded
	CompressSnapshot  = estimatedRamPerWorker(1 * datasize.GB)   //1-file-compression is multi-threaded
	ReconstituteState = estimatedRamPerWorker(512 * datasize.MB) //state-reconstitution is multi-threaded
)

// AlmostAllCPUs - return all-but-one cpus. Leaving 1 cpu for "work producer", also cloud-providers do recommend leave 1 CPU for their IO software
// user can reduce GOMAXPROCS env variable
func AlmostAllCPUs() int {
	return cmp.Max(1, runtime.GOMAXPROCS(-1)-1)
}
func totalMemory() uint64 {
	mem := memory.TotalMemory()

	if cgroupsMemLimit, ok := cgroupsMemoryLimit(); ok {
		mem = cmp.Min(mem, cgroupsMemLimit)
	}

	return mem
}

// apply limit from docker if can, treat errors as "not available or maybe non-docker environment
// supports only cgroups v1, for v2 see: https://github.com/shirou/gopsutil/issues/1416
func cgroupsMemoryLimit() (mem uint64, ok bool) {
	hostname, err := os.Hostname()
	if err != nil {
		return 0, false
	}
	cgmem, err := docker.CgroupMemDocker(hostname)
	if err != nil {
		return 0, false
	}
	if cgmem == nil || cgmem.MemLimitInBytes <= 0 {
		return 0, false
	}
	return cgmem.MemLimitInBytes, true
}
