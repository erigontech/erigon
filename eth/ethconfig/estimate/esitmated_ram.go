package estimate

import (
	"runtime"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/pbnjay/memory"
)

type estimatedRamPerWorker datasize.ByteSize

// Workers - return max workers amount based on total Memory/CPU's and estimated RAM per worker
func (r estimatedRamPerWorker) Workers() int {
	// 50% of TotalMemory. Better don't count on 100% because OOM Killer may have aggressive defaults and other software may need RAM
	maxWorkersForGivenMemory := (memory.TotalMemory() / 2) / uint64(r)
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
