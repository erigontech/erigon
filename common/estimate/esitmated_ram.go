// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package estimate

import (
	"os"
	"runtime"
	"strconv"
	"sync"

	"github.com/c2h5oh/datasize"
)

type EstimatedRamPerWorker datasize.ByteSize

// Workers - return max workers amount based on total Memory/CPU's and estimated RAM per worker
func (r EstimatedRamPerWorker) Workers() int {
	maxWorkersForGivenMemory := r.WorkersByRAMOnly()
	return min(AlmostAllCPUs(), maxWorkersForGivenMemory)
}
func (r EstimatedRamPerWorker) WorkersHalf() int { return max(1, r.Workers()/2) }

// WorkersByRAMOnly - return max workers amount based on total Memory and estimated RAM per worker
func (r EstimatedRamPerWorker) WorkersByRAMOnly() int {
	// 50% of TotalMemory. Better don't count on 100% because OOM Killer may have aggressive defaults and other software may need RAM
	return max(1, int((TotalMemory()/2)/uint64(r)))
}

const (
	// elias-fano index building is single-threaded
	// when set it to 4GB - observed OOM-kill at server with 128Gb ram and 32CPU
	IndexSnapshot = EstimatedRamPerWorker(1 * datasize.GB)

	// 1-file-compression is multi-threaded
	CompressSnapshot = EstimatedRamPerWorker(1 * datasize.GB)

	StateV3Collate = EstimatedRamPerWorker(5 * datasize.GB)

	// BlocksExecution - in multi-threaded mode
	BlocksExecution = EstimatedRamPerWorker(512 * datasize.MB)
)

// AlmostAllCPUs - return all-but-one cpus. Leaving 1 cpu for "work producer", also cloud-providers do recommend leave 1 CPU for their IO software
// user can reduce GOMAXPROCS env variable
func AlmostAllCPUs() int {
	return max(1, runtime.GOMAXPROCS(-1)-1)
}

func AllCPUs() int {
	return max(1, runtime.GOMAXPROCS(-1))
}

// IOOverlapFactor sizes the Ps above the CPU count and the pools that mostly wait on
// page faults of cold mmap'd state files: a faulting goroutine holds its P.
const IOOverlapFactor = 4

// IOWorkers is the default size of a pool that mostly blocks on page faults.
func IOWorkers() int {
	return IOOverlapFactor * AllCPUs()
}

var ioProcs struct {
	sync.Mutex
	holders, base int
}

// RaiseProcsForIO raises GOMAXPROCS by GOMAXPROCS_EXTRA (default (IOOverlapFactor-1) x the
// current value) for a stretch of page-fault-bound work and returns its release. Overlapping
// stretches share one raise; the last release restores the runtime default. A set GOMAXPROCS
// env is respected.
func RaiseProcsForIO() (release func()) {
	if os.Getenv("GOMAXPROCS") != "" {
		return func() {}
	}
	ioProcs.Lock()
	defer ioProcs.Unlock()
	if ioProcs.holders == 0 {
		ioProcs.base = runtime.GOMAXPROCS(0)
		extra := (IOOverlapFactor - 1) * ioProcs.base
		if v, err := strconv.Atoi(os.Getenv("GOMAXPROCS_EXTRA")); err == nil {
			extra = v
		}
		if extra <= 0 {
			return func() {}
		}
		runtime.GOMAXPROCS(ioProcs.base + extra)
	}
	ioProcs.holders++
	return sync.OnceFunc(func() {
		ioProcs.Lock()
		defer ioProcs.Unlock()
		if ioProcs.holders--; ioProcs.holders == 0 {
			runtime.SetDefaultGOMAXPROCS() // also turns automatic updates back on
		}
	})
}

func HalfCPUs() int {
	return max(1, runtime.GOMAXPROCS(-1)/2)
}
