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
	"runtime"

	"github.com/c2h5oh/datasize"
)

type EstimatedRamPerWorker datasize.ByteSize

// Workers - return max workers amount based on total Memory/CPU's and estimated RAM per worker
func (r EstimatedRamPerWorker) Workers() int {
	maxWorkersForGivenMemory := r.WorkersByRAMOnly()
	res := min(AlmostAllCPUs(), maxWorkersForGivenMemory)
	return max(1, res) // must have at-least 1 worker
}

func (r EstimatedRamPerWorker) WorkersHalf() int {
	return max(1, r.Workers()/2)
}

func (r EstimatedRamPerWorker) WorkersQuarter() int {
	return max(1, r.Workers()/4)
}

// WorkersByRAMOnly - return max workers amount based on total Memory and estimated RAM per worker
func (r EstimatedRamPerWorker) WorkersByRAMOnly() int {
	// 50% of TotalMemory. Better don't count on 100% because OOM Killer may have aggressive defaults and other software may need RAM
	return max(1, int((TotalMemory()/2)/uint64(r)))
}

const (
	//elias-fano index building is single-threaded
	// when set it to 3GB - observed OOM-kil at server with 128Gb ram and 32CPU
	IndexSnapshot = EstimatedRamPerWorker(4 * datasize.GB)

	//1-file-compression is multi-threaded
	CompressSnapshot = EstimatedRamPerWorker(1 * datasize.GB)

	StateV3Collate = EstimatedRamPerWorker(5 * datasize.GB)

	//BlocksExecution - in multi-threaded mode
	BlocksExecution = EstimatedRamPerWorker(512 * datasize.MB)
)

// AlmostAllCPUs - return all-but-one cpus. Leaving 1 cpu for "work producer", also cloud-providers do recommend leave 1 CPU for their IO software
// user can reduce GOMAXPROCS env variable
func AlmostAllCPUs() int {
	return max(1, runtime.GOMAXPROCS(-1)-1)
}
func HalfCPUs() int {
	return max(1, runtime.GOMAXPROCS(-1)/2)
}
