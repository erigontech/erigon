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

package debug

import (
	"fmt"
	"runtime"

	"github.com/erigontech/erigon-lib/common/dbg"
)

func PrintMemStats(short bool) {
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	if short {
		fmt.Printf("HeapInuse: %vMb\n", ByteToMb(m.HeapInuse))
	} else {
		fmt.Printf("HeapInuse: %vMb, Alloc: %vMb, TotalAlloc: %vMb, Sys: %vMb, NumGC: %v, PauseNs: %d\n", ByteToMb(m.HeapInuse), ByteToMb(m.Alloc), ByteToMb(m.TotalAlloc), ByteToMb(m.Sys), m.NumGC, m.PauseNs[(m.NumGC+255)%256])
	}
}

func ByteToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
