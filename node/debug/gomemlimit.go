// Copyright 2026 The Erigon Authors
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
	"math"
	"os"
	"runtime/debug"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
)

// defaultGoMemLimitShare
// See: https://go.dev/doc/gc-guide#Suggested_uses
// Do take advantage of the memory limit
// For web-apps good rule of thumb: leave 5-10% headroom to account for memory sources the Go runtime is unaware of
//
// Erigon has such resources:
// - mdbx dirty_space (C-owned)
// - PageCache (OS-owned)
// - External CL (OS-owned)
const defaultGoMemLimitShare = 0.7

func goMemLimitIsSet(current int64) bool {
	_, fromEnv := os.LookupEnv("GOMEMLIMIT")
	return fromEnv || current != math.MaxInt64
}

func SetGoMemLimit(logger log.Logger) {
	current := debug.SetMemoryLimit(-1)
	if goMemLimitIsSet(current) {
		logger.Info("[mem] GOMEMLIMIT already set, leaving it alone", "limit", datasize.ByteSize(uint64(current)).HR())
		return
	}

	total := estimate.TotalMemory() // cgroups-aware
	if total == 0 {
		logger.Info("[mem] GOMEMLIMIT unset and available memory is unknown, leaving the heap unbounded")
		return
	}

	limit := int64(float64(total) * defaultGoMemLimitShare)
	debug.SetMemoryLimit(limit)
	logger.Info("[mem] GOMEMLIMIT derived from available memory",
		"available", datasize.ByteSize(total).HR(),
		"GOMEMLIMIT", datasize.ByteSize(uint64(limit)).HR(),
		"share", defaultGoMemLimitShare)
}
