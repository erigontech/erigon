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

// goMemLimitShare leaves the rest of the budget to what GOMEMLIMIT cannot see:
// MDBX's write buffer, other cgo allocations, and reclaimable page cache.
const goMemLimitShare = 0.7

// goMemLimitInForce reports whether the heap ceiling is already decided. The
// runtime reports GOMEMLIMIT=off exactly like an unset variable, so only the
// environment tells that choice apart.
func goMemLimitInForce(current int64) bool {
	_, fromEnv := os.LookupEnv("GOMEMLIMIT")
	return fromEnv || current != math.MaxInt64
}

// SetGoMemLimit gives the Go heap a ceiling, unless a limit is already in force.
// Without one, GOGC alone decides when to collect, so a process whose live heap
// is over half its budget targets a number it is not allowed to reach and is
// killed before the collection that would have saved it.
//
// TotalMemory is the budget: system RAM, or the cgroup limit when that is lower.
// It has to be read before the limit is installed, or it would fold our own
// ceiling back into the number that sizes the caches.
func SetGoMemLimit(logger log.Logger) {
	current := debug.SetMemoryLimit(-1)
	if goMemLimitInForce(current) {
		logger.Info("[mem] GOMEMLIMIT already set, leaving it alone", "limit", datasize.ByteSize(uint64(current)).HR())
		return
	}

	total := estimate.TotalMemory()
	if total == 0 {
		logger.Info("[mem] GOMEMLIMIT unset and available memory is unknown, leaving the heap unbounded")
		return
	}

	limit := int64(float64(total) * goMemLimitShare)
	debug.SetMemoryLimit(limit)
	logger.Info("[mem] GOMEMLIMIT derived from available memory",
		"available", datasize.ByteSize(total).HR(),
		"GOMEMLIMIT", datasize.ByteSize(uint64(limit)).HR(),
		"share", goMemLimitShare)
}
