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

// goMemLimitShare leaves the rest of the cgroup to what GOMEMLIMIT cannot see:
// MDBX's write buffer, other cgo allocations, and reclaimable page cache.
const goMemLimitShare = 0.7

// goMemLimitInForce reports whether the heap ceiling is already decided. The
// runtime reports GOMEMLIMIT=off exactly like an unset variable, so only the
// environment tells that choice apart.
func goMemLimitInForce(current int64) bool {
	_, fromEnv := os.LookupEnv("GOMEMLIMIT")
	return fromEnv || current != math.MaxInt64
}

// SetGoMemLimit gives the Go heap a ceiling derived from the cgroup this process
// runs in, unless a limit is already in force.
func SetGoMemLimit(logger log.Logger) {
	current := debug.SetMemoryLimit(-1)
	if goMemLimitInForce(current) {
		logger.Info("[mem] GOMEMLIMIT already set, leaving it alone", "limit", datasize.ByteSize(uint64(current)).HR())
		return
	}

	cgroup, system := estimate.CgroupsMemoryLimit(), estimate.SystemMemory()
	limit := derivedGoMemLimit(cgroup, system)
	if limit == 0 {
		logger.Info("[mem] GOMEMLIMIT unset and no cgroup limit constrains this process",
			"system", datasize.ByteSize(system).HR())
		return
	}

	debug.SetMemoryLimit(limit)
	logger.Info("[mem] GOMEMLIMIT derived from cgroup limit",
		"cgroup", datasize.ByteSize(cgroup).HR(),
		"GOMEMLIMIT", datasize.ByteSize(uint64(limit)).HR(),
		"share", goMemLimitShare)
}

// derivedGoMemLimit returns the heap ceiling for a cgroup limit, or 0 when the
// cgroup does not constrain this process. system is physical memory, not
// TotalMemory: the latter folds the cgroup in, so it can never show the cgroup
// as the smaller of the two, and it is what rules out both unlimited sentinels.
func derivedGoMemLimit(cgroup, system uint64) int64 {
	if cgroup == 0 || cgroup >= math.MaxInt64 || (system > 0 && cgroup >= system) {
		return 0
	}
	return int64(float64(cgroup) * goMemLimitShare)
}
