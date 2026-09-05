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

// defaultGoMemLimitShare leaves headroom for memory the Go runtime does not
// account for: mdbx dirty pages (~1G, C-owned), an external CL, and the OS page
// cache Erigon leans on heavily. The gc-guide's 5-10% rule of thumb assumes a
// web app with none of those.
// See: https://go.dev/doc/gc-guide#Suggested_uses
const defaultGoMemLimitShare = 0.8

// goMemLimitIsSet reports an operator's choice. An empty value is not one: the
// runtime reads it exactly like "off", and it is what an unrendered k8s or
// compose template leaves behind.
func goMemLimitIsSet(current int64) bool {
	if v, ok := os.LookupEnv("GOMEMLIMIT"); ok && v != "" {
		return true
	}
	return current != math.MaxInt64
}

func goMemLimitFor(total uint64) (int64, bool) {
	if total == 0 {
		return 0, false
	}
	return int64(float64(total) * defaultGoMemLimitShare), true
}

func SetGoMemLimit(logger log.Logger) {
	current := debug.SetMemoryLimit(-1)
	if goMemLimitIsSet(current) {
		if current == math.MaxInt64 {
			logger.Info("[mem] GOMEMLIMIT is off, leaving the heap unbounded")
		} else {
			logger.Info("[mem] GOMEMLIMIT already set, leaving it alone", "limit", datasize.ByteSize(uint64(current)).HR())
		}
		return
	}

	total := estimate.TotalMemory() // cgroups-aware
	limit, ok := goMemLimitFor(total)
	if !ok {
		logger.Info("[mem] GOMEMLIMIT unset and available memory is unknown, leaving the heap unbounded")
		return
	}
	debug.SetMemoryLimit(limit)
	logger.Info("[mem] GOMEMLIMIT derived from available memory",
		"available", datasize.ByteSize(total).HR(),
		"GOMEMLIMIT", datasize.ByteSize(uint64(limit)).HR(),
		"share", defaultGoMemLimitShare)
}
