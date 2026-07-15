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

package merkle_tree

// TEMPORARY INSTRUMENTATION - not for merge.
// Records the real distribution of HashTreeRoot schema lengths so that
// maxStackLeaves can be picked from data instead of a source scan.

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
)

const (
	htrStatsLogEvery = 40_000
	htrHistMax       = 64 // leaf counts >= this land in the overflow bucket
)

var (
	// htrLeavesHist is indexed by len(schema); the last slot counts everything above.
	htrLeavesHist [htrHistMax + 1]atomic.Uint64
	htrStackHits  atomic.Uint64
	htrHeapMisses atomic.Uint64
	htrCalls      atomic.Uint64
)

func htrObserve(nLeaves int, stackHit bool) {
	if stackHit {
		htrStackHits.Add(1)
	} else {
		htrHeapMisses.Add(1)
	}
	if nLeaves < 0 {
		nLeaves = 0
	}
	if nLeaves > htrHistMax {
		nLeaves = htrHistMax
	}
	htrLeavesHist[nLeaves].Add(1)
	if n := htrCalls.Add(1); n%htrStatsLogEvery == 0 {
		htrLogStats(n)
	}
}

// htrCoverage reports the share of calls whose buffer would fit a stack array
// of maxLeaves*32 bytes, given the buffer is NextPowerOfTwo(n*32) bytes.
func htrCoverage(counts []uint64, total uint64, maxLeaves uint64) float64 {
	var fits uint64
	for n, c := range counts {
		if n > 0 && NextPowerOfTwo(uint64(n)*32) <= maxLeaves*32 {
			fits += c
		}
	}
	return 100 * float64(fits) / float64(total)
}

func htrLogStats(calls uint64) {
	counts := make([]uint64, len(htrLeavesHist))
	var total uint64
	for i := range htrLeavesHist {
		counts[i] = htrLeavesHist[i].Load()
		total += counts[i]
	}
	if total == 0 {
		return
	}

	var dist strings.Builder
	for n, c := range counts {
		if c == 0 {
			continue
		}
		if dist.Len() > 0 {
			dist.WriteString(" ")
		}
		label := fmt.Sprintf("%d", n)
		if n == htrHistMax {
			label = fmt.Sprintf(">=%d", htrHistMax)
		}
		fmt.Fprintf(&dist, "%s:%d(%.2f%%)", label, c, 100*float64(c)/float64(total))
	}

	var cov strings.Builder
	for _, k := range []uint64{4, 8, 16, 32} {
		if cov.Len() > 0 {
			cov.WriteString(" ")
		}
		fmt.Fprintf(&cov, "%d(%dB):%.3f%%", k, k*32, htrCoverage(counts, total, k))
	}

	log.Warn("[htr-stats] HashTreeRoot schema length distribution",
		"calls", calls,
		"maxStackLeaves", maxStackLeaves,
		"stackHit", htrStackHits.Load(),
		"heapMiss", htrHeapMisses.Load(),
		"leaves", dist.String(),
		"coverageIfStack", cov.String(),
	)
}
