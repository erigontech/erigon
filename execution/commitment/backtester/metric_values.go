// Copyright 2025 The Erigon Authors
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

package backtester

import "github.com/erigontech/erigon/execution/commitment"

type MetricValues struct {
	commitment.MetricValues
	BatchId uint64
}

type crossPageAggMetrics struct {
	top                  *slowestBatchesHeap
	branchJumpdestCounts *[128][16]uint64
	branchKeyLenCounts   *[128]uint64
}

type slowestBatchesHeap []MetricValues

func (h *slowestBatchesHeap) Len() int {
	return len(*h)
}

func (h *slowestBatchesHeap) Less(i, j int) bool {
	return (*h)[i].SpentProcessing < (*h)[j].SpentProcessing
}

func (h *slowestBatchesHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *slowestBatchesHeap) Push(x any) {
	*h = append(*h, x.(MetricValues))
}

func (h *slowestBatchesHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
