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

package tiering

import "github.com/erigontech/erigon/db/kv"

// ColdBoundary returns the first block of the hot window of the last lookback blocks before head.
func ColdBoundary(head, lookback uint64) uint64 {
	if head < lookback {
		return 0
	}
	return head - lookback
}

// ColdFiles returns the files that hold only txnums below boundaryTxNum.
func ColdFiles(files kv.VisibleFiles, boundaryTxNum uint64) kv.VisibleFiles {
	var cold kv.VisibleFiles
	for _, f := range files {
		if f.EndRootNum() <= boundaryTxNum {
			cold = append(cold, f)
		}
	}
	return cold
}
