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

package bscsync

import (
	"context"

	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/types"
)

// computeResume returns the last persisted block and its header, from which the
// driver resumes. Effective head = max(FrozenBlocks, CurrentHeader.Number); the
// header is nil only when nothing beyond an unavailable genesis is present.
func computeResume(ctx context.Context, chainRW chainreader.ChainReaderWriterEth1) (uint64, *types.Header) {
	frozen, _ := chainRW.FrozenBlocks(ctx)
	head := chainRW.CurrentHeader(ctx)
	var headNum uint64
	if head != nil {
		headNum = head.Number.Uint64()
	}
	if frozen > headNum {
		return frozen, head
	}
	return headNum, head
}
