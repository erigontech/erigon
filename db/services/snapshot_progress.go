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

package services

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
)

// MaxCollatableTxNum returns the upper bound txNum that state collation may
// target without running ahead of block snapshot files. Used by the
// integrity check at db/integrity/commitment_progress_check.go to detect
// state-ahead-of-blocks drift. The Aggregator enforces the same cap
// internally via Aggregator.SetFrozenBlocksProvider.
func MaxCollatableTxNum(ctx context.Context, tx kv.Tx, blockReader FullBlockReader) (uint64, error) {
	return blockReader.TxnumReader().Max(ctx, tx, blockReader.FrozenBlocks())
}
