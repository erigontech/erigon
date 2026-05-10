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

package storage

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// TxRootValidator verifies DeriveSha(block.Transactions) == header.TxHash
// for each block in each retired transactions.seg.
type TxRootValidator struct {
	DB          kv.RoDB
	BlockReader services.FullBlockReader
}

// Name implements validation.StepValidator.
func (TxRootValidator) Name() string { return "transactions_root_per_block" }

// ValidateStep implements validation.StepValidator.
func (v TxRootValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 {
		return nil
	}
	type txSeg struct{ from, to uint64 }
	var segs []txSeg
	for _, f := range files {
		if f.Domain != "" {
			continue
		}
		typ, from, to, ok := snaptype.ParseRange(f.Name)
		if !ok || typ != statecfg.Transactions {
			continue
		}
		segs = append(segs, txSeg{from, to})
	}
	if len(segs) == 0 {
		return nil
	}
	if v.DB == nil {
		return fmt.Errorf("TxRootValidator: nil DB")
	}
	if v.BlockReader == nil {
		return fmt.Errorf("TxRootValidator: nil BlockReader")
	}

	tx, err := v.DB.BeginRo(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, seg := range segs {
		for n := seg.from; n < seg.to; n++ {
			block, err := v.BlockReader.BlockByNumber(ctx, tx, n)
			if err != nil {
				return fmt.Errorf("BlockByNumber(%d): %w", n, err)
			}
			if block == nil {
				return fmt.Errorf("missing block %d in transactions.seg [%d, %d)", n, seg.from, seg.to)
			}
			derived := types.DeriveSha(types.Transactions(block.Transactions()))
			if h := block.Header(); h.TxHash != derived {
				return fmt.Errorf("transactions root mismatch at block %d: header.TxHash=%x, DeriveSha(txs)=%x (transactions.seg [%d, %d))",
					n, h.TxHash, derived, seg.from, seg.to)
			}
		}
	}
	return nil
}
