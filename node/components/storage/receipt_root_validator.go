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

	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// ReceiptRootValidator verifies DeriveSha(receipts) == header.ReceiptHash
// for each block in a retired receipt-domain step. Delegates per-block
// work to integrity.ReceiptRootIntegrityRange so this validator and
// `erigon seg integrity` share the same logic. Pre-Byzantium blocks
// are skipped (PostState encoding doesn't reproduce).
type ReceiptRootValidator struct {
	DB          kv.TemporalRoDB
	BlockReader services.FullBlockReader
	ChainConfig *chain.Config
}

// Name implements validation.StepValidator.
func (ReceiptRootValidator) Name() string { return "receipts_root_per_block" }

// ValidateStep implements validation.StepValidator.
func (v ReceiptRootValidator) ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error {
	if len(files) == 0 || files[0].Domain != snapshot.DomainReceipt {
		return nil
	}
	if v.DB == nil {
		return fmt.Errorf("ReceiptRootValidator: nil DB")
	}
	if v.BlockReader == nil {
		return fmt.Errorf("ReceiptRootValidator: nil BlockReader")
	}
	if v.ChainConfig == nil {
		return fmt.Errorf("ReceiptRootValidator: nil ChainConfig")
	}

	fromStep := files[0].FromStep
	toStep := files[0].ToStep
	if toStep == 0 {
		return fmt.Errorf("receipt step has zero ToStep")
	}

	var fromBlock, toBlock uint64
	if err := v.DB.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		stepSize := tx.Debug().StepSize()
		startTxNum := fromStep * stepSize
		endTxNum := toStep * stepSize
		txNumReader := v.BlockReader.TxnumReader()

		fb, ok, err := txNumReader.FindBlockNum(ctx, tx, startTxNum)
		if err != nil || !ok {
			return fmt.Errorf("FindBlockNum(startTxNum=%d): %w (ok=%v)", startTxNum, err, ok)
		}
		tb, ok, err := txNumReader.FindBlockNum(ctx, tx, endTxNum-1)
		if err != nil || !ok {
			return fmt.Errorf("FindBlockNum(endTxNum-1=%d): %w (ok=%v)", endTxNum-1, err, ok)
		}
		fromBlock, toBlock = fb, tb
		return nil
	}); err != nil {
		return err
	}

	// Skip pre-Byzantium blocks (the integrity helper does too).
	if v.ChainConfig.ByzantiumBlock != nil && *v.ChainConfig.ByzantiumBlock > fromBlock {
		fromBlock = *v.ChainConfig.ByzantiumBlock
	}
	if fromBlock > toBlock {
		return nil
	}

	if err := integrity.ReceiptRootIntegrityRange(ctx, fromBlock, toBlock, v.DB, v.BlockReader, true /* failFast */); err != nil {
		return fmt.Errorf("receipt step [%d, %d) blocks [%d, %d]: %w", fromStep, toStep, fromBlock, toBlock, err)
	}
	return nil
}
