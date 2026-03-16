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

package integrity

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/types"
)

// CheckReceiptRootIntegrity verifies that receipts from RCache domain produce
// receipt roots matching block headers for a range of blocks with sampling.
func CheckReceiptRootIntegrity(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	defer func() {
		log.Info("[integrity] ReceiptRootIntegrity: done", "err", err)
	}()

	txNumsReader := blockReader.TxnumReader()

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rcacheDomainProgress := tx.Debug().DomainProgress(kv.RCacheDomain)
	fromBlock := uint64(1)
	toBlock, _, _ := txNumsReader.FindBlockNum(ctx, tx, rcacheDomainProgress)

	if err := ValidateDomainProgress(ctx, db, kv.RCacheDomain, txNumsReader); err != nil {
		return err
	}

	log.Info("[integrity] ReceiptRootIntegrity starting", "fromBlock", fromBlock, "toBlock", toBlock)

	return parallelChunkCheck(ctx, sc.NewSampler(), fromBlock, toBlock, db, blockReader, failFast, string(ReceiptRootIntegrity), ReceiptRootIntegrityRange)
}

// ReceiptRootIntegrityRange verifies receipt roots for a range of blocks.
func ReceiptRootIntegrityRange(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
	if fromBlock > toBlock {
		panic(fmt.Sprintf("fromBlock(%d) > toBlock(%d)", fromBlock, toBlock))
	}

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txNumsReader := blockReader.TxnumReader()

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := checkReceiptRootAtBlock(ctx, tx, blockReader, txNumsReader, blockNum); err != nil {
			if failFast {
				return err
			}
			log.Error(err.Error())
		}
	}

	return nil
}

// checkReceiptRootAtBlock verifies that receipts from RCache produce a receipt root
// matching the block header's ReceiptHash for a single block.
func checkReceiptRootAtBlock(ctx context.Context, tx kv.TemporalTx, blockReader services.FullBlockReader, txNumsReader rawdbv3.TxNumsReader, blockNum uint64) error {
	// Get block header
	header, err := blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to get header for block %d: %w", blockNum, err)
	}
	if header == nil {
		return fmt.Errorf("ReceiptRootIntegrity: missing header for block %d", blockNum)
	}

	// Get txNum range for the block
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to get minTxNum for block %d: %w", blockNum, err)
	}
	maxTxNum, err := txNumsReader.Max(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to get maxTxNum for block %d: %w", blockNum, err)
	}

	// Read all receipts for the block from RCache
	it, err := rawdb.ReceiptCacheV2Stream(tx, minTxNum, maxTxNum)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to stream receipts for block %d: %w", blockNum, err)
	}
	defer it.Close()

	var receipts types.Receipts
	for it.HasNext() {
		_, r, err := it.Next()
		if err != nil {
			return fmt.Errorf("ReceiptRootIntegrity: failed to read receipt for block %d: %w", blockNum, err)
		}
		if r != nil {
			receipts = append(receipts, r)
		}
	}

	// Compute receipt root
	computedRoot := types.DeriveSha(receipts)

	// Compare with header's ReceiptHash
	if computedRoot != header.ReceiptHash {
		return fmt.Errorf("%w: ReceiptRootIntegrity: receipt root mismatch at block %d: computed=%s, header=%s",
			ErrIntegrity, blockNum, computedRoot, header.ReceiptHash)
	}

	return nil
}
