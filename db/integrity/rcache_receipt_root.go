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
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// CheckReceiptRootIntegrity verifies that receipts from RCache domain produce
// receipt roots matching block headers for a range of blocks with sampling.
//
// Pre-Byzantium blocks are skipped: their consensus receipt encoding includes
// the 32-byte intermediate state root (PostState), which Erigon does not
// compute or persist at execution time, so RCache cannot reconstruct the
// canonical receipt root for those blocks.
func CheckReceiptRootIntegrity(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, blockReader services.FullBlockReader, cc *chain.Config, failFast bool) (err error) {
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
	if cc.ByzantiumBlock != nil && *cc.ByzantiumBlock > fromBlock {
		fromBlock = *cc.ByzantiumBlock
	}
	toBlock, _, _ := txNumsReader.FindBlockNum(ctx, tx, rcacheDomainProgress)

	if err := ValidateDomainProgress(ctx, db, kv.RCacheDomain, txNumsReader); err != nil {
		return err
	}

	if fromBlock > toBlock {
		log.Info("[integrity] ReceiptRootIntegrity skipped (no post-Byzantium blocks in RCache)", "byzantium", fromBlock, "rcacheTip", toBlock)
		return nil
	}
	log.Info("[integrity] ReceiptRootIntegrity starting", "fromBlock", fromBlock, "toBlock", toBlock)

	return parallelChunkCheck(ctx, sc.NewSampler(), fromBlock, toBlock, db, blockReader, failFast, string(ReceiptRootIntegrity), ReceiptRootIntegrityRange)
}

// ReceiptRootIntegrityRange verifies receipt roots for a range of blocks.
// It opens a single ReceiptCacheV2Stream covering [fromBlock, toBlock] and
// walks blocks in lockstep with the stream's txNum cursor, so we avoid one
// stream + one Min query per block.
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

	fromTxNum, err := txNumsReader.Min(ctx, tx, fromBlock)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to get minTxNum for block %d: %w", fromBlock, err)
	}
	toTxNum, err := txNumsReader.Max(ctx, tx, toBlock)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to get maxTxNum for block %d: %w", toBlock, err)
	}

	it, err := rawdb.ReceiptCacheV2Stream(tx, fromTxNum, toTxNum)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to stream receipts for blocks [%d,%d]: %w", fromBlock, toBlock, err)
	}
	defer it.Close()

	blockNum := fromBlock
	curMax, err := txNumsReader.Max(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	header, err := blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("ReceiptRootIntegrity: failed to get header for block %d: %w", blockNum, err)
	}
	if header == nil {
		return fmt.Errorf("ReceiptRootIntegrity: missing header for block %d", blockNum)
	}
	var receipts types.Receipts

	verifyAndAdvance := func() error {
		computedRoot := types.DeriveSha(receipts)
		if computedRoot != header.ReceiptHash {
			mismatch := fmt.Errorf("%w: ReceiptRootIntegrity: receipt root mismatch at block %d: computed=%s, header=%s",
				ErrIntegrity, blockNum, computedRoot, header.ReceiptHash)
			if failFast {
				return mismatch
			}
			log.Error(mismatch.Error())
		}
		receipts = receipts[:0]
		blockNum++
		if blockNum > toBlock {
			return nil
		}
		curMax, err = txNumsReader.Max(ctx, tx, blockNum)
		if err != nil {
			return err
		}
		header, err = blockReader.HeaderByNumber(ctx, tx, blockNum)
		if err != nil {
			return fmt.Errorf("ReceiptRootIntegrity: failed to get header for block %d: %w", blockNum, err)
		}
		if header == nil {
			return fmt.Errorf("ReceiptRootIntegrity: missing header for block %d", blockNum)
		}
		return nil
	}

	for it.HasNext() {
		txNum, r, err := it.Next()
		if err != nil {
			return fmt.Errorf("ReceiptRootIntegrity: failed to read receipt: %w", err)
		}

		for txNum > curMax && blockNum <= toBlock {
			if err := verifyAndAdvance(); err != nil {
				return err
			}
		}

		if r != nil {
			r.Bloom = types.CreateBloom(types.Receipts{r})
			receipts = append(receipts, r)
		}

		if txNum%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	for blockNum <= toBlock {
		if err := verifyAndAdvance(); err != nil {
			return err
		}
	}

	return nil
}
