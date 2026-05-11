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
// receipt roots matching block headers. It auto-detects the range
// [Byzantium, rcacheTip] and delegates to CheckRCacheRootAtBlkRange.
//
// Pre-Byzantium blocks need PostState in RCache to derive the correct receipt
// root. When PostState is absent (datadirs built before the fix), the check
// starts at Byzantium instead of block 1.
func CheckReceiptRootIntegrity(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, blockReader services.FullBlockReader, cc *chain.Config, failFast bool, logger log.Logger) (err error) {
	defer func() {
		logger.Info("[integrity] ReceiptRootIntegrity: done", "err", err)
	}()

	txNumsReader := blockReader.TxnumReader()

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rcacheDomainProgress := tx.Debug().DomainProgress(kv.RCacheDomain)
	rcacheTip, _, _ := txNumsReader.FindBlockNum(ctx, tx, rcacheDomainProgress)

	if err := ValidateDomainProgress(ctx, db, kv.RCacheDomain, txNumsReader); err != nil {
		return err
	}
	tx.Rollback()

	return CheckRCacheRootAtBlkRange(ctx, sc, db, blockReader, cc, 1, rcacheTip+1, failFast, logger)
}

// CheckRCacheRootAtBlk verifies the receipt root for a single block by
// reconstructing receipts from RCache and comparing against the block header.
func CheckRCacheRootAtBlk(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, cc *chain.Config, blockNum uint64, failFast bool, logger log.Logger) error {
	if cc.ByzantiumBlock != nil && blockNum < *cc.ByzantiumBlock {
		if !rcacheHasPostStateForPreByzantium(ctx, db, blockReader, blockNum, *cc.ByzantiumBlock) {
			logger.Warn("[integrity] check-rcache-root-at-blk: skipping pre-Byzantium block (no PostState in RCache)",
				"block", blockNum, "byzantium", *cc.ByzantiumBlock)
			return nil
		}
	}
	return checkRCacheRootAtBlkChunk(ctx, blockNum, blockNum, db, blockReader, failFast)
}

// CheckRCacheRootAtBlkRange verifies receipt roots over [from, to) using
// sampling. Pre-Byzantium blocks are skipped when RCache does not contain
// PostState; probes the first available pre-Byzantium receipt to decide.
func CheckRCacheRootAtBlkRange(ctx context.Context, sc SamplerCfg, db kv.TemporalRoDB, blockReader services.FullBlockReader, cc *chain.Config, from, to uint64, failFast bool, logger log.Logger) error {
	if from >= to {
		logger.Info("[integrity] check-rcache-root-at-blk-range: empty range, skipping", "from", from, "to", to)
		return nil
	}

	if cc.ByzantiumBlock != nil && from < *cc.ByzantiumBlock {
		if !rcacheHasPostStateForPreByzantium(ctx, db, blockReader, from, *cc.ByzantiumBlock) {
			byzantium := *cc.ByzantiumBlock
			logger.Warn("[integrity] check-rcache-root-at-blk-range: clamping from to Byzantium (pre-Byzantium blocks have no PostState in RCache)",
				"from", from, "byzantium", byzantium)
			from = byzantium
			if from >= to {
				logger.Info("[integrity] check-rcache-root-at-blk-range: range entirely pre-Byzantium, skipping", "to", to, "byzantium", byzantium)
				return nil
			}
		}
	}
	logger.Info("[integrity] check-rcache-root-at-blk-range starting", "from", from, "to", to)
	return parallelChunkCheck(ctx, sc.NewSampler(), from, to-1, db, blockReader, failFast, string(ReceiptRootIntegrity), checkRCacheRootAtBlkChunk)
}

// checkRCacheRootAtBlkChunk verifies receipt roots for an inclusive range of
// blocks. It opens a single ReceiptCacheV2Stream covering [fromBlock, toBlock]
// and walks blocks in lockstep with the stream's txNum cursor, so we avoid one
// stream + one Min query per block.
func checkRCacheRootAtBlkChunk(ctx context.Context, fromBlock, toBlock uint64, db kv.TemporalRoDB, blockReader services.FullBlockReader, failFast bool) (err error) {
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
		return fmt.Errorf("check-rcache-root-at-blk: failed to get minTxNum for block %d: %w", fromBlock, err)
	}
	toTxNum, err := txNumsReader.Max(ctx, tx, toBlock)
	if err != nil {
		return fmt.Errorf("check-rcache-root-at-blk: failed to get maxTxNum for block %d: %w", toBlock, err)
	}

	it, err := rawdb.ReceiptCacheV2Stream(tx, fromTxNum, toTxNum)
	if err != nil {
		return fmt.Errorf("check-rcache-root-at-blk: failed to stream receipts for blocks [%d,%d]: %w", fromBlock, toBlock, err)
	}
	defer it.Close()

	blockNum := fromBlock
	curMax, err := txNumsReader.Max(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	header, err := blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return fmt.Errorf("check-rcache-root-at-blk: failed to get header for block %d: %w", blockNum, err)
	}
	if header == nil {
		return fmt.Errorf("check-rcache-root-at-blk: missing header for block %d", blockNum)
	}
	var receipts types.Receipts

	verifyAndAdvance := func() error {
		computedRoot := types.DeriveSha(receipts)
		if computedRoot != header.ReceiptHash {
			mismatch := fmt.Errorf("%w: check-rcache-root-at-blk: receipt root mismatch at block %d: computed=%s, header=%s",
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
			return fmt.Errorf("check-rcache-root-at-blk: failed to get header for block %d: %w", blockNum, err)
		}
		if header == nil {
			return fmt.Errorf("check-rcache-root-at-blk: missing header for block %d", blockNum)
		}
		return nil
	}

	for it.HasNext() {
		txNum, r, err := it.Next()
		if err != nil {
			return fmt.Errorf("check-rcache-root-at-blk: failed to read receipt: %w", err)
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
	}

	for blockNum <= toBlock {
		if err := verifyAndAdvance(); err != nil {
			return err
		}
	}

	return nil
}

// rcacheHasPostStateForPreByzantium probes a few pre-Byzantium blocks to
// check whether RCache was built with PostState support.
func rcacheHasPostStateForPreByzantium(ctx context.Context, db kv.TemporalRoDB, blockReader services.FullBlockReader, fromBlock, byzantiumBlock uint64) bool {
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return false
	}
	defer tx.Rollback()

	const maxProbes = 10
	probed := 0
	end := min(fromBlock+maxProbes*10, byzantiumBlock)

	txNumsReader := blockReader.TxnumReader()
	for blockNum := fromBlock; blockNum < end; blockNum++ {
		minTxNum, err := txNumsReader.Min(ctx, tx, blockNum)
		if err != nil {
			return false
		}
		// minTxNum is the system-begin tx; receipts start at minTxNum+1.
		receipt, ok, err := rawdb.ReadReceiptCacheV2(tx, rawdb.RCacheV2Query{TxNum: minTxNum + 1, DontCalcBloom: true})
		if err != nil || !ok || receipt == nil {
			continue
		}
		probed++
		if len(receipt.PostState) > 0 {
			return true
		}
		if probed >= maxProbes {
			break
		}
	}
	return false
}
