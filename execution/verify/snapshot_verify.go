package verify

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// NewSnapshotVerifier returns a function that verifies newly built state
// snapshot files by re-executing all blocks in [fromTxNum, toTxNum) and
// checking that each block produces the expected receipts, gas usage, and
// bloom filters. If any block fails, the returned error includes the block
// number.
//
// The db parameter is a temporary TemporalRoDB backed by a fresh aggregator
// that sees all files on disk (including newly built ones not yet activated).
func NewSnapshotVerifier(
	blockReader services.FullBlockReader,
	chainConfig *chain.Config,
	engine rules.Engine,
	logger log.Logger,
) func(ctx context.Context, db kv.TemporalRoDB, fromTxNum, toTxNum uint64) error {
	return func(ctx context.Context, db kv.TemporalRoDB, fromTxNum, toTxNum uint64) error {
		txNumsReader := blockReader.TxnumReader()

		ttx, err := db.BeginTemporalRo(ctx)
		if err != nil {
			return fmt.Errorf("snapshot verify: begin temporal ro: %w", err)
		}
		defer ttx.Rollback()

		// Convert txNum range to block range.
		fromBlock, ok, err := txNumsReader.FindBlockNum(ctx, ttx, fromTxNum)
		if err != nil {
			return fmt.Errorf("snapshot verify: find block for txNum %d: %w", fromTxNum, err)
		}
		if !ok {
			return fmt.Errorf("snapshot verify: no block found for txNum %d", fromTxNum)
		}
		toBlock, ok, err := txNumsReader.FindBlockNum(ctx, ttx, toTxNum)
		if err != nil {
			return fmt.Errorf("snapshot verify: find block for txNum %d: %w", toTxNum, err)
		}
		if !ok {
			// toTxNum is exclusive; use the last known block
			toBlock, _, err = txNumsReader.FindBlockNum(ctx, ttx, toTxNum-1)
			if err != nil {
				return fmt.Errorf("snapshot verify: find block for txNum %d: %w", toTxNum-1, err)
			}
		}

		chainReader := consensuschain.NewReader(chainConfig, ttx, blockReader, logger)

		logger.Info("[snapshots] verifying blocks", "from", fromBlock, "to", toBlock,
			"fromTxNum", fromTxNum, "toTxNum", toTxNum)

		for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("snapshot verify: cancelled at block %d: %w", blockNum, err)
			}

			// Skip genesis block — nothing to execute.
			if blockNum == 0 {
				continue
			}

			hash, ok, err := blockReader.CanonicalHash(ctx, ttx, blockNum)
			if err != nil {
				return fmt.Errorf("snapshot verify: canonical hash for block %d: %w", blockNum, err)
			}
			if !ok {
				return fmt.Errorf("snapshot verify: no canonical hash for block %d", blockNum)
			}

			block, _, err := blockReader.BlockWithSenders(ctx, ttx, hash, blockNum)
			if err != nil {
				return fmt.Errorf("snapshot verify: read block %d: %w", blockNum, err)
			}
			if block == nil {
				return fmt.Errorf("snapshot verify: block %d not found", blockNum)
			}

			// Read historical state as of the block's first transaction.
			blockStartTxNum, err := txNumsReader.Min(ctx, ttx, blockNum)
			if err != nil {
				return fmt.Errorf("snapshot verify: min txNum for block %d: %w", blockNum, err)
			}

			stateReader := state.NewHistoryReaderV3(ttx, blockStartTxNum)
			stateWriter := state.NewNoopWriter()

			blockHashFunc := protocol.GetHashFn(block.Header(), func(hash common.Hash, number uint64) (*types.Header, error) {
				return blockReader.Header(ctx, ttx, hash, number)
			})

			vmConfig := vm.Config{}
			_, err = protocol.ExecuteBlockEphemerally(
				chainConfig, &vmConfig, blockHashFunc,
				engine, block,
				stateReader, stateWriter,
				chainReader, nil, logger,
			)
			if err != nil {
				return fmt.Errorf("snapshot verify: block %d execution failed: %w", blockNum, err)
			}
		}

		logger.Info("[snapshots] verification complete", "blocks", toBlock-fromBlock+1)
		return nil
	}
}
