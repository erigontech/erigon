// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/mclock"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

// InsertStats tracks and reports on block insertion.
type InsertStats struct {
	queued, lastIndex, ignored int
	UsedGas                    uint64
	Processed                  int
	StartTime                  mclock.AbsTime
}

// InsertBodyChain attempts to insert the given batch of block into the
// canonical chain, without executing those blocks
func InsertBodyChain(logPrefix string, ctx context.Context, db ethdb.Database, chain types.Blocks, newCanonical bool) (bool, error) {
	// Sanity check that we have something meaningful to import
	if len(chain) == 0 {
		return true, nil
	}
	// Remove already known canon-blocks
	var (
		block, prev *types.Block
	)
	// Do a sanity check that the provided chain is actually ordered and linked
	for i := 1; i < len(chain); i++ {
		block = chain[i]
		prev = chain[i-1]
		if block.NumberU64() != prev.NumberU64()+1 || block.ParentHash() != prev.Hash() {
			// Chain broke ancestry, log a message (programming error) and skip insertion
			log.Error("Non contiguous block insert", "number", block.Number(), "hash", block.Hash(),
				"parent", block.ParentHash(), "prevnumber", prev.Number(), "prevhash", prev.Hash())

			return true, fmt.Errorf("non contiguous insert: item %d is #%d [%x…], item %d is #%d [%x…] (parent [%x…])", i-1, prev.NumberU64(),
				prev.Hash().Bytes()[:4], i, block.NumberU64(), block.Hash().Bytes()[:4], block.ParentHash().Bytes()[:4])
		}
	}

	return InsertBodies(
		logPrefix,
		ctx,
		chain,
		db,
		newCanonical,
	)
}

// InsertBodies is insertChain with execute=false and ommission of blockchain object
func InsertBodies(
	logPrefix string,
	ctx context.Context,
	chain types.Blocks,
	db ethdb.Database,
	newCanonical bool,
) (bool, error) {
	batch := db.NewBatch()
	defer batch.Rollback()
	stats := InsertStats{StartTime: mclock.Now()}

	var parentNumber = chain[0].NumberU64() - 1
	parentHash := chain[0].ParentHash()

	if parent := rawdb.ReadStorageBodyRLP(batch, parentHash, parentNumber); parent == nil {
		log.Error("chain segment could not be inserted, missing parent", "hash", parentHash)
		return true, fmt.Errorf("chain segment could not be inserted, missing parent %x", parentHash)
	}

	// Iterate over the blocks and insert when the verifier permits
	for _, block := range chain {
		start := time.Now()

		// Calculate the total difficulty of the block
		ptd, err := rawdb.ReadTd(batch, block.ParentHash(), block.NumberU64()-1)
		if err != nil {
			return true, err
		}

		if ptd == nil {
			return true, consensus.ErrUnknownAncestor
		}

		// Irrelevant of the canonical status, write the block itself to the database
		if common.IsCanceled(ctx) {
			return true, ctx.Err()
		}

		err = rawdb.WriteBodyDeprecated(batch, block.Hash(), block.NumberU64(), block.Body())
		if err != nil {
			return true, err
		}

		log.Debug("Inserted new block", "number", block.Number(), "hash", block.Hash(),
			"uncles", len(block.Uncles()), "txs", len(block.Transactions()), "gas", block.GasUsed(),
			"elapsed", common.PrettyDuration(time.Since(start)),
			"root", block.Root())
	}
	stats.Processed = len(chain)
	stats.Report(logPrefix, chain, len(chain)-1, true)
	if newCanonical {
		rawdb.WriteHeadBlockHash(batch, chain[len(chain)-1].Hash())
	}
	if err := batch.Commit(); err != nil {
		return true, fmt.Errorf("commit inserting bodies: %w", err)
	}
	return false, nil
}
