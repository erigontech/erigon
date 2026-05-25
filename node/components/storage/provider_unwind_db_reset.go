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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// unwindDBPastBlock resets all writable-DB content covering blocks
// past toBlock. Mode-B sub-op #2. Runs after the snapshot-trim sub-op
// has shortened the on-disk file set to [0, toBlock]. The pairing is
// load-bearing: snapshot-trim alone would leave a wedge of orphan
// TxNums / canonical-hashes / stage-progress / diffsets pointing at
// blocks that no longer exist in snapshots.
//
// The resulting DB is observably equivalent to a freshly-started node
// that has just processed the frozen blocks up to toBlock — see the
// "Equivalence to the cold-start 'processed frozen blocks' state"
// section of docs/plans/20260525-admin-sethead-unwind-design.md.
//
// Resets performed:
//
//   - TxNums truncated to [0, lastTxNum(toBlock)].
//   - Canonical hashes for > toBlock removed.
//   - Headers / Bodies / BlockHashes / Execution stage progress set
//     to toBlock.
//   - HeadBlockHash / HeadHeaderHash / ForkchoiceHead set to
//     toBlock's canonical hash.
//   - ChangeSets3 entries for > toBlock deleted (mode-B mandates a
//     clean post-state; remaining diffsets above toBlock would be
//     orphaned references to blocks that no longer have canonical
//     hashes).
//
// Does NOT modify the commitment domain — sub-op #3
// (ensureCommitmentAtBlock) handles that with its own validation /
// recompute logic.
func (p *Provider) unwindDBPastBlock(ctx context.Context, tx kv.TemporalRwTx, toBlock uint64) error {
	if p.BlockReader == nil {
		return fmt.Errorf("Provider.BlockReader is nil — cannot resolve canonical hash at toBlock")
	}
	targetHash, ok, err := p.BlockReader.CanonicalHash(ctx, tx, toBlock)
	if err != nil {
		return fmt.Errorf("resolve canonical hash at toBlock %d: %w", toBlock, err)
	}
	if !ok || targetHash == (common.Hash{}) {
		return fmt.Errorf("no canonical hash recorded at toBlock %d — cannot mode-B unwind to an off-chain target", toBlock)
	}

	if err := rawdbv3.TxNums.Truncate(tx, toBlock+1); err != nil {
		return fmt.Errorf("TxNums.Truncate(%d): %w", toBlock+1, err)
	}

	if err := rawdb.TruncateCanonicalHash(tx, toBlock+1, false); err != nil {
		return fmt.Errorf("TruncateCanonicalHash(%d): %w", toBlock+1, err)
	}

	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.BlockHashes, stages.Execution} {
		if err := stages.SaveStageProgress(tx, stage, toBlock); err != nil {
			return fmt.Errorf("SaveStageProgress(%s, %d): %w", stage, toBlock, err)
		}
	}

	rawdb.WriteHeadBlockHash(tx, targetHash)
	if err := rawdb.WriteHeadHeaderHash(tx, targetHash); err != nil {
		return fmt.Errorf("WriteHeadHeaderHash(%x): %w", targetHash, err)
	}
	rawdb.WriteForkchoiceHead(tx, targetHash)

	if err := deleteChangeSetsPastBlock(tx, toBlock); err != nil {
		return fmt.Errorf("deleteChangeSetsPastBlock(%d): %w", toBlock, err)
	}

	return nil
}

// deleteChangeSetsPastBlock cursor-iterates kv.ChangeSets3 and
// removes every entry whose 8-byte block-number prefix is > toBlock.
// ChangeSets3 has no public "truncate past block" helper today; we
// keep the cursor walk local to mode-B so the broader changeset
// package stays untouched ahead of the execution-component cutover
// that owns diffset-retention policy.
func deleteChangeSetsPastBlock(tx kv.RwTx, toBlock uint64) error {
	c, err := tx.RwCursor(kv.ChangeSets3)
	if err != nil {
		return err
	}
	defer c.Close()

	// Keys in ChangeSets3 are at least 8 bytes (the encoded block
	// number prefix). Seek to the first entry at toBlock+1 — anything
	// from that point onward is past the new tip.
	seekKey := dbutils.EncodeBlockNumber(toBlock + 1)
	for k, _, err := c.Seek(seekKey); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil
}
