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
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// unwindDBPastBlock resets all writable-DB content covering blocks
// past toBlock. Mode-B sub-op #2. Runs after the snapshot-trim sub-op
// has shortened the on-disk file set to [0, toBlock]. The pairing is
// load-bearing: snapshot-trim alone would leave a wedge of orphan DB
// entries pointing at blocks that no longer exist in snapshots.
//
// The resulting DB is observably equivalent to a freshly-started node
// that has just processed the frozen blocks up to toBlock — see the
// "Equivalence to the cold-start 'processed frozen blocks' state"
// section of docs/plans/20260525-admin-sethead-unwind-design.md.
// "Empty MDBX past toBlock; snapshots populated with state" is the
// invariant.
//
// Resets performed:
//
//   - TxNums truncated to [0, lastTxNum(toBlock)].
//   - Canonical hashes for > toBlock removed.
//   - Block-data tables — Headers, BlockBody, BlockTransaction (EthTx),
//     TxSender, BlockAccessList, HeadersTotalDifficulty — truncated
//     past toBlock. The CL can have pushed forward NewPayloads while
//     mode B was preparing the unwind; those headers/bodies land in
//     the writable DB independently of stage progress and would
//     otherwise survive as orphans past the new tip. The
//     firstNonGenesisCheck in stage_snapshots reads kv.Headers
//     directly — leaving orphans there wedges the next startup with
//     "some blocks are not in snapshots and not in db".
//   - HeaderNumber (hash → number) orphan entries for the truncated
//     blocks are removed. TruncateBlocks does not touch HeaderNumber;
//     we walk kv.Headers' deleted-range first to collect the hashes.
//   - Headers / Bodies / BlockHashes / Execution stage progress set
//     to toBlock.
//   - HeadBlockHash / HeadHeaderHash / ForkchoiceHead set to
//     toBlock's canonical hash.
//   - ChangeSets3 entries for > toBlock deleted.
//   - Writable-domain MDBX shadow (accounts / storage / code /
//     commitment + standalone IIs) wiped past lastTxNum via the
//     aggregator's WipeWritableShadowPast primitive. The cold-start
//     equivalence requires that the next forward execution sees the
//     snapshot files as the authoritative state — any DB-shadow
//     override would defeat it.
//
// After this runs, sub-op #3 (ensureCommitmentAtBlock) is a pure
// verification: LatestCommitmentState should resolve to the
// commitment file's entry at toBlock's step boundary because no
// DB-shadow entry shadows it.
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

	if err := deleteHeaderNumbersPastBlock(tx, toBlock); err != nil {
		return fmt.Errorf("deleteHeaderNumbersPastBlock(%d): %w", toBlock, err)
	}

	if err := rawdb.TruncateBlocks(ctx, tx, toBlock+1); err != nil {
		return fmt.Errorf("TruncateBlocks(%d): %w", toBlock+1, err)
	}

	if err := rawdb.TruncateTd(tx, toBlock+1); err != nil {
		return fmt.Errorf("TruncateTd(%d): %w", toBlock+1, err)
	}

	// Reset every chain-block-following stage's progress to toBlock.
	// Stages whose progress tracks the chain head — Headers, Bodies,
	// BlockHashes, Senders, Execution, TxLookup, Finish — must not
	// remain at a higher value than the unwind target: post-unwind
	// the chain "is at" toBlock, so any stage cursor past toBlock is
	// an invariant violation. The verifier (Phase 0) catches misses
	// here, but it's cheaper to set them right than to lean on the
	// assertion. Forward-leaning stages (OtterSync / CustomTrace /
	// WitnessProcessing) track the snapshot or index frontier rather
	// than the chain head and are deliberately not touched.
	for _, stage := range []stages.SyncStage{
		stages.Headers,
		stages.BlockHashes,
		stages.Bodies,
		stages.Senders,
		stages.Execution,
		stages.TxLookup,
		stages.Finish,
	} {
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

	if p.Aggregator != nil {
		lastTxNum, err := rawdbv3.TxNums.Max(ctx, tx, toBlock)
		if err != nil {
			return fmt.Errorf("TxNums.Max(%d) for shadow wipe: %w", toBlock, err)
		}
		if err := p.Aggregator.WipeWritableShadowPast(ctx, tx, lastTxNum); err != nil {
			return fmt.Errorf("WipeWritableShadowPast(lastTxNum=%d): %w", lastTxNum, err)
		}
	}

	return nil
}

// deleteHeaderNumbersPastBlock walks kv.Headers from toBlock+1 and
// removes the corresponding kv.HeaderNumber (hash → number) entries.
//
// kv.Headers is keyed as 8-byte big-endian block-number || 32-byte
// hash; the hash portion is the kv.HeaderNumber key we need to delete.
// rawdb.TruncateBlocks (called next) wipes kv.Headers itself but does
// not touch kv.HeaderNumber, so the lookup map for the truncated
// hashes would survive as orphans pointing at deleted headers. Walk
// before TruncateBlocks so the hashes are still readable.
func deleteHeaderNumbersPastBlock(tx kv.RwTx, toBlock uint64) error {
	c, err := tx.RwCursor(kv.Headers)
	if err != nil {
		return err
	}
	defer c.Close()

	seekKey := dbutils.EncodeBlockNumber(toBlock + 1)
	for k, _, err := c.Seek(seekKey); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		if len(k) < 8+length.Hash {
			continue
		}
		// Defensive: stop if the iterator somehow moved back below
		// toBlock+1 (shouldn't happen with a sorted cursor, but a
		// short key could read as a small number).
		if binary.BigEndian.Uint64(k[:8]) <= toBlock {
			continue
		}
		hash := k[8 : 8+length.Hash]
		if err := tx.Delete(kv.HeaderNumber, hash); err != nil {
			return err
		}
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
