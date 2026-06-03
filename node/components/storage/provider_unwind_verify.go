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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// verifyPostUnwindDBImage asserts the writable DB image reflects an
// unwind to (toBlock, lastTxNum) and not a partially-completed one.
//
// Runs same-tx, after every sub-op of Provider.Unwind, before commit.
// If any check fails the caller surfaces the error so the mode-B tx
// rolls back — a DB image inconsistent with the unwind target is
// unrecoverable without a full restart-with-wipe otherwise, and the
// failure mode that surfaces hours later (wrong-block-data, wrong
// state root, firstNonGenesisCheck wedge) is far worse than aborting
// the unwind in-place.
//
// Checks:
//
//   - rawdbv3.TxNums.Last == toBlock (the canonical "where is the
//     chain" cursor).
//   - kv.Headers, kv.BlockBody, kv.HeaderTD, kv.Senders: no entry
//     whose key encodes a blockNum > toBlock.
//   - kv.EthTx: no entry whose key encodes a txnID > lastTxNum.
//   - Stage progress for Execution / Headers / BlockHashes / Bodies /
//     Senders / TxLookup / Finish: all ≤ toBlock. (Forward-leaning
//     stages — OtterSync, CustomTrace, WitnessProcessing — can
//     legitimately sit at higher values; they track the snapshot or
//     index frontier, not the chain head.)
//
// Out of scope (deferred):
//   - Writable-shadow per-domain tip. Aggregator's WipeWritableShadowPast
//     is independently exercised + tested; the Aggregator doesn't yet
//     expose a per-domain "max txNum present" cursor that's cheap to
//     call here. Add when Phase 3 surfaces a need or when Aggregator
//     ships that accessor.
//   - kv.HeaderNumber. Covered by deleteHeaderNumbersPastBlock
//     (f91116d31e); checking it here would require either a full scan
//     (the table is hash→num so there's no blockNum-ordered cursor)
//     or trusting kv.Headers's blockNum-ordered walk to imply
//     consistency — for now we trust the kv.Headers check.
//
// Returns a single error that names every check that failed (joined),
// so the caller sees the full picture rather than fixing one and
// re-running to surface the next.
func verifyPostUnwindDBImage(ctx context.Context, tx kv.Tx, toBlock, lastTxNum uint64) error {
	var failures []string

	if blk, _, err := rawdbv3.TxNums.Last(tx); err != nil {
		failures = append(failures, fmt.Sprintf("TxNums.Last: %v", err))
	} else if blk != toBlock {
		failures = append(failures, fmt.Sprintf("TxNums.Last=%d, want %d", blk, toBlock))
	}

	blockTables := []string{
		kv.Headers,
		kv.BlockBody,
		kv.HeaderTD,
		kv.Senders,
	}
	for _, t := range blockTables {
		if bn, ok, err := lastBlockNumInBlockTable(tx, t); err != nil {
			failures = append(failures, fmt.Sprintf("%s last-key: %v", t, err))
		} else if ok && bn > toBlock {
			failures = append(failures, fmt.Sprintf("%s last blockNum=%d > toBlock=%d", t, bn, toBlock))
		}
	}

	if tid, ok, err := lastUint64KeyInTable(tx, kv.EthTx); err != nil {
		failures = append(failures, fmt.Sprintf("EthTx last-key: %v", err))
	} else if ok && tid > lastTxNum {
		failures = append(failures, fmt.Sprintf("EthTx last txnID=%d > lastTxNum=%d", tid, lastTxNum))
	}

	stagesToCheck := []stages.SyncStage{
		stages.Headers,
		stages.BlockHashes,
		stages.Bodies,
		stages.Senders,
		stages.Execution,
		stages.TxLookup,
		stages.Finish,
	}
	for _, s := range stagesToCheck {
		p, err := stages.GetStageProgress(tx, s)
		if err != nil {
			failures = append(failures, fmt.Sprintf("stage %s progress: %v", s, err))
			continue
		}
		if p > toBlock {
			failures = append(failures, fmt.Sprintf("stage %s progress=%d > toBlock=%d", s, p, toBlock))
		}
	}

	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("post-unwind DB image inconsistent with toBlock=%d lastTxNum=%d: %v", toBlock, lastTxNum, failures)
}

// lastBlockNumInBlockTable returns the blockNum of the last key in a
// table whose keys are formatted as `blockNum (8 bytes BE) + hash (32
// bytes)`. The blockNum-prefix encoding makes the cursor's Last() the
// max-blockNum entry. Empty table → ok=false.
func lastBlockNumInBlockTable(tx kv.Tx, table string) (blockNum uint64, ok bool, err error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return 0, false, err
	}
	defer c.Close()
	k, _, err := c.Last()
	if err != nil {
		return 0, false, err
	}
	if len(k) < 8 {
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(k[:8]), true, nil
}

// lastUint64KeyInTable returns the uint64 of the last key in a table
// whose keys are bare 8-byte big-endian uint64 (kv.EthTx). Empty table
// → ok=false.
func lastUint64KeyInTable(tx kv.Tx, table string) (val uint64, ok bool, err error) {
	c, err := tx.Cursor(table)
	if err != nil {
		return 0, false, err
	}
	defer c.Close()
	k, _, err := c.Last()
	if err != nil {
		return 0, false, err
	}
	if len(k) < 8 {
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(k[:8]), true, nil
}
