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

package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// WipeWritableShadowPast clears every writable-domain MDBX entry whose
// txnum coordinate > lastTxNum. The post-state contract is per-tx, not
// per-step: the last tx surviving the wipe in any writable-shadow
// table is exactly lastTxNum (= the last txnum of toBlock), regardless
// of step alignment.
//
// Mode-B admin SetHead (docs/plans/20260525-admin-sethead-unwind-design.md,
// sub-op #2) calls this after snapshot files past toBlock have been
// trimmed. With the per-tx contract above, the MDBX writable-domain
// shadow holds nothing newer than lastTxNum, so the next forward
// execution sees the snapshot files as the authoritative state and
// LatestCommitmentState resolves to the file's entry at toBlock's step
// boundary — no DB-shadow override of a write past toBlock.
//
// Per writable domain (accounts, storage, code, commitment):
//
//   - ValuesTable: cursor-walk, decode the step coordinate from the
//     key suffix (LargeValues) or value prefix (DupSort), delete entries
//     whose step >= stepBoundary. The precondition guarantees lastTxNum
//     is the last txnum of step (stepBoundary - 1), so every entry at
//     step >= stepBoundary covers only txnums > lastTxNum.
//   - History.ValuesTable + InvertedIndex.{Keys,Values}Table: existing
//     HistoryRoTx.prune machinery (the same call d.unwind issues), with
//     txFrom = lastTxNum+1, txTo = math.MaxUint64.
//
// Standalone inverted indexes (Receipts, LogAddresses, LogTopics,
// TracesFrom, TracesTo): existing InvertedIndexRoTx.unwind with the same
// (lastTxNum+1, math.MaxUint64) range.
//
// Preconditions enforced inline:
//
//   - StepSize() > 0.
//   - (lastTxNum+1) % StepSize() == 0 — lastTxNum lands on a step
//     boundary. This is what makes the per-tx contract expressible by
//     step-granular deletion in ValuesTable: if lastTxNum sits mid-step
//     the values-table entry for that step would mix txnums on both
//     sides of lastTxNum and a step-granular wipe couldn't be precise.
//     Mode B's snapshot-trim already enforces this against toBlock; the
//     check here makes the file-vs-DB layering invariant explicit for
//     any future caller (fork-from CLI, tooling).
//
// Caller owns tx lifecycle and the commit. WipeWritableShadowPast does
// not commit and does not flush.
func (a *Aggregator) WipeWritableShadowPast(ctx context.Context, tx kv.TemporalRwTx, lastTxNum uint64) error {
	stepSize := a.StepSize()
	if stepSize == 0 {
		return fmt.Errorf("WipeWritableShadowPast: StepSize() == 0")
	}
	if (lastTxNum+1)%stepSize != 0 {
		return fmt.Errorf("WipeWritableShadowPast: lastTxNum=%d does not land on a step boundary (stepSize=%d, remainder=%d) — caller must align toBlock to a step boundary",
			lastTxNum, stepSize, (lastTxNum+1)%stepSize)
	}
	stepBoundary := (lastTxNum + 1) / stepSize

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	atRo := a.BeginFilesRo()
	defer atRo.Close()

	writableDomains := []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain}
	for _, name := range writableDomains {
		d := a.d[name]
		dt := atRo.d[name]

		if err := wipeDomainValuesPastStep(tx, d, stepBoundary); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: domain=%s ValuesTable: %w", d.FilenameBase, err)
		}

		if _, err := dt.ht.prune(ctx, tx, lastTxNum+1, math.MaxUint64, math.MaxUint64, true, logEvery); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: domain=%s history prune: %w", d.FilenameBase, err)
		}
	}

	for _, iit := range atRo.standaloneIIs() {
		if err := iit.unwind(ctx, tx, lastTxNum+1, math.MaxUint64, math.MaxUint64, logEvery, true); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: II=%s unwind: %w", iit.ii.FilenameBase, err)
		}
	}

	return nil
}

// wipeDomainValuesPastStep deletes ValuesTable rows whose step
// coordinate >= stepBoundary. With the caller's invariant
// `stepBoundary = (lastTxNum+1) / stepSize` and lastTxNum being the
// last txnum of toBlock (which the precondition guarantees is on a
// step boundary), step `stepBoundary-1` is exactly the step containing
// lastTxNum, and steps >= stepBoundary contain only txnums > lastTxNum
// — i.e. writes past toBlock that must not survive the wipe.
//
// The step coordinate's location depends on the domain's LargeValues
// setting:
//
//   - LargeValues=true: rows are key=fullKey+stepBytes, value=raw.
//     stepBytes is the last 8 bytes of the key, encoded as
//     ^binary.BigEndian.Uint64(step).
//   - LargeValues=false (DupSort): rows are key=fullKey,
//     value=stepBytes+raw. stepBytes is the first 8 bytes of the value,
//     same encoding.
//
// Mirrors the encoding d.unwind uses on its diff-entry write path.
func wipeDomainValuesPastStep(tx kv.RwTx, d *Domain, stepBoundary uint64) error {
	if d.LargeValues {
		c, err := tx.RwCursor(d.ValuesTable)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) < 8 {
				continue
			}
			encodedStep := binary.BigEndian.Uint64(k[len(k)-8:])
			step := ^encodedStep
			if step >= stepBoundary {
				if err := c.DeleteCurrent(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	c, err := tx.RwCursorDupSort(d.ValuesTable)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if len(v) < 8 {
			continue
		}
		encodedStep := binary.BigEndian.Uint64(v[:8])
		step := ^encodedStep
		if step >= stepBoundary {
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
	}
	return nil
}
