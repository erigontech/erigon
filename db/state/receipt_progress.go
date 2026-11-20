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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/erigontech/erigon/db/kv"
)

func isReceiptDomain(name kv.Domain) bool {
	return name == kv.ReceiptDomain || name == kv.RCacheDomain
}

var (
	receiptBackfillKey = [kv.DomainLen][]byte{
		kv.ReceiptDomain: []byte(kv.TblReceiptVals + "backfill"),
		kv.RCacheDomain:  []byte(kv.TblRCacheVals + "backfill"),
	}
	receiptExecutionKey = [kv.DomainLen][]byte{
		kv.ReceiptDomain: []byte(kv.TblReceiptVals + "receiptExecution"),
		kv.RCacheDomain:  []byte(kv.TblRCacheVals + "receiptExecution"),
	}
)

// ReceiptBackfillProgress returns the next transaction to replay and the exclusive
// target. A paused replay remains authoritative even if live execution is far ahead.
func ReceiptBackfillProgress(tx kv.Tx, domain kv.Domain) (next, target uint64, ok bool, err error) {
	return readSnapshotRange(tx, receiptBackfillKey[domain])
}

func saveReceiptBackfillProgress(tx kv.Putter, domain kv.Domain, next, target uint64) error {
	return tx.Put(kv.TblPruningValsProg, receiptBackfillKey[domain], encodeRange(next, target))
}

// ResetReceiptBackfill keeps the cleared tables writable until replay establishes a target.
func ResetReceiptBackfill(tx kv.Putter, domain kv.Domain) error {
	if err := tx.Delete(kv.TblPruningValsProg, receiptExecutionKey[domain]); err != nil {
		return err
	}
	return saveReceiptBackfillProgress(tx, domain, 0, math.MaxUint64)
}

// BeginReceiptBackfill resumes after the last replayed transaction or the frozen
// range, whichever is further ahead. A reserved file may still be under construction.
func (at *AggregatorRoTx) BeginReceiptBackfill(tx kv.RwTx, domain kv.Domain, target uint64) (uint64, error) {
	d := at.d[domain]
	s, _, err := readSnapshotStart(tx, d.d.ValuesTable)
	if err != nil {
		return 0, err
	}
	next, _, _, err := ReceiptBackfillProgress(tx, domain)
	if err != nil {
		return 0, err
	}
	next = max(next, s.frozenEnd, d.files.EndTxNum())
	return next, saveReceiptBackfillProgress(tx, domain, next, target)
}

// AdvanceReceiptBackfill returns the first transaction this domain still needs.
// The caller replays the batch and commits its receipts in this same transaction.
func AdvanceReceiptBackfill(tx kv.RwTx, domain kv.Domain, from, end uint64) (uint64, error) {
	next, target, found, err := ReceiptBackfillProgress(tx, domain)
	if err != nil {
		return 0, err
	}
	if !found || from > next {
		return 0, fmt.Errorf("%s backfill starts at transaction %d, expected %d", domain, from, next)
	}
	return next, saveReceiptBackfillProgress(tx, domain, max(next, end), target)
}

// RecordReceiptExecution joins consecutive execution batches, including batches
// with no receipts. A batch that starts above a gap certifies only its own range.
func RecordReceiptExecution(tx kv.GetPut, domain kv.Domain, from, end uint64) error {
	if from >= end {
		return nil
	}
	key := receiptExecutionKey[domain]
	lo, hi, found, err := readSnapshotRange(tx, key)
	if err != nil {
		return err
	}
	if found && lo <= from && from <= hi {
		from = lo
		if end == hi {
			return nil
		}
	}
	return tx.Put(kv.TblPruningValsProg, key, encodeRange(from, end))
}

func (d *Domain) receiptStepComplete(tx kv.Tx, step kv.Step) (bool, error) {
	first, last := step.ToTxNum(d.stepSize), step.LastTxNum(d.stepSize)
	next, target, found, err := ReceiptBackfillProgress(tx, d.Name)
	if err != nil {
		return false, err
	}
	if found {
		if next > last {
			return true, nil
		}
		if next < target {
			return false, nil
		}
		// The last replay batch can end in the middle of a step. Live
		// execution must cover the rest, including any gap before this step.
		first = next
	}
	from, end, hasLiveRange, err := readSnapshotRange(tx, receiptExecutionKey[d.Name])
	if err != nil {
		return false, err
	}
	if from <= first && last < end {
		return true, nil
	}
	if found {
		return false, nil
	}
	// Older databases have no producer checkpoint. An empty domain can
	// publish an empty file. Otherwise use its own history to establish
	// how far receipt production reached.
	cursor, err := tx.Cursor(d.Hist.IiCfg.KeysTable)
	if err != nil {
		return false, err
	}
	defer cursor.Close()
	k, _, err := cursor.Seek(binary.BigEndian.AppendUint64(nil, last))
	if err != nil {
		return false, err
	}
	if k == nil {
		v, err := kv.FirstKey(tx, d.ValuesTable)
		return v == nil, err
	}
	if len(k) != 8 {
		return false, fmt.Errorf("invalid transaction number in %s: %x", d.Hist.IiCfg.KeysTable, k)
	}
	// A receipt from a newer execution range says nothing about the gap
	// between the old producer and the start of that range.
	return !hasLiveRange || binary.BigEndian.Uint64(k) < from, nil
}

func unwindReceiptBackfill(tx kv.RwTx, domain kv.Domain, end uint64) error {
	next, target, _, err := ReceiptBackfillProgress(tx, domain)
	if err != nil || next <= end {
		return err
	}
	if next >= target {
		target = end
	}
	return saveReceiptBackfillProgress(tx, domain, end, target)
}

// UnwindReceiptExecution truncates the live producer's range in the same
// overlay as execution progress, before any replacement blocks extend it.
func UnwindReceiptExecution(tx kv.GetPut, end uint64) error {
	for _, domain := range []kv.Domain{kv.ReceiptDomain, kv.RCacheDomain} {
		key := receiptExecutionKey[domain]
		from, to, _, err := readSnapshotRange(tx, key)
		if err != nil {
			return err
		}
		if end < to {
			if err := tx.Put(kv.TblPruningValsProg, key, encodeRange(min(from, end), end)); err != nil {
				return err
			}
		}
	}
	return nil
}
