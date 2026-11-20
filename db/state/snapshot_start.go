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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// Reads and pruning treat everything below the file end as frozen. A fileless
// domain tracks its earliest write in firstTxNum. Writers can lower that start
// until the builder reserves its first file. frozenEnd is the exclusive end
// of the reserved range, including receipt files still being built.
type snapshotStart struct {
	firstTxNum, frozenEnd uint64
}

func snapshotStartKey(table string) []byte { return []byte(table + "snapshotStart") }

func readSnapshotRange(tx kv.Getter, key []byte) (from, to uint64, found bool, err error) {
	v, err := tx.GetOne(kv.TblPruningValsProg, key)
	if err != nil || v == nil {
		return 0, 0, false, err
	}
	from, to, err = decodeRange(v)
	return from, to, true, err
}

func readSnapshotStart(tx kv.Tx, table string) (s snapshotStart, found bool, err error) {
	s.firstTxNum, s.frozenEnd, found, err = readSnapshotRange(tx, snapshotStartKey(table))
	return s, found, err
}

// ResetSnapshotProgress clears the snapshot start and pruning bookmarks when a table is emptied.
func ResetSnapshotProgress(tx kv.Putter, table string) error {
	if err := InvalidatePruneProgress(tx, table); err != nil {
		return err
	}
	return tx.Delete(kv.TblPruningValsProg, snapshotStartKey(table))
}

func (s snapshotStart) save(tx kv.RwTx, table string) error {
	return tx.Put(kv.TblPruningValsProg, snapshotStartKey(table), encodeRange(s.firstTxNum, s.frozenEnd))
}

func (w *DomainBufferedWriter) recordSnapshotStart(tx kv.RwTx) error {
	s, found, err := readSnapshotStart(tx, w.valsTable)
	if err != nil {
		return err
	}
	first := w.minWriteStep.ToTxNum(w.h.ii.stepSize)
	if end := max(s.frozenEnd, w.visibleEnd); first < end {
		return fmt.Errorf("cannot write %s step %d: snapshot already covers txNums below %d", w.name, w.minWriteStep, end)
	}
	if found && first >= s.firstTxNum {
		return nil
	}
	if !found {
		existing, err := kv.FirstKey(tx, w.valsTable)
		if err != nil {
			return err
		}
		if existing != nil {
			// Older binaries wrote no start record. Their rows may predate this batch.
			legacy, err := legacySnapshotStart(tx, w.name, w.valsTable, w.largeVals)
			if err != nil {
				return err
			}
			first = min(first, legacy.ToTxNum(w.h.ii.stepSize))
		}
	}
	return (snapshotStart{firstTxNum: first}).save(tx, w.valsTable)
}

// Receipt steps are stored in reverse order within each logical key. Receipt
// metadata uses three keys and the receipt cache uses one, so finding the oldest
// step needs a bounded number of seeks even when millions of receipts are in MDBX.
func legacySnapshotStart(tx kv.Tx, name kv.Domain, table string, largeValues bool) (kv.Step, error) {
	if !isReceiptDomain(name) {
		return 0, nil // A late start cannot be proven without scanning these tables.
	}
	if largeValues {
		first, err := kv.FirstKey(tx, table)
		if err != nil || first == nil {
			return kv.NoStepBound, err
		}
		last, err := kv.LastKey(tx, table)
		if err != nil {
			return 0, err
		}
		if len(first) < 8 || len(last) < 8 || !bytes.Equal(first[:len(first)-8], last[:len(last)-8]) {
			return 0, nil // A different key layout cannot certify a nonzero start.
		}
		return kv.Step(^binary.BigEndian.Uint64(last[len(last)-8:])), nil
	}

	c, err := tx.CursorDupSort(table)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	oldest := kv.NoStepBound
	for k, _, err := c.First(); ; k, _, err = c.NextNoDup() {
		if err != nil || k == nil {
			return oldest, err
		}
		v, err := c.LastDup()
		if err != nil {
			return 0, err
		}
		if len(v) < 8 {
			return 0, fmt.Errorf("invalid %s value: missing step", table)
		}
		oldest = min(oldest, kv.Step(^binary.BigEndian.Uint64(v)))
		if oldest == 0 {
			return oldest, nil
		}
	}
}

// Reserve before collation so a writer either commits in time to be included or
// sees the reservation at flush. The files themselves are built outside the lock.
func (d *Domain) prepareSnapshotStep(ctx context.Context, db kv.TemporalRoDB, next, step kv.Step) (bool, error) {
	receipt := isReceiptDomain(d.Name)
	if next != 0 && (step != next || !receipt) {
		return step == next, nil
	}
	var start snapshotStart
	var ready, reserved bool
	check := func(tx kv.Tx) error {
		s, found, err := readSnapshotStart(tx, d.ValuesTable)
		if err != nil {
			return err
		}
		switch {
		case next != 0:
			start = s
			ready, reserved = true, s.frozenEnd >= (step+1).ToTxNum(d.stepSize)
		case step == 0:
			// An explicit backfill can start at zero until a late start is reserved.
			ready = s.frozenEnd == 0 || s.firstTxNum < d.stepSize
			reserved = found && s.firstTxNum == 0 && (!receipt || s.frozenEnd >= d.stepSize)
			start = snapshotStart{frozenEnd: s.frozenEnd}
		default:
			if !found {
				var first kv.Step
				first, err = legacySnapshotStart(tx, d.Name, d.ValuesTable, d.LargeValues)
				if err != nil || first == kv.NoStepBound {
					ready = false
					return err
				}
				s.firstTxNum = first.ToTxNum(d.stepSize)
			}
			start = s
			ready = kv.Step(s.firstTxNum/d.stepSize) == step
			reserved = s.frozenEnd >= (step + 1).ToTxNum(d.stepSize)
		}
		if ready && !reserved && receipt {
			ready, err = d.receiptStepComplete(tx, step)
		}
		return err
	}
	if err := db.View(ctx, check); err != nil || !ready || reserved {
		return ready, err
	}
	writer, ok := db.(kv.RwDB)
	if !ok || writer.ReadOnly() {
		return step == 0 && !receipt, nil
	}
	err := writer.Update(ctx, func(tx kv.RwTx) error {
		if err := check(tx); err != nil || !ready || reserved {
			return err
		}
		if step != 0 || receipt {
			start.frozenEnd = (step + 1).ToTxNum(d.stepSize)
		}
		return start.save(tx, d.ValuesTable)
	})
	return ready, err
}
