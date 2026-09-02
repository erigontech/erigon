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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/murmur3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func (at *AggregatorRoTx) ReadCommitmentRecords(roTx kv.Tx, nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64, wm kv.GetLatestMetrics) (records [16][]byte, present uint16, step kv.Step, err error) {
	return at.readCommitmentRecords(roTx, nodeKey, mask, maskKnown, maxTxNum, true, wm)
}

func (at *AggregatorRoTx) ReadCommitmentRecordsFromFiles(nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64) (records [16][]byte, present uint16, step kv.Step, err error) {
	return at.readCommitmentRecords(nil, nodeKey, mask, maskKnown, maxTxNum, false, nil)
}

func (at *AggregatorRoTx) readCommitmentRecords(roTx kv.Tx, nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64, includeDB bool, wm kv.GetLatestMetrics) (records [16][]byte, present uint16, step kv.Step, err error) {
	if at == nil || at.d[kv.CommitmentDomain] == nil {
		return records, 0, 0, nil
	}
	if !dbg.KVReadLevelledMetrics {
		wm = nil
	}
	wanted := mask
	if !maskKnown {
		wanted = ^uint16(0)
	}
	if wanted == 0 {
		return records, 0, 0, nil
	}

	dt := at.d[kv.CommitmentDomain]
	maxStep := kv.NoStepBound
	if maxTxNum != math.MaxUint64 {
		maxStep = kv.Step(maxTxNum / at.StepSize())
	}
	// Fill the cache only from an unbounded read that consulted the DB first: a bounded read sees
	// a staged unwind, and a files-only read would cache a value a newer DB write supersedes.
	cacheBranch := includeDB && roTx != nil && maxTxNum == math.MaxUint64

	childKey := make([]byte, len(nodeKey)+1)
	copy(childKey, nodeKey)

	// Cache fills are gathered and applied once per node: one Put per record costs two allocations
	// each, and a node resolves 3 records on average.
	var cacheMask uint16
	var cacheSteps, cacheTxNums [16]uint64

	if includeDB && roTx != nil {
		var dbSteps [16]kv.Step
		var dbStart time.Time
		if wm != nil {
			dbStart = time.Now()
		}
		before := present
		if maxStep == kv.NoStepBound {
			present, err = scanCommitmentChildrenFromDb(dt, roTx, nodeKey, childKey, wanted, present, &records, &dbSteps)
			if err != nil {
				return records, present, step, err
			}
		} else {
			// A step-bounded read wants the newest value at or below the bound, which is a
			// SeekBothRange within one key rather than a walk across keys.
			for bitset := wanted; bitset != 0; {
				bit := bitset & -bitset
				nibble := bits.TrailingZeros16(bit)
				childKey[len(nodeKey)] = 0x80 | byte(nibble)
				value, valueStep, found, readErr := dt.getLatestFromDb(childKey, roTx, maxStep)
				if readErr != nil {
					return records, present, step, readErr
				}
				if found {
					records[nibble] = bytes.Clone(value)
					dbSteps[nibble] = valueStep
					present |= bit
				}
				bitset ^= bit
			}
		}
		for bitset := present &^ before; bitset != 0; {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cacheMask |= bit
			cacheSteps[nibble] = uint64(dbSteps[nibble])
			cacheTxNums[nibble] = dbSteps[nibble].LastTxNum(at.StepSize())
			if dbSteps[nibble] > step {
				step = dbSteps[nibble]
			}
			if wm != nil {
				wm.UpdateDbReads(kv.CommitmentDomain, dbStart)
			}
			bitset ^= bit
		}
	}

	useExistence := dt.d.Accessors.Has(statecfg.AccessorExistence) && dt.ht.iit.salt != nil
	var salt uint32
	if useExistence {
		salt = *dt.ht.iit.salt
	}
	var childHashes [16]uint64
	var hashed uint16

	for i := len(dt.files) - 1; i >= 0 && present&wanted != wanted; i-- {
		file := dt.files[i]
		if maxTxNum != math.MaxUint64 && file.startTxNum > maxTxNum {
			continue
		}
		if !statecfg.CommitmentEdgeRecords(file.src.version) || file.src.bindex == nil {
			continue
		}
		missing := wanted &^ present
		if useExistence && file.src.existence != nil &&
			!childMayBeInFile(salt, file.src.existence, nodeKey, missing, childKey, &childHashes, &hashed) {
			continue
		}
		before := present
		var fileStart time.Time
		if wm != nil {
			fileStart = time.Now()
		}
		present, err = scanCommitmentRecordFile(dt, i, nodeKey, childKey, wanted, present, &records)
		if err != nil {
			return records, present, step, err
		}
		if present != before {
			fileStep := kv.Step(file.endTxNum / at.StepSize())
			if fileStep > step {
				step = fileStep
			}
			for bitset := present &^ before; bitset != 0; {
				bit := bitset & -bitset
				nibble := bits.TrailingZeros16(bit)
				cacheMask |= bit
				cacheSteps[nibble] = uint64(fileStep)
				cacheTxNums[nibble] = file.endTxNum
				if wm != nil {
					childKey[len(nodeKey)] = 0x80 | byte(nibble)
					wm.UpdateFileReadsUnique(kv.CommitmentDomain, childKey, fileStart)
				}
				bitset ^= bit
			}
		}
	}
	at.cacheLatestBranchChildren(cacheBranch, nodeKey, cacheMask, &records, &cacheSteps, &cacheTxNums)
	return records, present, step, nil
}

// childMayBeInFile probes the file's existence filter for the children still wanted. The filter is
// built over the same key bytes under the same salt, and a bloom filter has no false negatives, so
// "none of them" skips a whole B-tree descent. Hashes are computed once and reused across files.
func childMayBeInFile(salt uint32, filter *existence.Filter, nodeKey []byte, missing uint16,
	childKey []byte, hashes *[16]uint64, hashed *uint16) bool {
	for bitset := missing; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if *hashed&bit == 0 {
			childKey[len(nodeKey)] = 0x80 | byte(nibble)
			hashes[nibble], _ = murmur3.Sum128WithSeed(childKey, salt)
			*hashed |= bit
		}
		if filter.ContainsHash(hashes[nibble]) {
			return true
		}
		bitset ^= bit
	}
	return false
}

func scanCommitmentRecordFile(dt *DomainRoTx, fileIndex int, nodeKey, childKey []byte, wanted, present uint16, records *[16][]byte) (uint16, error) {
	index := dt.statelessBtree(fileIndex)
	reader := dt.reusableReader(fileIndex)
	return scanCommitmentRecordRun(nodeKey, childKey, wanted, present,
		func(key []byte) (commitmentRecordCursor, error) {
			return commitmentCursor(index.Seek(reader, key))
		},
		func(nibble int, cursor commitmentRecordCursor) bool {
			records[nibble] = bytes.Clone(cursor.Value())
			return true
		})
}

// scanCommitmentChildrenFromDb reads a node's child records with one cursor run instead of a
// B-tree descent per nibble. A node's 16 edge keys share key(P) and differ only in the last byte,
// so they sit next to each other in the vals table; only a node whose path ends in nibble 0xf can
// have a descendant sort between them, and the run re-seeks past those.
func scanCommitmentChildrenFromDb(dt *DomainRoTx, roTx kv.Tx, nodeKey, childKey []byte, wanted, present uint16,
	records *[16][]byte, steps *[16]kv.Step) (uint16, error) {
	valsC, err := dt.valsCursor(roTx)
	if err != nil {
		return present, err
	}
	dup, ok := valsC.(kv.CursorDupSort)
	if !ok {
		return present, errCommitmentValsNotDupSort
	}
	cursor := &dbRecordCursor{c: dup}
	filesEndTxNum := dt.files.EndTxNum()
	return scanCommitmentRecordRun(nodeKey, childKey, wanted, present,
		func(key []byte) (commitmentRecordCursor, error) {
			k, v, err := dup.Seek(key)
			if err != nil || k == nil {
				return nil, err
			}
			if len(v) < 8 {
				return nil, fmt.Errorf("commitment vals: value for %x is %d bytes, shorter than the step prefix", k, len(v))
			}
			cursor.k, cursor.v = k, v
			return cursor, nil
		},
		func(nibble int, _ commitmentRecordCursor) bool {
			// A db value older than the files holds no news: getLatest treats it as absent and
			// lets the file scan supply the current record.
			step := cursor.step()
			if step.LastTxNum(dt.stepSize) < filesEndTxNum {
				return false
			}
			records[nibble] = bytes.Clone(cursor.Value())
			steps[nibble] = step
			return true
		})
}

var errCommitmentValsNotDupSort = errors.New("commitment vals table is not dup-sorted")

// dbRecordCursor walks one distinct key at a time. The dup value carries the inverse step ahead of
// the record, which is why Value trims it.
type dbRecordCursor struct {
	c kv.CursorDupSort
	k []byte
	v []byte
}

func (d *dbRecordCursor) Key() []byte   { return d.k }
func (d *dbRecordCursor) Value() []byte { return d.v[8:] }
func (d *dbRecordCursor) Close()        {} // the cursor belongs to the DomainRoTx

func (d *dbRecordCursor) step() kv.Step {
	return kv.Step(^binary.BigEndian.Uint64(d.v[:8]))
}

func (d *dbRecordCursor) Next() bool {
	k, v, err := d.c.NextNoDup()
	if err != nil || k == nil || len(v) < 8 {
		return false
	}
	d.k, d.v = k, v
	return true
}

// commitmentCursor converts Seek's result. Seek yields a nil *Cursor past the end of the
// index, and returning that straight into the interface would make a non-nil interface
// holding a nil pointer, which no == nil check catches.
func commitmentCursor(cursor *btindex.Cursor, err error) (commitmentRecordCursor, error) {
	if cursor == nil {
		return nil, err
	}
	return cursor, err
}

type commitmentRecordCursor interface {
	Key() []byte
	Value() []byte
	Next() bool
	Close()
}

func scanCommitmentRecordRun(nodeKey, childKey []byte, wanted, present uint16,
	seek func([]byte) (commitmentRecordCursor, error),
	take func(nibble int, cursor commitmentRecordCursor) bool) (uint16, error) {
	childKey[len(nodeKey)] = 0x80
	cursor, err := seek(childKey)
	if err != nil || cursor == nil {
		return present, err
	}

	seekedAtExpected := true
	for expected := 0; expected < 16 && present&wanted != wanted; {
		key := cursor.Key()
		if nibble, ok := directCommitmentChild(key, nodeKey); ok && nibble >= expected {
			bit := uint16(1) << nibble
			if wanted&bit != 0 && present&bit == 0 && take(nibble, cursor) {
				present |= bit
			}
			expected = nibble + 1
			if expected == 16 || present&wanted == wanted || !cursor.Next() {
				cursor.Close()
				return present, nil
			}
			seekedAtExpected = false
			continue
		}

		childKey[len(nodeKey)] = 0x8f
		if bytes.Compare(key, childKey) > 0 {
			cursor.Close()
			return present, nil
		}

		cursor.Close()
		// A seek at expected that returns anything but that child proves the source holds no record
		// for it: descendants of it sort after its key, so re-seeking the same slot returns this
		// key again forever.
		if seekedAtExpected {
			expected++
		}
		if expected == 16 {
			return present, nil
		}
		seekedAtExpected = true
		childKey[len(nodeKey)] = 0x80 | byte(expected)
		cursor, err = seek(childKey)
		if err != nil || cursor == nil {
			return present, err
		}
	}
	cursor.Close()
	return present, nil
}

func directCommitmentChild(key, nodeKey []byte) (int, bool) {
	if len(key) != len(nodeKey)+1 || !bytes.Equal(key[:len(nodeKey)], nodeKey) {
		return 0, false
	}
	last := key[len(nodeKey)]
	if last < 0x80 || last > 0x8f {
		return 0, false
	}
	return int(last & 0x0f), true
}
