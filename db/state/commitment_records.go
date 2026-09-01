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
	"math"
	"math/bits"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func (at *AggregatorRoTx) ReadCommitmentRecords(roTx kv.Tx, nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64) (records [16][]byte, present uint16, step kv.Step, err error) {
	return at.readCommitmentRecords(roTx, nodeKey, mask, maskKnown, maxTxNum, true)
}

func (at *AggregatorRoTx) ReadCommitmentRecordsFromFiles(nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64) (records [16][]byte, present uint16, step kv.Step, err error) {
	return at.readCommitmentRecords(nil, nodeKey, mask, maskKnown, maxTxNum, false)
}

func (at *AggregatorRoTx) readCommitmentRecords(roTx kv.Tx, nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64, includeDB bool) (records [16][]byte, present uint16, step kv.Step, err error) {
	if at == nil || at.d[kv.CommitmentDomain] == nil {
		return records, 0, 0, nil
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
	if includeDB && roTx != nil {
		for bitset := wanted; bitset != 0; {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			key := nibbles.ChildKeyV3(nodeKey, byte(nibble))
			value, valueStep, found, readErr := dt.getLatestFromDb(key, roTx, maxStep)
			if readErr != nil {
				return records, present, step, readErr
			}
			if found {
				records[nibble] = bytes.Clone(value)
				present |= bit
				if valueStep > step {
					step = valueStep
				}
			}
			bitset ^= bit
		}
	}

	for i := len(dt.files) - 1; i >= 0 && present&wanted != wanted; i-- {
		file := dt.files[i]
		if maxTxNum != math.MaxUint64 && file.startTxNum > maxTxNum {
			continue
		}
		if !statecfg.CommitmentEdgeRecords(file.src.version) || file.src.bindex == nil {
			continue
		}
		before := present
		present, err = scanCommitmentRecordFile(dt, i, nodeKey, wanted, present, &records)
		if err != nil {
			return records, present, step, err
		}
		if present != before {
			fileStep := kv.Step(file.endTxNum / at.StepSize())
			if fileStep > step {
				step = fileStep
			}
		}
	}
	return records, present, step, nil
}

func scanCommitmentRecordFile(dt *DomainRoTx, fileIndex int, nodeKey []byte, wanted, present uint16, records *[16][]byte) (uint16, error) {
	index := dt.statelessBtree(fileIndex)
	reader := dt.reusableReader(fileIndex)
	return scanCommitmentRecordRun(nodeKey, wanted, present, records, func(key []byte) (commitmentRecordCursor, error) {
		return index.Seek(reader, key)
	})
}

type commitmentRecordCursor interface {
	Key() []byte
	Value() []byte
	Next() bool
	Close()
}

func scanCommitmentRecordRun(nodeKey []byte, wanted, present uint16, records *[16][]byte, seek func([]byte) (commitmentRecordCursor, error)) (uint16, error) {
	cursor, err := seek(nibbles.ChildKeyV3(nodeKey, 0))
	if err != nil || cursor == nil {
		return present, err
	}

	seekedAtExpected := true
	for expected := 0; expected < 16 && present&wanted != wanted; {
		key := cursor.Key()
		if nibble, ok := directCommitmentChild(key, nodeKey); ok && nibble >= expected {
			bit := uint16(1) << nibble
			if wanted&bit != 0 && present&bit == 0 {
				records[nibble] = bytes.Clone(cursor.Value())
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

		if bytes.Compare(key, nibbles.ChildKeyV3(nodeKey, 15)) > 0 {
			cursor.Close()
			return present, nil
		}

		cursor.Close()
		// A seek at expected that returns anything but that child proves the file holds no record
		// for it: descendants of it sort after its key, so re-seeking the same slot returns this
		// key again forever.
		if seekedAtExpected {
			expected++
		}
		if expected == 16 {
			return present, nil
		}
		seekedAtExpected = true
		cursor, err = seek(nibbles.ChildKeyV3(nodeKey, byte(expected)))
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
