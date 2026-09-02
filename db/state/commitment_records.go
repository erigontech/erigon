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

	"github.com/erigontech/erigon/common/murmur3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
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
	// Fill the cache only from an unbounded read that consulted the DB first: a bounded read sees
	// a staged unwind, and a files-only read would cache a value a newer DB write supersedes.
	cacheBranch := includeDB && roTx != nil && maxTxNum == math.MaxUint64

	childKey := make([]byte, len(nodeKey)+1)
	copy(childKey, nodeKey)

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
				at.cacheLatestBranch(cacheBranch, key, records[nibble], valueStep, valueStep.LastTxNum(at.StepSize()))
				if valueStep > step {
					step = valueStep
				}
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
		present, err = scanCommitmentRecordFile(dt, i, nodeKey, wanted, present, &records)
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
				childKey[len(nodeKey)] = 0x80 | byte(nibble)
				at.cacheLatestBranch(cacheBranch, childKey, records[nibble], fileStep, file.endTxNum)
				bitset ^= bit
			}
		}
	}
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

func scanCommitmentRecordFile(dt *DomainRoTx, fileIndex int, nodeKey []byte, wanted, present uint16, records *[16][]byte) (uint16, error) {
	index := dt.statelessBtree(fileIndex)
	reader := dt.reusableReader(fileIndex)
	return scanCommitmentRecordRun(nodeKey, wanted, present, records, func(key []byte) (commitmentRecordCursor, error) {
		return commitmentCursor(index.Seek(reader, key))
	})
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
