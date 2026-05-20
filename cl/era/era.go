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

package era

import (
	"errors"
	"fmt"
	"io"
)

// SlotsPerHistoricalRoot is the era length on the mainnet configuration.
const SlotsPerHistoricalRoot = 8192

var errNoVersion = errors.New("era: record encountered before Version record")

// GroupSummary is a structural description of one era group: how many block
// records it carries, whether it has a state, and its slot indices. No
// block or state payload is decompressed or SSZ-decoded.
type GroupSummary struct {
	BlockRecords int
	HasState     bool
	BlockIndex   *SlotIndex // nil for the genesis era (no blocks)
	StateIndex   *SlotIndex
	UnknownTypes map[[2]byte]int // unrecognised record types -> count
}

// Scan reads an .era file sequentially and reports the record structure of
// each group, without decompressing or SSZ-decoding block/state payloads.
// It is the structural-validation entry point: a file that Scans cleanly is
// well-framed e2store with a valid .era group layout.
func Scan(r io.Reader) ([]GroupSummary, error) {
	var groups []GroupSummary
	var cur *GroupSummary
	var curSlotIndexes []SlotIndex

	finalize := func() error {
		if cur == nil {
			return nil
		}
		switch len(curSlotIndexes) {
		case 1:
			// genesis era: state index only.
			si := curSlotIndexes[0]
			cur.StateIndex = &si
		case 2:
			bi, si := curSlotIndexes[0], curSlotIndexes[1]
			cur.BlockIndex = &bi
			cur.StateIndex = &si
		default:
			return fmt.Errorf("era: group has %d SlotIndex records, want 1 or 2", len(curSlotIndexes))
		}
		return nil
	}

	for {
		rec, err := readE2storeRecord(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch rec.typ {
		case TypeVersion:
			if len(rec.data) != 0 {
				return nil, fmt.Errorf("era: Version record must be empty, got %d bytes", len(rec.data))
			}
			if err := finalize(); err != nil {
				return nil, err
			}
			groups = append(groups, GroupSummary{UnknownTypes: map[[2]byte]int{}})
			cur = &groups[len(groups)-1]
			curSlotIndexes = curSlotIndexes[:0]

		case TypeCompressedBlock:
			if cur == nil {
				return nil, errNoVersion
			}
			cur.BlockRecords++

		case TypeCompressedState:
			if cur == nil {
				return nil, errNoVersion
			}
			cur.HasState = true

		case TypeSlotIndex:
			if cur == nil {
				return nil, errNoVersion
			}
			si, err := parseSlotIndex(rec.data)
			if err != nil {
				return nil, err
			}
			curSlotIndexes = append(curSlotIndexes, si)

		default:
			// The spec requires readers to skip unknown record types.
			if cur == nil {
				return nil, errNoVersion
			}
			cur.UnknownTypes[rec.typ]++
		}
	}

	if err := finalize(); err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, errors.New("era: no groups (empty or non-era file)")
	}
	return groups, nil
}
