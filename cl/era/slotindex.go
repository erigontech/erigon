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
	"encoding/binary"
	"fmt"
)

// SlotIndex is a decoded SlotIndex record. Each offset is measured in bytes
// from the start of the SlotIndex record to the start of the data record for
// that slot; an offset of 0 means no data is present for the slot.
//
// On-wire layout of the record payload:
//
//	starting-slot [8 LE] | offset[count] [8 LE signed each] | count [8 LE]
type SlotIndex struct {
	StartSlot uint64
	Offsets   []int64
}

// parseSlotIndex decodes a SlotIndex record payload.
func parseSlotIndex(data []byte) (SlotIndex, error) {
	if len(data) < 16 {
		return SlotIndex{}, fmt.Errorf("era: SlotIndex payload too short: %d bytes", len(data))
	}
	startSlot := binary.LittleEndian.Uint64(data[:8])
	count := binary.LittleEndian.Uint64(data[len(data)-8:])

	want := 16 + count*8
	if uint64(len(data)) != want {
		return SlotIndex{}, fmt.Errorf("era: SlotIndex length mismatch: have %d, want %d (count=%d)", len(data), want, count)
	}

	offsets := make([]int64, count)
	for i := uint64(0); i < count; i++ {
		offsets[i] = int64(binary.LittleEndian.Uint64(data[8+i*8 : 16+i*8]))
	}
	return SlotIndex{StartSlot: startSlot, Offsets: offsets}, nil
}

// encode serialises the SlotIndex into a record payload.
func (s SlotIndex) encode() []byte {
	out := make([]byte, 16+len(s.Offsets)*8)
	binary.LittleEndian.PutUint64(out[:8], s.StartSlot)
	for i, off := range s.Offsets {
		binary.LittleEndian.PutUint64(out[8+i*8:16+i*8], uint64(off))
	}
	binary.LittleEndian.PutUint64(out[len(out)-8:], uint64(len(s.Offsets)))
	return out
}
