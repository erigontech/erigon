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
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// countingWriter wraps an io.Writer and tracks how many bytes have been
// written, so slot-index offsets can be computed in a single pass.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// WriteEra writes e as a single-group .era file to w. Blocks are written in
// slot order; empty slots are skipped in the block records and marked in the
// block SlotIndex with the sentinel offset (pointing at the group's Version
// record at byte 0).
func WriteEra(w io.Writer, e *Era) error {
	if e.State == nil {
		return fmt.Errorf("era: WriteEra: era %d has no state", e.Number)
	}
	cw := &countingWriter{w: w}

	if _, err := writeE2storeRecord(cw, TypeVersion, nil); err != nil {
		return err
	}

	var startSlot uint64
	if e.Number > 0 {
		startSlot = (e.Number - 1) * SlotsPerHistoricalRoot
	}

	// Blocks, recording the file offset of each one's record.
	blockPos := make(map[uint64]int64, len(e.Blocks))
	for _, blk := range e.Blocks {
		ssz, err := blk.EncodeSSZ(nil)
		if err != nil {
			return fmt.Errorf("era: block SSZ encode (slot %d): %w", blk.Block.Slot, err)
		}
		framed, err := snappyEncode(ssz)
		if err != nil {
			return fmt.Errorf("era: block snappy encode (slot %d): %w", blk.Block.Slot, err)
		}
		blockPos[blk.Block.Slot] = cw.n
		if _, err := writeE2storeRecord(cw, TypeCompressedBlock, framed); err != nil {
			return err
		}
	}

	// State.
	statePos := cw.n
	stateSSZ, err := e.State.EncodeSSZ(nil)
	if err != nil {
		return fmt.Errorf("era: state SSZ encode: %w", err)
	}
	stateFramed, err := snappyEncode(stateSSZ)
	if err != nil {
		return fmt.Errorf("era: state snappy encode: %w", err)
	}
	if _, err := writeE2storeRecord(cw, TypeCompressedState, stateFramed); err != nil {
		return err
	}

	// Block SlotIndex — omitted for the genesis era (no blocks).
	if e.Number > 0 {
		blockIndexPos := cw.n
		bi := SlotIndex{
			StartSlot: startSlot,
			Offsets:   make([]int64, SlotsPerHistoricalRoot),
		}
		for i := range bi.Offsets {
			slot := startSlot + uint64(i)
			if p, ok := blockPos[slot]; ok {
				bi.Offsets[i] = p - blockIndexPos
			} else {
				// Sentinel: point at byte 0 (the Version record).
				bi.Offsets[i] = -blockIndexPos
			}
		}
		if _, err := writeE2storeRecord(cw, TypeSlotIndex, bi.encode()); err != nil {
			return err
		}
	}

	// State SlotIndex — count 1, start slot identifies the era.
	stateIndexPos := cw.n
	si := SlotIndex{
		StartSlot: e.Number * SlotsPerHistoricalRoot,
		Offsets:   []int64{statePos - stateIndexPos},
	}
	if _, err := writeE2storeRecord(cw, TypeSlotIndex, si.encode()); err != nil {
		return err
	}
	return nil
}

// snappyEncode encodes raw into snappy framed format.
func snappyEncode(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	sw := snappy.NewWriter(&buf)
	if _, err := sw.Write(raw); err != nil {
		return nil, err
	}
	if err := sw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
