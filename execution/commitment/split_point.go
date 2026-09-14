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

package commitment

import (
	"context"
	"math/bits"
)

func unfoldToRow(ctx context.Context, base *HexPatriciaHashed, prefix []byte) (bool, error) {
	probe := make([]byte, len(prefix)+1)
	copy(probe, prefix)
	for base.needFolding(probe) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		foldDone := base.metrics.StartFolding(nil)
		foldErr := base.fold()
		if foldDone != nil {
			foldDone()
		}
		if foldErr != nil {
			return false, foldErr
		}
	}
	for u := base.needUnfolding(probe); u > 0; u = base.needUnfolding(probe) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		depth := int16(0)
		if base.activeRows > 0 {
			depth = base.depths[base.activeRows-1]
		}
		unfoldDone := base.metrics.StartUnfolding(nil)
		unfoldErr := base.unfold(probe, min(u, int16(len(probe))-depth))
		if unfoldDone != nil {
			unfoldDone()
		}
		if unfoldErr != nil {
			return false, unfoldErr
		}
	}
	return base.activeRows > 0 && base.depths[base.activeRows-1] == int16(len(prefix))+1, nil
}

type openedRow struct {
	opened    bool
	row       int
	upBit     uint16
	upTouched bool
	upPresent bool
}

func openEmptyRow(base *HexPatriciaHashed, prefix []byte) openedRow {
	if unfoldDone := base.metrics.StartUnfolding(nil); unfoldDone != nil {
		unfoldDone()
	}
	row := base.activeRows
	out := openedRow{opened: true, row: row}
	if row > 0 {
		up := row - 1
		out.upBit = uint16(1) << prefix[base.depths[up]-1]
		out.upTouched = base.touchMap[up]&out.upBit != 0
		out.upPresent = base.afterMap[up]&out.upBit != 0
		base.touchMap[up] |= out.upBit
		base.afterMap[up] |= out.upBit
	}
	copy(base.currentKey[:], prefix)
	base.currentKeyLen = int16(len(prefix))
	base.depths[row] = int16(len(prefix)) + 1
	base.touchMap[row], base.afterMap[row], base.branchBefore[row] = 0, 0, false
	for i := range base.grid[row] {
		base.grid[row][i].reset()
	}
	base.activeRows = row + 1
	return out
}

func (o openedRow) closeIfEmpty(base *HexPatriciaHashed) {
	if !o.opened || base.afterMap[o.row] != 0 {
		return
	}
	base.activeRows = o.row
	base.currentKeyLen = 0
	if o.row == 0 {
		return
	}
	up := o.row - 1
	base.currentKeyLen = base.depths[up] - 1
	if o.upTouched {
		base.touchMap[up] |= o.upBit
	} else {
		base.touchMap[up] &^= o.upBit
	}
	if o.upPresent {
		base.afterMap[up] |= o.upBit
	} else {
		base.afterMap[up] &^= o.upBit
	}
}

func stitchSplitCells(base *HexPatriciaHashed, cells *[16]cell, present uint16) {
	row := max(base.activeRows-1, 0)
	for bm := present; bm != 0; {
		bit := bm & -bm
		nib := bits.TrailingZeros16(bit)
		base.touchMap[row] |= bit
		if cells[nib].IsEmpty() {
			base.afterMap[row] &^= bit
			base.grid[row][nib].reset()
		} else {
			base.afterMap[row] |= bit
			base.grid[row][nib] = cells[nib]
		}
		bm ^= bit
	}
}

func foldSplitRow(ctx context.Context, base *HexPatriciaHashed) (cell, error) {
	for base.activeRows > 0 {
		if err := ctx.Err(); err != nil {
			return cell{}, err
		}
		foldDone := base.metrics.StartFolding(nil)
		foldErr := base.fold()
		if foldDone != nil {
			foldDone()
		}
		if foldErr != nil {
			return cell{}, foldErr
		}
	}
	return base.root, nil
}
