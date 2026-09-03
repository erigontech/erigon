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
	"errors"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

var errSplitNotBranch = errors.New("commitment: no stored branch at split prefix")

func unfoldSplitBase(base *HexPatriciaHashed, prefix []byte) error {
	d := int16(len(prefix))
	copy(base.currentKey[:], prefix)
	base.currentKeyLen = d
	base.depths[0] = d + 1
	base.activeRows = 1
	for i := range base.grid[0] {
		base.grid[0][i].reset()
	}
	base.touchMap[0], base.afterMap[0], base.branchBefore[0] = 0, 0, false

	branch, err := base.branchFromCacheOrDB(nibbles.HexToCompactInto(base.compactKeyBuf[:], prefix))
	if err != nil {
		return err
	}
	if len(branch) == 0 {
		return errSplitNotBranch
	}
	if len(branch) < 4 {
		return fmt.Errorf("unfoldSplitBase: corrupt branch record at %x: %d bytes", prefix, len(branch))
	}
	if BranchData(branch).ChildCount() == 0 {
		return errSplitNotBranch
	}
	base.branchBefore[0] = true
	return base.decodeBranchIntoRow(0, d+1, branch[2:], false)
}

func stitchSplitCells(base *HexPatriciaHashed, cells *[16]cell, present uint16) {
	for bm := present; bm != 0; {
		bit := bm & -bm
		nib := bits.TrailingZeros16(bit)
		base.touchMap[0] |= bit
		if cells[nib].IsEmpty() {
			base.afterMap[0] &^= bit
			base.grid[0][nib].reset()
		} else {
			base.afterMap[0] |= bit
			base.grid[0][nib] = cells[nib]
		}
		bm ^= bit
	}
}

type foldTo uint8

const (
	foldToCell foldTo = iota
	foldToRoot
)

func foldSplitRow(ctx context.Context, base *HexPatriciaHashed, to foldTo) (cell, error) {
	if to == foldToRoot {
		if base.activeRows == 0 {
			base.activeRows = 1
		}
		for base.activeRows > 0 {
			if err := ctx.Err(); err != nil {
				return cell{}, err
			}
			if err := base.fold(); err != nil {
				return cell{}, err
			}
		}
		return base.root, nil
	}
	if base.afterMap[0] == 0 && !base.branchBefore[0] {
		base.activeRows = 0
		return cell{}, nil
	}
	if kind, _ := afterMapUpdateKind(base.afterMap[0]); kind == updateKindPropagate {
		return splitCellFromSingleChild(base)
	}
	if err := base.fold(); err != nil {
		return cell{}, err
	}
	out := base.root
	out.extLen = 0
	return out, nil
}

func splitCellFromSingleChild(base *HexPatriciaHashed) (cell, error) {
	survNib := bits.TrailingZeros16(base.afterMap[0])
	child := base.grid[0][survNib]

	if base.branchBefore[0] {
		if err := base.collectDeleteUpdate(nibbles.HexToCompactInto(base.compactKeyBuf[:], base.currentKey[:base.currentKeyLen]), 0); err != nil {
			return cell{}, err
		}
	}
	base.activeRows = 0

	var out cell
	d := base.depths[0]
	if child.hashLen > 0 && ((child.accountAddrLen == 0 && d < 64) || (child.storageAddrLen == 0 && d > 64)) {
		out.extLen = child.extLen + 1
		out.extension[0] = byte(survNib)
		copy(out.extension[1:], child.extension[:child.extLen])
		out.hashLen = child.hashLen
		copy(out.hash[:], child.hash[:child.hashLen])
		out.hashedExtLen = out.extLen
		copy(out.hashedExtension[:], out.extension[:out.extLen])
	} else {
		out = child
	}
	return out, nil
}
