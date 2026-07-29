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
	"errors"
	"fmt"
)

// PBinPatriciaHashed computes commitment over EIP-8297's partitioned binary
// tree. It borrows the hex engine's grid, unfold and fold shape and none of its
// node model: arity is 2, there is no extension node and no storage root, and a
// leaf commits its complete tree key.
type PBinPatriciaHashed struct {
	grid       pbinGrid
	currentKey pbinBitpath // path from the root to the deepest active row, one bit per level
	ctx        PatriciaContext
	hasher     pbinHasher

	rootChecked bool // whether the root record is known to be absent
	rootTouched bool
	rootPresent bool
}

func NewPBinPatriciaHashed(ctx PatriciaContext) *PBinPatriciaHashed {
	return &PBinPatriciaHashed{ctx: ctx}
}

var errPBinMissingBranch = errors.New("pbin: branch record missing")

// pbinUnfoldAction is what needUnfolding tells unfold to do about one cell.
type pbinUnfoldAction uint8

const (
	// pbinUnfoldNone means the probe key's slot is already in the grid.
	pbinUnfoldNone pbinUnfoldAction = iota
	// pbinUnfoldRecord means the cell points straight at a stored node: read it.
	pbinUnfoldRecord
	// pbinUnfoldDescend means the probe key agrees with the cell's whole prefix,
	// so the descent runs through it and nothing below moves.
	pbinUnfoldDescend
	// pbinUnfoldSplit means the probe key leaves the cell's prefix partway, so the
	// node below drops a level and its prefix shrinks.
	pbinUnfoldSplit
)

// pbinUnfolding is needUnfolding's answer. Descend and Split are separate
// answers rather than one bit count because only Split shortens a stored node's
// prefix, and the prefix is inside that node's hash.
type pbinUnfolding struct {
	action pbinUnfoldAction
	// matched counts the cell prefix bits the probe key agrees with: the whole
	// prefix for Descend, short of it for Split.
	matched int16
}

// needUnfolding reports what the grid still needs before probe's slot is in it.
// Unlike the hex engine there is no terminator to discount and no account
// boundary to clamp to — one key space, one bit per level.
func (pph *PBinPatriciaHashed) needUnfolding(probe *pbinBitpath) pbinUnfolding {
	var cell *pbinCell
	var depth int16

	if pph.grid.activeRows == 0 {
		if pph.grid.root.kind == pbinNodeEmpty {
			if pph.rootChecked {
				return pbinUnfolding{}
			}
			return pbinUnfolding{action: pbinUnfoldRecord}
		}
		cell = &pph.grid.root
	} else {
		row := pph.grid.activeRows - 1
		depth = pph.grid.depths[row]
		if probe.bitLen <= depth {
			return pbinUnfolding{}
		}
		cell = &pph.grid.rows[row][probe.bit(depth-1)]
	}

	if cell.kind == pbinNodeEmpty {
		return pbinUnfolding{}
	}
	if cell.prefix.bitLen == 0 {
		if cell.kind == pbinNodeBranch {
			return pbinUnfolding{action: pbinUnfoldRecord}
		}
		return pbinUnfolding{}
	}

	matched := pbinCommonPrefixBitsAt(probe, depth, &cell.prefix)
	if matched < cell.prefix.bitLen {
		return pbinUnfolding{action: pbinUnfoldSplit, matched: matched}
	}
	if cell.kind == pbinNodeLeaf {
		return pbinUnfolding{} // keys are prefix-free, so probe is this leaf's key
	}
	return pbinUnfolding{action: pbinUnfoldDescend, matched: matched}
}

// unfold opens one more level of the grid along probe, per the plan needUnfolding
// produced.
func (pph *PBinPatriciaHashed) unfold(probe *pbinBitpath, u pbinUnfolding) error {
	if u.action == pbinUnfoldNone {
		return nil
	}
	g := &pph.grid

	var upCell *pbinCell
	var touched, present bool
	var upDepth int16

	if g.activeRows == 0 {
		if pph.rootChecked && g.root.kind == pbinNodeEmpty {
			return nil
		}
		upCell = &g.root
		touched, present = pph.rootTouched, pph.rootPresent
	} else {
		upRow := g.activeRows - 1
		upDepth = g.depths[upRow]
		upBit := probe.bit(upDepth - 1)
		upCell = &g.rows[upRow][upBit]
		touched = g.touchMap[upRow]&(uint16(1)<<upBit) != 0
		present = g.afterMap[upRow]&(uint16(1)<<upBit) != 0
		pph.currentKey.appendBit(upBit)
	}

	row := g.activeRows
	g.rows[row][0].reset()
	g.rows[row][1].reset()
	g.touchMap[row], g.afterMap[row], g.branchBefore[row] = 0, 0, false

	if u.action == pbinUnfoldRecord {
		return pph.unfoldBranchNode(row, upDepth+1, touched && !present)
	}

	consumed, err := pbinUnfoldConsumed(u, &upCell.prefix)
	if err != nil {
		return err
	}
	bit := upCell.prefix.bit(consumed - 1)
	if touched {
		g.touchMap[row] = uint16(1) << bit
	}
	if present {
		g.afterMap[row] = uint16(1) << bit
	}
	g.rows[row][bit].fillFromUpperCell(upCell, consumed)

	if consumed > 1 {
		head := upCell.prefix.slice(0, consumed-1)
		pph.currentKey.append(&head)
	}
	g.depths[row] = upDepth + consumed
	g.activeRows++
	return nil
}

// pbinUnfoldConsumed is how many of the cell's prefix bits this unfold takes:
// all of them when the probe key matched, and one past the divergence when it
// did not — that extra bit is what the new row branches on.
func pbinUnfoldConsumed(u pbinUnfolding, prefix *pbinBitpath) (int16, error) {
	switch u.action {
	case pbinUnfoldDescend:
		return prefix.bitLen, nil
	case pbinUnfoldSplit:
		if u.matched >= prefix.bitLen {
			return 0, fmt.Errorf("pbin: %d matched bits of a %d-bit prefix is not a split", u.matched, prefix.bitLen)
		}
		return u.matched + 1, nil
	default:
		return 0, fmt.Errorf("pbin: unfold action %d consumes no prefix bits", u.action)
	}
}

// unfoldBranchNode loads the record at the current descent key into a row. The
// key is reconstructed from the parent cell's stored prefix, which is the only
// place the bits between the two nodes exist.
func (pph *PBinPatriciaHashed) unfoldBranchNode(row int, depth int16, deleted bool) error {
	g := &pph.grid
	key := pbinEncodeBitPath(&pph.currentKey)

	data, _, err := pph.ctx.Branch(key)
	if err != nil {
		return fmt.Errorf("pbin: read branch at %x: %w", key, err)
	}
	if len(data) == 0 {
		if !pph.rootChecked && pph.currentKey.bitLen == 0 {
			pph.rootChecked = true
			return nil
		}
		return fmt.Errorf("%w at %x (%d bits)", errPBinMissingBranch, key, pph.currentKey.bitLen)
	}

	_, afterMap, err := pbinDecodeBranch(data, &g.rows[row])
	if err != nil {
		return fmt.Errorf("pbin: decode branch at %x: %w", key, err)
	}
	// The record's own touch map is write-time bookkeeping; nothing in this run
	// has touched the row yet. A parent cell that is touched but gone takes the
	// whole subtree with it.
	if deleted {
		g.touchMap[row], g.afterMap[row] = afterMap, 0
	} else {
		g.touchMap[row], g.afterMap[row] = 0, afterMap
	}
	g.branchBefore[row] = true
	g.depths[row] = depth
	g.activeRows++
	return nil
}

// fillFromUpperCell moves a cell one level down, dropping the prefix bits the
// descent has taken over. skip counts those bits and includes the one the new
// row branches on.
func (c *pbinCell) fillFromUpperCell(up *pbinCell, skip int16) {
	c.reset()
	if skip < up.prefix.bitLen {
		c.prefix = up.prefix.slice(skip, up.prefix.bitLen)
	}
	c.kind = up.kind
	c.accountAddrLen = up.accountAddrLen
	if up.accountAddrLen > 0 {
		c.accountAddr = up.accountAddr
	}
	c.storageAddrLen = up.storageAddrLen
	if up.storageAddrLen > 0 {
		c.storageAddr = up.storageAddr
	}
	c.hashLen = up.hashLen
	if up.hashLen > 0 {
		c.hash = up.hash
	}
	c.loaded = up.loaded
	c.Update = up.Update
}
