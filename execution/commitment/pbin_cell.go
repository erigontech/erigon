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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// pbinNodeKind says what a cell points at. EIP-8297 admits a branch whose
// prefix is empty, so the prefix length cannot stand in for the kind.
type pbinNodeKind uint8

const (
	pbinNodeEmpty pbinNodeKind = iota
	pbinNodeLeaf
	pbinNodeBranch
)

// pbinCell is one of the two child slots of a binary node.
//
// It carries a single prefix, unlike the hex engine's cell: HPH keeps hashed and
// plain key spaces apart because it navigates in one and stores plain keys in
// the other, whereas PBin derives the tree key from the plain key on demand, so
// the one prefix is always tree-key-space bits. There is no memoized leaf hash
// either — H(0x00||key||value) commits the complete key and has nothing worth
// caching.
//
// A branch cell's prefix is inside its hash, so re-cutting the prefix
// invalidates it. Two invariants keep that from going unnoticed: a non-zero
// hashLen means hash covers the prefix the cell holds now, and childrenSet means
// the cell can re-derive the hash for any prefix without touching the database.
type pbinCell struct {
	prefix      pbinBitpath
	hash        common.Hash
	children    [2]common.Hash
	accountAddr common.Address
	storageAddr [length.Addr + length.Hash]byte

	accountAddrLen int16
	storageAddrLen int16
	hashLen        int16
	kind           pbinNodeKind
	childrenSet    bool
	loaded         loadFlags
	Update
}

func (c *pbinCell) setFromUpdate(u *Update) { c.Update.Merge(u) }

func (c *pbinCell) reset() {
	*c = pbinCell{}
	c.Update.Reset()
}

const (
	// pbinGridRows bounds the active rows: a row consumes at least the bit it
	// splits on, so one row per path bit is enough.
	pbinGridRows = pbinMaxPathBits
	// pbinMaxDepths bounds anything indexed by bit depth, which is inclusive of a
	// full-length path and so runs one past the row count.
	pbinMaxDepths = pbinMaxPathBits + 1
)

// pbinGrid is the unfolded part of the tree: one row per level of descent, two
// cells per row. touchMap/afterMap are uint16 so the OnesCount16 /
// TrailingZeros16 arithmetic ports from the hex engine unchanged; only bits 0
// and 1 are ever set.
type pbinGrid struct {
	root         pbinCell
	rows         [pbinGridRows][2]pbinCell
	depths       [pbinGridRows]int16
	branchBefore [pbinGridRows]bool
	touchMap     [pbinGridRows]uint16
	afterMap     [pbinGridRows]uint16
	activeRows   int
}

func (g *pbinGrid) reset() {
	g.resetRows(len(g.rows))
}

// resetForReuse clears only the rows the finished run left live. Rows above
// activeRows keep stale cells, which is safe because unfold initializes a row
// before anything reads it.
func (g *pbinGrid) resetForReuse() {
	g.resetRows(g.activeRows)
}

func (g *pbinGrid) resetRows(rows int) {
	g.root.reset()
	g.activeRows = 0
	for row := range rows {
		g.rows[row][0].reset()
		g.rows[row][1].reset()
		g.depths[row] = 0
		g.branchBefore[row] = false
		g.touchMap[row] = 0
		g.afterMap[row] = 0
	}
}
