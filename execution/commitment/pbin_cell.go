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

// pbinCell is one of the two child slots of a binary node. Its prefix is always
// tree-key-space bits: unlike the hex engine's cell it needs no second key
// space, because PBin derives the tree key from the plain key on demand.
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

// pbinGridRows bounds the active rows: a row consumes at least the bit it splits
// on, so one row per path bit is enough.
const pbinGridRows = pbinMaxPathBits

// pbinGrid is the unfolded part of the tree: one row per level of descent, two
// cells per row. touchMap/afterMap are uint16 so the OnesCount16 /
// TrailingZeros16 arithmetic ports from the hex engine unchanged; only bits 0
// and 1 are ever set.
type pbinGrid struct {
	root         pbinCell
	rows         [pbinGridRows][2]pbinCell
	depths       [pbinGridRows]int16
	branchBefore [pbinGridRows]bool
	prevRecord   [pbinGridRows][]byte
	touchMap     [pbinGridRows]uint16
	afterMap     [pbinGridRows]uint16
	activeRows   int
}

// resetForReuse clears only the rows below activeRows. The stale cells above
// are safe because unfold initializes a row before anything reads it.
func (g *pbinGrid) resetForReuse() {
	g.root.reset()
	for row := range g.activeRows {
		g.rows[row][0].reset()
		g.rows[row][1].reset()
		g.depths[row] = 0
		g.branchBefore[row] = false
		g.prevRecord[row] = nil
		g.touchMap[row] = 0
		g.afterMap[row] = 0
	}
	g.activeRows = 0
}

// prevRecordFor returns the bytes the row unfolded from, zero-length when it had
// no record. Never nil, so the write layer takes it as the known previous value
// instead of reading the store itself.
func (g *pbinGrid) prevRecordFor(row int) []byte {
	if g.prevRecord[row] == nil {
		return []byte{}
	}
	return g.prevRecord[row]
}
