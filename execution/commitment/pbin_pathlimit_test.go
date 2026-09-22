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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

// A key path of exactly pbinMaxPathBits is decodable off the wire but cannot be
// a branch: naming either child needs one bit more than a path holds. Both
// places that descend from a branch build that child path, so an untrusted node
// set must be refused there rather than panic inside the bit-path arithmetic.

func pbinTestFullLengthPath() pbinBitpath {
	return pbinPathFromBits(bytes.Repeat([]byte{0xAA}, pbinMaxPathBits/8), pbinMaxPathBits)
}

type pbinTestFixedBranchCtx struct {
	PatriciaContext
	record []byte
}

func (c *pbinTestFixedBranchCtx) Branch([]byte) ([]byte, kv.Step, error) { return c.record, 0, nil }

// TestPBinWitnessRefusesBranchAtMaxPath: a witness whose node at the longest
// representable path is a branch is malformed, and reading its record must say
// so instead of panicking.
func TestPBinWitnessRefusesBranchAtMaxPath(t *testing.T) {
	t.Parallel()

	prefix := pbinTestFullLengthPath()
	root := common.Hash{0x01}
	tree := &pbinWitnessTree{
		root: root,
		nodes: map[common.Hash]pbinWitnessNode{
			root: {tag: pbinBranchTag, prefix: prefix, children: [2]common.Hash{{0x02}, {0x03}}},
		},
	}

	_, _, err := pbinNewWitnessContext(tree).Branch(pbinEncodeBitPath(&prefix))
	require.ErrorIs(t, err, errPBinWitnessNode)
}

// TestPBinMaterializeRefusesBranchAtMaxPath: a prefix decoded from a witness is
// bounded on its own, so a cell reached at 528-n bits may carry an n-bit prefix
// that lands the branch exactly at the limit.
func TestPBinMaterializeRefusesBranchAtMaxPath(t *testing.T) {
	t.Parallel()

	empty := pbinNewWitnessContext(&pbinWitnessTree{nodes: map[common.Hash]pbinWitnessNode{}})
	var atRoot pbinBitpath
	record, err := empty.branchRecord(&pbinWitnessNode{
		tag: pbinBranchTag, children: [2]common.Hash{{0x02}, {0x03}},
	}, &atRoot)
	require.NoError(t, err)

	pph := NewPBinPatriciaHashed(&pbinTestFixedBranchCtx{record: record})
	defer pph.Release()

	var cell pbinCell
	cell.reset()
	cell.kind = pbinNodeBranch
	cell.prefix = pbinPathFromBits([]byte{0xAA}, 8)
	path := pbinPathFromBits(bytes.Repeat([]byte{0xAA}, pbinMaxPathBits/8-1), pbinMaxPathBits-8)

	require.ErrorIs(t, pph.materializeBranch(&cell, &path), errPBinCellHash)
}
